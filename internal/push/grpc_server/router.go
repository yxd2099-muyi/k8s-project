package grpc_server

import (
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"sync"
)

//var clog = logger.L

// Router 双索引路由表
// uidToGate: 推送时根据玩家ID快速找到网关连接（读多写少）
// gateToUids: 网关断开时批量清理对应所有玩家路由，避免脏数据
type Router struct {
	mu         sync.RWMutex
	uidToGate  map[uint64]*GateConn
	gateToUids map[*GateConn]map[uint64]bool
	clog       *zap.Logger
}

func NewRouter() *Router {
	return &Router{
		uidToGate:  make(map[uint64]*GateConn),
		gateToUids: make(map[*GateConn]map[uint64]bool),
		clog:       logger.L,
	}
}

// Add 玩家上线，绑定UID与网关；支持跨网关重连迁移
func (r *Router) Add(uid uint64, gc *GateConn) {
	clog := r.clog
	r.mu.Lock()
	defer r.mu.Unlock()

	// 优化：同一UID已经绑定到当前网关，直接返回，减少map操作
	if oldGc, ok := r.uidToGate[uid]; ok && oldGc == gc {
		return
	}

	// 清理旧网关绑定
	if oldGc, ok := r.uidToGate[uid]; ok {
		if uidSet, exist := r.gateToUids[oldGc]; exist {
			delete(uidSet, uid)
			// 子集合为空则回收key，释放内存
			if len(uidSet) == 0 {
				delete(r.gateToUids, oldGc)
			}
		}
		clog.Debug("user migrate to new gate",
			zap.Uint64("uid", uid),
			zap.String("old_gate", oldGc.gateAddr),
			zap.String("new_gate", gc.gateAddr))
	}

	// 写入新映射
	r.uidToGate[uid] = gc
	if _, exist := r.gateToUids[gc]; !exist {
		r.gateToUids[gc] = make(map[uint64]bool)
	}
	r.gateToUids[gc][uid] = true

	clog.Debug("user online bind gate",
		zap.Uint64("uid", uid),
		zap.String("gate_addr", gc.gateAddr))
}

// Remove 玩家主动下线，单条删除路由
func (r *Router) Remove(uid uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	clog := r.clog
	gc, ok := r.uidToGate[uid]
	if !ok {
		return
	}

	delete(r.uidToGate, uid)

	if uidSet, exist := r.gateToUids[gc]; exist {
		delete(uidSet, uid)
		if len(uidSet) == 0 {
			delete(r.gateToUids, gc)
		}
	}

	clog.Debug("user offline remove route",
		zap.Uint64("uid", uid),
		zap.String("gate_addr", gc.gateAddr))
}

// Get 根据UID查询网关连接，高并发读锁
func (r *Router) Get(uid uint64) *GateConn {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.uidToGate[uid]
}

// RemoveGate 网关连接断开，批量清理网关下全部玩家路由，杜绝僵尸数据
func (r *Router) RemoveGate(gc *GateConn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	clog := r.clog
	uidSet, exist := r.gateToUids[gc]
	if !exist {
		clog.Debug("gate has no online users, skip clean", zap.String("gate_addr", gc.gateAddr))
		return
	}

	// 清理所有uid正向索引
	cleanCnt := 0
	for uid := range uidSet {
		delete(r.uidToGate, uid)
		cleanCnt++
	}
	// 删除网关反向索引
	delete(r.gateToUids, gc)

	clog.Info("gate disconnected, batch clean route",
		zap.String("gate_addr", gc.gateAddr),
		zap.Int("clean_user_count", cleanCnt))
}

// ClearAll 全局清空路由，服务优雅关闭调用
func (r *Router) ClearAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	clog := r.clog
	r.uidToGate = make(map[uint64]*GateConn)
	r.gateToUids = make(map[*GateConn]map[uint64]bool)
	clog.Info("router cleared all route data")
}

// CheckConsistency 调试用：校验双向索引一致，排查脏路由
func (r *Router) CheckConsistency() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	clog := r.clog
	valid := true
	for uid, gc := range r.uidToGate {
		if set, exist := r.gateToUids[gc]; !exist || !set[uid] {
			clog.Error("route index inconsistent", zap.Uint64("uid", uid))
			valid = false
		}
	}
	return valid
}

func (r *Router) Close() {
	r.ClearAll()
}
