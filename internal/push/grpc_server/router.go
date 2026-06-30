package grpc_server

import (
	"sync"
)

type Router struct {
	mu         sync.RWMutex
	uidToGate  map[uint64]*GateConn          // uid → gate连接
	gateToUids map[*GateConn]map[uint64]bool // gate → 其上的uids
}

func NewRouter() *Router {
	return &Router{
		uidToGate:  make(map[uint64]*GateConn),
		gateToUids: make(map[*GateConn]map[uint64]bool),
	}
}

func (r *Router) Add(uid uint64, gc *GateConn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// 如果 uid 已存在，先移除旧的
	if old, ok := r.uidToGate[uid]; ok && old != gc {
		delete(r.gateToUids[old], uid)
		if len(r.gateToUids[old]) == 0 {
			delete(r.gateToUids, old)
		}
	}
	r.uidToGate[uid] = gc
	if _, ok := r.gateToUids[gc]; !ok {
		r.gateToUids[gc] = make(map[uint64]bool)
	}
	r.gateToUids[gc][uid] = true
}

func (r *Router) Remove(uid uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if gc, ok := r.uidToGate[uid]; ok {
		delete(r.uidToGate, uid)
		if uids, ok := r.gateToUids[gc]; ok {
			delete(uids, uid)
			if len(uids) == 0 {
				delete(r.gateToUids, gc)
			}
		}
	}
}

func (r *Router) Get(uid uint64) *GateConn {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.uidToGate[uid]
}

// RemoveGate 当 gate 断开时清理所有关联 uid
func (r *Router) RemoveGate(gc *GateConn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if uids, ok := r.gateToUids[gc]; ok {
		for uid := range uids {
			delete(r.uidToGate, uid)
		}
		delete(r.gateToUids, gc)
	}
}

func (r *Router) Close() {
	// 无额外操作
}
