package balancerx

import (
	"fmt"
	"github.com/k8s/muyi/shared/infra/cconst"
	"hash/fnv"
	"math/rand"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/metadata"
)

// 常量定义
const (
	defaultReplicas = 150
)

func init() {
	//balancer.Register(newConsistentHashBuilder())
}

// HashRing 一致性哈希环，内置读写锁 + uint32二分查找
type HashRing struct {
	mu       sync.RWMutex
	hashes   []uint32          // 有序哈希数组
	nodeMap  map[uint32]string // hash -> 节点地址
	replicas int               // 虚拟节点数量
	hashFunc func([]byte) uint32
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		nodeMap:  make(map[uint32]string),
		replicas: replicas,
		hashFunc: func(b []byte) uint32 {
			h := fnv.New32a()
			h.Write(b)
			return h.Sum32()
		},
	}
}

// AddNode 添加节点并生成虚拟节点
func (r *HashRing) AddNode(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.replicas; i++ {
		h := r.hashFunc([]byte(fmt.Sprintf("%s#%d", addr, i)))
		r.nodeMap[h] = addr
	}
	r.rebuildSortedHashes()
}

// RemoveNode 移除节点及所有虚拟节点
func (r *HashRing) RemoveNode(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.replicas; i++ {
		h := r.hashFunc([]byte(fmt.Sprintf("%s#%d", addr, i)))
		delete(r.nodeMap, h)
	}
	r.rebuildSortedHashes()
}

func (r *HashRing) rebuildSortedHashes() {
	hs := make([]uint32, 0, len(r.nodeMap))
	for h := range r.nodeMap {
		hs = append(hs, h)
	}
	sort.Slice(hs, func(i, j int) bool {
		return hs[i] < hs[j]
	})
	r.hashes = hs
}

// GetNode 二分查找，返回顺时针最多3个候选节点（自动去重）
func (r *HashRing) GetNode(key string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.hashes) == 0 {
		return nil
	}

	hash := r.hashFunc([]byte(key))
	idx := sort.Search(len(r.hashes), func(i int) bool {
		return r.hashes[i] >= hash
	})
	if idx >= len(r.hashes) {
		idx = 0
	}

	// 候选节点自动去重
	set := make(map[string]struct{})
	candidates := make([]string, 0, 3)
	for i := 0; i < len(r.hashes) && len(candidates) < 3; i++ {
		pos := (idx + i) % len(r.hashes)
		addr := r.nodeMap[r.hashes[pos]]
		if _, exist := set[addr]; !exist {
			set[addr] = struct{}{}
			candidates = append(candidates, addr)
		}
	}
	return candidates
}

// consistentHashPicker 选择器
type consistentHashPicker struct {
	mu       sync.RWMutex
	ring     *HashRing
	subConns map[string]balancer.SubConn
	rnd      *rand.Rand
}

// consistentHashPickerBuilder 构建器
type consistentHashPickerBuilder struct {
	replicas int
}

func newConsistentHashBuilder() balancer.Builder {
	return base.NewBalancerBuilder(
		string(cconst.LBConsistentHash),
		&consistentHashPickerBuilder{replicas: defaultReplicas},
		base.Config{HealthCheck: true},
	)
}

func (b *consistentHashPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	subConns := make(map[string]balancer.SubConn, len(info.ReadySCs))
	ring := NewHashRing(b.replicas)

	for sc, scInfo := range info.ReadySCs {
		addr := scInfo.Address.Addr
		subConns[addr] = sc
		ring.AddNode(addr)
	}

	return &consistentHashPicker{
		ring:     ring,
		subConns: subConns,
		rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Pick 核心逻辑：从metadata读取roomId，有值则哈希分片，无值则随机负载均衡
func (p *consistentHashPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	// 第一步：先读取metadata，无锁操作，提前拿到roomId（缩小锁范围）
	var roomID string
	md, ok := metadata.FromOutgoingContext(info.Ctx)
	if ok {
		arr := md.Get(cconst.ContextFieldRouterXRoomId)
		if len(arr) > 0 {
			roomID = arr[0]
		}
	}

	// 进入临界区
	p.mu.RLock()
	defer p.mu.RUnlock()

	// 场景1：没有roomId，降级随机选择连接
	if roomID == "" {
		scList := make([]balancer.SubConn, 0, len(p.subConns))
		for _, sc := range p.subConns {
			scList = append(scList, sc)
		}
		if len(scList) == 0 {
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
		}
		selected := scList[p.rnd.Intn(len(scList))]
		return balancer.PickResult{SubConn: selected}, nil
	}

	// 场景2：携带roomId，执行一致性哈希分片
	candidates := p.ring.GetNode(roomID)
	for _, addr := range candidates {
		if sc, ok := p.subConns[addr]; ok {
			return balancer.PickResult{SubConn: sc}, nil
		}
	}

	return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
}
