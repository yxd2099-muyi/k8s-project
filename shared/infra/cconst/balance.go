package cconst

// LBStrategy 两种负载均衡策略
type LBStrategy string

const (
	LBRoundRobin     LBStrategy = "round_robin"     // 普通轮询（默认）
	LBConsistentHash LBStrategy = "consistent_hash" // 房间一致性hash
	LBTargetDirect   LBStrategy = "target_direct"   // 房间一致性hash

)
