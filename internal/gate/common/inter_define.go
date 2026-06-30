package common

// 用户连接接口
type UserConnInter interface {
	BatchPushAsync(uids []uint64, data []byte) error //异步推送
	BatchPushSync(uids []uint64, data []byte)        //同步推送
	BroadcastAll(data []byte)                        // 全量推送
	AllUids() []uint64                               // 获取所有在线用户
}

// 推送接口
type PushInter interface {
	OnUserOnline(uid uint64, address string)
	OnUserOffline(uid uint64, reason string)
}
