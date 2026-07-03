package mq

// Topic 业务topic，可扩展多业务
const (
	TopicChatPushEvent = "topic_chat_push_event" // 聊天推送主topic
)

// Tag 消息标签，区分业务类型
const (
	TagChatMsg       = "tag_chat_msg"
	TagSystemNotice  = "tag_system_notice"
	TagRoomBroadcast = "tag_room_broadcast"
)

// 消费组
const (
	GroupPushServerChat = "group_push_server_chat"
)

//
//type MQConfig struct {
//	NameServerAddr []string // 127.0.0.1:9876
//	Producer       ProducerConfig
//	Consumer       ConsumerConfig
//}
//
//type ProducerConfig struct {
//	SendTimeoutMs int64 // 发送超时
//}
//
//type ConsumerConfig struct {
//	ConsumeThreadNum int // 单实例消费并发协程
//	MaxRetryTimes    int // 消息最大重试次数
//}
//
//// DefaultMQConfig 默认配置
//func DefaultMQConfig() *MQConfig {
//	return &MQConfig{
//		NameServerAddr: []string{"127.0.0.1:9876"},
//		Producer: ProducerConfig{
//			SendTimeoutMs: 3000,
//		},
//		Consumer: ConsumerConfig{
//			ConsumeThreadNum: 16,
//			MaxRetryTimes:    3,
//		},
//	}
//}
