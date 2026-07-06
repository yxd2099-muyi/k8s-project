package mq

import (
	"context"
	rmq "github.com/apache/rocketmq-clients/golang/v5"
)

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

// Handler 业务处理函数
type Handler func(ctx context.Context, msg *rmq.MessageView) error

// TopicHandler 按 Topic + Tag 注册
type TopicHandler struct {
	Topic   string
	Tag     string // "*" 表示全部
	Handler Handler
}
