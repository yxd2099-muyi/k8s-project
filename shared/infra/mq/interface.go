package mq

import "context"

// IMQAdmin Topic管理（创建Topic，可指定队列数量）
type IMQAdmin interface {
	CreateTopic(ctx context.Context, topic string, queueNum int) error
	Close() error
}

// IMQProducer 生产者抽象，支持多Topic/Tag发送
type IMQProducer interface {
	Send(ctx context.Context, topic, tag, key string, body []byte) error
	Close() error
}
type IMQConsumer interface {
	GracefulStop()
	Start() error
	RegisterHandler(topic, tag string, handler Handler)
}
