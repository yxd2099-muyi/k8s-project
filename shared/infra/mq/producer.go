package mq

import (
	"context"
	"fmt"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

type MQConfig struct {
	Endpoint      string `validate:"required"`
	NameSpace     string
	AccessKey     string `json:"accessKey" validate:"required"`
	AccessSecret  string `json:"accessSecret" validate:"required"`
	SecurityToken string `json:"securityToken"`
}

// Producer 全局单例生产者
type Producer struct {
	client rmq.Producer
	cfg    *MQConfig
	once   sync.Once
	clog   *zap.Logger
	closed int32 // 0运行 1关闭
}

var globalProducer = &Producer{}

// InitProducer 获取全局生产者（单例并发安全）
func InitProducer() (*Producer, error) {
	gcfg := config.GetConfig().RocketMq
	cfg := &rmq.Config{}
	cfg.Endpoint = gcfg.Endpoint
	cfg.NameSpace = gcfg.Namespace
	cfg.Credentials = &credentials.SessionCredentials{
		AccessKey:    gcfg.AccessKey,
		AccessSecret: gcfg.AccessSecret,
	}
	opts := []rmq.ProducerOption{
		rmq.WithTopics(cconst.TopicPushEvents),
	}
	var err error
	var producer rmq.Producer
	clog := logger.L
	globalProducer.clog = clog
	globalProducer.once.Do(func() {
		producer, err = rmq.NewProducer(cfg, opts...)
		if err != nil {
			clog.Error("fail to create producer", zap.Error(err))
			return
		}
		err = producer.Start()
		if err != nil {
			clog.Error("fail to start producer", zap.Error(err))
			return
		}
		globalProducer.client = producer

	})
	if err != nil {
		clog.Error("fail to init producer", zap.Error(err))
		return nil, err
	}
	return globalProducer, nil
}

// SendMsg 发送protobuf消息到指定topic+tag
// key 相当于索引， 要使用业务中唯一字符串， 这里可以是 eventId
// messageGroup 消息分组， 同样的分组肯定在一个队列中， 能保证一个分组中的顺序性，在公会聊天中可以以公会Id 为其值
// tag 标签 表明消息属于什么类型， 比如 chat. email
// topic tag 中起名不能带 冒号 : 因为后面消费的时候会用到
func (p *Producer) SendMsg(ctx context.Context, topic, tag, key, messageGroup string, body []byte) error {
	if atomic.LoadInt32(&p.closed) == 1 {
		return fmt.Errorf("producer is closed")
	}
	msg := &rmq.Message{
		Topic: topic,
		Body:  body,
	}
	msg.SetTag(tag)
	msg.SetKeys(key)
	if messageGroup != "" {
		msg.SetMessageGroup(messageGroup) // 这行主动会设置分组顺序
	}

	res, err := p.client.Send(ctx, msg)
	if err != nil {
		p.clog.Error("fail to send message", zap.Error(err))
		return err
	}
	p.clog.Debug("send message", zap.Any("res", res))
	return nil
}

// Close 优雅关闭生产者
func (p *Producer) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}
	if p.client == nil {
		return nil
	}
	err := p.client.GracefulStop()
	if err != nil {
		p.clog.Error("fail to close producer", zap.Error(err))
	}
	p.clog.Info("mq producer graceful stop complete")
	return err
}

// 发送普通消息
func SendNormal(ctx context.Context, topic, tag, key string, body []byte) error {
	err := globalProducer.SendMsg(ctx, topic, tag, key, "", body)
	if err != nil {
		return err
	}
	return nil
}

// 发送顺序消息
func SendFIFO(ctx context.Context, topic, tag, key, messageGroup string, body []byte) error {
	err := globalProducer.SendMsg(ctx, topic, tag, key, messageGroup, body)
	if err != nil {
		return err
	}
	return nil
}
