package mq

import (
	"context"
	"fmt"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"strings"
	"sync"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// Handler 业务处理函数
type Handler func(ctx context.Context, msg *rmq.MessageView) error

// TopicHandler 按 Topic + Tag 注册
type TopicHandler struct {
	Topic   string
	Tag     string // "*" 表示全部
	Handler Handler
}

// Consumer 封装对象
type Consumer struct {
	config             MQConfig
	consumerGroup      string
	simpleConsumer     rmq.SimpleConsumer
	handlers           map[string]Handler // key: topic:tag
	mu                 sync.RWMutex
	receiveConcurrency int
	maxMessageNum      int32
	invisibleDuration  time.Duration
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	once               sync.Once
	clog               *zap.Logger
}

//type MQConfig struct {
//	Endpoint     string
//	AccessKey    string
//	AccessSecret string
//}

// NewConsumer 创建消费对象
func NewConsumer(consumerGroup string, opts ...ConsumerOption) (*Consumer, error) {
	mqcfg := config.GetConfig().RocketMq
	cfg := MQConfig{}
	cfg.Endpoint = mqcfg.Endpoint
	cfg.AccessKey = mqcfg.AccessKey
	cfg.AccessSecret = mqcfg.AccessSecret
	c := &Consumer{
		config:             cfg,
		consumerGroup:      consumerGroup,
		handlers:           make(map[string]Handler),
		receiveConcurrency: 8,
		maxMessageNum:      16,
		invisibleDuration:  30 * time.Second,
	}

	for _, opt := range opts {
		opt(c)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	c.cancel = cancel
	c.clog = logger.L

	return c, nil
}

type ConsumerOption func(*Consumer)

func WithConcurrency(n int) ConsumerOption {
	return func(c *Consumer) { c.receiveConcurrency = n }
}

func WithMaxMessageNum(n int32) ConsumerOption {
	return func(c *Consumer) { c.maxMessageNum = n }
}

func WithInvisibleDuration(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.invisibleDuration = d }
}

// RegisterHandler 注册 Topic+Tag 处理函数
func (c *Consumer) RegisterHandler(topic, tag string, handler Handler) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.getKey(topic, tag)
	c.handlers[key] = handler
	c.clog.Debug("register handler", zap.String("topic", topic), zap.String("tag", tag), zap.String("key", key))
}

func (c *Consumer) getKey(topic, tag string) string {
	key := topic
	if tag != "" && tag != "*" {
		key = topic + ":" + tag
	}
	return key
}

func (c *Consumer) getTopicByKey(key string) string {
	parts := strings.SplitN(key, ":", 2)
	return parts[0]
}

// Start 启动消费
func (c *Consumer) Start() error {
	var err error
	clog := c.clog
	c.once.Do(func() {
		subExpressions := make(map[string]*rmq.FilterExpression)
		topics := make(map[string]bool)

		c.mu.RLock()
		for key := range c.handlers {
			topic := c.getTopicByKey(key)
			topics[topic] = true
		}
		c.mu.RUnlock()

		if len(topics) == 0 {
			err = fmt.Errorf("no handler registered")
			clog.Error(err.Error())
			return
		}
		// 订阅所有注册的topic，全tag过滤
		for t := range topics {
			subExpressions[t] = rmq.SUB_ALL
		}

		cons, errInner := rmq.NewSimpleConsumer(&rmq.Config{
			Endpoint:      c.config.Endpoint,
			ConsumerGroup: c.consumerGroup,
			Credentials: &credentials.SessionCredentials{
				AccessKey:    c.config.AccessKey,
				AccessSecret: c.config.AccessSecret,
			},
		},
			rmq.WithSimpleAwaitDuration(30*time.Second),
			rmq.WithSimpleSubscriptionExpressions(subExpressions),
		)
		if errInner != nil {
			err = errInner
			clog.Error("create simple consumer failed", zap.Error(err))
			return
		}

		if errInner = cons.Start(); errInner != nil {
			err = errInner
			clog.Error("simple consumer start failed", zap.Error(err))
			return
		}

		c.simpleConsumer = cons

		// 启动多协程消费循环
		for i := 0; i < c.receiveConcurrency; i++ {
			c.wg.Add(1)
			go c.consumeLoop(i)
		}
		clog.Info("consumer start success", zap.Int("concurrency", c.receiveConcurrency))
	})

	return err
}

func (c *Consumer) consumeLoop(id int) {
	clog := c.clog
	defer func() {
		if r := recover(); r != nil {
			clog.Error("consume loop panic recover", zap.Any("panic", r), zap.Stack("stack"))
		}
		c.wg.Done()
		clog.Info("consume loop exit", zap.Int("loopId", id))
	}()

	// 分级休眠：正常空消息短休眠，流断开/异常长休眠
	var sleepDur = 100 * time.Millisecond

	for {
		select {
		case <-c.ctx.Done():
			clog.Info("consume loop receive stop signal, exit", zap.Int("loopId", id))
			return
		default:
			msgs, err := c.simpleConsumer.Receive(c.ctx, c.maxMessageNum, c.invisibleDuration)
			if err != nil {
				// 1. 上下文关闭，直接退出循环
				if c.ctx.Err() != nil {
					return
				}

				errMsg := err.Error()
				switch {
				// 正常长轮询无消息，仅打印debug，短休眠
				case strings.Contains(errMsg, "MESSAGE_NOT_FOUND"):
					sleepDur = 50 * time.Millisecond
					//clog.Debug("receive no new message, wait next poll", zap.Int("loopId", id))
				// gRPC 流被正常断开（空闲超时），info日志，加长休眠
				case strings.Contains(errMsg, "RST_STREAM") || strings.Contains(errMsg, "Canceled"):
					sleepDur = 200 * time.Millisecond
					clog.Info("rpc stream idle closed, re-poll later", zap.Int("loopId", id), zap.Error(err))
				// 真正异常，ERROR日志，加长休眠避免疯狂打印
				default:
					sleepDur = 500 * time.Millisecond
					clog.Error("receive message fatal error", zap.Int("loopId", id), zap.Error(err))
				}

				time.Sleep(sleepDur)
				continue
			}

			// 正常拉取到消息，重置休眠
			sleepDur = 100 * time.Millisecond
			for _, msg := range msgs {
				c.processMessage(msg)
			}
		}
	}
}

// getHandler 精准匹配 topic:tag 处理器，无则匹配 topic 全局处理器
func (c *Consumer) getHandler(topic, tag string) (Handler, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// 优先精准匹配 topic:tag
	keyExact := c.getKey(topic, tag)
	if h, ok := c.handlers[keyExact]; ok {
		return h, true
	}
	// 匹配全局topic处理器 tag=*
	keyAll := topic
	if h, ok := c.handlers[keyAll]; ok {
		return h, true
	}
	return nil, false
}

func (c *Consumer) processMessage(msg *rmq.MessageView) {
	clog := c.clog
	topic := msg.GetTopic()
	tag := *msg.GetTag()
	msgID := msg.GetMessageId()

	handler, exists := c.getHandler(topic, tag)
	if !exists {
		clog.Warn("no matched handler, ack discard message",
			zap.String("topic", topic), zap.String("tag", tag), zap.String("msgId", msgID))
		_ = c.simpleConsumer.Ack(context.Background(), msg)
		return
	}

	// 处理超时时间和消息不可见时长对齐，留2s缓冲
	handleTimeout := c.invisibleDuration - 2*time.Second
	if handleTimeout < 1*time.Second {
		handleTimeout = 1 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), handleTimeout)
	defer cancel()

	if err := handler(ctx, msg); err != nil {
		clog.Error("message handler execute failed, skip ack for retry",
			zap.String("topic", topic), zap.String("tag", tag),
			zap.String("msgId", msgID), zap.Error(err))
		// 不执行Ack，消息超时自动重试
		return
	}

	// 业务处理成功，确认消费
	if err := c.simpleConsumer.Ack(context.Background(), msg); err != nil {
		clog.Error("ack message failed",
			zap.String("topic", topic), zap.String("tag", tag),
			zap.String("msgId", msgID), zap.Error(err))
	} else {
		clog.Debug("message consume success and ack",
			zap.String("topic", topic), zap.String("tag", tag), zap.String("msgId", msgID))
	}
}

// GracefulStop 优雅停止
func (c *Consumer) GracefulStop() {
	c.clog.Info("start graceful stop consumer")
	c.cancel()
	c.wg.Wait()

	if c.simpleConsumer != nil {
		_ = c.simpleConsumer.GracefulStop()
	}
	c.clog.Info("rocketmq consumer gracefully stopped")
}
