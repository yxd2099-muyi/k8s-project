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
	config         MQConfig
	consumerGroup  string
	simpleConsumer rmq.SimpleConsumer
	handlers       map[string]Handler // key: topic:tag
	mu             sync.RWMutex

	receiveConcurrency int
	maxMessageNum      int32
	invisibleDuration  time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
	clog   *zap.Logger
}

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
		receiveConcurrency: 8, // 默认并发数
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

// RegisterHandler 注册 Topic 处理函数
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
	// 使用 SplitN 将字符串按 ":" 最多切分成 2 部分
	// 这样可以防止 topic 名字本身包含冒号时导致切分过多
	parts := strings.SplitN(key, ":", 2)
	// 第一部分永远是 topic
	return parts[0]
}

// Start 启动消费
func (c *Consumer) Start() error {
	var err error
	clog := c.clog
	c.once.Do(func() {
		subExpressions := make(map[string]*rmq.FilterExpression)

		c.mu.RLock()
		for key := range c.handlers {
			topic := c.getTopicByKey(key)
			subExpressions[topic] = rmq.SUB_ALL // 支持 Tag 过滤可后续增强
		}
		c.mu.RUnlock()

		if len(subExpressions) == 0 {
			err = fmt.Errorf("no handler registered")
			clog.Error(err.Error())
			return
		}

		// 创建 SimpleConsumer
		cons, errInner := rmq.NewSimpleConsumer(&rmq.Config{
			Endpoint:      c.config.Endpoint,
			ConsumerGroup: c.consumerGroup,
			Credentials: &credentials.SessionCredentials{
				AccessKey:    c.config.AccessKey,
				AccessSecret: c.config.AccessSecret,
			},
		},
			rmq.WithSimpleAwaitDuration(5*time.Second),
			rmq.WithSimpleSubscriptionExpressions(subExpressions),
		)
		if errInner != nil {
			err = errInner
			clog.Error(err.Error())
			return
		}

		if errInner = cons.Start(); errInner != nil {
			err = errInner
			clog.Error(err.Error())
			return
		}

		c.simpleConsumer = cons

		// 启动多个消费 goroutine
		for i := 0; i < c.receiveConcurrency; i++ {
			c.wg.Add(1)
			go c.consumeLoop(i)
		}
	})

	return err
}

func (c *Consumer) consumeLoop(id int) {
	clog := c.clog
	defer func() {
		if r := recover(); r != nil {
			clog.Error("panic", zap.Any("recover", r), zap.Stack("stack"))
		}
		c.wg.Done()
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			msgs, err := c.simpleConsumer.Receive(c.ctx, c.maxMessageNum, c.invisibleDuration)
			if err != nil {
				if c.ctx.Err() != nil {
					return
				}
				clog.Error("receive error", zap.Any("id", id), zap.Any("error", err))
				time.Sleep(100 * time.Millisecond)
				continue
			}

			for _, msg := range msgs {
				c.processMessage(msg)
			}
		}
	}
}
func (c *Consumer) getHandler(handlerKey string) (Handler, bool) {
	c.mu.RLock()
	// 可扩展支持 Tag 精准匹配: handlerKey = msg.Topic + ":" + msg.GetTag()
	handler, exists := c.handlers[handlerKey]
	c.mu.RUnlock()
	return handler, exists
}
func (c *Consumer) processMessage(msg *rmq.MessageView) {
	clog := c.clog
	clog.Debug("process message", zap.Any("msg", msg))
	handlerKey := msg.GetTopic()
	handler, exists := c.getHandler(handlerKey)
	if !exists {
		clog.Warn("no handler for topic", zap.String("topic", handlerKey))
		_ = c.simpleConsumer.Ack(context.Background(), msg)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	if err := handler(ctx, msg); err != nil {
		clog.Error(fmt.Sprintf("Handler error, topic=%s, msgID=%s, err=%v", msg.GetTopic(), msg.GetMessageId(), err))
		// 不 Ack，让 RocketMQ 重试
		return
	}

	// 成功处理后 Ack
	if err := c.simpleConsumer.Ack(context.Background(), msg); err != nil {
		clog.Error(fmt.Sprintf("Ack failed, topic=%s, msgID=%s", msg.GetTopic(), msg.GetMessageId()))
	}
}

// GracefulStop 优雅停止
func (c *Consumer) GracefulStop() {
	c.cancel()
	c.wg.Wait()

	if c.simpleConsumer != nil {
		_ = c.simpleConsumer.GracefulStop()
	}
	c.clog.Info("rocketmq consumer gracefully stopped")
}
