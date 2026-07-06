package mq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// Handler 业务处理函数
//type Handler func(ctx context.Context, msg *rmq.MessageView) error
//
//// TopicHandler 按 Topic + Tag 注册
//type TopicHandler struct {
//	Topic   string
//	Tag     string // "*" 表示全部
//	Handler Handler
//}

// PushConsumer 封装对象
type PushConsumer struct {
	config                 MQConfig
	consumerGroup          string
	pushConsumer           rmq.PushConsumer
	handlers               map[string]Handler // key: topic:tag
	mu                     sync.RWMutex
	consumptionThreadCount int32
	maxCacheMessageCount   int32
	awaitDuration          time.Duration
	ctx                    context.Context
	cancel                 context.CancelFunc
	once                   sync.Once
	clog                   *zap.Logger
}

// NewPushConsumer 创建 PushConsumer
func NewPushConsumer(consumerGroup string, opts ...PushConsumerOption) (*PushConsumer, error) {
	mqcfg := config.GetConfig().RocketMq

	c := &PushConsumer{
		config: MQConfig{
			Endpoint:     mqcfg.Endpoint,
			AccessKey:    mqcfg.AccessKey,
			AccessSecret: mqcfg.AccessSecret,
			NameSpace:    mqcfg.Namespace, // 如果你的配置中有
		},
		consumerGroup:          consumerGroup,
		handlers:               make(map[string]Handler),
		consumptionThreadCount: 20, // 默认值
		maxCacheMessageCount:   1024,
		awaitDuration:          5 * time.Second,
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

type PushConsumerOption func(*PushConsumer)

func WithPushConsumptionThreadCount(n int32) PushConsumerOption {
	return func(c *PushConsumer) { c.consumptionThreadCount = n }
}

func WithPushMaxCacheMessageCount(n int32) PushConsumerOption {
	return func(c *PushConsumer) { c.maxCacheMessageCount = n }
}

func WithPushAwaitDuration(d time.Duration) PushConsumerOption {
	return func(c *PushConsumer) { c.awaitDuration = d }
}

// RegisterHandler 注册 Topic+Tag 处理函数
func (c *PushConsumer) RegisterHandler(topic, tag string, handler Handler) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.getKey(topic, tag)
	c.handlers[key] = handler
	c.clog.Debug("register handler", zap.String("topic", topic), zap.String("tag", tag), zap.String("key", key))
}

func (c *PushConsumer) getKey(topic, tag string) string {
	if tag != "" && tag != "*" {
		return topic + ":" + tag
	}
	return topic
}

func (c *PushConsumer) getTopicByKey(key string) string {
	parts := strings.SplitN(key, ":", 2)
	return parts[0]
}

// Start 启动消费
func (c *PushConsumer) Start() error {
	var err error
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
			c.clog.Error(err.Error())
			return
		}

		// 订阅所有注册的 Topic
		for t := range topics {
			subExpressions[t] = rmq.SUB_ALL
		}

		// 创建 PushConsumer
		cons, errInner := rmq.NewPushConsumer(&rmq.Config{
			Endpoint:      c.config.Endpoint,
			ConsumerGroup: c.consumerGroup,
			Credentials: &credentials.SessionCredentials{
				AccessKey:    c.config.AccessKey,
				AccessSecret: c.config.AccessSecret,
			},
			NameSpace: c.config.NameSpace,
		},
			rmq.WithPushAwaitDuration(c.awaitDuration),
			rmq.WithPushSubscriptionExpressions(subExpressions),
			rmq.WithPushMessageListener(&rmq.FuncMessageListener{
				Consume: c.messageListener, // 使用封装的 listener
			}),
			rmq.WithPushConsumptionThreadCount(c.consumptionThreadCount),
			rmq.WithPushMaxCacheMessageCount(c.maxCacheMessageCount),
		)
		if errInner != nil {
			err = errInner
			c.clog.Error("create push consumer failed", zap.Error(err))
			return
		}

		if errInner = cons.Start(); errInner != nil {
			err = errInner
			c.clog.Error("push consumer start failed", zap.Error(err))
			return
		}

		c.pushConsumer = cons
		c.clog.Info("PushConsumer started successfully",
			zap.String("consumerGroup", c.consumerGroup),
			zap.Int32("threadCount", c.consumptionThreadCount))
	})

	return err
}

// messageListener 统一消息监听器
func (c *PushConsumer) messageListener(msg *rmq.MessageView) rmq.ConsumerResult {
	topic := msg.GetTopic()
	tag := ""
	if msg.GetTag() != nil {
		tag = *msg.GetTag()
	}
	msgID := msg.GetMessageId()

	handler, exists := c.getHandler(topic, tag)
	if !exists {
		c.clog.Warn("no matched handler, discard message",
			zap.String("topic", topic),
			zap.String("tag", tag),
			zap.String("msgId", msgID))
		return rmq.SUCCESS // 直接成功，丢弃
	}

	// 处理超时（建议比 invisibleDuration 短一些）
	handleTimeout := 25 * time.Second // 可根据业务调整
	ctx, cancel := context.WithTimeout(context.Background(), handleTimeout)
	defer cancel()

	if err := handler(ctx, msg); err != nil {
		c.clog.Error("message handler failed, will retry",
			zap.String("topic", topic),
			zap.String("tag", tag),
			zap.String("msgId", msgID),
			zap.Error(err))
		return rmq.FAILURE // 失败 → 重试
	}

	return rmq.SUCCESS // 成功
}

func (c *PushConsumer) getHandler(topic, tag string) (Handler, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 优先精准匹配 topic:tag
	if h, ok := c.handlers[c.getKey(topic, tag)]; ok {
		return h, true
	}
	// 降级匹配 topic 全局处理器
	if h, ok := c.handlers[topic]; ok {
		return h, true
	}
	return nil, false
}

// GracefulStop 优雅停止
func (c *PushConsumer) GracefulStop() {
	c.clog.Info("starting graceful stop PushConsumer", zap.String("consumerGroup", c.consumerGroup))
	if c.pushConsumer != nil {
		_ = c.pushConsumer.GracefulStop()
	}
	c.cancel()
	c.clog.Info("PushConsumer gracefully stopped")
}
