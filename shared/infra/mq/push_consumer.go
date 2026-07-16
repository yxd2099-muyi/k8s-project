package mq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// ConsumerState 消费者状态枚举
type ConsumerState int32

const (
	StateUnStarted ConsumerState = iota
	StateRunning
	StateStopped
)

// PushConsumer 封装对象
type PushConsumer struct {
	config                 MQConfig
	consumerGroup          string
	pushConsumer           rmq.PushConsumer
	handlers               map[string]Handler
	mu                     sync.RWMutex
	consumptionThreadCount int32
	maxCacheMessageCount   int32
	awaitDuration          time.Duration
	handleTimeout          time.Duration
	stopWaitTimeout        time.Duration
	preStopWait            time.Duration // 停止前前置等待时间
	ctx                    context.Context
	cancel                 context.CancelFunc
	once                   sync.Once
	clog                   *zap.Logger
	state                  atomic.Int32
}

// NewPushConsumer 创建 PushConsumer
func NewPushConsumer(consumerGroup string, mqcfg config.RocketMq, opts ...PushConsumerOption) (IMQConsumer, error) {

	c := &PushConsumer{
		config: MQConfig{
			Endpoint:     mqcfg.Endpoint,
			AccessKey:    mqcfg.AccessKey,
			AccessSecret: mqcfg.AccessSecret,
			NameSpace:    mqcfg.Namespace,
		},
		consumerGroup:          consumerGroup,
		handlers:               make(map[string]Handler),
		consumptionThreadCount: 20,
		maxCacheMessageCount:   1024,
		awaitDuration:          5 * time.Second,
		handleTimeout:          20 * time.Second,
		stopWaitTimeout:        30 * time.Second,
		preStopWait:            1 * time.Second, // 前置等待拉长至1s
		clog:                   logger.L,
	}
	c.state.Store(int32(StateUnStarted))

	for _, opt := range opts {
		opt(c)
	}

	// 强制保证关闭等待时长 > 业务处理超时，预留缓冲5s
	minStopWait := c.handleTimeout + 5*time.Second
	if c.stopWaitTimeout <= minStopWait {
		c.stopWaitTimeout = minStopWait
		c.clog.Info("auto adjust stopWaitTimeout larger than handleTimeout", zap.Duration("newStopWait", c.stopWaitTimeout), zap.Duration("handleTimeout", c.handleTimeout))
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	c.cancel = cancel

	return c, nil
}

// PushConsumerOption 选项函数
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

func WithPushHandleTimeout(d time.Duration) PushConsumerOption {
	return func(c *PushConsumer) { c.handleTimeout = d }
}

func WithPushStopWaitTimeout(d time.Duration) PushConsumerOption {
	return func(c *PushConsumer) { c.stopWaitTimeout = d }
}

// WithPushPreStopWait 自定义停止前置等待时间
func WithPushPreStopWait(d time.Duration) PushConsumerOption {
	return func(c *PushConsumer) { c.preStopWait = d }
}

// RegisterHandler 注册 Topic+Tag 处理函数（启动后禁止注册）
func (c *PushConsumer) RegisterHandler(topic, tag string, handler Handler) {
	if c.state.Load() == int32(StateRunning) {
		c.clog.Warn("consumer already running, cannot register new handler",
			zap.String("topic", topic), zap.String("tag", tag))
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.getKey(topic, tag)
	c.handlers[key] = handler
	c.clog.Debug("register handler success",
		zap.String("consumerGroup", c.consumerGroup),
		zap.String("topic", topic),
		zap.String("tag", tag),
		zap.String("key", key))
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

// Start 启动消费，幂等控制
func (c *PushConsumer) Start() error {
	state := c.state.Load()
	switch ConsumerState(state) {
	case StateRunning:
		return fmt.Errorf("consumer %s already running", c.consumerGroup)
	case StateStopped:
		return fmt.Errorf("consumer %s already stopped, cannot restart", c.consumerGroup)
	}

	var startErr error
	c.once.Do(func() {
		subExpressions := make(map[string]*rmq.FilterExpression)
		topicTagMap := make(map[string]map[string]struct{})

		c.mu.RLock()
		for key := range c.handlers {
			topic := c.getTopicByKey(key)
			if _, ok := topicTagMap[topic]; !ok {
				topicTagMap[topic] = make(map[string]struct{})
			}
			_, tagPart, hasTag := strings.Cut(key, ":")
			if hasTag && tagPart != "" {
				topicTagMap[topic][tagPart] = struct{}{}
			}
		}
		c.mu.RUnlock()

		if len(topicTagMap) == 0 {
			startErr = fmt.Errorf("no handler registered for consumer group %s", c.consumerGroup)
			c.clog.Error(startErr.Error())
			return
		}

		// 精准订阅tag，减少无效消息下发，降低关闭压力
		for topic, tagSet := range topicTagMap {
			if len(tagSet) == 0 {
				subExpressions[topic] = rmq.SUB_ALL
				continue
			}
			var tags []string
			for t := range tagSet {
				tags = append(tags, t)
			}
			subExpressions[topic] = rmq.NewFilterExpression(strings.Join(tags, "||"))
		}

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
				Consume: c.messageListener,
			}),
			rmq.WithPushConsumptionThreadCount(c.consumptionThreadCount),
			rmq.WithPushMaxCacheMessageCount(c.maxCacheMessageCount),
		)
		if errInner != nil {
			startErr = fmt.Errorf("create push consumer failed: %w", errInner)
			c.clog.Error("create push consumer failed", zap.Error(startErr))
			return
		}

		if errInner = cons.Start(); errInner != nil {
			startErr = fmt.Errorf("push consumer start failed: %w", errInner)
			c.clog.Error("push consumer start failed", zap.Error(startErr))
			return
		}

		c.pushConsumer = cons
		c.state.Store(int32(StateRunning))
		c.clog.Info("PushConsumer started successfully",
			zap.String("consumerGroup", c.consumerGroup),
			zap.Int32("threadCount", c.consumptionThreadCount),
			zap.Duration("handleTimeout", c.handleTimeout),
			zap.Duration("stopWaitTimeout", c.stopWaitTimeout),
			zap.Duration("preStopWait", c.preStopWait))
	})

	return startErr
}

// messageListener 统一消息监听器
func (c *PushConsumer) messageListener(msg *rmq.MessageView) rmq.ConsumerResult {
	// 停止信号触发后，新消息直接丢弃ack，不进入业务
	if c.ctx.Err() != nil {
		c.clog.Warn("consumer stopping, skip handle, ack success",
			zap.String("topic", msg.GetTopic()),
			zap.String("msgId", msg.GetMessageId()))
		return rmq.SUCCESS
	}

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
		return rmq.SUCCESS
	}

	handleCtx, handleCancel := context.WithTimeout(c.ctx, c.handleTimeout)
	defer handleCancel()

	err := handler(handleCtx, msg)
	if err != nil {
		if c.ctx.Err() != nil {
			c.clog.Warn("consumer stopping, interrupt handler, ack success",
				zap.String("topic", topic),
				zap.String("msgId", msgID),
				zap.Error(err))
			return rmq.SUCCESS
		}
		c.clog.Error("message handler execute failed, will retry",
			zap.String("topic", topic),
			zap.String("tag", tag),
			zap.String("msgId", msgID),
			zap.Error(err))
		return rmq.FAILURE
	}

	return rmq.SUCCESS
}

func (c *PushConsumer) getHandler(topic, tag string) (Handler, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if h, ok := c.handlers[c.getKey(topic, tag)]; ok {
		return h, true
	}
	if h, ok := c.handlers[topic]; ok {
		return h, true
	}
	return nil, false
}

// GracefulStop 优雅停止，加长前置等待、超时告警增强
func (c *PushConsumer) GracefulStop() {
	if !c.state.CompareAndSwap(int32(StateRunning), int32(StateStopped)) {
		c.clog.Warn("PushConsumer skip stop, not running",
			zap.String("consumerGroup", c.consumerGroup),
			zap.Int32("state", c.state.Load()))
		return
	}

	c.clog.Info("start graceful stop PushConsumer",
		zap.String("consumerGroup", c.consumerGroup),
		zap.Duration("stopWaitTimeout", c.stopWaitTimeout),
		zap.Duration("handleTimeout", c.handleTimeout),
		zap.Duration("preStopWait", c.preStopWait))

	// 1. 下发全局取消信号
	c.cancel()
	// 加长前置等待，给IO操作充足时间感知ctx取消
	time.Sleep(c.preStopWait)

	// 2. 异步关闭SDK consumer
	stopCh := make(chan struct{}, 1)
	go func() {
		defer close(stopCh)
		if c.pushConsumer == nil {
			return
		}
		c.clog.Info("waiting SDK consumer to drain all running handlers...")
		_ = c.pushConsumer.GracefulStop()
		c.clog.Info("underlying rocketmq consumer closed complete")
	}()

	// 3. 等待关闭完成
	select {
	case <-stopCh:
		c.clog.Info("PushConsumer graceful stop success, all handlers finished")
	case <-time.After(c.stopWaitTimeout):
		c.clog.Error("PushConsumer graceful stop TIMEOUT WARNING",
			zap.String("consumerGroup", c.consumerGroup),
			zap.String("rootCause", "business handler not listening ctx.Done()"),
			zap.String("suggestion1", "all db/redis/http must use WithContext(ctx)"),
			zap.String("suggestion2", "loop logic add select {case <-ctx.Done(): return}"),
			zap.String("suggestion3", "increase stopWaitTimeout via WithPushStopWaitTimeout"),
			zap.Duration("timeout", c.stopWaitTimeout))
	}

	c.clog.Info("PushConsumer graceful stop finished", zap.String("consumerGroup", c.consumerGroup))
}
