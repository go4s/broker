// Package broker 内存消息 Broker,功能模型类似 MQTT:
// 通配符订阅、QoS 0/1、保留消息、遗嘱消息。
package broker

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Message 一条待发布的主题消息。
type Message struct {
	Topic   string `json:"topic"`   // 发布主题,不允许通配符
	Payload string `json:"payload"` // 消息体
	QoS     int    `json:"qos"`     // 0 或 1;1 要求订阅方 HTTP ACK,超时重投
	// Retain 为 true 时作为保留消息存储;payload 为空则清除该主题的保留消息
	Retain bool `json:"retain"`
}

// Event 推送到 SSE 流的一条消息事件。
type Event struct {
	MessageID string `json:"message_id"` // Broker 级单调递增 ID,QoS1 ACK 时引用
	Topic     string `json:"topic"`
	Payload   string `json:"payload"`
	QoS       int    `json:"qos"`    // 实际投递 QoS:发布与订阅取小
	Retain    bool   `json:"retain"` // true 表示这是一条保留消息补发
}

// 各配置项的 Broker 级默认值,可被 With* 选项覆盖。
const (
	defaultStreamBuffer      = 1
	defaultRedeliverInterval = 5 * time.Second
	defaultWillGracePeriod   = 30 * time.Second
	defaultHeartbeatInterval = 10 * time.Minute
	defaultMaxInflight       = 16
	defaultSessionHeartbeat  = 3 * time.Minute // 创建会话未指定心跳间隔时采用
)

// SessionInfo 会话快照,用于查询接口。
type SessionInfo struct {
	ID                string         `json:"id"`
	ClientID          string         `json:"client_id"`
	CleanStart        bool           `json:"clean_start"`
	Online            bool           `json:"online"`
	CreatedAt         time.Time      `json:"created_at"`
	WillGracePeriod   string         `json:"will_grace_period"`  // 生效的遗嘱宽限期,如 "30s"
	HeartbeatInterval string         `json:"heartbeat_interval"` // 生效的 SSE 心跳间隔,如 "3m0s"
	MaxInflight       int            `json:"max_inflight"`       // 会话级 QoS1 in-flight 窗口
	Subscriptions     []Subscription `json:"subscriptions"`
}

// Broker 内存消息 Broker,功能模型类似 MQTT。
type Broker struct {
	mu       sync.Mutex
	sessions map[string]*Session
	byClient map[string]string // clientID -> sessionID
	retained map[string]Message
	routes   *routeNode    // 订阅路由树,随订阅增删与会话销毁维护
	msgSeq   atomic.Uint64 // 全 Broker 单调递增的消息 ID 源

	streamBuffer      int
	redeliverInterval time.Duration
	willGracePeriod   time.Duration
	heartbeatInterval time.Duration
	maxSessions       int // 会话数上限,0 表示不限制
	maxInflight       int // 每会话 QoS1 in-flight 窗口上限,满则拒绝新投递并记录日志

	stop   chan struct{}
	stopWg sync.WaitGroup
	closed atomic.Bool
}

// New 创建 Broker 并启动 QoS1 重投循环。
func New(opts ...Option) *Broker {
	b := &Broker{
		sessions:          make(map[string]*Session),
		byClient:          make(map[string]string),
		retained:          make(map[string]Message),
		routes:            newRouteNode(),
		streamBuffer:      defaultStreamBuffer,
		redeliverInterval: defaultRedeliverInterval,
		willGracePeriod:   defaultWillGracePeriod,
		heartbeatInterval: defaultHeartbeatInterval,
		maxInflight:       defaultMaxInflight,
		stop:              make(chan struct{}),
	}
	for _, opt := range opts {
		opt(b)
	}
	b.stopWg.Add(1)
	go b.redeliverLoop()
	return b
}

// Close 停止重投循环并销毁所有会话(不触发遗嘱)。幂等,可重复调用。
func (b *Broker) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	close(b.stop)
	b.stopWg.Wait()
	b.mu.Lock()
	defer b.mu.Unlock()
	for id := range b.sessions {
		b.destroyLocked(id)
	}
}

// nextMessageID 分配一个全 Broker 单调递增的消息 ID(十进制字符串)。
func (b *Broker) nextMessageID() string {
	return fmt.Sprint(b.msgSeq.Add(1))
}

// snapshot 生成会话的只读快照。调用方需持锁。
func (b *Broker) snapshot(s *Session) SessionInfo {
	return SessionInfo{
		ID:                s.ID,
		ClientID:          s.ClientID,
		CleanStart:        s.CleanStart,
		Online:            s.Online,
		CreatedAt:         s.CreatedAt,
		WillGracePeriod:   b.gracePeriodOf(s).String(),
		HeartbeatInterval: b.heartbeatIntervalOf(s).String(),
		MaxInflight:       s.maxInflight,
		Subscriptions:     s.Subscriptions(),
	}
}

func randomID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf[:])
}
