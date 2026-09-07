package broker

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
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
	Retain  bool   `json:"retain"`  // true 时作为保留消息存储;payload 为空则清除该主题的保留消息
}

// Event 推送到 SSE 流的一条消息事件。
type Event struct {
	MessageID string `json:"message_id"` // Broker 级单调递增 ID,QoS1 ACK 时引用
	Topic     string `json:"topic"`
	Payload   string `json:"payload"`
	QoS       int    `json:"qos"`    // 实际投递 QoS:发布与订阅取小
	Retain    bool   `json:"retain"` // true 表示这是一条保留消息补发
}

// SessionInfo 会话快照,用于查询接口。
// defaultSessionHeartbeat 会话级心跳缺省值:创建会话未指定心跳间隔时采用。
const defaultSessionHeartbeat = 3 * time.Minute

type SessionInfo struct {
	ID                string         `json:"id"`
	ClientID          string         `json:"client_id"`
	CleanStart        bool           `json:"clean_start"`
	Online            bool           `json:"online"`
	CreatedAt         time.Time      `json:"created_at"`
	WillGracePeriod   string         `json:"will_grace_period"`  // 生效的遗嘱宽限期,如 "30s"
	HeartbeatInterval string         `json:"heartbeat_interval"` // 生效的 SSE 心跳间隔,如 "3m0s"
	Subscriptions     []Subscription `json:"subscriptions"`
}

var (
	ErrSessionNotFound = errors.New("session not found")
	ErrStreamNotOpen   = errors.New("stream not open")
	ErrTooManySessions = errors.New("too many sessions")
)

type ReviewMarkShouldBeRemovedByAI struct{}

// Broker 内存消息 Broker,功能模型类似 MQTT:
// 通配符订阅、QoS 0/1、保留消息、遗嘱消息。
type Broker struct {
	mu       sync.Mutex
	sessions map[string]*Session
	byClient map[string]string // clientID -> sessionID
	retained map[string]Message
	routes   *routeNode                    // 订阅路由树,随订阅增删与会话销毁维护
	seq      atomic.Uint64                 // Human : why seq placed here? \
	_        ReviewMarkShouldBeRemovedByAI // service level publish count should not be used as per-session message id

	streamBuffer      int
	redeliverInterval time.Duration
	willGracePeriod   time.Duration
	heartbeatInterval time.Duration
	maxSessions       int // 会话数上限,0 表示不限制

	// authorizer 订阅/发布授权器;nil 表示不启用 ACL(向后兼容,默认放行)。
	authorizer Authorizer

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
		streamBuffer:      1,
		redeliverInterval: 5 * time.Second,
		willGracePeriod:   30 * time.Second,
		heartbeatInterval: 10 * time.Minute,
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

// CreateSession 创建会话。cleanStart=true 时同 clientID 的旧会话被清空重建;
// cleanStart=false 且旧会话存在时复用其订阅表(resumed=true)。
// willGracePeriod 为会话级遗嘱宽限期,<=0 表示未定义(采用 Broker 预定义设置)。
// heartbeatInterval 为会话级 SSE 心跳间隔,>0 记入会话,<=0 时新会话取会话级缺省 3min。
// 恢复既有会话时,两者仅在 >0 时覆盖原设置。
// 新建会话数达到上限(WithMaxSessions)时返回 ErrTooManySessions;恢复既有会话不受限。
func (b *Broker) CreateSession(clientID string, cleanStart bool, will *Will, willGracePeriod, heartbeatInterval time.Duration) (info SessionInfo, resumed bool, err error) {
	if will != nil {
		if err := ValidateTopic(will.Topic); err != nil {
			return SessionInfo{}, false, fmt.Errorf("invalid will topic: %w", err)
		}
		if will.QoS < 0 || will.QoS > 1 {
			return SessionInfo{}, false, fmt.Errorf("invalid will qos %d", will.QoS)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if clientID != "" {
		if oldID, ok := b.byClient[clientID]; ok {
			if !cleanStart {
				old := b.sessions[oldID]
				old.will = will
				if willGracePeriod > 0 {
					old.willGracePeriod = willGracePeriod
				}
				if heartbeatInterval > 0 {
					old.heartbeatInterval = heartbeatInterval
				}
				if old.willTimer != nil {
					old.willTimer.Stop()
					old.willTimer = nil
				}
				b.closeStreamLocked(old)
				return b.snapshot(old), true, nil
			}
			b.destroyLocked(oldID)
		}
	}
	if b.maxSessions > 0 && len(b.sessions) >= b.maxSessions {
		return SessionInfo{}, false, ErrTooManySessions
	}
	id := randomID()
	s := newSession(id, clientID, cleanStart, will)
	if willGracePeriod > 0 {
		s.willGracePeriod = willGracePeriod
	}
	if heartbeatInterval > 0 {
		s.heartbeatInterval = heartbeatInterval
	} else {
		s.heartbeatInterval = defaultSessionHeartbeat
	}
	b.sessions[id] = s
	if clientID != "" {
		b.byClient[clientID] = id
	}
	return b.snapshot(s), false, nil
}

// ListSessions 返回全部会话快照。
func (b *Broker) ListSessions() []SessionInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]SessionInfo, 0, len(b.sessions))
	for _, s := range b.sessions {
		out = append(out, b.snapshot(s))
	}
	return out
}

// GetSession 返回单个会话快照。
func (b *Broker) GetSession(id string) (SessionInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return SessionInfo{}, ErrSessionNotFound
	}
	return b.snapshot(s), nil
}

// CloseSession 正常断开:销毁会话,不触发遗嘱。
func (b *Broker) CloseSession(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.sessions[id]; !ok {
		return ErrSessionNotFound
	}
	b.destroyLocked(id)
	return nil
}

// SetSubscriptions 为会话新增/更新订阅(按 filter upsert),立即生效。
// 若启用 ACL,逐一校验 identity 对 filter 的订阅权限,任一被拒则整体拒绝(ErrForbidden)。
// 若会话流在线,对新增 filter 匹配到的保留消息立即补发。
func (b *Broker) SetSubscriptions(id string, subs []Subscription, identity Identity) error {
	for _, sub := range subs {
		if err := ValidateFilter(sub.Filter); err != nil {
			return err
		}
		if sub.QoS < 0 || sub.QoS > 1 {
			return fmt.Errorf("invalid subscription qos %d", sub.QoS)
		}
		if err := b.authorizeSubscribe(identity, sub.Filter); err != nil {
			return err
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return ErrSessionNotFound
	}
	var added []Subscription
	for _, sub := range subs {
		if _, exists := s.subs[sub.Filter]; exists {
			b.routes.remove(sub.Filter, id)
		} else {
			added = append(added, sub)
		}
		s.subs[sub.Filter] = sub
		b.routes.add(sub.Filter, routeSub{sess: s, qos: sub.QoS})
	}
	if s.stream != nil {
		b.resendRetainedLocked(s, added)
	}
	return nil
}

// GetSubscriptions 返回会话订阅表。
func (b *Broker) GetSubscriptions(id string) ([]Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return nil, ErrSessionNotFound
	}
	return s.Subscriptions(), nil
}

// DeleteSubscriptions 按 filter 删除订阅项,立即生效。
func (b *Broker) DeleteSubscriptions(id string, filters []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return ErrSessionNotFound
	}
	for _, f := range filters {
		if _, ok := s.subs[f]; ok {
			b.routes.remove(f, id)
			delete(s.subs, f)
		}
	}
	return nil
}

// OpenStream 打开会话的推送流。同一会话同时只允许一条流,重复打开会顶掉旧流。
// 打开时对当前订阅表匹配到的保留消息做一次补发。
// willGracePeriod 为流级遗嘱宽限期,>0 时覆盖会话级设置,<=0 表示不覆盖。
// heartbeatInterval 为流级 SSE 心跳间隔,规则同上。
func (b *Broker) OpenStream(id string, willGracePeriod, heartbeatInterval time.Duration) (<-chan Event, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return nil, ErrSessionNotFound
	}
	b.closeStreamLocked(s)
	if s.willTimer != nil {
		s.willTimer.Stop()
		s.willTimer = nil
	}
	if willGracePeriod > 0 {
		s.willGracePeriod = willGracePeriod
	}
	if heartbeatInterval > 0 {
		s.heartbeatInterval = heartbeatInterval
	}
	s.stream = make(chan Event, b.streamBuffer)
	s.Online = true
	b.resendRetainedLocked(s, s.Subscriptions())
	return s.stream, nil
}

// heartbeatOf 返回会话生效的 SSE 心跳间隔:会话/流未定义时回落到 Broker 预定义设置。
// 会话不存在时返回 Broker 预定义值。
func (b *Broker) heartbeatOf(id string) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.sessions[id]; ok && s.heartbeatInterval > 0 {
		return s.heartbeatInterval
	}
	return b.heartbeatInterval
}

// DetachStream 在推送流连接结束时由 HTTP 层调用,标记会话离线并启动遗嘱宽限期。
// 若流已被顶掉或主动关闭(身份不匹配),不做任何处理。
func (b *Broker) DetachStream(id string, ch <-chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok || s.stream == nil || s.stream != ch {
		return
	}
	s.stream = nil
	s.Online = false
	s.inflight = make(map[string]*inflightMsg)
	s.willTimer = time.AfterFunc(b.gracePeriodOf(s), func() { b.onGraceExpired(id) })
}

// CloseStream 主动断开会话的推送流;会话与订阅表保留,不触发遗嘱。
func (b *Broker) CloseStream(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return ErrSessionNotFound
	}
	if s.stream == nil {
		return ErrStreamNotOpen
	}
	b.closeStreamLocked(s)
	return nil
}

// Ack 确认一条 QoS 1 消息,幂等。
func (b *Broker) Ack(id, messageID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return ErrSessionNotFound
	}
	delete(s.inflight, messageID)
	return nil
}

// Publish 发布主题消息。返回消息 ID 与实际投递的会话数。
// 仅投递给当前流在线的会话;离线会话的消息直接丢弃。
// 若启用 ACL,发布前校验 identity 对该 topic 的发布权限,被拒返回 ErrForbidden。
func (b *Broker) Publish(msg Message, identity Identity) (messageID string, delivered int, err error) {
	if err := ValidateTopic(msg.Topic); err != nil {
		return "", 0, err
	}
	if msg.QoS < 0 || msg.QoS > 1 {
		return "", 0, fmt.Errorf("invalid publish qos %d", msg.QoS)
	}
	if err := b.authorizePublish(identity, msg.Topic); err != nil {
		return "", 0, err
	}
	messageID = fmt.Sprint(b.seq.Add(1))
	b.mu.Lock()
	defer b.mu.Unlock()
	if msg.Retain {
		if msg.Payload == "" {
			delete(b.retained, msg.Topic)
		} else {
			b.retained[msg.Topic] = msg
		}
	}
	delivered = b.deliverLocked(messageID, msg, false)
	return messageID, delivered, nil
}

// deliverLocked 通过路由树匹配在线会话并投递。
func (b *Broker) deliverLocked(messageID string, msg Message, retain bool) int {
	// 每个会话至多投递一份,QoS 取所有匹配订阅的最大值。
	normalQos := make(map[*Session]int)
	for _, sub := range b.routes.match(msg.Topic) {
		s := sub.sess
		if s.stream == nil {
			continue // 仅投递在线会话
		}
		if qos, ok := normalQos[s]; !ok || sub.qos > qos {
			normalQos[s] = sub.qos
		}
	}

	delivered := 0
	for s, qos := range normalQos {
		if b.sendLocked(s, Event{MessageID: messageID, Topic: msg.Topic, Payload: msg.Payload, QoS: min(msg.QoS, qos), Retain: retain}) {
			delivered++
		}
	}
	return delivered
}

// sendLocked 非阻塞投递;QoS1 记入 in-flight 待重投。缓冲满则丢弃。
func (b *Broker) sendLocked(s *Session, ev Event) bool {
	select {
	case s.stream <- ev:
		if ev.QoS == 1 {
			s.inflight[ev.MessageID] = &inflightMsg{event: ev, sent: time.Now()}
		}
		return true
	default:
		return false
	}
}

// resendRetainedLocked 对给定订阅补发匹配到的保留消息。
func (b *Broker) resendRetainedLocked(s *Session, subs []Subscription) {
	for topic, msg := range b.retained {
		qos := -1
		for _, sub := range subs {
			if Match(sub.Filter, topic) && sub.QoS > qos {
				qos = sub.QoS
			}
		}
		if qos >= 0 {
			id := fmt.Sprint(b.seq.Add(1))
			b.sendLocked(s, Event{MessageID: id, Topic: topic, Payload: msg.Payload, QoS: min(msg.QoS, qos), Retain: true})
		}
	}
}

// onGraceExpired 遗嘱宽限期到期:发布遗嘱;cleanStart 会话销毁,否则保留订阅表待恢复。
// 遗嘱投递规则(对齐 MQTT):retain=false 时 fire-and-forget(仅投给当前在线订阅者);
// retain=true 时无论 clean_start 与否都驻留为保留消息,直至被同主题新保留消息覆盖
// 或空 payload 清除——上线状态由客户端自行发布 retained 消息维护,Broker 不自动收回。
func (b *Broker) onGraceExpired(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok || s.Online {
		return
	}
	s.willTimer = nil
	if s.will != nil {
		b.deliverLocked(fmt.Sprint(b.seq.Add(1)), Message{
			Topic:   s.will.Topic,
			Payload: s.will.Payload,
			QoS:     s.will.QoS,
			Retain:  s.will.Retain,
		}, false)
		if s.will.Retain {
			b.retained[s.will.Topic] = Message{Topic: s.will.Topic, Payload: s.will.Payload, QoS: s.will.QoS, Retain: true}
		}
	}
	if s.CleanStart {
		b.destroyLocked(id)
	}
}

// closeStreamLocked 关闭会话当前推送流并清空 in-flight。
func (b *Broker) closeStreamLocked(s *Session) {
	if s.stream != nil {
		close(s.stream)
		s.stream = nil
	}
	s.Online = false
	s.inflight = make(map[string]*inflightMsg)
}

// destroyLocked 销毁会话,不触发遗嘱。
func (b *Broker) destroyLocked(id string) {
	s, ok := b.sessions[id]
	if !ok {
		return
	}
	if s.willTimer != nil {
		s.willTimer.Stop()
	}
	b.closeStreamLocked(s)
	for f := range s.subs {
		b.routes.remove(f, id)
	}
	if s.ClientID != "" {
		delete(b.byClient, s.ClientID)
	}
	delete(b.sessions, id)
}

// redeliverLoop 周期性重投超时未 ACK 的 QoS1 消息。
func (b *Broker) redeliverLoop() {
	defer b.stopWg.Done()
	ticker := time.NewTicker(b.redeliverInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case now := <-ticker.C:
			b.mu.Lock()
			for _, s := range b.sessions {
				if s.stream == nil {
					continue
				}
				for _, im := range s.inflight {
					if now.Sub(im.sent) >= b.redeliverInterval {
						select {
						case s.stream <- im.event:
							im.sent = now
						default:
						}
					}
				}
			}
			b.mu.Unlock()
		}
	}
}

// gracePeriodOf 返回会话生效的遗嘱宽限期:会话/流未定义时回落到 Broker 预定义设置。
func (b *Broker) gracePeriodOf(s *Session) time.Duration {
	if s.willGracePeriod > 0 {
		return s.willGracePeriod
	}
	return b.willGracePeriod
}

// heartbeatIntervalOf 返回会话生效的 SSE 心跳间隔:未定义时回落到 Broker 预定义设置。
// 调用方需持锁。
func (b *Broker) heartbeatIntervalOf(s *Session) time.Duration {
	if s.heartbeatInterval > 0 {
		return s.heartbeatInterval
	}
	return b.heartbeatInterval
}

func (b *Broker) snapshot(s *Session) SessionInfo {
	return SessionInfo{
		ID:                s.ID,
		ClientID:          s.ClientID,
		CleanStart:        s.CleanStart,
		Online:            s.Online,
		CreatedAt:         s.CreatedAt,
		WillGracePeriod:   b.gracePeriodOf(s).String(),
		HeartbeatInterval: b.heartbeatIntervalOf(s).String(),
		Subscriptions:     s.Subscriptions(),
	}
}

// authorizePublish 校验发布权限;未启用 ACL 时默认放行。
func (b *Broker) authorizePublish(identity Identity, topic string) error {
	if b.authorizer == nil {
		return nil
	}
	return b.authorizer.AuthorizePublish(identity, topic)
}

// authorizeSubscribe 校验订阅权限;未启用 ACL 时默认放行。
func (b *Broker) authorizeSubscribe(identity Identity, filter string) error {
	if b.authorizer == nil {
		return nil
	}
	return b.authorizer.AuthorizeSubscribe(identity, filter)
}

func randomID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf[:])
}
