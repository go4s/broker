package broker

import (
	"fmt"
	"time"
)

// Subscription 一条订阅关系。
type Subscription struct {
	Filter string `json:"filter"` // 主题过滤器,支持 + 与 # 通配符
	QoS    int    `json:"qos"`    // 0 或 1,投递时与发布 QoS 取小
}

// Will 遗嘱消息:会话异常终止(宽限期内未重连)时由 Broker 代为发布。
// Retain=false 时 fire-and-forget;Retain=true 时驻留为保留消息(对齐 MQTT),
// 直至被同主题新保留消息覆盖或空 payload 清除;上线状态由客户端自行发布
// retained 消息(如 "online")维护,Broker 不自动收回。
type Will struct {
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
	QoS     int    `json:"qos"`
	Retain  bool   `json:"retain"`
}

// inflightMsg 已投递未确认的 QoS 1 消息。
type inflightMsg struct {
	id    uint64 // 数值型消息 ID(Broker 级单调递增),批量 ACK 的定界依据
	event Event
	sent  time.Time
}

// inflightRing QoS1 待确认窗口:定长数组 + head/count 实现的环形缓冲
// (tail 由 head+count 推导)。条目按 id 递增排列,结构上保证批量 ACK
// ("ack id=N 即 N 及之前全部收到")只从 head 连续出队;窗口满时由调用方拒绝投递。
type inflightRing struct {
	buf   []inflightMsg
	head  int // 最老条目下标
	count int // 活跃条目数
}

// newInflightRing 构造容量为 capacity 的空环形缓冲。
func newInflightRing(capacity int) *inflightRing {
	if capacity <= 0 {
		capacity = 1
	}
	return &inflightRing{buf: make([]inflightMsg, capacity)}
}

func (r *inflightRing) len() int { return r.count }

func (r *inflightRing) full() bool { return r.count == len(r.buf) }

func (r *inflightRing) empty() bool { return r.count == 0 }

// push 追加一条到窗口尾部;满时返回 false 且不覆盖任何已有条目。
func (r *inflightRing) push(m inflightMsg) bool {
	if r.full() {
		return false
	}
	r.buf[(r.head+r.count)%len(r.buf)] = m
	r.count++
	return true
}

// ackUpTo 批量确认:仅当 id 落在当前窗口内时生效,把 id<=n 的最老连续条目
// 全部移出窗口,返回移除条数。环中每个位置只有"队列内/空闲"两种状态,
// 窗口外的 id(低于 head 的陈旧 ack、高于 tail 的未见 ack)一律视为无效 no-op。
func (r *inflightRing) ackUpTo(n uint64) int {
	if r.empty() {
		return 0
	}
	last := r.at(r.count - 1).id // 窗口内最大(最新)条目 id
	if n < r.buf[r.head].id || n > last {
		return 0
	}
	removed := 0
	for !r.empty() && r.buf[r.head].id <= n {
		r.head = (r.head + 1) % len(r.buf)
		r.count--
		removed++
	}
	return removed
}

// reset 清空窗口(不释放底层数组)。
func (r *inflightRing) reset() {
	r.head = 0
	r.count = 0
}

// at 返回第 i 个活跃条目(0 为最老),供重投循环修改 sent 时间。
func (r *inflightRing) at(i int) *inflightMsg {
	return &r.buf[(r.head+i)%len(r.buf)]
}

// Session 一个客户端会话:订阅表、推送流、QoS1 in-flight 队列与遗嘱。
// 订阅表独立于推送流存在(clean_start=false 时跨断线保留)。
type Session struct {
	ID         string    `json:"id"`
	ClientID   string    `json:"client_id"`
	CleanStart bool      `json:"clean_start"`
	Online     bool      `json:"online"`
	CreatedAt  time.Time `json:"created_at"`

	will              *Will
	willGracePeriod   time.Duration // 遗嘱宽限期;0 表示未定义,采用 Broker 预定义设置
	heartbeatInterval time.Duration // SSE 心跳间隔;0 表示未定义,采用 Broker 预定义设置
	// maxInflight QoS1 in-flight 窗口,创建时定容环形缓冲,固化后不可变
	maxInflight int
	subs        map[string]Subscription // filter -> sub
	inflight    *inflightRing           // QoS1 待确认环形窗口,容量=maxInflight
	stream      chan Event              // 当前推送流,nil 表示离线
	willTimer   *time.Timer
}

func newSession(id, clientID string, cleanStart bool, will *Will, maxInflight int) *Session {
	if maxInflight <= 0 {
		maxInflight = 1
	}
	return &Session{
		ID:          id,
		ClientID:    clientID,
		CleanStart:  cleanStart,
		CreatedAt:   time.Now(),
		will:        will,
		maxInflight: maxInflight,
		subs:        make(map[string]Subscription),
		inflight:    newInflightRing(maxInflight),
	}
}

// Subscriptions 返回订阅表副本。
func (s *Session) Subscriptions() []Subscription {
	out := make([]Subscription, 0, len(s.subs))
	for _, sub := range s.subs {
		out = append(out, sub)
	}
	return out
}

// CreateSessionOptions 创建会话的可选参数;零值字段表示未定义(采用 Broker 级设置)。
type CreateSessionOptions struct {
	ClientID   string // 业务侧客户端标识;空则由服务端生成
	CleanStart bool   // true 时同 ClientID 的旧会话被清空重建
	Will       *Will  // 遗嘱消息,可选
	// 会话级遗嘱宽限期;<=0 表示未定义(采用 Broker 预定义设置)
	WillGracePeriod time.Duration
	// 会话级 SSE 心跳间隔;>0 记入会话,<=0 时新会话取默认 3min
	HeartbeatInterval time.Duration
	// 会话级 QoS1 in-flight 窗口;<=0 时采用 Broker 预定义值
	MaxInflight int
}

// CreateSession 创建会话。cleanStart=true 时同 clientID 的旧会话被清空重建;
// cleanStart=false 且旧会话存在时复用其订阅表(resumed=true)。
// willGracePeriod / heartbeatInterval / maxInflight 仅在 >0 时覆盖既有会话的设置。
// 新建会话数达到上限(WithMaxSessions)时返回 ErrTooManySessions;恢复既有会话不受限。
func (b *Broker) CreateSession(opts CreateSessionOptions) (info SessionInfo, resumed bool, err error) {
	if opts.Will != nil {
		if err := ValidateTopic(opts.Will.Topic); err != nil {
			return SessionInfo{}, false, fmt.Errorf("invalid will topic: %w", err)
		}
		if opts.Will.QoS < 0 || opts.Will.QoS > 1 {
			return SessionInfo{}, false, fmt.Errorf("invalid will qos %d", opts.Will.QoS)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if opts.ClientID != "" {
		if oldID, ok := b.byClient[opts.ClientID]; ok {
			if !opts.CleanStart {
				return b.resumeLocked(oldID, opts), true, nil
			}
			b.destroyLocked(oldID)
		}
	}
	if b.maxSessions > 0 && len(b.sessions) >= b.maxSessions {
		return SessionInfo{}, false, ErrTooManySessions
	}
	id := randomID()
	inflightCap := opts.MaxInflight
	if inflightCap <= 0 {
		inflightCap = b.maxInflight
	}
	s := newSession(id, opts.ClientID, opts.CleanStart, opts.Will, inflightCap)
	if opts.WillGracePeriod > 0 {
		s.willGracePeriod = opts.WillGracePeriod
	}
	if opts.HeartbeatInterval > 0 {
		s.heartbeatInterval = opts.HeartbeatInterval
	} else {
		s.heartbeatInterval = defaultSessionHeartbeat
	}
	b.sessions[id] = s
	if opts.ClientID != "" {
		b.byClient[opts.ClientID] = id
	}
	return b.snapshot(s), false, nil
}

// resumeLocked 复用既有会话的订阅表(clean_start=false)。调用方需持锁。
func (b *Broker) resumeLocked(oldID string, opts CreateSessionOptions) SessionInfo {
	old := b.sessions[oldID]
	old.will = opts.Will
	if opts.WillGracePeriod > 0 {
		old.willGracePeriod = opts.WillGracePeriod
	}
	if opts.HeartbeatInterval > 0 {
		old.heartbeatInterval = opts.HeartbeatInterval
	}
	if opts.MaxInflight > 0 {
		old.maxInflight = opts.MaxInflight
	}
	if old.willTimer != nil {
		old.willTimer.Stop()
		old.willTimer = nil
	}
	b.closeStreamLocked(old)
	return b.snapshot(old)
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
