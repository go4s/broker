package broker

import "time"

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
	event Event
	sent  time.Time
}

// Session 一个客户端会话:订阅表、推送流、QoS1 in-flight 队列与遗嘱。
// 订阅表独立于推送流存在(clean_start=false 时跨断线保留)。
type Session struct {
	ID         string    `json:"id"`
	ClientID   string    `json:"client_id"`
	CleanStart bool      `json:"clean_start"`
	Online     bool      `json:"online"`
	CreatedAt  time.Time `json:"created_at"`

	will      *Will
	willGracePeriod time.Duration // 遗嘱宽限期;0 表示未定义,采用 Broker 预定义设置
	heartbeatInterval time.Duration // SSE 心跳间隔;0 表示未定义,采用 Broker 预定义设置
	subs      map[string]Subscription // filter -> sub
	inflight  map[string]*inflightMsg // messageID -> msg
	stream    chan Event              // 当前推送流,nil 表示离线
	willTimer *time.Timer
}

func newSession(id, clientID string, cleanStart bool, will *Will) *Session {
	return &Session{
		ID:         id,
		ClientID:   clientID,
		CleanStart: cleanStart,
		CreatedAt:  time.Now(),
		will:       will,
		subs:       make(map[string]Subscription),
		inflight:   make(map[string]*inflightMsg),
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
