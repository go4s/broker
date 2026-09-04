package broker

import "time"

// Option 配置 Broker 行为。
type Option func(*Broker)

// WithStreamBuffer 设置每个会话推送流的事件缓冲大小,默认 1。
// 缓冲满时新消息对该会话丢弃(慢消费者保护)。
func WithStreamBuffer(n int) Option {
	return func(b *Broker) {
		if n > 0 {
			b.streamBuffer = n
		}
	}
}

// WithRedeliverInterval 设置 QoS 1 消息未收到 ACK 时的重投间隔,默认 5s。
func WithRedeliverInterval(d time.Duration) Option {
	return func(b *Broker) {
		if d > 0 {
			b.redeliverInterval = d
		}
	}
}

// WithMaxSessions 设置会话数上限,默认 0(不限制)。
// 达到上限后新建会话返回 ErrTooManySessions;恢复既有会话(clean_start=false)不受限。
func WithMaxSessions(n int) Option {
	return func(b *Broker) {
		if n > 0 {
			b.maxSessions = n
		}
	}
}

// WithHeartbeatInterval 设置 SSE 心跳间隔的 Broker 级预定义值,默认 10min。
// 会话创建时未指定心跳则取会话级缺省 3min;打开推送流时可再覆盖。
// 心跳写失败即判定客户端离线。
func WithHeartbeatInterval(d time.Duration) Option {
	return func(b *Broker) {
		if d > 0 {
			b.heartbeatInterval = d
		}
	}
}

// WithWillGracePeriod 设置推送流异常断开后的遗嘱宽限期,默认 30s。
// 宽限期内重连则取消遗嘱,超时则发布遗嘱消息。
// 这是 Broker 级预定义值;会话可在创建时给出会话级设置,打开推送流时再覆盖。
func WithWillGracePeriod(d time.Duration) Option {
	return func(b *Broker) {
		if d > 0 {
			b.willGracePeriod = d
		}
	}
}
