package broker

import (
	"fmt"
	"strconv"
	"time"
)

// OpenStream 打开会话的推送流。同一会话同时只允许一条流,重复打开会顶掉旧流。
// 打开时对当前订阅表匹配到的保留消息做一次补发。
// willGracePeriod 为流级遗嘱宽限期,>0 时覆盖会话级设置,<=0 表示不覆盖。
// heartbeatInterval 为流级 SSE 心跳间隔,规则同上。
func (b *Broker) OpenStream(id string, willGracePeriod, heartbeatInterval time.Duration) (
	<-chan Event, error,
) {
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

// heartbeatOf 返回会话生效的 SSE 心跳间隔:
// 会话/流未定义时回落到 Broker 预定义设置,会话不存在时同样返回预定义值。
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
	s.inflight.reset()
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

// closeStreamLocked 关闭会话当前推送流并清空 in-flight。调用方需持锁。
func (b *Broker) closeStreamLocked(s *Session) {
	if s.stream != nil {
		close(s.stream)
		s.stream = nil
	}
	s.Online = false
	s.inflight.reset()
}

// Ack 批量确认一条 QoS 1 消息:确认 N 即视为该会话 N 及之前投递的全部消息
// 均已收到,整个出队移出 in-flight 窗口。幂等;ID 非数值视为非法参数报错。
func (b *Broker) Ack(id, messageID string) error {
	n, err := strconv.ParseUint(messageID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid message id %q: %v", messageID, err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return ErrSessionNotFound
	}
	s.inflight.ackUpTo(n)
	return nil
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
				for i := 0; i < s.inflight.len(); i++ {
					im := s.inflight.at(i)
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
