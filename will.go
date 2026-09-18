package broker

import "time"

// onGraceExpired 遗嘱宽限期到期:发布遗嘱;cleanStart 会话销毁,否则保留订阅表待恢复。
// 遗嘱投递规则(对齐 MQTT):retain=false 时 fire-and-forget(仅投给当前在线订阅者);
// retain=true 时无论 clean_start 与否都驻留为保留消息,直至被同主题新保留消息覆盖
// 或空 payload 清除——上线状态由客户端自行发布 retained 消息维护,
// Broker 不自动收回。
func (b *Broker) onGraceExpired(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok || s.Online {
		return
	}
	s.willTimer = nil
	if s.will != nil {
		b.deliverLocked(b.nextMessageID(), Message{
			Topic:   s.will.Topic,
			Payload: s.will.Payload,
			QoS:     s.will.QoS,
			Retain:  s.will.Retain,
		}, false)
		if s.will.Retain {
			b.retained[s.will.Topic] = Message{
				Topic:   s.will.Topic,
				Payload: s.will.Payload,
				QoS:     s.will.QoS,
				Retain:  true,
			}
		}
	}
	if s.CleanStart {
		b.destroyLocked(id)
	}
}

// gracePeriodOf 返回会话生效的遗嘱宽限期:
// 会话/流未定义时回落到 Broker 预定义设置。
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

// destroyLocked 销毁会话,不触发遗嘱。调用方需持锁。
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
