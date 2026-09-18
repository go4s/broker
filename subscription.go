package broker

import "fmt"

// SetSubscriptions 为会话新增/更新订阅(按 filter upsert),立即生效。
// 若会话流在线,对新增 filter 匹配到的保留消息立即补发。
func (b *Broker) SetSubscriptions(id string, subs []Subscription) error {
	for _, sub := range subs {
		if err := ValidateFilter(sub.Filter); err != nil {
			return err
		}
		if sub.QoS < 0 || sub.QoS > 1 {
			return fmt.Errorf("invalid subscription qos %d", sub.QoS)
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
