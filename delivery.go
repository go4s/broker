package broker

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

// Publish 发布主题消息。返回消息 ID 与实际投递的会话数。
// 仅投递给当前流在线的会话;离线会话的消息直接丢弃。
func (b *Broker) Publish(msg Message) (messageID string, delivered int, err error) {
	if err := ValidateTopic(msg.Topic); err != nil {
		return "", 0, err
	}
	if msg.QoS < 0 || msg.QoS > 1 {
		return "", 0, fmt.Errorf("invalid publish qos %d", msg.QoS)
	}
	messageID = b.nextMessageID()
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

// newEvent 构造一条投递事件,投递 QoS 为发布与订阅取小。
func newEvent(messageID string, msg Message, qos int, retain bool) Event {
	return Event{
		MessageID: messageID,
		Topic:     msg.Topic,
		Payload:   msg.Payload,
		QoS:       qos,
		Retain:    retain,
	}
}

// deliverLocked 通过路由树匹配在线会话并投递。调用方需持锁。
// 每个会话至多投递一份,QoS 取所有匹配订阅的最大值。
func (b *Broker) deliverLocked(messageID string, msg Message, retain bool) int {
	sessionQoS := make(map[*Session]int)
	for _, sub := range b.routes.match(msg.Topic) {
		s := sub.sess
		if s.stream == nil {
			continue // 仅投递在线会话
		}
		if qos, ok := sessionQoS[s]; !ok || sub.qos > qos {
			sessionQoS[s] = sub.qos
		}
	}

	delivered := 0
	for s, qos := range sessionQoS {
		if b.sendLocked(s, newEvent(messageID, msg, min(msg.QoS, qos), retain)) {
			delivered++
		}
	}
	return delivered
}

// sendLocked 非阻塞投递;QoS1 记入 in-flight 环形窗口待重投。缓冲满则丢弃。
// QoS1 窗口达上限时拒绝写入:不投递、不入队,记录消息 ID 与丢弃原因
// (missed Ack 过多时保护内存,窗口随批量 ACK 释放)。
func (b *Broker) sendLocked(s *Session, ev Event) bool {
	var id uint64
	if ev.QoS == 1 {
		parsed, err := strconv.ParseUint(ev.MessageID, 10, 64)
		if err != nil {
			log.Printf("broker: qos1 message dropped: session=%s msgID=%s reason=invalid message id: %v",
				s.ID, ev.MessageID, err)
			return false
		}
		id = parsed
		if s.inflight.full() {
			log.Printf("broker: qos1 message dropped: session=%s msgID=%s reason=inflight window full (%d/%d)",
				s.ID, ev.MessageID, s.inflight.len(), s.maxInflight)
			return false
		}
	}
	select {
	case s.stream <- ev:
		if ev.QoS == 1 {
			if !s.inflight.push(inflightMsg{id: id, event: ev, sent: time.Now()}) {
				log.Printf("broker: qos1 message queue full: session=%s msgID=%s reason=inflight unintended overflow",
					s.ID, ev.MessageID)
			}
		}
		return true
	default:
		return false
	}
}

// resendRetainedLocked 对给定订阅补发匹配到的保留消息。调用方需持锁。
func (b *Broker) resendRetainedLocked(s *Session, subs []Subscription) {
	for topic, msg := range b.retained {
		matched, bestQoS := false, -1
		for _, sub := range subs {
			if Match(sub.Filter, topic) {
				matched = true
				if sub.QoS > bestQoS {
					bestQoS = sub.QoS
				}
			}
		}
		if matched {
			b.sendLocked(s, newEvent(b.nextMessageID(), msg, min(msg.QoS, bestQoS), true))
		}
	}
}
