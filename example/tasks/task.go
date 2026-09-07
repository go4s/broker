// taskManager:手动启动任务、单 worker 串行执行,分阶段把进度经 broker 推送给订阅方。
// QoS 分层:阶段切换等关键节点用 QoS1(期望订阅方 ACK,未确认会重投);
// 同一阶段内的纯百分比变化用 QoS0(高频、不占 in-flight 窗口、无需确认)。
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/go4s/broker"
)

// stage 一个任务阶段:进入时发一条关键节点(qos1),阶段内 points 为纯百分比增量(qos0)。
type stage struct {
	name   string
	entry  int   // 进入本阶段时的进度(qos1 关键节点,含此条)
	points []int // 阶段内的百分比刻度(qos0)
}

func taskPipeline() []stage {
	return []stage{
		{name: "preparing", entry: 15, points: []int{20, 25}},
		{name: "compiling", entry: 35, points: []int{45, 55}},
		{name: "packaging", entry: 65, points: []int{70, 75}},
		{name: "publishing", entry: 85, points: []int{95}},
	}
}

// taskRequest 一次启动请求。
type taskRequest struct {
	id   string
	name string
}

// TaskInfo 单任务状态,JSON 序列化为推送 payload 与查询响应。
type TaskInfo struct {
	ID       string `json:"task_id"`
	Name     string `json:"name"`
	Stage    string `json:"stage"`
	Progress int    `json:"progress"`
	Status   string `json:"status"` // running | done
}

// taskManager 串行执行任务:channel 队列 + 单 worker,同一时刻至多一个任务在跑。
type taskManager struct {
	b       *broker.Broker
	queue   chan taskRequest
	nextID  atomic.Uint64
	current atomic.Pointer[TaskInfo] // 执行中的任务快照;无任务时 nil
}

func newTaskManager(b *broker.Broker) *taskManager {
	m := &taskManager{b: b, queue: make(chan taskRequest, 8)}
	go m.worker()
	return m
}

// Start 入队一个任务,立即返回 task_id;实际执行由 worker 串行完成。
func (m *taskManager) Start(name string) string {
	id := fmt.Sprintf("t%d", m.nextID.Add(1))
	if name == "" {
		name = "manual-" + id
	}
	m.queue <- taskRequest{id: id, name: name}
	return id
}

// Current 返回当前执行中的任务;无任务时返回 nil。
func (m *taskManager) Current() *TaskInfo {
	return m.current.Load()
}

func (m *taskManager) worker() {
	for req := range m.queue {
		m.run(req)
	}
}

func (m *taskManager) run(req taskRequest) {
	info := &TaskInfo{ID: req.id, Name: req.name, Status: "running"}
	m.current.Store(info)
	defer m.current.Store(nil)
	defer log.Printf("task %s done", info.ID)

	for _, st := range taskPipeline() {
		info.Stage = st.name
		info.Progress = st.entry
		m.publish(info, true) // 关键节点:阶段切换,qos1
		time.Sleep(400 * time.Millisecond)
		for _, p := range st.points {
			info.Progress = p
			m.publish(info, false) // 仅百分比变化,qos0
			time.Sleep(400 * time.Millisecond)
		}
	}
	info.Stage = "finished"
	info.Progress = 100
	info.Status = "done"
	m.publish(info, true) // 终态:retain=true,晚订阅者也能补发
}

// publish 把当前进度发布到 tasks/{task_id},按 key/critical 决定 QoS 与 retain。
func (m *taskManager) publish(info *TaskInfo, key bool) {
	payload, err := json.Marshal(info)
	if err != nil {
		log.Printf("marshal %s: %v", info.ID, err)
		return
	}
	msg := broker.Message{
		Topic:   "tasks/" + info.ID,
		Payload: string(payload),
	}
	if key {
		msg.QoS = 1
	}
	if info.Status == "done" {
		msg.Retain = true
	}
	id, delivered, err := m.b.Publish(msg)
	if err != nil {
		log.Printf("publish tasks/%s: %v", info.ID, err)
		return
	}
	log.Printf("[%s qos%d retain=%v delivered=%d] %s (%d%%): %s",
		id, msg.QoS, msg.Retain, delivered, info.Stage, info.Progress, info.Name)
}
