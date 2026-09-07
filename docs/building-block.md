# 构建块采用描述:内存消息总线(MQTT-like, HTTP + SSE)

> 本文描述本仓库所实现的**标准构建块**——一个可嵌入宿主应用的内存消息总线。
> 它以"HTTP 发布 / SSE 订阅推送"的形态,复刻 MQTT 的会话、订阅、QoS、
> 保留消息、遗嘱等语义,作为微服务/物联网场景下进程内轻量 pub/sub 的
> 标准构建块。本文聚焦**采用层**:定位、职责、语义契约、对内有线协议、
> 配置与扩展点、验证清单。重建提示词见 [rebuild-prompt.md](./rebuild-prompt.md)。

---

## 1. 构建块定位

**一句话**:一个"内存中的迷你 MQTT Broker",用 HTTP + SSE 对外提供发布/订阅能力,
让 REST 风格客户端(浏览器、设备、微服务)能拿到消息级的异步推送。
**核心心智模型**:`会话(Session)` 持有 `订阅表(Subscription)`,
**推送走 SSE 流**,**发布走普通 HTTP 请求**。

目标使用者:需要在进程内获得基于主题的 pub/sub,但不想引入独立消息中间件
(RabbitMQ/Kafka/EMQX)的场景;典型如事件通知、进度推送、状态广播、物联设备上行。

### 适用场景
- 设备/客户端状态与告警上行、下发
- 任务进度、流水线事件的实时推送(web 前端 SSE 消费)
- 服务间点对点/一对多轻量广播(单实例、内存态可接受)

### 非目标(职责边界)
- **不持久化**:全部状态在内存,重启即失;离线消息不做存储转发(仅保留消息留一份)
- **不做跨进程/集群/分片**:单实例内存模型;需要多副本分布时将本块作为单机内核演进
- **不实现 MQTT over TCP 协议栈**:只暴露 HTTP/SSE 传输面;语义取 MQTT 子集
- **不做鉴权实现本身**:身份识别与授权规则由宿主注入(见 §7)
- **不做消费确认之外的事务/回滚**;QoS1 为 at-least-once,非 exactly-once

---

## 2. 领域模型

| 概念 | 说明 |
|---|---|
| **Topic(主题)** | `a/b/c` 层级字符串,发布主题**禁止通配符**,每级非空 |
| **Filter(过滤器)** | 订阅用,支持 `+`(单级)、`#`(多级,须在末尾,独占一级);拒绝 `$share/` 共享订阅 |
| **Message** | `{topic, payload, qos, retain}` |
| **Event** | 推入 SSE 流的一则消息事件 `{message_id, topic, payload, qos, retain}` |
| **Session** | 客户端服务端会话:`id/client_id/clean_start/online/订阅表/遗嘱/inflight 窗口` |
| **Subscription** | `{filter, qos}`,按 filter upsert |

---

## 3. 语义契约(行为不变式)

### 3.1 主题过滤匹配
```
#       匹配任意层级数(含零层):# 匹配 a, a/b, a/b/c
+       恰好匹配一层:a/+ 匹配 a/b,不匹配 a/b/c、不匹配 a
a/#     匹配 a 及 a 以下任意层
+/+     匹配 a/b,不匹配 a
a/b     精确匹配 a/b,不匹配 a/b/c
```

### 3.2 QoS 语义
- QoS ∈ {0, 1};**QoS1 = at-least-once**:发布即入会话的 in-flight 窗口,订阅方
  必须以 `id`(SSE 事件中的 `message_id`)回 ACK;超重投间隔未 ACK 则重复推送。
- 实际投递 QoS = min(发布 QoS, 订阅 QoS)。
- **批量 ACK**:确认 `N` 视同"`id ≤ N` 已全部收到",一次出窗;出窗避免重复投递。
- **in-flight 窗口**:每会话定长窗口(默认 16),满则拒绝新的 QoS1 投递
  (不投递、不入窗,记录原因日志);窗口随批量 ACK 释放。

### 3.3 in-flight 环形队列(实现语义,重建时须保持)
固定容量数组 + 头/尾指针:
- 窗口中的消息 id 递增排列(全 Broker 单调递增 id 保证)。
- **窗口满 → 拒绝入窗**(不覆盖已有)。
- ACK 语义(与纯指针直觉一致,只有"队列内/空闲"两态):
  - ack id **落在窗口内** → 移除 `id<=n` 的最老连续条目;
  - ack id **低于窗口首条**(陈旧/重复)→ 幂等 no-op;
  - ack id **高于窗口末条**(未见/未来)→ 视为无效 no-op。

### 3.4 保留消息(Retain)
- `retain=true` 发布:存为该主题的保留消息,仅保留最后一条。
- 空 payload + retain:清除该主题保留消息。
- 推送流在线时:新增订阅 / 新开流,凡过滤器匹配的保留消息**立即补发**(`retain=true` 标记)。
- 遗嘱 `retain=true` 同样驻留为保留消息(Broker 不自动收回,由业务自行覆盖或清除)。

### 3.5 遗嘱(Will)
- 会话创建时可登记遗嘱;推送流**异常断开**进入遗嘱宽限期,宽限期内未重连 →
  超时后 Broker 代发遗嘱消息。
- **正常断开**(DELETE `/sessions/{id}`=`Close`、DELETE `/sessions/{id}/stream`=`CloseStream`)
  **不触发遗嘱**。
- 宽限期三级生效:Broker 预定义(默认 30s)→ 会话创建级 → 打开流级(查询参数覆盖)。
- `clean_start=false` 会话超时仅发遗嘱,不销毁会话(保留订阅表待重连恢复);
  `clean_start=true` 超时发遗嘱后销毁。

### 3.6 会话与断线语义
- 同一会话同一时刻只允许一条推送流,重复打开**顶掉旧流**。
- `clean_start=false` + 同 `client_id` 再创建 → **恢复(resumed)** 既有会话与订阅表。
- 只投递**流在线**的会话;离线期间消息直接丢弃(QoS1 in-flight 随流断开清空)。
- 慢消费者保护:每会话流缓冲(默认 1),缓冲满则丢弃,不改阻塞发布方。

### 3.7 SSE 心跳
- 按生效心跳间隔周期推送 `: ping` 注释行;写失败即判定连接已死 → 进入遗嘱判定。
- 心跳间隔三级生效(流级 → 会话级缺省 3min → Broker 预定义默认 10min)。

---

## 4. 对内有线契约(HTTP/SSE + 错误码)

### 4.1 错误约定
统一的 `{"error": "<描述>"}` 4xx 响应与映射:

| 状态码 | 触发 |
|---|---|
| 400 | topic/filter/qos/时长参数非法、body 解析失败 |
| 404 | session 不存在 |
| 409 | DELETE stream 时推送流未打开 |
| 413 | body 超过 1024B |
| 429 | 会话数达上限 |

### 4.2 REST 路由(可挂任意路径前缀)
| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/sessions` | 创建会话 `{client_id?, clean_start?, will?, will_grace_period?, heartbeat_interval?, max_inflight?}` → `{session_id, resumed}` |
| GET | `/sessions` | 列会话(调试/管理) |
| GET | `/sessions/:id` | 单会话快照 |
| DELETE | `/sessions/:id` | 正常断开(不触发遗嘱) |
| PUT | `/sessions/:id/subscriptions` | upsert 订阅表 `{subscriptions:[{filter,qos}]}` |
| GET | `/sessions/:id/subscriptions` | 查询订阅表 |
| DELETE | `/sessions/:id/subscriptions` | 删除订阅项 `{filters:[...]}` |
| GET | `/sessions/:id/stream` | 打开 SSE 流 `?will_grace_period=&heartbeat_interval=` |
| DELETE | `/sessions/:id/stream` | 主动断流(保留会话与订阅表) |
| POST | `/sessions/:id/acks` | ACK `{message_id}`(批量语义) |
| POST | `/publish` | 发布 `{topic,payload,qos,retain?}` → `{message_id,delivered}` |

### 4.3 SSE 事件格式
```
id: 17                 # QoS1 ACK 引用同一 message_id
event: message
data: {"message_id":"17","topic":"a/b","payload":"hello","qos":1,"retain":false}
(空行)
```
心跳行:`: ping`(仅一行,可安全忽略)。

### 4.4 语义要点复述
- `resumed=true` ⇒ 服务端复用了既有会话订阅表。
- 订阅/删订阅在流存活期间**调用即生效**;新订阅即时补发匹配的保留消息。
- 消息 id 为**全 Broker 单调递增**(跨所有 session 共享一个计数器)。

---

## 5. 内容面:配置与默认值

| 项 | 默认 | 覆盖途径 |
|---|---|---|
| in-flight 窗口(QoS1) | 16 | `WithMaxInflight`(Broker 级)/ 创建会话 `max_inflight`(会话级覆盖) |
| QoS1 重投间隔 | 5s | `WithRedeliverInterval` |
| 遗嘱宽限期 | 30s | `WithWillGracePeriod` + 会话级/流级 |
| SSE 心跳 | 会话级缺省 3min / Broker 10min | 创建 / dial 查询参数 / `WithHeartbeatInterval` |
| 会话数上限 | 不限 | `WithMaxSessions` |
| 会话推送流缓冲 | 1 | `WithStreamBuffer` |
| 请求体上限 | 1024B | 固定 |
| 无效参数行为 | 拒绝(400) | 各 option 对非法值静默忽略(`>0` 才生效) |

---

## 6. 并发与生命周期模型(实现须遵守的约束)

- **单把互斥锁串行化全部共享状态**(会话表、订阅树、保留消息、in-flight)。
- **持锁期间禁止阻塞操作**:不阻塞收发、不网络 I/O、不 sleep;
  所有 channel 投递均**非阻塞**(buffer 满即丢),持锁分支均为有界计算。
- 线程=goroutine/任务构成:
  1. 重投循环(定时扫全量会话 in-flight,非阻塞补投);
  2. `will_timer`(到期回调,进锁前置空操作再判);
  3. 每推送流一条读循环(锁外做网络写,锁内只读事件)。
- **优雅关闭**:幂等;先关重投通道 → 等待停止 → 再持锁销毁全部会话并关流;
  closed 标志保证并发关闭安全。
- 流顶替防护:旧流 detach 时按"channel 指针仍等于当前流"才生效,不会误拆新流。

---

## 7. 集成与扩展点

1. **宿主框架挂载**:把全部路由一次性注册到 HTTP 框架(gin 用 `Mount(ir)`),
   可挂任意路径前缀(如 `/broker`)。
2. **鉴权扩展位**:身份由宿主中间件解析(Header/JWT/Cookie)后注入上下文;
   构建块在受保护操作(发布/订阅)前读取并交给可插拔 `Authorizer`
   (`allow/deny` 规则或自实现),未启用时行为后向兼容(全放行)。
   - 语义:未注入身份 → 401;已注入但被拒 → 403;规则默认拒绝(default-deny)。
3. **发布入口**:内置 `/publish` HTTP 端点;进程内直用 `Publish(msg)` 同语义。
4. **任务/事件驱动**:`example/tasks` 演示"后台异步任务 → 分阶段发布进度
   (关键节点 QoS1 / 仅百分比 QoS0)→ 前端 SSE 订阅实时渲染"的端到端模式。

---

## 8. 采用步骤(宿主集成 checklist)

1. 创建 Broker 实例并注入构造选项(窗口、重投、宽限、心跳、会话上限)。
2. (可选)挂鉴权中间件 + 配置 Authorizer。
3. `Mount` 到 HTTP router(或进程内直接用库 API)。
4. 前端/客户端流程:创建会话 → PUT 订阅表 → 打开 SSE 流 → (发布方 POST `/publish`)→
   收到 QoS1 用事件 id 回 ACK。
5. 端点关闭时:DELETE stream(不触发遗嘱)或直接 DELETE session。
6. **(必做)对照 §9 验证清单跑通回归**,特别是主题匹配、批量 ACK、遗嘱、顶流。

---

## 9. 验证清单(验收口径)

### 9.1 单元级
- **主题匹配矩阵**:§3.1 全表 + 反例(`a/+` vs `a/b/c`、`a/#` vs 其他分支等)。
- **过滤器校验**:空 filter / 空层级 `a//b` / `#` 非末尾 `a/#/b` / `#` 混级 `a#` /
  `+` 混级 `a+/b` / `$share/` 共享订阅 → 全部拒绝。
- **发布校验**:空 topic / topic 含通配符 / qos=2 → 拒绝。
- **in-flight 环形队列**(重点):正常插入到满;满后 push 拒绝且不覆盖;
  ack 在窗口内(批量移前缀)/ 低于窗口首条(no-op)/ 高于窗口末条(no-op);
  头指针推进后 tail 环绕仍有序;reset 后从头开始。

### 9.2 语义级
- QoS1:发布→投递→(不 ACK)超时重投→ACK 后不再重投;幂等 ACK。
- 批量 ACK:同窗多条,ack N 后 id≤N 全部出窗。
- 保留消息:发布保留→新订阅在线补发(`retain=true`)→空 payload 清除;
  reopen stream 补发。
- 遗嘱:异常断流 + 宽限期超时→代发;正常 CLOSE→不发;clean_start=false 超时保会话。
- 会话恢复:`clean_start=false` 同 client_id→`resumed=true` 复订阅表;
  `clean_start=true`→清空重建。
- 顶流:DELETE stream 保留会话;B 次打开顶掉 A,Publish 只到 B。

### 9.3 契约/集成级
- HTTP 状态码全表(400/404/409/413/429)逐条命中。
- SSE 事件格式(4 行块 + `: ping` 心跳)可被标准 EventSource/SSE 客户端消费。
- 真实浏览器/curl 全流程:创建→订阅→开流→发布→ACK→完成。

### 9.4 并发/健壮性
- race detector(Go)/等价工具(其他语言)全绿。
- `Close` 幂等、不泄漏重投 goroutine/定时器。
- handler panic 兜底:SSE 写循环 panic 不拖垮进程且会话最终被回收。
- 依赖 curl 长连接场景写失败判定(心跳写失败 → 离线)。

---

## 10. 已知边界与取舍

- 单内存实例,无持久化、无离线队列:设计取舍,非缺陷(场景内置)。
- 每个会话推送流缓冲=1 会丢瞬时突刺(慢消费者保护设计);需要不丢请增大
  `WithStreamBuffer` 并在语义上接受 in-flight 窗口限制。
- 遗嘱保留消息不自动收回;上线状态由业务自行发布覆盖。
- 请求体 1024B 上限面向事件型载荷(非大消息通道);大负载走业务侧对象引用/附件。
- QoS 只有 0/1:at-most-once / at-least-once,无 exactly-once、无有序性之外
  (实际同会话按发布顺序单调递增 id 投递,保证单会话有序)。