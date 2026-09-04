# broker

内存消息 Broker 库,功能模型类似 MQTT。以极简方式集成到 gin:**订阅/推送走 SSE,发布走 HTTP**。

## 特性

- `#`(多级)/ `+`(单级)通配符订阅
- QoS 0 / QoS 1(SSE 投递 + HTTP ACK,超时未确认自动重投)
- retained 保留消息(新订阅立即补发匹配主题的最后一条)
- 遗嘱消息(推送流异常断开且宽限期内未重连时发布;宽限期支持会话级/流级设置,缺省用 Broker 预定义值)
- 共享订阅 `$share/{group}/{filter}`(组内轮询一份,组间各一份)
- `clean_start=false` 时服务端保留订阅关系,重连后恢复;**不做订阅消息的离线保留**
- 订阅表通过 HTTP 维护,流存活期间增删**立即生效**

## 集成

```go
b := broker.New()
r := gin.Default()
b.Mount(r)                  // 或 b.Mount(r.Group("/broker")) 加路径前缀
r.Run(":8080")
```

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/sessions` | 创建会话 `{client_id?, clean_start?, will?, will_grace_period?}` → `{session_id, resumed}` |
| GET | `/sessions` | 列出会话 |
| DELETE | `/sessions/{id}` | 正常断开(不触发遗嘱) |
| PUT | `/sessions/{id}/subscriptions` | 按 filter upsert 订阅 `{subscriptions:[{filter,qos}]}`,立即生效 |
| GET | `/sessions/{id}/subscriptions` | 查看订阅表 |
| DELETE | `/sessions/{id}/subscriptions` | 删除订阅项 `{filters:[...]}`,立即生效 |
| GET | `/sessions/{id}/stream?will_grace_period=` | 打开 SSE 推送流(订阅表先经 HTTP 维护);查询参数可覆盖遗嘱宽限期 |
| DELETE | `/sessions/{id}/stream` | 主动断开推送流(会话与订阅表保留,不触发遗嘱) |
| POST | `/sessions/{id}/acks` | QoS1 确认 `{message_id}` |
| POST | `/publish` | 发布 `{topic, payload, qos, retain?}` → `{message_id, delivered}` |

完整接口契约(含响应结构与字段说明)见 [client.http](./client.http)。

## 快速体验

```bash
go run ./example &
# 创建会话并拿到 session_id
SID=$(curl -s -XPOST localhost:8080/sessions -d '{"client_id":"demo"}' | jq -r .session_id)
# 维护订阅表
curl -XPUT localhost:8080/sessions/$SID/subscriptions \
  -d '{"subscriptions":[{"filter":"sensors/+","qos":1}]}'
# 打开推送流(保持连接)
curl -N localhost:8080/sessions/$SID/stream &
# 发布消息
curl -XPOST localhost:8080/publish -d '{"topic":"sensors/temp","payload":"21.5","qos":1}'
# 确认(SSE 事件里的 id 字段)
curl -XPOST localhost:8080/sessions/$SID/acks -d '{"message_id":"1"}'
# 流存活期间动态增删订阅,立即生效
curl -XPUT localhost:8080/sessions/$SID/subscriptions \
  -d '{"subscriptions":[{"filter":"alerts/#","qos":0}]}'
```

SSE 事件格式:

```
id: 1
event: message
data: {"message_id":"1","topic":"sensors/temp","payload":"21.5","qos":1,"retain":false}
```

## 语义说明

- 仅投递给**推送流在线**的会话;离线期间的消息直接丢弃,QoS1 in-flight 也随流断开清空
- 实际投递 QoS = 发布 QoS 与订阅 QoS 取小
- 慢消费者保护:每会话流缓冲默认 256(`WithStreamBuffer`),缓冲满则丢弃
- 每个会话同时只允许一条推送流,重复打开顶掉旧流
- 默认参数:QoS1 重投间隔 5s(`WithRedeliverInterval`)、遗嘱宽限期 30s(`WithWillGracePeriod`,Broker 级预定义值)
- 遗嘱宽限期三级生效:创建会话 `will_grace_period`(会话级)→ 打开流 `?will_grace_period=`(流级覆盖)→ 均未定义时用 Broker 预定义值

## 测试

```bash
go test ./...
```
