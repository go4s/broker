#!/usr/bin/env bash
# 面向消费方的功能示例:演示消费侧完整生命周期 ——
#   创建会话 → 维护订阅表 → 打开 SSE 推送流 → 收消息 / QoS1 ACK → 主动断开
# 接口契约见仓库根目录 client.http;步骤注释里标注了对应的请求编号。
#
# 前提:example 服务已启动(go run ./example),监听 127.0.0.1:8080。
# 依赖:curl、jq。
#
# 用法:
#   ./example/consumer.sh                # 只消费;另开终端用 client.http 请求 6 发消息
#   ./example/consumer.sh --self-test    # 自测模式:连上后自动发布两条演示消息
# 环境变量:
#   HOST       默认 http://127.0.0.1:8080
#   CLIENT_ID  默认 consumer-demo

set -euo pipefail

HOST="${HOST:-http://127.0.0.1:8080}"
CLIENT_ID="${CLIENT_ID:-consumer-demo}"
SELF_TEST=0
[ "${1:-}" = "--self-test" ] && SELF_TEST=1

for cmd in curl jq; do
	command -v "$cmd" >/dev/null || { echo "缺少依赖: $cmd" >&2; exit 1; }
done

log() { printf '[consumer] %s\n' "$*"; }

# ---------- 1. 创建会话(client.http 请求 1) ----------
# clean_start=false:异常掉线后重连可复用同 client_id 会话,保留服务端订阅表。
# will:流异常断开且遗嘱宽限期(默认 30s)内未重连时,Broker 代发遗嘱消息。
session_resp=$(curl -sS -X POST "$HOST/sessions" -H 'Content-Type: application/json' -d @- <<EOF
{
  "client_id": "$CLIENT_ID",
  "clean_start": false,
  "will": { "topic": "status/$CLIENT_ID", "payload": "offline", "qos": 0, "retain": true }
}
EOF
)
SID=$(jq -r '.session_id // empty' <<<"$session_resp")
[ -n "$SID" ] || { log "创建会话失败: $session_resp"; exit 1; }
log "会话已创建: session_id=$SID resumed=$(jq -r '.resumed' <<<"$session_resp")"

STREAM_PID=""
cleanup() {
	# 退出时走正常断开:先关流再删会话,均不触发遗嘱(client.http 请求 9/10)
	[ -n "$STREAM_PID" ] && kill "$STREAM_PID" 2>/dev/null || true
	curl -sS -o /dev/null -X DELETE "$HOST/sessions/$SID/stream" 2>/dev/null || true
	curl -sS -o /dev/null -X DELETE "$HOST/sessions/$SID" 2>/dev/null || true
	log "会话已正常断开(不触发遗嘱)"
}
trap cleanup EXIT INT TERM

# ---------- 2. 设置订阅表(client.http 请求 3;流存活期间增删立即生效) ----------
curl -sS -o /dev/null -X PUT "$HOST/sessions/$SID/subscriptions" -H 'Content-Type: application/json' -d '{
  "subscriptions": [
    { "filter": "sensors/+", "qos": 1 },
    { "filter": "alerts/#",  "qos": 0 }
  ]
}'
log "订阅表已设置: sensors/+(qos1), alerts/#(qos0)"

# ---------- 3. 打开 SSE 推送流并消费(client.http 请求 5) ----------
ack() { # QoS1 确认,幂等(client.http 请求 7);message_id 取自事件 id 字段
	curl -sS -o /dev/null -X POST "$HOST/sessions/$SID/acks" -H 'Content-Type: application/json' \
		-d "{\"message_id\": \"$1\"}"
}

consume() {
	local msg_id="" line data topic payload qos retain
	# 长连接流式响应;同一会话同时只允许一条流,重复打开会顶掉旧流
	curl -sS -N -H 'Accept: text/event-stream' "$HOST/sessions/$SID/stream" |
	while IFS= read -r line; do
		line=${line%$'\r'}
		case "$line" in
			": "*) ;; # 心跳行 ": ping",可安全忽略
			"id: "*) msg_id=${line#id: } ;;
			"data: "*)
				data=${line#data: }
				topic=$(jq -r .topic <<<"$data")
				payload=$(jq -r .payload <<<"$data")
				qos=$(jq -r .qos <<<"$data")
				retain=$(jq -r .retain <<<"$data")
				log "收到消息 id=$msg_id topic=$topic qos=$qos retain=$retain payload=$payload"
				if [ "$qos" = "1" ]; then
					ack "$msg_id" && log "已 ACK: $msg_id" || log "ACK 失败: $msg_id"
				fi
				;;
		esac
	done
}

consume &
STREAM_PID=$!
log "推送流已打开,等待消息…(Ctrl+C 退出)"

# ---------- 4. 可选自测:发布演示消息(client.http 请求 6,通常由生产方调用) ----------
if [ "$SELF_TEST" = "1" ]; then
	(
		sleep 1 # 等流建立
		curl -sS -o /dev/null -X POST "$HOST/publish" -H 'Content-Type: application/json' \
			-d '{"topic":"sensors/temp","payload":"21.5","qos":1}'
		curl -sS -o /dev/null -X POST "$HOST/publish" -H 'Content-Type: application/json' \
			-d '{"topic":"alerts/temp","payload":"too hot","qos":0}'
	) &
fi

wait "$STREAM_PID"
