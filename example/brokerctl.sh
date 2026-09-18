#!/usr/bin/env bash
# brokerctl.sh — broker 命令行游玩工具
#
# 覆盖 broker 的完整交互:建会话 / 订阅 / 发布 / 打开 SSE 消费(可自动 ACK)/ 查看 / 断开。
#
# 用法:
#   ./example/brokerctl.sh <command> [args...]
#   ./example/brokerctl.sh                 # 进入交互模式(REPL)
#
# 命令:
#   new [client_id]              建会话并记为当前会话
#   use <session_id>             切换当前会话
#   status                       显示当前会话与环境
#   ls                           列出全部会话
#   info                         查看当前会话快照
#   sub <filter> [qos]           新增/更新订阅(流存活期间立即生效)
#   subs                         查看当前订阅表
#   unsub <filter>...            删除订阅
#   pub <topic> <payload> [qos] [retain]   发布消息
#   ack <message_id>             确认 QoS1 消息(批量:确认 N 即 N 及之前全部)
#   stream [--no-ack]            前台消费 SSE(默认对 qos1 自动 ACK);Ctrl+C 退出
#   drain <seconds>              消费指定秒数后退出(便于脚本/自动化)
#   close                        正常断开会话(不触发遗嘱)
#   demo                         一键跑通:建会话→订阅→消费→发布(qos0/qos1)
#   help                         显示本帮助
#
# 环境变量:
#   HOST     服务地址,默认 http://127.0.0.1:8080
#   PREFIX   路由前缀,默认空;部署镜像(路由挂在 /sse)时设为 /sse
#   STATE    会话状态文件,默认 ${TMPDIR:-/tmp}/brokerctl.<user>.state
#
# 例:
#   ./example/brokerctl.sh demo
#   HOST=http://127.0.0.1:8429 PREFIX=/sse ./example/brokerctl.sh demo   # 打已部署镜像
#
# 依赖: curl、jq。

set -euo pipefail

HOST="${HOST:-http://127.0.0.1:8080}"
PREFIX="${PREFIX:-}"
case "$PREFIX" in "" | /*) ;; *) PREFIX="/$PREFIX" ;; esac
BASE="${HOST%/}${PREFIX}"
STATE="${STATE:-${TMPDIR:-/tmp}/brokerctl.${USER:-$(id -un)}.state}"
SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

for cmd in curl jq; do
	command -v "$cmd" >/dev/null || {
		echo "缺少依赖: $cmd" >&2
		exit 1
	}
done

die() {
	echo "错误: $*" >&2
	return 1
}

save_sid() { printf '%s\n' "$1" >"$STATE"; }
load_sid() { [ -f "$STATE" ] && cat "$STATE" || true; }
need_sid() {
	local sid
	sid="$(load_sid)"
	if [ -z "$sid" ]; then
		die "还没有当前会话,先执行: $(basename "$0") new [client_id]"
		return 1
	fi
	printf '%s' "$sid"
}

# api <METHOD> <path> [json] —— 出错时把服务端 {"error":...} 打到 stderr。
api() {
	local method="$1" path="$2" data="${3:-}" resp
	local args=(-sS -X "$method")
	if [ -n "$data" ]; then
		args+=(-H 'Content-Type: application/json' -d "$data")
	fi
	if ! resp="$(curl "${args[@]}" "${BASE}${path}")"; then
		die "请求失败: ${method} ${BASE}${path}"
		return 1
	fi
	if jq -e '.error' >/dev/null 2>&1 <<<"$resp"; then
		echo "$resp" >&2
		return 1
	fi
	printf '%s' "$resp"
}

cmd_new() {
	local client_id="${1:-}" body resp sid
	if [ -n "$client_id" ]; then
		body="$(jq -nc --arg c "$client_id" '{client_id:$c, clean_start:true}')"
	else
		body='{"clean_start":true}'
	fi
	resp="$(api POST /sessions "$body")" || return 1
	sid="$(jq -r '.session_id // empty' <<<"$resp")"
	[ -n "$sid" ] || die "响应缺少 session_id: $resp"
	save_sid "$sid"
	printf '会话已创建: %s (client_id=%s)\n' "$sid" "${client_id:-<自动>}"
}

cmd_use() {
	[ -n "${1:-}" ] || die "用法: use <session_id>"
	save_sid "$1"
	printf '当前会话: %s\n' "$1"
}

cmd_status() {
	printf 'BASE  = %s\n' "$BASE"
	printf 'STATE = %s\n' "$STATE"
	printf '会话  = %s\n' "$(load_sid || true)"
}

cmd_ls() { api GET /sessions | jq .; }

cmd_info() {
	local sid
	sid="$(need_sid)" || return 1
	api GET "/sessions/$sid" | jq .
}

cmd_sub() {
	local filter="${1:-}" qos="${2:-0}" sid
	[ -n "$filter" ] || die "用法: sub <filter> [qos]"
	sid="$(need_sid)" || return 1
	api PUT "/sessions/$sid/subscriptions" \
		"$(jq -nc --arg f "$filter" --argjson q "$qos" '{subscriptions:[{filter:$f,qos:$q}]}')" |
		jq -c '.subscriptions'
}

cmd_subs() {
	local sid
	sid="$(need_sid)" || return 1
	api GET "/sessions/$sid/subscriptions" | jq -c '.subscriptions'
}

cmd_unsub() {
	local sid filters_json
	[ "$#" -gt 0 ] || die "用法: unsub <filter>..."
	sid="$(need_sid)" || return 1
	filters_json="$(jq -nc --args '{filters:$ARGS.positional}' -- "$@")"
	api DELETE "/sessions/$sid/subscriptions" "$filters_json" >/dev/null
	printf '已删除订阅: %s\n' "$*"
}

cmd_pub() {
	local topic="${1:-}" payload="${2:-}" qos="${3:-0}" retain_raw="${4:-false}" retain body resp
	[ -n "$topic" ] || die "用法: pub <topic> <payload> [qos] [retain]"
	case "$retain_raw" in true | 1 | yes) retain=true ;; *) retain=false ;; esac
	body="$(jq -nc --arg t "$topic" --arg p "$payload" --argjson q "$qos" --argjson r "$retain" \
		'{topic:$t,payload:$p,qos:$q,retain:$r}')"
	resp="$(api POST /publish "$body")" || return 1
	printf '已发布 id=%s delivered=%s\n' "$(jq -r .message_id <<<"$resp")" "$(jq -r .delivered <<<"$resp")"
}

cmd_ack() {
	local mid="${1:-}" sid
	[ -n "$mid" ] || die "用法: ack <message_id>"
	sid="$(need_sid)" || return 1
	api POST "/sessions/$sid/acks" "$(jq -nc --arg m "$mid" '{message_id:$m}')" >/dev/null
	printf '已 ACK %s\n' "$mid"
}

cmd_stream() {
	local auto_ack=1
	[ "${1:-}" = "--no-ack" ] && auto_ack=0
	local sid
	sid="$(need_sid)" || return 1
	printf '打开 SSE 流(会话 %s,自动ACK=%s),Ctrl+C 退出…\n' "$sid" "$auto_ack" >&2
	local mid="" line data topic payload qos retain
	curl -sS -N -H 'Accept: text/event-stream' "${BASE}/sessions/$sid/stream" |
		while IFS= read -r line; do
			line=${line%$'\r'}
			case "$line" in
				": "*) ;; # 心跳 ": ping",忽略
				"id: "*) mid=${line#id: } ;;
				"data: "*)
					data=${line#data: }
					topic=$(jq -r .topic <<<"$data")
					payload=$(jq -r .payload <<<"$data")
					qos=$(jq -r .qos <<<"$data")
					retain=$(jq -r .retain <<<"$data")
					printf '◀ id=%s topic=%s qos=%s retain=%s payload=%s\n' \
						"$mid" "$topic" "$qos" "$retain" "$payload"
					if [ "$qos" = "1" ] && [ "$auto_ack" = 1 ]; then
						curl -sS -o /dev/null -X POST -H 'Content-Type: application/json' \
							"${BASE}/sessions/$sid/acks" \
							-d "$(jq -nc --arg m "$mid" '{message_id:$m}')" &&
							printf '  ↳ 已 ACK %s\n' "$mid" >&2
					fi
					;;
			esac
		done
}

cmd_drain() {
	local secs="${1:-5}"
	timeout "$secs" "$SELF" stream --no-ack || true
}

cmd_close() {
	local sid
	sid="$(need_sid)" || return 1
	api DELETE "/sessions/$sid" >/dev/null
	: >"$STATE"
	printf '会话已断开: %s\n' "$sid"
}

cmd_demo() {
	printf '== demo:建会话 → 订阅 demo/# → 消费 → 发布 ==\n'
	cmd_new "demo-$RANDOM"
	cmd_sub 'demo/#' 1
	timeout 5 "$SELF" stream &
	local spid=$!
	sleep 1
	cmd_pub demo/hello world 0
	cmd_pub demo/temp 21.5 1
	cmd_pub demo/temp 22.0 1 retain
	local sid
	sid="$(need_sid)" || return 1
	wait "$spid" 2>/dev/null || true
	printf '\n== demo 结束;会话 %s 保留,可继续 pub/stream,清理执行: %s close\n' "$sid" "$(basename "$0")"
}

cmd_help() {
	cat <<'EOF'
brokerctl.sh — broker 命令行游玩工具

用法:
  ./example/brokerctl.sh <command> [args...]
  ./example/brokerctl.sh                 # 进入交互模式(REPL)

命令:
  new [client_id]              建会话并记为当前会话
  use <session_id>             切换当前会话
  status                       显示当前会话与环境
  ls                           列出全部会话
  info                         查看当前会话快照
  sub <filter> [qos]           新增/更新订阅(流存活期间立即生效)
  subs                         查看当前订阅表
  unsub <filter>...            删除订阅
  pub <topic> <payload> [qos] [retain]   发布消息
  ack <message_id>             确认 QoS1 消息(批量:确认 N 即 N 及之前全部)
  stream [--no-ack]            前台消费 SSE(默认对 qos1 自动 ACK);Ctrl+C 退出
  drain <seconds>              消费指定秒数后退出
  close                        正常断开会话(不触发遗嘱)
  demo                         一键跑通:建会话→订阅→消费→发布(qos0/qos1)
  help                         显示本帮助

环境变量:
  HOST     服务地址,默认 http://127.0.0.1:8080
  PREFIX   路由前缀,默认空;部署镜像(路由挂在 /sse)时设为 /sse
  STATE    会话状态文件,默认 ${TMPDIR:-/tmp}/brokerctl.<user>.state
EOF
}

dispatch() {
	local cmd="${1:-interactive}"
	shift || true
	case "$cmd" in
		new) cmd_new "$@" ;;
		use) cmd_use "$@" ;;
		status | env) cmd_status "$@" ;;
		ls | sessions) cmd_ls "$@" ;;
		info) cmd_info "$@" ;;
		sub | subscribe) cmd_sub "$@" ;;
		subs | subscriptions) cmd_subs "$@" ;;
		unsub) cmd_unsub "$@" ;;
		pub | publish) cmd_pub "$@" ;;
		ack) cmd_ack "$@" ;;
		stream | watch) cmd_stream "$@" ;;
		drain) cmd_drain "$@" ;;
		close | rm) cmd_close "$@" ;;
		demo) cmd_demo "$@" ;;
		help | -h | --help) cmd_help ;;
		interactive | repl | "") cmd_interactive ;;
		*) die "未知命令: $cmd(用 help 查看)" ;;
	esac
}

cmd_interactive() {
	printf 'brokerctl 交互模式 — BASE=%s;输入 help 查看命令,Ctrl-D 退出。\n' "$BASE"
	while true; do
		local line
		read -r -p "broker> " line || break
		[ -z "${line// }" ] && continue
		local args
		read -r -a args <<<"$line"
		dispatch "${args[@]}" || true
	done
}

dispatch "$@"
