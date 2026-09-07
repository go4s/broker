package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// IdentityKey 默认把主体身份注入 gin.Context 的键。
// 接入方可在中间件里调用 SetIdentity(c, id) 写入;启用 ACL 时本包读取。
const IdentityKey = "broker.identity"

// SetIdentity 把身份写入 gin.Context,供本包读取。通常在接入方的鉴权中间件里调用。
func SetIdentity(c *gin.Context, id Identity) {
	c.Set(IdentityKey, id)
}

// identityOf 从 gin.Context 读取身份;未注入或 ID 为空时返回 false。
func identityOf(c *gin.Context) (Identity, bool) {
	v, ok := c.Get(IdentityKey)
	if !ok {
		return Identity{}, false
	}
	id, ok := v.(Identity)
	return id, ok && id.ID != ""
}

// Mount 把全部功能 API 注册到 gin 路由上,可传入 router 或 group:
//
//	b := broker.New()
//	b.Mount(r)                    // /sessions, /publish, ...
//	b.Mount(r.Group("/broker"))   // /broker/sessions, /broker/publish, ...
func (b *Broker) Mount(ir gin.IRouter) {
	ir.POST("/sessions", b.handleCreateSession)
	ir.GET("/sessions", b.handleListSessions)
	ir.GET("/sessions/:id", b.handleGetSession)
	ir.DELETE("/sessions/:id", b.handleDeleteSession)
	ir.PUT("/sessions/:id/subscriptions", b.handlePutSubscriptions)
	ir.GET("/sessions/:id/subscriptions", b.handleGetSubscriptions)
	ir.DELETE("/sessions/:id/subscriptions", b.handleDeleteSubscriptions)
	ir.GET("/sessions/:id/stream", b.handleStream)
	ir.DELETE("/sessions/:id/stream", b.handleCloseStream)
	ir.POST("/sessions/:id/acks", b.handleAck)
	ir.POST("/publish", b.handlePublish)
}

type createSessionRequest struct {
	ClientID          string `json:"client_id"`          // 业务侧客户端标识;缺省由服务端生成
	CleanStart        *bool  `json:"clean_start"`        // 默认 true;false 时复用同 client_id 会话的服务端订阅表
	Will              *Will  `json:"will"`               // 遗嘱消息,可选
	WillGracePeriod   string `json:"will_grace_period"`  // 会话级遗嘱宽限期,如 "30s";缺省用 Broker 预定义设置
	HeartbeatInterval string `json:"heartbeat_interval"` // 会话级 SSE 心跳间隔,如 "3m";缺省用会话级缺省值
}

type createSessionResponse struct {
	SessionID  string `json:"session_id"` // 后续所有会话级 API 的路径参数
	ClientID   string `json:"client_id"`
	CleanStart bool   `json:"clean_start"`
	Resumed    bool   `json:"resumed"` // true 表示复用了既有会话的订阅表
}

func (b *Broker) handleCreateSession(c *gin.Context) {
	limitBody(c)
	var req createSessionRequest
	if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, io.EOF) {
		abortBind(c, err)
		return
	}
	cleanStart := true
	if req.CleanStart != nil {
		cleanStart = *req.CleanStart
	}
	grace, err := parseDuration("will_grace_period", req.WillGracePeriod)
	if err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	heartbeat, err := parseDuration("heartbeat_interval", req.HeartbeatInterval)
	if err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	info, resumed, err := b.CreateSession(req.ClientID, cleanStart, req.Will, grace, heartbeat)
	if err != nil {
		abort(c, statusOf(err), err)
		return
	}
	c.JSON(http.StatusCreated, createSessionResponse{
		SessionID:  info.ID,
		ClientID:   info.ClientID,
		CleanStart: info.CleanStart,
		Resumed:    resumed,
	})
}

func (b *Broker) handleListSessions(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"sessions": b.ListSessions()})
}

func (b *Broker) handleGetSession(c *gin.Context) {
	info, err := b.GetSession(c.Param("id"))
	if err != nil {
		abort(c, statusOf(err), err)
		return
	}
	c.JSON(http.StatusOK, info)
}

func (b *Broker) handleDeleteSession(c *gin.Context) {
	if err := b.CloseSession(c.Param("id")); err != nil {
		abort(c, statusOf(err), err)
		return
	}
	c.Status(http.StatusNoContent)
}

type putSubscriptionsRequest struct {
	Subscriptions []Subscription `json:"subscriptions"` // 按 filter upsert,立即生效
}

func (b *Broker) handlePutSubscriptions(c *gin.Context) {
	var req putSubscriptionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	if len(req.Subscriptions) == 0 {
		abort(c, http.StatusBadRequest, errors.New("subscriptions must not be empty"))
		return
	}
	identity, ok := b.identityOr401(c)
	if !ok {
		return
	}
	if err := b.SetSubscriptions(c.Param("id"), req.Subscriptions, identity); err != nil {
		abort(c, statusOf(err), err)
		return
	}
	subs, _ := b.GetSubscriptions(c.Param("id"))
	c.JSON(http.StatusOK, gin.H{"subscriptions": subs})
}

func (b *Broker) handleGetSubscriptions(c *gin.Context) {
	subs, err := b.GetSubscriptions(c.Param("id"))
	if err != nil {
		abort(c, statusOf(err), err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"subscriptions": subs})
}

type deleteSubscriptionsRequest struct {
	Filters []string `json:"filters"` // 要删除的过滤器,立即生效
}

func (b *Broker) handleDeleteSubscriptions(c *gin.Context) {
	var req deleteSubscriptionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	if err := b.DeleteSubscriptions(c.Param("id"), req.Filters); err != nil {
		abort(c, statusOf(err), err)
		return
	}
	c.Status(http.StatusNoContent)
}

// handleStream 打开 SSE 推送流:订阅表需先通过 HTTP 维护;断开即视为异常离线。
// 查询参数 will_grace_period / heartbeat_interval(如 "30s"/"5s")在本次 dial 覆盖会话级设置。
// 按生效心跳间隔周期推送 `: ping` 注释行;任何写失败都视为连接已死,立即触发离线判定。
func (b *Broker) handleStream(c *gin.Context) {
	id := c.Param("id")
	grace, err := parseDuration("will_grace_period", c.Query("will_grace_period"))
	if err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	heartbeat, err := parseDuration("heartbeat_interval", c.Query("heartbeat_interval"))
	if err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	ch, err := b.OpenStream(id, grace, heartbeat)
	if err != nil {
		abort(c, statusOf(err), err)
		return
	}
	defer b.DetachStream(id, ch)

	w := c.Writer
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	w.Flush()

	ticker := time.NewTicker(b.heartbeatOf(id))
	defer ticker.Stop()
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return // 流被顶掉或被主动关闭
			}
			data, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "id: %s\nevent: message\ndata: %s\n\n", ev.MessageID, data); err != nil {
				return // 写失败:连接已死
			}
			w.Flush()
		case <-ticker.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return // 心跳写失败:连接已死,触发离线判定
			}
			w.Flush()
		case <-c.Request.Context().Done():
			return // 客户端断开
		}
	}
}

func (b *Broker) handleCloseStream(c *gin.Context) {
	if err := b.CloseStream(c.Param("id")); err != nil {
		abort(c, statusOf(err), err)
		return
	}
	c.Status(http.StatusNoContent)
}

type ackRequest struct {
	MessageID string `json:"message_id"` // SSE 事件 id 字段
}

func (b *Broker) handleAck(c *gin.Context) {
	var req ackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	if err := b.Ack(c.Param("id"), req.MessageID); err != nil {
		abort(c, statusOf(err), err)
		return
	}
	c.Status(http.StatusNoContent)
}

type publishResponse struct {
	MessageID string `json:"message_id"`
	Delivered int    `json:"delivered"` // 实际投递的会话数
}

func (b *Broker) handlePublish(c *gin.Context) {
	limitBody(c)
	var msg Message
	if err := c.ShouldBindJSON(&msg); err != nil {
		abortBind(c, err)
		return
	}
	identity, ok := b.identityOr401(c)
	if !ok {
		return
	}
	id, delivered, err := b.Publish(msg, identity)
	if err != nil {
		abort(c, statusOf(err), err)
		return
	}
	c.JSON(http.StatusOK, publishResponse{MessageID: id, Delivered: delivered})
}

func abort(c *gin.Context, status int, err error) {
	c.AbortWithStatusJSON(status, gin.H{"error": err.Error()})
}

// maxRequestBytes 携带 payload 的请求体上限,超出返回 413。
const maxRequestBytes = 1024

// limitBody 限制请求体大小;超限时 JSON 绑定会返回 *http.MaxBytesError。
func limitBody(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxRequestBytes)
}

// abortBind 按绑定错误类型返回状态码:请求体超限 413,其余 400。
func abortBind(c *gin.Context, err error) {
	var mbErr *http.MaxBytesError
	if errors.As(err, &mbErr) {
		abort(c, http.StatusRequestEntityTooLarge, mbErr)
		return
	}
	abort(c, http.StatusBadRequest, err)
}

// parseDuration 解析时长字符串(如 "30s");空串表示未定义(返回 0)。
func parseDuration(name, s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid %s %q", name, s)
	}
	return d, nil
}

// identityOr401 读取身份;启用 ACL 但未注入身份时返回 401。
// 未启用 ACL(未配置 Authorizer)时放行,身份可空。
func (b *Broker) identityOr401(c *gin.Context) (Identity, bool) {
	if b.authorizer == nil {
		return Identity{}, true
	}
	id, ok := identityOf(c)
	if !ok {
		abort(c, http.StatusUnauthorized, ErrUnauthorized)
		return Identity{}, false
	}
	return id, true
}

func statusOf(err error) int {
	switch {
	case errors.Is(err, ErrUnauthorized):
		return http.StatusUnauthorized
	case errors.Is(err, ErrForbidden):
		return http.StatusForbidden
	case errors.Is(err, ErrSessionNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrStreamNotOpen):
		return http.StatusConflict
	case errors.Is(err, ErrTooManySessions):
		return http.StatusTooManyRequests
	default:
		return http.StatusBadRequest
	}
}
