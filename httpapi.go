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
	ClientID        string `json:"client_id"`         // 业务侧客户端标识;缺省由服务端生成
	CleanStart      *bool  `json:"clean_start"`       // 默认 true;false 时复用同 client_id 会话的服务端订阅表
	Will            *Will  `json:"will"`              // 遗嘱消息,可选
	WillGracePeriod string `json:"will_grace_period"` // 会话级遗嘱宽限期,如 "30s";缺省用 Broker 预定义设置
}

type createSessionResponse struct {
	SessionID  string `json:"session_id"` // 后续所有会话级 API 的路径参数
	ClientID   string `json:"client_id"`
	CleanStart bool   `json:"clean_start"`
	Resumed    bool   `json:"resumed"` // true 表示复用了既有会话的订阅表
}

func (b *Broker) handleCreateSession(c *gin.Context) {
	var req createSessionRequest
	if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, io.EOF) {
		abort(c, http.StatusBadRequest, err)
		return
	}
	cleanStart := true
	if req.CleanStart != nil {
		cleanStart = *req.CleanStart
	}
	grace, err := parseGracePeriod(req.WillGracePeriod)
	if err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	info, resumed, err := b.CreateSession(req.ClientID, cleanStart, req.Will, grace)
	if err != nil {
		abort(c, http.StatusBadRequest, err)
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
	if err := b.SetSubscriptions(c.Param("id"), req.Subscriptions); err != nil {
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
// 查询参数 will_grace_period(如 "30s")在本次 dial 覆盖会话级遗嘱宽限期。
func (b *Broker) handleStream(c *gin.Context) {
	id := c.Param("id")
	grace, err := parseGracePeriod(c.Query("will_grace_period"))
	if err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	ch, err := b.OpenStream(id, grace)
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
			fmt.Fprintf(w, "id: %s\nevent: message\ndata: %s\n\n", ev.MessageID, data)
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
	Delivered int    `json:"delivered"` // 实际投递的会话数(含共享组每组一份)
}

func (b *Broker) handlePublish(c *gin.Context) {
	var msg Message
	if err := c.ShouldBindJSON(&msg); err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	id, delivered, err := b.Publish(msg)
	if err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, publishResponse{MessageID: id, Delivered: delivered})
}

func abort(c *gin.Context, status int, err error) {
	c.AbortWithStatusJSON(status, gin.H{"error": err.Error()})
}

// parseGracePeriod 解析遗嘱宽限期字符串(如 "30s");空串表示未定义(返回 0)。
func parseGracePeriod(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid will_grace_period %q", s)
	}
	return d, nil
}

func statusOf(err error) int {
	switch {
	case errors.Is(err, ErrSessionNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrStreamNotOpen):
		return http.StatusConflict
	default:
		return http.StatusBadRequest
	}
}
