package broker

import "errors"

// 会话与流操作的哨兵错误,HTTP 层经 statusOf 映射为对应状态码。
var (
	ErrSessionNotFound = errors.New("session not found")
	ErrStreamNotOpen   = errors.New("stream not open")
	ErrTooManySessions = errors.New("too many sessions")
)
