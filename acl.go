package broker

import (
	"errors"
)

// Identity 一个经过鉴权的主体(用户/应用/设备),由接入方从 gin 中间件解析注入。
// Broker 只依据它做授权判断,不关心其来源(Header/JWT/Cookie)。
type Identity struct {
	ID    string   `json:"id"`    // 主体唯一标识,如用户 ID
	Roles []string `json:"roles"` // 主体角色,供授权规则匹配;可空
}

// 鉴权失败的两类错误,映射到 HTTP:401 未认证、403 已认证但被拒。
var (
	// ErrUnauthorized 请求缺少可识别身份(中间件未注入 identity,或 identity 为空)。
	ErrUnauthorized = errors.New("unauthorized")
	// ErrForbidden 身份已被识别但无权限执行该操作。
	ErrForbidden = errors.New("forbidden")
)

// Authorizer 订阅/发布的授权接口,接入方自行实现(如查库、按角色、按 ACL 表)。
// 返回 nil 表示放行;返回 ErrUnauthorized / ErrForbidden(或任意错误)表示拒绝,
// 错误将被映射为对应的 HTTP 状态码。
type Authorizer interface {
	// AuthorizePublish 判定 identity 是否可向 topic 发布。topic 不含通配符。
	AuthorizePublish(identity Identity, topic string) error
	// AuthorizeSubscribe 判定 identity 是否可订阅 filter。filter 支持 + # 通配符。
	AuthorizeSubscribe(identity Identity, filter string) error
}

// Action 授权规则的资源操作类型。
type Action int8

const (
	// ActionPublish 发布操作。
	ActionPublish Action = 1 << iota
	// ActionSubscribe 订阅操作。
	ActionSubscribe
	// ActionAny 发布或订阅均生效(默认)。
	ActionAny = ActionPublish | ActionSubscribe
)

// Rule 一条授权规则:身份、资源、操作均匹配时按 Allow 放行或拒绝。
//
//	效果为拒绝 + 未匹配任何规则 → 默认拒绝(default-deny)。
type Rule struct {
	Allow    bool   // true=allow,false=deny
	Action   Action // 命中此操作的规则;0 视为 ActionAny
	Pattern  string // 资源(topic / filter),支持 + # 通配符;空串匹配任意
	Identity string // 身份匹配模式,支持 + # 通配符;空串匹配任意身份
}

// RuleAuthorizer 基于规则的授权器,默认拒绝(default-deny):
// 按声明顺序取第一条 身份+资源+操作 均匹配的规则,按 Allow 放行或拒绝;
// 无匹配或首条命中为拒绝则拒绝。用于无需接入方写代码的内置方案。
type RuleAuthorizer struct {
	rules []Rule
}

// NewRuleAuthorizer 用给定规则构造授权器(默认拒绝)。规则顺序即优先级。
func NewRuleAuthorizer(rules ...Rule) *RuleAuthorizer {
	return &RuleAuthorizer{rules: rules}
}

func (r *RuleAuthorizer) AuthorizePublish(identity Identity, topic string) error {
	return r.authorize(identity, ActionPublish, topic)
}

func (r *RuleAuthorizer) AuthorizeSubscribe(identity Identity, filter string) error {
	return r.authorize(identity, ActionSubscribe, filter)
}

func (r *RuleAuthorizer) authorize(identity Identity, action Action, resource string) error {
	for _, rule := range r.rules {
		if rule.Action != 0 && rule.Action&action == 0 {
			continue
		}
		if !identityMatches(rule.Identity, identity) {
			continue
		}
		if rule.Pattern != "" && !Match(rule.Pattern, resource) {
			continue
		}
		if rule.Allow {
			return nil
		}
		return ErrForbidden
	}
	return ErrForbidden
}

// identityMatches 判定 ruleIdentity 是否匹配 identity.ID。
// 空串匹配任意身份,否则需完全相等。身份是平铺 ID(非主题层级),因此只用精确匹配,
// 需要"某命名空间只允许对应身份"时请用 Pattern 通配符表达资源,身份用精确值。
func identityMatches(ruleIdentity string, identity Identity) bool {
	if ruleIdentity == "" {
		return true
	}
	return ruleIdentity == identity.ID
}

// AllowAllAuthorizer 放行所有身份与操作。
type AllowAllAuthorizer struct{}

func (AllowAllAuthorizer) AuthorizePublish(Identity, string) error   { return nil }
func (AllowAllAuthorizer) AuthorizeSubscribe(Identity, string) error { return nil }

// DenyAllAuthorizer 拒绝所有身份与操作。
type DenyAllAuthorizer struct{}

func (DenyAllAuthorizer) AuthorizePublish(Identity, string) error   { return ErrForbidden }
func (DenyAllAuthorizer) AuthorizeSubscribe(Identity, string) error { return ErrForbidden }

// authorizeError 汇总权限拒绝错误,供上层做 401/403 区分。
func isUnauthorized(err error) bool {
	return errors.Is(err, ErrUnauthorized)
}

func isForbidden(err error) bool {
	return errors.Is(err, ErrForbidden)
}
