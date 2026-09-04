package broker

import (
	"errors"
	"fmt"
	"strings"
)

var (
	errEmptyFilter = errors.New("topic filter is empty")
	errEmptyTopic  = errors.New("topic is empty")
)

// ValidateFilter 校验订阅过滤器:支持 + (单级)与 # (多级,必须在末尾)通配符。
// 不支持共享订阅,$share/ 前缀一律拒绝。
func ValidateFilter(filter string) error {
	if strings.HasPrefix(filter, "$share/") {
		return fmt.Errorf("shared subscription %q is not supported", filter)
	}
	return validateFilterLevels(filter)
}

// ValidateTopic 校验发布主题:不允许包含通配符。
func ValidateTopic(topic string) error {
	if topic == "" {
		return errEmptyTopic
	}
	if strings.ContainsAny(topic, "+#") {
		return fmt.Errorf("publish topic %q must not contain wildcards", topic)
	}
	return validateLevels(topic)
}

func validateFilterLevels(filter string) error {
	if filter == "" {
		return errEmptyFilter
	}
	levels := strings.Split(filter, "/")
	for i, lv := range levels {
		if lv == "" {
			return fmt.Errorf("topic filter %q contains empty level", filter)
		}
		if strings.Contains(lv, "#") {
			if lv != "#" {
				return fmt.Errorf("topic filter %q: # must occupy an entire level", filter)
			}
			if i != len(levels)-1 {
				return fmt.Errorf("topic filter %q: # must be the last level", filter)
			}
		}
		if strings.Contains(lv, "+") && lv != "+" {
			return fmt.Errorf("topic filter %q: + must occupy an entire level", filter)
		}
	}
	return nil
}

func validateLevels(topic string) error {
	for _, lv := range strings.Split(topic, "/") {
		if lv == "" {
			return fmt.Errorf("topic %q contains empty level", topic)
		}
	}
	return nil
}

// Match 判断过滤器是否匹配主题。
// # 匹配其所在层级及之后所有层级(含零层,+ 恰好匹配一层)。
func Match(filter, topic string) bool {
	f := strings.Split(filter, "/")
	t := strings.Split(topic, "/")
	for i, lv := range f {
		if lv == "#" {
			return true
		}
		if i >= len(t) {
			return false
		}
		if lv != "+" && lv != t[i] {
			return false
		}
	}
	return len(f) == len(t)
}

// routeSub 路由树终端节点上的一条订阅记录。
type routeSub struct {
	sess *Session
	qos  int
}

// routeNode 主题路由树节点:按主题层级组织,子节点键为层级名、+ 或 #;
// 终端节点的 subs 保存该过滤器路径上的订阅,键为 sessionID+filter。
type routeNode struct {
	children map[string]*routeNode
	subs     map[string]routeSub
}

func newRouteNode() *routeNode {
	return &routeNode{
		children: make(map[string]*routeNode),
		subs:     make(map[string]routeSub),
	}
}

// routeKey 返回路由树中一条订阅记录的唯一键。
func routeKey(sessionID, filter string) string {
	return sessionID + "\x00" + filter
}

// routeLevels 返回过滤器参与路由的层级。
func routeLevels(filter string) []string {
	return strings.Split(filter, "/")
}

// add 把订阅插入过滤器路径的终端节点。
func (n *routeNode) add(filter string, sub routeSub) {
	cur := n
	for _, lv := range routeLevels(filter) {
		child, ok := cur.children[lv]
		if !ok {
			child = newRouteNode()
			cur.children[lv] = child
		}
		cur = child
	}
	cur.subs[routeKey(sub.sess.ID, filter)] = sub
}

// remove 删除过滤器路径终端节点上的订阅记录,并回收路径上的空节点。
func (n *routeNode) remove(filter, sessionID string) {
	n.removeLevels(routeLevels(filter), routeKey(sessionID, filter))
}

// removeLevels 返回 true 表示本节点已空,可由父节点回收。
func (n *routeNode) removeLevels(levels []string, key string) bool {
	if len(levels) == 0 {
		delete(n.subs, key)
		return len(n.subs) == 0 && len(n.children) == 0
	}
	child, ok := n.children[levels[0]]
	if !ok {
		return false
	}
	if child.removeLevels(levels[1:], key) {
		delete(n.children, levels[0])
	}
	return len(n.subs) == 0 && len(n.children) == 0
}

// match 收集匹配 topic 的全部订阅记录,语义与 Match 一致:
// # 匹配所在层级及之后所有层级(含零层),+ 恰好匹配一层。
func (n *routeNode) match(topic string) []routeSub {
	var out []routeSub
	n.matchLevels(strings.Split(topic, "/"), &out)
	return out
}

func (n *routeNode) matchLevels(levels []string, out *[]routeSub) {
	if hash, ok := n.children["#"]; ok {
		for _, sub := range hash.subs {
			*out = append(*out, sub)
		}
	}
	if len(levels) == 0 {
		for _, sub := range n.subs {
			*out = append(*out, sub)
		}
		return
	}
	if child, ok := n.children[levels[0]]; ok {
		child.matchLevels(levels[1:], out)
	}
	if plus, ok := n.children["+"]; ok {
		plus.matchLevels(levels[1:], out)
	}
}
