// 最小可运行示例:把 broker 挂载到 gin,监听 127.0.0.1:8080。
// 演示:
//   - 鉴权中间件:从 X-User / X-Roles 请求头解析身份,注入 broker.IdentityKey。
//   - 订阅/发布 ACL:基于规则,默认拒绝;仅放行 sensors/# 的发布与 sensors/+ 的订阅。
// 接口契约见仓库根目录 client.http。
package main

import (
	"log"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go4s/broker"
)

func main() {
	// 规则:默认拒绝;示例仅放行以下两类操作(Identity 为空表示任意身份)。
	az := broker.NewRuleAuthorizer([]broker.Rule{
		{Allow: true, Action: broker.ActionSubscribe, Pattern: "sensors/+"},
		{Allow: true, Action: broker.ActionSubscribe, Pattern: "alerts/#"},
		{Allow: true, Action: broker.ActionPublish, Pattern: "sensors/#"},
		{Allow: true, Action: broker.ActionPublish, Pattern: "alerts/+"},
	}...)

	b := broker.New(
		broker.WithAuthorizer(az),
		broker.WithRedeliverInterval(5*time.Second),
		broker.WithWillGracePeriod(30*time.Second),
		broker.WithStreamBuffer(1),
	)
	defer b.Close()

	r := gin.Default()
	// 鉴权中间件:从请求头解析身份并注入。生产环境替换为 JWT/Session 校验。
	r.Use(authMiddleware)
	b.Mount(r) // 或 b.Mount(r.Group("/broker")) 加前缀

	if err := r.Run(); err != nil {
		log.Fatal(err)
	}
}

// authMiddleware 解析 X-User(必填)/ X-Roles(逗号分隔)请求头为身份,注入 gin.Context。
// 未携带 X-User 视为未认证;本包在启用 ACL 时会对受保护操作返回 401。
func authMiddleware(c *gin.Context) {
	id := broker.Identity{ID: c.GetHeader("X-User")}
	for _, role := range strings.Split(c.GetHeader("X-Roles"), ",") {
		if role = strings.TrimSpace(role); role != "" {
			id.Roles = append(id.Roles, role)
		}
	}
	broker.SetIdentity(c, id)
	c.Next()
}

