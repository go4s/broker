// 任务进度示例:浏览器页面进入即自动订阅 tasks/#,点击"启动任务"后后台异步执行,
// broker 经 SSE 推送增量进度(qos0)与关键节点消息(qos1,前端回 ACK)以及完成终态(retain)。
// 运行:go run ./example/tasks → http://127.0.0.1:8080
package main

import (
	"embed"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go4s/broker"
)

//go:embed index.html
var staticFS embed.FS

func main() {
	b := broker.New(
		broker.WithRedeliverInterval(5*time.Second), // qos1 未 ACK 的重投间隔
		broker.WithWillGracePeriod(30*time.Second),
		broker.WithStreamBuffer(16),
	)
	defer b.Close()

	man := newTaskManager(b)

	r := gin.Default()
	b.Mount(r) // /sessions, /publish, ... (契约见根目录 client.http)

	// 极简前端页面
	r.GET("/", func(c *gin.Context) {
		html, err := staticFS.ReadFile("index.html")
		if err != nil {
			c.String(500, "index.html missing")
			return
		}
		c.Data(200, "text/html; charset=utf-8", html)
	})
	// 当前执行中的任务回显
	r.GET("/tasks/current", func(c *gin.Context) {
		if cur := man.Current(); cur != nil {
			c.JSON(200, cur)
			return
		}
		c.JSON(200, gin.H{})
	})
	// 启动任务:立即返回 task_id,后台异步执行
	r.POST("/tasks", func(c *gin.Context) {
		var req struct {
			Name string `json:"name"`
		}
		_ = c.ShouldBindJSON(&req)
		id := man.Start(req.Name)
		c.JSON(201, gin.H{"task_id": id})
	})

	if err := r.Run(); err != nil {
		log.Fatal(err)
	}
}
