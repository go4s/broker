// 最小可运行示例:把 broker 挂载到 gin,监听 127.0.0.1:8080。
// 接口契约见仓库根目录 client.http。
package main

import (
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go4s/broker"
)

func main() {
	b := broker.New(
		broker.WithRedeliverInterval(5*time.Second),
		broker.WithWillGracePeriod(30*time.Second),
		broker.WithStreamBuffer(1),
	)
	defer b.Close()

	r := gin.Default()
	b.Mount(r) // 或 b.Mount(r.Group("/broker")) 加前缀

	if err := r.Run(); err != nil {
		log.Fatal(err)
	}
}
