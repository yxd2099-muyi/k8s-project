package handler

import (
	"fmt"
	"github.com/gin-gonic/gin"
	pb_push "github.com/k8s/muyi/api/pb/push"
	pb_web "github.com/k8s/muyi/api/pb/web"
	"github.com/k8s/muyi/shared/infra/logger"
)

type DemoHandler struct{}

func NewDemo() *DemoHandler {
	return &DemoHandler{}
}

func (d *DemoHandler) Hello(c *gin.Context) {
	clog := logger.L
	clog.Debug("hello demo")
	clog.Info("hello demo info")
	g := pb_push.SyncUserInfoResp{}
	u := pb_web.SyncUserInfoReq{}
	fmt.Println(g)
	fmt.Println(u)
	c.JSON(200, gin.H{"msg": "k8s-web demo ok"})
}
