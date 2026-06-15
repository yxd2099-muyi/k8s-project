package handler

import "github.com/gin-gonic/gin"

type DemoHandler struct{}

func NewDemo() *DemoHandler {
	return &DemoHandler{}
}

func (d *DemoHandler) Hello(c *gin.Context) {
	c.JSON(200, gin.H{"msg": "k8s-web demo ok"})
}
