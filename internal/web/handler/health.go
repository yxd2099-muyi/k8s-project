package handler

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

type HealthHandler struct{}

func NewHealth() *HealthHandler {
	return &HealthHandler{}
}

// 启动探针
func (h *HealthHandler) Startup(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "startup ok"})
}

// 存活探针
func (h *HealthHandler) Liveness(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "alive"})
}

// 就绪探针（可扩展校验mysql/redis连通性）
func (h *HealthHandler) Readiness(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "ready"})
}
