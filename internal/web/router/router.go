package router

import (
	"github.com/gin-gonic/gin"
	"github.com/k8s/muyi/internal/web/handler"
)

func RegisterRoutes(r *gin.Engine) {
	// K8s探针路由
	health := handler.NewHealth()
	r.GET("/health/startup", health.Startup)
	r.GET("/health/liveness", health.Liveness)
	r.GET("/health/readiness", health.Readiness)
	// 业务接口
	demo := handler.NewDemo()
	api := r.Group("/api/v1")
	{
		api.GET("/hello", demo.Hello)
	}
}
