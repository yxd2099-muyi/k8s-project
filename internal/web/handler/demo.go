package handler

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/infra/tracex"
	"github.com/k8s/muyi/shared/infra/xctx"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type DemoHandler struct{}

func NewDemo() *DemoHandler {
	return &DemoHandler{}
}

func (d *DemoHandler) Hello(c *gin.Context) {
	clog := logger.L
	clog.Debug("hello demo")
	clog.Info("hello demo info")
	//g := pb_push.SyncUserInfoResp{}
	//u := pb_web.SyncUserInfoReq{}
	//fmt.Println(g)
	fmt.Println("")
	c.JSON(200, gin.H{"msg": "k8s-web demo ok"})
}
func WithContext(ctx context.Context, l *zap.Logger) *zap.Logger {
	if ctx == nil {
		return l
	}
	spanCtx := trace.SpanContextFromContext(ctx)
	if !spanCtx.IsValid() {
		return l
	}
	return l.With(
		zap.String("trace_id", spanCtx.TraceID().String()),
		zap.String("span_id", spanCtx.SpanID().String()),
	)
}
func (d *DemoHandler) OTel(c *gin.Context) {
	clog := logger.L
	ctx := c.Request.Context()
	//tracer := otel.Tracer("web-service")
	//ctx, span := tracer.Start(ctx, "web-1-handler")
	//defer span.End()
	//WithContext(ctx, clog).Info("收到 web 请求",
	//	zap.String("method", c.Request.Method),
	//	zap.String("path", c.Request.URL.Path),
	//	zap.String("client_ip", c.Request.RemoteAddr),
	//	zap.Any("first_log", "hello world web666"),
	//)
	ctx, span := tracex.GetSpan(ctx, "OTel1")
	defer span.End()
	xctx.ContextWithTrace(ctx, clog).Info("this test progress")
	c.JSON(200, gin.H{"msg": "otel demo ok", "trace_id": span.SpanContext().TraceID().String()})
}
