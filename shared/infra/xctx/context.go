package xctx

import (
	"context"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// ContextWithTrace 带上 相关的trace_id 数据
func ContextWithTrace(ctx context.Context, l *zap.Logger) *zap.Logger {
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
