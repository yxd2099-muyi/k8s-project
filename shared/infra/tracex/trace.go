package tracex

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var traceG trace.Tracer

// InitTrace 程序启动时候初始化
func InitTrace(name string) {
	traceG = otel.Tracer(name)
}

// GetSpan 获取Span
func GetSpan(ctx context.Context, spanName string) (context.Context, trace.Span) {
	ctx, span := traceG.Start(ctx, spanName)
	return ctx, span
}
