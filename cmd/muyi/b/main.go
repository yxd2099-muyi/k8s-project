package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	otelzapbridge "go.opentelemetry.io/contrib/bridges/otelzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// 依然使用 Go 社区标准的 *zap.Logger
var logger *zap.Logger

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

func initTelemetry() func() {
	ctx := context.Background()
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("demo-service22"),
			semconv.ServiceVersionKey.String("v1.0.0"),
		),
	)
	if err != nil {
		panic(err)
	}

	// 1. Trace Exporter
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint("localhost:4317"),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		panic(err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// 2. Log Exporter
	logExporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint("localhost:4317"),
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		panic(err)
	}

	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithResource(res),
	)
	global.SetLoggerProvider(lp)

	// 3. 构造控制台 + OTLP 组合 Core
	encoderConfig := zap.NewProductionEncoderConfig()
	consoleCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.Lock(zapcore.AddSync(os.Stdout)),
		zap.InfoLevel,
	)

	// otelzapbridge 只负责把 Zap 记录导流给 OpenTelemetry Provider
	otelCore := otelzapbridge.NewCore("demo-service33", otelzapbridge.WithLoggerProvider(lp))

	core := zapcore.NewTee(consoleCore, otelCore)
	logger = zap.New(core)

	return func() {
		_ = logger.Sync()
		_ = tp.Shutdown(context.Background())
		_ = lp.Shutdown(context.Background())
	}
}

func helloHandler(w http.ResponseWriter, r *http.Request) {
	ctx0 := r.Context()
	tracer := otel.Tracer("demo-service")

	ctx, span := tracer.Start(ctx0, "6-handler")
	defer span.End()

	// 关键用法：传入 ctx 自动带上 trace_id 和 span_id
	WithContext(ctx, logger).Info("收到 6 请求",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("client_ip", r.RemoteAddr),
	)
	//WithContext(ctx, logger).Info("收到 HTTP 请求",
	//	zap.String("name", "yang"),
	//	zap.Any("age", 100),
	//)
	//WithContext(ctx, logger).Info(fmt.Sprintf("hello %s exciting", "dongdong"),
	//	zap.String("name", "yang"),
	//	zap.Any("age", 100),
	//)
	//
	ctx1, span1 := tracer.Start(ctx0, "7-handler")
	defer span1.End()

	// 关键用法：传入 ctx 自动带上 trace_id 和 span_id
	WithContext(ctx1, logger).Info("收到 7",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("client_ip", r.RemoteAddr),
	)
	ctx2, span2 := tracer.Start(ctx, "8-handler")
	defer span2.End()

	// 关键用法：传入 ctx 自动带上 trace_id 和 span_id
	WithContext(ctx2, logger).Info("收到 8",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("client_ip", r.RemoteAddr),
	)
	time.Sleep(200 * time.Millisecond)
	_, _ = fmt.Fprintf(w, "Hello! TraceID: %s\n", span.SpanContext().TraceID().String())
}

func main() {
	cleanup := initTelemetry()
	defer cleanup()

	http.HandleFunc("/hello", helloHandler)
	logger.Info("服务启动", zap.String("addr", ":8080"))
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		logger.Fatal("服务启动失败", zap.Error(err))
	}
}
