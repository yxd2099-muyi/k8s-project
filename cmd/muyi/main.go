package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	// 1. 使用官方的 otelzap bridge 替换 uptrace 包
	otelzapbridge "go.opentelemetry.io/contrib/bridges/otelzap"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger // 恢复为原生的 *zap.Logger

func initTelemetry() func() {
	ctx := context.Background()
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("demo-service22"),
			semconv.ServiceVersionKey.String("v1.0.0"),
		),
	)
	if err != nil {
		log.Fatalf("创建 resource 失败: %v", err)
	}

	// ================== 1. Trace (链路追踪) ==================
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint("localhost:4317"),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		log.Fatal(err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// ================== 2. Log (日志发往 OTLP) ==================
	logExporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint("localhost:4317"),
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		log.Fatal(err)
	}

	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithResource(res),
	)
	global.SetLoggerProvider(lp)

	// ================== 3. 官方 Zap Bridge 设置 ==================
	// 创建 OTel 专用 core
	otelCore := otelzapbridge.NewCore("demo-service33", otelzapbridge.WithLoggerProvider(lp))

	// 如果你既想在控制台看日志，又想发送到 OTLP，使用 zapcore.NewTee 组合
	consoleCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.Lock(zapcore.AddSync(log.Writer())), // 或 os.Stdout
		zap.InfoLevel,
	)

	// 将控制台与 OTel 输出通道结合
	core := zapcore.NewTee(consoleCore, otelCore)
	logger = zap.New(core)

	return func() {
		logger.Sync()
		tp.Shutdown(context.Background())
		lp.Shutdown(context.Background())
	}
}

func helloHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tracer := otel.Tracer("demo-service")

	ctx, span := tracer.Start(ctx, "hello-handler")
	defer span.End()

	// 传递 ctx 可以把 TraceID 和 SpanID 自动带到日志中
	logger.Info("收到 HTTP 请求",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("client_ip", r.RemoteAddr),
	)

	fmt.Fprintf(w, "Hello! TraceID: %s\n", span.SpanContext().TraceID())
}

func main() {
	cleanup := initTelemetry()
	defer cleanup()

	http.HandleFunc("/hello", helloHandler)
	fmt.Println("服务启动 → http://localhost:8080/hello")
	http.ListenAndServe(":8080", nil)
}
