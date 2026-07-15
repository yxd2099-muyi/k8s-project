package main

import (
	"context"
	"fmt"
	otelzapbridge "go.opentelemetry.io/contrib/bridges/otelzap"
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

	//otelzapbridge "go.opentelemetry.io/contrib/bridges/otelzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

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

	// 1. Trace Exporter：修改为 otel-collector:4317（Docker服务名）
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint("otel-collector:4317"), // ✅ 修改
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		panic(err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// 2. Log Exporter：修改为 otel-collector:4317
	logExporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint("otel-collector:4317"), // ✅ 修改
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

	// 3. 控制台Core：纯单行JSON输出，**不再把 otelCore 合并进主logger** ✅关键改动
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	consoleCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.Lock(zapcore.AddSync(os.Stdout)),
		zap.InfoLevel,
	)
	// otelCore仅用于遥测上报，不混入stdout
	otelCore := otelzapbridge.NewCore("demo-service22", otelzapbridge.WithLoggerProvider(lp))
	// ✅主logger只用consoleCore输出纯单行JSON给Promtail采集
	core := zapcore.NewTee(consoleCore, otelCore)
	logger = zap.New(core, zap.AddCaller())
	//logger = zap.New(consoleCore, zap.AddCaller())

	return func() {
		ctxShutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = logger.Sync()
		_ = tp.Shutdown(ctxShutdown)
		_ = lp.Shutdown(ctxShutdown)
	}
}

func helloHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tracer := otel.Tracer("demo-service")

	ctx, span := tracer.Start(ctx, "hello-handler")
	defer span.End()

	WithContext(ctx, logger).Info("收到 HTTP 请求",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("client_ip", r.RemoteAddr),
	)
	WithContext(ctx, logger).Info("收到 HTTP 请求",
		zap.String("name", "dong"),
		zap.Any("age", 100),
	)
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
