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

	otelzapbridge "go.opentelemetry.io/contrib/bridges/otelzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

func initTelemetry() func() {
	ctx := context.Background()
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("demo-service22"),
			semconv.ServiceVersionKey.String("v1.0.0"),
		),
	)
	if err != nil {
		zap.L().Fatal("创建 resource 失败", zap.Error(err))
	}

	// ================== 1. Trace (链路追踪) ==================
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint("localhost:4317"),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		zap.L().Fatal("创建 trace exporter 失败", zap.Error(err))
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	// 完整传播器：TraceContext + Baggage
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// ================== 2. OTLP Log Exporter ==================
	logExporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint("localhost:4317"),
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		zap.L().Fatal("创建 log exporter 失败", zap.Error(err))
	}

	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithResource(res),
	)
	global.SetLoggerProvider(lp)

	// ================== 3. Zap 单行标准JSON控制台输出 + OTLP遥测 ==================
	encoderConfig := zap.NewProductionEncoderConfig()
	// 确保JSON为单行、无换行符
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder

	// 控制台：严格单行JSON
	consoleCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.Lock(zapcore.AddSync(os.Stdout)),
		zap.InfoLevel,
	)
	// OTel bridge core
	otelCore := otelzapbridge.NewCore("demo-service22", otelzapbridge.WithLoggerProvider(lp))
	core := zapcore.NewTee(consoleCore, otelCore)
	logger = zap.New(core, zap.AddCaller())

	// 替换全局zap方便调用
	zap.ReplaceGlobals(logger)

	return func() {
		// 优雅关闭，使用超时上下文
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

	spanCtx := span.SpanContext()
	traceID := spanCtx.TraceID().String()
	spanID := spanCtx.SpanID().String()

	// 输出标准单行JSON日志
	logger.Info("收到 HTTP 请求",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("client_ip", r.RemoteAddr),
		zap.String("trace_id", traceID),
		zap.String("span_id", spanID),
	)

	_, _ = fmt.Fprintf(w, "Hello! TraceID: %s\n", traceID)
}

func main() {
	// ❗删除main里提前创建的baseCore logger，避免覆盖/格式错乱
	cleanup := initTelemetry()
	defer cleanup()

	http.HandleFunc("/hello", helloHandler)
	logger.Info("服务启动", zap.String("addr", ":8080"))
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		logger.Fatal("服务启动失败", zap.Error(err))
	}
}
