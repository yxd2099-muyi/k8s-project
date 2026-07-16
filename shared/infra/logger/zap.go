package logger

import (
	"context"
	"fmt"
	"github.com/k8s/muyi/shared/infra/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"gopkg.in/natefinch/lumberjack.v2"
	"log"
	"os"
	"strings"
	"time"

	otelzapbridge "go.opentelemetry.io/contrib/bridges/otelzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func getLogLevel(logLevel string) zapcore.Level {
	switch strings.ToLower(logLevel) {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

var L *zap.Logger

type Zlogger struct {
	ctx    context.Context
	cancel context.CancelFunc
	cfg    config.Log
	tp     *sdktrace.TracerProvider
	lp     *sdklog.LoggerProvider
}

func NewLogger() *Zlogger {
	cfg := config.GlobalConf.Log
	ctx, cancel := context.WithCancel(context.Background())
	z := &Zlogger{
		ctx:    ctx,
		cancel: cancel,
		cfg:    cfg,
	}
	z.init()
	return z
}
func (z *Zlogger) Close() {
	z.cancel()
	log.Println("zap logger close")
	if L != nil {
		err := L.Sync()
		if err != nil {
			log.Println(fmt.Sprintf("Error syncing zap logger: %v", err))
		}
	}
	if z.tp != nil {
		z.tp.Shutdown(context.Background()) //todo 超时设置

	}
	if z.lp != nil {
		z.lp.Shutdown(context.Background())
	}
}
func (z *Zlogger) initTelemetry() (*otelzapbridge.Core, error) {
	ctx := z.ctx
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("demo-web-service100"),
			semconv.ServiceVersionKey.String("v1.0.0"),
		),
	)
	if err != nil {
		return nil, err
	}
	// 1. Trace Exporter
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint("localhost:4317"),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithResource(res),
	)
	global.SetLoggerProvider(lp)
	// otelzapbridge 只负责把 Zap 记录导流给 OpenTelemetry Provider
	otelCore := otelzapbridge.NewCore("web-service200", otelzapbridge.WithLoggerProvider(lp))
	return otelCore, nil
}

// 本地开发
func (z *Zlogger) initLocal(level zapcore.Level) {
	encoderConfig := zap.NewProductionEncoderConfig() // 生产环境
	customTimeEncoder := func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		shanghai, _ := time.LoadLocation("Asia/Shanghai") //所有时区都以转化为北京时间输出
		enc.AppendString(t.In(shanghai).Format("2006-01-02 15:04:05.000"))
	}
	if z.cfg.Debug {
		encoderConfig = zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
		encoderConfig.EncodeTime = customTimeEncoder //都输出为北京时间
	} else {
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}
	// 创建编码器
	var encoder zapcore.Encoder
	if z.cfg.Debug {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	}

	// 创建日志写入器
	var cores []zapcore.Core
	// 普通日志写入器（包含所有级别日志）
	l := &lumberjack.Logger{
		Filename:   z.cfg.FileName,
		MaxSize:    z.cfg.MaxSize,    // megabytes 兆字节
		MaxBackups: z.cfg.MaxBackups, //保留文件数
		MaxAge:     z.cfg.MaxDays,    // days
		LocalTime:  true,
		Compress:   z.cfg.Compress, //日志生成压缩包,大幅降低磁盘空间,必要时使用
	}
	if z.cfg.FileName != "" {
		core := newCore(encoder, l, level)
		cores = append(cores, core)
	}
	//错误
	errL := &lumberjack.Logger{
		Filename:   z.cfg.ErrLogPath,
		MaxSize:    z.cfg.MaxSize,    // megabytes 兆字节
		MaxBackups: z.cfg.MaxBackups, //保留文件数
		MaxAge:     z.cfg.MaxDays,    // days
		LocalTime:  true,
		Compress:   z.cfg.Compress, //日志生成压缩包,大幅降低磁盘空间,必要时使用
	}
	//开启24小时轮转一次
	if z.cfg.RotateByDay {
		go z.rotate(l, errL)
	}
	if len(z.cfg.ErrLogPath) > 0 {
		core := newCore(encoder, errL, zapcore.ErrorLevel)
		cores = append(cores, core)
	}
	//开发模式默认开启控制台输出
	if z.cfg.Debug {
		consoleEncoderConfig := encoderConfig
		consoleEncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder //控制台带颜色
		consoleEncoder := zapcore.NewConsoleEncoder(consoleEncoderConfig)
		consoleCore := zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), level)
		cores = append(cores, consoleCore)
	}
	if z.cfg.OtelOpen {
		consoleEncoderConfig := encoderConfig
		consoleEncoder := zapcore.NewJSONEncoder(consoleEncoderConfig)
		consoleCore := zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), level)
		otelCore, err := z.initTelemetry()
		if err == nil {
			cores = append(cores, consoleCore, otelCore)
		}
	}
	// 创建核心
	coreTree := zapcore.NewTee(cores...)
	// 创建日志器
	if z.cfg.Debug {
		L = zap.New(coreTree, zap.AddCaller(), zap.Development()) // 提供更详细的错误信息， 便于调试和定位问题
	} else {
		L = zap.New(coreTree, zap.AddCaller())
	}
}

// k8s 情况
func (z *Zlogger) initK8s(level zapcore.Level) {
	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	var cores []zapcore.Core
	// k8s环境：只输出标准输出
	cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level))
	L = zap.New(zapcore.NewTee(cores...), zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
}
func (z *Zlogger) init() {
	var level = getLogLevel(z.cfg.LogLevel)
	env := config.GetEnv()
	if env == config.EnvLocal {
		z.initLocal(level)
	} else {
		z.initK8s(level)
	}
	zap.ReplaceGlobals(L) // 这个看是否有必要
	//// 方式1：使用自定义全局变量
	//logger.L.Info("info msg", zap.String("key", "val"))
	//
	//// 方式2：使用 zap.L()（前提是调用了 ReplaceGlobals）
	//zap.L().Info("info msg", zap.String("key", "val"))
	//
	//// 方式3：获取 sugared logger（更易写）
	//zap.S().Infow("info msg", "key", "val")
}

func newCore(encoder zapcore.Encoder, writer *lumberjack.Logger, minLevel zapcore.Level) zapcore.Core {
	enab := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= minLevel
	})
	core := zapcore.NewCore(encoder, zapcore.AddSync(writer), enab)
	return core

}
func (z *Zlogger) rotate(writer *lumberjack.Logger, errWriter *lumberjack.Logger) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("panic rotate error:", err)
		}
	}()
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop() // 确保释放资源
	// 计算到第二天午夜的时间
	now := time.Now()
	nextMidnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	initialDelay := nextMidnight.Sub(now)
	// 首次轮转
	select {
	case <-time.After(initialDelay):
		if err := z.doRotate(writer, errWriter); err != nil {
			log.Println(fmt.Sprint("initial rotation failed", zap.Error(err)))
		}
	case <-z.ctx.Done():
		return
	}
	// 定期轮转
	for {
		select {
		case <-ticker.C:
			if err := z.doRotate(writer, errWriter); err != nil {
				log.Println(fmt.Sprint("initial rotation failed", zap.Error(err)))
			}
		case <-z.ctx.Done():
			return
		}
	}

}
func (z *Zlogger) doRotate(writer *lumberjack.Logger, errWriter *lumberjack.Logger) error {
	if writer != nil {
		err := writer.Rotate()
		if err != nil {
			log.Println("rotate err:", err)
			return err
		}
	}

	if z.cfg.NeedErrLog && errWriter != nil {
		err := errWriter.Rotate()
		if err != nil {
			log.Println("errWriter rotate err:", err)
			return err
		}
	}
	return nil
}
