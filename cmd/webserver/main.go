package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/k8s/muyi/internal/web/router"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"log"
	"net/http"
	_ "net/http/pprof" // 自动注册 /debug/pprof
	"os"
	"os/signal"
	"syscall"
	"time"
)

func getPort() string {
	portD := os.Getenv(cconst.WEB_PORT)
	if portD == "" {
		cfg := config.GetWebServerCfg()
		portD = cfg.Port
	}
	return portD
}
func main() {
	err := config.Init()
	if err != nil {
		log.Fatalf("init config failed: %v", err)
	}
	zlogger := logger.NewLogger()
	defer zlogger.Close()
	clog := logger.L
	r := gin.New()
	router.RegisterRoutes(r)
	port := getPort()
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", port),
		Handler: r,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			clog.Fatal("listen err", zap.Error(err))
		}
	}()
	//pprof使用
	go func() {
		clog.Info("pprof start at :6060")
		err = http.ListenAndServe("0.0.0.0:6060", nil)
		if err != nil {
			clog.Fatal("listen err", zap.Error(err))
		}
	}()
	// prometheus metrics 独立端口
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		err = http.ListenAndServe("0.0.0.0:7070", mux) // 独立端口 7070
		if err != nil {
			clog.Fatal("listen err", zap.Error(err))
		}
		clog.Info("prometheus start at :7070")
	}()
	clog.Info("service start success, env:" + config.GetEnv())
	// 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		clog.Error("server shutdown err", zap.Error(err))
	}
	clog.Info("service exit complete")
}
