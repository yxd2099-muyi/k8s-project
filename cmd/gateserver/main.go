package main

import (
	"context"
	"fmt"
	"github.com/k8s/muyi/internal/gate"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/infra/redisClient"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	err := config.Init()
	if err != nil {
		log.Fatalf("init config failed: %v", err)
	}
	zlogger := logger.NewLogger()
	defer zlogger.Close()
	clog := logger.L
	clog.Info("hello world")
	rc := redisClient.GetClient()
	cfg := config.GlobalConf
	err = rc.Init(clog, &cfg.Redis)
	if err != nil {
		clog.Error("redis init failed", zap.Error(err))
		return
	}
	defer rc.Close()
	wsPort, gPort := getPort()
	gateAddr := fmt.Sprintf(":%s", wsPort) // 这样写对吗？ todo
	grpcAddr := fmt.Sprintf(":%s", gPort)
	gateSvc := gate.NewGateService(cfg.Gate, gateAddr, grpcAddr)
	err = gateSvc.Start()
	if err != nil {
		clog.Error("start gate failed", zap.Error(err))
		return
	}
	clog.Info("===============================gate grpc http success==============================================")
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err = gateSvc.Shutdown(ctx)
	if err != nil {
		clog.Error("shutdown gate failed", zap.Error(err))
		return
	}
	clog.Info("gateserver shutdown")
	//ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	//defer cancel()
	//shutdownCtx := WaitShutdown(10 * time.Second)
	//<-shutdownCtx.Done()

}

// WaitShutdown 阻塞等待退出信号，返回带超时的ctx用于资源释放
func WaitShutdown(waitTimeout time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()
	return ctx
}
func getPort() (wsPort, gPort string) {
	wsPort = os.Getenv(cconst.GATE_WS_PORT)
	cfg := config.GetGateServerCfg()
	if wsPort == "" {
		wsPort = cfg.WsPort
	}
	gPort = os.Getenv(cconst.GATE_GRPC_PORT)
	if gPort == "" {
		gPort = cfg.GRpcPort
	}
	return
}
