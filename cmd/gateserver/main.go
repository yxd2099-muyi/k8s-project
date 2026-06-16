package main

import (
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/infra/redisClient"
	"go.uber.org/zap"
	"log"
	"os"
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
