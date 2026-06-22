package main

import (
	"flag"
	"fmt"
	"github.com/k8s/muyi/internal/game/common"
	"github.com/k8s/muyi/internal/game/server"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/env"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/infra/rediscli"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	err := config.Init()
	if err != nil {
		log.Fatalf("init config failed: %v", err)
	}
	zlogger := logger.NewLogger()
	defer zlogger.Close()
	clog := logger.L
	clog.Info("hello world gameserver")
	common.InitArgConfig()
	initArgConfig()
	rc := rediscli.GetClient()
	cfg := config.GlobalConf
	err = rc.Init(clog, &cfg.Redis)
	if err != nil {
		clog.Error("redis init failed", zap.Error(err))
		return
	}
	defer rc.Close()

	gameSvc, err := server.NewGameService()
	if err != nil {
		clog.Error("create game service failed")
		return
	}
	if err = gameSvc.Start(); err != nil {
		clog.Error("start game service failed")
		return
	}
	defer gameSvc.Shutdown()
	clog.Info("===============================game server success==============================================")
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	clog.Info("shutting down gracefully...")
}

var (
	Version   = "1.0.0.1"
	BuildTime = "1970-01-01_0:0:0"
)

// InitArgConfig 从两个环境中获取 区分是k8s 还是本地项目
func initArgConfig() {
	isK8s := env.IsK8sEnv()
	if !flag.Parsed() {
		parseFlag()
	}
	if isK8s {
		ip := os.Getenv(cconst.GamePodIP)
		port := os.Getenv(cconst.GamePodPort)
		cfg := common.GetArgConfig()
		cfg.Port = port
		cfg.IPString = ip
	}
}
func parseFlag() {
	var showVersion bool
	var showVersionTime bool
	var port string
	var ipString string
	flag.BoolVar(&showVersion, "v", false, "show version")
	flag.BoolVar(&showVersionTime, "t", false, "显示版本编译时间")
	flag.StringVar(&ipString, "ip", "127.0.0.1", "服务实例IP")
	flag.StringVar(&port, "port", "9000", "服务器监听端口号")
	flag.Parse()
	if showVersion {
		fmt.Printf("Version: %s\n", Version)
		os.Exit(0)
	}
	if showVersionTime {
		fmt.Printf("BuildTime: %s\n", BuildTime)
		os.Exit(0)
	}
	argConfig := common.GetArgConfig()
	argConfig.Port = port
	argConfig.IPString = ipString
}
