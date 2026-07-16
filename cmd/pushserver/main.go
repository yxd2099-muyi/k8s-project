package main

import (
	"flag"
	"fmt"
	"github.com/k8s/muyi/internal/push/common"
	"github.com/k8s/muyi/internal/push/server"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/env"
	"github.com/k8s/muyi/shared/infra/etcdx"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/infra/rediscli"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
)

var (
	Version   = "1.0.0.1"
	BuildTime = "1970-01-01_0:0:0"
)

func main() {
	var err error
	baseCfgPath := "configs/base_config.toml" //k8s path/app/config/config.toml
	cfgPath := "configs/config.toml"
	common.InitStaticConfig(baseCfgPath, cfgPath)
	baseCfg := common.GetBaseCfg()
	cfgLog := baseCfg.Log
	cfg := common.GetPush()
	zlogger := logger.NewLogger(cfgLog, cfg.LogPath, cfg.ErrLogPath, cfg.ServiceNameKey)
	defer zlogger.Close()
	clog := logger.L
	clog.Info("hello world pushserver")
	initArgConfig()
	clog.Debug("push", zap.Any("base", baseCfg))
	clog.Debug("push", zap.Any("cfg", cfg))
	clog.Debug("arg", zap.Any("arg", common.GetArgCfg()))
	rc := rediscli.GetClient()
	err = rc.Init(clog, &baseCfg.Redis)
	if err != nil {
		clog.Error("redis init failed", zap.Error(err))
		return
	}
	defer rc.Close()
	etcdCli, err := etcdx.InitGlobalLeaseEtcd(baseCfg.Etcd)
	if err != nil {
		clog.Error("init etcd failed", zap.Error(err))
		return
	}
	defer etcdCli.Close()
	pushSvr := server.NewPushService()
	err = pushSvr.Start()
	if err != nil {
		clog.Error("start push server failed", zap.Error(err))
		return
	}
	defer pushSvr.Shutdown()

	clog.Info("===============================push server success==============================================")
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	clog.Info("shutting down gracefully...")
}

func parseFlag() {
	var showVersion bool
	var showVersionTime bool
	var port string
	var ipString string
	var podName string
	var podIndex uint64
	flag.BoolVar(&showVersion, "v", false, "show version")
	flag.BoolVar(&showVersionTime, "t", false, "显示版本编译时间")
	flag.StringVar(&ipString, "ip", "127.0.0.1", "服务实例IP")
	flag.StringVar(&podName, "pod_name", "game-0", "服务实例IP")
	flag.StringVar(&port, "port", "", "服务器监听端口号")
	flag.Uint64Var(&podIndex, "pod_index", 0, "当前服务 pod 索引")

	flag.Parse()
	if showVersion {
		fmt.Printf("Version: %s\n", Version)
		os.Exit(0)
	}
	if showVersionTime {
		fmt.Printf("BuildTime: %s\n", BuildTime)
		os.Exit(0)
	}
	argConfig := common.GetArgCfg()
	if len(port) == 0 {
		cfg := common.GetPush()
		port = cfg.Port
	}
	argConfig.Port = port
	argConfig.PodName = podName
	argConfig.IPString = ipString
	argConfig.GRpcAddr = fmt.Sprintf("%s:%s", ipString, port)
	argConfig.RegisterAddr = fmt.Sprintf("%s:%s", ipString, port)
}

// InitArgConfig 从两个环境中获取 区分是k8s 还是本地项目
func initArgConfig() {
	//common.InitArgConfig()
	isK8s := env.IsK8sEnv()
	if !flag.Parsed() {
		parseFlag()
	}
	if isK8s {
		ip := os.Getenv(cconst.GamePodIP)
		port := os.Getenv(cconst.GamePodPort)
		name := os.Getenv(cconst.GamePodName)
		cfg := common.GetArgCfg()
		cfg.Port = port
		cfg.IPString = ip
		cfg.PodName = name
		cfg.GRpcAddr = fmt.Sprintf(":%s", port)
		cfg.RegisterAddr = fmt.Sprintf("%s:%s", ip, port)
	}
}
