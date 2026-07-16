package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/k8s/muyi/internal/gate"
	"github.com/k8s/muyi/internal/gate/common"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/env"
	"github.com/k8s/muyi/shared/infra/etcdx"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/infra/rediscli"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var err error
	baseCfgPath := "configs/base_config.toml" //k8s path/app/config/config.toml
	cfgPath := "configs/config.toml"
	common.InitStaticConfig(baseCfgPath, cfgPath)
	baseCfg := common.GetBaseCfg()
	cfgLog := baseCfg.Log
	gateCfg := common.GetGate()
	zlogger := logger.NewLogger(cfgLog, gateCfg.LogPath, gateCfg.ErrLogPath, gateCfg.ServiceNameKey)
	defer zlogger.Close()
	clog := logger.L
	clog.Info("hello world")
	clog.Debug("baseCfg", zap.Any("baseCfg", baseCfg))
	clog.Debug("gateCfg", zap.Any("gateCfg", gateCfg))
	initArgConfig()
	clog.Debug("arg", zap.Any("arg", common.GetGateArg()))
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
	gateSvc := gate.NewGateService(gateCfg)
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

}

var (
	Version   = "1.0.0.1"
	BuildTime = "1970-01-01_0:0:0"
)

// InitArgConfig 从两个环境中获取 区分是k8s 还是本地项目
func initArgConfig() {
	//common.InitArgConfig()
	if !flag.Parsed() {
		parseFlag()
	}
	isK8s := env.IsK8sEnv()
	if !isK8s {
		return
	}
	// 处理k8s 的环境情况
	wsPort := os.Getenv(cconst.GATE_WS_PORT)
	gPort := os.Getenv(cconst.GATE_GRPC_PORT)
	podIP := os.Getenv(cconst.GATE_POD_IP)
	wsAddr := fmt.Sprintf(":%s", wsPort)
	grpcAddr := fmt.Sprintf(":%s", gPort)
	wsAddrRegister := fmt.Sprintf("%s:%s", podIP, wsPort)
	grpcAddrRegister := fmt.Sprintf("%s:%s", podIP, gPort)
	argConfig := common.GetGateArg()
	argConfig.WsAddress = wsAddr
	argConfig.WsAddressRegister = wsAddrRegister
	argConfig.GRpcAddress = grpcAddr
	argConfig.GRpcAddressRegister = grpcAddrRegister
	argConfig.WsPort = wsPort
	argConfig.GRpcPort = gPort
	argConfig.IPString = podIP
}
func parseFlag() {
	var showVersion bool
	var showVersionTime bool
	var ipString string
	var wsPort string
	var grpcPort string
	flag.BoolVar(&showVersion, "v", false, "show version")
	flag.BoolVar(&showVersionTime, "t", false, "显示版本编译时间")
	flag.StringVar(&ipString, "ip", "127.0.0.1", "服务实例IP")
	flag.StringVar(&wsPort, "wsPort", "", "websocket监听端口号")
	flag.StringVar(&grpcPort, "grpc_port", "", "grpc监听端口号")
	flag.Parse()
	if showVersion {
		fmt.Printf("Version: %s\n", Version)
		os.Exit(0)
	}
	if showVersionTime {
		fmt.Printf("BuildTime: %s\n", BuildTime)
		os.Exit(0)
	}

	argConfig := common.GetGateArg()
	gateCfg := common.GetGate()
	argConfig.IPString = ipString
	argConfig.WsPort = gateCfg.WsPort
	argConfig.GRpcPort = gateCfg.GRpcPort
	if wsPort != "" {
		argConfig.WsPort = wsPort
	}
	if grpcPort != "" {
		argConfig.GRpcPort = grpcPort
	}
	wsAddr := fmt.Sprintf("%s:%s", argConfig.IPString, argConfig.WsPort)
	wsAddrRegister := fmt.Sprintf("%s:%s", argConfig.IPString, argConfig.WsPort)
	grpcAddr := fmt.Sprintf("%s:%s", argConfig.IPString, argConfig.GRpcPort)
	grpcAddrRegister := fmt.Sprintf("%s:%s", argConfig.IPString, argConfig.GRpcPort)
	argConfig.WsAddress = wsAddr
	argConfig.WsAddressRegister = wsAddrRegister
	argConfig.GRpcAddress = grpcAddr
	argConfig.GRpcAddressRegister = grpcAddrRegister
}
