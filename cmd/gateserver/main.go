package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/k8s/muyi/internal/gate"
	"github.com/k8s/muyi/internal/gate/common"
	//_ "github.com/k8s/muyi/shared/infra/balancerx"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/env"
	"github.com/k8s/muyi/shared/infra/etcdx"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/infra/rediscli"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	os.Setenv("GRPC_GO_LOG_SEVERITY_LEVEL", "INFO")
	os.Setenv("GRPC_GO_LOG_VERBOSITY_LEVEL", "99")
	err := config.Init()
	if err != nil {
		log.Fatalf("init config failed: %v", err)
	}
	zlogger := logger.NewLogger()
	defer zlogger.Close()
	clog := logger.L
	clog.Info("hello world")
	initArgConfig()
	rc := rediscli.GetClient()
	cfg := config.GlobalConf
	err = rc.Init(clog, &cfg.Redis)
	if err != nil {
		clog.Error("redis init failed", zap.Error(err))
		return
	}
	defer rc.Close()
	//wsPort, gPort := getPort()
	//gateAddr := fmt.Sprintf(":%s", wsPort) //k8s
	//grpcAddr := fmt.Sprintf(":%s", gPort)
	gateAddr, grpcAddr := getAddress()
	clog.Info("grpc addr", zap.String("gateAddr", gateAddr), zap.String("grpcAddr", grpcAddr))
	etcdCli, err := etcdx.InitGlobalLeaseEtcd()
	if err != nil {
		clog.Error("init etcd failed", zap.Error(err))
		return
	}
	defer etcdCli.Close()
	//balancerx.InitTargetDirectBalanceBuilder()
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

}
func getAddress() (wsAddr string, grpcAddr string) {
	wsPort, gPort := getPort()
	ip := common.GetArgConfig().IPString
	wsAddr = fmt.Sprintf("%s:%s", ip, wsPort)
	grpcAddr = fmt.Sprintf("%s:%s", ip, gPort)
	return
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

var (
	Version   = "1.0.0.1"
	BuildTime = "1970-01-01_0:0:0"
)

// InitArgConfig 从两个环境中获取 区分是k8s 还是本地项目
func initArgConfig() {
	common.InitArgConfig()
	isK8s := env.IsK8sEnv()
	if !isK8s && !flag.Parsed() {
		parseFlag()
	}
}
func parseFlag() {
	var showVersion bool
	var showVersionTime bool
	//var port string
	//var gPort string
	var ipString string
	flag.BoolVar(&showVersion, "v", false, "show version")
	flag.BoolVar(&showVersionTime, "t", false, "显示版本编译时间")
	flag.StringVar(&ipString, "ip", "127.0.0.1", "服务实例IP")
	//flag.StringVar(&port, "port", "8090", "websocket监听端口号")
	//flag.StringVar(&gPort, "grpc_port", "8099", "grpc监听端口号")
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
	argConfig.IPString = ipString
}
