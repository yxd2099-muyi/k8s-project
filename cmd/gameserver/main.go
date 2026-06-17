package main

import (
	"github.com/k8s/muyi/internal/game/server"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/logger"
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
