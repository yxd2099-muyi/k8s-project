package main

import (
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/logger"
	"log"
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
}
