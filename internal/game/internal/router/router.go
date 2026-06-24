package router

import "github.com/k8s/muyi/internal/game/internal/handler"

func InitRouter() {
	handler.NewHandler()
	handler.NewRoomHandler()
}
