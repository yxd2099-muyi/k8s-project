package common

import (
	"context"
	"fmt"
	pb_base "github.com/k8s/muyi/api/pb/base"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/kit"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func ContextInterception() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		newCtx, err := ContextInterceptor(ctx)
		if err != nil {
			res := &pb_service.ForwardRsp{}
			res.Code = pb_base.ErrCode_EC_ERROR
			res.Msg = err.Error()
			return res, err
		}
		resp, err := handler(newCtx, req)
		if err != nil {
			res := &pb_service.ForwardRsp{}
			res.Code = pb_base.ErrCode_EC_ERROR
			res.Msg = err.Error()
			return res, err
		}
		// 处理完成后，从context中获取TContext并释放
		if tCtx, ok := newCtx.Value(TContextKey{}).(*TContext); ok {
			FreeContext(tCtx)
		}
		return resp, err
	}
}
func ContextInterceptor(ctx context.Context) (context.Context, error) {
	clog := logger.L
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		m := fmt.Sprintf("ContextInterceptor not found in context")
		clog.Error(m)
		return nil, fmt.Errorf(m)
	}
	UIds := md.Get(cconst.GRpcContextFieldUID)
	if len(UIds) == 0 {
		m := fmt.Sprintf("ContextInterceptor UIds size is 0")
		clog.Error(m)
		return nil, fmt.Errorf(m)
	}
	uidStr := UIds[0]
	uid, _ := kit.StringToUint64(uidStr)
	tCtx := NewContext(uid)
	childLogger := clog.With(zap.Uint64("uid", uid))
	tCtx.Logger = childLogger
	newCtx := context.WithValue(ctx, TContextKey{}, tCtx)
	return newCtx, nil
}
