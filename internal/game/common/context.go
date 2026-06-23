package common

import (
	"go.uber.org/zap"
	"sync"
)

var gDummyTContext TContext

type TContext struct {
	Logger  *zap.Logger
	Uid     uint64
	Address string
	IPStr   string // ip 字符串形式
	GID     uint64 // 所在公会id
	RoomId  uint32
}

func (t *TContext) Reset() {
	*t = gDummyTContext
	//如果有零值需要特殊处理
	t.Logger = nil
}

var poolTContext = &sync.Pool{
	New: func() interface{} {
		c := &TContext{}
		return c
	},
}

// NewContext 创建新的context
func NewContext(uid uint64) *TContext {
	obj := poolTContext.Get().(*TContext)
	obj.Reset()
	obj.Uid = uid
	return obj
}

// FreeContext   放回池子中
func FreeContext(obj *TContext) {
	if obj == nil {
		return
	}
	obj.Uid = 0
	obj.Address = ""
	obj.Logger = nil
	obj.IPStr = ""
	obj.GID = 0
	poolTContext.Put(obj)
}

// TContextKey 上下文键类型
type TContextKey struct{}
