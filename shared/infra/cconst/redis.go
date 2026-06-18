package cconst

import (
	"fmt"
	"time"
)

const (
	ExpireTimeOut2s = 2 * time.Second
	ExpireTimeOut3s = 3 * time.Second
)

// RedisKeyForUserSession 用户session key
func RedisKeyForUserSession(uId uint64) string {
	return fmt.Sprintf("hash:user:session:%d", uId)
}
