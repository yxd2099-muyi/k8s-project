package cconst

import (
	"fmt"
)

// RedisKeyForUserSession 用户session key
func RedisKeyForUserSession(uId uint64) string {
	return fmt.Sprintf("hash:user:session:%d", uId)
}
