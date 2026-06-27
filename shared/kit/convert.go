package kit

import (
	"strconv"
	"strings"
)

func StringToUint64(s string) (uint64, error) {
	num, err := strconv.ParseUint(s, 10, 64)
	return num, err
}
func StringToInt64(s string) (int64, error) {
	num, err := strconv.ParseInt(s, 10, 64)
	return num, err
}
func Uint64ToString(num uint64) string {
	str := strconv.FormatUint(num, 10)
	return str
}

// ExtractAddrFromEtcdKey 从 etcd key 中提取末尾 ip:port
// key格式：xxx/xxx/xxx/ip:port
func ExtractAddrFromEtcdKey(key string) string {
	pos := strings.LastIndex(key, "/")
	if pos <= 0 || pos >= len(key)-1 {
		return ""
	}
	return key[pos+1:]
}
