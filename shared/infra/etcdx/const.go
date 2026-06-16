package etcdx

import (
	"fmt"
	"time"
)

const (
	DefaultLeaseTTL    = 10
	DefaultReqTimeout  = 5 * time.Second
	WatchRetryInterval = 2 * time.Second
	SnapshotSyncPeriod = 5 * time.Minute
	MaxReRegisterRetry = 3
	BackoffStep        = time.Second
	MaxBackoffInterval = 4 * time.Second
)
const (
	ETCDUpdateTypePut    = 1 //添加或者修改
	ETCDUpdateTypeDelete = 2 //删除
)

func GetKey(prefix, name, id string) string {
	return fmt.Sprintf("%s/%s/%s", prefix, name, id)
}

type ServiceInstance struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	ServerId string `json:"server_id"`
	Address  string `json:"address"` // "ip:port"
	IP       string `json:"ip"`
	Port     int    `json:"port"`
	Status   int    `json:"status"`
	Version  string `json:"version"`
	Weight   int    `json:"weight"`
	Key      string `json:"key"`
	Env      string `json:"env"`

	Metadata map[string]string `json:"metadata"`
}
type UpdateServiceHandler func(updateType int, key string, serviceInstance *ServiceInstance)
