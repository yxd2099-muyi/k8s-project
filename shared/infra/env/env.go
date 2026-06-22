package env

import (
	"github.com/k8s/muyi/shared/infra/cconst"
	"os"
)

func GetEnv() string {
	runEnv := os.Getenv(cconst.AppEnv)
	if runEnv != cconst.EnvK8s {
		runEnv = cconst.EnvK8s
	}
	return runEnv
}

// IsK8sEnv 是否是k8s 环境
func IsK8sEnv() bool {
	runEnv := os.Getenv(cconst.AppEnv)
	return runEnv == cconst.EnvK8s
}
