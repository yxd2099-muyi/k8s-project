package cconst

import (
	"go.etcd.io/etcd/api/v3/mvccpb"
)

type UpdateEtcdHandler func(key, value string, eType mvccpb.Event_EventType)
