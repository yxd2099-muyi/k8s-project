package cconst

import (
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
)

type UpdateEtcdHandler func(key, value string, eType mvccpb.Event_EventType)

type UpdateEtcdEndPointGrpcHandler func(key, address string, value any, eType endpoints.Operation)
