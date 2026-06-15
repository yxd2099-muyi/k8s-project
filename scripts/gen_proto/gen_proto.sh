#!/bin/bash

export PATH="$PATH:$(go env GOPATH)/bin"

# 1. 获取脚本自身绝对目录：scripts/gen_proto
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "脚本目录: $SCRIPT_DIR"

# 2. 向上两层，定位【项目根目录】
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
echo "项目根目录: $PROJECT_ROOT"

# 3. 定义固定路径
PROTO_ROOT="$PROJECT_ROOT/api"          # proto搜索根
PROTO_SRC_PUSH="$PROTO_ROOT/proto/push"
PROTO_SRC_WEB="$PROTO_ROOT/proto/web"
PB_OUT="$PROJECT_ROOT/api/pb"
PROTOC_BIN="$SCRIPT_DIR/protoc/31.1-win64/bin/protoc"

# 自动创建输出目录
mkdir -p "$PB_OUT"

# 执行protoc
"$PROTOC_BIN" \
    --go_out="$PB_OUT" \
    --go-grpc_out="$PB_OUT" \
    --proto_path="$PROTO_ROOT" \
    "$PROTO_SRC_PUSH"/*.proto \
    "$PROTO_SRC_WEB"/*.proto

echo "proto 生成完成，输出目录：$PB_OUT"