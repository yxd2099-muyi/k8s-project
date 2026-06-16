#!/bin/bash
export PATH="$PATH:$(go env GOPATH)/bin"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

PROTO_ROOT="$PROJECT_ROOT/api/proto"
PB_OUT="$PROJECT_ROOT/api/pb"
PROTOC_BIN="$SCRIPT_DIR/protoc/31.1-win64/bin/protoc"

mkdir -p "$PB_OUT"

cd "$PROTO_ROOT" || exit

"$PROTOC_BIN" \
    --proto_path=. \
    --go_out="$PB_OUT" --go_opt=paths=source_relative \
    --go-grpc_out="$PB_OUT" --go-grpc_opt=paths=source_relative \
    base/*.proto service/*.proto web/*.proto push/*.proto

echo "生成完成，输出目录：$PB_OUT"
##!/bin/bash
#export PATH="$PATH:$(go env GOPATH)/bin"
#
#SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
#PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
#
## 将 PROTO_ROOT 指向 api/proto（即 proto 文件的根目录）
#PROTO_ROOT="$PROJECT_ROOT/api/proto"
#
#PROTO_SRC_BASE="$PROTO_ROOT/base"
#PROTO_SRC_SERVICE="$PROTO_ROOT/service"
#PROTO_SRC_PUSH="$PROTO_ROOT/push"
#PROTO_SRC_WEB="$PROTO_ROOT/web"
#
#PB_OUT="$PROJECT_ROOT/api/pb"
#PROTOC_BIN="$SCRIPT_DIR/protoc/31.1-win64/bin/protoc"
#
#mkdir -p "$PB_OUT"
#
## 只用 --proto_path，去掉 -I（或保留但指向同一目录）
#"$PROTOC_BIN" \
#    --proto_path="$PROTO_ROOT" \
#    --go_out="$PB_OUT" --go_opt=paths=source_relative \
#    --go-grpc_out="$PB_OUT" --go-grpc_opt=paths=source_relative \
#    "$PROTO_SRC_BASE"/*.proto \
#    "$PROTO_SRC_SERVICE"/*.proto \
#    "$PROTO_SRC_PUSH"/*.proto \
#    "$PROTO_SRC_WEB"/*.proto
#
#echo "proto 生成完成，输出目录：$PB_OUT"

##!/bin/bash
#
#export PATH="$PATH:$(go env GOPATH)/bin"
#
## 1. 获取脚本自身绝对目录：scripts/gen_proto
#SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
#echo "脚本目录: $SCRIPT_DIR"
#
## 2. 向上两层，定位【项目根目录】
#PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
#echo "项目根目录: $PROJECT_ROOT"
#
## 3. 定义固定路径
#PROTO_ROOT="$PROJECT_ROOT/api"          # proto搜索根
#PROTO_SRC_PUSH="$PROTO_ROOT/proto/push"
#PROTO_SRC_WEB="$PROTO_ROOT/proto/web"
#PROTO_SRC_BASE="$PROTO_ROOT/proto/base"
#PROTO_SRC_SERVICE="$PROTO_ROOT/proto/service"
#PB_OUT="$PROJECT_ROOT/api/pb"
#PROTOC_BIN="$SCRIPT_DIR/protoc/31.1-win64/bin/protoc"
#
## 自动创建输出目录
#mkdir -p "$PB_OUT"
#
## 执行protoc
#"$PROTOC_BIN" \
#    --go_out="$PB_OUT" \
#    --go-grpc_out="$PB_OUT" \
#    --proto_path="$PROTO_ROOT" \
#    "$PROTO_SRC_PUSH"/*.proto \
#    "$PROTO_SRC_BASE"/*.proto \
#    "$PROTO_SRC_SERVICE"/*.proto \
#    "$PROTO_SRC_WEB"/*.proto
#
#echo "proto 生成完成，输出目录：$PB_OUT"