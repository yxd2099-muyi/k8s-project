# 构建阶段
FROM golang:1.26.4-alpine AS builder
WORKDIR /build
# 复制项目根的go mod文件
COPY go.mod go.sum ./
RUN go mod download
# 复制项目全量源码
COPY . .
# 编译入口改为 cmd/webserver/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o app ./cmd/webserver/main.go

# 运行阶段轻量镜像
FROM alpine:3.19
WORKDIR /app
COPY --from=builder /build/app ./
ENV APP_ENV=k8s
EXPOSE 8080
CMD ["/app/app"]