.PHONY: run-local build push-k8s-node clean

# 本地运行：入口改为 cmd/webserver/main.go
run-local:
	APP_ENV=local go run ./cmd/webserver/main.go

# 构建：compose 文件完整相对根路径
build:
	docker compose -f ./deploy/web/docker/docker-compose.yml build k8s-web-builder

# 导出镜像分发节点（逻辑不变，tar包生成在项目根）
push-k8s-node:
	docker save k8s-web:v1.0.0 -o k8s-web.tar
	# master节点
	scp k8s-web.tar yang@192.168.96.10:/tmp/
	ssh -tt yang@192.168.96.10 "sudo ctr -n k8s.io images import /tmp/k8s-web.tar && rm /tmp/k8s-web.tar"
	# node1
	scp k8s-web.tar yang@192.168.96.11:/tmp/
	ssh -tt yang@192.168.96.11 "sudo ctr -n k8s.io images import /tmp/k8s-web.tar && rm /tmp/k8s-web.tar"
	# node2
	scp k8s-web.tar yang@192.168.96.12:/tmp/
	ssh -tt yang@192.168.96.12 "sudo ctr -n k8s.io images import /tmp/k8s-web.tar && rm /tmp/k8s-web.tar"
	rm k8s-web.tar

# 清理tar包
clean:
	rm -f k8s-web.tar