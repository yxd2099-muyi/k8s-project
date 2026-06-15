# 1 命令测试
```
curl http://127.0.0.1:8080/api/v1/hello
```

# 2 项目跟目录运行
```shell
# 本地运行
make -f deploy/web/Makefile run-local

# 构建镜像（修复报错的核心命令）
make -f deploy/web/Makefile build

# 打包分发到k8s节点
make -f deploy/web/Makefile push-k8s-node

# 清理tar包
make -f deploy/web/Makefile clean
```