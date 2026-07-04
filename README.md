# k8s-project
k8s learning 项目

# 执行不同的 makefile
必须在【项目根目录】执行，指定 make 文件：
```shell
# 本地运行
make -f deploy/web/Makefile run-local

# 构建镜像
make -f deploy/web/Makefile build

# 打包分发k8s节点
make -f deploy/web/Makefile push-k8s-node

# 清理
make -f deploy/web/Makefile clean
```
# 目录

```text
game-project/
├─ cmd/                                   # 所有服务可执行入口，一个文件夹=一个二进制
│  ├─ webserver/
│  │  └─ main.go
│  ├─ gateserver/
│  │  └─ main.go
│  └─ gameserver/
│     └─ main.go
├─ api/                                    # 全局公共协议（全服务共用proto+生成pb.go）
│  ├─ proto/                               # 原始公共proto文件
│  │  ├─ base.proto
│  │  ├─ login.proto
│  │  └─ push.proto
│  ├─ pb/                                  # protoc自动生成 *.pb.go（提交仓库，不手动修改）
│  │  ├─ base.pb.go
│  │  ├─ login.pb.go
│  │  └─ push.pb.go
│  └─ model/                               # 全局公共结构体、错误码、常量、枚举
│     ├─ code.go
│     └─ const.go
├─ shared/                                 # 全服务共享底层公共代码（核心复用层）
│  ├─ infra/                               # 基础设施：外部中间件客户端封装
│  │  ├─ config/                           # viper统一配置解析
│  │  ├─ logger/                           # zap日志封装、日志切割、链路日志
│  │  ├─ mysql/                            # gorm连接池、事务、通用基础DAO
│  │  ├─ redis/                            # redis客户端、分布式锁、zset工具
│  │  └─ etcd/                             # etcd服务发现、配置订阅
│  └─ kit/                                 # 纯内存工具集，无网络/外部依赖
│     ├─ snowflake/
│     ├─ crypto/
│     ├─ timex/
│     ├─ strx/
│     └─ xerr/
├─ internal/                               # 仓库私有业务，服务完全隔离（Go internal私有约束）
│  ├─ web/                                 # web后台独有业务
│  │  ├─ handler/
│  │  ├─ service/
│  │  ├─ dao/
│  │  └─ proto/                            # web私有协议（不放入api）
│  ├─ gate/                                # 网关独有：长连接、会话、消息路由
│  │  ├─ session/
│  │  ├─ router/
│  │  ├─ net/
│  │  └─ proto/                            # 网关私有协议
│  └─ game/                                # 游戏逻辑服独有：房间、战斗、角色
│     ├─ room/
│     ├─ battle/
│     ├─ role/
│     ├─ dao/
│     └─ proto/                            # 战斗/房间私有协议
├─ configs/                                # 各服务独立yaml配置文件
│  ├─ web.yaml
│  ├─ gate.yaml
│  └─ game.yaml
├─ deploy/                                 # 部署相关：Docker、DockerCompose、K8s YAML
│  ├─ docker/
│  │  ├─ webserver/
│  │  │  └─ Dockerfile
│  │  ├─ gateserver/
│  │  │  └─ Dockerfile
│  │  └─ gameserver/
│  │     └─ Dockerfile
│  ├─ compose/                             # 本地测试 docker-compose.yml
│  │  └─ docker-compose.yml
│  └─ k8s/                                 # 集群部署yaml Deployment/Service/Ingress
│     ├─ web.yaml
│     ├─ gate.yaml
│     └─ game.yaml
├─ scripts/                                # 自动化脚本：生成proto、编译、镜像构建
│  ├─ gen_proto.sh                         # 一键编译api下所有proto，输出pb/
│  ├─ build.sh                             # 批量编译所有服务二进制
│  └─ build_image.sh                       # 一键构建全部服务Docker镜像
├─ go.mod
├─ go.sum
└─ .gitignore
```

# mq
程序访问 endpoint  127.0.0.1:8081
可视化界面 http://localhost:8080