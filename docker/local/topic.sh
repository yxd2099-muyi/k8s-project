#!/bin/bash

# ====================== 配置 ======================
BROKER_CONTAINER="rmq-broker"
NAMESRV_ADDR="rmq-namesrv:9876"
CLUSTER_NAME="DefaultCluster"

# 格式：topic名称:类型，类型=normal / fifo
# push_events:normal 普通消息
# chat_events:fifo 顺序消息
TOPIC_LIST=(
  "push_events:fifo"
#  "chat_events:normal"
#  "system_notify:normal"
  # 新增示例 "test_fifo:fifo"
)

READ_QUEUE_NUM=8
WRITE_QUEUE_NUM=8
PERM=6
# ================================================

echo "===== RocketMQ 5.x Topic 初始化脚本 ====="

for item in "${TOPIC_LIST[@]}"; do
  # 拆分 topic名称 和 消息类型
  IFS=':' read -r topic msgType <<< "$item"
  echo ">>> 处理 Topic: $topic , 消息类型: $msgType"

  # 1. 判断Topic是否存在
  existCmd="/home/rocketmq/rocketmq-5.3.2/bin/mqadmin queryTopic -t $topic -n $NAMESRV_ADDR"
  existRet=$(docker exec "$BROKER_CONTAINER" sh -c "$existCmd >/dev/null 2>&1; echo \$?")

  if [ "$existRet" -ne 0 ]; then
    # 不存在 -> 创建Topic
    createCmd="/home/rocketmq/rocketmq-5.3.2/bin/mqadmin createTopic \
      -n $NAMESRV_ADDR \
      -t $topic \
      -c $CLUSTER_NAME \
      -w $WRITE_QUEUE_NUM"

    # fifo 顺序topic追加消息类型参数
    if [ "$msgType" = "fifo" ]; then
      createCmd+=" --message-type FIFO"
    fi

    docker exec "$BROKER_CONTAINER" sh -c "$createCmd"
    if [ $? -eq 0 ]; then
      echo "✅ 新建Topic成功[$msgType]: $topic"
    else
      echo "❌ 新建Topic失败[$msgType]: $topic"
    fi
  else
    # 已存在 -> 仅更新队列读写数量、权限（无法修改消息类型）
    updateCmd="/home/rocketmq/rocketmq-5.3.2/bin/mqadmin updatetopic \
      -n $NAMESRV_ADDR \
      -t $topic \
      -c $CLUSTER_NAME \
      -r $READ_QUEUE_NUM \
      -w $WRITE_QUEUE_NUM \
      -p $PERM"

    docker exec "$BROKER_CONTAINER" sh -c "$updateCmd"
    if [ $? -eq 0 ]; then
      echo "✅ 更新Topic队列配置成功: $topic"
    else
      echo "❌ 更新Topic队列配置失败: $topic"
    fi
  fi

  echo "-----------------------------------------"
done

echo "===== 所有Topic初始化完成 ====="
echo "当前集群全部Topic列表："
docker exec "$BROKER_CONTAINER" sh -c \
  "/home/rocketmq/rocketmq-5.3.2/bin/mqadmin topicList -n $NAMESRV_ADDR"
