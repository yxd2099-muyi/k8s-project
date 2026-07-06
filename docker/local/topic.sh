#!/bin/bash

# ====================== 配置 ======================
BROKER_CONTAINER="rmq-broker"
NAMESRV_ADDR="rmq-namesrv:9876"
CLUSTER_NAME="DefaultCluster"

# 格式说明：
# topic:消息类型:消费组(可选，仅fifo时建议填写)
# 示例：
#   push_events:fifo:chat-push
#   system_notify:normal
TOPIC_LIST=(
  "push_events:fifo:chat-push"
  # "chat_events:normal"
  # "system_notify:normal"
  # "test_fifo:fifo:my-fifo-group"
)

READ_QUEUE_NUM=8
WRITE_QUEUE_NUM=8
PERM=6
# ================================================

echo "===== RocketMQ 5.x Topic & ConsumerGroup 初始化脚本 ====="
echo "Namesrv: $NAMESRV_ADDR | Cluster: $CLUSTER_NAME"
echo "=================================================="

for item in "${TOPIC_LIST[@]}"; do
  # 拆分参数
  IFS=':' read -r topic msgType groupName <<< "$item"

  echo ">>> 处理 Topic: $topic | 类型: $msgType | 消费组: ${groupName:-无}"

  # 1. 创建/更新 Topic
  echo "   → 处理 Topic..."

  if [ "$msgType" = "fifo" ]; then
    # FIFO Topic 使用 updateTopic + -a +message.type=FIFO
    cmd="/home/rocketmq/rocketmq-5.3.2/bin/mqadmin updateTopic \
      -n $NAMESRV_ADDR \
      -t $topic \
      -c $CLUSTER_NAME \
      -r $READ_QUEUE_NUM \
      -w $WRITE_QUEUE_NUM \
      -p $PERM \
      -a +message.type=FIFO \
      -o true"

    docker exec "$BROKER_CONTAINER" sh -c "$cmd"
    if [ $? -eq 0 ]; then
      echo "✅ FIFO Topic 创建/更新成功: $topic"
    else
      echo "❌ FIFO Topic 操作失败: $topic"
    fi

  else
    # Normal Topic
    cmd="/home/rocketmq/rocketmq-5.3.2/bin/mqadmin updateTopic \
      -n $NAMESRV_ADDR \
      -t $topic \
      -c $CLUSTER_NAME \
      -r $READ_QUEUE_NUM \
      -w $WRITE_QUEUE_NUM \
      -p $PERM \
      -a +message.type=NORMAL"

    docker exec "$BROKER_CONTAINER" sh -c "$cmd"
    if [ $? -eq 0 ]; then
      echo "✅ Normal Topic 创建/更新成功: $topic"
    else
      echo "❌ Normal Topic 操作失败: $topic"
    fi
  fi

  # 2. 如果是 FIFO 且提供了消费组，则同时创建/更新有序消费组
  if [ "$msgType" = "fifo" ] && [ -n "$groupName" ]; then
    echo "   → 处理有序消费组: $groupName"

    groupCmd="/home/rocketmq/rocketmq-5.3.2/bin/mqadmin updateSubGroup \
      -n $NAMESRV_ADDR \
      -c $CLUSTER_NAME \
      -g $groupName \
      -o true"

    docker exec "$BROKER_CONTAINER" sh -c "$groupCmd"
    if [ $? -eq 0 ]; then
      echo "✅ 有序消费组创建/更新成功: $groupName"
    else
      echo "❌ 有序消费组操作失败: $groupName"
    fi
  fi

  echo "-----------------------------------------"
done

echo "===== 所有 Topic 初始化完成 ====="
echo "当前集群 Topic 列表："
docker exec "$BROKER_CONTAINER" sh -c \
  "/home/rocketmq/rocketmq-5.3.2/bin/mqadmin topicList -n $NAMESRV_ADDR"

echo "当前集群 Consumer Group 列表："
docker exec "$BROKER_CONTAINER" sh -c \
  "/home/rocketmq/rocketmq-5.3.2/bin/mqadmin consumerList -n $NAMESRV_ADDR"