# RabbitMQ Python 学习示例

这个项目用 Python 和 Pika 演示 RabbitMQ 的四种经典交换机：

- `direct`：routing key 必须完全匹配。
- `fanout`：忽略 routing key，广播到所有绑定队列。
- `topic`：使用 `*` 和 `#` 通配 routing key。
- `headers`：忽略 routing key，按照消息 headers 匹配。

消息的基本流向是：

```text
Producer -> Exchange -> Queue -> Consumer
```

## 第 1 步：启动 RabbitMQ

在本目录执行：

```bash
docker compose up -d
docker compose ps
```

管理页面是 <http://localhost:15672>，用户名为 `admin`，密码为
`Admin@123456`。

## 第 2 步：激活虚拟环境并安装依赖

`.venv` 位于本项目的上一级目录，因此执行：

```bash
source ../.venv/bin/activate
python -m pip install -r requirements.txt
```

后续命令都在 `rabbitmg-test` 目录中执行。建议先启动 Consumer，确保队列和绑定
已经创建，再运行 Producer；消息发布时若没有任何匹配的队列，RabbitMQ 默认会丢弃该
消息。

## 第 3 步：Direct——精确路由

打开三个终端，分别监听三个 routing key：

```bash
python -m src.direct.consumer info
python -m src.direct.consumer warning
python -m src.direct.consumer error
```

再打开一个终端发送消息：

```bash
python -m src.direct.producer
```

观察结果：每个 Consumer 只收到 routing key 与自己完全相等的消息。

## 第 4 步：Fanout——广播

启动两个不同用途的 Consumer：

```bash
python -m src.fanout.consumer vector
python -m src.fanout.consumer elasticsearch
```

发送一条文档消息：

```bash
python -m src.fanout.producer
```

观察结果：两个 Consumer 都会各收到一份相同消息。Fanout 完全不看 routing key。

## 第 5 步：Topic——通配符路由

可以打开四个终端测试四种绑定：

```bash
python -m src.topic.consumer 'doc.*.parsed'
python -m src.topic.consumer 'doc.pdf.#'
python -m src.topic.consumer 'doc.#'
python -m src.topic.consumer '#.failed'
```

发送四条不同 routing key 的消息：

```bash
python -m src.topic.producer
```

规则：

- `*` 只匹配一个单词，例如 `doc.*.parsed`。
- `#` 匹配零个或多个单词，例如 `doc.pdf.#`。
- 单词之间用英文句点 `.` 分隔。

## 第 6 步：Headers——根据消息头路由

启动 Consumer，要求 `format=pdf` 和 `priority=high` 两项都匹配：

```bash
python -m src.headers.consumer --format pdf --priority high --match all
```

发送三条带不同 headers 的消息：

```bash
python -m src.headers.producer
```

观察结果：只有 headers 同时为 `format=pdf`、`priority=high` 的消息被收到。
把 `--match all` 改成 `--match any` 后，只要任一条件匹配即可路由到队列。

## 阅读代码时抓住这五个调用

1. `exchange_declare`：声明交换机及其类型。
2. `queue_declare`：声明真正保存消息的队列。
3. `queue_bind`：设置交换机到队列的路由规则。
4. `basic_publish`：Producer 把消息交给交换机。
5. `basic_consume`：Consumer 监听队列并处理消息。

示例使用手动确认：成功后调用 `basic_ack`，失败后调用 `basic_nack`。同时，交换机、
队列和消息均设置为持久化，适合观察 RabbitMQ 重启后的行为。

## 停止服务

```bash
docker compose down
```

`rabbitmq_data/` 保存 RabbitMQ 数据并已写入 `.gitignore`，停止容器不会自动删除它。
