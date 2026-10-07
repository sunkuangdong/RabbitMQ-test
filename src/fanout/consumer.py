"""Run one of two independent consumers of the same fanout event."""

import argparse

import pika

from src.config import create_connection, decode_message

EXCHANGE = "doc.event.fanout"
QUEUES = {
    "vector": "doc.vectorize",
    "elasticsearch": "doc.elasticsearch",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fanout exchange consumer")
    parser.add_argument("service", choices=QUEUES, help="Consumer to run")
    return parser.parse_args()


def main() -> None:
    service = parse_args().service
    queue_name = QUEUES[service]
    connection = create_connection()
    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="fanout",
        durable=True,
    )
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(queue=queue_name, exchange=EXCHANGE)
    channel.basic_qos(prefetch_count=1)

    def on_message(
        ch: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        _properties: pika.BasicProperties,
        body: bytes,
    ) -> None:
        try:
            data = decode_message(body)
            action = "写入 Milvus" if service == "vector" else "写入 Elasticsearch"
            print(f"[fanout/{service}] 收到 {data['source']}，模拟{action}: {data['docId']}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            print(f"[fanout/{service}] 处理失败: {exc}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=queue_name, on_message_callback=on_message)
    print(f"[fanout] 正在监听 queue={queue_name}，按 Ctrl+C 停止")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()

