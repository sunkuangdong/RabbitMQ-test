"""Consume one severity from a direct exchange."""

import argparse

import pika

from src.config import create_connection, decode_message

EXCHANGE = "doc.event.direct"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Direct exchange consumer")
    parser.add_argument(
        "routing_key",
        choices=["info", "warning", "error"],
        nargs="?",
        default="info",
        help="Only consume messages with this exact routing key",
    )
    return parser.parse_args()


def main() -> None:
    routing_key = parse_args().routing_key
    queue_name = f"doc.direct.{routing_key}"
    connection = create_connection()
    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="direct",
        durable=True,
    )
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(
        queue=queue_name,
        exchange=EXCHANGE,
        routing_key=routing_key,
    )
    channel.basic_qos(prefetch_count=1)

    def on_message(
        ch: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        _properties: pika.BasicProperties,
        body: bytes,
    ) -> None:
        try:
            data = decode_message(body)
            print(f"[direct/{routing_key}] 收到: {data}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            print(f"[direct/{routing_key}] 处理失败: {exc}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=queue_name, on_message_callback=on_message)
    print(f"[direct] 正在监听 queue={queue_name}，按 Ctrl+C 停止")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()

