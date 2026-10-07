"""Consume messages by matching AMQP header key/value pairs."""

import argparse

import pika

from src.config import create_connection, decode_message

EXCHANGE = "doc.event.headers"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Headers exchange consumer")
    parser.add_argument("--format", default="pdf")
    parser.add_argument("--priority", default="high")
    parser.add_argument(
        "--match",
        choices=["all", "any"],
        default="all",
        help="all: every header must match; any: at least one must match",
    )
    parser.add_argument("--queue", default="doc.headers.pdf.high")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bind_arguments = {
        "x-match": args.match,
        "format": args.format,
        "priority": args.priority,
    }
    connection = create_connection()
    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="headers",
        durable=True,
    )
    channel.queue_declare(queue=args.queue, durable=True)
    channel.queue_bind(
        queue=args.queue,
        exchange=EXCHANGE,
        routing_key="",
        arguments=bind_arguments,
    )
    channel.basic_qos(prefetch_count=1)

    def on_message(
        ch: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.BasicProperties,
        body: bytes,
    ) -> None:
        try:
            data = decode_message(body)
            print(f"[headers] 收到 headers={properties.headers}, body={data}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            print(f"[headers] 处理失败: {exc}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=args.queue, on_message_callback=on_message)
    print(f"[headers] 正在监听 queue={args.queue}, 匹配条件={bind_arguments}")
    print("按 Ctrl+C 停止")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()

