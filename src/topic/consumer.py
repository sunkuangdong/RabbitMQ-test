"""Consume topic messages using * and # wildcard binding patterns."""

import argparse
import re

import pika

from src.config import create_connection, decode_message

EXCHANGE = "doc.event.topic"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Topic exchange consumer")
    parser.add_argument(
        "binding_key",
        nargs="?",
        default="doc.*.parsed",
        help="For example: doc.*.parsed, doc.pdf.#, doc.# or #.failed",
    )
    parser.add_argument("--queue", help="Optional explicit durable queue name")
    return parser.parse_args()


def default_queue_name(binding_key: str) -> str:
    readable = binding_key.replace("*", "star").replace("#", "hash")
    safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", readable)
    return f"topic.{safe_name}"


def main() -> None:
    args = parse_args()
    queue_name = args.queue or default_queue_name(args.binding_key)
    connection = create_connection()
    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic",
        durable=True,
    )
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(
        queue=queue_name,
        exchange=EXCHANGE,
        routing_key=args.binding_key,
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
            print(
                f"[topic] routing_key={method.routing_key}, "
                f"binding={args.binding_key}, 内容={data}"
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            print(f"[topic] 处理失败: {exc}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=queue_name, on_message_callback=on_message)
    print(
        f"[topic] 正在监听 queue={queue_name}, "
        f"binding_key={args.binding_key}，按 Ctrl+C 停止"
    )
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()

