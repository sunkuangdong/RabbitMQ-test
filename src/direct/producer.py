"""Publish messages that are routed by an exact routing-key match."""

from src.config import create_connection, encode_message, persistent_json_properties

EXCHANGE = "doc.event.direct"


def main() -> None:
    tasks = [
        ("info", {"level": "info", "text": "文档解析完成"}),
        ("warning", {"level": "warning", "text": "文档页数过多，耗时较长"}),
        ("error", {"level": "error", "text": "OCR 识别失败"}),
    ]

    connection = create_connection()
    try:
        channel = connection.channel()
        channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type="direct",
            durable=True,
        )

        for routing_key, message in tasks:
            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=routing_key,
                body=encode_message(message),
                properties=persistent_json_properties(),
            )
            print(f"[direct producer] 已发送 routing_key={routing_key}: {message}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()

