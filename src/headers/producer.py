"""Publish messages whose AMQP headers control routing."""

from src.config import create_connection, encode_message, persistent_json_properties

EXCHANGE = "doc.event.headers"


def main() -> None:
    messages = [
        (
            {"format": "pdf", "priority": "high"},
            {"docId": "1", "note": "高优先级 PDF"},
        ),
        (
            {"format": "pdf", "priority": "low"},
            {"docId": "2", "note": "低优先级 PDF"},
        ),
        (
            {"format": "docx", "priority": "high"},
            {"docId": "3", "note": "高优先级 DOCX"},
        ),
    ]

    connection = create_connection()
    try:
        channel = connection.channel()
        channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type="headers",
            durable=True,
        )

        for headers, message in messages:
            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key="",  # headers exchange ignores the routing key
                body=encode_message(message),
                properties=persistent_json_properties(headers=headers),
            )
            print(f"[headers producer] 已发送 headers={headers}: {message}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()

