"""Publish document events with multi-segment routing keys."""

from src.config import create_connection, encode_message, persistent_json_properties

EXCHANGE = "doc.event.topic"


def main() -> None:
    events = [
        ("doc.pdf.parsed", {"type": "parsed", "format": "pdf"}),
        ("doc.docx.parsed", {"type": "parsed", "format": "docx"}),
        ("doc.pptx.failed", {"type": "failed", "format": "pptx"}),
        ("doc.pdf.failed", {"type": "failed", "format": "pdf"}),
    ]

    connection = create_connection()
    try:
        channel = connection.channel()
        channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type="topic",
            durable=True,
        )

        for routing_key, message in events:
            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=routing_key,
                body=encode_message(message),
                properties=persistent_json_properties(),
            )
            print(f"[topic producer] 已发送 routing_key={routing_key}: {message}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()

