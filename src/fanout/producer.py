"""Broadcast one document event to every queue bound to the exchange."""

from src.config import create_connection, encode_message, persistent_json_properties

EXCHANGE = "doc.event.fanout"


def main() -> None:
    message = {
        "docId": "doc-1785059436458",
        "markdown": "# Hello RAG\n\n这是解析后的 Markdown 内容。",
        "source": "report.pdf",
    }

    connection = create_connection()
    try:
        channel = connection.channel()
        channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type="fanout",
            durable=True,
        )
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key="",  # fanout ignores the routing key
            body=encode_message(message),
            properties=persistent_json_properties(),
        )
        print(f"[fanout producer] 已广播: {message}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()

