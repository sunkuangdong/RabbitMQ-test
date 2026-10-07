"""Shared RabbitMQ connection and JSON helpers."""

from __future__ import annotations

import json
import os
from typing import Any

import pika


def create_connection() -> pika.BlockingConnection:
    """Create a connection using the same defaults as docker-compose.yml."""
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "admin"),
        os.getenv("RABBITMQ_PASSWORD", "Admin@123456"),
    )
    parameters = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        port=int(os.getenv("RABBITMQ_PORT", "5672")),
        virtual_host=os.getenv("RABBITMQ_VHOST", "/"),
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=30,
    )
    return pika.BlockingConnection(parameters)


def encode_message(message: dict[str, Any]) -> bytes:
    """Encode a Python dictionary as UTF-8 JSON bytes."""
    return json.dumps(message, ensure_ascii=False).encode("utf-8")


def decode_message(body: bytes) -> dict[str, Any]:
    """Decode RabbitMQ message bytes into a Python dictionary."""
    return json.loads(body.decode("utf-8"))


def persistent_json_properties(
    *, headers: dict[str, Any] | None = None
) -> pika.BasicProperties:
    """Mark a JSON message as persistent and optionally attach headers."""
    return pika.BasicProperties(
        content_type="application/json",
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=headers,
    )

