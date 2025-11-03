"""
Serialization/Deserialization implementations for Kafka messages.

Provides:
1. Simple JSON serializer (no Schema Registry)
2. Schema Registry-aware Avro serializer (using Confluent's implementation)
"""

import json
import time
from typing import Dict, Any, Optional
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer


# Simple message schema for demo
MESSAGE_SCHEMA_STR = """
{
  "type": "record",
  "name": "DemoMessage",
  "namespace": "com.example.demo",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "payload", "type": "string"},
    {"name": "metadata", "type": {"type": "map", "values": "string"}, "default": {}}
  ]
}
"""


class JsonSerializer:
    """Simple JSON serializer without Schema Registry."""

    def __call__(self, obj: Dict[str, Any], ctx: SerializationContext) -> bytes:
        """Serialize dict to JSON bytes."""
        if obj is None:
            return None
        return json.dumps(obj).encode('utf-8')


class JsonDeserializer:
    """Simple JSON deserializer without Schema Registry."""

    def __call__(self, data: bytes, ctx: SerializationContext) -> Dict[str, Any]:
        """Deserialize JSON bytes to dict."""
        if data is None:
            return None
        return json.loads(data.decode('utf-8'))


def create_message(msg_id: int, payload_size: int = 100) -> Dict[str, Any]:
    """
    Create a test message with the expected schema.

    Args:
        msg_id: Unique message identifier
        payload_size: Size of the payload string in characters

    Returns:
        Dictionary matching the DemoMessage schema
    """
    return {
        "id": f"msg-{msg_id:06d}",
        "timestamp": int(time.time() * 1000),
        "payload": "x" * payload_size,
        "metadata": {
            "source": "demo",
            "version": "1.0"
        }
    }


def get_json_serializers() -> tuple:
    """
    Get JSON serializer and deserializer (no Schema Registry).

    Returns:
        Tuple of (serializer, deserializer)
    """
    return JsonSerializer(), JsonDeserializer()


def get_avro_serializers(schema_registry_url: str) -> tuple:
    """
    Get Schema Registry-aware Avro serializer and deserializer.

    Args:
        schema_registry_url: URL of the Schema Registry server

    Returns:
        Tuple of (serializer, deserializer)
    """
    schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})

    # Avro serializer - automatically registers schema with SR
    avro_serializer = AvroSerializer(
        schema_registry_client,
        MESSAGE_SCHEMA_STR,
        lambda obj, ctx: obj  # Just return the dict as-is
    )

    # Avro deserializer - automatically retrieves schema from SR
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        MESSAGE_SCHEMA_STR,
        lambda obj, ctx: obj  # Just return the dict as-is
    )

    return avro_serializer, avro_deserializer


def dict_to_avro(obj: Dict[str, Any], ctx: SerializationContext) -> Dict[str, Any]:
    """
    Helper function to convert dict to Avro-compatible format.
    Required by AvroSerializer.
    """
    return obj


def avro_to_dict(obj: Dict[str, Any], ctx: SerializationContext) -> Dict[str, Any]:
    """
    Helper function to convert Avro object back to dict.
    Required by AvroDeserializer.
    """
    return obj
