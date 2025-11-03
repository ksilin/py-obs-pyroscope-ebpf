"""
Kafka producer and consumer implementations.

Provides:
1. ProfiledProducer - Kafka producer (previously with Pyroscope tagging, now profiled via eBPF)
2. ProfiledConsumer - Kafka consumer (previously with Pyroscope tagging, now profiled via eBPF)

Note: Profiling is now handled by Grafana Alloy using eBPF, no SDK tagging needed.
"""

import logging
from typing import Dict, Any, Optional, Callable
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)


class ProfiledProducer:
    """
    Kafka producer with Pyroscope profiling integration.

    Automatically tags profiles with topic, operation, and custom tags.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        value_serializer: Optional[Callable] = None,
        key_serializer: Optional[Callable] = None,
        **kwargs
    ):
        """
        Initialize the profiled producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            value_serializer: Optional value serializer
            key_serializer: Optional key serializer
            **kwargs: Additional producer configuration
        """
        config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'py-flamegraph-producer',
            **kwargs
        }
        self.producer = Producer(config)
        self.value_serializer = value_serializer
        self.key_serializer = key_serializer

    def produce(
        self,
        topic: str,
        value: Any,
        key: Optional[Any] = None,
        tags: Optional[Dict[str, str]] = None,
        callback: Optional[Callable] = None
    ):
        """
        Produce a message (profiled automatically by Alloy eBPF).

        Args:
            topic: Kafka topic
            value: Message value
            key: Optional message key
            tags: Optional tags (kept for API compatibility, not used)
            callback: Optional delivery callback
        """
        # Serialize value
        serialized_value = None
        if self.value_serializer and value is not None:
            ctx = SerializationContext(topic, MessageField.VALUE)
            serialized_value = self.value_serializer(value, ctx)
        else:
            serialized_value = value

        # Serialize key if present
        serialized_key = None
        if key is not None:
            if self.key_serializer:
                ctx = SerializationContext(topic, MessageField.KEY)
                serialized_key = self.key_serializer(key, ctx)
            else:
                serialized_key = key

        # Produce message
        self.producer.produce(
            topic,
            value=serialized_value,
            key=serialized_key,
            callback=callback
        )

    def flush(self, timeout: float = -1):
        """
        Flush pending messages.

        Args:
            timeout: Timeout in seconds (-1 for infinite)
        """
        self.producer.flush(timeout)

    def poll(self, timeout: float = 0):
        """
        Poll for delivery reports.

        Args:
            timeout: Timeout in seconds
        """
        return self.producer.poll(timeout)


class ProfiledConsumer:
    """
    Kafka consumer with Pyroscope profiling integration.

    Automatically tags profiles with topic, consumer group, and custom tags.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        value_deserializer: Optional[Callable] = None,
        key_deserializer: Optional[Callable] = None,
        auto_offset_reset: str = 'earliest',
        **kwargs
    ):
        """
        Initialize the profiled consumer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            group_id: Consumer group ID
            value_deserializer: Optional value deserializer
            key_deserializer: Optional key deserializer
            auto_offset_reset: Auto offset reset policy
            **kwargs: Additional consumer configuration
        """
        config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': True,
            **kwargs
        }
        self.consumer = Consumer(config)
        self.value_deserializer = value_deserializer
        self.key_deserializer = key_deserializer
        self.group_id = group_id

    def subscribe(self, topics: list):
        """Subscribe to topics."""
        self.consumer.subscribe(topics)

    def consume(
        self,
        num_messages: int = 1,
        timeout: float = 1.0,
        tags: Optional[Dict[str, str]] = None
    ) -> list:
        """
        Consume messages (profiled automatically by Alloy eBPF).

        Args:
            num_messages: Number of messages to consume
            timeout: Timeout in seconds
            tags: Optional tags (kept for API compatibility, not used)

        Returns:
            List of consumed messages
        """
        messages = self.consumer.consume(num_messages=num_messages, timeout=timeout)

        result = []
        for msg in messages:
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            # Deserialize value
            value = msg.value()
            if self.value_deserializer and value is not None:
                ctx = SerializationContext(msg.topic(), MessageField.VALUE)
                value = self.value_deserializer(value, ctx)

            # Deserialize key if present
            key = msg.key()
            if key is not None and self.key_deserializer:
                ctx = SerializationContext(msg.topic(), MessageField.KEY)
                key = self.key_deserializer(key, ctx)

            result.append({
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'key': key,
                'value': value
            })

        return result

    def poll(self, timeout: float = 1.0, tags: Optional[Dict[str, str]] = None):
        """
        Poll for a single message (profiled automatically by Alloy eBPF).

        Args:
            timeout: Timeout in seconds
            tags: Optional tags (kept for API compatibility, not used)

        Returns:
            Message or None
        """
        msg = self.consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            else:
                raise KafkaException(msg.error())

        # Deserialize value
        value = msg.value()
        if self.value_deserializer and value is not None:
            ctx = SerializationContext(msg.topic(), MessageField.VALUE)
            value = self.value_deserializer(value, ctx)

        # Deserialize key if present
        key = msg.key()
        if key is not None and self.key_deserializer:
            ctx = SerializationContext(msg.topic(), MessageField.KEY)
            key = self.key_deserializer(key, ctx)

        return {
            'topic': msg.topic(),
            'partition': msg.partition(),
            'offset': msg.offset(),
            'key': key,
            'value': value
        }

    def close(self):
        """Close the consumer."""
        self.consumer.close()


def create_topics(bootstrap_servers: str, topics: list, num_partitions: int = 1, replication_factor: int = 1):
    """
    Create Kafka topics if they don't exist.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        topics: List of topic names to create
        num_partitions: Number of partitions per topic
        replication_factor: Replication factor
    """
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # Get existing topics
    metadata = admin_client.list_topics(timeout=10)
    existing_topics = set(metadata.topics.keys())

    # Create topics that don't exist
    new_topics = [
        NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)
        for topic in topics
        if topic not in existing_topics
    ]

    if new_topics:
        logger.info(f"Creating topics: {[t.topic for t in new_topics]}")
        futures = admin_client.create_topics(new_topics)

        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic {topic} created successfully")
            except Exception as e:
                logger.warning(f"Failed to create topic {topic}: {e}")
    else:
        logger.info("All topics already exist")
