"""
Scenario: Message Processing Logic Comparison

Compares performance of different message processing patterns:
1. Simple processing: Extract field and log
2. Complex processing: Aggregation, transformation, validation

Expected Outcome:
- Simple: Fast processing, minimal CPU
- Complex: Higher CPU usage in business logic
- Pyroscope should clearly show the difference in processing time
"""

import logging
import os
import time
import hashlib
from typing import Dict, Any
from src.py_flamegraph.kafka_client import ProfiledProducer, ProfiledConsumer, create_topics
from src.py_flamegraph.serdes import get_json_serializers, create_message

logger = logging.getLogger(__name__)


def simple_processing(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simple message processing: just extract a field.

    Args:
        message: Input message

    Returns:
        Processed result
    """
    # Just extract the ID and timestamp
    return {
        "id": message.get("id"),
        "timestamp": message.get("timestamp"),
        "processed": True
    }


def complex_processing(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Complex message processing: aggregation, transformation, validation.

    Args:
        message: Input message

    Returns:
        Processed result
    """
    # Extract fields
    msg_id = message.get("id", "")
    timestamp = message.get("timestamp", 0)
    payload = message.get("payload", "")
    metadata = message.get("metadata", {})

    # Complex transformations
    # 1. Calculate hash of payload
    payload_hash = hashlib.sha256(payload.encode()).hexdigest()

    # 2. Aggregate statistics
    char_freq = {}
    for char in payload[:100]:  # Sample first 100 chars
        char_freq[char] = char_freq.get(char, 0) + 1

    # 3. Perform some calculations
    stats = {
        "length": len(payload),
        "unique_chars": len(char_freq),
        "avg_freq": sum(char_freq.values()) / len(char_freq) if char_freq else 0,
        "hash": payload_hash[:16]  # Truncate
    }

    # 4. Validation rules
    is_valid = (
        len(msg_id) > 0 and
        timestamp > 0 and
        len(payload) > 0 and
        isinstance(metadata, dict)
    )

    # 5. Enrichment
    enriched_metadata = {
        **metadata,
        "processed_at": int(time.time() * 1000),
        "processing_version": "2.0"
    }

    return {
        "id": msg_id,
        "timestamp": timestamp,
        "stats": stats,
        "valid": is_valid,
        "metadata": enriched_metadata,
        "processed": True
    }


def run_processing_logic_comparison(
    bootstrap_servers: str,
    schema_registry_url: str,
    message_count: int = 1000,
    payload_size: int = 100
):
    """
    Run the processing logic comparison scenario.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        schema_registry_url: Schema Registry URL (not used, but kept for consistency)
        message_count: Number of messages to process
        payload_size: Size of message payload
    """
    # Topics
    topic_simple = os.getenv('TOPIC_SIMPLE', 'py-flamegraph-simple')
    topic_complex = os.getenv('TOPIC_COMPLEX', 'py-flamegraph-complex')

    # Create topics
    logger.info("Creating topics...")
    create_topics(bootstrap_servers, [topic_simple, topic_complex])

    # Get serializers (using JSON for this scenario)
    json_serializer, json_deserializer = get_json_serializers()

    # ============================================
    # Part 1: Produce messages to both topics
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info(f"Part 1: Producing {message_count} messages to both topics")
    logger.info(f"{'='*60}")

    producer = ProfiledProducer(
        bootstrap_servers,
        value_serializer=json_serializer
    )

    start_time = time.time()
    for i in range(message_count):
        message = create_message(i, payload_size)

        # Produce to both topics
        producer.produce(topic_simple, value=message)
        producer.produce(topic_complex, value=message)

        if (i + 1) % 100 == 0:
            producer.poll(0)

    producer.flush()
    elapsed = time.time() - start_time
    logger.info(f"Production complete: {message_count * 2} messages in {elapsed:.2f}s")

    # ============================================
    # Part 2: Consume with Simple Processing
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info(f"Part 2: Consuming {message_count} messages with SIMPLE processing")
    logger.info(f"{'='*60}")

    consumer_simple = ProfiledConsumer(
        bootstrap_servers,
        group_id='processing-logic-simple',
        value_deserializer=json_deserializer,
        auto_offset_reset='earliest'
    )
    consumer_simple.subscribe([topic_simple])

    consumed = 0
    processed_results = []
    start_time = time.time()

    while consumed < message_count:
        messages = consumer_simple.consume(
            num_messages=100,
            timeout=5.0,
            tags={"processing": "simple"}
        )

        for msg in messages:
            result = simple_processing(msg['value'])
            processed_results.append(result)

        consumed += len(messages)

        if consumed % 100 == 0:
            logger.info(f"Simple processing: {consumed}/{message_count} messages")

    elapsed = time.time() - start_time
    logger.info(f"Simple processing complete: {consumed} messages in {elapsed:.2f}s ({consumed/elapsed:.0f} msg/s)")
    consumer_simple.close()

    # ============================================
    # Part 3: Consume with Complex Processing
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info(f"Part 3: Consuming {message_count} messages with COMPLEX processing")
    logger.info(f"{'='*60}")

    consumer_complex = ProfiledConsumer(
        bootstrap_servers,
        group_id='processing-logic-complex',
        value_deserializer=json_deserializer,
        auto_offset_reset='earliest'
    )
    consumer_complex.subscribe([topic_complex])

    consumed = 0
    processed_results = []
    start_time = time.time()

    while consumed < message_count:
        messages = consumer_complex.consume(
            num_messages=100,
            timeout=5.0,
            tags={"processing": "complex"}
        )

        for msg in messages:
            result = complex_processing(msg['value'])
            processed_results.append(result)

        consumed += len(messages)

        if consumed % 100 == 0:
            logger.info(f"Complex processing: {consumed}/{message_count} messages")

    elapsed = time.time() - start_time
    logger.info(f"Complex processing complete: {consumed} messages in {elapsed:.2f}s ({consumed/elapsed:.0f} msg/s)")
    consumer_complex.close()

    # ============================================
    # Summary
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info("Scenario Complete!")
    logger.info(f"{'='*60}")
    logger.info("View profiles in Pyroscope UI and compare:")
    logger.info(f"  - Filter by processing=simple vs processing=complex")
    logger.info(f"  - Complex should show higher CPU in business logic")
    logger.info(f"  - Look for hashlib, dict operations in complex processing")
    logger.info(f"{'='*60}\n")
