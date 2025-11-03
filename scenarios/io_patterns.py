"""
Scenario: I/O Patterns Comparison

Compares performance of different I/O patterns:
1. Batch production: Produce many messages, then flush
2. Streaming production: Flush after each message

Expected Outcome:
- Batch: Higher throughput, less network overhead
- Streaming: Lower throughput, more flush operations
- Pyroscope should show time spent in flush operations
"""

import logging
import os
import time
from src.py_flamegraph.kafka_client import ProfiledProducer, ProfiledConsumer, create_topics
from src.py_flamegraph.serdes import get_json_serializers, create_message

logger = logging.getLogger(__name__)


def run_io_patterns_comparison(
    bootstrap_servers: str,
    schema_registry_url: str,
    message_count: int = 1000,
    payload_size: int = 100,
    batch_size: int = 100
):
    """
    Run the I/O patterns comparison scenario.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        schema_registry_url: Schema Registry URL (not used, kept for consistency)
        message_count: Number of messages to produce
        payload_size: Size of message payload
        batch_size: Batch size for batch production
    """
    # Topics
    topic_batch = os.getenv('TOPIC_BATCH', 'py-flamegraph-batch')
    topic_streaming = os.getenv('TOPIC_STREAMING', 'py-flamegraph-streaming')

    # Create topics
    logger.info("Creating topics...")
    create_topics(bootstrap_servers, [topic_batch, topic_streaming])

    # Get serializers (using JSON for this scenario)
    json_serializer, json_deserializer = get_json_serializers()

    # ============================================
    # Part 1: Batch Production Pattern
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info(f"Part 1: BATCH production - produce {batch_size} messages, then flush")
    logger.info(f"{'='*60}")

    producer_batch = ProfiledProducer(
        bootstrap_servers,
        value_serializer=json_serializer
    )

    start_time = time.time()
    messages_produced = 0

    for i in range(message_count):
        message = create_message(i, payload_size)
        producer_batch.produce(
            topic_batch,
            value=message,
            tags={"io_pattern": "batch"}
        )
        messages_produced += 1

        # Flush every batch_size messages
        if (i + 1) % batch_size == 0:
            producer_batch.flush()
            logger.info(f"Batch: Flushed after {i + 1} messages")

    # Final flush for remaining messages
    producer_batch.flush()

    elapsed = time.time() - start_time
    flush_count = (message_count // batch_size) + (1 if message_count % batch_size > 0 else 0)
    logger.info(f"Batch production complete: {messages_produced} messages in {elapsed:.2f}s")
    logger.info(f"  ({messages_produced/elapsed:.0f} msg/s, {flush_count} flush operations)")

    # ============================================
    # Part 2: Streaming Production Pattern
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info(f"Part 2: STREAMING production - flush after EVERY message")
    logger.info(f"{'='*60}")

    producer_streaming = ProfiledProducer(
        bootstrap_servers,
        value_serializer=json_serializer
    )

    start_time = time.time()
    messages_produced = 0

    for i in range(message_count):
        message = create_message(i, payload_size)
        producer_streaming.produce(
            topic_streaming,
            value=message,
            tags={"io_pattern": "streaming"}
        )
        messages_produced += 1

        # Flush after EVERY message (streaming pattern)
        producer_streaming.flush()

        if (i + 1) % 100 == 0:
            logger.info(f"Streaming: Produced and flushed {i + 1} messages")

    elapsed = time.time() - start_time
    logger.info(f"Streaming production complete: {messages_produced} messages in {elapsed:.2f}s")
    logger.info(f"  ({messages_produced/elapsed:.0f} msg/s, {messages_produced} flush operations)")

    # ============================================
    # Part 3: Batch Consumption
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info(f"Part 3: Consuming {message_count} messages from BATCH topic")
    logger.info(f"{'='*60}")

    consumer_batch = ProfiledConsumer(
        bootstrap_servers,
        group_id='io-patterns-batch',
        value_deserializer=json_deserializer,
        auto_offset_reset='earliest'
    )
    consumer_batch.subscribe([topic_batch])

    consumed = 0
    start_time = time.time()

    while consumed < message_count:
        messages = consumer_batch.consume(
            num_messages=100,
            timeout=5.0,
            tags={"io_pattern": "batch"}
        )
        consumed += len(messages)

        if consumed % 100 == 0:
            logger.info(f"Batch consumption: {consumed}/{message_count} messages")

    elapsed = time.time() - start_time
    logger.info(f"Batch consumption complete: {consumed} messages in {elapsed:.2f}s ({consumed/elapsed:.0f} msg/s)")
    consumer_batch.close()

    # ============================================
    # Part 4: Streaming Consumption
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info(f"Part 4: Consuming {message_count} messages from STREAMING topic")
    logger.info(f"{'='*60}")

    consumer_streaming = ProfiledConsumer(
        bootstrap_servers,
        group_id='io-patterns-streaming',
        value_deserializer=json_deserializer,
        auto_offset_reset='earliest'
    )
    consumer_streaming.subscribe([topic_streaming])

    consumed = 0
    start_time = time.time()

    while consumed < message_count:
        messages = consumer_streaming.consume(
            num_messages=100,
            timeout=5.0,
            tags={"io_pattern": "streaming"}
        )
        consumed += len(messages)

        if consumed % 100 == 0:
            logger.info(f"Streaming consumption: {consumed}/{message_count} messages")

    elapsed = time.time() - start_time
    logger.info(f"Streaming consumption complete: {consumed} messages in {elapsed:.2f}s ({consumed/elapsed:.0f} msg/s)")
    consumer_streaming.close()

    # ============================================
    # Summary
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info("Scenario Complete!")
    logger.info(f"{'='*60}")
    logger.info("View profiles in Pyroscope UI and compare:")
    logger.info(f"  - Filter by io_pattern=batch vs io_pattern=streaming")
    logger.info(f"  - Streaming should show much more time in flush operations")
    logger.info(f"  - Batch should show higher throughput with fewer flushes")
    logger.info(f"{'='*60}\n")
