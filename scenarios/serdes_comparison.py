"""
Scenario: Serialization/Deserialization Comparison

Compares performance of:
1. Simple JSON serialization (no Schema Registry)
2. Avro serialization with Schema Registry

Expected Outcome:
- JSON: Fast serialization, but larger message size
- Avro: Slower due to SR interaction, but compact messages
- Pyroscope should show CPU differences in serialization phase
"""

import logging
import os
import time
from src.py_flamegraph.kafka_client import ProfiledProducer, ProfiledConsumer, create_topics
from src.py_flamegraph.serdes import (
    get_json_serializers,
    get_avro_serializers,
    create_message
)

logger = logging.getLogger(__name__)


def run_serdes_comparison(
    bootstrap_servers: str,
    schema_registry_url: str,
    message_count: int = 1000,
    payload_size: int = 100
):
    """
    Run the serialization comparison scenario.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        schema_registry_url: Schema Registry URL
        message_count: Number of messages to produce/consume
        payload_size: Size of message payload in characters
    """
    # Topics
    topic_json = os.getenv('TOPIC_JSON', 'py-flamegraph-json')
    topic_avro = os.getenv('TOPIC_AVRO', 'py-flamegraph-avro')

    # Create topics
    logger.info("Creating topics...")
    create_topics(bootstrap_servers, [topic_json, topic_avro])

    # Get serializers
    json_serializer, json_deserializer = get_json_serializers()
    avro_serializer, avro_deserializer = get_avro_serializers(schema_registry_url)

    # ============================================
    # Part 1: JSON Serialization
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info("Part 1: Producing {message_count} messages with JSON serialization")
    logger.info(f"{'='*60}")

    producer_json = ProfiledProducer(
        bootstrap_servers,
        value_serializer=json_serializer
    )

    start_time = time.time()
    for i in range(message_count):
        message = create_message(i, payload_size)
        producer_json.produce(
            topic_json,
            value=message,
            tags={"serdes_type": "json"}
        )

        if (i + 1) % 100 == 0:
            producer_json.poll(0)

    producer_json.flush()
    elapsed = time.time() - start_time
    logger.info(f"JSON production complete: {message_count} messages in {elapsed:.2f}s ({message_count/elapsed:.0f} msg/s)")

    # ============================================
    # Part 2: Avro Serialization with SR
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info(f"Part 2: Producing {message_count} messages with Avro + Schema Registry")
    logger.info(f"{'='*60}")

    producer_avro = ProfiledProducer(
        bootstrap_servers,
        value_serializer=avro_serializer
    )

    start_time = time.time()
    for i in range(message_count):
        message = create_message(i, payload_size)
        producer_avro.produce(
            topic_avro,
            value=message,
            tags={"serdes_type": "avro"}
        )

        if (i + 1) % 100 == 0:
            producer_avro.poll(0)

    producer_avro.flush()
    elapsed = time.time() - start_time
    logger.info(f"Avro production complete: {message_count} messages in {elapsed:.2f}s ({message_count/elapsed:.0f} msg/s)")

    # ============================================
    # Part 3: Consume and Deserialize - JSON
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info(f"Part 3: Consuming {message_count} messages with JSON deserialization")
    logger.info(f"{'='*60}")

    consumer_json = ProfiledConsumer(
        bootstrap_servers,
        group_id='serdes-comparison-json',
        value_deserializer=json_deserializer,
        auto_offset_reset='earliest'
    )
    consumer_json.subscribe([topic_json])

    consumed_json = 0
    start_time = time.time()
    while consumed_json < message_count:
        messages = consumer_json.consume(
            num_messages=100,
            timeout=5.0,
            tags={"serdes_type": "json"}
        )
        consumed_json += len(messages)

        if consumed_json % 100 == 0:
            logger.info(f"Consumed {consumed_json}/{message_count} JSON messages")

    elapsed = time.time() - start_time
    logger.info(f"JSON consumption complete: {consumed_json} messages in {elapsed:.2f}s ({consumed_json/elapsed:.0f} msg/s)")
    consumer_json.close()

    # ============================================
    # Part 4: Consume and Deserialize - Avro
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info(f"Part 4: Consuming {message_count} messages with Avro deserialization")
    logger.info(f"{'='*60}")

    consumer_avro = ProfiledConsumer(
        bootstrap_servers,
        group_id='serdes-comparison-avro',
        value_deserializer=avro_deserializer,
        auto_offset_reset='earliest'
    )
    consumer_avro.subscribe([topic_avro])

    consumed_avro = 0
    start_time = time.time()
    while consumed_avro < message_count:
        messages = consumer_avro.consume(
            num_messages=100,
            timeout=5.0,
            tags={"serdes_type": "avro"}
        )
        consumed_avro += len(messages)

        if consumed_avro % 100 == 0:
            logger.info(f"Consumed {consumed_avro}/{message_count} Avro messages")

    elapsed = time.time() - start_time
    logger.info(f"Avro consumption complete: {consumed_avro} messages in {elapsed:.2f}s ({consumed_avro/elapsed:.0f} msg/s)")
    consumer_avro.close()

    # ============================================
    # Summary
    # ============================================
    logger.info(f"\n{'='*60}")
    logger.info("Scenario Complete!")
    logger.info(f"{'='*60}")
    logger.info("View profiles in Pyroscope UI and compare:")
    logger.info(f"  - Filter by serdes_type=json vs serdes_type=avro")
    logger.info(f"  - Compare CPU time in serialization functions")
    logger.info(f"  - Avro should show Schema Registry overhead")
    logger.info(f"{'='*60}\n")
