"""
Integration tests for Kafka producer and consumer.

These tests require Kafka and Schema Registry to be running.
Run with: docker-compose up -d && pytest tests/test_kafka_integration.py
"""

import unittest
import os
import time
from src.py_flamegraph.kafka_client import (
    ProfiledProducer,
    ProfiledConsumer,
    create_topics
)
from src.py_flamegraph.serdes import (
    get_json_serializers,
    create_message
)


class TestKafkaIntegration(unittest.TestCase):
    """Integration tests for Kafka functionality"""

    @classmethod
    def setUpClass(cls):
        """Set up test configuration"""
        cls.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9192')
        cls.test_topic = 'test-integration-topic'
        cls.consumer_group = 'test-integration-group'

        # Create test topic
        try:
            create_topics(cls.bootstrap_servers, [cls.test_topic])
            time.sleep(1)  # Give Kafka time to create topic
        except Exception as e:
            raise unittest.SkipTest(f"Kafka not available: {e}")

    def test_produce_and_consume_json(self):
        """Test producing and consuming messages with JSON serialization"""
        # Get serializers
        json_serializer, json_deserializer = get_json_serializers()

        # Create producer
        producer = ProfiledProducer(
            self.bootstrap_servers,
            value_serializer=json_serializer
        )

        # Produce test messages
        test_messages = [create_message(i, payload_size=50) for i in range(10)]

        for msg in test_messages:
            producer.produce(self.test_topic, value=msg)

        producer.flush()

        # Create consumer
        consumer = ProfiledConsumer(
            self.bootstrap_servers,
            group_id=f'{self.consumer_group}-json-{int(time.time())}',
            value_deserializer=json_deserializer,
            auto_offset_reset='earliest'
        )
        consumer.subscribe([self.test_topic])

        # Consume messages
        consumed_messages = []
        timeout_counter = 0
        max_timeout = 10

        while len(consumed_messages) < len(test_messages) and timeout_counter < max_timeout:
            messages = consumer.consume(num_messages=10, timeout=1.0)
            consumed_messages.extend(messages)
            if not messages:
                timeout_counter += 1

        consumer.close()

        # Verify
        self.assertEqual(len(consumed_messages), len(test_messages),
                        "Should consume all produced messages")

        for msg in consumed_messages:
            self.assertIn('value', msg)
            self.assertIn('id', msg['value'])
            self.assertIn('timestamp', msg['value'])
            self.assertIn('payload', msg['value'])

    def test_produce_multiple_messages(self):
        """Test producing multiple messages in batch"""
        json_serializer, _ = get_json_serializers()

        producer = ProfiledProducer(
            self.bootstrap_servers,
            value_serializer=json_serializer
        )

        message_count = 100
        start_time = time.time()

        for i in range(message_count):
            message = create_message(i, payload_size=100)
            producer.produce(self.test_topic, value=message)

            if (i + 1) % 10 == 0:
                producer.poll(0)

        producer.flush()
        elapsed = time.time() - start_time

        # Should complete in reasonable time
        self.assertLess(elapsed, 10.0, "Batch production should be fast")

    def test_consumer_poll(self):
        """Test consumer poll functionality"""
        json_serializer, json_deserializer = get_json_serializers()

        # Produce a test message
        producer = ProfiledProducer(
            self.bootstrap_servers,
            value_serializer=json_serializer
        )
        test_msg = create_message(999, payload_size=50)
        producer.produce(self.test_topic, value=test_msg)
        producer.flush()

        # Consumer with poll
        consumer = ProfiledConsumer(
            self.bootstrap_servers,
            group_id=f'{self.consumer_group}-poll-{int(time.time())}',
            value_deserializer=json_deserializer,
            auto_offset_reset='earliest'
        )
        consumer.subscribe([self.test_topic])

        # Poll for message
        msg = None
        for _ in range(10):  # Try up to 10 times
            msg = consumer.poll(timeout=1.0)
            if msg is not None:
                break

        consumer.close()

        # Verify
        self.assertIsNotNone(msg, "Should receive a message")
        self.assertIn('value', msg)
        self.assertEqual(msg['value']['id'], test_msg['id'])

    def test_create_topics(self):
        """Test topic creation functionality"""
        test_topics = [
            f'test-topic-{int(time.time())}-1',
            f'test-topic-{int(time.time())}-2'
        ]

        # Should not raise exception
        try:
            create_topics(self.bootstrap_servers, test_topics, num_partitions=1)
        except Exception as e:
            self.fail(f"Topic creation failed: {e}")


if __name__ == "__main__":
    unittest.main()
