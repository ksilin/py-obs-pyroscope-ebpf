"""
Tests for serialization/deserialization functionality.

Tests both JSON and Avro serializers.
Avro tests require Schema Registry to be running.
"""

import unittest
import os
from confluent_kafka.serialization import SerializationContext, MessageField
from src.py_flamegraph.serdes import (
    JsonSerializer,
    JsonDeserializer,
    get_json_serializers,
    get_avro_serializers,
    create_message,
    MESSAGE_SCHEMA_STR
)


class TestJsonSerdes(unittest.TestCase):
    """Test JSON serialization/deserialization"""

    def setUp(self):
        """Set up test fixtures"""
        self.serializer, self.deserializer = get_json_serializers()
        self.test_topic = 'test-topic'
        self.ctx = SerializationContext(self.test_topic, MessageField.VALUE)

    def test_json_serializer(self):
        """Test JSON serializer"""
        message = create_message(1, payload_size=50)

        # Serialize
        serialized = self.serializer(message, self.ctx)

        # Verify
        self.assertIsInstance(serialized, bytes)
        self.assertGreater(len(serialized), 0)

    def test_json_deserializer(self):
        """Test JSON deserializer"""
        original = create_message(2, payload_size=50)

        # Serialize then deserialize
        serialized = self.serializer(original, self.ctx)
        deserialized = self.deserializer(serialized, self.ctx)

        # Verify
        self.assertEqual(deserialized['id'], original['id'])
        self.assertEqual(deserialized['timestamp'], original['timestamp'])
        self.assertEqual(deserialized['payload'], original['payload'])
        self.assertEqual(deserialized['metadata'], original['metadata'])

    def test_json_roundtrip(self):
        """Test full JSON serialization roundtrip"""
        messages = [create_message(i, payload_size=100) for i in range(10)]

        for original in messages:
            serialized = self.serializer(original, self.ctx)
            deserialized = self.deserializer(serialized, self.ctx)
            self.assertEqual(deserialized, original)

    def test_json_none_handling(self):
        """Test JSON serializer handles None"""
        result = self.serializer(None, self.ctx)
        self.assertIsNone(result)

        result = self.deserializer(None, self.ctx)
        self.assertIsNone(result)


class TestAvroSerdes(unittest.TestCase):
    """Test Avro serialization/deserialization with Schema Registry"""

    @classmethod
    def setUpClass(cls):
        """Set up Avro serializers"""
        cls.schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8181')

        try:
            cls.serializer, cls.deserializer = get_avro_serializers(cls.schema_registry_url)
        except Exception as e:
            raise unittest.SkipTest(f"Schema Registry not available: {e}")

    def setUp(self):
        """Set up test fixtures"""
        self.test_topic = 'test-avro-topic'
        self.ctx = SerializationContext(self.test_topic, MessageField.VALUE)

    def test_avro_serializer(self):
        """Test Avro serializer"""
        message = create_message(1, payload_size=50)

        # Serialize
        serialized = self.serializer(message, self.ctx)

        # Verify
        self.assertIsInstance(serialized, bytes)
        self.assertGreater(len(serialized), 0)

    def test_avro_deserializer(self):
        """Test Avro deserializer"""
        original = create_message(2, payload_size=50)

        # Serialize then deserialize
        serialized = self.serializer(original, self.ctx)
        deserialized = self.deserializer(serialized, self.ctx)

        # Verify
        self.assertEqual(deserialized['id'], original['id'])
        self.assertEqual(deserialized['timestamp'], original['timestamp'])
        self.assertEqual(deserialized['payload'], original['payload'])

    def test_avro_roundtrip(self):
        """Test full Avro serialization roundtrip"""
        messages = [create_message(i, payload_size=100) for i in range(10)]

        for original in messages:
            serialized = self.serializer(original, self.ctx)
            deserialized = self.deserializer(serialized, self.ctx)

            # Compare key fields
            self.assertEqual(deserialized['id'], original['id'])
            self.assertEqual(deserialized['timestamp'], original['timestamp'])
            self.assertEqual(deserialized['payload'], original['payload'])

    def test_avro_schema_validation(self):
        """Test that Avro validates schema"""
        # Valid message
        valid_msg = create_message(1, payload_size=50)
        serialized = self.serializer(valid_msg, self.ctx)
        self.assertIsNotNone(serialized)

        # Invalid message (missing required field) should fail
        invalid_msg = {"id": "test"}  # Missing timestamp and payload
        with self.assertRaises(Exception):
            self.serializer(invalid_msg, self.ctx)


class TestMessageCreation(unittest.TestCase):
    """Test message creation helper"""

    def test_create_message_basic(self):
        """Test basic message creation"""
        msg = create_message(1, payload_size=100)

        self.assertIn('id', msg)
        self.assertIn('timestamp', msg)
        self.assertIn('payload', msg)
        self.assertIn('metadata', msg)

    def test_create_message_id_format(self):
        """Test message ID formatting"""
        msg = create_message(42, payload_size=10)
        self.assertEqual(msg['id'], 'msg-000042')

        msg = create_message(999999, payload_size=10)
        self.assertEqual(msg['id'], 'msg-999999')

    def test_create_message_payload_size(self):
        """Test payload size control"""
        sizes = [10, 50, 100, 500, 1000]

        for size in sizes:
            msg = create_message(1, payload_size=size)
            self.assertEqual(len(msg['payload']), size)

    def test_create_message_metadata(self):
        """Test message metadata"""
        msg = create_message(1, payload_size=10)

        self.assertIsInstance(msg['metadata'], dict)
        self.assertIn('source', msg['metadata'])
        self.assertIn('version', msg['metadata'])


class TestMessageSchema(unittest.TestCase):
    """Test message schema definition"""

    def test_schema_is_valid_json(self):
        """Test that schema is valid JSON"""
        import json
        try:
            schema = json.loads(MESSAGE_SCHEMA_STR)
            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'DemoMessage')
        except json.JSONDecodeError:
            self.fail("Schema is not valid JSON")

    def test_schema_has_required_fields(self):
        """Test that schema defines required fields"""
        import json
        schema = json.loads(MESSAGE_SCHEMA_STR)

        field_names = [field['name'] for field in schema['fields']]
        self.assertIn('id', field_names)
        self.assertIn('timestamp', field_names)
        self.assertIn('payload', field_names)
        self.assertIn('metadata', field_names)


if __name__ == "__main__":
    unittest.main()
