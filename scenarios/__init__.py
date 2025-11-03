"""
Profiling scenarios for Pyroscope + Kafka demo.

This package contains different profiling scenarios:
- serdes_comparison: Compare JSON vs Avro serialization
- processing_logic: Compare simple vs complex processing
- io_patterns: Compare batch vs streaming I/O
"""

__all__ = [
    'serdes_comparison',
    'processing_logic',
    'io_patterns'
]
