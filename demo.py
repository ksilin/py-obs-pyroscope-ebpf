#!/usr/bin/env python3
"""
Pyroscope + Python + Kafka Profiling Demo

Main entry point to run different profiling scenarios.

Usage:
    python demo.py --scenario all
    python demo.py --scenario serdes --messages 2000
    python demo.py --scenario processing
    python demo.py --scenario io
"""

import argparse
import logging
import os
import sys
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_serdes_scenario(args):
    """Run the serialization comparison scenario."""
    from scenarios.serdes_comparison import run_serdes_comparison

    logger.info("\n" + "="*70)
    logger.info("SCENARIO: Serialization/Deserialization Comparison")
    logger.info("="*70)
    logger.info("Comparing JSON vs Avro (with Schema Registry)")
    logger.info(f"Messages: {args.messages}")
    logger.info(f"Payload size: {args.payload_size} characters")
    logger.info("="*70 + "\n")

    run_serdes_comparison(
        bootstrap_servers=args.kafka_brokers,
        schema_registry_url=args.schema_registry,
        message_count=args.messages,
        payload_size=args.payload_size
    )


def run_processing_scenario(args):
    """Run the processing logic comparison scenario."""
    from scenarios.processing_logic import run_processing_logic_comparison

    logger.info("\n" + "="*70)
    logger.info("SCENARIO: Message Processing Logic Comparison")
    logger.info("="*70)
    logger.info("Comparing simple vs complex message processing")
    logger.info(f"Messages: {args.messages}")
    logger.info(f"Payload size: {args.payload_size} characters")
    logger.info("="*70 + "\n")

    run_processing_logic_comparison(
        bootstrap_servers=args.kafka_brokers,
        schema_registry_url=args.schema_registry,
        message_count=args.messages,
        payload_size=args.payload_size
    )


def run_io_scenario(args):
    """Run the I/O patterns comparison scenario."""
    from scenarios.io_patterns import run_io_patterns_comparison

    logger.info("\n" + "="*70)
    logger.info("SCENARIO: I/O Patterns Comparison")
    logger.info("="*70)
    logger.info("Comparing batch vs streaming production patterns")
    logger.info(f"Messages: {args.messages}")
    logger.info(f"Payload size: {args.payload_size} characters")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info("="*70 + "\n")

    run_io_patterns_comparison(
        bootstrap_servers=args.kafka_brokers,
        schema_registry_url=args.schema_registry,
        message_count=args.messages,
        payload_size=args.payload_size,
        batch_size=args.batch_size
    )


def main():
    """Main entry point."""
    # Load environment variables
    load_dotenv()

    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Pyroscope + Python + Kafka Profiling Demo',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all scenarios
  python demo.py --scenario all

  # Run specific scenario with custom message count
  python demo.py --scenario serdes --messages 5000

  # Run with custom configuration
  python demo.py --scenario io --messages 2000 --batch-size 50
        """
    )

    parser.add_argument(
        '--scenario',
        choices=['all', 'serdes', 'processing', 'io'],
        default='all',
        help='Which scenario to run (default: all)'
    )

    parser.add_argument(
        '--messages',
        type=int,
        default=int(os.getenv('MESSAGE_COUNT', '1000')),
        help='Number of messages to produce/consume (default: 1000)'
    )

    parser.add_argument(
        '--payload-size',
        type=int,
        default=100,
        help='Size of message payload in characters (default: 100)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Batch size for batch I/O pattern (default: 100)'
    )

    parser.add_argument(
        '--kafka-brokers',
        default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9192'),
        help='Kafka bootstrap servers (default: localhost:9192)'
    )

    parser.add_argument(
        '--schema-registry',
        default=os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8181'),
        help='Schema Registry URL (default: http://localhost:8181)'
    )

    parser.add_argument(
        '--pyroscope-server',
        default=os.getenv('PYROSCOPE_SERVER_URL', 'http://localhost:4140'),
        help='Pyroscope server URL (default: http://localhost:4140)'
    )

    parser.add_argument(
        '--app-name',
        default=os.getenv('PYROSCOPE_APP_NAME', 'python-kafka-profiling-demo'),
        help='Application name for Pyroscope (default: python-kafka-profiling-demo)'
    )

    args = parser.parse_args()

    # Print configuration
    logger.info("="*70)
    logger.info("Pyroscope + Python + Kafka Profiling Demo")
    logger.info("="*70)
    logger.info(f"Kafka Brokers: {args.kafka_brokers}")
    logger.info(f"Schema Registry: {args.schema_registry}")
    logger.info(f"Pyroscope Server: {args.pyroscope_server}")
    logger.info(f"Application Name: {args.app_name}")
    logger.info("="*70 + "\n")

    # Note: Profiling is handled by Grafana Alloy via eBPF (no SDK needed)
    logger.info("Profiling: Grafana Alloy eBPF (auto-discovery)")
    logger.info(f"Profiles will be sent to: {args.pyroscope_server}\n")

    # Run scenarios
    try:
        if args.scenario == 'all':
            logger.info("Running ALL scenarios sequentially...\n")
            run_serdes_scenario(args)
            run_processing_scenario(args)
            run_io_scenario(args)
        elif args.scenario == 'serdes':
            run_serdes_scenario(args)
        elif args.scenario == 'processing':
            run_processing_scenario(args)
        elif args.scenario == 'io':
            run_io_scenario(args)

        # Final summary
        logger.info("\n" + "="*70)
        logger.info("ALL SCENARIOS COMPLETE!")
        logger.info("="*70)
        logger.info(f"View profiles at: {args.pyroscope_server}")
        logger.info("="*70 + "\n")

    except KeyboardInterrupt:
        logger.info("\nDemo interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Demo failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
