#!/usr/bin/env python3
"""
Continuous Profiling Demo

Runs the processing scenario continuously to generate CPU-intensive profile data.
This is designed to run long enough for Alloy eBPF profiling to capture data.
"""

import argparse
import logging
import os
import sys
import time
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Run continuous profiling demo"""
    load_dotenv()

    # Parse arguments
    parser = argparse.ArgumentParser(description='Continuous Pyroscope + Kafka Profiling Demo')
    parser.add_argument('--kafka-brokers', default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9192'))
    parser.add_argument('--schema-registry', default=os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8181'))
    parser.add_argument('--messages', type=int, default=500, help='Messages per iteration')
    parser.add_argument('--iterations', type=int, default=10, help='Number of iterations')
    parser.add_argument('--sleep', type=int, default=5, help='Sleep seconds between iterations')
    args = parser.parse_args()

    logger.info("="*70)
    logger.info("Continuous Profiling Demo for Grafana Alloy eBPF")
    logger.info("="*70)
    logger.info(f"Kafka Brokers: {args.kafka_brokers}")
    logger.info(f"Schema Registry: {args.schema_registry}")
    logger.info(f"Messages per iteration: {args.messages}")
    logger.info(f"Iterations: {args.iterations}")
    logger.info(f"Sleep between iterations: {args.sleep}s")
    logger.info("="*70)
    logger.info("Note: Profiling via Grafana Alloy eBPF (no SDK)")
    logger.info("="*70 + "\n")

    # Import scenarios
    from scenarios.processing_logic import run_processing_logic_comparison
    from scenarios.serdes_comparison import run_serdes_comparison

    try:
        for iteration in range(1, args.iterations + 1):
            logger.info(f"\n{'='*70}")
            logger.info(f"ITERATION {iteration}/{args.iterations}")
            logger.info(f"{'='*70}\n")

            # Run processing logic comparison (most CPU-intensive)
            logger.info("Running PROCESSING LOGIC scenario (complex hashing + aggregation)...")
            run_processing_logic_comparison(
                bootstrap_servers=args.kafka_brokers,
                schema_registry_url=args.schema_registry,
                message_count=args.messages,
                payload_size=500  # Larger payload for more CPU work
            )

            logger.info(f"\nIteration {iteration} complete.")

            if iteration < args.iterations:
                logger.info(f"Sleeping {args.sleep}s before next iteration...\n")
                time.sleep(args.sleep)

        logger.info("\n" + "="*70)
        logger.info(f"ALL {args.iterations} ITERATIONS COMPLETE!")
        logger.info("="*70)
        logger.info("Profiles should now be visible in Pyroscope at http://pyroscope:4040")
        logger.info("Look for service: /py-flamegraph-demo-app")
        logger.info("Python-level stack traces visible (complex_processing, <module>, etc.)")
        logger.info("Note: C extensions like hashlib.sha256 require frame pointers to be visible")
        logger.info("="*70 + "\n")

    except KeyboardInterrupt:
        logger.info("\nDemo interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Demo failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
