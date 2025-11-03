#!/usr/bin/env python3
"""
Infrastructure Health Check

Verifies that Kafka (KRaft mode), Schema Registry, and Pyroscope are all running and accessible.
"""

import sys
import time
import urllib.request
import urllib.error
from confluent_kafka.admin import AdminClient


def check_service(name: str, url: str, path: str = "/") -> bool:
    """Check if a service is accessible via HTTP."""
    try:
        full_url = f"{url}{path}"
        with urllib.request.urlopen(full_url, timeout=2) as response:
            if response.status == 200:
                print(f"✓ {name} is running at {url}")
                return True
            else:
                print(f"✗ {name} responded with status {response.status}")
                return False
    except urllib.error.URLError as e:
        print(f"✗ {name} not reachable at {url}: {e.reason}")
        return False
    except Exception as e:
        print(f"✗ {name} check failed: {e}")
        return False


def check_kafka(bootstrap_servers: str) -> bool:
    """Check if Kafka broker is running and accessible."""
    try:
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        metadata = admin_client.list_topics(timeout=5)

        print(f"✓ Kafka broker is running at {bootstrap_servers}")
        print(f"  - Cluster ID: {metadata.cluster_id}")
        print(f"  - Broker count: {len(metadata.brokers)}")
        print(f"  - Topics: {len(metadata.topics)}")
        return True
    except Exception as e:
        print(f"✗ Kafka broker not accessible at {bootstrap_servers}: {e}")
        return False


def main():
    """Run all health checks."""
    print("=" * 60)
    print("Infrastructure Health Check")
    print("=" * 60)
    print()

    checks = []

    # Check Kafka (KRaft mode)
    print("Checking Kafka (KRaft mode)...")
    checks.append(("Kafka", check_kafka("localhost:9192")))
    print()

    # Check Schema Registry
    print("Checking Schema Registry...")
    checks.append(("Schema Registry", check_service(
        "Schema Registry",
        "http://localhost:8181",
        "/"
    )))
    print()

    # Check Pyroscope
    print("Checking Pyroscope...")
    checks.append(("Pyroscope", check_service(
        "Pyroscope",
        "http://localhost:4140",
        "/ready"
    )))
    print()

    # Summary
    print("=" * 60)
    print("Summary:")
    print("=" * 60)

    passed = sum(1 for _, status in checks if status)
    total = len(checks)

    for name, status in checks:
        symbol = "✓" if status else "✗"
        print(f"{symbol} {name}")

    print()
    print(f"Passed: {passed}/{total}")

    if passed == total:
        print()
        print("✓ All services are running!")
        print("You're ready to run: python demo.py --scenario all")
        return 0
    else:
        print()
        print("✗ Some services are not running.")
        print("Run: docker-compose up -d")
        print("Wait 30 seconds, then try again.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
