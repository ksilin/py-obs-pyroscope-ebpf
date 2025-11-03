#!/usr/bin/env python3
"""
Check if profiles exist in Pyroscope.

Queries the Pyroscope API to verify that profiles have been uploaded.
"""

import sys
import urllib.request
import json
from datetime import datetime, timedelta


def check_profiles(pyroscope_url: str = "http://localhost:4140"):
    """Check if profiles exist in Pyroscope."""

    print("=" * 60)
    print("Checking Pyroscope for uploaded profiles")
    print("=" * 60)
    print()

    # Query for label names (applications)
    try:
        url = f"{pyroscope_url}/api/apps"
        print(f"Querying: {url}")

        with urllib.request.urlopen(url, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())
                apps = data if isinstance(data, list) else []

                print(f"✓ Pyroscope is responding")
                print()
                print(f"Found {len(apps)} application(s):")

                if apps:
                    for app in apps:
                        print(f"  - {app}")

                    # Check if our demo app exists
                    demo_app = "python-kafka-profiling-demo"
                    if demo_app in apps:
                        print()
                        print(f"✓ Found demo application: {demo_app}")
                        print()
                        print("To view profiles:")
                        print(f"1. Open: {pyroscope_url}")
                        print(f"2. Select application: {demo_app}")
                        print("3. Select profile type: process_cpu")
                        print("4. View Python function-level stack traces")
                        print()
                        print("Note: eBPF profiling shows Python functions but not C extensions")
                        print("      (e.g., complex_processing is visible, hashlib.sha256 is not)")
                        return 0
                    else:
                        print()
                        print(f"✗ Demo application '{demo_app}' not found")
                        print("Did you run the demo? Try:")
                        print("  python demo.py --scenario all")
                        return 1
                else:
                    print("  (none)")
                    print()
                    print("✗ No applications found in Pyroscope")
                    print("Run the demo to generate profiles:")
                    print("  python demo.py --scenario all")
                    return 1
            else:
                print(f"✗ Unexpected response: {response.status}")
                return 1

    except urllib.error.URLError as e:
        print(f"✗ Cannot connect to Pyroscope at {pyroscope_url}")
        print(f"Error: {e.reason}")
        print()
        print("Is Pyroscope running?")
        print("  docker-compose ps pyroscope")
        return 1
    except Exception as e:
        print(f"✗ Error: {e}")
        return 1


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:4140"
    sys.exit(check_profiles(url))
