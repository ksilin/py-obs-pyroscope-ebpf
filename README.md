# py-flamegraph

Python profiling with Pyroscope + Kafka - demonstrating the performance impact of serialization, application logic, and I/O patterns.

## Overview

This project demonstrates continuous profiling of Python Kafka applications using Pyroscope. It provides three distinct scenarios to profile and compare:

1. **Serialization Comparison**: JSON vs Avro with Schema Registry
2. **Processing Logic**: Simple vs complex message processing
3. **I/O Patterns**: Batch vs streaming production

Each scenario generates distinct CPU profiles that can be visualized and compared in Pyroscope's flame graphs.

## Infrastructure

This project uses modern Kafka infrastructure with eBPF-based profiling:

- **Confluent Platform 8.1.0 (KRaft mode)** (Apache Kafka 4.1)
- **Schema Registry 8.1.0**
- **Grafana Pyroscope 1.14.1** (continuous profiling backend)
- **Grafana Alloy 1.11.3** (eBPF profiler)

## Prerequisites

- **Python 3.11.x** (recommended for eBPF profiling - Python 3.12+ may have offset mapping issues)
- Docker and Docker Compose
- `uv` package manager (or `pip`)

> **Note**: This project uses eBPF-based profiling via Grafana Alloy, which requires specific Python version support. Python 3.11 has well-tested offset mappings for reliable stack trace collection.

## Quick Start

### 1. Clone and Setup

```bash
cd py-flamegraph
cp .env.example .env
```

### 2. Install Dependencies

Using `uv`:

```bash
uv pip install -e .
```

Or using `pip`:

```bash
pip install -e .
```

### 3. Start Infrastructure

Start Kafka, Schema Registry, Pyroscope, Alloy, and the demo app:

```bash
docker compose up -d
```

Wait ~30 seconds for all services to be ready. Verify services are running:

```bash
docker compose ps
```

You should see:

- `py-flamegraph-kafka` - Kafka broker (port 9192, KRaft mode)
- `py-flamegraph-schema-registry` - Schema Registry (port 8181)
- `py-flamegraph-pyroscope` - Pyroscope server (port 4140)
- `py-flamegraph-alloy` - Grafana Alloy eBPF profiler (port 12345)
- `py-flamegraph-demo-app` - Python demo application (auto-profiled)

Optionally, verify all services are healthy:

```bash
python util_scripts/check_infrastructure.py
```

This will check connectivity to Kafka, Schema Registry, and Pyroscope.

### 4. Automatic Profiling

**The demo runs automatically!** When you start the infrastructure with `docker compose up -d`, the `demo-app` container starts running `continuous_demo.py`, which:

- Runs 100 iterations of CPU-intensive workloads
- Each iteration processes 500 messages
- Includes complex processing (SHA-256 hashing, character frequency analysis)
- Takes ~10 minutes to complete, then restarts

**Grafana Alloy automatically profiles** the Python process using eBPF.

To run scenarios manually from your laptop:

```bash
python demo.py --scenario processing --messages 1000
```

### 5. View Profiles in Pyroscope UI

Open Pyroscope web interface:

```bash
open http://localhost:4140
```

**How to view Python flamegraphs:**

1. **Select Service**: Choose `/py-flamegraph-demo-app` from the service dropdown

2. **Select Profile Type**: Choose `process_cpu` (CPU profiling)

3. **Adjust Time Range**: Set to "Last 15 minutes" or longer to capture demo activity

4. **View the Flamegraph**: You should see Python stack traces including:

   - `complex_processing` (from message processing)
   - `simple_processing` functions
   - `confluent_kafka` producer/consumer operations
   - Python runtime functions

   **Note**: C extensions like `hashlib.sha256` require frame pointers to be visible in eBPF profiling. Python-level functions are visible, but native C code may not appear in the flamegraph.

5. **Analyze Performance**:
   - Wider boxes = more CPU time
   - Stack depth shows call hierarchy
   - Hover over functions to see exact timings

**Expected CPU hotspots:**

- `complex_processing()` - Character frequency analysis and aggregation
- `simple_processing()` - Minimal processing
- Kafka serialization (JSON vs Avro)
- Python runtime overhead

## Demo Scenarios

### 1. Serialization Comparison (`--scenario serdes`)

Compares JSON vs Avro serialization with Schema Registry.

**What it does:**

- Produces 1000 messages with JSON serialization
- Produces 1000 messages with Avro + Schema Registry
- Consumes and deserializes both
- All profiled automatically via eBPF (no application tags)

**Expected Results:**

- Avro shows Schema Registry overhead
- JSON is faster but produces larger messages
- Clear CPU difference in serialization functions

**View in Pyroscope:**

With eBPF profiling, all scenarios appear under the same service (`/py-flamegraph-demo-app`). To observe differences:

1. Run scenarios at different times
2. Compare time-based flamegraphs
3. Look for function names indicating serialization type:
   - JSON: `json.dumps`, `json.loads`
   - Avro: Schema Registry client calls, Avro serialization functions

### 2. Processing Logic (`--scenario processing`)

Compares simple vs complex message processing.

**What it does:**

- Simple: Extract fields only
- Complex: Hash calculation, aggregation, validation, enrichment
- All profiled automatically via eBPF (no application tags)

**Expected Results:**

- Complex processing shows higher CPU usage
- Visible hotspots: `complex_processing`, dict operations, string processing, character frequency loops
- Clear difference in business logic time

**View in Pyroscope:**

With eBPF profiling, look for these function names in the flamegraph:
- `complex_processing` / `simple_processing` - Python function-level granularity
- Character frequency analysis loops
- Dict operations and string processing
- Python runtime overhead (GC, object allocation)

**Note**: C extensions (like `hashlib.sha256`) won't be visible without frame pointers. You'll see time spent in Python code calling these functions, but not the C internals.

Compare CPU time by running scenarios at different times and selecting those time ranges in Pyroscope UI.

### 3. I/O Patterns (`--scenario io`)

Compares batch vs streaming production patterns.

**What it does:**

- Batch: Produce 100 messages, then flush (10 flush operations for 1000 msgs)
- Streaming: Flush after every message (1000 flush operations)
- All profiled automatically via eBPF (no application tags)

**Expected Results:**

- Streaming shows much more time in flush operations
- Batch shows higher throughput
- Network overhead clearly visible

**View in Pyroscope:**

With eBPF profiling, look for:
- `Producer.flush` - time spent in Kafka flush operations
- `confluent_kafka` library functions
- System calls related to network I/O

Compare batch vs streaming by running scenarios at different times and observing the proportion of CPU time spent in flush operations.

## Configuration

Edit `.env` file to customize:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9192
SCHEMA_REGISTRY_URL=http://localhost:8181

# Pyroscope
PYROSCOPE_SERVER_URL=http://localhost:4140
PYROSCOPE_APP_NAME=python-kafka-profiling-demo

# Demo Parameters
MESSAGE_COUNT=1000
```

## CLI Options

```bash
python demo.py --help

Options:
  --scenario {all,serdes,processing,io}
                        Which scenario to run (default: all)
  --messages N          Number of messages (default: 1000)
  --payload-size N      Message payload size in chars (default: 100)
  --batch-size N        Batch size for I/O pattern (default: 100)
  --kafka-brokers ADDR  Kafka bootstrap servers
  --schema-registry URL Schema Registry URL
  --pyroscope-server URL Pyroscope server URL
```

## Testing

Run unit tests:

```bash
uv run pytest tests/test_serdes.py
```

Run integration tests (requires running infrastructure):

```bash
docker-compose up -d
uv run pytest tests/test_kafka_integration.py
```

Run all tests:

```bash
uv run pytest tests/
```

## Project Structure

```
py-flamegraph/
├── demo.py                          # ⭐ MAIN ENTRY POINT - Run this!
├── continuous_demo.py               # Long-running demo for eBPF profiling
├── docker-compose.yml               # Infrastructure setup (Kafka + SR + Pyroscope + Alloy)
├── alloy-config.alloy               # Grafana Alloy eBPF profiler configuration
├── .env.example                     # Configuration template
├── src/py_flamegraph/
│   ├── kafka_client.py             # Kafka producer/consumer (profiled via eBPF)
│   └── serdes.py                   # JSON and Avro serializers
├── scenarios/
│   ├── serdes_comparison.py        # Serialization scenario
│   ├── processing_logic.py         # Processing scenario
│   └── io_patterns.py              # I/O patterns scenario
├── tests/
│   ├── test_kafka_integration.py   # Integration tests
│   └── test_serdes.py              # Serialization tests
└── util_scripts/
    ├── check_infrastructure.py     # Health check for services
    └── check_profiles.py           # Verify profiles exist in Pyroscope
```

## How Profiling Works

### eBPF-Based Profiling with Grafana Alloy

This project uses modern eBPF-based profiling:

1. **Grafana Alloy** runs with:

   - `--privileged` mode (for eBPF kernel access)
   - `--pid=host` (to see all process PIDs)
   - Docker socket mounted (for container discovery)

2. **Automatic Discovery**:

   ```alloy
   discovery.docker "local_containers" {
     host = "unix:///var/run/docker.sock"
   }

   pyroscope.ebpf "instance" {
     forward_to = [pyroscope.write.pyroscope_server.receiver]
     targets    = discovery.docker.local_containers.targets
   }
   ```

3. **Alloy discovers running containers** and automatically:

   - Identifies Python processes
   - Attaches eBPF probes to the Python interpreter
   - Samples stack traces at 97 Hz
   - Sends profiles to Pyroscope server

4. **Zero code changes** - your Python application runs normally, no SDK imports needed!

### How It Works Under the Hood

```
┌─────────────────┐
│ Python Process  │ ← Your Kafka application
│ (demo-app)      │
└────────┬────────┘
         │
         │ eBPF probes attached
         ↓
┌─────────────────┐
│ Linux Kernel    │ ← Stack trace sampling (100 Hz)
│ (eBPF)          │
└────────┬────────┘
         │
         │ Stack traces
         ↓
┌─────────────────┐
│ Grafana Alloy   │ ← Collects & forwards profiles
└────────┬────────┘
         │
         │ HTTP POST
         ↓
┌─────────────────┐
│ Pyroscope       │ ← Storage & visualization
│ (Flamegraphs)   │
└─────────────────┘
```

### Advantages of eBPF Profiling

✅ **No code instrumentation** - works with any Python application
✅ **Low overhead** - ~1-2% CPU impact
✅ **Production-safe** - kernel-level safety guarantees
✅ **Language-agnostic** - profiles Python, Java, Go, Ruby, etc.
✅ **No SDK version conflicts** - independent of application dependencies

### Requirements

- **Linux kernel ≥ 4.9** (for `BPF_PROG_TYPE_PERF_EVENT`)
- **Docker with privileged mode** support
- Python application running in a container

## Profiling Approaches: eBPF vs SDK

This project uses **eBPF-based profiling** via Grafana Alloy. Understanding when to use eBPF vs SDK-based profiling helps you choose the right approach for your use case.

### eBPF-Based Profiling (This Project)

**Architecture:**
```
Python App (no SDK) → eBPF probes (kernel) → Alloy → Pyroscope
```

**When to Use:**
- ✅ Profile existing applications **without code changes**
- ✅ **Multi-language environments** (Python, Java, Go, Ruby, etc.)
- ✅ **Production deployments** requiring low overhead (~1-2% CPU)
- ✅ **Platform teams** managing profiling infrastructure centrally
- ✅ Profile **3rd-party code** you don't control
- ✅ Avoid SDK dependency conflicts

**Limitations:**
- ❌ No **application-level tagging** (per-request, per-function labels)
- ❌ Container-level granularity only (not per-route or per-handler)
- ❌ Limited to Python versions with offset mappings (3.11 works well)
- ❌ Requires privileged container access and host PID namespace

**Configuration:**
```alloy
// Centralized in alloy-config.alloy
pyroscope.ebpf "instance" {
  forward_to = [pyroscope.write.pyroscope_server.receiver]
  targets = discovery.docker.local_containers.targets
  sample_rate = 97  // Stack traces per second
}
```

---

### SDK-Based Profiling (Alternative)

**Architecture:**
```
Python App (with SDK) → Pyroscope SDK → Pyroscope Server
```

**When to Use:**
- ✅ Need **fine-grained tags** (per-endpoint, per-function, per-user)
- ✅ **Single language** environment (all Python)
- ✅ Want **application-level control** over profiling
- ✅ Need **GIL-only** or subprocess-specific profiling
- ✅ Cannot modify infrastructure (no eBPF/privileged access)

**Limitations:**
- ❌ Requires **code instrumentation** (`import pyroscope`)
- ❌ Additional **dependency** (`pip install pyroscope-io`)
- ❌ SDK **version compatibility** issues (e.g., 0.8.11 incompatible with Pyroscope 1.14.1)
- ❌ Must redeploy application to change profiling config

**Configuration:**
```python
# In application code
import pyroscope

pyroscope.configure(
    application_name = "my-app",
    server_address = "http://pyroscope:4040",
    tags = {"region": "us-east", "version": "v1.2.3"}
)

# Dynamic tagging
with pyroscope.tag_wrapper({"endpoint": "/api/orders"}):
    process_order()
```

---

### Feature Comparison

| Feature | eBPF (This Project) | SDK (Alternative) |
|---------|---------------------|-------------------|
| **Code Changes** | None | Required |
| **Dependencies** | None (app-side) | `pyroscope-io` |
| **Sample Rate** | 97 Hz (configurable) | 100 Hz (default) |
| **Tagging** | External labels only | Dynamic per-function |
| **Languages** | Python, Java, Go, Ruby, etc. | Python only |
| **Overhead** | ~1-2% CPU | ~1-5% CPU |
| **Python Version** | 3.11 (well-tested) | Any |
| **Deployment** | Infrastructure change | App change |
| **Granularity** | Container-level | Function-level |

---

### Hybrid Approach (Advanced)

You can combine both approaches:
- **eBPF** for continuous baseline profiling (low overhead, no code changes)
- **SDK** for targeted deep-dives with application tags (when debugging specific issues)

This gives you continuous observability with the option to add fine-grained instrumentation when needed.

---

### Our Choice: eBPF

This project demonstrates **eBPF profiling** because:
1. **Zero instrumentation** - works with any Python application
2. **Production-ready** - kernel-level safety guarantees
3. **Educational** - shows modern profiling without SDK complexity
4. **Scalable** - one Alloy instance can profile many containers

For scenarios requiring per-function tagging, see the [official Pyroscope Python SDK examples](https://github.com/grafana/pyroscope/tree/main/examples/language-sdk-instrumentation/python).

## Verifying Profiling is Working

Check that Alloy is profiling the Python container:

```bash
# 1. Check Alloy logs for "pyperf process profiling init success"
docker compose logs alloy | grep "pyperf process profiling init success"

# Expected output:
# level=info msg="pyperf process profiling init success" pid=12345 service_name="/py-flamegraph-demo-app"

# 2. Verify service appears in Pyroscope
curl -s "http://localhost:4140/querier.v1.QuerierService/LabelValues" \
  -H "Content-Type: application/json" \
  -d '{"name":"service_name"}' | grep demo-app

# Expected output: "/py-flamegraph-demo-app"

# 3. Check demo-app is running and processing messages
docker compose logs demo-app --tail=20
```

If you see `/py-flamegraph-demo-app` in Pyroscope, **profiling is working!** Open http://localhost:4140 to view flamegraphs.

## Troubleshooting

### Profiles not appearing in Pyroscope

**Symptom**: `/py-flamegraph-demo-app` doesn't appear in Pyroscope service list.

**Solutions**:

1. Check Alloy logs: `docker compose logs alloy | grep -i error`
2. Verify demo-app is running: `docker compose ps demo-app`
3. Ensure Linux kernel ≥ 4.9: `docker info | grep Kernel`
4. Restart Alloy: `docker compose restart alloy`

### Services not starting

```bash
# Check logs
docker compose logs kafka
docker compose logs schema-registry
docker compose logs pyroscope
docker compose logs alloy

# Restart services
docker-compose down
docker-compose up -d
```

### Kafka connection errors

```bash
# Verify Kafka is ready
docker-compose exec kafka kafka-topics --bootstrap-server localhost:19192 --list

# Check port is accessible
nc -zv localhost 9192
```

### Schema Registry issues

```bash
# Check Schema Registry health
curl http://localhost:8181/subjects

# Verify connectivity
docker-compose exec schema-registry curl http://schema-registry:8181/subjects
```

### Demo app exits immediately

**Symptom**: `demo-app` container shows "Exited" status.

**Cause**: demo-app completed all 100 iterations.

**Solution**: It will restart automatically (configured with `restart: on-failure`). Or restart manually:

```bash
docker compose restart demo-app
```

### Python processes not being profiled

**Symptom**: Only Java services appear in Pyroscope (Kafka, Schema Registry), but not Python.

**Diagnosis**:

```bash
# Check if Alloy detected Python:
docker compose logs alloy | grep "pyperf process profiling"

# If you see "pyperf process profiling init success" → ✅ Working
# If nothing → Alloy hasn't discovered the Python container yet
```

**Solutions**:

1. Ensure `demo-app` is running: `docker compose ps demo-app`
2. Wait 30 seconds after starting for discovery
3. Check Alloy config is correct: `docker exec py-flamegraph-alloy cat /etc/alloy/config.alloy`
4. Restart Alloy: `docker compose restart alloy`

## Cleanup

Stop and remove all containers:

```bash
docker-compose down -v
```

This removes containers, networks, and volumes (including Kafka data).

## Further Reading

### eBPF Profiling

- [Grafana Alloy eBPF Profiling](https://grafana.com/docs/alloy/latest/reference/components/pyroscope/pyroscope.ebpf/)
- [Setup eBPF Profiling in Docker](https://grafana.com/docs/pyroscope/latest/configure-client/grafana-alloy/ebpf/setup-docker/)
- [Pyroscope Documentation](https://grafana.com/docs/pyroscope/)

### Kafka & Schema Registry

- [Confluent Kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [KRaft Mode](https://kafka.apache.org/documentation/#kraft)

## License

MIT
