# OpenSearch CLI Monitor

A terminal-based monitoring CLI for OpenSearch clusters. Built with Python, Rich, Click, and simple-term-menu.

---

## Setup

```bash
cd /path/to/HPE-Interface
pip install -r requirements.txt
```

## Usage

```bash
# Interactive menus
python -m monitor

# Direct to OpenSearch menu
python -m monitor --service opensearch

# Non-interactive one-shot summary
python -m monitor --summary

# Auto-refresh every 10 seconds
python -m monitor --summary --watch 10

# Historical trend window (poller JSONL with Prometheus fallback)
python -m monitor --timeframe 6h

# Force historical trend source to poller or Prometheus API
python -m monitor --source poller
python -m monitor --source prometheus

# Force real-time node metrics
python -m monitor --timeframe real-time
```

### CLI Flags

| Flag          | Values                                   | Default | Description                                |
|---------------|------------------------------------------|---------|--------------------------------------------|
| `--timeframe` | `real-time`, `30m`, `1h`, `6h`, `4d`    | `1h`    | Routes metric source by time window        |
| `--source` | `auto`, `poller`, `prometheus`     | `auto`  | Historical trend backend selection         |
| `--service`   | `opensearch`                             | —       | Skip service menu, go direct to OpenSearch |
| `--summary`   | flag                                     | off     | Jump straight to Quick Summary             |
| `--watch`     | integer (seconds)                        | off     | Auto-refresh interval                      |

## Views

1. **Quick Summary** — 10-second health check: cluster status, resources (CPU / JVM Heap / RAM / Disk), index activity, shard status
2. **Historical Trends** — 5-minute buckets for CPU, JVM Heap, and Indexing Rate from poller JSONL (`poller/data`), with Prometheus fallback when poller data is unavailable
3. **Cluster Health** — Detailed cluster status with plain English explanations
4. **Index Deep Dive** — All indices sorted by size, drill into shard layouts
5. **Node Performance** — Per-node CPU, JVM heap, RAM, disk + indexing/search counters + bottleneck diagnostics
6. **Shard Overview** — All shards grouped by state, unassigned highlighted in red
7. **Data Streams** — Data streams sorted by size with pipeline staleness detection via `maximum_timestamp`

## Configuration

Edit `monitor/config.py`:

```python
OPENSEARCH_HOST = "localhost"
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = "admin"
OPENSEARCH_PASS = "admin"
OPENSEARCH_SSL  = False

PROMETHEUS_HOST = "localhost"
PROMETHEUS_PORT = 9090

POLLER_DATA_DIR = "poller/data"
HISTORICAL_METRICS_SOURCE = "auto"  # auto | poller | prometheus

PA_HOST = "localhost"
PA_PORT = 9600
```

### Alert Thresholds

| Metric       | Warn | Critical |
|--------------|------|----------|
| CPU          | 70%  | 90%      |
| JVM Heap     | 75%  | 90%      |
| Disk         | 80%  | 90%      |
| Stream stale | 60 min | 240 min |

> **System RAM is informational only.** OpenSearch uses spare RAM as the filesystem page cache — high OS memory usage is normal. Only JVM Heap triggers alarms.

---

## OpenSearch API Endpoints

All HTTP calls are centralised in `monitor/client.py`. No view touches the HTTP layer directly.

### `GET /_cluster/health`

**Function:** `fetch_cluster_health()`  
**Used in:** Quick Summary, Cluster Health

Returns the overall cluster status (`green` / `yellow` / `red`), node count, and shard counts. The primary health signal — cheap and unambiguous.

| Field | Purpose |
|---|---|
| `status` | Colour-coded status badge + warn/crit routing |
| `number_of_nodes` | Active node count |
| `active_shards` | Shard panel totals |
| `relocating_shards` | Shard panel warning |
| `initializing_shards` | Shard panel warning |
| `unassigned_shards` | Red alert — potential data loss |
| `number_of_pending_tasks` | Cluster Health view detail |

---

### `GET /_cluster/stats`

**Function:** `fetch_cluster_stats()`  
**Used in:** Quick Summary

Returns cluster-wide **pre-aggregated** totals — OpenSearch sums across all nodes server-side, so a single call replaces N node calls for the summary panel.

| Path | Purpose |
|---|---|
| `nodes.os.cpu.percent` | Cluster-average CPU % |
| `nodes.os.mem.used_in_bytes` / `total_in_bytes` | System RAM (informational) |
| `nodes.jvm.mem.heap_used_in_bytes` / `heap_max_in_bytes` | JVM Heap — primary memory alarm |
| `nodes.fs.total_in_bytes` / `available_in_bytes` | Disk watermark warnings |
| `indices.docs.count` | Total document count |
| `indices.indexing.index_total` | Cumulative indexing operations |
| `indices.search.query_total` | Cumulative search query count |

---

### `GET /_nodes/stats/os,jvm,fs,indices`

**Function:** `fetch_node_stats()`  
**Used in:** Quick Summary (per-node warnings), Node Performance

Per-node breakdown of CPU, JVM heap, OS memory, disk (exact bytes via `fs.total` — byte-accurate for watermark checks), and per-node indexing/search counters.

| Path | Purpose |
|---|---|
| `os.cpu.percent` | Per-node CPU % |
| `jvm.mem.heap_used_in_bytes` / `heap_max_in_bytes` | Per-node JVM Heap % — alarm source |
| `os.mem.used_in_bytes` / `total_in_bytes` | Per-node RAM (dim, informational) |
| `fs.total.total_in_bytes` / `available_in_bytes` | Per-node disk — watermark alarms |
| `indices.indexing.index_total` | Per-node indexing ops |
| `indices.search.query_total` | Per-node search queries |

---

### `GET /api/v1/query_range` (Prometheus)

**Function:** `fetch_historical_trends(timeframe)` via `MetricsProvider`
**Used in:** Historical Trends fallback path, long-window routing (`--timeframe > 1h`)

Executes PromQL queries for 5-minute-bucket trend lines (`max_over_time`) and collapses multi-node series into a cluster-wide spike line.

---

### `GET /_plugins/_performanceanalyzer/metrics` (port `9600`)

**Function:** `fetch_bottleneck_metrics(node_name)` via `MetricsProvider`
**Used in:** Node Performance diagnostics (only when CPU or Disk crosses threshold)

Fetches `Disk_Utilization` and `IO_TotWait` to provide plain-English bottleneck explanations.

---

### `GET /_cat/allocation?v&format=json`

**Function:** `fetch_disk_allocation()`  
**Used in:** Quick Summary (per-node disk warning detail text)

Human-readable disk usage per node. Used only to name which node is approaching a watermark threshold in the Alerts footer. Node Performance uses `fs.total` from `/_nodes/stats` for exact byte accuracy instead.

| Field | Purpose |
|---|---|
| `node` | Node name for targeted warning |
| `disk.used` | Used disk string (e.g. `65gb`) |
| `disk.total` | Total disk string |

---

### `GET /_cat/indices?v&s=store.size:desc&format=json`

**Function:** `fetch_indices()`  
**Used in:** Quick Summary (largest index), Index Deep Dive

All indices sorted by store size descending.

| Field | Purpose |
|---|---|
| `index` | Index name |
| `store.size` | Size string (parsed to bytes internally) |
| `docs.count` | Document count |
| `health` | Colour-coded health badge |
| `pri` / `rep` | Primary / replica shard counts |

---

### `GET /_cat/shards?v&format=json`

**Function:** `fetch_shards(index=None)`  
**Used in:** Quick Summary, Shard Overview, Index Deep Dive (drill-down)

All shards with their current state. Grouped into `STARTED` / `RELOCATING` / `INITIALIZING` / `UNASSIGNED`. Accepts an optional `index` parameter to filter to a single index.

| Field | Purpose |
|---|---|
| `state` | Shard group classification |
| `index` | Which index the shard belongs to |
| `shard` | Shard number |
| `prirep` | Primary (`p`) or replica (`r`) |
| `node` | Node the shard lives on |
| `store` | Shard size on disk |
| `reason` | Why a shard is unassigned (unassigned only) |

---

### `GET /_data_stream`

**Function:** `fetch_data_streams()`  
**Used in:** Data Streams

All data streams with metadata. The critical field is `maximum_timestamp` — the epoch millisecond of the newest document in the stream. Used to detect **pipeline staleness**: if OpenSearch is healthy but no new data has arrived in 60+ minutes (warn) or 240+ minutes (crit), the issue is upstream (Logstash, Kafka, Beats) rather than OpenSearch itself.

| Field | Purpose |
|---|---|
| `name` | Stream name |
| `store_size` / `store_size_bytes` | Disk usage (sorted descending) |
| `indices` | Backing index count |
| `maximum_timestamp` | Last ingested document → pipeline staleness |

---

## Project Structure

```
monitor/
├── __main__.py          # python -m monitor entry point
├── cli.py               # Click CLI flag definitions
├── config.py            # Connection settings + alert thresholds
├── client.py            # All OpenSearch API calls (single source of truth)
├── utils.py             # format_bytes, status_symbol, timeframe_to_minutes, etc.
├── menus.py             # Arrow-key menus (simple-term-menu)
└── Opensearch/
    └── views/
        ├── quick_summary.py      # View 1 — cluster-wide health check
        ├── trends.py             # View 2 — historical trends (poller-first, Prometheus fallback)
        ├── cluster_health.py     # View 3 — detailed health + shard counts
        ├── index_deep_dive.py    # View 4 — index table + shard drill-down
        ├── node_performance.py   # View 5 — per-node CPU / heap / disk / ops + diagnostics
        ├── shard_overview.py     # View 6 — shards grouped by state
        └── data_streams.py       # View 7 — data streams + pipeline staleness
```

---

## Historical Trend Sources

Historical Trends first reads poller JSONL snapshots from `poller/data` (or `POLLER_DATA_DIR`) and aggregates 5-minute buckets for:
- `cpu_pct` (max bucket value)
- `heap_used_bytes` (max bucket value)
- `index_total` (converted to per-second indexing rate)

When poller history is unavailable, it falls back to Prometheus queries.

## Prometheus Metrics Used (Fallback)

Historical Trends currently uses these Prometheus metric names:
- `opensearch_jvm_mem_heap_used_bytes`
- `opensearch_os_cpu_percent`
- `opensearch_indices_indexing_index_total` (primary, summed rate)
- `opensearch_indices_indexing_index_count` (fallback when `_total` is absent)
