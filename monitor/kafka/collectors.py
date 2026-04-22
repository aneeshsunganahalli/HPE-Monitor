import datetime
import pathlib
import subprocess
import time

import psutil
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from kafka import KafkaAdminClient, KafkaConsumer
    KAFKA_PY = True
except ImportError:
    KAFKA_PY = False

from monitor.config import (
    PROMETHEUS_HOST,
    PROMETHEUS_PORT,
    PROMETHEUS_SCHEME,
    OPENSEARCH_USER,
    OPENSEARCH_PASS,
)


# ── Thresholds / metadata (used by display_utils + views) ─────
THRESHOLDS = {
    "K1": (30, 70),
    "K2": (40, 75),
    "K3": (99, 99),
    "K4": (50, 80),
    "K5": (50, 75),
    "K6": (25, 40),
    "K7": (0, 0),
    "K8": (0, 0),
    "K9": (99, 99),
}
INVERTED = {"K9", "K3"}
METRIC_META = {
    "K1": ("Health", "Time-Based Consumer Lag", "seconds behind"),
    "K2": ("Health", "Message Throughput Ratio", "prod/cons ratio"),
    "K3": ("Health", "Under-Replicated Partitions", "count"),
    "K4": ("Resource", "Kafka Process CPU", "%"),
    "K5": ("Resource", "Kafka Process Memory", "% of system RAM"),
    "K6": ("Resource", "Kafka Total Disk Usage", "% of root disk"),
    "K7": ("Throughput", "Messages Produced Rate", "msgs/sec"),
    "K8": ("Throughput", "Messages Consumed Rate", "msgs/sec"),
    "K9": ("Health", "Active Broker Count", "brokers"),
}
GROUP_COLORS = {"Health": "red", "Resource": "cyan", "Throughput": "yellow"}

PROM_URL = f"{PROMETHEUS_SCHEME}://{PROMETHEUS_HOST}:{PROMETHEUS_PORT}"
PROM_AUTH = (OPENSEARCH_USER, OPENSEARCH_PASS)
PROM_VERIFY = False

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "test-topic"
KAFKA_GROUP = "logstash-consumer-group"
KAFKA_LOG_DIR = "/opt/kafka/kafka/logs"
KAFKA_BASE_DIR = "/opt/kafka/kafka"


# ── Prometheus helpers ─────────────────────────────────────────
def prom_instant(promql: str):
    try:
        r = requests.get(
            f"{PROM_URL}/api/v1/query",
            params={"query": promql},
            auth=PROM_AUTH,
            verify=PROM_VERIFY,
            timeout=8,
        )
        r.raise_for_status()
        res = r.json().get("data", {}).get("result", [])
        return float(res[0]["value"][1]) if res else None
    except Exception:
        return None


def prom_range(promql: str, minutes: int = 30) -> list:
    end = int(time.time())
    start = end - minutes * 60
    step = max(15, (minutes * 60) // 60)
    try:
        r = requests.get(
            f"{PROM_URL}/api/v1/query_range",
            params={"query": promql, "start": start, "end": end, "step": f"{step}s"},
            auth=PROM_AUTH,
            verify=PROM_VERIFY,
            timeout=10,
        )
        r.raise_for_status()
        res = r.json().get("data", {}).get("result", [])
        return [(float(v[0]), float(v[1])) for v in res[0]["values"]] if res else []
    except Exception:
        return []


def prom_instant_vector(promql: str, label: str) -> dict:
    try:
        r = requests.get(
            f"{PROM_URL}/api/v1/query",
            params={"query": promql},
            auth=PROM_AUTH,
            verify=PROM_VERIFY,
            timeout=8,
        )
        r.raise_for_status()
        res = r.json().get("data", {}).get("result", [])
        out = {}
        for item in res:
            metric_labels = item.get("metric", {})
            key = metric_labels.get(label)
            value = item.get("value", [None, None])[1]
            if key is not None and value is not None:
                out[key] = float(value)
        return out
    except Exception:
        return {}


def prom_partition_vector(promql: str) -> dict:
    raw = prom_instant_vector(promql, "partition")
    out = {}
    for k, v in raw.items():
        try:
            out[int(k)] = v
        except Exception:
            continue
    return out


def find_kafka_proc():
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            name = (proc.info.get("name") or "").lower()
            cmdline = " ".join(proc.info.get("cmdline") or []).lower()
            if ("java" in name or "java" in cmdline) and "kafka" in cmdline:
                if "zookeeper" not in cmdline and "console-consumer" not in cmdline:
                    return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


def _safe_round(value, digits=4, default=0.0):
    try:
        return round(float(value), digits)
    except Exception:
        return default


# ══════════════════════════════════════════════════════════════
# COLLECTORS — K1 through K9
# ══════════════════════════════════════════════════════════════

def collect_k1() -> dict:
    """
    How many SECONDS behind the consumer is.
    Formula: lag_messages / production_rate_msgs_per_sec
    Score : 0s = 0 | 300s (5 min) = 100 | capped at 100
    Source : kafka-python (live lag count) + Prometheus (rate)
    """
    lag_msgs = 0

    if KAFKA_PY:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                request_timeout_ms=5000,
            )
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                request_timeout_ms=5000,
            )
            committed = admin.list_consumer_group_offsets(KAFKA_GROUP)
            tps = list(committed.keys())
            end_offs = consumer.end_offsets(tps)
            lag_msgs = sum(max(end_offs.get(tp, 0) - off.offset, 0) for tp, off in committed.items())
            admin.close()
            consumer.close()
        except Exception:
            lag_msgs = prom_instant(f'sum(kafka_consumergroup_lag{{topic="{KAFKA_TOPIC}"}})') or 0
    else:
        lag_msgs = prom_instant(f'sum(kafka_consumergroup_lag{{topic="{KAFKA_TOPIC}"}})') or 0

    prod_rate = prom_instant(
        f'sum(rate(kafka_topic_partition_current_offset{{topic="{KAFKA_TOPIC}"}}[60s]))'
    )
    prod_rate = max(prod_rate or 0.001, 0.001)
    lag_secs = lag_msgs / prod_rate
    score = round(min(lag_secs / 300.0, 1.0) * 100, 2)

    return {
        "value": score,
        "lag_secs": round(lag_secs, 2),
        "lag_msgs": int(lag_msgs),
        "prod_rate": round(prod_rate, 4),
        "source": "kafka-python" if KAFKA_PY else "prometheus-fallback",
    }


def collect_k2() -> dict:
    """
    prod_rate / cons_rate.
    Ratio 1.0 = perfectly balanced → score 0.
    Ratio 2.0+ = consumer severely behind → score 100.
    Score : (ratio - 1.0) × 100, capped 0-100
    Source : Prometheus kafka_exporter
    """
    prod = prom_instant(
        f'sum(rate(kafka_topic_partition_current_offset{{topic="{KAFKA_TOPIC}"}}[60s]))'
    ) or 0.0

    cons = prom_instant(
        f'sum(rate(kafka_consumergroup_current_offset{{topic="{KAFKA_TOPIC}",consumergroup="{KAFKA_GROUP}"}}[60s]))'
    )
    cons = max(cons or 0.001, 0.001)
    ratio = prod / cons
    score = round(min(max(ratio - 1.0, 0.0), 1.0) * 100, 2)

    return {
        "value": score,
        "ratio": round(ratio, 4),
        "prod_rate": round(prod, 4),
        "cons_rate": round(cons, 4),
    }


def collect_k3() -> dict:
    """
    Count of partitions where not all replicas are in sync.
    Score : 0 partitions = 100 | any > 0 = 0 (zero tolerance)
    Source : Prometheus kafka_exporter
    """
    val = prom_instant(
        f'sum(kafka_topic_partition_under_replicated_partition{{topic="{KAFKA_TOPIC}"}})'
    )
    count = int(val) if val is not None else -1
    score = 100 if count == 0 else (0 if count > 0 else 50)
    return {
        "value": score,
        "count": count,
    }


def collect_k4() -> dict:
    """
    CPU % used by the Kafka JVM process specifically.
    Score : direct mapping — cpu_pct IS the score (0-100)
    Source : psutil (process filter by cmdline)
    Note : interval=1.0 means psutil blocks for 1 second to
    measure CPU accurately — this is intentional.
    """
    proc = find_kafka_proc()
    if proc is None:
        return {
            "value": 0,
            "cpu_pct": None,
            "pid": None,
            "status": "PROCESS NOT FOUND",
        }
    try:
        cpu_pct = proc.cpu_percent(interval=1.0)
        return {
            "value": round(min(cpu_pct, 100), 2),
            "cpu_pct": round(cpu_pct, 2),
            "pid": proc.pid,
            "status": "running",
        }
    except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
        return {
            "value": 0,
            "cpu_pct": None,
            "pid": None,
            "status": f"ERROR: {e}",
        }


def collect_k5() -> dict:
    """
    RSS (Resident Set Size) of the Kafka JVM process.
    Score : (rss_bytes / total_system_ram) * 100, × 2 scaling
    so 50% of system RAM consumed = score 100.
    This keeps the score meaningful on a 7.5 GiB system.
    Source : psutil (process filter)
    """
    proc = find_kafka_proc()
    if proc is None:
        return {
            "value": 0,
            "rss_mb": None,
            "rss_pct": None,
            "status": "PROCESS NOT FOUND",
        }
    try:
        mem_info = proc.memory_info()
        total_ram = psutil.virtual_memory().total
        rss_mb = round(mem_info.rss / (1024 ** 2), 2)
        rss_pct = round((mem_info.rss / total_ram) * 100, 2)
        score = round(min(rss_pct * 2.0, 100), 2)
        return {
            "value": score,
            "rss_mb": rss_mb,
            "rss_pct": rss_pct,
            "total_ram_gb": round(total_ram / (1024 ** 3), 2),
            "pid": proc.pid,
            "status": "running",
        }
    except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
        return {
            "value": 0,
            "rss_mb": None,
            "rss_pct": None,
            "status": f"ERROR: {e}",
        }


def _du_bytes(path: str) -> int:
    """Return total bytes used by path via du -sb. Returns 0 on error."""
    try:
        result = subprocess.run(["du", "-sb", path], capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            return int(result.stdout.split()[0])
    except Exception:
        pass
    return 0


def collect_k6() -> dict:
    """
    Total disk footprint of entire Kafka installation vs root partition size.
    Score : (kafka_total_bytes / root_partition_total) * 100
    Source : subprocess du -sb on /opt/kafka/kafka + psutil.disk_usage("/")
    States : SAFE < 25% | WARNING 25-40% | CRITICAL >= 40%
    """
    try:
        kafka_total_bytes = _du_bytes(KAFKA_BASE_DIR)

        root = psutil.disk_usage("/")
        root_total_gb = round(root.total / (1024 ** 3), 3)
        root_used_gb = round(root.used / (1024 ** 3), 3)
        root_free_gb = round(root.free / (1024 ** 3), 3)

        kafka_gb = round(kafka_total_bytes / (1024 ** 3), 3)
        kafka_pct = round((kafka_total_bytes / root.total) * 100, 2) if root.total > 0 else 0
        score = round(min(kafka_pct, 100), 2)
        state = "SAFE" if kafka_pct < 25 else "WARNING" if kafka_pct < 40 else "CRITICAL"

        logs_bytes = _du_bytes(f"{KAFKA_BASE_DIR}/logs")
        libs_bytes = _du_bytes(f"{KAFKA_BASE_DIR}/libs")
        bin_bytes = _du_bytes(f"{KAFKA_BASE_DIR}/bin")
        config_bytes = _du_bytes(f"{KAFKA_BASE_DIR}/config")
        other_bytes = max(kafka_total_bytes - logs_bytes - libs_bytes - bin_bytes - config_bytes, 0)

        msg_bytes = 0
        kraft_bytes = 0
        applog_bytes = 0

        try:
            for item in pathlib.Path(KAFKA_LOG_DIR).iterdir():
                if item.name == "__cluster_metadata-0":
                    kraft_bytes += _du_bytes(str(item))
                elif item.is_dir():
                    msg_bytes += _du_bytes(str(item))
                elif item.is_file() and item.suffix in (".log", ".out"):
                    applog_bytes += item.stat().st_size
        except Exception:
            pass

        index_bytes = max(logs_bytes - msg_bytes - kraft_bytes - applog_bytes, 0)

        def gb(b):
            return round(b / (1024 ** 3), 3)

        return {
            "value": score,
            "kafka_pct": kafka_pct,
            "kafka_gb": kafka_gb,
            "root_total_gb": root_total_gb,
            "root_used_gb": root_used_gb,
            "root_free_gb": root_free_gb,
            "state": state,
            "logs_gb": gb(logs_bytes),
            "libs_gb": gb(libs_bytes),
            "bin_gb": gb(bin_bytes),
            "config_gb": gb(config_bytes),
            "other_gb": gb(other_bytes),
            "msg_gb": gb(msg_bytes),
            "index_gb": gb(index_bytes),
            "kraft_gb": gb(kraft_bytes),
            "applog_gb": gb(applog_bytes),
        }
    except Exception as e:
        return {
            "value": 0,
            "kafka_pct": None,
            "state": "ERROR",
            "error": str(e),
        }


def collect_k7() -> dict:
    """
    Current messages-per-second production rate.
    Informational — no score threshold, raw rate displayed.
    Source : Prometheus kafka_exporter rate()
    """
    rate = prom_instant(
        f'sum(rate(kafka_topic_partition_current_offset{{topic="{KAFKA_TOPIC}"}}[60s]))'
    )
    return {
        "value": round(rate, 4) if rate is not None else None,
        "unit": "msgs/sec",
    }


def collect_k8() -> dict:
    """
    Current messages-per-second consumption rate by Logstash.
    Informational — shown alongside K7 to visualize the gap.
    Source : Prometheus kafka_exporter rate()
    """
    rate = prom_instant(
        f'sum(rate(kafka_consumergroup_current_offset{{topic="{KAFKA_TOPIC}",consumergroup="{KAFKA_GROUP}"}}[60s]))'
    )
    return {
        "value": round(rate, 4) if rate is not None else None,
        "unit": "msgs/sec",
    }


def collect_k9() -> dict:
    """
    Number of active Kafka brokers.
    Score : 1+ broker = 100 | 0 brokers = 0 (hard gate)
    Source : Prometheus kafka_exporter
    """
    val = prom_instant("kafka_brokers")
    count = int(val) if val is not None else 0
    score = 100 if count >= 1 else 0
    return {
        "value": score,
        "count": count,
    }


# ══════════════════════════════════════════════════════════════
# DETAIL COLLECTORS — topic / partition / broker views
# ══════════════════════════════════════════════════════════════

def collect_topic_overview() -> list:
    parts = prom_instant_vector(
        'sum(kafka_topic_partitions) by (topic)',
        'topic',
    )
    prod = prom_instant_vector(
        'sum(rate(kafka_topic_partition_current_offset[5m])) by (topic)',
        'topic',
    )
    offsets = prom_instant_vector(
        'sum(kafka_topic_partition_current_offset) by (topic)',
        'topic',
    )
    urp = prom_instant_vector(
        'sum(kafka_topic_partition_under_replicated_partition) by (topic)',
        'topic',
    )

    all_topics = sorted(set(parts) | set(prod) | set(offsets) | set(urp))
    rows = []

    for topic in all_topics:
        rows.append({
            "topic": topic,
            "partitions": int(parts.get(topic, 0) or 0),
            "produced_rate": round(prod.get(topic, 0.0) or 0.0, 3),
            "current_offset": int(offsets.get(topic, 0.0) or 0),
            "under_replicated": int(urp.get(topic, 0.0) or 0),
            "consumed_rate": 0.0,
            "lag_msgs": 0,
            "throughput_ratio": 1.0,
        })

    rows.sort(
        key=lambda x: (x["under_replicated"], x["partitions"], x["produced_rate"]),
        reverse=True,
    )
    return rows


def _get_topic_partition_metadata(topic: str) -> dict:
    if not KAFKA_PY:
        return {}

    admin = None
    meta = {}
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=5000,
        )
        described = admin.describe_topics([topic])
        for topic_data in described:
            for part in topic_data.get("partitions", []):
                meta[part["partition"]] = {
                    "leader": part.get("leader"),
                    "replicas": len(part.get("replicas", [])),
                    "isr": len(part.get("isr", [])),
                }
    except Exception:
        return {}
    finally:
        if admin is not None:
            try:
                admin.close()
            except Exception:
                pass
    return meta


def collect_partition_overview(topic: str = None):
    topic = topic or KAFKA_TOPIC

    offsets = prom_partition_vector(f'kafka_topic_partition_current_offset{{topic="{topic}"}}')
    prod = prom_partition_vector(f'rate(kafka_topic_partition_current_offset{{topic="{topic}"}}[60s])')
    urp = prom_partition_vector(f'kafka_topic_partition_under_replicated_partition{{topic="{topic}"}}')
    leaders = prom_partition_vector(f'kafka_topic_partition_leader{{topic="{topic}"}}')
    replicas = prom_partition_vector(f'kafka_topic_partition_replicas{{topic="{topic}"}}')
    isr = prom_partition_vector(f'kafka_topic_partition_in_sync_replica{{topic="{topic}"}}')
    lag = prom_instant_vector(f'sum(kafka_consumergroup_lag{{topic="{topic}"}}) by (partition)', 'partition')
    cons = prom_instant_vector(f'sum(rate(kafka_consumergroup_current_offset{{topic="{topic}"}}[60s])) by (partition)', 'partition')

    def norm(d):
        out = {}
        for k, v in d.items():
            try:
                out[int(k)] = v
            except Exception:
                continue
        return out

    offsets = norm(offsets)
    prod = norm(prod)
    urp = norm(urp)
    leaders = norm(leaders)
    replicas = norm(replicas)
    isr = norm(isr)
    lag = norm(lag)
    cons = norm(cons)

    all_parts = sorted(set(offsets) | set(prod) | set(urp) | set(leaders) | set(replicas) | set(isr) | set(lag) | set(cons))

    rows = []
    for pid in all_parts:
        rows.append({
            "topic": topic,
            "partition": pid,
            "leader": int(leaders.get(pid, -1)) if pid in leaders else None,
            "replicas": int(replicas.get(pid, 0)) if pid in replicas else None,
            "isr": int(isr.get(pid, 0)) if pid in isr else None,
            "lag_msgs": int(lag.get(pid, 0.0) or 0),
            "produced_rate": round(prod.get(pid, 0.0) or 0.0, 3),
            "consumed_rate": round(cons.get(pid, 0.0) or 0.0, 3),
            "throughput_ratio": round((prod.get(pid, 0.0) or 0.0) / max(cons.get(pid, 0.0) or 0.0, 0.001), 3),
            "under_replicated": int(urp.get(pid, 0.0) or 0),
            "current_offset": int(offsets.get(pid, 0.0) or 0),
        })

    rows.sort(key=lambda x: (x["under_replicated"], x["lag_msgs"], x["produced_rate"]), reverse=True)
    return rows


def collect_broker_performance() -> list:
    broker_count = prom_instant("kafka_brokers")
    count = int(broker_count) if broker_count is not None else 0

    rows = []
    for i in range(count):
        rows.append({
            "broker": f"broker-{i}",
            "up": 1,
            "bytes_in": 0.0,
            "bytes_out": 0.0,
            "request_rate": 0.0,
            "leader_partitions": 0,
        })

    if not rows:
        rows.append({
            "broker": "cluster",
            "up": 0,
            "bytes_in": 0.0,
            "bytes_out": 0.0,
            "request_rate": 0.0,
            "leader_partitions": 0,
        })

    return rows


# ══════════════════════════════════════════════════════════════
# MASTER COLLECTOR
# ══════════════════════════════════════════════════════════════

def collect_all() -> dict:
    """
    Collect all 9 Kafka metrics in one pass.
    K4 blocks for 1 second (CPU measurement interval) — this is
    intentional and gives accurate CPU readings.
    """
    return {
        "timestamp": datetime.datetime.now().isoformat(),
        "K1": collect_k1(),
        "K2": collect_k2(),
        "K3": collect_k3(),
        "K4": collect_k4(),
        "K5": collect_k5(),
        "K6": collect_k6(),
        "K7": collect_k7(),
        "K8": collect_k8(),
        "K9": collect_k9(),
    }

def collect_all() -> dict:
    return {
        "timestamp": datetime.datetime.now().isoformat(),
        "K1": collect_k1(),
        "K2": collect_k2(),
        "K3": collect_k3(),
        "K4": collect_k4(),
        "K5": collect_k5(),
        "K6": collect_k6(),
        "K7": collect_k7(),
        "K8": collect_k8(),
        "K9": collect_k9(),
    }


# Backward compatibility aliases
collectall = collect_all
METRICMETA = METRIC_META
GROUPCOLORS = GROUP_COLORS
prominstant = prom_instant
promrange = prom_range
