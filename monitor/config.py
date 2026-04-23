"""
Configuration constants for the OpenSearch CLI Monitor.

Edit these values to match your OpenSearch cluster setup.
"""

import os
from rich.console import Console

from dotenv import load_dotenv

load_dotenv()


def _env_int(name: str, default: int) -> int:
    """Read an integer env var with a safe fallback."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    """Read a boolean env var with permissive true/false values."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_choice(name: str, default: str, allowed: set[str]) -> str:
    """Read a string enum env var and safely fall back when invalid."""
    raw = os.getenv(name)
    if raw is None:
        return default

    normalized = raw.strip().lower()
    if normalized in allowed:
        return normalized
    return default

# ─────────────────────────── CONFIG ────────────────────────────
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT = _env_int("OPENSEARCH_PORT", 9200)
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER", "admin")
OPENSEARCH_PASS = os.getenv("OPENSEARCH_PASS", "admin")
OPENSEARCH_SSL = _env_bool("OPENSEARCH_SSL", False)

PROMETHEUS_HOST = os.getenv("PROMETHEUS_HOST", OPENSEARCH_HOST)
PROMETHEUS_PORT = _env_int("PROMETHEUS_PORT", 9090)
PROMETHEUS_SCHEME = os.getenv("PROMETHEUS_SCHEME", "http")
PROMETHEUS_TIMEOUT_SECONDS = _env_int("PROMETHEUS_TIMEOUT_SECONDS", 6)
PROMETHEUS_USER = os.getenv("PROMETHEUS_USER", "")
PROMETHEUS_PASS = os.getenv("PROMETHEUS_PASS", "")

PA_HOST = os.getenv("PA_HOST", OPENSEARCH_HOST)
PA_PORT = _env_int("PA_PORT", 9600)
PA_SCHEME = os.getenv("PA_SCHEME", "http")
PA_TIMEOUT_SECONDS = _env_int("PA_TIMEOUT_SECONDS", 4)
POLLER_DATA_DIR = os.getenv("POLLER_DATA_DIR", "poller/data")
HISTORICAL_METRICS_SOURCE = _env_choice(
    "HISTORICAL_METRICS_SOURCE",
    "auto",
    {"auto", "poller", "prometheus"},
)
# ───────────────────────────────────────────────────────────────

# ─────────────────────── THRESHOLDS ────────────────────────────
# CPU thresholds (percentage)
CPU_WARN = 70
CPU_CRIT = 90

# JVM Heap thresholds (percentage)
HEAP_WARN = 75
HEAP_CRIT = 90

# System RAM thresholds (percentage)
MEM_WARN = 85
MEM_CRIT = 95

# Disk thresholds (percentage)
DISK_WARN = 80
DISK_CRIT = 90
# ───────────────────────────────────────────────────────────────

# ──────────────── Log Configuration ────────────────────────────
OPENSEARCH_INDEX = os.getenv("OPENSEARCH_INDEX", "system-logs-*")

# Log colors for UI consistency
LOG_COLORS = {
    "error": "red",
    "critical": "bold red",
    "warn": "yellow",
    "warning": "yellow",
    "info": "cyan",
    "debug": "dim",
}

# ──────────────── Analysis Patterns ────────────────────────────
# Keyword tags for anomaly interpretation
KEYWORD_TAGS = {
    "CPU": ["cpu", "merge", "aggregat", "lucene"],
    "HEAP": ["heap", "memory", "outofmemory", "oom"],
    "GC": ["gc", "garbage", "pause", "overhead"],
    "DISK": ["disk", "watermark", "flood", "space", "read-only", "readonly"],
    "THREAD": ["rejected", "queue", "bulk", "thread pool"],
    "SEARCH": ["timeout", "slowlog", "slow", "circuit"],
}

# Diagnostic labels for root cause analysis
ROOT_CAUSE_PATTERNS = [
    ("OutOfMemoryError", "🔴 JVM OOM — heap exhausted"),
    ("GC overhead limit", "🟠 GC overhead — excessive garbage collection"),
    ("disk usage exceeded", "🔴 Disk full / watermark breached"),
    ("circuit_breaking_exception", "🟠 Circuit breaker tripped — memory pressure"),
    ("flood stage", "🔴 Disk flood-stage — index set read-only"),
    ("high disk watermark", "🟡 High disk watermark crossed"),
    ("rejected execution", "🟠 Thread pool rejection — queue full"),
    ("bulk rejected", "🟠 Bulk indexing rejected — backpressure"),
    ("timeout", "🟡 Operation timeout"),
    ("failed to obtain", "🟡 Lock/resource contention"),
    ("connection refused", "🟡 Downstream connection refused"),
    ("shard failed", "🔴 Shard failure"),
    ("unassigned", "🟡 Unassigned shards detected"),
    ("slowlog", "🟡 Slow query/index detected"),
]
# ───────────────────────────────────────────────────────────────

# Shared console instance
console = Console()
