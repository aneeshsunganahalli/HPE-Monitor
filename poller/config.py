"""
Poller configuration.

Reads from the same .env as monitor/config.py. Override any value via
environment variables before running.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# ─── OpenSearch connection (shared with monitor/) ─────────────────────────────
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT = _env_int("OPENSEARCH_PORT", 9200)
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER", "admin")
OPENSEARCH_PASS = os.getenv("OPENSEARCH_PASS", "admin")
OPENSEARCH_SSL  = _env_bool("OPENSEARCH_SSL", False)

# ─── Poller settings ──────────────────────────────────────────────────────────
POLL_INTERVAL_SECONDS = _env_int("POLL_INTERVAL_SECONDS", 15)

# Override with OUTPUT_DIR env var or --output-dir CLI flag.
DEFAULT_OUTPUT_DIR = Path(__file__).parent / "data"

# Name of the OpenSearch JVM process — used to find PID for system metrics
OS_PROCESS_KEYWORD = os.getenv(
    "OS_PROCESS_KEYWORD", "org.opensearch.bootstrap.OpenSearch"
)

# Leave empty to auto-detect from the device that "/" is mounted on.
OS_DATA_DEVICE = os.getenv("OS_DATA_DEVICE", "")
