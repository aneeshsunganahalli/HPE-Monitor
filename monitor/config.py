"""
Configuration constants for the OpenSearch CLI Monitor.

Edit these values to match your OpenSearch cluster setup.
"""

from rich.console import Console
import os

from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────── CONFIG ────────────────────────────
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST")
OPENSEARCH_PORT = os.getenv("OPENSEARCH_PORT")
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER")
OPENSEARCH_PASS = os.getenv("OPENSEARCH_PASS")
OPENSEARCH_SSL = os.getenv("OPENSEARCH_SSL")
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

# Shared console instance
console = Console()
