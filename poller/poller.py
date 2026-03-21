"""
Poller orchestrator.

Runs a continuous loop that:
  1. Calls the OpenSearch API collector (CPU, heap, disk, GC, thread pool)
  2. Calls the system collector (FD count, process-level I/O)
  3. Computes rate metrics from cumulative counters (GC pause rate, I/O rate)
  4. Assembles a single timestamped record
  5. Appends it to the JSONL store

GC Pause Rate
-------------
Stored as `gc_pause_rate_ms_per_s` — milliseconds of GC pause per second of
wall time. Range 0–1000. Values > 100 indicate noticeable GC pressure.

Thread Pool Rejected Rate
-------------------------
Stored as `tp_<pool>_rejected_per_s` — new rejections in this interval / elapsed s.
If the cumulative counter resets (node restart), the delta is discarded (set to 0).

I/O Rate
--------
Stored as `io_read_bps` / `io_write_bps` (bytes per second) computed from the
per-process /proc/<pid>/io deltas.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from opensearchpy import OpenSearch
import urllib3

from poller.config import (
    OPENSEARCH_HOST,
    OPENSEARCH_PORT,
    OPENSEARCH_USER,
    OPENSEARCH_PASS,
    OPENSEARCH_SSL,
    OS_PROCESS_KEYWORD,
)
from poller.collectors import opensearch_api, system as sys_collector
from poller.storage.writer import append_record

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)


def _build_os_client() -> OpenSearch:
    scheme = "https" if OPENSEARCH_SSL else "http"
    return OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
        use_ssl=OPENSEARCH_SSL,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        scheme=scheme,
    )


def _safe_delta(current: int | float, previous: int | float | None) -> float:
    """Return current - previous, clamped to 0 to handle counter resets."""
    if previous is None:
        return 0.0
    delta = current - previous
    return max(0.0, float(delta))


def _compute_gc_rate(
    node_name: str,
    snapshot: dict[str, Any],
    prev_snapshots: dict[str, dict[str, Any]],
    elapsed_s: float,
) -> float:
    """Return total GC pause ms per second of wall time for this node."""
    if elapsed_s <= 0:
        return 0.0

    prev = prev_snapshots.get(node_name, {})
    young_delta = _safe_delta(snapshot["gc_young_ms"], prev.get("gc_young_ms"))
    old_delta   = _safe_delta(snapshot["gc_old_ms"],   prev.get("gc_old_ms"))
    total_gc_ms = young_delta + old_delta
    return round(total_gc_ms / elapsed_s, 3)


def _compute_tp_rejected_rates(
    node_name: str,
    snapshot: dict[str, Any],
    prev_snapshots: dict[str, dict[str, Any]],
    elapsed_s: float,
) -> dict[str, float]:
    """Return per-pool rejected-per-second rates."""
    if elapsed_s <= 0:
        return {}

    prev_tp = prev_snapshots.get(node_name, {}).get("thread_pool", {})
    rates: dict[str, float] = {}
    for pool, data in snapshot.get("thread_pool", {}).items():
        prev_rejected = prev_tp.get(pool, {}).get("rejected")
        delta = _safe_delta(data["rejected"], prev_rejected)
        rates[f"tp_{pool}_rejected_per_s"] = round(delta / elapsed_s, 4)
    return rates


def _compute_io_rates(
    host_snapshot: dict[str, Any],
    prev_host: dict[str, Any] | None,
    elapsed_s: float,
) -> dict[str, float]:
    """Return process-level I/O byte rates."""
    if elapsed_s <= 0 or prev_host is None:
        return {}

    rates: dict[str, float] = {}
    for key, rate_key in (
        ("io_read_bytes",  "io_read_bps"),
        ("io_write_bytes", "io_write_bps"),
    ):
        curr = host_snapshot.get(key)
        prev = prev_host.get(key)
        if curr is not None and prev is not None:
            rates[rate_key] = round(_safe_delta(curr, prev) / elapsed_s, 2)
    return rates


def run(
    output_dir: str | Path,
    interval: int,
    verbose: bool = False,
) -> None:
    """
    Main polling loop. Runs until KeyboardInterrupt.

    Parameters
    ----------
    output_dir:
        Directory where JSONL files are written.
    interval:
        Seconds between poll cycles.
    verbose:
        If True, prints each record to stdout after writing.
    """
    client = _build_os_client()

    # State carried across poll cycles for delta computation
    prev_api_snapshots: dict[str, dict[str, Any]] = {}   # {node_name: last raw snapshot}
    prev_host_snapshot: dict[str, Any] | None = None
    prev_tick: float | None = None

    print(f"[poller] Starting — interval={interval}s  output={Path(output_dir).resolve()}")
    print("[poller] Press Ctrl+C to stop.\n")

    try:
        while True:
            tick_start = time.monotonic()
            now_epoch  = int(time.time())
            now_iso    = datetime.now(timezone.utc).astimezone().isoformat()
            elapsed_s  = (tick_start - prev_tick) if prev_tick is not None else float(interval)

            # ── Collect ────────────────────────────────────────────────────
            api_snapshots = opensearch_api.collect(client)
            host_snapshot = sys_collector.collect(OS_PROCESS_KEYWORD)

            # ── Build enriched per-node records ───────────────────────────
            nodes_out: dict[str, Any] = {}
            for node_name, snap in api_snapshots.items():
                gc_rate = _compute_gc_rate(node_name, snap, prev_api_snapshots, elapsed_s)
                tp_rates = _compute_tp_rejected_rates(node_name, snap, prev_api_snapshots, elapsed_s)

                nodes_out[node_name] = {
                    # CPU (OpenSearch process %)
                    "cpu_pct":              snap["cpu_pct"],
                    # JVM Heap (the memory that matters for OpenSearch)
                    "heap_pct":             snap["heap_pct"],
                    "heap_used_bytes":      snap["heap_used_bytes"],
                    "heap_max_bytes":       snap["heap_max_bytes"],
                    # Disk: bytes OpenSearch owns vs filesystem capacity
                    "disk_store_bytes":     snap["disk_store_bytes"],
                    "disk_total_bytes":     snap["disk_total_bytes"],
                    "disk_pct":             snap["disk_pct"],
                    # GC pause rate (ms of GC per second of wall time)
                    "gc_pause_rate_ms_per_s": gc_rate,
                    # Thread pool (current queue depth + rejected rate)
                    "thread_pool":          snap["thread_pool"],
                    **tp_rates,
                }

            # ── Build host record (FD + I/O) ───────────────────────────────
            host_out: dict[str, Any] = {}
            if host_snapshot:
                io_rates = _compute_io_rates(host_snapshot, prev_host_snapshot, elapsed_s)
                host_out["pid"] = host_snapshot.get("pid")

                # FD / IO metrics — only available when running as the same OS user as OpenSearch
                if host_snapshot.get("permission_error"):
                    host_out["fd_note"] = (
                        "Run poller as the opensearch OS user or with sudo "
                        "to enable FD and I/O metric collection"
                    )
                else:
                    if "fd_count" in host_snapshot:
                        host_out["fd_count"] = host_snapshot["fd_count"]
                        host_out["fd_limit"] = host_snapshot["fd_limit"]
                        host_out["fd_pct"]   = host_snapshot["fd_pct"]
                    if "io_read_bytes" in host_snapshot:
                        host_out["io_read_bytes"]  = host_snapshot["io_read_bytes"]
                        host_out["io_write_bytes"] = host_snapshot["io_write_bytes"]
                    host_out.update(io_rates)

            # ── Assemble full record ───────────────────────────────────────
            record: dict[str, Any] = {
                "ts":        now_epoch,
                "timestamp": now_iso,
                "nodes":     nodes_out,
                "host":      host_out,
            }

            # ── Persist ───────────────────────────────────────────────────
            written_path = append_record(output_dir, record)

            # ── Update state for next cycle ────────────────────────────────
            prev_api_snapshots = api_snapshots
            prev_host_snapshot = host_snapshot if host_snapshot else prev_host_snapshot
            prev_tick = tick_start

            # ── Status line ───────────────────────────────────────────────
            node_str = ", ".join(nodes_out.keys()) if nodes_out else "no nodes"
            if host_out:
                fd_val = host_out.get("fd_count", "no-perms")
                host_status = f"pid={host_out.get('pid')} fd={fd_val}"
            else:
                host_status = "process not found"
            print(f"[{now_iso}]  nodes=[{node_str}]  host={host_status}  → {written_path.name}")

            if verbose:
                import json
                print(json.dumps(record, indent=2))

            # ── Sleep for the remainder of the interval ────────────────────
            elapsed_wall = time.monotonic() - tick_start
            sleep_s = max(0.0, interval - elapsed_wall)
            time.sleep(sleep_s)

    except KeyboardInterrupt:
        print("\n[poller] Stopped.")
