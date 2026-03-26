"""
OpenSearch API collector.

Fetches per-node metrics from GET /_nodes/stats/process,jvm,fs,thread_pool,indices.

All metrics are scoped to the OpenSearch process itself — not the host OS:

  cpu_pct           process.cpu.percent           (OS process CPU, not system-wide)
  heap_pct          jvm.mem.heap_used_percent      (JVM heap — the relevant memory for OS)
  heap_used_bytes   jvm.mem.heap_used_in_bytes
  heap_max_bytes    jvm.mem.heap_max_in_bytes
  disk_store_bytes  indices.store.size_in_bytes    (data owned by OpenSearch on this node)
  disk_total_bytes  fs.total.total_in_bytes        (capacity of the data filesystem)
  disk_pct          disk_store_bytes / disk_total_bytes * 100
  gc_young_ms       jvm.gc.collectors.young.collection_time_in_millis  (cumulative; caller diffs)
  gc_old_ms         jvm.gc.collectors.old.collection_time_in_millis    (cumulative; caller diffs)
  tp_write_queue    thread_pool.write.queue
  tp_write_rejected thread_pool.write.rejected     (cumulative count; caller diffs for rate)
  tp_search_queue   thread_pool.search.queue
  tp_search_rejected thread_pool.search.rejected   (cumulative count; caller diffs for rate)
    index_total       indices.indexing.index_total   (cumulative; caller diffs for rate)
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

# Thread-pool keys we care about
_TP_POOLS = ("write", "search")


def collect(client) -> dict[str, dict[str, Any]]:
    """
    Return per-node metric snapshots.

    Parameters
    ----------
    client:
        An opensearch-py `OpenSearch` client instance.

    Returns
    -------
    dict keyed by node name, each value is a flat metric dict.
    Returns {} on any error so the caller can degrade gracefully.
    """
    try:
        response = client.nodes.stats(
            metric="process,jvm,fs,thread_pool,indices"
        )
    except Exception as exc:
        log.warning("OpenSearch API unreachable: %s", exc)
        return {}

    nodes_raw: dict[str, Any] = response.get("nodes", {})
    if not nodes_raw:
        return {}

    results: dict[str, dict[str, Any]] = {}

    for node_id, node in nodes_raw.items():
        name: str = node.get("name", node_id[:12])

        # ── CPU (OpenSearch process, not system-wide) ─────────────────────
        cpu_pct: float = node.get("process", {}).get("cpu", {}).get("percent", 0.0)

        # ── JVM Heap (the memory metric that matters for OpenSearch) ───────
        jvm_mem = node.get("jvm", {}).get("mem", {})
        heap_used = jvm_mem.get("heap_used_in_bytes", 0)
        heap_max  = jvm_mem.get("heap_max_in_bytes", 0)
        heap_pct  = (heap_used / heap_max * 100.0) if heap_max > 0 else 0.0

        # ── Disk: indices store vs filesystem capacity ─────────────────────
        # indices.store.size_in_bytes = bytes OpenSearch itself wrote on this node
        # fs.total.total_in_bytes     = total capacity of the data filesystem
        store_bytes = node.get("indices", {}).get("store", {}).get("size_in_bytes", 0)
        fs_total    = node.get("fs", {}).get("total", {})
        disk_total  = fs_total.get("total_in_bytes", 0)
        disk_pct    = (store_bytes / disk_total * 100.0) if disk_total > 0 else 0.0

        # ── GC pause time (cumulative ms — caller computes rate) ───────────
        gc_collectors = node.get("jvm", {}).get("gc", {}).get("collectors", {})
        gc_young_ms = gc_collectors.get("young", {}).get("collection_time_in_millis", 0)
        gc_old_ms   = gc_collectors.get("old",   {}).get("collection_time_in_millis", 0)

        # ── Thread pools ──────────────────────────────────────────────────
        tp_raw = node.get("thread_pool", {})
        thread_pool: dict[str, Any] = {}
        for pool in _TP_POOLS:
            pool_data = tp_raw.get(pool, {})
            thread_pool[pool] = {
                "queue":    pool_data.get("queue",    0),
                "rejected": pool_data.get("rejected", 0), 
                "active":   pool_data.get("active",   0),
            }

        index_total = node.get("indices", {}).get("indexing", {}).get("index_total", 0)

        results[name] = {
            "cpu_pct":         round(cpu_pct, 2),
            "heap_pct":        round(heap_pct, 2),
            "heap_used_bytes": heap_used,
            "heap_max_bytes":  heap_max,
            "disk_store_bytes": store_bytes,
            "disk_total_bytes": disk_total,
            "disk_pct":        round(disk_pct, 2),
            "gc_young_ms":     gc_young_ms,  
            "gc_old_ms":       gc_old_ms,
            "thread_pool":     thread_pool,
            "index_total":     index_total,
        }

    return results
