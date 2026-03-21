"""
OpenSearch client and data-fetching helpers.

All OpenSearch API calls are centralized here. Each function wraps errors
and returns structured Python dicts/lists so views never touch raw HTTP.
"""

from __future__ import annotations


from typing import Any

import urllib3
from opensearchpy import OpenSearch

from monitor.config import (
    OPENSEARCH_HOST,
    OPENSEARCH_PORT,
    OPENSEARCH_USER,
    OPENSEARCH_PASS,
    OPENSEARCH_SSL,
    OPENSEARCH_INDEX,
    console,
)
from monitor.metrics_service import get_metrics_provider
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_os_client() -> OpenSearch:
    """Return a configured OpenSearch client instance."""
    scheme = "https" if OPENSEARCH_SSL else "http"
    return OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
        use_ssl=OPENSEARCH_SSL,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        scheme=scheme,
        timeout=30,
        max_retries=2,
        retry_on_timeout=True,
    )


def fetch_cluster_health() -> dict:
    """GET /_cluster/health → cluster status, node count, shard counts."""
    try:
        client = get_os_client()
        return client.cluster.health()
    except Exception as e:
        console.print(f"[red]Error fetching cluster health:[/red] {e}")
        return {}


def fetch_cluster_stats() -> dict:
    """GET /_cluster/stats → cluster-wide aggregated CPU, JVM, OS memory, disk, and index stats."""
    try:
        client = get_os_client()
        return client.cluster.stats()
    except Exception as e:
        console.print(f"[red]Error fetching cluster stats:[/red] {e}")
        return {}


def fetch_node_stats() -> dict:
    """GET /_nodes/stats/os,jvm,fs,indices → per-node CPU, memory, JVM heap, disk, and indexing/search stats."""
    try:
        return get_metrics_provider().fetch_node_stats(timeframe="real-time")
    except Exception as e:
        console.print(f"[red]Error fetching node stats:[/red] {e}")
        return {}


def fetch_node_stats_for_timeframe(timeframe: str) -> dict[str, Any]:
    """Route node stats lookup by timeframe using the MetricsProvider."""
    try:
        return get_metrics_provider().fetch_node_stats(timeframe=timeframe)
    except Exception as e:
        console.print(f"[red]Error fetching node stats for timeframe '{timeframe}':[/red] {e}")
        return {}


def fetch_disk_allocation() -> list:
    """GET /_cat/allocation?v&format=json → disk used/total per node."""
    try:
        client = get_os_client()
        return client.cat.allocation(format="json", v=True)
    except Exception as e:
        console.print(f"[red]Error fetching disk allocation:[/red] {e}")
        return []


def fetch_indices() -> list:
    """GET /_cat/indices?v&s=store.size:desc&format=json → index names and sizes."""
    try:
        client = get_os_client()
        return client.cat.indices(format="json", v=True, s="store.size:desc")
    except Exception as e:
        console.print(f"[red]Error fetching indices:[/red] {e}")
        return []


def fetch_shards(index: str = None) -> list:
    """GET /_cat/shards?v&format=json → shard states. Optionally filter by index."""
    try:
        client = get_os_client()
        if index:
            return client.cat.shards(index=index, format="json", v=True)
        return client.cat.shards(format="json", v=True)
    except Exception as e:
        console.print(f"[red]Error fetching shards:[/red] {e}")
        return []


def fetch_data_streams() -> dict:
    """GET /_data_stream → list of data streams with name, store size, and latest timestamp."""
    try:
        client = get_os_client()
        return client.indices.get_data_stream()
    except Exception as e:
        console.print(f"[red]Error fetching data streams:[/red] {e}")
        return {}



def fetch_bottleneck_metrics(node_name: str) -> dict[str, float | None]:
    """Fetch Performance Analyzer diagnostics for bottleneck triage."""
    try:
        return get_metrics_provider().fetch_performance_analyzer_metrics(node_name=node_name)
    except Exception as e:
        console.print(f"[yellow]Unable to fetch diagnostics for '{node_name}':[/yellow] {e}")
        return {
            "disk_utilization": None,
            "os_memory_utilization": None,
        }


def search_logs(query_str: str = "*", minutes: int = 30, size: int = 20, level: str = None) -> list:
    """Search for logs using query string and filters."""
    must = [
        {
            "query_string": {
                "query": query_str,
                "analyze_wildcard": False
            }
        },
        {"range": {"@timestamp": {"gte": f"now-{minutes}m", "lte": "now"}}}
    ]
    if level:
        must.append({"match": {"log.level": level.lower()}})

    body = {
        "size": size,
        "timeout": "20s",
        "sort": [{"@timestamp": {"order": "desc"}}],
        "query": {"bool": {"must": must}},
        "_source": ["@timestamp", "message", "log.level", "hostname", "instance", "program"]
    }
    try:
        client = get_os_client()
        return client.search(index=OPENSEARCH_INDEX, body=body)["hits"]["hits"]
    except Exception as e:
        console.print(f"[red]Error searching logs:[/red] {e}")
        return []


def fetch_error_summary(minutes: int = 60) -> list:
    """Fetch aggregation of errors and warnings by host."""
    body = {
        "size": 0,
        "timeout": "20s",
        "query": {
            "bool": {
                "must": [
                    {"range": {"@timestamp": {"gte": f"now-{minutes}m", "lte": "now"}}},
                    {"terms": {"log.level": ["error", "warn", "warning", "critical"]}}
                ]
            }
        },
        "aggs": {
            "by_host": {
                "terms": {"field": "hostname.keyword", "size": 10},
                "aggs": {
                    "by_level": {"terms": {"field": "log.level.keyword", "size": 5}}
                }
            }
        }
    }
    try:
        client = get_os_client()
        res = client.search(index=OPENSEARCH_INDEX, body=body)
        return res.get("aggregations", {}).get("by_host", {}).get("buckets", [])
    except Exception as e:
        console.print(f"[red]Error fetching error summary:[/red] {e}")
        return []


def fetch_log_rate(minutes: int = 60, interval: str = "5m") -> list:
    """Fetch log rate distribution over time."""
    body = {
        "size": 0,
        "timeout": "20s",
        "query": {"range": {"@timestamp": {"gte": f"now-{minutes}m", "lte": "now"}}},
        "aggs": {
            "over_time": {
                "date_histogram": {
                    "field": "@timestamp",
                    "fixed_interval": interval,
                    "min_doc_count": 0
                },
                "aggs": {
                    "by_level": {"terms": {"field": "log.level.keyword", "size": 5}}
                }
            }
        }
    }
    try:
        client = get_os_client()
        res = client.search(index=OPENSEARCH_INDEX, body=body)
        return res.get("aggregations", {}).get("over_time", {}).get("buckets", [])
    except Exception as e:
        console.print(f"[red]Error fetching log rate:[/red] {e}")
        return []


def fetch_logs_for_spike(start: str, end: str, size: int = 100) -> list:
    """Fetch logs within a specific time range for RCA."""
    body = {
        "size": size,
        "timeout": "20s",
        "sort": [{"@timestamp": {"order": "asc"}}],
        "query": {"range": {"@timestamp": {"gte": start, "lte": end}}},
        "_source": ["@timestamp", "message", "log.level", "hostname", "program"]
    }
    try:
        client = get_os_client()
        return client.search(index=OPENSEARCH_INDEX, body=body)["hits"]["hits"]
    except Exception as e:
        console.print(f"[red]Error fetching logs for spike:[/red] {e}")
        return []
