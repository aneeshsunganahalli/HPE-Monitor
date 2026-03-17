"""
OpenSearch client and data-fetching helpers.

All OpenSearch API calls are centralized here. Each function wraps errors
and returns structured Python dicts/lists so views never touch raw HTTP.
"""

import urllib3
from opensearchpy import OpenSearch

from monitor.config import (
    OPENSEARCH_HOST,
    OPENSEARCH_PORT,
    OPENSEARCH_USER,
    OPENSEARCH_PASS,
    OPENSEARCH_SSL,
    console,
)

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
        client = get_os_client()
        return client.nodes.stats(metric="os,jvm,fs,indices")
    except Exception as e:
        console.print(f"[red]Error fetching node stats:[/red] {e}")
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
