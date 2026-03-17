import sys
import time
import datetime
import urllib3
from opensearchpy import OpenSearch
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt
from rich import box

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────── CONFIG ────────────────────────────
OPENSEARCH_HOST  = "localhost"
OPENSEARCH_PORT  = 9200
OPENSEARCH_USER  = "admin"
OPENSEARCH_PASS  = "admin"
OPENSEARCH_INDEX = "system-logs-*"
OPENSEARCH_SSL   = False
# ───────────────────────────────────────────────────────────────

console = Console()


# ──────────────── Client Factory ───────────────────────────────

def get_client() -> OpenSearch:
    return OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
        use_ssl=OPENSEARCH_SSL,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        scheme="http",
        timeout=30,
        max_retries=2,
        retry_on_timeout=True,
    )


# ──────────────── OpenSearch Metric Helpers ─────────────────────

def os_cluster_health() -> dict:
    try:
        return get_client().cluster.health()
    except Exception as e:
        console.print(f"[red]Cluster health error:[/red] {e}")
        return {}


def os_nodes_stats() -> dict:
    try:
        return get_client().nodes.stats(
            metric=["jvm", "os", "fs", "thread_pool", "indices", "process"]
        )
    except Exception as e:
        console.print(f"[red]Nodes stats error:[/red] {e}")
        return {}


def os_indices_stats() -> dict:
    try:
        return get_client().indices.stats(index=OPENSEARCH_INDEX)
    except Exception as e:
        console.print(f"[red]Indices stats error:[/red] {e}")
        return {}


def os_pending_tasks() -> list:
    try:
        return get_client().cluster.pending_tasks().get("tasks", [])
    except Exception as e:
        console.print(f"[red]Pending tasks error:[/red] {e}")
        return []


def os_cat_shards() -> list:
    try:
        return get_client().cat.shards(
            format="json",
            h="index,shard,prirep,state,unassigned.reason,node"
        )
    except Exception as e:
        console.print(f"[red]Shards error:[/red] {e}")
        return []


# ──────────────── Log Helpers ───────────────────────────────────

def os_search_logs(query_str="*", minutes=30, size=20, level=None) -> list:
    must = [
        {
            "query_string": {
                "query": query_str,
                "analyze_wildcard": False          # no expensive wildcard scans
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
        "_source": ["@timestamp", "message", "log.level",
                    "hostname", "instance", "program"]
    }
    try:
        return get_client().search(index=OPENSEARCH_INDEX, body=body)["hits"]["hits"]
    except Exception as e:
        console.print(f"[red]Log search error:[/red] {e}")
        return []


def os_search_logs_by_keywords(keywords: list, minutes=60, size=30) -> list:
    """
    Uses `match` queries — leverages inverted index properly.
    No leading wildcards, no full scans, no timeouts.
    """
    should_clauses = [{"match": {"message": kw}} for kw in keywords]
    body = {
        "size": size,
        "timeout": "20s",
        "sort": [{"@timestamp": {"order": "desc"}}],
        "query": {
            "bool": {
                "must": [
                    {"range": {"@timestamp": {"gte": f"now-{minutes}m", "lte": "now"}}}
                ],
                "should": should_clauses,
                "minimum_should_match": 1
            }
        },
        "_source": ["@timestamp", "message", "log.level",
                    "hostname", "instance", "program"]
    }
    try:
        return get_client().search(index=OPENSEARCH_INDEX, body=body)["hits"]["hits"]
    except Exception as e:
        console.print(f"[red]Keyword log search error:[/red] {e}")
        return []


def os_error_summary(minutes=60) -> list:
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
        res = get_client().search(index=OPENSEARCH_INDEX, body=body)
        return res.get("aggregations", {}).get("by_host", {}).get("buckets", [])
    except Exception as e:
        console.print(f"[red]Error summary error:[/red] {e}")
        return []


def os_log_rate_over_time(minutes=60, interval="5m") -> list:
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
        res = get_client().search(index=OPENSEARCH_INDEX, body=body)
        return res.get("aggregations", {}).get("over_time", {}).get("buckets", [])
    except Exception as e:
        console.print(f"[red]Log rate error:[/red] {e}")
        return []


def os_spike_root_cause(spike_ts_iso: str, window_min=5) -> list:
    PATTERNS = [
        ("OutOfMemoryError",           "🔴 JVM OOM — heap exhausted"),
        ("GC overhead limit",          "🟠 GC overhead — excessive garbage collection"),
        ("disk usage exceeded",        "🔴 Disk full / watermark breached"),
        ("circuit_breaking_exception", "🟠 Circuit breaker tripped — memory pressure"),
        ("flood stage",                "🔴 Disk flood-stage — index set read-only"),
        ("high disk watermark",        "🟡 High disk watermark crossed"),
        ("rejected execution",         "🟠 Thread pool rejection — queue full"),
        ("bulk rejected",              "🟠 Bulk indexing rejected — backpressure"),
        ("timeout",                    "🟡 Operation timeout"),
        ("failed to obtain",           "🟡 Lock/resource contention"),
        ("connection refused",         "🟡 Downstream connection refused"),
        ("shard failed",               "🔴 Shard failure"),
        ("unassigned",                 "🟡 Unassigned shards detected"),
        ("slowlog",                    "🟡 Slow query/index detected"),
    ]

    spike_dt = datetime.datetime.fromisoformat(spike_ts_iso.replace("Z", ""))
    start    = (spike_dt - datetime.timedelta(minutes=window_min)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end      = (spike_dt + datetime.timedelta(minutes=window_min)).strftime("%Y-%m-%dT%H:%M:%SZ")

    body = {
        "size": 100,
        "timeout": "20s",
        "sort": [{"@timestamp": {"order": "asc"}}],
        "query": {"range": {"@timestamp": {"gte": start, "lte": end}}},
        "_source": ["@timestamp", "message", "log.level", "hostname", "program"]
    }
    try:
        hits = get_client().search(index=OPENSEARCH_INDEX, body=body)["hits"]["hits"]
    except Exception as e:
        console.print(f"[red]Spike root cause error:[/red] {e}")
        return []

    annotated = []
    seen = set()
    for h in hits:
        src = h["_source"]
        msg = src.get("message", "")
        if msg[:120] in seen:
            continue
        seen.add(msg[:120])
        reason = next(
            (label for kw, label in PATTERNS if kw.lower() in msg.lower()),
            None
        )
        annotated.append({
            "ts":     src.get("@timestamp", "")[:19].replace("T", " "),
            "host":   src.get("hostname", "—"),
            "level":  src.get("log", {}).get("level", "info").lower(),
            "msg":    msg[:220],
            "reason": reason
        })

    annotated.sort(key=lambda x: (x["reason"] is None, x["ts"]))
    return annotated


# ──────────────── Shared Helpers ───────────────────────────────

LEVEL_COLORS = {
    "error":    "red",
    "critical": "bold red",
    "warn":     "yellow",
    "warning":  "yellow",
    "info":     "cyan",
    "debug":    "dim"
}

def _lvl_colored(lvl: str) -> str:
    col = LEVEL_COLORS.get(lvl, "white")
    return f"[{col}]{lvl.upper()}[/{col}]"


# ──────────────── Anomaly → Log Correlator ─────────────────────

ANOMALY_KEYWORDS = {
    "cpu":    ["cpu", "merge", "aggregation", "lucene", "indexing"],
    "heap":   ["heap", "memory", "OutOfMemoryError", "GC", "jvm"],
    "gc":     ["GC", "garbage", "pause", "overhead", "collection"],
    "disk":   ["disk", "watermark", "flood", "space", "read-only",
               "readonly", "filesystem"],
    "thread": ["rejected", "queue", "bulk", "thread pool", "backpressure"],
    "search": ["timeout", "slowlog", "slow query",
               "circuit breaker", "circuit_breaking"],
}

KEYWORD_TAGS = {
    "CPU":    ["cpu", "merge", "aggregat", "lucene"],
    "HEAP":   ["heap", "memory", "outofmemory", "oom"],
    "GC":     ["gc", "garbage", "pause", "overhead"],
    "DISK":   ["disk", "watermark", "flood", "space", "read-only", "readonly"],
    "THREAD": ["rejected", "queue", "bulk", "thread pool"],
    "SEARCH": ["timeout", "slowlog", "slow", "circuit"],
}


def _correlate_node_logs_now(node_name: str, reasons: list):
    console.print(Panel(
        "\n".join(f"  ⚠  {r}" for r in reasons),
        title="[bold red]⚡ Metric Anomalies Detected[/bold red]",
        border_style="red", expand=False
    ))

    reason_text = " ".join(reasons).lower()
    keywords = []
    for key, kws in ANOMALY_KEYWORDS.items():
        if key in reason_text or any(key in r.lower() for r in reasons):
            keywords.extend(kws)

    keywords = list(dict.fromkeys(keywords))   # deduplicate, preserve order
    if not keywords:
        keywords = ["error", "warn", "failed", "exception"]

    console.print(
        f"\n[dim]  Querying logs with keywords:[/dim] "
        f"[cyan]{', '.join(keywords[:10])}[/cyan]\n"
    )

    hits = os_search_logs_by_keywords(keywords, minutes=60, size=30)

    if not hits:
        console.print(
            "[yellow]  No keyword-matched logs — falling back to recent errors...[/yellow]"
        )
        hits = os_search_logs("*", minutes=60, size=15, level="error")

    if not hits:
        console.print(Panel(
            "[yellow]No logs found in the last 60 minutes.\n\n"
            "[dim]Possible reasons:\n"
            "  • Logstash has not forwarded recent logs yet (Kafka consumer lag)\n"
            "  • Log fields (hostname / log.level) may not match index mapping\n"
            "  • Try option [8] Error/Warning Summary to see what logs exist[/dim]",
            border_style="yellow", expand=False
        ))
        return

    log_table = Table(
        box=box.MINIMAL_DOUBLE_HEAD, expand=True,
        header_style="bold yellow",
        title="[bold red]📋 System Logs — Reason for Metric Anomaly[/bold red]"
    )
    log_table.add_column("Time",         style="dim",       width=20)
    log_table.add_column("Host",         style="yellow",    width=18)
    log_table.add_column("Level",        width=9,            justify="center")
    log_table.add_column("Anomaly Link", style="bold cyan", width=14)
    log_table.add_column("Message",      ratio=1)

    for h in hits:
        src       = h.get("_source", {})
        ts        = src.get("@timestamp", "—")[:19].replace("T", " ")
        host      = src.get("hostname", src.get("instance", "—"))
        lvl       = src.get("log", {}).get("level", "info").lower()
        msg       = src.get("message", "—")
        msg_lower = msg.lower()
        tag = next(
            (t for t, kws in KEYWORD_TAGS.items()
             if any(kw in msg_lower for kw in kws)),
            "—"
        )
        log_table.add_row(ts, host, _lvl_colored(lvl), tag, msg[:220])

    console.print(log_table)
    console.print(
        f"[dim]  Showing {len(hits)} log entries linked to detected anomalies.[/dim]\n"
    )


# ──────────────── Display Functions ────────────────────────────

# ── 1. Cluster Health ───────────────────────────────────────────
def display_cluster_health():
    console.rule("[bold cyan]🏥 OpenSearch Cluster Health[/bold cyan]")
    h = os_cluster_health()
    if not h:
        return

    status = h.get("status", "unknown")
    color  = {"green": "green", "yellow": "yellow", "red": "red"}.get(status, "white")

    table = Table(box=box.ROUNDED, header_style="bold magenta")
    table.add_column("Metric", style="cyan",  width=38)
    table.add_column("Value",  style="white", width=22, justify="right")
    table.add_column("Status", width=12,       justify="center")

    def ok_zero(val):
        return "🟢 OK" if val == 0 else "🔴 HIGH"

    table.add_row("Cluster Name",   h.get("cluster_name", "—"), "")
    table.add_row("Status",         f"[{color}]{status.upper()}[/{color}]", "")
    table.add_row("Nodes",          str(h.get("number_of_nodes", 0)), "")
    table.add_row("Data Nodes",     str(h.get("number_of_data_nodes", 0)), "")
    table.add_row("Active Primary Shards", str(h.get("active_primary_shards", 0)), "")
    table.add_row("Active Shards",  str(h.get("active_shards", 0)), "")
    table.add_row("Relocating Shards",
                  str(h.get("relocating_shards", 0)),
                  ok_zero(h.get("relocating_shards", 0)))
    table.add_row("Initializing Shards",
                  str(h.get("initializing_shards", 0)),
                  ok_zero(h.get("initializing_shards", 0)))
    table.add_row("Unassigned Shards",
                  str(h.get("unassigned_shards", 0)),
                  ok_zero(h.get("unassigned_shards", 0)))
    table.add_row("Pending Tasks",
                  str(h.get("number_of_pending_tasks", 0)),
                  ok_zero(h.get("number_of_pending_tasks", 0)))
    table.add_row("In-Flight Fetch",
                  str(h.get("number_of_in_flight_fetch", 0)),
                  ok_zero(h.get("number_of_in_flight_fetch", 0)))
    table.add_row("Active Shards %",
                  f"{h.get('active_shards_percent_as_number', 0):.1f}%", "")

    console.print(table)


# ── 2. Node Stats + Log Correlation ────────────────────────────
def display_nodes_stats():
    console.rule("[bold cyan]🖥️  Node Statistics[/bold cyan]")
    data  = os_nodes_stats()
    nodes = data.get("nodes", {})
    if not nodes:
        console.print("[yellow]No node data.[/yellow]")
        return

    for node_id, n in nodes.items():
        name = n.get("name", node_id)
        os_  = n.get("os",      {})
        jvm  = n.get("jvm",     {})
        fs   = n.get("fs",      {}).get("total", {})
        proc = n.get("process", {})
        tp   = n.get("thread_pool", {})

        # ── CPU
        cpu_pct = os_.get("cpu", {}).get("percent", 0)
        cpu_col = "green" if cpu_pct < 70 else ("yellow" if cpu_pct < 90 else "red")

        # ── JVM Heap
        heap_used = jvm.get("mem", {}).get("heap_used_in_bytes", 0)
        heap_max  = jvm.get("mem", {}).get("heap_max_in_bytes",  1)
        heap_pct  = round(heap_used / heap_max * 100, 1) if heap_max else 0
        heap_col  = "green" if heap_pct < 70 else ("yellow" if heap_pct < 85 else "red")

        # ── GC
        gc_old_count = (jvm.get("gc", {}).get("collectors", {})
                           .get("old",   {}).get("collection_count", 0))
        gc_old_ms    = (jvm.get("gc", {}).get("collectors", {})
                           .get("old",   {}).get("collection_time_in_millis", 0))
        gc_yng_count = (jvm.get("gc", {}).get("collectors", {})
                           .get("young", {}).get("collection_count", 0))
        gc_yng_ms    = (jvm.get("gc", {}).get("collectors", {})
                           .get("young", {}).get("collection_time_in_millis", 0))

        # ── Disk
        disk_total    = fs.get("total_in_bytes", 1)
        disk_free     = fs.get("free_in_bytes",  0)
        disk_used_pct = round((disk_total - disk_free) / disk_total * 100, 1) if disk_total else 0
        disk_col      = "green" if disk_used_pct < 75 else ("yellow" if disk_used_pct < 85 else "red")

        # ── Thread Pool
        write_rej  = tp.get("write",  {}).get("rejected", 0)
        search_rej = tp.get("search", {}).get("rejected", 0)
        bulk_rej   = tp.get("bulk",   {}).get("rejected", 0)

        # ── File Descriptors
        open_fds = proc.get("open_file_descriptors", 0)
        max_fds  = proc.get("max_file_descriptors",  1)
        fd_pct   = round(open_fds / max_fds * 100, 1) if max_fds else 0

        # ── Metrics table
        table = Table(box=box.SIMPLE, header_style="bold blue", expand=True)
        table.add_column("Metric", style="cyan",  width=30)
        table.add_column("Value",  width=38,       justify="right")
        table.add_column("Status", width=12,        justify="center")

        table.add_row(
            "CPU Usage",
            f"[{cpu_col}]{cpu_pct}%[/{cpu_col}]",
            "🟢 OK" if cpu_pct < 70 else ("🟡 WARN" if cpu_pct < 90 else "🔴 HIGH")
        )
        table.add_row(
            "JVM Heap Used",
            f"[{heap_col}]{heap_pct}%[/{heap_col}]  "
            f"({heap_used // 1024 // 1024} MB / {heap_max // 1024 // 1024} MB)",
            "🟢 OK" if heap_pct < 70 else ("🟡 WARN" if heap_pct < 85 else "🔴 HIGH")
        )
        table.add_row(
            "GC Old Gen",
            f"{gc_old_count} collections  /  {gc_old_ms} ms",
            "🔴 HIGH" if gc_old_ms > 5000 else ("🟡 WARN" if gc_old_ms > 1000 else "🟢 OK")
        )
        table.add_row(
            "GC Young Gen",
            f"{gc_yng_count} collections  /  {gc_yng_ms} ms",
            ""
        )
        table.add_row(
            "Disk Used",
            f"[{disk_col}]{disk_used_pct}%[/{disk_col}]  "
            f"({(disk_total - disk_free) // 1024 // 1024 // 1024} GB"
            f" / {disk_total // 1024 // 1024 // 1024} GB)",
            "🟢 OK" if disk_used_pct < 75 else ("🟡 WARN" if disk_used_pct < 85 else "🔴 HIGH")
        )
        table.add_row(
            "File Descriptors",
            f"{open_fds} / {max_fds}  ({fd_pct}%)",
            ""
        )
        table.add_row(
            "Thread Pool Write Rejected",  str(write_rej),
            "🔴 HIGH" if write_rej  > 0 else "🟢 OK"
        )
        table.add_row(
            "Thread Pool Search Rejected", str(search_rej),
            "🔴 HIGH" if search_rej > 0 else "🟢 OK"
        )
        table.add_row(
            "Thread Pool Bulk Rejected",   str(bulk_rej),
            "🔴 HIGH" if bulk_rej   > 0 else "🟢 OK"
        )

        console.print(Panel(
            table,
            title=f"[bold yellow]Node: {name}[/bold yellow]",
            border_style="blue"
        ))

        # ── Detect anomalies (early thresholds)
        reasons = []
        if cpu_pct >= 70:
            reasons.append(
                f"cpu usage at {cpu_pct}% — possible heavy aggregations, "
                "merges, or bulk indexing"
            )
        if heap_pct >= 70:
            reasons.append(
                f"heap at {heap_pct}% — JVM pressure; "
                "risk of GC storms or OOM at >85%"
            )
        if gc_old_ms > 1000:
            reasons.append(
                f"gc old-gen took {gc_old_ms} ms — "
                "heap pressure causing stop-the-world pauses"
            )
        if disk_used_pct >= 75:
            reasons.append(
                f"disk usage at {disk_used_pct}% — approaching high watermark (85%); "
                "check large indices or unmerged segments"
            )
        if write_rej > 0 or bulk_rej > 0:
            reasons.append(
                f"write/bulk thread pool rejections ({write_rej + bulk_rej}) — "
                "indexing backlog; reduce throughput or increase queue size"
            )
        if search_rej > 0:
            reasons.append(
                f"search thread pool rejections ({search_rej}) — "
                "too many concurrent queries; optimize or add replicas"
            )

        # ── Correlate logs or show baseline
        if reasons:
            _correlate_node_logs_now(name, reasons)
        else:
            console.print(
                "[yellow]  ℹ No hard thresholds breached — "
                "showing recent error/warn logs as baseline:[/yellow]"
            )
            hits = os_search_logs("*", minutes=30, size=10, level="error")
            if hits:
                base_table = Table(box=box.SIMPLE, header_style="bold blue", expand=True)
                base_table.add_column("Time",    style="dim",    width=20)
                base_table.add_column("Host",    style="yellow", width=18)
                base_table.add_column("Level",   width=9,         justify="center")
                base_table.add_column("Message", ratio=1)
                for h in hits:
                    src  = h.get("_source", {})
                    ts   = src.get("@timestamp", "—")[:19].replace("T", " ")
                    host = src.get("hostname", src.get("instance", "—"))
                    lvl  = src.get("log", {}).get("level", "info").lower()
                    msg  = src.get("message", "—")[:200]
                    base_table.add_row(ts, host, _lvl_colored(lvl), msg)
                console.print(base_table)
            else:
                console.print(
                    "[green]  ✅ No errors in last 30 minutes either.[/green]"
                )
            console.print()


# ── 3. Index Stats ──────────────────────────────────────────────
def display_indices_stats():
    console.rule("[bold cyan]📂 Index Statistics[/bold cyan]")
    data    = os_indices_stats()
    indices = data.get("indices", {})
    if not indices:
        console.print("[yellow]No index data.[/yellow]")
        return

    table = Table(box=box.ROUNDED, header_style="bold magenta")
    table.add_column("Index",           style="cyan",   width=38)
    table.add_column("Docs",            style="green",  width=14, justify="right")
    table.add_column("Size (MB)",       style="yellow", width=12, justify="right")
    table.add_column("Index Ops",       width=12,        justify="right")
    table.add_column("Search Ops",      width=12,        justify="right")
    table.add_column("Avg Search Lat.", width=16,        justify="right")

    for idx_name, idx in sorted(indices.items()):
        total      = idx.get("total", {})
        docs       = total.get("docs",     {}).get("count", 0)
        size       = total.get("store",    {}).get("size_in_bytes", 0) // 1024 // 1024
        idx_total  = total.get("indexing", {}).get("index_total", 0)
        srch_total = total.get("search",   {}).get("query_total", 0)
        srch_time  = total.get("search",   {}).get("query_time_in_millis", 0)
        avg_lat    = f"{srch_time / srch_total:.1f} ms" if srch_total else "—"
        lat_color  = "green"
        if srch_total:
            lat_ms    = srch_time / srch_total
            lat_color = "green" if lat_ms < 100 else ("yellow" if lat_ms < 500 else "red")

        table.add_row(
            idx_name, f"{docs:,}", f"{size:,}",
            str(idx_total), str(srch_total),
            f"[{lat_color}]{avg_lat}[/{lat_color}]"
        )

    console.print(table)


# ── 4. Log Rate Spike + Root Cause ─────────────────────────────
def display_log_rate_spike_analysis(minutes=60):
    console.rule(
        f"[bold cyan]📈 Log Rate Analysis + Root Cause — Last {minutes}m[/bold cyan]"
    )
    buckets = os_log_rate_over_time(minutes=minutes, interval="5m")
    if not buckets:
        console.print("[yellow]No log data.[/yellow]")
        return

    counts = [b["doc_count"] for b in buckets]
    avg    = sum(counts) / len(counts) if counts else 0
    mn, mx = min(counts), max(counts)

    spark_chars = " ▁▂▃▄▅▆▇█"
    spark = "".join(
        spark_chars[int((v - mn) / (mx - mn + 1e-9) * 8)] if mx != mn else "▁"
        for v in counts
    )

    table = Table(box=box.SIMPLE, header_style="bold blue", expand=True)
    table.add_column("Window",   style="dim",    width=20)
    table.add_column("Total",    style="green",  width=10, justify="right")
    table.add_column("Errors",   style="red",    width=9,  justify="right")
    table.add_column("Warnings", style="yellow", width=10, justify="right")
    table.add_column("Bar",      width=30)
    table.add_column("⚠ Spike?", width=12,        justify="center")

    spike_windows = []

    for b in buckets:
        ts_label = b["key_as_string"][:16].replace("T", " ")
        cnt      = b["doc_count"]
        by_level = {
            lv["key"]: lv["doc_count"]
            for lv in b.get("by_level", {}).get("buckets", [])
        }
        errors   = by_level.get("error", 0) + by_level.get("critical", 0)
        warns    = by_level.get("warn",  0) + by_level.get("warning",  0)
        bar_len  = int((cnt - mn) / (mx - mn + 1e-9) * 28) if mx != mn else 1
        bar      = "█" * bar_len
        color    = "green" if cnt < avg * 1.5 else ("yellow" if cnt < avg * 2 else "red")
        is_spike = cnt > avg * 1.5
        if is_spike:
            spike_windows.append(b["key_as_string"])
        table.add_row(
            ts_label, str(cnt), str(errors), str(warns),
            f"[{color}]{bar}[/{color}]",
            "[bold red]▲ SPIKE[/bold red]" if is_spike else ""
        )

    console.print(Panel(
        table,
        title=f"[dim]avg={avg:.1f}  min={mn}  max={mx}[/dim]",
        border_style="blue"
    ))
    console.print(f"  Sparkline: [bold cyan]{spark}[/bold cyan]\n")

    if spike_windows:
        console.print(Panel(
            f"[bold red]{len(spike_windows)} log-rate spike window(s) detected — "
            f"fetching root-cause logs...[/bold red]",
            border_style="red", expand=False
        ))

        for spike_ts in spike_windows[:3]:
            console.print(f"\n[bold yellow]▶ Spike at: {spike_ts}[/bold yellow]")
            entries = os_spike_root_cause(spike_ts, window_min=5)

            if not entries:
                console.print("  [dim]No correlated logs found.[/dim]")
                continue

            rc_entries  = [e for e in entries if e["reason"]]
            all_entries = rc_entries + [e for e in entries if not e["reason"]]

            log_table = Table(
                box=box.MINIMAL_DOUBLE_HEAD, expand=True,
                header_style="bold yellow",
                title="[bold red]📋 Root Cause Analysis[/bold red]"
            )
            log_table.add_column("Time",            style="dim",       width=20)
            log_table.add_column("Host",            style="yellow",    width=18)
            log_table.add_column("Level",           width=9,            justify="center")
            log_table.add_column("Root Cause Hint", style="bold cyan", width=36)
            log_table.add_column("Message",         ratio=1)

            for e in all_entries[:25]:
                log_table.add_row(
                    e["ts"], e["host"],
                    _lvl_colored(e["level"]),
                    e["reason"] or "[dim]—[/dim]",
                    e["msg"]
                )
            console.print(log_table)

            unique_reasons = list({e["reason"] for e in rc_entries if e["reason"]})
            if unique_reasons:
                console.print(Panel(
                    "\n".join(f"  • {r}" for r in unique_reasons),
                    title="[bold red]⚡ Most Likely Causes for This Spike[/bold red]",
                    border_style="red", expand=False
                ))
    else:
        console.print("[green]✅ No abnormal log-rate spikes detected.[/green]")


# ── 5. Problem Shards ───────────────────────────────────────────
def display_problem_shards():
    console.rule("[bold cyan]🧩 Problem Shards (Unassigned / Initializing)[/bold cyan]")
    shards  = os_cat_shards()
    problem = [s for s in shards if s.get("state") not in ("STARTED", "RELOCATING")]
    if not problem:
        console.print("[green]✅ All shards are STARTED.[/green]")
        return

    table = Table(box=box.ROUNDED, header_style="bold red")
    table.add_column("Index",             style="cyan",  width=38)
    table.add_column("Shard",             style="yellow",width=7,  justify="center")
    table.add_column("P/R",               width=5,        justify="center")
    table.add_column("State",             style="red",   width=15)
    table.add_column("Unassigned Reason", style="white", ratio=1)

    for s in problem:
        table.add_row(
            s.get("index",            "—"),
            s.get("shard",            "—"),
            s.get("prirep",           "—"),
            s.get("state",            "—"),
            s.get("unassigned.reason","—")
        )
    console.print(table)


# ── 6. Pending Tasks ────────────────────────────────────────────
def display_pending_tasks():
    console.rule("[bold cyan]⏳ Pending Cluster Tasks[/bold cyan]")
    tasks = os_pending_tasks()
    if not tasks:
        console.print("[green]✅ No pending cluster tasks.[/green]")
        return

    table = Table(box=box.ROUNDED, header_style="bold blue")
    table.add_column("Priority",      style="yellow", width=12)
    table.add_column("Time In Queue", width=20)
    table.add_column("Source",        style="cyan",  ratio=1)

    for t in tasks:
        table.add_row(
            t.get("priority", "—"),
            str(t.get("timeInQueueMillis", "—")),
            t.get("source", "—")
        )
    console.print(table)


# ── 7. Raw Log Viewer ───────────────────────────────────────────
def display_logs(query_str="*", minutes=30, size=20, level=None):
    console.rule(f"[bold cyan]📋 Logs — '{query_str}' | Last {minutes}m[/bold cyan]")
    hits = os_search_logs(query_str, minutes, size, level)
    if not hits:
        console.print("[yellow]No logs found.[/yellow]")
        return

    table = Table(box=box.MINIMAL_DOUBLE_HEAD, header_style="bold magenta", expand=True)
    table.add_column("Timestamp", style="dim",    width=20)
    table.add_column("Host",      style="yellow", width=18)
    table.add_column("Level",     width=9,         justify="center")
    table.add_column("Message",   ratio=1)

    for h in hits:
        src  = h.get("_source", {})
        ts   = src.get("@timestamp", "—")[:19].replace("T", " ")
        host = src.get("hostname", src.get("instance", "—"))
        lvl  = src.get("log", {}).get("level", "info").lower()
        msg  = src.get("message", "—")[:200]
        table.add_row(ts, host, _lvl_colored(lvl), msg)

    console.print(table)
    console.print(f"[dim]Showing {len(hits)} results[/dim]")


# ── 8. Error Summary ────────────────────────────────────────────
def display_error_summary(minutes=60):
    console.rule(f"[bold cyan]❌ Error/Warning Summary — Last {minutes}m[/bold cyan]")
    buckets = os_error_summary(minutes)
    if not buckets:
        console.print("[green]No errors or warnings found.[/green]")
        return

    table = Table(box=box.ROUNDED, header_style="bold red")
    table.add_column("Host",      style="yellow", width=25)
    table.add_column("Total",     style="red",    width=8, justify="right")
    table.add_column("Breakdown", style="white",  ratio=1)

    for b in buckets:
        breakdown = "  ".join(
            f"[{'red' if l['key'] == 'error' else 'yellow'}]{l['key']}[/]: {l['doc_count']}"
            for l in b["by_level"]["buckets"]
        )
        table.add_row(b["key"], str(b["doc_count"]), breakdown)
    console.print(table)


# ── 9. Custom Log Search ────────────────────────────────────────
def display_custom_log_search():
    console.rule("[bold cyan]🔍 Custom Log Search[/bold cyan]")
    q       = Prompt.ask("[cyan]Search query (Lucene syntax)[/cyan]", default="*")
    minutes = IntPrompt.ask("Minutes back", default=30)
    level   = Prompt.ask(
        "Filter level (blank = all)", default="", show_default=False
    ) or None
    size    = IntPrompt.ask("Max results", default=20)
    display_logs(q, minutes, size, level)


# ── 10. Live Dashboard ──────────────────────────────────────────
def live_dashboard(refresh=10):
    console.rule(
        f"[bold green]⚡ Live Dashboard "
        f"(refresh={refresh}s — Ctrl+C to stop)[/bold green]"
    )
    try:
        while True:
            console.clear()
            display_cluster_health()
            display_nodes_stats()
            display_error_summary(30)
            console.print(
                f"\n[dim]Last updated: "
                f"{datetime.datetime.now().strftime('%H:%M:%S')} "
                f"— refreshing in {refresh}s[/dim]"
            )
            time.sleep(refresh)
    except KeyboardInterrupt:
        console.print("\n[yellow]Live dashboard stopped.[/yellow]")


# ──────────────── Menu ──────────────────────────────────────────

MENU_ITEMS = {
    "1":  ("🏥 Cluster Health Snapshot",              display_cluster_health),
    "2":  ("🖥️  Node Stats + Log Correlation",         display_nodes_stats),
    "3":  ("📂 Index Statistics",                      display_indices_stats),
    "4":  ("📈 Log Rate Spike + Root Cause Analysis",
           lambda: display_log_rate_spike_analysis(
               IntPrompt.ask("Minutes back", default=60))),
    "5":  ("🧩 Problem Shards",                        display_problem_shards),
    "6":  ("⏳ Pending Cluster Tasks",                 display_pending_tasks),
    "7":  ("📋 Recent Logs (all)",
           lambda: display_logs("*", 30, 25)),
    "8":  ("❌ Error/Warning Summary (last 60m)",
           lambda: display_error_summary(60)),
    "9":  ("🔍 Custom Log Search",                     display_custom_log_search),
    "10": ("⚡ Live Auto-Refresh Dashboard",
           lambda: live_dashboard(10)),
    "0":  ("🚪 Exit",                                  None),
}


def print_menu():
    console.print(Panel.fit(
        "[bold cyan]OpenSearch Observability Terminal[/bold cyan]\n"
        "[dim]Cluster metrics • Index stats • Log spike root-cause analysis[/dim]",
        border_style="cyan"
    ))
    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    table.add_column("Key",    style="bold yellow", width=5)
    table.add_column("Action", style="white")
    for key, (label, _) in MENU_ITEMS.items():
        table.add_row(f"[{key}]", label)
    console.print(table)


def main():
    while True:
        print_menu()
        choice = Prompt.ask(
            "\n[bold yellow]Select option[/bold yellow]",
            choices=list(MENU_ITEMS.keys()),
            default="1"
        )
        if choice == "0":
            console.print("[bold green]Goodbye![/bold green]")
            sys.exit(0)

        _, fn = MENU_ITEMS[choice]
        console.print()
        try:
            fn()
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
        console.print()
        Prompt.ask("[dim]Press Enter to return to menu[/dim]", default="")
        console.clear()


if __name__ == "__main__":
    main()
