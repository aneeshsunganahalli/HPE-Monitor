import sys
import time
import datetime
import requests
import urllib3
from opensearchpy import OpenSearch
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich.prompt import Prompt, IntPrompt
from rich.syntax import Syntax
from rich import box
from rich.live import Live
from rich.layout import Layout
from rich.text import Text

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────── CONFIG ────────────────────────────
PROMETHEUS_URL   = "http://localhost:9090"
OPENSEARCH_HOST  = "localhost"
OPENSEARCH_PORT  = 9200
OPENSEARCH_USER  = "admin"
OPENSEARCH_PASS  = "admin"
OPENSEARCH_INDEX = "system-logs-*"
OPENSEARCH_SSL   = False
# ───────────────────────────────────────────────────────────────

console = Console()

# ──────────────── Prometheus Helpers ───────────────────────────

def prom_query(promql: str) -> list:
    try:
        r = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": promql},
            timeout=60
        )
        r.raise_for_status()
        return r.json().get("data", {}).get("result", [])
    except Exception as e:
        console.print(f"[red]Prometheus error:[/red] {e}")
        return []


def prom_range_query(promql: str, minutes: int = 30, step: str = "60s") -> list:
    end   = int(time.time())
    start = end - minutes * 60
    try:
        r = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query_range",
            params={"query": promql, "start": start, "end": end, "step": step},
            timeout=60
        )
        r.raise_for_status()
        return r.json().get("data", {}).get("result", [])
    except Exception as e:
        console.print(f"[red]Prometheus range error:[/red] {e}")
        return []


def prom_targets() -> dict:
    try:
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/targets", timeout=60)
        r.raise_for_status()
        return r.json().get("data", {})
    except Exception as e:
        console.print(f"[red]Targets error:[/red] {e}")
        return {}


def prom_alerts() -> list:
    try:
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/alerts", timeout=60)
        r.raise_for_status()
        return r.json().get("data", {}).get("alerts", [])
    except Exception as e:
        console.print(f"[red]Alerts error:[/red] {e}")
        return []
    
    # ──────────────── OpenSearch Helpers ───────────────────────────

def get_os_client() -> OpenSearch:
    return OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
        use_ssl=OPENSEARCH_SSL,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        scheme="http",
    )


def os_search_logs(query_str: str = "*", minutes: int = 30,
                   size: int = 20, level: str = None) -> list:
    client = get_os_client()
    must = [
        {"query_string": {"query": query_str}},
        {"range": {"@timestamp": {"gte": f"now-{minutes}m", "lte": "now"}}}
    ]
    if level:
        must.append({"match": {"log.level": level.lower()}})
    body = {
        "size": size,
        "sort": [{"@timestamp": {"order": "desc"}}],
        "query": {"bool": {"must": must}},
        "_source": ["@timestamp", "message", "log.level",
                    "hostname", "instance", "program"]       # ✅ flat field names
    }
    try:
        res = client.search(index=OPENSEARCH_INDEX, body=body)
        return res["hits"]["hits"]
    except Exception as e:
        console.print(f"[red]OpenSearch error:[/red] {e}")
        return []


def os_correlate_metric_spike(instance: str, spike_time_unix: float,
                               window_min: int = 5) -> list:
    client   = get_os_client()
    spike_dt = datetime.datetime.utcfromtimestamp(spike_time_unix)
    start_dt = spike_dt - datetime.timedelta(minutes=window_min)
    end_dt   = spike_dt + datetime.timedelta(minutes=window_min)
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str   = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    hostname  = instance.split(":")[0]

    # ✅ Build index list covering spike window (handles cross-midnight)
    indices = set()
    for dt in [start_dt, spike_dt, end_dt]:
        indices.add(f"system-logs-{dt.strftime('%Y.%m.%d')}")
    index_target = ",".join(indices)   # e.g. "system-logs-2026.03.06,system-logs-2026.03.07"

    body = {
        "size": 50,
        "sort": [{"@timestamp": {"order": "asc"}}],
        "query": {
            "bool": {
                "must": [
                    {"range": {"@timestamp": {"gte": start_str, "lte": end_str}}}
                ],
                "should": [
                    {"match": {"instance": instance}},
                    {"match": {"hostname": hostname}}
                ],
                "minimum_should_match": 1
            }
        },
        "_source": ["@timestamp", "message", "log.level",
                    "hostname", "instance", "program"]
    }
    try:
        res = client.search(index=index_target, body=body)
        return res["hits"]["hits"]
    except Exception as e:
        console.print(f"[red]Correlation error:[/red] {e}")
        return []



def os_error_summary(minutes: int = 60) -> list:
    client = get_os_client()
    body = {
        "size": 0,
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
                "terms": {"field": "hostname.keyword", "size": 10},  # ✅ flat hostname
                "aggs": {
                    "by_level": {"terms": {"field": "log.level.keyword", "size": 5}}
                }
            }
        }
    }
    try:
        res = client.search(index=OPENSEARCH_INDEX, body=body)
        return res.get("aggregations", {}).get("by_host", {}).get("buckets", [])
    except Exception as e:
        console.print(f"[red]Error summary error:[/red] {e}")
        return []
    
    # ──────────────── Display Functions ────────────────────────────

def display_system_snapshot():
    console.rule("[bold cyan]📊 System Metrics Snapshot[/bold cyan]")

    metrics = {
        "CPU Usage %":    ('100 - (avg by(instance)(rate(node_cpu_seconds_total{mode="idle"}[2m]))*100)', "%"),
        "Memory Used %":  ('100 * (1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)', "%"),
        "Disk Used %":    ('100 - (node_filesystem_avail_bytes{mountpoint="/"} / node_filesystem_size_bytes{mountpoint="/"} * 100)', "%"),
        "Load 1m":        ("node_load1", ""),
        "Load 5m":        ("node_load5", ""),
        "Net RX MB/s":    ("rate(node_network_receive_bytes_total[2m]) / 1024 / 1024", "MB/s"),
        "Net TX MB/s":    ("rate(node_network_transmit_bytes_total[2m]) / 1024 / 1024", "MB/s"),
        "Disk Read MB/s": ("rate(node_disk_read_bytes_total[2m]) / 1024 / 1024", "MB/s"),
        "Disk Write MB/s":("rate(node_disk_written_bytes_total[2m]) / 1024 / 1024", "MB/s"),
    }

    table = Table(box=box.ROUNDED, show_header=True, header_style="bold magenta")
    table.add_column("Metric",    style="cyan",   width=22)
    table.add_column("Instance",  style="yellow", width=25)
    table.add_column("Value",     style="green",  width=14, justify="right")
    table.add_column("Status",    width=10,        justify="center")

    for label, (q, unit) in metrics.items():
        results = prom_query(q)
        for r in results:
            instance = r["metric"].get("instance", r["metric"].get("device", "—"))
            val = float(r["value"][1])
            val_str = f"{val:.2f} {unit}".strip()
            if "%" in unit:
                status = "🟢 OK" if val < 70 else ("🟡 WARN" if val < 90 else "🔴 HIGH")
            elif label.startswith("Load"):
                status = "🟢 OK" if val < 2.0 else ("🟡 WARN" if val < 4.0 else "🔴 HIGH")
            else:
                status = "—"
            table.add_row(label, instance, val_str, status)

    console.print(table)

def display_metric_range(promql: str, label: str, minutes: int = 30):
    console.rule(f"[bold cyan]📈 {label} — Last {minutes} Minutes[/bold cyan]")
    results = prom_range_query(promql, minutes=minutes)
    if not results:
        console.print("[yellow]No data returned.[/yellow]")
        return

    for series in results:
        instance = series["metric"].get("instance", "unknown")
        values   = [float(v[1]) for v in series["values"]]
        times    = [v[0] for v in series["values"]]
        labels   = [datetime.datetime.utcfromtimestamp(v[0]).strftime("%H:%M") for v in series["values"]]

        if not values:
            continue

        mn, mx  = min(values), max(values)
        avg_val = sum(values) / len(values)
        spark_chars = " ▁▂▃▄▅▆▇█"
        spark = "".join(
            spark_chars[int((v - mn) / (mx - mn + 1e-9) * 8)] if mx != mn else " "
            for v in values
        )

        spike_times = {}
        for i, (ts_unix, val) in enumerate(zip(times, values)):
            if val > avg_val * 1.10 or val > 85.0:
                spike_times[i] = (ts_unix, val)

        table = Table(box=box.SIMPLE, header_style="bold blue", expand=True)
        table.add_column("Time",     style="dim",   width=8)
        table.add_column("Value",    style="green", width=10, justify="right")
        table.add_column("Bar",      width=30)
        table.add_column("⚠ Spike?", width=10,     justify="center")

        for i, (t, v) in enumerate(zip(labels, values)):
            bar_len = int((v - mn) / (mx - mn + 1e-9) * 28) if mx != mn else 1
            bar     = "█" * bar_len
            color   = "green" if v < (mx * 0.7) else ("yellow" if v < (mx * 0.85) else "red")
            spike_marker = "[bold red]▲ SPIKE[/bold red]" if i in spike_times else ""
            table.add_row(t, f"{v:.3f}", f"[{color}]{bar}[/{color}]", spike_marker)

        console.print(Panel(
            table,
            title=f"[bold yellow]{instance}[/bold yellow]   "
                  f"[dim]min={mn:.2f}  max={mx:.2f}  avg={avg_val:.2f}[/dim]",
            title_align="left",
            border_style="blue"
        ))
        console.print(f"  Sparkline: [bold cyan]{spark}[/bold cyan]\n")

        if spike_times:
            console.print(Panel(
                f"[bold red]⚠  {len(spike_times)} spike(s) detected above avg {avg_val:.2f}%  "
                f"— fetching correlated logs from OpenSearch...[/bold red]",
                border_style="red", expand=False
            ))

            spike_indices   = sorted(spike_times.keys())
            first_spike_ts  = spike_times[spike_indices[0]][0]
            first_spike_val = spike_times[spike_indices[-1]][1]
            spike_start_str = datetime.datetime.utcfromtimestamp(first_spike_ts).strftime("%H:%M:%S UTC")

            console.print(f"\n  [bold yellow]Last spike:[/bold yellow] "
                          f"[red]{first_spike_val:.2f}%[/red] at [cyan]{spike_start_str}[/cyan]  "
                          f"(window: full spike duration)\n")

            last_spike_ts = spike_times[spike_indices[-1]][0]
            duration_min = int((last_spike_ts - first_spike_ts) / 60) + 5
            logs = os_correlate_metric_spike(instance, first_spike_ts, window_min=duration_min)

            if logs:
                log_table = Table(
                    box=box.MINIMAL_DOUBLE_HEAD, expand=True,
                    header_style="bold yellow",
                    title="[bold red]📋 Correlated System Logs (reason for spike)[/bold red]",
                    title_style="bold red"
                )
                log_table.add_column("Time",    style="dim",    width=20)
                log_table.add_column("Host",    style="yellow", width=18)
                log_table.add_column("Level",   width=9,        justify="center")
                log_table.add_column("Message", ratio=1)

                level_colors = {
                    "error":    "red",
                    "critical": "bold red",
                    "warn":     "yellow",
                    "warning":  "yellow",
                    "info":     "cyan",
                    "debug":    "dim"
                }

                seen_msgs = set()
                unique_logs = []
                for hit in logs:
                    _key = hit.get("_source", {}).get("message", "")[:120]
                    if _key not in seen_msgs:
                        seen_msgs.add(_key)
                        unique_logs.append(hit)
                logs = unique_logs
                for hit in logs:
                    src  = hit.get("_source", {})
                    ts   = src.get("@timestamp", "—")[:19].replace("T", " ")
                    host = src.get("hostname", src.get("instance", "—"))  # ✅ flat field
                    lvl  = src.get("log", {}).get("level", "info").lower()
                    msg  = src.get("message", "—")[:220]
                    col  = level_colors.get(lvl, "white")
                    log_table.add_row(ts, host,
                                      f"[{col}]{lvl.upper()}[/{col}]", msg)

                console.print(log_table)
                console.print(f"\n  [dim]Showing {len(logs)} log entries "
                               f"correlated around spike time (deduplicated).[/dim]")
            else:
                console.print(Panel(
                    "[yellow]No correlated logs found in OpenSearch for this spike window.\n\n"
                     "[dim]Possible reasons:\n"
                    "  • Logstash has not ingested logs for this time window yet\n"
                    "  • The 'instance' field in logs does not match Prometheus instance label\n"
                    "  • OpenSearch index pattern does not match (check OPENSEARCH_INDEX config)\n"
                    "  • Kafka consumer is lagging — logs not yet forwarded to OpenSearch[/dim]",
                    border_style="yellow", expand=False
                ))
        else:
            console.print(
                f"[green]✅ No significant spikes detected "
                f"(all values within 10% of avg {avg_val:.2f}%)[/green]\n"
            )


def display_logs(query_str: str = "*", minutes: int = 30,
                 size: int = 20, level: str = None):
    console.rule(f"[bold cyan]📋 Logs — '{query_str}' | Last {minutes}m[/bold cyan]")
    hits = os_search_logs(query_str, minutes, size, level)
    if not hits:
        console.print("[yellow]No logs found.[/yellow]")
        return

    table = Table(box=box.MINIMAL_DOUBLE_HEAD, show_header=True,
                  header_style="bold magenta", expand=True)
    table.add_column("Timestamp", style="dim",    width=20)
    table.add_column("Host",      style="yellow", width=18)
    table.add_column("Level",     width=8,         justify="center")
    table.add_column("Message",   style="white",   ratio=1)

    level_colors = {"error": "red", "critical": "bold red",
                    "warn": "yellow", "warning": "yellow",
                    "info": "cyan", "debug": "dim"}

    for h in hits:
        src   = h.get("_source", {})
        ts    = src.get("@timestamp", "—")[:19].replace("T", " ")
        host  = src.get("hostname", src.get("instance", "—"))  # ✅ flat field
        lvl   = src.get("log", {}).get("level", "info").lower()
        msg   = src.get("message", "—")[:200]
        color = level_colors.get(lvl, "white")
        table.add_row(ts, host, f"[{color}]{lvl.upper()}[/{color}]", msg)

    console.print(table)
    console.print(f"[dim]Showing {len(hits)} results[/dim]")


def display_correlated_spike():
    console.rule("[bold red]🔗 Metric Spike ↔ Log Correlation[/bold red]")

    results = prom_range_query(
        '100 - (avg by(instance)(rate(node_cpu_seconds_total{mode="idle"}[2m]))*100)',
        minutes=30, step="60s"
    )

    spike_found = False
    for series in results:
        instance = series["metric"].get("instance", "unknown")
        for ts_val in series["values"]:
            ts_unix = float(ts_val[0])
            val     = float(ts_val[1])
            if val > 80.0:
                spike_found = True
                spike_time  = datetime.datetime.utcfromtimestamp(ts_unix).strftime("%Y-%m-%d %H:%M:%S UTC")
                console.print(Panel(
                    f"[red]CPU spike detected![/red]  "
                    f"[yellow]{instance}[/yellow]  →  "
                    f"[bold red]{val:.1f}%[/bold red]  at  [cyan]{spike_time}[/cyan]",
                    border_style="red"
                ))
                logs = os_correlate_metric_spike(instance, ts_unix, window_min=5)
                if logs:
                    table = Table(box=box.SIMPLE, expand=True,
                                  header_style="bold yellow")
                    table.add_column("Time",    style="dim", width=20)
                    table.add_column("Level",   width=8,     justify="center")
                    table.add_column("Message", ratio=1)
                    for l in logs:
                        s   = l.get("_source", {})
                        lvl = s.get("log", {}).get("level", "info").lower()
                        col = "red" if lvl in ("error", "critical") else "yellow" if "warn" in lvl else "cyan"
                        table.add_row(
                            s.get("@timestamp", "—")[:19].replace("T", " "),
                            f"[{col}]{lvl.upper()}[/{col}]",
                            s.get("message", "—")[:200]
                        )
                    console.print(table)
                else:
                    console.print("[dim]  No correlated logs found in ±5 min window.[/dim]")
                break

    if not spike_found:
        console.print("[green]✅ No CPU spikes (>80%) detected in the last 30 minutes.[/green]")

def display_error_summary(minutes: int = 60):
    console.rule(f"[bold cyan]   Error Summary — Last {minutes}m[/bold cyan]")
    buckets = os_error_summary(minutes)
    if not buckets:
        console.print("[green]No errors or warnings found.[/green]")
        return

    table = Table(box=box.ROUNDED, header_style="bold red")
    table.add_column("Host",     style="yellow", width=25)
    table.add_column("Total",    style="red",    width=8,  justify="right")
    table.add_column("Breakdown (level: count)", style="white", ratio=1)
    for b in buckets:
        host  = b["key"]
        total = b["doc_count"]
        breakdown = "  ".join(
            f"[{'red' if l['key'] == 'error' else 'yellow'}]{l['key']}[/]: {l['doc_count']}"
            for l in b["by_level"]["buckets"]
        )
        table.add_row(host, str(total), breakdown)
    console.print(table)


def display_targets():
    console.rule("[bold cyan]🎯 Prometheus Scrape Targets[/bold cyan]")
    data   = prom_targets()
    active = data.get("activeTargets", [])
    if not active:
        console.print("[yellow]No targets found.[/yellow]")
        return

    table = Table(box=box.ROUNDED, header_style="bold blue")
    table.add_column("Job",        style="cyan",   width=20)
    table.add_column("Instance",   style="yellow", width=30)
    table.add_column("State",      width=10,        justify="center")
    table.add_column("Last Scrape",                 width=20)
    table.add_column("Error",      style="red",     ratio=1)

    for t in active:
        state = t.get("health", "unknown")
        color = "green" if state == "up" else "red"
        table.add_row(
            t["labels"].get("job", "—"),
            t["labels"].get("instance", "—"),
            f"[{color}]{state.upper()}[/{color}]",
            t.get("lastScrape", "—")[:19].replace("T", " "),
            t.get("lastError", "") or "[dim]none[/dim]"
        )
    console.print(table)


def display_alerts():
    console.rule("[bold red]🚨 Active Alerts[/bold red]")
    alerts = prom_alerts()
    if not alerts:
        console.print("[green]✅ No active alerts.[/green]")
        return

    for a in alerts:
        state   = a.get("state", "unknown")
        name    = a["labels"].get("alertname", "—")
        inst    = a["labels"].get("instance", "—")
        summary = a.get("annotations", {}).get("summary", "—")
        color   = "red" if state == "firing" else "yellow"
        console.print(Panel(
            f"[bold {color}]{name}[/bold {color}]  |  "
            f"instance: [yellow]{inst}[/yellow]\n"
            f"[dim]{summary}[/dim]",
            border_style=color, expand=False
        ))


def display_custom_promql():
    console.rule("[bold cyan]🔍 Custom PromQL Query[/bold cyan]")
    q    = Prompt.ask("[cyan]Enter PromQL expression[/cyan]")
    mode = Prompt.ask("Query type", choices=["instant", "range"], default="instant")

    if mode == "instant":
        results = prom_query(q)
        table = Table(box=box.ROUNDED)
        table.add_column("Metric Labels", style="cyan",  ratio=1)
        table.add_column("Value",         style="green", width=20, justify="right")
        for r in results:
            labels = "  ".join(f"[dim]{k}[/dim]=[yellow]{v}[/yellow]" for k, v in r["metric"].items())
            table.add_row(labels or "[dim]—[/dim]", r["value"][1])
        console.print(table if results else "[yellow]No results.[/yellow]")
    else:
        minutes = IntPrompt.ask("Minutes back", default=30)
        display_metric_range(q, label=q, minutes=minutes)


def display_custom_log_search():
    console.rule("[bold cyan]🔍 Custom Log Search[/bold cyan]")
    q       = Prompt.ask("[cyan]Search query (Lucene syntax)[/cyan]", default="*")
    minutes = IntPrompt.ask("Minutes back", default=30)
    level   = Prompt.ask("Filter level (leave blank for all)",
                          default="", show_default=False) or None
    size    = IntPrompt.ask("Max results", default=20)
    display_logs(q, minutes, size, level)


def live_dashboard(refresh: int = 10):
    console.rule(f"[bold green]⚡ Live Dashboard (refresh={refresh}s, Ctrl+C to stop)[/bold green]")
    try:
        while True:
            console.clear()
            console.rule("[bold green]⚡ Live Dashboard[/bold green]")
            display_system_snapshot()
            display_alerts()
            console.print(f"\n[dim]Last updated: {datetime.datetime.now().strftime('%H:%M:%S')} — refreshing in {refresh}s[/dim]")
            time.sleep(refresh)
    except KeyboardInterrupt:
        console.print("\n[yellow]Live dashboard stopped.[/yellow]")

# ──────────────── Main Menu ────────────────────────────────────

MENU_ITEMS = {
    "1":  ("📊 System Metrics Snapshot",        display_system_snapshot),
    "2":  ("📈 CPU Range (last 30 min)",
           lambda: display_metric_range(
               '100 - (avg by(instance)(rate(node_cpu_seconds_total{mode="idle"}[2m]))*100)',
               "CPU Usage %", 30)),
    "3":  ("💾 Memory Range (last 30 min)",
           lambda: display_metric_range(
               "100*(1-node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes)",
               "Memory Used %", 30)),
    "4":  ("🔗 Spike ↔ Log Correlation",        display_correlated_spike),
    "5":  ("📋 Recent Logs (all)",
           lambda: display_logs("*", 30, 25)),
    "6":  ("❌ Error/Warning Log Summary",
           lambda: display_error_summary(60)),
    "7":  ("🎯 Prometheus Scrape Targets",       display_targets),
    "8":  ("🚨 Active Prometheus Alerts",        display_alerts),
    "9":  ("🔍 Custom PromQL Query",             display_custom_promql),
    "10": ("🔍 Custom Log Search",               display_custom_log_search),
    "11": ("⚡ Live Auto-Refresh Dashboard",
           lambda: live_dashboard(10)),
    "0":  ("🚪 Exit",                            None),
}


def print_menu():
    console.print(Panel.fit(
        "[bold cyan]Observability Terminal — Prometheus + OpenSearch[/bold cyan]\n"
        "[dim]Node Exporter metrics correlated with Kafka/Logstash logs[/dim]",
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
        choice = Prompt.ask("\n[bold yellow]Select option[/bold yellow]",
                             choices=list(MENU_ITEMS.keys()), default="1")
        if choice == "0":
            console.print("[bold green]Goodbye![/bold green]")
            sys.exit(0)

        label, fn = MENU_ITEMS[choice]
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

                                              