import sys
import subprocess
import os
import glob
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

urllib3.disable_warnings(urllib3.exceptions.Ins

# ─────────────────────────── CONFIG ──────────
PROMETHEUS_URL   = "http://localhost:9090"
OPENSEARCH_HOST  = "localhost"
OPENSEARCH_PORT  = 9200
OPENSEARCH_USER  = "admin"
OPENSEARCH_PASS  = "admin"
OPENSEARCH_INDEX = "system-logs-*"
OPENSEARCH_SSL   = False
# ─────────────────────────────────────────────

console = Console()

# ──────────────── Prometheus Helpers ─────────

def prom_query(promql: str) -> list:
    try:
        r = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": promql},
            timeout=60
        )
        r.raise_for_status()
        return r.json().get("data", {}).get("re
    except Exception as e:
        console.print(f"[red]Prometheus error:[
        return []


def prom_range_query(promql: str, minutes: int 
    end   = int(time.time())
    start = end - minutes * 60
    try:
        r = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query_ran
            params={"query": promql, "start": s
            timeout=60
        )
        r.raise_for_status()
        return r.json().get("data", {}).get("re
    except Exception as e:
        console.print(f"[red]Prometheus range e
        return []


def prom_targets() -> dict:
    try:
        r = requests.get(f"{PROMETHEUS_URL}/api
        r.raise_for_status()
        return r.json().get("data", {})
    except Exception as e:
        console.print(f"[red]Targets error:[/re
        return {}


def prom_alerts() -> list:
    try:
        r = requests.get(f"{PROMETHEUS_URL}/api
        r.raise_for_status()
        return r.json().get("data", {}).get("al
    except Exception as e:
        console.print(f"[red]Alerts error:[/red
        return []
    
    # ──────────────── OpenSearch Helpers ─────

def get_os_client() -> OpenSearch:
    return OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port"
        http_auth=(OPENSEARCH_USER, OPENSEARCH_
        use_ssl=OPENSEARCH_SSL,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        scheme="http",
    )


def os_search_logs(query_str: str = "*", minute
                   size: int = 20, level: str =
    client = get_os_client()
    must = [
        {"query_string": {"query": query_str}},
        {"range": {"@timestamp": {"gte": f"now-
    ]
    if level:
        must.append({"match": {"log.level": lev
    body = {
        "size": size,
        "sort": [{"@timestamp": {"order": "desc
        "query": {"bool": {"must": must}},
        "_source": ["@timestamp", "message", "l
                    "hostname", "instance", "pr
    }
    try:
        res = client.search(index=OPENSEARCH_IN
        return res["hits"]["hits"]
    except Exception as e:
        console.print(f"[red]OpenSearch error:[
        return []


def os_correlate_metric_spike(instance: str, sp
                               window_min: int 
    client   = get_os_client()
    spike_dt = datetime.datetime.utcfromtimesta
    start_dt = spike_dt - datetime.timedelta(mi
    end_dt   = spike_dt + datetime.timedelta(mi
    start_str = start_dt.strftime("%Y-%m-%dT%H:
    end_str   = end_dt.strftime("%Y-%m-%dT%H:%M
    hostname  = instance.split(":")[0]

    # ✅ Build index list covering spike window 
    indices = set()
    for dt in [start_dt, spike_dt, end_dt]:
        indices.add(f"system-logs-{dt.strftime('%Y.%m.
    index_target = ",".join(indices)   # e.g. "

    body = {
        "size": 50,
        "sort": [{"@timestamp": {"order": "asc"
        "query": {
            "bool": {
                "must": [
                    {"range": {"@timestamp": {"
                ],
                "should": [
                    {"match": {"instance": inst
                    {"match": {"hostname": host
                ],
                "minimum_should_match": 1
            }
        },
        "_source": ["@timestamp", "message", "l
                    "hostname", "instance", "pr
    }
    try:
        res = client.search(index=index_target,
        return res["hits"]["hits"]
    except Exception as e:
        console.print(f"[red]Correlation error:
        return []



def os_error_summary(minutes: int = 60) -> list
    client = get_os_client()
    body = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"range": {"@timestamp": {"
                    {"terms": {"log.level": ["e
                ]
            }
        },
        "aggs": {
            "by_host": {
                "terms": {"field": "hostname.ke
                "aggs": {
                    "by_level": {"terms": {"fie
                }
            }
        }
    }
    try:
        res = client.search(index=OPENSEARCH_IN
        return res.get("aggregations", {}).get(
    except Exception as e:
        console.print(f"[red]Error summary erro
        return []
    
    # ──────────────── Display Functions ──────

def display_system_snapshot():
    console.rule("[bold cyan]📊 System Metrics 

    metrics = {
        "CPU Usage %":    ('100 - (avg by(insta
        "Memory Used %":  ('100 * (1 - node_mem
        "Disk Used %":    ('100 - (node_filesys
        "Load 1m":        ("node_load1", ""),
        "Load 5m":        ("node_load5", ""),
        "Net RX MB/s":    ("rate(node_network_r
        "Net TX MB/s":    ("rate(node_network_t
        "Disk Read MB/s": ("rate(node_disk_read
        "Disk Write MB/s":("rate(node_disk_writ
    }

    table = Table(box=box.ROUNDED, show_header=
    table.add_column("Metric",    style="cyan",
    table.add_column("Instance",  style="yellow
    table.add_column("Value",     style="green"
    table.add_column("Status",    width=10,    

    for label, (q, unit) in metrics.items():
        results = prom_query(q)
        for r in results:
            instance = r["metric"].get("instanc
            val = float(r["value"][1])
            val_str = f"{val:.2f} {unit}".strip
            if "%" in unit:
                status = "🟢 OK" if val < 70 el
            elif label.startswith("Load"):
                status = "🟢 OK" if val < 2.0 e
            else:
                status = "—"
            table.add_row(label, instance, val_

    console.print(table)

def display_metric_range(promql: str, label: st
    console.rule(f"[bold cyan]📈 {label} — Last
    results = prom_range_query(promql, minutes=
    if not results:
        console.print("[yellow]No data returned
        return

    for series in results:
        instance = series["metric"].get("instan
        values   = [float(v[1]) for v in series
        times    = [v[0] for v in series["value
        labels   = [datetime.datetime.utcfromti

        if not values:
            continue

        mn, mx  = min(values), max(values)
        avg_val = sum(values) / len(values)
        spark_chars = " ▁▂▃▄▅▆▇█"
        spark = "".join(
            spark_chars[int((v - mn) / (mx - mn
            for v in values
        )

        spike_times = {}
        for i, (ts_unix, val) in enumerate(zip(
            if val > avg_val * 1.10 or val > 85
                spike_times[i] = (ts_unix, val)

        table = Table(box=box.SIMPLE, header_st
        table.add_column("Time",     style="dim
        table.add_column("Value",    style="gre
        table.add_column("Bar",      width=30)
        table.add_column("⚠ Spike?", width=10, 

        for i, (t, v) in enumerate(zip(labels, 
            bar_len = int((v - mn) / (mx - mn +
            bar     = "█" * bar_len
            color   = "green" if v < (mx * 0.7)
            spike_marker = "[bold red]▲ SPIKE[/
            table.add_row(t, f"{v:.3f}", f"[{co

        console.print(Panel(
            table,
            title=f"[bold yellow]{instance}[/bo
                  f"[dim]min={mn:.2f}  max={mx:
            title_align="left",
            border_style="blue"
        ))
        console.print(f"  Sparkline: [bold cyan

        if spike_times:
            console.print(Panel(
                f"[bold red]⚠  {len(spike_times
                f"— fetching correlated logs fr
                border_style="red", expand=Fals
            ))

            spike_indices   = sorted(spike_time
            first_spike_ts  = spike_times[spike
            first_spike_val = spike_times[spike
            spike_start_str = datetime.datetime

            console.print(f"\n  [bold yellow]La
                          f"[red]{first_spike_v
                          f"(window: full spike

            last_spike_ts = spike_times[spike_i
            duration_min = int((last_spike_ts -
            logs = os_correlate_metric_spike(in

            if logs:
                log_table = Table(
                    box=box.MINIMAL_DOUBLE_HEAD
                    header_style="bold yellow",
                    title="[bold red]📋 Correla
                    title_style="bold red"
                )
                log_table.add_column("Time",   
                log_table.add_column("Host",   
                log_table.add_column("Level",  
                log_table.add_column("Message",

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
                    _key = hit.get("_source", {
                    if _key not in seen_msgs:
                        seen_msgs.add(_key)
                        unique_logs.append(hit)
                logs = unique_logs
                for hit in logs:
                    src  = hit.get("_source", {
                    ts   = src.get("@timestamp"
                    host = src.get("hostname", 
                    lvl  = src.get("log", {}).g
                    msg  = src.get("message", "
                    col  = level_colors.get(lvl
                    log_table.add_row(ts, host,
                                      f"[{col}]

                console.print(log_table)
                console.print(f"\n  [dim]Showin
                               f"correlated aro
            else:
                console.print(Panel(
                    "[yellow]No correlated logs
                     "[dim]Possible reasons:\n"
                    "  • Logstash has not inges
                    "  • The 'instance' field i
                    "  • OpenSearch index patte
                    "  • Kafka consumer is lagg
                    border_style="yellow", expa
                ))
        else:
            console.print(
                f"[green]✅ No significant spike
                f"(all values within 10% of avg
            )


def display_logs(query_str: str = "*", minutes:
                 size: int = 20, level: str = N
    console.rule(f"[bold cyan]📋 Logs — '{query
    hits = os_search_logs(query_str, minutes, s
    if not hits:
        console.print("[yellow]No logs found.[/
        return

    table = Table(box=box.MINIMAL_DOUBLE_HEAD, 
                  header_style="bold magenta", 
    table.add_column("Timestamp", style="dim", 
    table.add_column("Host",      style="yellow
    table.add_column("Level",     width=8,     
    table.add_column("Message",   style="white"

    level_colors = {"error": "red", "critical":
                    "warn": "yellow", "warning"
                    "info": "cyan", "debug": "d

    for h in hits:
        src   = h.get("_source", {})
        ts    = src.get("@timestamp", "—")[:19]
        host  = src.get("hostname", src.get("in
        lvl   = src.get("log", {}).get("level",
        msg   = src.get("message", "—")[:200]
        color = level_colors.get(lvl, "white")
        table.add_row(ts, host, f"[{color}]{lvl

    console.print(table)
    console.print(f"[dim]Showing {len(hits)} re


def display_correlated_spike():
    console.rule("[bold red]🔗 Metric Spike ↔ L

    results = prom_range_query(
        '100 - (avg by(instance)(rate(node_cpu_
        minutes=30, step="60s"
    )

    spike_found = False
    for series in results:
        instance = series["metric"].get("instan
        for ts_val in series["values"]:
            ts_unix = float(ts_val[0])
            val     = float(ts_val[1])
            if val > 80.0:
                spike_found = True
                spike_time  = datetime.datetime
                console.print(Panel(
                    f"[red]CPU spike detected![
                    f"[yellow]{instance}[/yello
                    f"[bold red]{val:.1f}%[/bol
                    border_style="red"
                ))
                logs = os_correlate_metric_spik
                if logs:
                    table = Table(box=box.SIMPL
                                  header_style=
                    table.add_column("Time",   
                    table.add_column("Level",  
                    table.add_column("Message",
                    for l in logs:
                        s   = l.get("_source", 
                        lvl = s.get("log", {}).
                        col = "red" if lvl in (
                        table.add_row(
                            s.get("@timestamp",
                            f"[{col}]{lvl.upper
                            s.get("message", "—
                        )
                    console.print(table)
                else:
                    console.print("[dim]  No co
                break

    if not spike_found:
        console.print("[green]✅ No CPU spikes (

def display_error_summary(minutes: int = 60):
    console.rule(f"[bold cyan]   Error Summary 
    buckets = os_error_summary(minutes)
    if not buckets:
        console.print("[green]No errors or warn
        return

    table = Table(box=box.ROUNDED, header_style
    table.add_column("Host",     style="yellow"
    table.add_column("Total",    style="red",  
    table.add_column("Breakdown (level: count)"
    for b in buckets:
        host  = b["key"]
        total = b["doc_count"]
        breakdown = "  ".join(
            f"[{'red' if l['key'] == 'error' el
            for l in b["by_level"]["buckets"]
        )
        table.add_row(host, str(total), breakdo
    console.print(table)


def display_targets():
    console.rule("[bold cyan]🎯 Prometheus Scra
    data   = prom_targets()
    active = data.get("activeTargets", [])
    if not active:
        console.print("[yellow]No targets found
        return

    table = Table(box=box.ROUNDED, header_style
    table.add_column("Job",        style="cyan"
    table.add_column("Instance",   style="yello
    table.add_column("State",      width=10,   
    table.add_column("Last Scrape",            
    table.add_column("Error",      style="red",

    for t in active:
        state = t.get("health", "unknown")
        color = "green" if state == "up" else "
        table.add_row(
            t["labels"].get("job", "—"),
            t["labels"].get("instance", "—"),
            f"[{color}]{state.upper()}[/{color}
            t.get("lastScrape", "—")[:19].repla
            t.get("lastError", "") or "[dim]non
        )
    console.print(table)


def display_alerts():
    console.rule("[bold red]🚨 Active Alerts[/b
    alerts = prom_alerts()
    if not alerts:
        console.print("[green]✅ No active alert
        return

    for a in alerts:
        state   = a.get("state", "unknown")
        name    = a["labels"].get("alertname", 
        inst    = a["labels"].get("instance", "
        summary = a.get("annotations", {}).get(
        color   = "red" if state == "firing" el
        console.print(Panel(
            f"[bold {color}]{name}[/bold {color
            f"instance: [yellow]{inst}[/yellow]
            f"[dim]{summary}[/dim]",
            border_style=color, expand=False
        ))


def display_custom_promql():
    console.rule("[bold cyan]🔍 Custom PromQL Q
    q    = Prompt.ask("[cyan]Enter PromQL expre
    mode = Prompt.ask("Query type", choices=["i

    if mode == "instant":
        results = prom_query(q)
        table = Table(box=box.ROUNDED)
        table.add_column("Metric Labels", style
        table.add_column("Value",         style
        for r in results:
            labels = "  ".join(f"[dim]{k}[/dim]
            table.add_row(labels or "[dim]—[/di
        console.print(table if results else "[y
    else:
        minutes = IntPrompt.ask("Minutes back",
        display_metric_range(q, label=q, minute


def display_custom_log_search():
    console.rule("[bold cyan]🔍 Custom Log Sear
    q       = Prompt.ask("[cyan]Search query (L
    minutes = IntPrompt.ask("Minutes back", def
    level   = Prompt.ask("Filter level (leave b
                          default="", show_defa
    size    = IntPrompt.ask("Max results", defa
    display_logs(q, minutes, size, level)


def live_dashboard(refresh: int = 10):
    console.rule(f"[bold green]⚡ Live Dashboard
    try:
        while True:
            console.clear()
            console.rule("[bold green]⚡ Live Da
            display_system_snapshot()
            display_alerts()
            console.print(f"\n[dim]Last updated
            time.sleep(refresh)
    except KeyboardInterrupt:
        console.print("\n[yellow]Live dashboard

# ──────────────── Deep-Dive Functions ────────

def display_process_deep_dive(service_name: str
    """Reusable Linux process deep-dive for Kaf
    console.rule(f"[bold cyan]🔬 {service_name}

    # ── 1. Find PID ──────────────────────────
    try:
        raw = subprocess.check_output(
            ["pgrep", "-f", grep_pattern], time
        )
        pid = int(raw.decode().strip().splitlin
        console.print(f"  [green]PID found:[/gr
    except (subprocess.CalledProcessError, subp
            IndexError, ValueError):
        console.print(Panel(
            f"[bold red]{service_name} process 
            f"[dim]pgrep -f '{grep_pattern}' re
            border_style="red", expand=False
        ))
        return

    # ── 2. Kernel CPU ticks from /proc/<pid>/s
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().split()
        utime = int(fields[13])
        stime = int(fields[14])
        ticks_per_sec = os.sysconf("SC_CLK_TCK"
        cpu_seconds = (utime + stime) / ticks_p
        console.print(Panel(
            f"[cyan]Kernel CPU time (ticks):[/c
            f"[bold green]{cpu_seconds:.2f}[/bo
            f"[dim](utime={utime}  stime={stime
            border_style="cyan", expand=False
        ))
    except Exception as e:
        console.print(f"[red]Cannot read /proc/

    # ── 3. Thread count from /proc/<pid>/statu
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("Threads:"):
                    thread_count = line.split()
                    console.print(
                        f"  [cyan]Threads:[/cya
                    )
                    break
    except Exception as e:
        console.print(f"[red]Cannot read /proc/

    # ── 4. Process resource table (ps) ───────
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "pid,p
            capture_output=True, timeout=10, ch
        )
        if result.returncode == 0 and result.st
            table = Table(
                title=f"[bold magenta]{service_
                box=box.ROUNDED, header_style="
            )
            table.add_column("PID",     style="
            table.add_column("CPU%",    style="
            table.add_column("MEM%",    style="
            table.add_column("Command", style="
            for line in result.stdout.decode().
                parts = line.split(None, 3)
                if len(parts) >= 4:
                    table.add_row(parts[0], par
            console.print(table)
            console.print()
    except Exception as e:
        console.print(f"[red]ps command failed:

    # ── 5. Thread table (ps -L) ──────────────
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-L", "-o", 
             "--no-headers"],
            capture_output=True, timeout=10, ch
        )
        if result.returncode == 0 and result.st
            table = Table(
                title=f"[bold magenta]{service_
                box=box.ROUNDED, header_style="
            )
            table.add_column("PID",     style="
            table.add_column("TID",     style="
            table.add_column("CPU%",    style="
            table.add_column("State",   width=8
            table.add_column("Command", style="
            state_colors = {"R": "green", "S": 
            for line in result.stdout.decode().
                parts = line.split(None, 4)
                if len(parts) >= 5:
                    st = parts[3]
                    col = state_colors.get(st, 
                    table.add_row(
                        parts[0], parts[1], par
                        f"[{col}]{st}[/{col}]",
                    )
            console.print(table)
            console.print()
    except Exception as e:
        console.print(f"[red]Thread listing fai

    # ── 6. Context switches/sec (system-wide, 
    try:
        with open("/proc/stat") as f:
            for line in f:
                if line.startswith("ctxt "):
                    ctxt1 = int(line.split()[1]
                    break
        time.sleep(1)
        with open("/proc/stat") as f:
            for line in f:
                if line.startswith("ctxt "):
                    ctxt2 = int(line.split()[1]
                    break
        cs_per_sec = ctxt2 - ctxt1
        console.print(Panel(
            f"[cyan]Context switches/sec (syste
            f"[bold yellow]{cs_per_sec:,}[/bold
            border_style="cyan", expand=False
        ))
    except Exception as e:
        console.print(f"[red]Cannot read /proc/


def display_opensearch_internals():
    """Hit the OpenSearch REST API for node sta
    console.rule("[bold cyan]📡 OpenSearch API 

    scheme = "https"
    base   = f"{scheme}://{OPENSEARCH_HOST}:{OP
    auth   = (OPENSEARCH_USER, OPENSEARCH_PASS)

    # ── 1. Node stats ────────────────────────
    try:
        r = requests.get(
            f"{base}/_nodes/stats/os,process",
            auth=auth, verify=False, timeout=10
        )
        r.raise_for_status()
        data = r.json()
        table = Table(
            title="[bold magenta]Node Resource 
            box=box.ROUNDED, header_style="bold
        )
        table.add_column("Node",      style="cy
        table.add_column("Proc CPU%", width=10,
        table.add_column("OS Load 1m", style="y
        table.add_column("Open FDs",  style="ye
        table.add_column("Max FDs",   style="di

        for node_id, node in data.get("nodes", 
            name       = node.get("name", node_
            proc_cpu   = node.get("process", {}
            os_load    = node.get("os", {}).get
            open_fds   = node.get("process", {}
            max_fds    = node.get("process", {}

            if proc_cpu > 90:
                cpu_style = "bold red"
            elif proc_cpu > 70:
                cpu_style = "yellow"
            else:
                cpu_style = "green"

            table.add_row(
                name,
                f"[{cpu_style}]{proc_cpu}%[/{cp
                f"{os_load:.2f}",
                str(open_fds), str(max_fds)
            )
        console.print(table)
        console.print()
    except Exception as e:
        console.print(f"[red]Node stats error:[

    # ── 2. Thread pool stats ─────────────────
    try:
        r = requests.get(
            f"{base}/_cat/thread_pool/write,sea
            params={"v": "", "h": "name,active,
                    "format": "json"},
            auth=auth, verify=False, timeout=10
        )
        r.raise_for_status()
        pools = r.json()
        table = Table(
            title="[bold magenta]Thread Pool St
            box=box.ROUNDED, header_style="bold
        )
        table.add_column("Pool",      style="cy
        table.add_column("Active",    style="ye
        table.add_column("Queue",     width=8, 
        table.add_column("Rejected",  width=10,
        table.add_column("Completed", style="di

        for p in pools:
            queue_val    = int(p.get("queue", 0
            rejected_val = int(p.get("rejected"

            if queue_val > 50:
                q_style = "red"
            elif queue_val > 10:
                q_style = "yellow"
            else:
                q_style = "green"

            r_style = "bold red" if rejected_va

            table.add_row(
                p.get("name", "—"),
                str(p.get("active", 0)),
                f"[{q_style}]{queue_val}[/{q_st
                f"[{r_style}]{rejected_val}[/{r
                str(p.get("completed", 0))
            )
        console.print(table)
        console.print()
    except Exception as e:
        console.print(f"[red]Thread pool error:

    # ── 3. Cluster health ────────────────────
    try:
        r = requests.get(
            f"{base}/_cluster/health",
            params={"format": "json"},
            auth=auth, verify=False, timeout=10
        )
        r.raise_for_status()
        h = r.json()
        status = h.get("status", "unknown")
        status_color = {"green": "green", "yell
                        "red": "red"}.get(statu
        console.print(Panel(
            f"[bold]Cluster:[/bold] {h.get('clu
            f"Status: [{status_color}]● {status
            f"Nodes: [yellow]{h.get('number_of_
            f"Active shards: [green]{h.get('act
            f"Relocating: [yellow]{h.get('reloc
            f"Unassigned: [red]{h.get('unassign
            title="[bold magenta]Cluster Health
            border_style="cyan", expand=False
        ))
    except Exception as e:
        console.print(f"[red]Cluster health err


def display_logstash_internals():
    """Hit the Logstash monitoring API for proc
    console.rule("[bold cyan]⚙️  Logstash API I

    base = "http://localhost:9600"

    # ── 1. Process stats ─────────────────────
    try:
        r = requests.get(f"{base}/_node/stats/p
        r.raise_for_status()
        proc = r.json().get("process", {})
        cpu_pct = proc.get("cpu", {}).get("perc
        cpu_total = proc.get("cpu", {}).get("to
        mem = proc.get("mem", {})
        heap_pct = mem.get("total_virtual_in_by

        if cpu_pct > 90:
            cpu_style = "bold red"
        elif cpu_pct > 70:
            cpu_style = "yellow"
        else:
            cpu_style = "green"

        console.print(Panel(
            f"[cyan]CPU %:[/cyan] [{cpu_style}]
            f"[cyan]CPU total:[/cyan] [yellow]{
            f"[cyan]Heap used %:[/cyan] [yellow
            f"[cyan]Non-heap used:[/cyan] [yell
            f"{proc.get('mem', {}).get('non_hea
            title="[bold magenta]Logstash Proce
            border_style="cyan", expand=False
        ))
        console.print()
    except Exception as e:
        console.print(f"[red]Process stats erro

    # ── 2. Pipeline stats ────────────────────
    try:
        r = requests.get(f"{base}/_node/stats/p
        r.raise_for_status()
        pipelines = r.json().get("pipelines", {

        table = Table(
            title="[bold magenta]Pipeline Throu
            box=box.ROUNDED, header_style="bold
        )
        table.add_column("Pipeline",    style="
        table.add_column("Events In",   style="
        table.add_column("Events Out",  style="
        table.add_column("Duration ms", style="
        table.add_column("Throughput",  width=1

        for name, pdata in pipelines.items():
            events   = pdata.get("events", {})
            ev_in    = events.get("in", 0)
            ev_out   = events.get("out", 0)
            duration = events.get("duration_in_
            throughput = (ev_out / duration * 1

            if throughput > 1000:
                tp_style = "green"
            elif throughput >= 100:
                tp_style = "yellow"
            else:
                tp_style = "red"

            table.add_row(
                name, str(ev_in), str(ev_out), 
                f"[{tp_style}]{throughput:.2f} 
            )
        console.print(table)
        console.print()
    except Exception as e:
        console.print(f"[red]Pipeline stats err

    # ── 3. JVM stats ─────────────────────────
    try:
        r = requests.get(f"{base}/_node/stats/j
        r.raise_for_status()
        jvm  = r.json().get("jvm", {})
        mem  = jvm.get("mem", {})
        heap_pct = mem.get("heap_used_percent",
        heap_max = mem.get("heap_max_in_bytes",

        if heap_pct > 80:
            heap_style = "bold red"
        elif heap_pct >= 60:
            heap_style = "yellow"
        else:
            heap_style = "green"

        console.print(Panel(
            f"[cyan]Heap used:[/cyan] [{heap_st
            f"[cyan]Heap max:[/cyan] [yellow]{h
            title="[bold magenta]JVM Memory[/bo
            border_style="cyan", expand=False
        ))

        # GC Collectors
        gc = jvm.get("gc", {}).get("collectors"
        if gc:
            gc_table = Table(
                title="[bold magenta]GC Collect
                box=box.ROUNDED, header_style="
            )
            gc_table.add_column("Collector", st
            gc_table.add_column("Collections", 
            gc_table.add_column("Time (ms)",   
            for cname, cdata in gc.items():
                gc_table.add_row(
                    cname,
                    str(cdata.get("collection_c
                    str(cdata.get("collection_t
                )
            console.print(gc_table)
    except Exception as e:
        console.print(f"[red]JVM stats error:[/


def display_node_cpu_deep_dive():
    """Pure /proc and /sys CPU deep-dive — no P
    console.rule("[bold cyan]🖥️  Node CPU Deep

    # ── 1. Per-mode CPU percentages ──────────
    try:
        cpu_fields = ["user", "nice", "system",
                      "irq", "softirq", "steal"

        def read_cpu_stat():
            with open("/proc/stat") as f:
                line = f.readline()          # 
            vals = list(map(int, line.split()[1
            return vals

        snap1 = read_cpu_stat()
        time.sleep(1)
        snap2 = read_cpu_stat()

        deltas = [b - a for a, b in zip(snap1, 
        total  = sum(deltas) or 1

        table = Table(
            title="[bold magenta]CPU Mode Break
            box=box.ROUNDED, header_style="bold
        )
        table.add_column("Mode",    style="cyan
        table.add_column("%",       width=8,   
        table.add_column("Bar",     width=40)

        for mode_name, delta in zip(cpu_fields,
            pct = delta / total * 100
            bar_len = int(pct / 100 * 38)
            bar = "█" * bar_len

            if (mode_name == "iowait" and pct >
               (mode_name == "steal" and pct > 
                color = "red"
            elif mode_name == "idle":
                color = "dim"
            elif mode_name in ("user", "system"
                color = "green"
            else:
                color = "cyan"

            table.add_row(mode_name, f"[{color}
                          f"[{color}]{bar}[/{co

        console.print(table)
        console.print()
    except Exception as e:
        console.print(f"[red]CPU mode breakdown

    # ── 2. Load averages ─────────────────────
    try:
        with open("/proc/loadavg") as f:
            parts = f.read().split()
        load1, load5, load15 = float(parts[0]),

        result = subprocess.run(
            ["nproc"], capture_output=True, tim
        )
        cores = int(result.stdout.decode().stri

        table = Table(
            title="[bold magenta]Load Averages[
            box=box.ROUNDED, header_style="bold
        )
        table.add_column("Period",     style="c
        table.add_column("Load",      style="ye
        table.add_column("Normalized", width=14
        table.add_column("Status",    width=10,

        for period, load_val in [("1 min", load
            norm = load_val / cores
            if norm > 1.0:
                norm_style, status = "red", "🔴
            elif norm >= 0.7:
                norm_style, status = "yellow", 
            else:
                norm_style, status = "green", "
            table.add_row(
                period, f"{load_val:.2f}",
                f"[{norm_style}]{norm:.2f}[/{no
                status
            )
        console.print(table)
        console.print()
    except Exception as e:
        console.print(f"[red]Load average error

    # ── 3. CPU frequency / throttling ────────
    try:
        cur_files = sorted(glob.glob(
            "/sys/devices/system/cpu/cpu*/cpufr
        ))
        if not cur_files:
            raise FileNotFoundError("cpufreq no

        table = Table(
            title="[bold magenta]CPU Frequency 
            box=box.ROUNDED, header_style="bold
        )
        table.add_column("Core",     style="cya
        table.add_column("Cur kHz",  style="yel
        table.add_column("Max kHz",  style="dim
        table.add_column("% of Max", width=10, 
        table.add_column("Bar",      width=22)

        for cur_path in cur_files:
            core_name = cur_path.split("/")[-3]
            max_path  = cur_path.replace("scali
            try:
                with open(cur_path) as f:
                    cur_khz = int(f.read().stri
                with open(max_path) as f:
                    max_khz = int(f.read().stri
            except (FileNotFoundError, ValueErr
                continue

            pct = cur_khz / max_khz * 100 if ma
            bar_len = int(pct / 100 * 20)
            bar = "█" * bar_len

            if pct < 70:
                color = "red"
            elif pct < 90:
                color = "yellow"
            else:
                color = "green"

            table.add_row(
                core_name, str(cur_khz), str(ma
                f"[{color}]{pct:.0f}%[/{color}]
                f"[{color}]{bar}[/{color}]"
            )
        console.print(table)
        console.print()
    except FileNotFoundError:
        console.print(Panel(
            "[yellow]cpufreq data not available
            "[dim]This VM or container does not
            "/sys/devices/system/cpu/cpu*/cpufr
            border_style="yellow", expand=False
        ))
    except Exception as e:
        console.print(f"[red]CPU frequency erro

    # ── 4. Context switches/sec and Interrupts
    try:
        def read_stat_counters():
            ctxt = intr = 0
            with open("/proc/stat") as f:
                for line in f:
                    if line.startswith("ctxt ")
                        ctxt = int(line.split()
                    elif line.startswith("intr 
                        intr = int(line.split()
            return ctxt, intr

        c1, i1 = read_stat_counters()
        time.sleep(1)
        c2, i2 = read_stat_counters()

        console.print(Panel(
            f"[cyan]Context switches/sec:[/cyan
            f"[bold yellow]{c2 - c1:,}[/bold ye
            f"[cyan]Interrupts/sec:[/cyan]     
            f"[bold yellow]{i2 - i1:,}[/bold ye
            title="[bold magenta]Kernel Counter
            border_style="cyan", expand=False
        ))
    except Exception as e:
        console.print(f"[red]Kernel counters er


# ──────────────── Main Menu ──────────────────

MENU_ITEMS = {
    "1":  ("📊 System Metrics Snapshot",       
    "2":  ("📈 CPU Range (last 30 min)",
           lambda: display_metric_range(
               '100 - (avg by(instance)(rate(no
               "CPU Usage %", 30)),
    "3":  ("💾 Memory Range (last 30 min)",
           lambda: display_metric_range(
               "100*(1-node_memory_MemAvailable
               "Memory Used %", 30)),
    "4":  ("🔗 Spike ↔ Log Correlation",       
    "5":  ("📋 Recent Logs (all)",
           lambda: display_logs("*", 30, 25)),
    "6":  ("❌ Error/Warning Log Summary",
           lambda: display_error_summary(60)),
    "7":  ("🎯 Prometheus Scrape Targets",     
    "8":  ("🚨 Active Prometheus Alerts",      
    "9":  ("🔍 Custom PromQL Query",           
    "10": ("🔍 Custom Log Search",             
    "11": ("⚡ Live Auto-Refresh Dashboard",
           lambda: live_dashboard(10)),
    "12": ("🔬 Kafka Process Deep Dive",
           lambda: display_process_deep_dive("K
    "13": ("🔬 OpenSearch Process Deep Dive",
           lambda: display_process_deep_dive("O
    "14": ("🔬 Logstash Process Deep Dive",
           lambda: display_process_deep_dive("L
    "15": ("📡 OpenSearch API Internals",      
    "16": ("⚙️  Logstash API Internals",       
    "17": ("🖥️  Node CPU Deep Dive (/proc)",  
    "0":  ("🚪 Exit",                          
}


def print_menu():
    console.print(Panel.fit(
        "[bold cyan]Observability Terminal — Pr
        "[dim]Node Exporter metrics correlated 
        border_style="cyan"
    ))
    table = Table(box=box.SIMPLE, show_header=F
    table.add_column("Key",    style="bold yell
    table.add_column("Action", style="white")
    for key, (label, _) in MENU_ITEMS.items():
        table.add_row(f"[{key}]", label)
    console.print(table)


def main():
    while True:
        print_menu()
        choice = Prompt.ask("\n[bold yellow]Sel
                             choices=list(MENU_
        if choice == "0":
            console.print("[bold green]Goodbye!
            sys.exit(0)

        label, fn = MENU_ITEMS[choice]
        console.print()
        try:
            fn()
        except Exception as e:
            console.print(f"[red]Error:[/red] {
        console.print()
        Prompt.ask("[dim]Press Enter to return 
        console.clear()


if __name__ == "__main__":
    main()

                                              
