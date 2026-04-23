"""
View -- Topic Deep Dive

Per-topic table: offset depth (summed across partitions), produce rate,
consume rate, consumer lag, retention depth, retention policy.
Drill-down into a selected topic for:
  - Per-partition breakdown table
  - Consumer Lag bar chart (this topic)
  - Produced vs Consumed sparklines (this topic)
  - Retention policy panel (this topic)
Skips internal topics (__consumer_offsets, __transaction_state).
Analogous to OpenSearch's index_deep_dive.py.
"""

from __future__ import annotations

import shutil
import datetime
import requests
import urllib3
from rich.panel import Panel
from rich.table import Table
from rich import box
from simple_term_menu import TerminalMenu

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from monitor.config import console
from monitor.kafka.collectors import (
    PROM_URL, PROM_AUTH, PROM_VERIFY, KAFKA_GROUP, KAFKA_BOOTSTRAP,
    prom_range,
)
from monitor.kafka.display_utils import score_color
from monitor.utils import press_enter_to_return

_SKIP_TOPICS = {"__consumer_offsets", "__transaction_state"}
_BLOCKS = " ▁▂▃▄▅▆▇█"


# ---------------------------------------------------------------------------
# Prometheus helpers
# ---------------------------------------------------------------------------

def _fetch_vec(promql: str, *label_keys: str) -> dict:
    """Query Prometheus instant vector, key by tuple of requested labels."""
    result = {}
    try:
        r = requests.get(
            f"{PROM_URL}/api/v1/query",
            params={"query": promql},
            auth=PROM_AUTH,
            verify=PROM_VERIFY,
            timeout=8,
        )
        r.raise_for_status()
        for item in r.json().get("data", {}).get("result", []):
            key = tuple(item["metric"].get(k, "") for k in label_keys)
            result[key] = float(item["value"][1])
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# Retention policy fetch
# ---------------------------------------------------------------------------

def _fetch_retention(topic: str) -> dict:
    """
    Fetch topic-level retention config via kafka-python KafkaAdminClient.
    Falls back to broker defaults from server.properties if unavailable.
    Returns a dict with keys: policy, retention_ms, retention_bytes,
    retention_human, size_human, segment_bytes.
    """
    defaults = {
        "policy":          "delete",
        "retention_ms":    604_800_000,   # 7 days
        "retention_bytes": -1,
        "segment_bytes":   1_073_741_824, # 1 GiB
        "source":          "broker-default",
    }

    # Try to load broker defaults from server.properties first
    try:
        props_path = "/opt/kafka/kafka/config/server.properties"
        with open(props_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip()
                if k == "log.retention.ms":
                    defaults["retention_ms"] = int(v)
                elif k == "log.retention.hours":
                    defaults["retention_ms"] = int(v) * 3_600_000
                elif k == "log.retention.bytes":
                    defaults["retention_bytes"] = int(v)
                elif k == "log.segment.bytes":
                    defaults["segment_bytes"] = int(v)
                elif k == "log.cleanup.policy":
                    defaults["policy"] = v
    except Exception:
        pass

    result = dict(defaults)

    # Try topic-level overrides via kafka-python
    try:
        from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=5000,
        )
        resource = ConfigResource(ConfigResourceType.TOPIC, topic)
        configs = admin.describe_configs([resource])
        admin.close()

        for res, cfg in configs.items():
            if hasattr(cfg, "resources"):
                cfg = cfg.resources[0][4]  # nested structure varies by version
            for entry in (cfg if isinstance(cfg, list) else []):
                name  = entry.get("config_names",  entry.get("name",  ""))
                value = entry.get("config_values", entry.get("value", None))
                if value in (None, ""):
                    continue
                if name == "cleanup.policy":
                    result["policy"]          = value
                    result["source"]          = "topic-override"
                elif name == "retention.ms":
                    result["retention_ms"]    = int(value)
                    result["source"]          = "topic-override"
                elif name == "retention.bytes":
                    result["retention_bytes"] = int(value)
                    result["source"]          = "topic-override"
                elif name == "segment.bytes":
                    result["segment_bytes"]   = int(value)
                    result["source"]          = "topic-override"
    except Exception:
        pass

    # Human-readable conversions
    ms  = result["retention_ms"]
    byt = result["retention_bytes"]
    seg = result["segment_bytes"]

    def _ms_human(ms: int) -> str:
        if ms < 0:
            return "unlimited"
        s = ms // 1000
        if s < 60:
            return f"{s}s"
        if s < 3_600:
            return f"{s // 60}m"
        if s < 86_400:
            return f"{s // 3_600}h"
        return f"{s // 86_400}d"

    def _bytes_human(b: int) -> str:
        if b < 0:
            return "unlimited"
        for unit, div in (("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
            if b >= div:
                return f"{b / div:.1f} {unit}"
        return f"{b} B"

    result["retention_human"] = _ms_human(ms)
    result["size_human"]      = _bytes_human(byt)
    result["segment_human"]   = _bytes_human(seg)
    return result


# ---------------------------------------------------------------------------
# Data collectors
# ---------------------------------------------------------------------------

def _collect_topic_summary() -> list[dict]:
    """Return one summary dict per user topic, sorted by offset descending."""
    cur_offsets   = _fetch_vec("kafka_topic_partition_current_offset",            "topic", "partition")
    cons_offsets  = _fetch_vec("kafka_consumergroup_current_offset",              "topic", "partition")
    old_offsets   = _fetch_vec("kafka_topic_oldest_offset",                       "topic", "partition")
    under_rep     = _fetch_vec("kafka_topic_partition_under_replicated_partition", "topic", "partition")
    total_parts   = _fetch_vec("kafka_topic_partitions",                          "topic")
    lag_vec       = _fetch_vec(
        f'kafka_consumergroup_lag{{consumergroup="{KAFKA_GROUP}"}}',
        "topic", "partition",
    )
    prod_rate_vec = _fetch_vec(
        "rate(kafka_topic_partition_current_offset[60s])",
        "topic", "partition",
    )
    cons_rate_vec = _fetch_vec(
        f'rate(kafka_consumergroup_current_offset{{consumergroup="{KAFKA_GROUP}"}}[60s])',
        "topic", "partition",
    )

    all_topics = {k[0] for k in cur_offsets} | {k[0] for k in old_offsets}
    rows = []
    for topic in all_topics:
        if topic in _SKIP_TOPICS:
            continue

        total_cur      = sum(v for (t, _), v in cur_offsets.items()   if t == topic)
        total_cons_off = sum(v for (t, _), v in cons_offsets.items()  if t == topic)
        total_old      = sum(v for (t, _), v in old_offsets.items()   if t == topic)
        total_ur       = sum(v for (t, _), v in under_rep.items()     if t == topic)
        total_lag      = sum(v for (t, _), v in lag_vec.items()       if t == topic)
        prod_rate      = sum(v for (t, _), v in prod_rate_vec.items() if t == topic)
        cons_rate      = sum(v for (t, _), v in cons_rate_vec.items() if t == topic)
        num_parts      = int(total_parts.get((topic,), 0))

        rows.append({
            "topic":       topic,
            "partitions":  num_parts,
            "cur_offset":  int(total_cur),
            "cons_offset": int(total_cons_off),
            "retention":   int(max(total_cur - total_old, 0)),
            "lag":         int(total_lag),
            "prod_rate":   round(prod_rate, 4),
            "cons_rate":   round(cons_rate, 4),
            "under_rep":   int(total_ur),
        })
    return sorted(rows, key=lambda x: x["cur_offset"], reverse=True)


def _collect_partition_detail(topic: str) -> list[dict]:
    """Per-partition breakdown for a single topic."""
    cur      = _fetch_vec(f'kafka_topic_partition_current_offset{{topic="{topic}"}}',             "topic", "partition")
    cons_off = _fetch_vec(
        f'kafka_consumergroup_current_offset{{topic="{topic}",consumergroup="{KAFKA_GROUP}"}}',
        "topic", "partition",
    )
    old      = _fetch_vec(f'kafka_topic_oldest_offset{{topic="{topic}"}}',                        "topic", "partition")
    ur       = _fetch_vec(f'kafka_topic_partition_under_replicated_partition{{topic="{topic}"}}',  "topic", "partition")
    isr      = _fetch_vec(f'kafka_topic_partition_in_sync_replica{{topic="{topic}"}}',             "topic", "partition")
    ld       = _fetch_vec(f'kafka_topic_partition_leader{{topic="{topic}"}}',                      "topic", "partition")
    lag      = _fetch_vec(
        f'kafka_consumergroup_lag{{topic="{topic}",consumergroup="{KAFKA_GROUP}"}}',
        "topic", "partition",
    )
    prod_rate = _fetch_vec(
        f'rate(kafka_topic_partition_current_offset{{topic="{topic}"}}[60s])',
        "topic", "partition",
    )
    cons_rate = _fetch_vec(
        f'rate(kafka_consumergroup_current_offset{{topic="{topic}",consumergroup="{KAFKA_GROUP}"}}[60s])',
        "topic", "partition",
    )

    all_parts = {k[1] for k in cur} | {k[1] for k in old}
    rows = []
    for part in sorted(all_parts, key=lambda x: int(x) if x.isdigit() else 0):
        k = (topic, part)
        c  = int(cur.get(k, 0))
        co = int(cons_off.get(k, 0))
        o  = int(old.get(k, 0))
        rows.append({
            "partition":  part,
            "leader":     int(ld.get(k, -1)),
            "isr":        int(isr.get(k, -1)),
            "under_rep":  int(ur.get(k, 0)),
            "cur_offset": c,
            "cons_offset": co,
            "old_offset": o,
            "retention":  max(c - o, 0),
            "lag":        int(lag.get(k, 0)),
            "prod_rate":  round(prod_rate.get(k, 0.0), 4),
            "cons_rate":  round(cons_rate.get(k, 0.0), 4),
        })
    return rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _lag_color(lag: int) -> str:
    return "red" if lag > 10000 else "yellow" if lag > 1000 else "green"


def _rate_str(rate: float) -> str:
    return f"{rate:.4f} msg/s" if rate else "[dim]0.0000 msg/s[/dim]"

def _ask_minutes(default: int = 30) -> int:
    """
    Prompt the user for how many minutes of historical data to plot.
    Accepts blank input (uses default), enforces 5–1440 min (24 h) range.
    """
    console.print(
        f"\n  [dim]How many minutes of data to display? "
        f"(5–1440, default={default}):[/dim]"
    )
    try:
        raw = input("  ❯ ").strip()
        if not raw:
            return default
        val = int(raw)
        if val < 5:
            console.print("  [yellow]Minimum is 5 min — using 5.[/yellow]")
            return 5
        if val > 1440:
            console.print("  [yellow]Maximum is 1440 min (24 h) — using 1440.[/yellow]")
            return 1440
        return val
    except (ValueError, EOFError, KeyboardInterrupt):
        console.print(f"  [yellow]Invalid input — using default ({default} min).[/yellow]")
        return default


# ---------------------------------------------------------------------------
# Per-topic graph: Consumer Lag bar chart
# ---------------------------------------------------------------------------

def _graph_consumer_lag(topic: str, minutes: int = 30) -> None:
    minutes = _ask_minutes(default=minutes)

    promql = f'sum(kafka_consumergroup_lag{{topic="{topic}",consumergroup="{KAFKA_GROUP}"}})'
    series = prom_range(promql, minutes=minutes)

    console.print()
    console.rule(
        f"[bold yellow]📈 Consumer Lag — {topic} "
        f"[dim](last {minutes} min)[/dim][/bold yellow]"
    )
    console.print()

    if not series:
        console.print(Panel(
            "[yellow]No lag data from Prometheus for this topic.[/yellow]\n"
            "[dim]kafka_exporter may not be scraping this consumer group.[/dim]",
            border_style="yellow", expand=False,
        ))
        press_enter_to_return()
        return

    term_w = shutil.get_terminal_size((100, 30)).columns
    width  = max(40, term_w - 12)
    values = [v for _, v in series]
    times  = [t for t, _ in series]

    if len(values) > width:
        step   = len(values) / width
        values = [values[int(i * step)] for i in range(width)]
        times  = [times[int(i * step)]  for i in range(width)]

    mn, mx  = min(values), max(values)
    avg_v   = sum(values) / len(values)
    latest  = values[-1]

    def to_score(v):
        return min(v / 300.0, 1.0) * 100

    rows      = 8
    bar_lines = ["" for _ in range(rows)]
    for v in values:
        score = to_score(v)
        c     = score_color(score, "K1")
        level = int((v - mn) / (mx - mn + 1e-9) * rows) if mx != mn else 1
        level = max(1, min(level, rows))
        for row in range(rows):
            bar_lines[row] += f"[{c}]█[/{c}]" if (rows - 1 - row) < level else " "

    y_ticks = {0: f"{mx:.2f}", 2: f"{mx*0.75:.2f}",
               4: f"{mx*0.5:.2f}", 6: f"{mx*0.25:.2f}", 7: f"{mn:.2f}"}
    for i, line in enumerate(bar_lines):
        lbl = y_ticks.get(i, "")
        console.print(f"[dim]{lbl:>8}[/dim] │" + line)
    console.print("         └" + "─" * min(len(values), width))

    if times:
        t_s = datetime.datetime.fromtimestamp(times[0]).strftime("%H:%M")
        t_e = datetime.datetime.fromtimestamp(times[-1]).strftime("%H:%M")
        pad = max(0, min(len(values), width) - 10)
        console.print(f"          [dim]{t_s}{' ' * pad}{t_e} IST[/dim]")

    mid   = len(values) // 2
    first = sum(values[:mid]) / max(mid, 1)
    last  = sum(values[mid:]) / max(len(values[mid:]), 1)
    trend = ("↑ Rising"  if last > first + (mx * 0.02) else
             "↓ Falling" if last < first - (mx * 0.02) else "→ Stable")
    tc    = "red" if trend.startswith("↑") else "green" if trend.startswith("↓") else "cyan"
    lc    = score_color(to_score(latest), "K1")

    console.print(
        f"\n  [bold]Stats:[/bold]  "
        f"Latest=[bold {lc}]{latest:.0f}[/bold {lc}] msgs  "
        f"Min=[green]{mn:.0f}[/green]  "
        f"Max=[red]{mx:.0f}[/red]  "
        f"Avg=[cyan]{avg_v:.1f}[/cyan]  "
        f"Trend=[{tc}]{trend}[/{tc}]\n"
    )
    press_enter_to_return()


# ---------------------------------------------------------------------------
# Per-topic graph: Produced vs Consumed sparklines
# ---------------------------------------------------------------------------

def _graph_throughput(topic: str, minutes: int = 30) -> None:
    minutes = _ask_minutes(default=minutes)

    promql_prod = (
        f'sum(rate(kafka_topic_partition_current_offset{{topic="{topic}"}}[60s]))'
    )
    promql_cons = (
        f'sum(rate(kafka_consumergroup_current_offset'
        f'{{topic="{topic}",consumergroup="{KAFKA_GROUP}"}}[60s]))'
    )

    s_prod = prom_range(promql_prod, minutes=minutes)
    s_cons = prom_range(promql_cons, minutes=minutes)

    console.print()
    console.rule(
        f"[bold yellow]📊 Produced vs Consumed — {topic} "
        f"[dim](last {minutes} min)[/dim][/bold yellow]"
    )
    console.print()

    if not s_prod and not s_cons:
        console.print(Panel(
            "[yellow]No throughput data from Prometheus for this topic.[/yellow]",
            border_style="yellow", expand=False,
        ))
        press_enter_to_return()
        return

    term_w = shutil.get_terminal_size((100, 30)).columns
    width  = max(40, term_w - 20)

    def _sparkline(series, color):
        vals = [v for _, v in series]
        if len(vals) > width:
            step = len(vals) / width
            vals = [vals[int(i * step)] for i in range(width)]
        if not vals:
            return "", 0.0, 0.0, 0.0, 0.0
        mn_, mx_ = min(vals), max(vals)
        spark = ""
        for v in vals:
            idx   = int((v - mn_) / (mx_ - mn_ + 1e-9) * 8) if mx_ != mn_ else 4
            idx   = max(0, min(8, idx))
            spark += f"[{color}]{_BLOCKS[idx]}[/{color}]"
        avg = sum(vals) / len(vals)
        return spark, vals[-1], mn_, mx_, avg

    if s_prod:
        sp, lat, mn_, mx_, av = _sparkline(s_prod, "yellow")
        console.print(
            f"  [yellow]{'Produced (K7)':<22}[/yellow]  "
            f"latest=[yellow]{lat:.4f}[/yellow] msg/s  "
            f"[dim]avg={av:.4f}  max={mx_:.4f}[/dim]"
        )
        console.print(f"  {'':22}  {sp}")
        console.print()

    if s_cons:
        sp, lat, mn_, mx_, av = _sparkline(s_cons, "cyan")
        console.print(
            f"  [cyan]{'Consumed (K8)':<22}[/cyan]  "
            f"latest=[cyan]{lat:.4f}[/cyan] msg/s  "
            f"[dim]avg={av:.4f}  max={mx_:.4f}[/dim]"
        )
        console.print(f"  {'':22}  {sp}")
        console.print()

    if s_prod and s_cons:
        lat_p = [v for _, v in s_prod][-1]
        lat_c = [v for _, v in s_cons][-1]
        gap   = lat_p - lat_c
        gc    = "red" if gap > 0.5 else ("yellow" if gap > 0 else "green")
        label = "producer ahead" if gap > 0 else "consumer caught up"
        console.print(
            f"  [bold]Current gap (prod − cons):[/bold]  "
            f"[{gc}]{gap:+.4f} msg/s[/{gc}]  [dim]({label})[/dim]\n"
        )

    press_enter_to_return()

# ---------------------------------------------------------------------------
# Retention policy panel
# ---------------------------------------------------------------------------

def _display_retention(topic: str) -> None:
    console.print()
    console.rule(f"[bold yellow]🗄  Retention Policy — {topic}[/bold yellow]")
    console.print()

    cfg = _fetch_retention(topic)

    policy       = cfg["policy"]
    policy_color = "cyan" if policy == "compact" else "yellow"
    ret_color    = "green" if cfg["retention_ms"] > 0 else "dim"
    size_color   = "green" if cfg["retention_bytes"] < 0 else "yellow"

    table = Table(box=box.SIMPLE, show_header=False, expand=False, padding=(0, 2))
    table.add_column("Key",   style="bold dim",  width=26)
    table.add_column("Value", style="white",     width=30)

    table.add_row("Cleanup Policy",    f"[{policy_color}]{policy}[/{policy_color}]")
    table.add_row("Retention (time)",  f"[{ret_color}]{cfg['retention_human']}[/{ret_color}]"
                                       f"  [dim]({cfg['retention_ms']:,} ms)[/dim]")
    table.add_row("Retention (size)",  f"[{size_color}]{cfg['size_human']}[/{size_color}]"
                                       f"  [dim]({cfg['retention_bytes']:,} bytes)[/dim]")
    table.add_row("Segment Size",      f"{cfg['segment_human']}"
                                       f"  [dim]({cfg['segment_bytes']:,} bytes)[/dim]")
    table.add_row("Config Source",     f"[dim]{cfg['source']}[/dim]")

    console.print(table)
    console.print()

    if policy == "delete":
        console.print(
            "  [dim]Messages are deleted once they exceed the time OR size limit "
            "(whichever is hit first).[/dim]"
        )
    elif policy == "compact":
        console.print(
            "  [dim]Log compaction is enabled — only the latest value per key is "
            "retained. Time/size limits still apply to non-compacted segments.[/dim]"
        )
    console.print()
    press_enter_to_return()


# ---------------------------------------------------------------------------
# Partition table drill-down
# ---------------------------------------------------------------------------

def _display_topic_drill(topic: str) -> None:
    console.print()
    console.rule(f"[bold yellow]Partition Detail — {topic}[/bold yellow]")
    console.print()

    rows = _collect_partition_detail(topic)
    if not rows:
        console.print(f"[yellow]No partition data found for '{topic}'.[/yellow]")
        return

    table = Table(box=box.ROUNDED, show_header=True, header_style="bold yellow", expand=True)
    table.add_column("Partition",           width=10, justify="center")
    table.add_column("Leader",              width=8,  justify="center")
    table.add_column("ISR",                 width=6,  justify="center")
    table.add_column("Under-Rep",           width=11, justify="center")
    table.add_column("Latest Offset (LEO)", width=18, justify="right")
    table.add_column("Consumer Offset",     width=16, justify="right")
    table.add_column("Oldest Offset",       width=14, justify="right")
    table.add_column("Retention",           width=12, justify="right")
    table.add_column("Consumer Lag",        width=14, justify="right")
    table.add_column("Produce Rate",        width=16, justify="right")
    table.add_column("Consume Rate",        width=16, justify="right")

    for p in rows:
        lc      = _lag_color(p["lag"])
        ur_str  = f"[red]{p['under_rep']}[/red]"  if p["under_rep"] > 0 else "[green]0[/green]"
        isr_str = str(p["isr"])    if p["isr"]    >= 0 else "[dim]--[/dim]"
        ld_str  = str(p["leader"]) if p["leader"] >= 0 else "[dim]--[/dim]"
        table.add_row(
            p["partition"], ld_str, isr_str, ur_str,
            f"{p['cur_offset']:,}",
            f"{p['cons_offset']:,}",
            f"{p['old_offset']:,}",
            f"{p['retention']:,}",
            f"[{lc}]{p['lag']:,}[/{lc}]",
            _rate_str(p["prod_rate"]),
            _rate_str(p["cons_rate"]),
        )

    console.print(table)
    console.print()


# ---------------------------------------------------------------------------
# Drill-down sub-menu
# ---------------------------------------------------------------------------

def _topic_submenu(topic: str) -> None:
    """Sub-menu shown after selecting a topic — table + graphs + retention."""
    while True:
        console.print()
        console.rule(f"[bold yellow]{topic}[/bold yellow]")
        console.print(
            "  [dim]Select a view for this topic:[/dim]\n"
        )

        options = [
            "Partition Table",
            "Consumer Lag Graph",
            "Produced vs Consumed Graph",
            "Retention Policy",
            "Back",
        ]
        menu   = TerminalMenu(
            options,
            menu_cursor="❯ ",
            menu_cursor_style=("fg_yellow", "bold"),
            menu_highlight_style=("fg_yellow", "bold"),
        )
        choice = menu.show()

        if choice is None or choice == 4:
            return
        elif choice == 0:
            _display_topic_drill(topic)
        elif choice == 1:
            _graph_consumer_lag(topic, minutes=30)
        elif choice == 2:
            _graph_throughput(topic, minutes=30)
        elif choice == 3:
            _display_retention(topic)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def display_topic_deep_dive(timeframe: str = "1h") -> None:
    """Render the Kafka Topic Deep Dive view."""
    console.print()
    console.rule("[bold yellow]Kafka — Topic Deep Dive[/bold yellow]")
    console.print()

    topics = _collect_topic_summary()

    if not topics:
        console.print("[yellow]No topic data returned from Prometheus.[/yellow]")
        console.print("[dim]Check that kafka_exporter is running and reachable.[/dim]")
        press_enter_to_return()
        return

    table = Table(
        box=box.ROUNDED,
        show_header=True,
        header_style="bold yellow",
        title="[bold]Topics[/bold]  [dim](internal topics excluded)[/dim]",
        title_style="bold white",
        expand=True,
    )
    table.add_column("#",                        style="dim", width=4,  justify="right")
    table.add_column("Topic",                    style="white", ratio=2)
    table.add_column("Parts",                    width=7,  justify="center")
    table.add_column("Latest Offset / LEO (Σ)", width=22, justify="right")
    table.add_column("Consumer Offset (Σ)",      width=20, justify="right")
    table.add_column("Retention Depth (Σ)",      width=20, justify="right")
    table.add_column("Consumer Lag",             width=14, justify="right")
    table.add_column("Produce Rate",             width=15, justify="right")
    table.add_column("Consume Rate",             width=15, justify="right")
    table.add_column("Under-Rep",                width=11, justify="center")

    topic_names: list[str] = []
    for i, t in enumerate(topics, 1):
        topic_names.append(t["topic"])
        lc     = _lag_color(t["lag"])
        ur_str = f"[red]{t['under_rep']}[/red]" if t["under_rep"] > 0 else "[green]0[/green]"
        table.add_row(
            str(i),
            t["topic"],
            str(t["partitions"]),
            f"{t['cur_offset']:,}",
            f"{t['cons_offset']:,}",
            f"{t['retention']:,}",
            f"[{lc}]{t['lag']:,}[/{lc}]",
            _rate_str(t["prod_rate"]),
            _rate_str(t["cons_rate"]),
            ur_str,
        )

    console.print(table)
    console.print()
    console.print(
        "  [dim]Latest Offset (LEO), Consumer Offset, and Retention Depth are "
        "sums across all partitions of each topic.[/dim]"
    )
    console.print()
    console.print("[dim]Select a topic to inspect:[/dim]")
    console.print()

    drill_options = topic_names + ["Back"]
    menu   = TerminalMenu(
        drill_options,
        menu_cursor="❯ ",
        menu_cursor_style=("fg_yellow", "bold"),
        menu_highlight_style=("fg_yellow", "bold"),
    )
    choice = menu.show()

    if choice is None or choice == len(topic_names):
        return

    _topic_submenu(drill_options[choice])