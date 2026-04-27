"""
View — Broker Performance

Shows broker identity (ID, address) alongside live resource metrics:
CPU (K4), Memory/RSS (K5), and Disk (K6). Includes a plain-English
diagnostic panel when resource pressure is detected.
Analogous to OpenSearch's node_performance.py.
"""

from __future__ import annotations

import requests
import urllib3
from rich.panel import Panel
from rich.table import Table
from rich import box

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from monitor.config import console
from monitor.kafka.collectors import (
    collect_k4, collect_k5, collect_k6,
    prom_instant, PROM_URL, PROM_AUTH, PROM_VERIFY,
)
from monitor.kafka.display_utils import score_color, score_bar, status_icon, fmt_score
from monitor.utils import press_enter_to_return


# ── Thresholds (mirror K4/K5/K6 warn thresholds from collectors.py) ──────────
_CPU_WARN  = 50
_CPU_CRIT  = 80
_MEM_WARN  = 50
_MEM_CRIT  = 75
_DISK_WARN = 25
_DISK_CRIT = 40


def _fetch_broker_info() -> list[dict]:
    """Fetch kafka_broker_info labels from Prometheus."""
    result = []
    try:
        r = requests.get(
            f"{PROM_URL}/api/v1/query",
            params={"query": "kafka_broker_info"},
            auth=PROM_AUTH,
            verify=PROM_VERIFY,
            timeout=8,
        )
        r.raise_for_status()
        for item in r.json().get("data", {}).get("result", []):
            m = item["metric"]
            result.append({
                "id":      m.get("id", "—"),
                "address": m.get("address", "—"),
            })
    except Exception:
        pass
    return result


def _fetch_broker_count() -> int:
    val = prom_instant("kafka_brokers")
    return int(val) if val is not None else 0


def _plain_english_diagnostic(cpu_pct, mem_pct, disk_pct) -> str:
    cpu  = cpu_pct  or 0.0
    mem  = mem_pct  or 0.0
    disk = disk_pct or 0.0
    if cpu >= _CPU_CRIT and mem >= _MEM_CRIT:
        return (
            "Broker is severely resource-constrained. Both CPU and memory are critically high. "
            "Reduce producer throughput or add broker capacity immediately."
        )
    if cpu >= _CPU_CRIT:
        return (
            "Broker is compute-bound. The Kafka JVM is consuming a large share of CPU. "
            "Check for unusually high producer rates, rebalance activity, or log compaction."
        )
    if mem >= _MEM_CRIT:
        return (
            "Broker RSS memory usage is critically high. The JVM may be under heap pressure. "
            "Consider increasing broker heap (-Xmx) or reducing partition count on this broker."
        )
    if disk >= _DISK_CRIT:
        return (
            "Disk usage is critically high. Kafka log retention may not be reclaiming space fast enough. "
            "Check log.retention.bytes / log.retention.hours and consider adding storage."
        )
    if disk >= _DISK_WARN:
        return (
            "Disk usage is elevated. Monitor closely — Kafka will stop accepting writes "
            "if the disk fills completely. Review retention policies."
        )
    if cpu >= _CPU_WARN:
        return (
            "CPU usage is elevated but not yet critical. "
            "Watch for spikes during consumer rebalances or partition reassignment."
        )
    if mem >= _MEM_WARN:
        return (
            "Memory usage is elevated. This is normal during high-throughput bursts "
            "but sustained high RSS can indicate memory pressure."
        )
    return "Broker resources are within normal operating range."


def _status_str(value, warn: float, crit: float, unit: str = "%") -> str:
    if value is None:
        return "[dim]N/A[/dim]"
    if value >= crit:
        color = "red"
    elif value >= warn:
        color = "yellow"
    else:
        color = "green"
    return f"[{color}]{value:.1f}{unit}[/{color}]"


def display_broker_performance(timeframe: str = "1h") -> None:
    """Render the Kafka Broker Performance view."""
    console.print()
    console.rule("[bold cyan]Kafka — Broker Performance[/bold cyan]")
    console.print()

    console.print("[dim]Collecting resource metrics (may take ~1 second for CPU sampling)…[/dim]")
    k4 = collect_k4()
    k5 = collect_k5()
    k6 = collect_k6()
    broker_infos = _fetch_broker_info()
    broker_count = _fetch_broker_count()
    console.print()

    # ── Broker identity panel ─────────────────────────────────────────────────
    if broker_infos:
        id_parts = [
            f"[bold white]Broker {b['id']}[/bold white]  [dim]{b['address']}[/dim]"
            for b in broker_infos
        ]
        broker_label = (
            f"[green]{broker_count} broker(s) active[/green]"
            if broker_count > 0 else "[red]0 brokers active[/red]"
        )
        console.print(Panel(
            "  " + "   ".join(id_parts) + "   " + broker_label,
            title="[bold]Broker Identity[/bold]",
            title_align="left",
            border_style="cyan",
            expand=False,
        ))
    else:
        no_info = "[dim]Broker identity unavailable from Prometheus.[/dim]"
        bcount = (
            f"[green]{broker_count} broker(s) reported active[/green]"
            if broker_count > 0 else "[red]No brokers detected[/red]"
        )
        console.print(Panel(
            f"  {no_info}   {bcount}",
            title="[bold]Broker Identity[/bold]",
            title_align="left",
            border_style="cyan",
            expand=False,
        ))
    console.print()

    # ── Resource metrics table ────────────────────────────────────────────────
    cpu_pct = k4.get("cpu_pct")
    mem_pct = k5.get("rss_pct")
    disk_pct   = k6.get("kafka_pct")
    rss_mb     = k5.get("rss_mb")
    kafka_gb   = k6.get("kafka_gb")
    root_gb    = k6.get("root_total_gb")
    disk_state = k6.get("state", "—")
    pid        = k4.get("pid") or k5.get("pid")
    proc_status = k4.get("status", "—")

    table = Table(
        box=box.ROUNDED,
        show_header=True,
        header_style="bold cyan",
        expand=True,
    )
    table.add_column("Metric",     style="bold white", ratio=1)
    table.add_column("Value",      width=28, justify="right")
    table.add_column("Score /100", width=12, justify="right")
    table.add_column("Bar",        width=22)
    table.add_column("St",         width=4,  justify="center")
    table.add_column("Detail",     ratio=2)

    # K4 CPU
    cpu_score = k4.get("value")
    cpu_color = score_color(cpu_score, "K4")
    cpu_detail = (
        f"PID [dim]{pid}[/dim]  process [dim]{proc_status}[/dim]"
        if pid else f"[dim]{proc_status}[/dim]"
    )
    table.add_row(
        "K4  Kafka Process CPU",
        _status_str(cpu_pct, _CPU_WARN, _CPU_CRIT),
        f"[{cpu_color}]{fmt_score(cpu_score)}[/{cpu_color}]",
        f"[{cpu_color}]{score_bar(cpu_score, 20)}[/{cpu_color}]",
        status_icon(cpu_score, "K4"),
        cpu_detail,
    )

    # K5 Memory
    mem_score  = k5.get("value")
    mem_color  = score_color(mem_score, "K5")
    total_ram  = k5.get("total_ram_gb")
    mem_detail = (
        f"RSS [dim]{rss_mb} MB[/dim]  of [dim]{total_ram} GB[/dim] system RAM"
        if rss_mb is not None and total_ram is not None
        else f"[dim]{k5.get('status', '—')}[/dim]"
    )
    table.add_row(
        "K5  Kafka Process Memory",
        _status_str(mem_pct, _MEM_WARN, _MEM_CRIT),
        f"[{mem_color}]{fmt_score(mem_score)}[/{mem_color}]",
        f"[{mem_color}]{score_bar(mem_score, 20)}[/{mem_color}]",
        status_icon(mem_score, "K5"),
        mem_detail,
    )

    # K6 Disk
    disk_score = k6.get("value")
    disk_color = score_color(disk_score, "K6")
    disk_detail = (
        f"[dim]{kafka_gb} GB[/dim] Kafka  /  [dim]{root_gb} GB[/dim] root  "
        f"state [dim]{disk_state}[/dim]"
        if kafka_gb is not None and root_gb is not None
        else f"[dim]{k6.get('error', '—')}[/dim]"
    )
    table.add_row(
        "K6  Kafka Total Disk",
        _status_str(disk_pct, _DISK_WARN, _DISK_CRIT),
        f"[{disk_color}]{fmt_score(disk_score)}[/{disk_color}]",
        f"[{disk_color}]{score_bar(disk_score, 20)}[/{disk_color}]",
        status_icon(disk_score, "K6"),
        disk_detail,
    )

    console.print(table)
    console.print()

    # ── Disk breakdown sub-panel ──────────────────────────────────────────────
    if k6.get("kafka_pct") is not None:
        breakdown = (
            f"  [bold]Logs dir:[/bold]  "
            f"messages [dim]{k6.get('msg_gb', 0)} GB[/dim]  "
            f"indexes [dim]{k6.get('index_gb', 0)} GB[/dim]  "
            f"KRaft meta [dim]{k6.get('kraft_gb', 0)} GB[/dim]  "
            f"app logs [dim]{k6.get('applog_gb', 0)} GB[/dim]\n"
            f"  [bold]Static:[/bold]    "
            f"libs [dim]{k6.get('libs_gb', 0)} GB[/dim]  "
            f"bin [dim]{k6.get('bin_gb', 0)} GB[/dim]  "
            f"config [dim]{k6.get('config_gb', 0)} GB[/dim]  "
            f"other [dim]{k6.get('other_gb', 0)} GB[/dim]"
        )
        console.print(Panel(
            breakdown,
            title="[bold]Disk Breakdown — Kafka Installation[/bold]",
            title_align="left",
            border_style="cyan",
            expand=True,
        ))
        console.print()

    # ── Issue summary & diagnostic ────────────────────────────────────────────
    issues: list[str] = []
    if cpu_pct is not None:
        if cpu_pct >= _CPU_CRIT:
            issues.append(f"[red]✗[/red] CPU critically high ({cpu_pct:.1f}%)")
        elif cpu_pct >= _CPU_WARN:
            issues.append(f"[yellow]⚠[/yellow] CPU elevated ({cpu_pct:.1f}%)")
    if mem_pct is not None:
        if mem_pct >= _MEM_CRIT:
            issues.append(f"[red]✗[/red] Memory critically high ({mem_pct:.1f}% of system RAM)")
        elif mem_pct >= _MEM_WARN:
            issues.append(f"[yellow]⚠[/yellow] Memory elevated ({mem_pct:.1f}% of system RAM)")
    if disk_pct is not None:
        if disk_pct >= _DISK_CRIT:
            issues.append(f"[red]✗[/red] Disk critically high ({disk_pct:.1f}%) — state: {disk_state}")
        elif disk_pct >= _DISK_WARN:
            issues.append(f"[yellow]⚠[/yellow] Disk elevated ({disk_pct:.1f}%) — state: {disk_state}")
    if proc_status and "NOT FOUND" in proc_status.upper():
        issues.append("[red]✗[/red] Kafka process not found — broker may be down")

    if issues:
        for issue in issues:
            console.print(f"  {issue}")
        console.print()
        diag_text = _plain_english_diagnostic(cpu_pct, mem_pct, disk_pct)
        border = "red" if any("✗" in i for i in issues) else "cyan"
        console.print(Panel(
            f"  {diag_text}",
            title="[bold]Diagnosis[/bold]",
            title_align="left",
            border_style=border,
            expand=True,
        ))
    else:
        console.print("  [green]✓ Broker resources within normal operating range.[/green]")

    console.print()