"""
View 4 — Node Performance

Per-node table showing CPU, memory, and disk usage with
green/yellow/red status indicators, plus bottleneck diagnostics.
"""

from __future__ import annotations

from typing import Any

from rich.panel import Panel
from rich.table import Table
from rich import box

from monitor.config import (
    console,
    CPU_WARN, CPU_CRIT,
    HEAP_WARN, HEAP_CRIT,
    DISK_WARN, DISK_CRIT,
)
from monitor.client import fetch_bottleneck_metrics, fetch_node_stats_for_timeframe
from monitor.utils import format_bytes, status_symbol, status_color


PA_IO_WAIT_WARN = 20.0


def _format_signal(value: float | None, unit: str | None = "%") -> str:
    """Format optional diagnostic metrics for table display."""
    if value is None:
        return "[dim]n/a[/dim]"
    if unit:
        return f"{value:.1f}{unit}"
    return f"{value:.1f}"


def _plain_english_diagnostic(
    cpu_pct: float,
    disk_pct: float,
    disk_util: float | None,
    io_wait: float | None,
) -> str:
    """Translate raw bottleneck metrics into admin-friendly explanations."""
    if io_wait is not None and io_wait >= PA_IO_WAIT_WARN and cpu_pct >= CPU_WARN:
        return (
            "CPU looks high, but the main issue is storage wait. "
            "IO_TotWait is elevated, so worker threads are blocked on disk I/O."
        )

    if disk_util is not None and disk_util >= DISK_WARN and disk_pct >= DISK_WARN:
        return (
            "Disk is the bottleneck. High Disk_Utilization indicates indexing/merge "
            "workloads are saturating storage bandwidth."
        )

    if cpu_pct >= CPU_WARN and disk_pct < DISK_WARN:
        return (
            "Node is compute-bound. Indexing/search workload is likely consuming most "
            "CPU cycles."
        )

    if disk_pct >= DISK_WARN:
        return (
            "Node is storage-bound. Disk pressure is rising and can increase both index "
            "and query latency."
        )

    return "Pressure is elevated, but the exact root cause is inconclusive from current telemetry."


def display_node_performance(timeframe: str = "1h"):
    """Render the Node Performance view."""
    console.print()
    console.rule("[bold cyan]OpenSearch — Node Performance[/bold cyan]")
    console.print()

    node_stats = fetch_node_stats_for_timeframe(timeframe=timeframe)

    if not node_stats or "nodes" not in node_stats:
        console.print("[red]Could not retrieve node stats.[/red]")
        return

    # ── Build Table ───────────────────────────────────────────
    table = Table(
        box=box.ROUNDED,
        show_header=True,
        header_style="bold cyan",
        expand=True,
    )
    table.add_column("Node", style="bold white", ratio=1)
    table.add_column("CPU", width=10, justify="right")
    table.add_column("", width=3, justify="center")  # CPU status
    table.add_column("JVM Heap", width=22, justify="right")
    table.add_column("", width=3, justify="center")  # Heap status
    table.add_column("System RAM", width=22, justify="right")
    table.add_column("Disk (fs.total)", width=22, justify="right")
    table.add_column("", width=3, justify="center")  # Disk status

    issues = []
    indexing_details = []
    diagnostics: list[dict[str, Any]] = []

    for node_id, node in node_stats["nodes"].items():
        os_info = node.get("os", {})
        node_name = node.get("name", node_id[:8])

        # CPU
        cpu_pct = os_info.get("cpu", {}).get("percent", 0)
        cpu_col = status_color(cpu_pct, CPU_WARN, CPU_CRIT)
        cpu_sym = status_symbol(cpu_pct, CPU_WARN, CPU_CRIT)
        cpu_str = f"[{cpu_col}]{cpu_pct}%[/{cpu_col}]"

        # JVM Heap (the important memory metric for OpenSearch)
        jvm_info = node.get("jvm", {}).get("mem", {})
        heap_used = jvm_info.get("heap_used_in_bytes", 0)
        heap_max = jvm_info.get("heap_max_in_bytes", 0)
        heap_pct = (heap_used / heap_max * 100) if heap_max > 0 else 0
        heap_col = status_color(heap_pct, HEAP_WARN, HEAP_CRIT)
        heap_sym = status_symbol(heap_pct, HEAP_WARN, HEAP_CRIT)
        heap_str = f"[{heap_col}]{format_bytes(heap_used)} / {format_bytes(heap_max)}[/{heap_col}]"

        # System RAM (informational only — high usage is normal for OpenSearch)
        mem_info = os_info.get("mem", {})
        mem_used = mem_info.get("used_in_bytes", 0)
        mem_total = mem_info.get("total_in_bytes", 0)
        mem_str = f"[dim]{format_bytes(mem_used)} / {format_bytes(mem_total)}[/dim]"

        # Disk — use fs.total from node stats for exact watermark-accurate bytes
        fs_total_info = node.get("fs", {}).get("total", {})
        disk_total = fs_total_info.get("total_in_bytes", 0)
        disk_avail = fs_total_info.get("available_in_bytes", 0)
        disk_used = disk_total - disk_avail
        disk_pct = (disk_used / disk_total * 100) if disk_total > 0 else 0
        disk_col = status_color(disk_pct, DISK_WARN, DISK_CRIT)
        disk_sym = status_symbol(disk_pct, DISK_WARN, DISK_CRIT)
        disk_str = f"[{disk_col}]{format_bytes(disk_used)} / {format_bytes(disk_total)}[/{disk_col}]"

        table.add_row(node_name, cpu_str, cpu_sym, heap_str, heap_sym, mem_str, disk_str, disk_sym)

        # Per-node indexing and search stats
        node_indices = node.get("indices", {})
        index_total = node_indices.get("indexing", {}).get("index_total", 0)
        query_total = node_indices.get("search", {}).get("query_total", 0)

        # Collect issues for summary
        if cpu_pct >= CPU_CRIT:
            issues.append(f"[red]✗[/red]  {node_name} — critically high CPU ({cpu_pct}%)")
        elif cpu_pct >= CPU_WARN:
            issues.append(f"[yellow]⚠[/yellow]  {node_name} — elevated CPU ({cpu_pct}%)")

        if heap_pct >= HEAP_CRIT:
            issues.append(f"[red]✗[/red]  {node_name} — JVM Heap at {heap_pct:.0f}% — risk of OutOfMemory error")
        elif heap_pct >= HEAP_WARN:
            issues.append(f"[yellow]⚠[/yellow]  {node_name} — JVM Heap at {heap_pct:.0f}% — consider increasing heap or reducing load")

        if disk_pct >= DISK_CRIT:
            issues.append(f"[red]✗[/red]  {node_name} — critically full disk ({disk_pct:.0f}%)")
        elif disk_pct >= DISK_WARN:
            issues.append(f"[yellow]⚠[/yellow]  {node_name} — disk getting full ({disk_pct:.0f}%)")

        if cpu_pct >= CPU_WARN or disk_pct >= DISK_WARN:
            pa_metrics = fetch_bottleneck_metrics(node_name=node_name)
            disk_util = pa_metrics.get("disk_utilization")
            io_wait = pa_metrics.get("io_tot_wait")

            diagnostics.append(
                {
                    "node": node_name,
                    "severity": "critical" if (cpu_pct >= CPU_CRIT or disk_pct >= DISK_CRIT) else "warning",
                    "cpu": cpu_pct,
                    "disk": disk_pct,
                    "disk_util": disk_util,
                    "io_wait": io_wait,
                    "explanation": _plain_english_diagnostic(
                        cpu_pct=cpu_pct,
                        disk_pct=disk_pct,
                        disk_util=disk_util,
                        io_wait=io_wait,
                    ),
                }
            )

        indexing_details.append((node_name, index_total, query_total))

    console.print(table)
    console.print()

    # ── Indexing & Search Activity (per node) ────────────────────────
    act_table = Table(
        box=box.SIMPLE,
        show_header=True,
        header_style="bold cyan",
        expand=True,
    )
    act_table.add_column("Node",          style="bold white", ratio=1)
    act_table.add_column("Indexing Ops",  width=18, justify="right",
                         style="cyan",  header_style="bold cyan")
    act_table.add_column("Search Queries", width=18, justify="right",
                         style="magenta", header_style="bold magenta")

    for node_name, idx_ops, srch_ops in indexing_details:
        idx_str  = f"{idx_ops:,}"  if idx_ops  else "[dim]0[/dim]"
        srch_str = f"{srch_ops:,}" if srch_ops else "[dim]0[/dim]"
        act_table.add_row(node_name, idx_str, srch_str)

    console.print(Panel(
        act_table,
        title="[bold]Indexing & Search Activity[/bold]  [dim](cumulative totals)[/dim]",
        title_align="left",
        border_style="cyan",
        expand=True,
    ))
    console.print()

    # ── Summary ───────────────────────────────────────────────
    if issues:
        for issue in issues:
            console.print(f"  {issue}")
    else:
        console.print("  [green]✓  All nodes healthy — no performance concerns detected.[/green]")

    console.print()

    # ── Diagnostic (high-level first, drill-down second) ─────────────────────
    critical_count = sum(1 for item in diagnostics if item["severity"] == "critical")
    warning_count = len(diagnostics) - critical_count

    if diagnostics:
        console.print(Panel(
            f"  {len(diagnostics)} node(s) crossed CPU/Disk thresholds in this snapshot.\n"
            f"  Critical: {critical_count}    Warning: {warning_count}\n"
            "  Drill-down below explains likely root cause using Performance Analyzer metrics.",
            title="[bold]Diagnostic[/bold]",
            title_align="left",
            border_style="cyan",
            expand=True,
        ))

        diag_table = Table(
            box=box.SIMPLE,
            show_header=True,
            header_style="bold cyan",
            expand=True,
        )
        diag_table.add_column("Node", style="bold white", ratio=1)
        diag_table.add_column("Pressure", width=10, justify="center")
        diag_table.add_column("PA Signals", width=34)
        diag_table.add_column("Plain English Diagnosis", ratio=3)

        for item in diagnostics:
            pressure = "[red]critical[/red]" if item["severity"] == "critical" else "[yellow]warning[/yellow]"
            signals = (
                f"Disk_Utilization={_format_signal(item['disk_util'])}  "
                f"IO_TotWait={_format_signal(item['io_wait'], unit=None)}"
            )
            diag_table.add_row(
                str(item["node"]),
                pressure,
                signals,
                str(item["explanation"]),
            )

        console.print(diag_table)
    else:
        console.print(Panel(
            "  [green]No CPU or disk bottlenecks detected. Performance Analyzer drill-down is not required.[/green]",
            title="[bold]Diagnostic[/bold]",
            title_align="left",
            border_style="green",
            expand=True,
        ))

    console.print()
