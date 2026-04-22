"""
View 1 — Quick Summary

The most important view: a 10-second health check for admins.
Shows cluster health, resource averages, index activity, and shard status
with plain English warnings.
"""

import datetime

from rich.panel import Panel
from rich.table import Table
from rich import box

from monitor.config import console, CPU_WARN, CPU_CRIT, HEAP_WARN, HEAP_CRIT, DISK_WARN, DISK_CRIT
from monitor.client import (
    fetch_cluster_health,
    fetch_cluster_stats,
    fetch_node_stats_for_timeframe,
    fetch_disk_allocation,
    fetch_indices,
    fetch_shards,
)
from monitor.utils import format_bytes, parse_size_string, status_symbol, cluster_status_symbol, cluster_status_styled


def display_quick_summary(timeframe: str = "1h"):
    """Render the Quick Summary view."""
    now = datetime.datetime.now().strftime("%H:%M")

    console.print()
    console.rule(f"[bold cyan]OpenSearch — Quick Summary[/bold cyan]  [dim](as of {now})[/dim]")
    console.print()

    warnings = []

    # ── Cluster Health ────────────────────────────────────────────────
    health = fetch_cluster_health()
    if health:
        status = health.get("status", "unknown")
        num_nodes = health.get("number_of_nodes", 0)
        sym = cluster_status_symbol(status)

        console.print(Panel(
            f"  Status  : {cluster_status_styled(status)} {sym}\n"
            f"  Nodes   : {num_nodes} active",
            title="[bold]Cluster Health[/bold]",
            title_align="left",
            border_style="cyan",
            expand=False,
        ))

        if status.lower() == "yellow":
            warnings.append("[yellow]⚠[/yellow]  Cluster is YELLOW — some replica shards are missing. Data is accessible but redundancy is reduced.")
        elif status.lower() == "red":
            warnings.append("[red]✗[/red]  Cluster is RED — some primary shards are unassigned. Data may be unavailable.")
    else:
        console.print("[red]Could not retrieve cluster health.[/red]")

    # ── Resources (via /_cluster/stats — pre-aggregated by OpenSearch) ──────────
    # One API call gives us cluster-wide CPU, JVM heap, OS RAM, and disk totals.
    # node_stats is fetched separately only to generate per-node targeted warnings.
    cluster_stats = fetch_cluster_stats()
    node_stats = fetch_node_stats_for_timeframe(timeframe=timeframe)
    disk_alloc = fetch_disk_allocation()

    # Cluster-wide totals — OpenSearch pre-aggregates these across all nodes
    avg_cpu = 0
    heap_used_total = heap_max_total = 0
    mem_used_total = mem_total_total = 0
    disk_used_total = disk_total_total = 0

    if cluster_stats:
        cs_nodes = cluster_stats.get("nodes", {})

        avg_cpu = cs_nodes.get("os", {}).get("cpu", {}).get("percent", 0)

        os_mem = cs_nodes.get("os", {}).get("mem", {})
        mem_used_total = os_mem.get("used_in_bytes", 0)
        mem_total_total = os_mem.get("total_in_bytes", 0)

        jvm_mem = cs_nodes.get("jvm", {}).get("mem", {})
        heap_used_total = jvm_mem.get("heap_used_in_bytes", 0)
        heap_max_total = jvm_mem.get("heap_max_in_bytes", 0)

        fs = cs_nodes.get("fs", {})
        fs_total = fs.get("total_in_bytes", 0)
        fs_available = fs.get("available_in_bytes", 0)
        disk_used_total = fs_total - fs_available
        disk_total_total = fs_total

    # Per-node breakdowns — used only to generate targeted warnings below
    node_cpu_details = []
    node_heap_details = []

    if node_stats and "nodes" in node_stats:
        for node_id, node in node_stats["nodes"].items():
            node_name = node.get("name", node_id[:8])

            cpu_pct = node.get("os", {}).get("cpu", {}).get("percent", 0)
            node_cpu_details.append((node_name, cpu_pct))

            jvm_info = node.get("jvm", {}).get("mem", {})
            heap_used = jvm_info.get("heap_used_in_bytes", 0)
            heap_max = jvm_info.get("heap_max_in_bytes", 0)
            if heap_max > 0:
                node_heap_details.append((node_name, (heap_used / heap_max) * 100))

    node_disk_details = []
    if disk_alloc:
        for entry in disk_alloc:
            node_name = entry.get("node", "unknown")
            disk_used = parse_size_string(entry.get("disk.used", "0"))
            disk_total = parse_size_string(entry.get("disk.total", "0"))
            if disk_total > 0:
                node_disk_details.append((node_name, (disk_used / disk_total) * 100))

    cpu_sym = status_symbol(avg_cpu, CPU_WARN, CPU_CRIT)
    heap_pct_total = (heap_used_total / heap_max_total * 100) if heap_max_total > 0 else 0
    heap_sym = status_symbol(heap_pct_total, HEAP_WARN, HEAP_CRIT)
    disk_sym = status_symbol(
        (disk_used_total / disk_total_total * 100) if disk_total_total > 0 else 0,
        DISK_WARN, DISK_CRIT,
    )

    resources_text = (
        f"  CPU        : {avg_cpu:.0f}%                       {cpu_sym}\n"
        f"  JVM Heap   : {format_bytes(heap_used_total)} / {format_bytes(heap_max_total)}   {heap_sym}\n"
        f"  System RAM : {format_bytes(mem_used_total)} / {format_bytes(mem_total_total)}"
        f"   [dim](normal — OpenSearch uses OS RAM as cache)[/dim]\n"
        f"  Disk       : {format_bytes(disk_used_total)} / {format_bytes(disk_total_total)} {disk_sym}"
    )

    console.print(Panel(
        resources_text,
        title="[bold]Resources (cluster-wide)[/bold]",
        title_align="left",
        border_style="cyan",
        expand=False,
    ))

    # Generate per-node resource warnings
    for node_name, cpu_pct in node_cpu_details:
        if cpu_pct >= CPU_CRIT:
            warnings.append(f"[red]✗[/red]  CPU is at {cpu_pct}% on {node_name} — critically high, investigate immediately.")
        elif cpu_pct >= CPU_WARN:
            warnings.append(f"[yellow]⚠[/yellow]  CPU is at {cpu_pct}% on {node_name} — consider checking running tasks.")

    for node_name, heap_pct in node_heap_details:
        if heap_pct >= HEAP_CRIT:
            warnings.append(f"[red]✗[/red]  JVM Heap is at {heap_pct:.0f}% on {node_name} — critically high, risk of OutOfMemory.")
        elif heap_pct >= HEAP_WARN:
            warnings.append(f"[yellow]⚠[/yellow]  JVM Heap is at {heap_pct:.0f}% on {node_name} — consider increasing heap or reducing load.")

    for node_name, disk_pct in node_disk_details:
        if disk_pct >= DISK_CRIT:
            warnings.append(f"[red]✗[/red]  Disk is at {disk_pct:.0f}% on {node_name} — critically full, free space urgently.")
        elif disk_pct >= DISK_WARN:
            warnings.append(f"[yellow]⚠[/yellow]  Disk is at {disk_pct:.0f}% on {node_name} — consider cleaning old indices soon.")

    # ── Index Activity ────────────────────────────────────────────────
    indices = fetch_indices()

    # Doc count, indexing ops, and search queries re-use the already-fetched cluster_stats (no extra API call)
    cs_indices = cluster_stats.get("indices", {}) if cluster_stats else {}
    total_docs = cs_indices.get("docs", {}).get("count", 0)
    index_ops_total = cs_indices.get("indexing", {}).get("index_total", 0)
    query_ops_total = cs_indices.get("search", {}).get("query_total", 0)

    if indices:
        total_indices = len(indices)
        total_data = sum(parse_size_string(idx.get("store.size", "0")) for idx in indices)

        largest_name = indices[0].get("index", "—")
        largest_size = format_bytes(parse_size_string(indices[0].get("store.size", "0")))

        console.print(Panel(
            f"  Total indices  : {total_indices}\n"
            f"  Total documents: {total_docs:,}\n"
            f"  Total data     : {format_bytes(total_data)}\n"
            f"  Indexing ops   : {index_ops_total:,}  [dim](cumulative — 0 means idle / no active writes)[/dim]\n"
            f"  Search queries : {query_ops_total:,}  [dim](cumulative — 0 means no queries have run)[/dim]\n"
            f"  Largest index  : {largest_name} ({largest_size})",
            title="[bold]Index Activity[/bold]",
            title_align="left",
            border_style="cyan",
            expand=False,
        ))
    else:
        console.print(Panel(
            "  No index data available.",
            title="[bold]Index Activity[/bold]",
            title_align="left",
            border_style="cyan",
            expand=False,
        ))

    # ── Shards ─────────────────────────────────────────────────────
    all_shards = fetch_shards()
    if all_shards:
        active_count = sum(1 for s in all_shards if s.get("state", "").upper() == "STARTED")
        unassigned_count = sum(1 for s in all_shards if s.get("state", "").upper() == "UNASSIGNED")
        relocating_count = sum(1 for s in all_shards if s.get("state", "").upper() == "RELOCATING")
        initializing_count = sum(1 for s in all_shards if s.get("state", "").upper() == "INITIALIZING")

        unassigned_sym = "[green]✓[/green]" if unassigned_count == 0 else "[red]✗[/red]"

        shard_text = f"  Active     : {active_count}\n"
        if relocating_count > 0:
            shard_text += f"  Relocating : {relocating_count}\n"
        if initializing_count > 0:
            shard_text += f"  Initializing: {initializing_count}\n"
        shard_text += f"  Unassigned : {unassigned_count}  {unassigned_sym}"

        console.print(Panel(
            shard_text,
            title="[bold]Shards[/bold]",
            title_align="left",
            border_style="cyan",
            expand=False,
        ))

        if unassigned_count > 0:
            warnings.append(f"[red]✗[/red]  {unassigned_count} unassigned shard(s) detected — data redundancy may be affected.")
    else:
        console.print(Panel(
            "  No shard data available.",
            title="[bold]Shards[/bold]",
            title_align="left",
            border_style="cyan",
            expand=False,
        ))

    # ── Warnings Footer ──────────────────────────────────────────────
    if warnings:
        console.print()
        console.rule("[bold yellow]Alerts[/bold yellow]")
        for w in warnings:
            console.print(f"  {w}")
    else:
        console.print()
        console.print("  [green]✓  All systems healthy — no issues detected.[/green]")

    console.print()
