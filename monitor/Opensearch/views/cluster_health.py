"""
View 2 — Cluster Health

Detailed cluster health view with plain English explanations
of what YELLOW and RED mean for admins.
"""

from rich.panel import Panel
from rich.table import Table
from rich import box

from monitor.config import console
from monitor.client import fetch_cluster_health
from monitor.utils import cluster_status_styled, cluster_status_symbol


def display_cluster_health(timeframe: str = "1h"):
    """Render the Cluster Health view."""
    console.print()
    console.rule("[bold cyan]OpenSearch — Cluster Health[/bold cyan]")
    console.print()

    health = fetch_cluster_health()
    if not health:
        console.print("[red]Could not retrieve cluster health data.[/red]")
        return

    status = health.get("status", "unknown")
    num_nodes = health.get("number_of_nodes", 0)
    num_data_nodes = health.get("number_of_data_nodes", 0)
    active_shards = health.get("active_shards", 0)
    relocating = health.get("relocating_shards", 0)
    initializing = health.get("initializing_shards", 0)
    unassigned = health.get("unassigned_shards", 0)
    pending = health.get("number_of_pending_tasks", 0)
    active_primary = health.get("active_primary_shards", 0)

    # ── Summary Panel ─────────────────────────────────────────
    sym = cluster_status_symbol(status)
    console.print(Panel(
        f"  Cluster Status : {cluster_status_styled(status)} {sym}",
        border_style="cyan",
        expand=False,
    ))

    # ── Details Table ─────────────────────────────────────────
    table = Table(box=box.ROUNDED, show_header=True, header_style="bold cyan", expand=False)
    table.add_column("Metric", style="white", width=28)
    table.add_column("Value", style="bold", width=20, justify="right")

    table.add_row("Total Nodes", str(num_nodes))
    table.add_row("Data Nodes", str(num_data_nodes))
    table.add_row("Active Primary Shards", str(active_primary))
    table.add_row("Active Shards (total)", str(active_shards))
    table.add_row("Relocating Shards",
                  f"[yellow]{relocating}[/yellow]" if relocating > 0 else f"[green]{relocating}[/green]")
    table.add_row("Initializing Shards",
                  f"[yellow]{initializing}[/yellow]" if initializing > 0 else f"[green]{initializing}[/green]")
    table.add_row("Unassigned Shards",
                  f"[red]{unassigned}[/red]" if unassigned > 0 else f"[green]{unassigned}[/green]")
    table.add_row("Pending Tasks",
                  f"[yellow]{pending}[/yellow]" if pending > 0 else f"[green]{pending}[/green]")

    console.print(table)
    console.print()

    # ── Plain English Explanation ─────────────────────────────
    if status.lower() == "green":
        console.print(Panel(
            "[green]All primary and replica shards are assigned.[/green]\n"
            "Your cluster is fully operational with complete data redundancy.",
            title="[bold green]What this means[/bold green]",
            title_align="left",
            border_style="green",
            expand=False,
        ))
    elif status.lower() == "yellow":
        console.print(Panel(
            "[yellow]Some replica shards are not assigned to any node.[/yellow]\n"
            "Your data is still fully accessible and searchable, but you have\n"
            "reduced redundancy. If a node goes down, you might lose copies of\n"
            "some data. This is common in single-node clusters or when a node\n"
            "has recently been added/removed.",
            title="[bold yellow]What this means[/bold yellow]",
            title_align="left",
            border_style="yellow",
            expand=False,
        ))
    elif status.lower() == "red":
        console.print(Panel(
            "[red]Some primary shards are not assigned — data may be unavailable.[/red]\n"
            "Searches and writes to affected indices will fail or return partial\n"
            "results. This requires immediate attention. Common causes:\n"
            "  • A node has gone down and hasn't recovered\n"
            "  • Disk space is full on one or more nodes\n"
            "  • Shard allocation is disabled",
            title="[bold red]What this means[/bold red]",
            title_align="left",
            border_style="red",
            expand=False,
        ))

    console.print()
