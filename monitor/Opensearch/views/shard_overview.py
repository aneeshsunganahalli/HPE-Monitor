"""
View 5 — Shard Overview

Shows all shards grouped by state: started, relocating, initializing,
unassigned. Highlights unassigned shards in red with a plain English note.
"""

from rich.panel import Panel
from rich.table import Table
from rich import box

from monitor.config import console
from monitor.client import fetch_shards
from monitor.utils import format_bytes, parse_size_string


def display_shard_overview(timeframe: str = "1h"):
    """Render the Shard Overview view."""
    console.print()
    console.rule("[bold cyan]OpenSearch — Shard Overview[/bold cyan]")
    console.print()

    all_shards = fetch_shards()
    if not all_shards:
        console.print("[yellow]No shard data found.[/yellow]")
        return

    # Group shards by state
    groups = {
        "STARTED": [],
        "RELOCATING": [],
        "INITIALIZING": [],
        "UNASSIGNED": [],
    }
    other_shards = []

    for shard in all_shards:
        state = shard.get("state", "UNKNOWN").upper()
        if state in groups:
            groups[state].append(shard)
        else:
            other_shards.append(shard)

    # ── Summary counts ────────────────────────────────────────
    summary_parts = []
    for state, shards in groups.items():
        count = len(shards)
        if state == "UNASSIGNED" and count > 0:
            summary_parts.append(f"[red]{state}: {count}[/red]")
        elif state == "STARTED":
            summary_parts.append(f"[green]{state}: {count}[/green]")
        elif count > 0:
            summary_parts.append(f"[yellow]{state}: {count}[/yellow]")
        else:
            summary_parts.append(f"[dim]{state}: {count}[/dim]")

    console.print(Panel(
        "  " + "    ".join(summary_parts),
        title="[bold]Shard State Summary[/bold]",
        title_align="left",
        border_style="cyan",
        expand=False,
    ))
    console.print()

    # ── Display each group ────────────────────────────────────
    display_order = ["STARTED", "RELOCATING", "INITIALIZING", "UNASSIGNED"]

    for state in display_order:
        shards = groups[state]
        if not shards:
            continue

        is_unassigned = state == "UNASSIGNED"
        border = "red" if is_unassigned else "cyan"
        header_style = "bold red" if is_unassigned else "bold cyan"

        table = Table(
            box=box.ROUNDED,
            show_header=True,
            header_style=header_style,
            title=f"[bold]{state}[/bold] ({len(shards)} shards)",
            title_style="bold red" if is_unassigned else "bold white",
            expand=True,
            border_style=border if is_unassigned else None,
        )
        table.add_column("Index", style="red" if is_unassigned else "white", ratio=2)
        table.add_column("Shard", width=8, justify="center")
        table.add_column("Type", width=10, justify="center")
        table.add_column("Node", style="yellow", ratio=1)
        table.add_column("Size", width=14, justify="right")
        table.add_column("Docs", width=14, justify="right")

        for shard in shards:
            index = shard.get("index", "—")
            shard_num = shard.get("shard", "—")
            prirep = "Primary" if shard.get("prirep", "") == "p" else "Replica"
            node = shard.get("node") or "[red]unassigned[/red]"
            size_raw = shard.get("store") or "0"
            docs = shard.get("docs") or "0"

            size_display = format_bytes(parse_size_string(size_raw))
            try:
                docs_display = f"{int(docs):,}"
            except (ValueError, TypeError):
                docs_display = str(docs)

            if node is None or node == "null":
                node = "[red]unassigned[/red]"

            table.add_row(index, str(shard_num), prirep, node, size_display, docs_display)

        console.print(table)
        console.print()

    # ── Unassigned warning ────────────────────────────────────
    unassigned_count = len(groups["UNASSIGNED"])
    if unassigned_count > 0:
        console.print(Panel(
            f"[red]{unassigned_count} unassigned shard(s) detected[/red] — this may affect data\n"
            "redundancy. Check that all nodes are online and have sufficient disk space.\n"
            "If a node was recently removed, the cluster may need time to redistribute shards.",
            title="[bold red]Attention[/bold red]",
            title_align="left",
            border_style="red",
            expand=False,
        ))
    else:
        console.print("  [green]✓  All shards are properly assigned.[/green]")

    console.print()
