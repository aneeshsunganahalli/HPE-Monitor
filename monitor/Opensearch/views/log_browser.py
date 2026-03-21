"""
View 8 — Log Browser

Search and browse recent logs with level-based filtering.
Consolidated from opensearch.py's log search logic.
"""

import datetime
from rich.panel import Panel
from rich.table import Table
from rich import box
from monitor.config import console, LOG_COLORS
from monitor.client import search_logs
from monitor.utils import timeframe_to_minutes


def display_log_browser(timeframe: str = "1h", query_str: str = "*", level: str = None):
    """Render the Log Browser view."""
    now = datetime.datetime.now().strftime("%H:%M")
    minutes = timeframe_to_minutes(timeframe)

    console.print()
    console.rule(f"[bold cyan]OpenSearch — Log Browser[/bold cyan] [dim]({timeframe} window, as of {now})[/dim]")
    console.print()

    hits = search_logs(query_str=query_str, minutes=minutes, size=30, level=level)

    if not hits:
        console.print(Panel(
            f"  No logs found matching [bold cyan]'{query_str}'[/bold cyan] in the last {timeframe}.\n"
            "  Check your query string or timeframe.",
            title="[bold]Logs[/bold]",
            title_align="left",
            border_style="yellow",
            expand=False,
        ))
        return

    table = Table(box=box.SIMPLE, expand=True, show_header=True, header_style="bold cyan")
    table.add_column("Timestamp", style="dim", width=20)
    table.add_column("Level", width=12)
    table.add_column("Host", style="bold green", width=20)
    table.add_column("Message")

    for h in hits:
        src = h["_source"]
        lvl = src.get("log", {}).get("level", "info").lower()
        col = LOG_COLORS.get(lvl, "white")
        
        ts = src.get("@timestamp", "")[:19].replace("T", " ")
        host = src.get("hostname", "—")
        msg = src.get("message", "")[:200]
        
        table.add_row(
            ts,
            f"[{col}]{lvl.upper()}[/{col}]",
            host,
            msg
        )

    console.print(table)
    console.print(f"\n[dim]Showing 30 most recent hits for query: [bold cyan]{query_str}[/bold cyan][/dim]")
