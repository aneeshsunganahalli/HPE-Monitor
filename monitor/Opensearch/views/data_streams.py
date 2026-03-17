"""
View — Data Streams

Shows all OpenSearch Data Streams sorted by store size, with:
  - Disk usage per stream
  - Document count
  - Last data received (derived from maximum_timestamp)
  - A "pipeline stale?" warning if no new data has arrived recently
"""

import datetime

from rich.panel import Panel
from rich.table import Table
from rich import box

from monitor.config import console
from monitor.client import fetch_data_streams
from monitor.utils import format_bytes, parse_size_string


# How old a stream's latest document must be before we warn (minutes)
STALE_WARN_MINUTES = 60
STALE_CRIT_MINUTES = 240


def _format_age(ts_ms: int | None) -> tuple[str, str]:
    """
    Given a maximum_timestamp in milliseconds, return:
      (human-readable age string, rich color)
    Returns ("—", "dim") if timestamp is missing.
    """
    if not ts_ms:
        return "—", "dim"

    last_seen = datetime.datetime.fromtimestamp(ts_ms / 1000, tz=datetime.timezone.utc)
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    delta = now - last_seen
    total_minutes = int(delta.total_seconds() / 60)

    if total_minutes < 1:
        label = "< 1 min ago"
    elif total_minutes < 60:
        label = f"{total_minutes} min ago"
    elif total_minutes < 1440:
        hours = total_minutes // 60
        mins = total_minutes % 60
        label = f"{hours}h {mins}m ago" if mins else f"{hours}h ago"
    else:
        days = total_minutes // 1440
        label = f"{days}d ago"

    if total_minutes >= STALE_CRIT_MINUTES:
        color = "red"
    elif total_minutes >= STALE_WARN_MINUTES:
        color = "yellow"
    else:
        color = "green"

    return label, color


def display_data_streams(timeframe: str = "1h"):
    """Render the Data Streams view."""
    console.print()
    console.rule("[bold cyan]OpenSearch — Data Streams[/bold cyan]")
    console.print()

    result = fetch_data_streams()
    streams = result.get("data_streams", []) if result else []

    if not streams:
        console.print(Panel(
            "  No data streams found.\n"
            "  [dim]Data streams are used for time-series data (logs, metrics, traces).\n"
            "  If you are using regular indices instead, see Index Deep Dive.[/dim]",
            title="[bold]Data Streams[/bold]",
            title_align="left",
            border_style="cyan",
            expand=False,
        ))
        console.print()
        return

    # ── Sort by store size descending ────────────────────────
    def _size_bytes(s: dict) -> int:
        raw = s.get("store_size") or s.get("store_size_bytes", "0")
        if isinstance(raw, int):
            return raw
        return int(parse_size_string(str(raw)))

    streams.sort(key=_size_bytes, reverse=True)

    # ── Summary panel ─────────────────────────────────────────
    total_size = sum(_size_bytes(s) for s in streams)

    console.print(Panel(
        f"  Total streams : {len(streams)}\n"
        f"  Total size    : {format_bytes(total_size)}",
        title="[bold]Data Streams Summary[/bold]",
        title_align="left",
        border_style="cyan",
        expand=False,
    ))
    console.print()

    # ── Per-stream table ──────────────────────────────────────
    table = Table(
        box=box.ROUNDED,
        show_header=True,
        header_style="bold cyan",
        expand=True,
    )
    table.add_column("#",               style="dim",         width=4,  justify="right")
    table.add_column("Stream Name",     style="bold white",  ratio=2)
    table.add_column("Size",            style="yellow",      width=12, justify="right")
    table.add_column("Backing Indices", style="dim",         width=10, justify="center")
    table.add_column("Last Data",       width=18,            justify="right")
    table.add_column("Status",          width=4,             justify="center")

    pipeline_warnings = []

    for i, stream in enumerate(streams, 1):
        name   = stream.get("name", "—")
        size   = format_bytes(_size_bytes(stream))
        ts_ms  = stream.get("maximum_timestamp")
        backing = len(stream.get("indices", []))

        age_label, age_color = _format_age(ts_ms)
        age_display = f"[{age_color}]{age_label}[/{age_color}]"

        if age_color == "red":
            sym = "[red]✗[/red]"
            pipeline_warnings.append(
                f"[red]✗[/red]  [bold]{name}[/bold] — last data received "
                f"[red]{age_label}[/red]. Pipeline (Logstash/Kafka) may have stopped."
            )
        elif age_color == "yellow":
            sym = "[yellow]⚠[/yellow]"
            pipeline_warnings.append(
                f"[yellow]⚠[/yellow]  [bold]{name}[/bold] — last data received "
                f"[yellow]{age_label}[/yellow]. Monitor the ingestion pipeline."
            )
        else:
            sym = "[green]✓[/green]"

        table.add_row(str(i), name, size, str(backing), age_display, sym)

    console.print(table)
    console.print()

    # ── Pipeline warnings ─────────────────────────────────────
    if pipeline_warnings:
        console.rule("[bold yellow]Pipeline Alerts[/bold yellow]")
        console.print()
        for w in pipeline_warnings:
            console.print(f"  {w}")
        console.print()
        console.print(Panel(
            "  [dim]OpenSearch itself is healthy — these warnings mean the upstream\n"
            "  pipeline (Logstash, Kafka, Beats) has slowed down or stopped sending\n"
            "  data to this stream. OpenSearch cannot fix this; check your pipeline.[/dim]",
            title="[bold cyan]What to check[/bold cyan]",
            title_align="left",
            border_style="cyan",
            expand=False,
        ))
    else:
        console.print("  [green]✓  All streams are receiving data — pipelines look healthy.[/green]")

    console.print()
