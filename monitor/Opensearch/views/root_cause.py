"""
View 9 — Root Cause Analysis (RCA)

Diagnostic view that takes a timestamp and correlates log patterns.
Consolidated from opensearch.py's spike root cause logic.
"""

import datetime
from rich.panel import Panel
from rich.table import Table
from rich import box
from monitor.config import console, LOG_COLORS, ROOT_CAUSE_PATTERNS
from monitor.client import fetch_logs_for_spike


def display_root_cause_analysis(spike_ts: str = None, window_min: int = 5):
    """Render the Root Cause Analysis view."""
    now = datetime.datetime.now().strftime("%H:%M")

    console.print()
    console.rule(f"[bold red]OpenSearch — Root Cause Analysis[/bold red] [dim](as of {now})[/dim]")
    console.print()

    if not spike_ts:
        console.print(Panel(
            "  Please provide a spike timestamp to begin analysis.\n"
            "  Use [bold yellow]--spike-ts ISO-TIMESTAMP[/bold yellow] in the CLI.",
            title="[bold yellow]RCA Required[/bold yellow]",
            title_align="left",
            border_style="yellow",
            expand=False,
        ))
        return

    try:
        spike_dt = datetime.datetime.fromisoformat(spike_ts.replace("Z", ""))
        start = (spike_dt - datetime.timedelta(minutes=window_min)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end = (spike_dt + datetime.timedelta(minutes=window_min)).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        console.print(f"[red]Error parsing timestamp:[/red] {e}")
        return

    hits = fetch_logs_for_spike(start, end)

    if not hits:
        console.print(Panel(
            f"  No logs found in the {window_min}m window around {spike_ts}.\n"
            "  Verify connectivity and indexing state.",
            title="[bold red]Analysis Results[/bold red]",
            title_align="left",
            border_style="red",
            expand=False,
        ))
        return

    annotated = []
    seen = set()
    for h in hits:
        src = h["_source"]
        msg = src.get("message", "")
        if msg[:120] in seen:
            continue
        seen.add(msg[:120])
        
        reason = next(
            (label for kw, label in ROOT_CAUSE_PATTERNS if kw.lower() in msg.lower()),
            None
        )
        annotated.append({
            "ts": src.get("@timestamp", "")[:19].replace("T", " "),
            "host": src.get("hostname", "—"),
            "level": src.get("log", {}).get("level", "info").lower(),
            "msg": msg[:220],
            "reason": reason
        })

    annotated.sort(key=lambda x: (x["reason"] is None, x["ts"]))

    table = Table(box=box.SIMPLE, expand=True, show_header=True, header_style="bold red")
    table.add_column("Timestamp", style="dim", width=20)
    table.add_column("Level", width=10)
    table.add_column("Host", style="bold green", width=15)
    table.add_column("Diagnostic / Message")

    for a in annotated:
        col = LOG_COLORS.get(a["level"], "white")
        msg_display = f"{a['reason']}\n[dim]{a['msg']}[/dim]" if a["reason"] else a["msg"]
        
        table.add_row(
            a["ts"],
            f"[{col}]{a['level'].upper()}[/{col}]",
            a["host"],
            msg_display
        )

    console.print(table)
    console.print(f"\n[dim]Analyzed 100 hits around {spike_ts}. Found {len([a for a in annotated if a['reason']])} potential indicators.[/dim]")
