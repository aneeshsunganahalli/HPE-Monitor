"""
CLI entry point for the OpenSearch Monitor.

Uses click for flag parsing:
    --timeframe  : real-time, 1h, 6h, 24h, 7d  (default: 1h)
  --watch      : auto-refresh interval in seconds (default: off)
  --summary    : jump straight to Quick Summary
  --service    : opensearch (default), kafka/logstash stubbed as coming soon
"""

import sys
import time
import datetime
import re

import click

from monitor.config import console
from monitor.menus import main_service_menu, opensearch_menu
from monitor.Opensearch.views.quick_summary import display_quick_summary
from monitor.utils import press_enter_to_return


@click.command()
@click.option(
    "--timeframe",
    default="1h",
    show_default=True,
    help="Time window for metric routing (real-time, 30m, 6h, 2d). Default: 1h.",
)
@click.option(
    "--watch",
    type=int,
    default=None,
    help="Auto-refresh interval in seconds. When set, the selected view refreshes on this interval.",
)
@click.option(
    "--summary",
    is_flag=True,
    default=False,
    help="Skip the menu and jump straight to Quick Summary.",
)
@click.option(
    "--service",
    type=click.Choice(["opensearch", "kafka", "logstash"], case_sensitive=False),
    default=None,
    help="Service to monitor. Omit to see the service selector menu.",
)
@click.option(
    "--query",
    default="*",
    help="Query string for log search. Use with Log Browser.",
)
@click.option(
    "--level",
    type=click.Choice(["error", "warn", "info", "debug", "critical"], case_sensitive=False),
    default=None,
    help="Log level filter.",
)
@click.option(
    "--spike-ts",
    default=None,
    help="ISO timestamp for Root Cause Analysis (e.g., 2026-03-20T10:00:00).",
)
def cli(timeframe, watch, summary, service, query, level, spike_ts):
    """OpenSearch Cluster Monitor — a terminal-based health checker."""

    # Validate --timeframe format (real-time or number + m/h/d)
    if not re.fullmatch(r"(real-time|\d+[mhd])", timeframe, re.IGNORECASE):
        raise click.BadParameter(
            f"'{timeframe}' is not a valid timeframe. "
            "Use 'real-time' or a number followed by m (minutes), h (hours), or d (days). "
            "Examples: real-time, 30m, 6h, 24h, 7d",
            param_hint="'--timeframe'",
        )
    timeframe = timeframe.lower()

    # Handle coming-soon services
    if service in ("kafka", "logstash"):
        console.print(f"\n[yellow]⚠  {service.title()} monitoring is coming soon.[/yellow]")
        sys.exit(0)

    # --summary flag: jump straight to Quick Summary
    if summary:
        if watch:
            _watch_loop(display_quick_summary, watch, timeframe=timeframe)
        else:
            display_quick_summary(timeframe=timeframe)
            press_enter_to_return()
        return

    # --watch without --summary: show menu once to pick a view, then loop it
    if watch:
        from monitor.menus import (
            OPENSEARCH_VIEWS, MENU_CURSOR, MENU_CURSOR_STYLE, MENU_HIGHLIGHT_STYLE,
        )
        from rich.panel import Panel
        from simple_term_menu import TerminalMenu

        console.clear()
        console.print()
        console.print(Panel.fit(
            "[bold cyan]OpenSearch Monitor — Watch Mode[/bold cyan]\n"
            f"[dim]Select a view to auto-refresh every {watch}s[/dim]",
            border_style="cyan",
        ))
        console.print()

        watch_options = [label for label, _ in OPENSEARCH_VIEWS]
        menu = TerminalMenu(
            watch_options,
            menu_cursor=MENU_CURSOR,
            menu_cursor_style=MENU_CURSOR_STYLE,
            menu_highlight_style=MENU_HIGHLIGHT_STYLE,
        )
        choice = menu.show()

        if choice is None:
            return

        label, view_fn = OPENSEARCH_VIEWS[choice]
        # Prepare watch arguments
        watch_args = {"timeframe": timeframe}
        if label == "Log Browser":
            watch_args.update({"query_str": query, "level": level})
        elif label == "Root Cause Analysis":
            watch_args = {"spike_ts": spike_ts}

        _watch_loop(view_fn, watch, **watch_args)
        return

    # Default routing:
    #   --service opensearch   → go directly to the OpenSearch menu
    #   no --service flag      → show the top-level service selector
    if service == "opensearch":
        opensearch_menu(timeframe=timeframe, query=query, level=level, spike_ts=spike_ts)
    else:
        main_service_menu(timeframe=timeframe, query=query, level=level, spike_ts=spike_ts)


def _watch_loop(view_fn, interval: int, **kwargs):
    """
    Watch mode: clear → render view → show timestamp → sleep → repeat.
    Catches KeyboardInterrupt for clean exit.
    """
    try:
        while True:
            console.clear()
            view_fn(**kwargs)
            now = datetime.datetime.now().strftime("%H:%M:%S")
            console.print(
                f"\n[dim]Last updated: {now} — refreshing in {interval}s  "
                f"(Ctrl+C to stop)[/dim]"
            )
            time.sleep(interval)
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped.[/yellow]")


if __name__ == "__main__":
    cli()
