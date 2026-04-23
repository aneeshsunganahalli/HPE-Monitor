"""
Menu navigation for the OpenSearch CLI Monitor.

Provides the main service menu and the OpenSearch submenu
with arrow-key selection (Vite-style) using simple-term-menu.
"""

import sys

from rich.panel import Panel
from simple_term_menu import TerminalMenu

from monitor.config import console
from monitor.kafka.views.snapshot import display_snapshot
from monitor.kafka.views.resource_gauges import display_resource_gauges
from monitor.kafka.views.partition_overview import display_partition_overview
from monitor.kafka.views.topic_deep_dive import display_topic_deep_dive
from monitor.kafka.views.broker_performance import display_broker_performance
from monitor.kafka.views.kafka_log_browser    import display_kafka_log_browser

from monitor.Opensearch.views.quick_summary import display_quick_summary
from monitor.Opensearch.views.trends import display_trends
from monitor.Opensearch.views.cluster_health import display_cluster_health
from monitor.Opensearch.views.index_deep_dive import display_index_deep_dive
from monitor.Opensearch.views.node_performance import display_node_performance
from monitor.Opensearch.views.shard_overview import display_shard_overview
from monitor.Opensearch.views.data_streams import display_data_streams
from monitor.Opensearch.views.log_browser import display_log_browser
from monitor.Opensearch.views.root_cause import display_root_cause_analysis
from monitor.utils import press_enter_to_return


# ──────────────── Menu Style ───────────────────────────────────

MENU_CURSOR = "❯ "
MENU_CURSOR_STYLE = ("fg_cyan", "bold")
MENU_HIGHLIGHT_STYLE = ("fg_cyan", "bold")


# ──────────────── Main Service Menu ────────────────────────────

SERVICE_OPTIONS = [
    "OpenSearch",
    "Kafka",
    "Logstash       (coming soon)",
    "---",
    "All Services   (coming soon)",
    "---",
    "Exit",
]


def main_service_menu(timeframe: str = "1h", query: str = "*", level: str = None, spike_ts: str = None):
    """Show the top-level service selector and loop until exit."""
    while True:
        console.clear()
        console.print()
        console.print(Panel.fit(
            "[bold cyan]Cluster Monitor[/bold cyan]\n"
            "[dim]Use arrow keys, press Enter to select[/dim]",
            border_style="cyan",
        ))
        console.print()

        menu = TerminalMenu(
            SERVICE_OPTIONS,
            menu_cursor=MENU_CURSOR,
            menu_cursor_style=MENU_CURSOR_STYLE,
            menu_highlight_style=MENU_HIGHLIGHT_STYLE,
        )
        choice = menu.show()

        if choice is None or choice == 6:  # Escape or Exit
            console.print("[bold green]Goodbye![/bold green]")
            sys.exit(0)
        elif choice == 0:
            opensearch_menu(timeframe=timeframe, query=query, level=level, spike_ts=spike_ts)
        elif choice == 1:
            kafka_menu()
        elif choice in (2, 3, 4):
            console.print("\n[yellow]⚠  This service is coming soon.[/yellow]")
            press_enter_to_return()


# ──────────────── OpenSearch Submenu ───────────────────────────

OPENSEARCH_VIEWS = [
    ("Quick Summary",  display_quick_summary),
    ("Historical Trends", display_trends),
    ("Cluster Health", display_cluster_health),
    ("Index Deep Dive", display_index_deep_dive),
    ("Node Performance", display_node_performance),
    ("Shard Overview",  display_shard_overview),
    ("Log Browser",     display_log_browser),
    ("Root Cause Analysis", display_root_cause_analysis),
    ("Data Streams",    display_data_streams),
]


def opensearch_menu(timeframe: str = "1h", query: str = "*", level: str = None, spike_ts: str = None):
    """Show the OpenSearch monitoring submenu and loop until back."""
    while True:
        console.clear()
        console.print()
        console.print(Panel.fit(
            "[bold cyan]OpenSearch Monitor[/bold cyan]\n"
            "[dim]Use arrow keys, Enter to select[/dim]",
            border_style="cyan",
        ))
        console.print()

        view_labels = [label for label, _ in OPENSEARCH_VIEWS]
        menu_options = view_labels + [
            "---",
            "Back to Main Menu",
        ]

        menu = TerminalMenu(
            menu_options,
            menu_cursor=MENU_CURSOR,
            menu_cursor_style=MENU_CURSOR_STYLE,
            menu_highlight_style=MENU_HIGHLIGHT_STYLE,
        )
        choice = menu.show()

        # Escape or "Back to Main Menu" (last item)
        if choice is None or choice == len(menu_options) - 1:
            return

        # Separator — do nothing
        if menu_options[choice] == "---":
            continue

        # View selection
        if choice < len(OPENSEARCH_VIEWS):
            label, view_fn = OPENSEARCH_VIEWS[choice]
            console.clear()
            try:
                # Pass extra context to specific views
                if label == "Log Browser":
                    view_fn(timeframe=timeframe, query_str=query, level=level)
                elif label == "Root Cause Analysis":
                    view_fn(spike_ts=spike_ts)
                else:
                    view_fn(timeframe=timeframe)
            except Exception as e:
                console.print(f"\n[red]Error:[/red] {e}")
            press_enter_to_return()

# ──────────────── Kafka Submenu ───────────────────────────

KAFKA_VIEWS = {
    "1": display_snapshot,
    "2": display_resource_gauges,
    "3": display_topic_deep_dive,
    "4": display_partition_overview,
    "5": display_broker_performance,
    "6": display_kafka_log_browser,
}

def kafka_menu() -> None:
    """Kafka monitoring sub-menu."""
    options = [
        "1. Snapshot              — All 9 metrics at a glance",
        "2. Resource Gauges       — CPU / Memory / Disk panels",
        "3. Topic Deep Dive       — Per-topic offsets, lag, rates, graphs, retention",
        "4. Partition Overview    — Leader, ISR, under-replicated, lag per partition",
        "5. Broker Performance    — Broker info + resource data combined",
        "6. Log Browser           — Browse Kafka broker operational logs",
        "---",
        "Back",
    ]

    while True:
        console.clear()
        console.print()
        console.print(Panel.fit(
            "[bold yellow]Kafka Monitor[/bold yellow]\n"
            "[dim]Use arrow keys, Enter to select[/dim]",
            border_style="yellow",
        ))
        console.print()

        menu = TerminalMenu(
            options,
            menu_cursor=MENU_CURSOR,
            menu_cursor_style=("fg_yellow", "bold"),
            menu_highlight_style=("fg_yellow", "bold"),
        )
        choice = menu.show()

        # Escape or Back (last item)
        if choice is None or choice == len(options) - 1:
            return

        # Separator
        if options[choice] == "---":
            continue

        # View selection — keys are "1" through "6", choice is 0-indexed
        view_key = str(choice + 1)
        if view_key in KAFKA_VIEWS:
            console.clear()
            try:
                KAFKA_VIEWS[view_key]()
            except Exception as e:
                console.print(f"\n[red]Error:[/red] {e}")
            press_enter_to_return()