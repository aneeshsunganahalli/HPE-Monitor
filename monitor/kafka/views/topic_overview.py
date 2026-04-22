from monitor.config import console
from monitor.kafka.collectors import collect_topic_overview
from monitor.utils import press_enter_to_return
from rich.table import Table
from rich import box


def _topic_state(row: dict):
    urp = row.get("under_replicated", 0) or 0
    lag = row.get("lag_msgs", 0) or 0
    ratio = row.get("throughput_ratio", 1.0) or 1.0

    if urp > 0:
        return "REPL", "red", "🔴"
    if lag >= 10000 or ratio >= 2.0:
        return "HOT", "red", "🔴"
    if lag >= 1000 or ratio >= 1.2:
        return "WARN", "yellow", "🟡"
    return "OK", "green", "🟢"


def display_topic_overview(timeframe: str = "1h"):
    rows = collect_topic_overview()

    console.rule("[bold cyan]🧭 Kafka Topic Overview[/bold cyan]")

    if not rows:
        console.print(
            "[yellow]No topic-level data found.[/yellow]\n"
            "[dim]Check Prometheus, kafka_exporter labels, and topic activity.[/dim]"
        )
        press_enter_to_return()
        return

    table = Table(
        box=box.ROUNDED,
        header_style="bold white on dark_blue",
        expand=True,
        padding=(0, 1),
    )
    table.add_column("Topic", style="bold cyan", ratio=2)
    table.add_column("Produced/s", justify="right", width=12)
    table.add_column("Consumed/s", justify="right", width=12)
    table.add_column("Ratio", justify="right", width=8)
    table.add_column("Lag Msgs", justify="right", width=12)
    table.add_column("URP", justify="right", width=6)
    table.add_column("State", justify="center", width=8)

    for row in rows:
        state, color, icon = _topic_state(row)
        table.add_row(
            row.get("topic", "—"),
            f"{row.get('produced_rate', 0):.3f}",
            f"{row.get('consumed_rate', 0):.3f}",
            f"{row.get('throughput_ratio', 0):.2f}",
            f"{row.get('lag_msgs', 0):,}",
            str(row.get("under_replicated", 0)),
            f"[{color}]{icon} {state}[/{color}]",
        )

    console.print(table)
    console.print(
        "[dim]State rules: REPL = under-replicated partitions present; "
        "HOT = very high lag or throughput imbalance; WARN = moderate lag or drift.[/dim]"
    )
    press_enter_to_return()