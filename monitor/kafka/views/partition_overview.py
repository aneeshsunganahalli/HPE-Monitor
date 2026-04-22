from monitor.config import console
from monitor.kafka.collectors import collect_partition_overview, KAFKA_TOPIC
from monitor.utils import press_enter_to_return
from rich.table import Table
from rich import box


def _partition_state(row: dict):
    urp = row.get("under_replicated", 0) or 0
    lag = row.get("lag_msgs", 0) or 0
    ratio = row.get("throughput_ratio", 1.0) or 1.0
    replicas = row.get("replicas")
    isr = row.get("isr")

    if urp > 0:
        return "REPL", "red", "🔴"
    if replicas is not None and isr is not None and isr < replicas:
        return "ISR", "yellow", "🟡"
    if lag >= 5000 or ratio >= 2.0:
        return "HOT", "red", "🔴"
    if lag >= 500 or ratio >= 1.2:
        return "WARN", "yellow", "🟡"
    return "OK", "green", "🟢"


def display_partition_overview(topic: str = None, timeframe: str = "1h"):
    topic = topic or KAFKA_TOPIC
    rows = collect_partition_overview(topic=topic)

    console.rule(f"[bold cyan]🧩 Kafka Partition Overview — {topic}[/bold cyan]")

    if not rows:
        console.print(
            f"[yellow]No partition-level data found for topic [bold]{topic}[/bold].[/yellow]\n"
            "[dim]Check topic name, exporter metrics, and whether partition labels are present.[/dim]"
        )
        press_enter_to_return()
        return

    table = Table(
        box=box.ROUNDED,
        header_style="bold white on dark_blue",
        expand=True,
        padding=(0, 1),
    )
    table.add_column("Partition", justify="right", width=10, style="bold cyan")
    table.add_column("Leader", justify="right", width=8)
    table.add_column("Replicas", justify="right", width=8)
    table.add_column("ISR", justify="right", width=6)
    table.add_column("Produced/s", justify="right", width=12)
    table.add_column("Consumed/s", justify="right", width=12)
    table.add_column("Ratio", justify="right", width=8)
    table.add_column("Lag Msgs", justify="right", width=12)
    table.add_column("URP", justify="right", width=6)
    table.add_column("State", justify="center", width=8)

    for row in rows:
        state, color, icon = _partition_state(row)

        leader = row.get("leader")
        replicas = row.get("replicas")
        isr = row.get("isr")

        table.add_row(
            str(row.get("partition", "—")),
            str(leader) if leader is not None else "—",
            str(replicas) if replicas is not None else "—",
            str(isr) if isr is not None else "—",
            f"{row.get('produced_rate', 0):.3f}",
            f"{row.get('consumed_rate', 0):.3f}",
            f"{row.get('throughput_ratio', 0):.2f}",
            f"{row.get('lag_msgs', 0):,}",
            str(row.get("under_replicated", 0)),
            f"[{color}]{icon} {state}[/{color}]",
        )

    console.print(table)
    console.print(
        "[dim]State rules: REPL = under-replicated; ISR = in-sync replicas below replica count; "
        "HOT/WARN = lag or throughput skew on specific partitions.[/dim]"
    )
    press_enter_to_return()
