from monitor.config import console
from monitor.kafka.collectors import collect_broker_performance
from monitor.utils import press_enter_to_return
from rich.table import Table
from rich import box


def _broker_state(row: dict):
    up = row.get("up", 0) or 0
    req = row.get("request_rate", 0.0) or 0.0
    bin_rate = row.get("bytes_in", 0.0) or 0.0
    bout_rate = row.get("bytes_out", 0.0) or 0.0

    if up <= 0:
        return "DOWN", "red", "🔴"
    if req >= 10000 or bin_rate >= 10000000 or bout_rate >= 10000000:
        return "HOT", "red", "🔴"
    if req >= 3000 or bin_rate >= 1000000 or bout_rate >= 1000000:
        return "WARN", "yellow", "🟡"
    return "OK", "green", "🟢"


def display_broker_performance(timeframe: str = "1h"):
    rows = collect_broker_performance()

    console.rule("[bold cyan]🏗 Kafka Broker Performance[/bold cyan]")

    if not rows:
        console.print(
            "[yellow]No broker-level data found.[/yellow]\n"
            "[dim]Check Prometheus labels such as instance, broker id, and kafka exporter coverage.[/dim]"
        )
        press_enter_to_return()
        return

    table = Table(
        box=box.ROUNDED,
        header_style="bold white on dark_blue",
        expand=True,
        padding=(0, 1),
    )
    table.add_column("Broker", style="bold cyan", ratio=2)
    table.add_column("Up", justify="center", width=6)
    table.add_column("Bytes In/s", justify="right", width=14)
    table.add_column("Bytes Out/s", justify="right", width=14)
    table.add_column("Req/s", justify="right", width=12)
    table.add_column("Leaders", justify="right", width=10)
    table.add_column("State", justify="center", width=8)

    for row in rows:
        state, color, icon = _broker_state(row)
        up = "YES" if row.get("up", 0) else "NO"

        table.add_row(
            row.get("broker", "—"),
            f"[{'green' if up == 'YES' else 'red'}]{up}[/{'green' if up == 'YES' else 'red'}]",
            f"{row.get('bytes_in', 0):.3f}",
            f"{row.get('bytes_out', 0):.3f}",
            f"{row.get('request_rate', 0):.3f}",
            str(row.get("leader_partitions", 0)),
            f"[{color}]{icon} {state}[/{color}]",
        )

    console.print(table)
    console.print(
        "[dim]State rules: DOWN = broker not up; HOT/WARN = high request or traffic rates. "
        "Tune thresholds to match your cluster scale.[/dim]"
    )
    press_enter_to_return()