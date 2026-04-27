from monitor.config import console
from monitor.kafka.collectors import collect_all, METRIC_META, GROUP_COLORS
from monitor.kafka.display_utils import score_color, score_bar, status_icon, fmt_score
from monitor.utils import press_enter_to_return
from rich.table import Table
from rich import box

def _key_value_str(key, m):
    """One-line human-readable raw value for the snapshot table."""
    if not isinstance(m, dict):
        return "N/A"
    try:
        if key == "K1":
            return f"{m.get('lag_secs','?')} s  ({m.get('lag_msgs','?')} msgs)"
        if key == "K2":
            return f"ratio = {m.get('ratio','?')}"
        if key == "K3":
            c = m.get("count", "?")
            return "✓ All in-sync" if c == 0 else f"⚠  {c} partition(s) at risk"
        if key == "K4":
            p = m.get("cpu_pct")
            return f"{p} %" if p is not None else m.get("status", "N/A")
        if key == "K5":
            r = m.get("rss_mb")
            p = m.get("rss_pct")
            return f"{r} MB  ({p} % of RAM)" if r is not None else m.get("status", "N/A")
        if key == "K6":
            return (f"{m.get('kafka_gb','?')} GB / {m.get('root_total_gb','?')} GB"
                    f"({m.get('kafka_pct','?')} %)  [{m.get('state','?')}]")
        if key in ("K7", "K8"):
            v = m.get("value")
            return f"{v:.3f} msgs/sec" if v is not None else "N/A"
        if key == "K9":
            return f"{m.get('count','?')} broker(s) active"
    except Exception:
        pass
    return "—"

def display_snapshot(timeframe: str = "1h"):
    """Print the full 9-metric Kafka snapshot table."""
    snap = collect_all()
    ts = snap.get("timestamp", "—")[:19].replace("T", " ")
    console.rule(
        f"[bold cyan]📊 Kafka Metrics Snapshot  [dim]{ts}[/dim][/bold cyan]"
    )

    table = Table(
        box=box.ROUNDED,
        header_style="bold cyan",
        expand=True,
        padding=(0, 1),
    )
    table.add_column("Group",      style="bold",    width=11)
    table.add_column("Metric",                      width=30)
    table.add_column("Score /100", justify="right", width=11)
    table.add_column("Bar (0─100)",                 width=22)
    table.add_column("St",         justify="center",width=4)
    table.add_column("Current Value",               ratio=1)

    current_group = None
    for key in ["K1","K2","K3","K4","K5","K6","K7","K8","K9"]:
        grp, name, _ = METRIC_META[key]
        gc           = GROUP_COLORS.get(grp, "white")
        m            = snap.get(key, {})
        score        = m.get("value") if isinstance(m, dict) else None
        color        = score_color(score, key)
        bar          = score_bar(score, 20)
        icon         = status_icon(score, key)
        raw          = _key_value_str(key, m)

        # K7/K8 are informational — score IS the raw rate, not a 0-100 index
        score_display = (f"{score:.3f}" if key in ("K7","K8") and score is not None
                         else fmt_score(score))

        if grp != current_group:
            current_group = grp
            table.add_row(
                f"[bold {gc}]{grp}[/bold {gc}]",
                "", "", "", "", "",
                style="on grey11"
            )

        table.add_row(
            "",
            f"[{gc}]{name}[/{gc}]",
            f"[{color}][bold]{score_display}[/bold][/{color}]",
            f"[{color}]{bar}[/{color}]" if key not in ("K7","K8") else "[dim]— rate —[/dim]",
            icon if key not in ("K7","K8") else "─",
            f"[dim]{raw}[/dim]",
        )

    console.print(table)
    console.print(
        "  [bold]Score scale:[/bold]  "
        "[green]🟢 0–50 SAFE[/green]   "
        "[yellow]🟡 50–75 WARNING[/yellow]   "
        "[red]🔴 75–100 CRITICAL[/red]   "
        "[dim](K3, K9 inverted: 100 = healthy)[/dim]\n"
    )