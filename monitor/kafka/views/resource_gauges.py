from monitor.config import console
from monitor.kafka.collectors import collect_all, METRIC_META, THRESHOLDS
from monitor.kafka.display_utils import score_color, fmt_score
from monitor.utils import press_enter_to_return
from rich.panel import Panel
from rich.text import Text
from rich.columns import Columns


def _key_value_str(key: str, m: dict) -> str:
    if not isinstance(m, dict):
        return "N/A"
    try:
        if key == "K4":
            p = m.get("cpu_pct")
            return f"{p} %" if p is not None else m.get("status", "N/A")
        if key == "K5":
            r = m.get("rss_mb")
            p = m.get("rss_pct")
            return f"{r} MB  ({p} % of RAM)" if r is not None else m.get("status", "N/A")
        if key == "K6":
            return (f"{m.get('kafka_gb','?')} GB / {m.get('root_total_gb','?')} GB "
                    f"({m.get('kafka_pct','?')} %)  [{m.get('state','?')}]")
    except Exception:
        pass
    return "—"


def _gauge_panel(key: str, m: dict) -> Panel:
    _, name, unit = METRIC_META[key]
    score      = m.get("value") if isinstance(m, dict) else None
    color      = score_color(score, key)
    raw        = _key_value_str(key, m)
    warn, crit = THRESHOLDS.get(key, (50, 75))

    bar_w  = 40
    filled = max(0, min(int(((score or 0) / 100.0) * bar_w), bar_w))

    warn_pos = int((warn / 100.0) * bar_w)
    crit_pos = int((crit / 100.0) * bar_w)
    axis     = list(" " * (bar_w + 2))
    axis[warn_pos] = "↑"
    axis[crit_pos] = "↑"
    axis_str = "".join(axis)

    body = Text()
    body.append("\n  ")
    body.append("█" * filled,           style=color)
    body.append("░" * (bar_w - filled), style="dim")
    body.append("\n")
    body.append(f"  {axis_str}\n",      style="dim")
    body.append(f"  warn={warn}    crit={crit}\n\n", style="dim")
    body.append("  Score : ", style="bold")
    body.append(f"{fmt_score(score)} / 100\n", style=f"bold {color}")
    body.append("  Value : ", style="bold")
    body.append(f"{raw}\n",   style="white")

    if key == "K6" and isinstance(m, dict) and m.get("kafka_pct") is not None:
        body.append(f"\n  Root Disk : ", style="bold")
        body.append(
            f"{m.get('root_used_gb','?')} / {m.get('root_total_gb','?')} GB  "
            f"(free {m.get('root_free_gb','?')} GB)\n",
            style="white"
        )
        body.append("\n  ── Kafka breakdown ────────────────\n")
        body.append("  logs/    ", style="bold")
        body.append(f"{m.get('logs_gb','?')} GB\n",   style="white")
        body.append("    ├ msg data  ", style="dim")
        body.append(f"{m.get('msg_gb','?')} GB\n",    style="white")
        body.append("    ├ indexes   ", style="dim")
        body.append(f"{m.get('index_gb','?')} GB\n",  style="white")
        body.append("    ├ KRaft     ", style="dim")
        body.append(f"{m.get('kraft_gb','?')} GB\n",  style="white")
        body.append("    └ app logs  ", style="dim")
        body.append(f"{m.get('applog_gb','?')} GB\n", style="white")
        body.append("  libs/    ", style="bold")
        body.append(f"{m.get('libs_gb','?')} GB\n",   style="white")
        body.append("  bin/     ", style="bold")
        body.append(f"{m.get('bin_gb','?')} GB\n",    style="white")
        body.append("  config/  ", style="bold")
        body.append(f"{m.get('config_gb','?')} GB\n", style="white")
        body.append("  other    ", style="bold")
        body.append(f"{m.get('other_gb','?')} GB\n",  style="white")

    panel_width = 58 if key == "K6" else 54
    return Panel(
        body,
        title=f"[bold cyan]{key} — {name}[/bold cyan]",
        border_style=color,
        padding=(0, 2),
        width=panel_width,
    )


def display_resource_gauges(timeframe: str = "1h"):
    """Display K4, K5, K6 as large side-by-side gauge panels."""
    snap = collect_all()
    console.rule("[bold cyan]⚙  Kafka Resource Gauges  (CPU / Memory / Disk)[/bold cyan]")
    panels = [
        _gauge_panel("K4", snap.get("K4", {})),
        _gauge_panel("K5", snap.get("K5", {})),
        _gauge_panel("K6", snap.get("K6", {})),
    ]
    console.print(Columns(panels, equal=True, expand=True))
    console.print(
        "[dim]  K4 CPU and K5 Memory are Kafka-process-specific (psutil).\n"
        "  K6 Disk measures Kafka's full installation footprint vs root partition (/).[/dim]"
    )