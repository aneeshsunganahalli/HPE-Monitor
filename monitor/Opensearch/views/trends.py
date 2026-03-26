"""
View 7 — Historical Trends

Poller-first trend view for CPU, JVM heap, and indexing rate
with Prometheus fallback when poller history is unavailable.
"""

from __future__ import annotations

from datetime import datetime

from rich.panel import Panel

from monitor.config import CPU_WARN, CPU_CRIT, console
from monitor.metrics_service import TrendSeries, fetch_historical_trends_with_source
from monitor.utils import format_bytes, is_realtime_timeframe, timeframe_to_prometheus_range

_BAR_CHAR = "█"
_EMPTY_CHAR = " "
_DEFAULT_CHART_WIDTH = 60
_DEFAULT_CHART_HEIGHT = 10
_GRAPH_COLOR = "bright_cyan"
_METRIC_COLORS = {
    "cpu": _GRAPH_COLOR,
    "heap": _GRAPH_COLOR,
    "indexing_rate": _GRAPH_COLOR,
}


def _downsample_indices(length: int, width: int) -> list[int]:
    """Return source indices for downsampling while preserving trend shape."""
    if length <= 0:
        return []
    if length <= width:
        return list(range(length))

    step = (length - 1) / (width - 1)
    return [round(i * step) for i in range(width)]


def _chart_width() -> int:
    """Pick a wide chart width that still fits narrow terminals."""
    terminal_width = getattr(console, "width", 120) or 120
    # Keep room for y-axis labels and panel framing.
    return max(36, min(_DEFAULT_CHART_WIDTH, terminal_width - 28))


def _downsample_series(series: TrendSeries, width: int) -> tuple[list[float], list[int]]:
    """Downsample values and timestamps together using identical sample points."""
    indices = _downsample_indices(len(series.values), width)
    reduced_values = [series.values[i] for i in indices]

    if series.timestamps and len(series.timestamps) == len(series.values):
        reduced_timestamps = [series.timestamps[i] for i in indices]
    else:
        reduced_timestamps = []

    return reduced_values, reduced_timestamps


def _format_metric_value(series: TrendSeries, value: float) -> str:
    """Format metric values by unit."""
    if series.unit == "%":
        return f"{value:.1f}%"
    if series.unit == "bytes":
        return format_bytes(value)
    if series.unit == "ops/s":
        if value >= 100:
            return f"{value:.0f}/s"
        if value >= 10:
            return f"{value:.1f}/s"
        if value >= 1:
            return f"{value:.2f}/s"
        if value > 0:
            return f"{value:.4f}/s"
        return "0/s"
    return f"{value:.2f}"


def _format_timestamp(ts: int) -> str:
    """Render a compact local time label for x-axis markers."""
    try:
        return datetime.fromtimestamp(ts).strftime("%H:%M")
    except (OverflowError, OSError, ValueError):
        return ""


def _vertical_chart(metric_key: str, series: TrendSeries, width: int, height: int = _DEFAULT_CHART_HEIGHT) -> str:
    """Render a vertical bar chart with y-axis labels and time direction."""
    if not series.values:
        return "[dim]no data[/dim]"

    reduced_values, reduced_timestamps = _downsample_series(series, width=width)
    if not reduced_values:
        return "[dim]no data[/dim]"

    lo = min(reduced_values)
    hi = max(reduced_values)
    span = hi - lo

    if span <= 0:
        levels = [height] * len(reduced_values)
    else:
        levels = [int(round(((value - lo) / span) * height)) for value in reduced_values]

    color = _METRIC_COLORS.get(metric_key, "white")
    y_top = _format_metric_value(series, hi)
    y_bottom = _format_metric_value(series, lo)
    y_mid = _format_metric_value(series, lo + (span / 2 if span > 0 else 0.0))
    label_width = max(len(y_top), len(y_mid), len(y_bottom), 6)

    tick_rows = {
        height,
        max(1, int(round(height * 0.75))),
        max(1, int(round(height * 0.50))),
        max(1, int(round(height * 0.25))),
        1,
    }

    lines: list[str] = []
    for row in range(height, 0, -1):
        if span > 0:
            axis_value = lo + (span * row / height)
        else:
            axis_value = hi
        axis_label = _format_metric_value(series, axis_value)
        prefix = f"{axis_label:>{label_width}} | " if row in tick_rows else f"{'':>{label_width}} | "

        bars = "".join(
            _BAR_CHAR if level >= row else _EMPTY_CHAR
            for level in levels
        )
        lines.append(f"[dim]{prefix}[/dim][{color}]{bars}[/{color}]")

    lines.append(f"[dim]{'':>{label_width}} +{'-' * len(levels)}[/dim]")

    start_ts = _format_timestamp(reduced_timestamps[0]) if len(reduced_timestamps) >= 1 else ""
    end_ts = _format_timestamp(reduced_timestamps[-1]) if len(reduced_timestamps) >= 2 else ""
    if start_ts and end_ts:
        gap = max(1, len(levels) - len(start_ts) - len(end_ts))
        lines.append(f"[dim]{'':>{label_width}}  {start_ts}{' ' * gap}{end_ts}[/dim]")
    lines.append(f"[dim]{'':>{label_width}}  older -> newer[/dim]")

    return "\n".join(lines)


def _series_average(series: TrendSeries) -> float:
    """Return the arithmetic mean of a series."""
    if not series.values:
        return 0.0
    return sum(series.values) / len(series.values)


def _metric_readout(metric_key: str, series: TrendSeries) -> str:
    """Generate a plain-English metric interpretation."""
    if not series.values:
        return "Prometheus returned no samples in this window."

    if metric_key == "cpu":
        if series.peak >= CPU_CRIT:
            return "Critical CPU spike detected. Investigate query/indexing pressure immediately."
        if series.peak >= CPU_WARN:
            return "Warning CPU spike detected. Monitor sustained load and hot shards."
        return "CPU trend is stable with no major spike."

    if metric_key == "heap":
        if series.latest > 0 and series.peak >= series.latest * 1.4:
            return "Heap spike is much higher than current level. Check for GC or burst traffic."
        return "Heap trend is steady relative to current usage."

    if metric_key == "indexing_rate":
        if series.peak >= max(series.latest * 1.5, 1.0):
            return "Burst indexing detected. Validate ingest pipeline throughput and backpressure."
        return "Indexing rate is consistent across the selected window."

    return "Trend captured."


def display_trends(timeframe: str = "1h"):
    """Render historical trends using 5-minute Prometheus max buckets."""
    console.print()
    console.rule("[bold cyan]OpenSearch — Historical Trends[/bold cyan]")
    console.print()

    effective_window = timeframe_to_prometheus_range(timeframe)
    if is_realtime_timeframe(timeframe):
        window_note = "real-time requested -> using 1h history for trend graph"
    else:
        window_note = f"using --timeframe {effective_window}"

    source, series_by_metric = fetch_historical_trends_with_source(timeframe=timeframe)
    cpu_series = series_by_metric.get("cpu", TrendSeries("CPU", [], [], "%"))
    heap_series = series_by_metric.get("heap", TrendSeries("JVM Heap", [], [], "bytes"))
    indexing_series = series_by_metric.get("indexing_rate", TrendSeries("Indexing Rate", [], [], "ops/s"))

    if not cpu_series.values and not heap_series.values and not indexing_series.values:
        console.print(Panel(
            "  No Prometheus trend data available.\n"
            "  Verify Prometheus connectivity and OpenSearch metric ingestion.",
            title="[bold]Historical Trends[/bold]",
            title_align="left",
            border_style="yellow",
            expand=False,
        ))
        console.print()
        return

    if source == "poller":
        source_text = "Poller JSONL (5m max buckets)"
    elif source == "mixed":
        source_text = "Mixed: poller JSONL + Prometheus fallback"
    elif source == "prometheus":
        source_text = "Prometheus (5m max_over_time buckets)"
    else:
        source_text = "Unavailable"

    # High-level summary first
    summary_text = (
        f"  Source        : {source_text}\n"
        f"  Time Window   : {effective_window} ({window_note})\n"
        f"  Peak CPU      : {_format_metric_value(cpu_series, cpu_series.peak)}\n"
        f"  Peak JVM Heap : {_format_metric_value(heap_series, heap_series.peak)}\n"
        f"  Peak Indexing : {_format_metric_value(indexing_series, indexing_series.peak)}"
    )
    console.print(Panel(
        summary_text,
        title="[bold]Summary[/bold]",
        title_align="left",
        border_style="cyan",
        expand=True,
    ))
    console.print()

    chart_width = _chart_width()

    for metric_key, series in (
        ("cpu", cpu_series),
        ("heap", heap_series),
        ("indexing_rate", indexing_series),
    ):
        latest = _format_metric_value(series, series.latest) if series.values else "—"
        peak = _format_metric_value(series, series.peak) if series.values else "—"
        average = _format_metric_value(series, _series_average(series)) if series.values else "—"
        minimum = _format_metric_value(series, min(series.values)) if series.values else "—"

        chart_text = _vertical_chart(metric_key, series, width=chart_width)
        details_text = (
            f"[bold]Latest:[/bold] {latest}    "
            f"[bold]Peak:[/bold] {peak}    "
            f"[bold]Average:[/bold] {average}    "
            f"[bold]Min:[/bold] {minimum}\n"
            f"[bold]Readout:[/bold] {_metric_readout(metric_key, series)}"
        )

        console.print(Panel(
            f"{chart_text}\n\n{details_text}",
            title=f"[bold]{series.label}[/bold]",
            title_align="left",
            border_style=_METRIC_COLORS.get(metric_key, "cyan"),
            expand=True,
        ))
        console.print()
