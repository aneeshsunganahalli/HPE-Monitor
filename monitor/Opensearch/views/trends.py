"""
View 7 — Historical Trends

Prometheus-backed trend view for CPU, JVM heap, and indexing rate.
"""

from __future__ import annotations

from rich import box
from rich.panel import Panel
from rich.table import Table

from monitor.config import CPU_WARN, CPU_CRIT, console
from monitor.metrics_service import TrendSeries, fetch_historical_trends
from monitor.utils import format_bytes, is_realtime_timeframe, timeframe_to_prometheus_range

_SPARK_BLOCKS = " ▁▂▃▄▅▆▇█"
_DEFAULT_SPARK_WIDTH = 68
_METRIC_COLORS = {
    "cpu": "bright_yellow",
    "heap": "bright_green",
    "indexing_rate": "bright_cyan",
}


def _downsample(values: list[float], width: int) -> list[float]:
    """Downsample a list to the target width while preserving the overall shape."""
    if not values:
        return []
    if len(values) <= width:
        return values

    step = (len(values) - 1) / (width - 1)
    return [values[round(i * step)] for i in range(width)]


def _sparkline(values: list[float], width: int = 42) -> str:
    """Render a compact unicode sparkline string."""
    if not values:
        return "[dim]no data[/dim]"

    reduced = _downsample(values, width=width)
    lo = min(reduced)
    hi = max(reduced)

    if hi == lo:
        return _SPARK_BLOCKS[-1] * len(reduced)

    scale = (len(_SPARK_BLOCKS) - 1) / (hi - lo)
    blocks = []
    for value in reduced:
        idx = int((value - lo) * scale)
        idx = max(0, min(idx, len(_SPARK_BLOCKS) - 1))
        blocks.append(_SPARK_BLOCKS[idx])
    return "".join(blocks)


def _sparkline_width() -> int:
    """Pick a wider chart width that adapts to terminal size."""
    # Reserve room for table borders and side columns while maximizing trend width.
    terminal_width = getattr(console, "width", 120) or 120
    return max(56, min(_DEFAULT_SPARK_WIDTH, terminal_width - 58))


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


def _trend_cell(metric_key: str, series: TrendSeries, width: int) -> str:
    """Render a larger sparkline plus min/max labels for easier scanning."""
    if not series.values:
        return "[dim]no data[/dim]"

    spark = _sparkline(series.values, width=width)
    color = _METRIC_COLORS.get(metric_key, "white")
    floor = _format_metric_value(series, min(series.values))
    peak = _format_metric_value(series, max(series.values))
    return (
        f"[{color}]{spark}[/{color}]\n"
        f"[dim]min {floor}  |  max {peak}[/dim]"
    )


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

    series_by_metric = fetch_historical_trends(timeframe=timeframe)
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

    # High-level summary first
    summary_text = (
        f"  Source        : Prometheus (5m max_over_time buckets)\n"
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

    # Drill-down detail
    spark_width = _sparkline_width()
    table = Table(
        box=box.ROUNDED,
        show_header=True,
        show_lines=True,
        header_style="bold cyan",
        padding=(1, 1),
        expand=True,
    )
    table.add_column("Metric", style="bold white", width=16)
    table.add_column("Trend (Historical)", ratio=4)
    table.add_column("Latest", width=12, justify="right")
    table.add_column("Peak", width=12, justify="right")
    table.add_column("Readout", ratio=2)

    for metric_key, series in (
        ("cpu", cpu_series),
        ("heap", heap_series),
        ("indexing_rate", indexing_series),
    ):
        latest = _format_metric_value(series, series.latest) if series.values else "—"
        peak = _format_metric_value(series, series.peak) if series.values else "—"
        table.add_row(
            series.label,
            _trend_cell(metric_key, series, width=spark_width),
            latest,
            peak,
            _metric_readout(metric_key, series),
        )

    console.print(table)
    console.print()
