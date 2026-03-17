"""
Utility functions: formatting, thresholds, status symbols.

Keeps all display logic DRY across views.
"""

import re

from rich.prompt import Prompt

from monitor.config import console


# ──────────────────── Byte Formatting ──────────────────────────

def format_bytes(num_bytes: float) -> str:
    """Convert a byte count to a human-readable string (e.g., '14.2 GB')."""
    if num_bytes is None or num_bytes < 0:
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}" if num_bytes != int(num_bytes) else f"{int(num_bytes)} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} EB"


def parse_size_string(size_str: str) -> float:
    """
    Convert OpenSearch size strings like '65gb', '120.5mb', '1.2tb' to bytes.
    Returns 0.0 if parsing fails.
    """
    if not size_str:
        return 0.0
    size_str = size_str.strip().lower()
    multipliers = {
        "b": 1,
        "kb": 1024,
        "mb": 1024 ** 2,
        "gb": 1024 ** 3,
        "tb": 1024 ** 4,
        "pb": 1024 ** 5,
    }
    match = re.match(r"^([\d.]+)\s*([a-z]+)$", size_str)
    if not match:
        try:
            return float(size_str)
        except ValueError:
            return 0.0
    value = float(match.group(1))
    unit = match.group(2)
    return value * multipliers.get(unit, 1)


# ──────────────────── Status Symbols ───────────────────────────

def status_symbol(value: float, warn_threshold: float, crit_threshold: float) -> str:
    """
    Return a colored status symbol based on thresholds.
      ✓ green  = healthy (below warning)
      ⚠ yellow = warning (at or above warning, below critical)
      ✗ red    = critical (at or above critical)
    """
    if value >= crit_threshold:
        return "[red]✗[/red]"
    elif value >= warn_threshold:
        return "[yellow]⚠[/yellow]"
    else:
        return "[green]✓[/green]"


def status_color(value: float, warn_threshold: float, crit_threshold: float) -> str:
    """Return the color string ('green', 'yellow', or 'red') for a value."""
    if value >= crit_threshold:
        return "red"
    elif value >= warn_threshold:
        return "yellow"
    else:
        return "green"


def cluster_status_symbol(status: str) -> str:
    """Return a colored symbol for cluster health status (green/yellow/red)."""
    status = status.lower()
    if status == "green":
        return "[green]✓[/green]"
    elif status == "yellow":
        return "[yellow]⚠[/yellow]"
    else:
        return "[red]✗[/red]"


def cluster_status_styled(status: str) -> str:
    """Return the cluster status string with appropriate color markup."""
    status_upper = status.upper()
    color = status.lower()
    if color not in ("green", "yellow", "red"):
        color = "white"
    return f"[{color}]{status_upper}[/{color}]"


# ──────────────────── Timeframe ───────────────────────────────

def timeframe_to_minutes(tf: str) -> int:
    """
    Convert a timeframe string (e.g. '30m', '2h', '7d') to total minutes.
    Returns 60 (1h) as the default fallback.
    """
    m = re.fullmatch(r"(\d+)([mhd])", tf.strip().lower())
    if not m:
        return 60
    n, unit = int(m.group(1)), m.group(2)
    if unit == "m":
        return n
    elif unit == "h":
        return n * 60
    else:  # d
        return n * 1440


# ──────────────────── Interaction ──────────────────────────────

def press_enter_to_return():
    """Standard prompt to return to the menu."""
    console.print()
    Prompt.ask("[dim]Press Enter to return[/dim]", default="")
