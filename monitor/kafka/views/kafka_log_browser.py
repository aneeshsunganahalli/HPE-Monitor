"""
View -- Kafka Log Browser

Reads Kafka broker's own operational log files directly from disk
(KAFKA_LOG_DIR: /opt/kafka/kafka/logs/).

Files browsed:
  server.log           -- broker lifecycle, partition events, controller elections
  controller.log       -- controller-specific events
  state-change.log     -- partition leader state transitions
  kafka-request.log    -- client request/response logging (if enabled)
  kafka.log            -- general Kafka internal log

Features:
  - Arrow-key menu to select which log file to browse
  - Colour-coded by severity: ERROR=red, WARN=yellow, INFO=dim, DEBUG=blue
  - Level filter: ALL / WARN+ / ERROR only
  - Shows last N lines (default 40, configurable)
  - Keyword search/highlight within the displayed lines
  - Auto-detects available log files (skips missing ones)
"""

from __future__ import annotations

import os
import re
import pathlib
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.text import Text
from simple_term_menu import TerminalMenu

from monitor.config import console
from monitor.kafka.collectors import KAFKA_LOG_DIR
from monitor.utils import press_enter_to_return


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_DEFAULT_LINES = 40

# Log files to surface, in preferred display order
_CANDIDATE_FILES = [
    "server.log",
    "controller.log",
    "state-change.log",
    "kafka-request.log",
    "kafka.log",
    "kafkaServer.out",
]

# Severity regex — matches the standard Kafka log4j pattern:
#   [2025-04-23 14:01:23,456] INFO  message (kafka.server.KafkaServer)
_LOG_RE = re.compile(
    r"^\[(?P<ts>[^\]]+)\]\s+(?P<level>ERROR|WARN|INFO|DEBUG|TRACE)\s+(?P<msg>.+)$"
)

_LEVEL_COLOR = {
    "ERROR": "bold red",
    "WARN":  "yellow",
    "INFO":  "dim",
    "DEBUG": "blue",
    "TRACE": "dim blue",
}

_LEVEL_ORDER = {"ERROR": 0, "WARN": 1, "INFO": 2, "DEBUG": 3, "TRACE": 4}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _available_logs() -> list[pathlib.Path]:
    """Return log files that actually exist in KAFKA_LOG_DIR."""
    base = pathlib.Path(KAFKA_LOG_DIR)
    found = []
    for name in _CANDIDATE_FILES:
        p = base / name
        if p.exists() and p.is_file():
            found.append(p)
    # Also pick up any .log files not in the candidate list
    for p in sorted(base.glob("*.log")):
        if p not in found:
            found.append(p)
    for p in sorted(base.glob("*.out")):
        if p not in found:
            found.append(p)
    return found


def _tail(path: pathlib.Path, n: int) -> list[str]:
    """Return the last n lines of a file efficiently."""
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            buf  = bytearray()
            pos  = size
            lines_found = 0
            chunk = 4096
            while pos > 0 and lines_found <= n:
                read_size = min(chunk, pos)
                pos      -= read_size
                f.seek(pos)
                buf = bytearray(f.read(read_size)) + buf
                lines_found = buf.count(b"\n")
            lines = buf.decode("utf-8", errors="replace").splitlines()
            return lines[-n:] if len(lines) > n else lines
    except Exception as e:
        return [f"[ERROR reading file: {e}]"]


def _parse_line(raw: str) -> tuple[str, str, str, str]:
    """
    Returns (timestamp, level, message, source_class).
    Falls back gracefully for non-standard lines.
    """
    m = _LOG_RE.match(raw.strip())
    if m:
        ts    = m.group("ts")
        level = m.group("level")
        rest  = m.group("msg")
        # Try to split trailing (ClassName) from message
        src_m = re.search(r"\(([^)]+)\)\s*$", rest)
        if src_m:
            msg = rest[:src_m.start()].strip()
            src = src_m.group(1)
        else:
            msg = rest
            src = ""
        return ts, level, msg, src
    return "", "INFO", raw.strip(), ""


def _colorize_line(raw: str, keyword: str = "") -> Text:
    """Build a Rich Text object for one log line, with optional keyword highlight."""
    ts, level, msg, src = _parse_line(raw)
    color = _LEVEL_COLOR.get(level, "white")
    t     = Text()

    if ts:
        t.append(f"[{ts}] ", style="dim")
        t.append(f"{level:<5} ", style=color)
        if keyword and keyword.lower() in msg.lower():
            # highlight keyword occurrences in message
            lower_msg = msg.lower()
            kw_lower  = keyword.lower()
            pos = 0
            while True:
                idx = lower_msg.find(kw_lower, pos)
                if idx < 0:
                    t.append(msg[pos:], style=color)
                    break
                t.append(msg[pos:idx], style=color)
                t.append(msg[idx:idx + len(keyword)], style="bold white on dark_cyan")
                pos = idx + len(keyword)
        else:
            t.append(msg, style=color)
        if src:
            t.append(f"  ({src})", style="dim")
    else:
        # Non-standard line — show as-is dimmed
        t.append(raw.strip(), style="dim")

    return t


# ---------------------------------------------------------------------------
# Sub-views
# ---------------------------------------------------------------------------

def _browse_file(path: pathlib.Path, level_filter: str, n_lines: int, keyword: str) -> None:
    """Display the tail of a single log file with filtering applied."""
    console.print()
    console.rule(
        f"[bold cyan]Kafka Log — {path.name}  "
        f"[dim](last {n_lines} lines | filter={level_filter}"
        f"{f' | search={keyword}' if keyword else ''})[/dim][/bold cyan]"
    )
    console.print()

    raw_lines = _tail(path, n_lines * 10)   # fetch extra to survive filtering
    if not raw_lines:
        console.print(Panel("[yellow]File is empty or unreadable.[/yellow]",
                            border_style="cyan", expand=False))
        press_enter_to_return()
        return

    # Apply level filter
    max_level = _LEVEL_ORDER.get(level_filter, 99)
    filtered  = []
    for line in raw_lines:
        _, level, _, _ = _parse_line(line)
        if _LEVEL_ORDER.get(level, 2) <= max_level:
            filtered.append(line)

    # Apply keyword filter
    if keyword:
        filtered = [l for l in filtered if keyword.lower() in l.lower()]

    # Trim to requested count
    display = filtered[-n_lines:]

    if not display:
        console.print(Panel(
            f"[yellow]No lines matching filter=[bold]{level_filter}[/bold]"
            f"{f' keyword=[bold]{keyword}[/bold]' if keyword else ''}[/yellow]",
            border_style="cyan", expand=False,
        ))
        press_enter_to_return()
        return

    # File info panel
    try:
        size_kb = path.stat().st_size / 1024
        info    = f"{path}  |  {size_kb:.1f} KiB  |  showing {len(display)} of {len(filtered)} filtered lines"
    except Exception:
        info = str(path)
    console.print(f"[dim]{info}[/dim]\n")

    # Count severities in the displayed slice
    counts = {"ERROR": 0, "WARN": 0, "INFO": 0, "DEBUG": 0, "TRACE": 0}
    for line in display:
        _, level, _, _ = _parse_line(line)
        counts[level] = counts.get(level, 0) + 1

    summary = (
        f"[bold red]ERROR {counts['ERROR']}[/bold red]  "
        f"[yellow]WARN {counts['WARN']}[/yellow]  "
        f"[dim]INFO {counts['INFO']}  DEBUG {counts['DEBUG']}[/dim]"
    )
    console.print(f"  {summary}\n")

    for line in display:
        console.print(_colorize_line(line, keyword))

    console.print()
    press_enter_to_return()


def _settings_menu() -> tuple[str, int, str]:
    """
    Let the user pick level filter, line count, and optional keyword.
    Returns (level_filter, n_lines, keyword).
    """
    # Level filter
    level_choices = ["ALL  (INFO + WARN + ERROR + DEBUG)", "WARN+  (WARN and ERROR only)", "ERROR  (errors only)"]
    level_map     = {0: "TRACE", 1: "WARN", 2: "ERROR"}

    console.print("\n  [dim]Level filter:[/dim]")
    menu   = TerminalMenu(
        level_choices,
        menu_cursor="❯ ",
        menu_cursor_style=("fg_cyan", "bold"),
        menu_highlight_style=("fg_cyan", "bold"),
    )
    choice = menu.show()
    level  = level_map.get(choice if choice is not None else 0, "TRACE")

    # Line count
    line_choices = ["20 lines", "40 lines", "80 lines", "200 lines"]
    line_map     = {0: 20, 1: 40, 2: 80, 3: 200}

    console.print("\n  [dim]Lines to show:[/dim]")
    menu   = TerminalMenu(
        line_choices,
        menu_cursor="❯ ",
        menu_cursor_style=("fg_cyan", "bold"),
        menu_highlight_style=("fg_cyan", "bold"),
    )
    choice  = menu.show()
    n_lines = line_map.get(choice if choice is not None else 1, 40)

    # Keyword
    console.print()
    console.print("  [dim]Keyword to search/highlight (leave blank for none):[/dim]")
    try:
        keyword = input("  ❯ ").strip()
    except (EOFError, KeyboardInterrupt):
        keyword = ""

    return level, n_lines, keyword


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def display_kafka_log_browser() -> None:
    """Main entry point for the Kafka Log Browser view."""
    while True:
        console.print()
        console.rule("[bold cyan]Kafka — Log Browser[/bold cyan]")
        console.print()

        logs = _available_logs()
        if not logs:
            console.print(Panel(
                f"[yellow]No log files found in:[/yellow]\n"
                f"[bold]{KAFKA_LOG_DIR}[/bold]\n\n"
                "[dim]Check that KAFKA_LOG_DIR is correct in config.[/dim]",
                border_style="cyan", expand=False,
            ))
            press_enter_to_return()
            return

        # Build menu entries with file size info
        entries = []
        for p in logs:
            try:
                kb = p.stat().st_size / 1024
                entries.append(f"{p.name:<35} {kb:>8.1f} KiB")
            except Exception:
                entries.append(p.name)
        entries.append("Settings")
        entries.append("Back")

        console.print("  [dim]Select a log file to browse:[/dim]\n")
        menu   = TerminalMenu(
            entries,
            menu_cursor="❯ ",
            menu_cursor_style=("fg_cyan", "bold"),
            menu_highlight_style=("fg_cyan", "bold"),
        )
        choice = menu.show()

        if choice is None or choice == len(logs) + 1:
            return
        elif choice == len(logs):
            # Settings
            level, n_lines, keyword = _settings_menu()
            # After settings, loop back to file selection with new settings
            # stored in a closure — re-enter browse directly is cleaner:
            console.print()
            console.print("  [dim]Now select a log file to browse with new settings:[/dim]\n")
            menu2  = TerminalMenu(
                entries[:-2],  # only file entries
                menu_cursor="❯ ",
                menu_cursor_style=("fg_cyan", "bold"),
                menu_highlight_style=("fg_cyan", "bold"),
            )
            choice2 = menu2.show()
            if choice2 is not None and choice2 < len(logs):
                _browse_file(logs[choice2], level, n_lines, keyword)
        else:
            # Default settings for quick browse
            _browse_file(logs[choice], level_filter="TRACE", n_lines=_DEFAULT_LINES, keyword="")