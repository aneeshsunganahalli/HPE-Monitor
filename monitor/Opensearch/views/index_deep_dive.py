"""
View 3 — Index Deep Dive

Shows all indices sorted by size. Allows drilling into a specific
index to see its shard layout.
"""

from rich.table import Table
from rich import box
from rich.prompt import Prompt
from simple_term_menu import TerminalMenu

from monitor.config import console
from monitor.client import fetch_indices, fetch_shards
from monitor.utils import format_bytes, parse_size_string


def _prompt_index_search(index_names: list[str]) -> str | None:
    """Prompt for an index search term with live completion when available."""
    try:
        from prompt_toolkit import prompt  # type: ignore[import-not-found]
        from prompt_toolkit.completion import FuzzyWordCompleter  # type: ignore[import-not-found]

        completer = FuzzyWordCompleter(index_names, WORD=True)
        value = prompt(
            "Index search (type to filter, Enter to apply): ",
            completer=completer,
            complete_while_typing=True,
        )
        return value.strip()
    except (EOFError, KeyboardInterrupt):
        return None
    except Exception:
        # Fallback for environments where prompt_toolkit is unavailable.
        return Prompt.ask(
            "[bold]Index search[/bold] [dim](partial name, blank = show all)[/dim]",
            default="",
        ).strip()


def display_index_deep_dive(timeframe: str = "1h"):
    """Render the Index Deep Dive view."""
    console.print()
    console.rule("[bold cyan]OpenSearch — Index Deep Dive[/bold cyan]")
    console.print()

    indices = fetch_indices()
    if not indices:
        console.print("[yellow]No indices found.[/yellow]")
        return

    all_index_names = [idx.get("index", "") for idx in indices if idx.get("index")]
    search_term = _prompt_index_search(all_index_names)
    if search_term is None:
        return

    filtered_indices = indices
    if search_term:
        lowered_search = search_term.lower()
        exact_match = next(
            (idx.get("index", "") for idx in indices if idx.get("index", "").lower() == lowered_search),
            None,
        )
        if exact_match:
            console.print(f"[green]Exact match found:[/green] [bold]{exact_match}[/bold]")
            _display_index_shards(exact_match)
            return

        filtered_indices = [
            idx for idx in indices
            if lowered_search in idx.get("index", "").lower()
        ]
        if not filtered_indices:
            console.print(f"[yellow]No indices matched '{search_term}'.[/yellow]")
            return

    # ── Index Table ───────────────────────────────────────────
    table = Table(
        box=box.ROUNDED,
        show_header=True,
        header_style="bold cyan",
        title="[bold]Indices (sorted by size)[/bold]",
        title_style="bold white",
        expand=True,
    )
    table.add_column("#", style="dim", width=5, justify="right")
    table.add_column("Index Name", style="white", ratio=2)
    table.add_column("Size", style="yellow", width=14, justify="right")
    table.add_column("Documents", style="cyan", width=16, justify="right")
    table.add_column("Health", width=10, justify="center")
    table.add_column("Shards", width=10, justify="right")
    table.add_column("Replicas", width=10, justify="right")

    index_names = []
    for i, idx in enumerate(filtered_indices, 1):
        name = idx.get("index", "—")
        index_names.append(name)

        size_raw = idx.get("store.size", "0")
        size_display = format_bytes(parse_size_string(size_raw))
        doc_count = idx.get("docs.count", "0")
        health = idx.get("health", "—")
        pri = idx.get("pri", "—")
        rep = idx.get("rep", "—")

        # Format doc count with commas
        try:
            doc_display = f"{int(doc_count):,}"
        except (ValueError, TypeError):
            doc_display = str(doc_count)

        health_color = {"green": "green", "yellow": "yellow", "red": "red"}.get(health.lower(), "white")
        health_display = f"[{health_color}]{health.upper()}[/{health_color}]"

        table.add_row(str(i), name, size_display, doc_display, health_display, str(pri), str(rep))

    if search_term:
        console.print(f"[dim]Filter:[/dim] '{search_term}' [dim]({len(filtered_indices)} matches)[/dim]")
    console.print(table)
    console.print()

    # ── Drill-Down Selector ───────────────────────────────────
    console.print("[dim]Select an index to inspect its shard layout:[/dim]")
    console.print()

    drill_options = index_names + ["Back"]
    menu = TerminalMenu(
        drill_options,
        menu_cursor="❯ ",
        menu_cursor_style=("fg_cyan", "bold"),
        menu_highlight_style=("fg_cyan", "bold"),
    )
    choice = menu.show()

    if choice is None or choice == len(index_names):  # Escape or Back
        return

    _display_index_shards(drill_options[choice])


def _display_index_shards(index_name: str):
    """Show the shard layout for a specific index."""
    console.print()
    console.rule(f"[bold cyan]Shard Layout — {index_name}[/bold cyan]")
    console.print()

    shards = fetch_shards(index=index_name)
    if not shards:
        console.print(f"[yellow]No shard data found for '{index_name}'.[/yellow]")
        return

    table = Table(
        box=box.ROUNDED,
        show_header=True,
        header_style="bold cyan",
        expand=True,
    )
    table.add_column("Shard", style="white", width=8, justify="center")
    table.add_column("Type", style="dim", width=10, justify="center")
    table.add_column("State", width=14, justify="center")
    table.add_column("Node", style="yellow", ratio=1)
    table.add_column("Size", style="cyan", width=14, justify="right")
    table.add_column("Docs", style="white", width=14, justify="right")

    for shard in shards:
        shard_num = shard.get("shard", "—")
        prirep = "Primary" if shard.get("prirep", "") == "p" else "Replica"
        state = shard.get("state", "—").upper()
        node = shard.get("node", "unassigned")
        size_raw = shard.get("store", "0") or "0"
        docs = shard.get("docs", "0") or "0"

        # Color the state
        state_color = {
            "STARTED": "green",
            "UNASSIGNED": "red",
            "RELOCATING": "yellow",
            "INITIALIZING": "yellow",
        }.get(state, "white")
        state_display = f"[{state_color}]{state}[/{state_color}]"

        size_display = format_bytes(parse_size_string(size_raw))

        try:
            docs_display = f"{int(docs):,}"
        except (ValueError, TypeError):
            docs_display = str(docs)

        if node is None or node == "null":
            node = "[red]unassigned[/red]"

        table.add_row(str(shard_num), prirep, state_display, node, size_display, docs_display)

    console.print(table)
    console.print()
