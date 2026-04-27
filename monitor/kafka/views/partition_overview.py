"""
View -- Partition Overview

Per-topic partition health: leader, ISR, replication gap, and a per-partition
lag ratio (what fraction of stored messages the consumer is behind on).
Analogous to OpenSearch's shard_overview.py.
"""

from __future__ import annotations

import requests
import urllib3
from rich.panel import Panel
from rich.table import Table
from rich import box

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from monitor.config import console
from monitor.kafka.collectors import PROM_URL, PROM_AUTH, PROM_VERIFY, KAFKA_GROUP
from monitor.utils import press_enter_to_return

_SKIP_TOPICS = {"__consumer_offsets", "__transaction_state"}


def _fetch_vector(promql: str) -> dict:
    """Query Prometheus instant vector, return dict keyed by (topic, partition)."""
    result = {}
    try:
        r = requests.get(
            f"{PROM_URL}/api/v1/query",
            params={"query": promql},
            auth=PROM_AUTH,
            verify=PROM_VERIFY,
            timeout=8,
        )
        r.raise_for_status()
        for item in r.json().get("data", {}).get("result", []):
            topic     = item["metric"].get("topic", "")
            partition = item["metric"].get("partition", "")
            if topic and partition:
                result[(topic, partition)] = float(item["value"][1])
    except Exception:
        pass
    return result


def _fetch_vector_single(promql: str, key: str) -> dict:
    """Query Prometheus instant vector keyed by a single label (e.g. topic only)."""
    result = {}
    try:
        r = requests.get(
            f"{PROM_URL}/api/v1/query",
            params={"query": promql},
            auth=PROM_AUTH,
            verify=PROM_VERIFY,
            timeout=8,
        )
        r.raise_for_status()
        for item in r.json().get("data", {}).get("result", []):
            k = item["metric"].get(key, "")
            if k:
                result[k] = float(item["value"][1])
    except Exception:
        pass
    return result


def _collect_partition_data() -> dict:
    """Pull all replication-health + lag metrics and organise by topic."""
    under_rep   = _fetch_vector("kafka_topic_partition_under_replicated_partition")
    in_sync     = _fetch_vector("kafka_topic_partition_in_sync_replica")
    leader      = _fetch_vector("kafka_topic_partition_leader")
    pref_leader = _fetch_vector("kafka_topic_partition_leader_is_preferred")
    replicas    = _fetch_vector("kafka_topic_partition_replicas")
    cur_offset  = _fetch_vector("kafka_topic_partition_current_offset")
    old_offset  = _fetch_vector("kafka_topic_oldest_offset")
    lag         = _fetch_vector(
        f'kafka_consumergroup_lag{{consumergroup="{KAFKA_GROUP}"}}'
    )

    all_keys = set(under_rep) | set(in_sync) | set(leader) | set(pref_leader)

    topics: dict[str, list[dict]] = {}
    for (topic, partition) in sorted(all_keys):
        if topic in _SKIP_TOPICS:
            continue

        ur  = int(under_rep.get((topic, partition), 0))
        isr = int(in_sync.get((topic, partition), -1))
        ld  = int(leader.get((topic, partition), -1))
        pl  = int(pref_leader.get((topic, partition), -1))
        rf  = int(replicas.get((topic, partition), -1))
        cur = int(cur_offset.get((topic, partition), 0))
        old = int(old_offset.get((topic, partition), 0))
        lag_msgs = int(lag.get((topic, partition), 0))

        # Replication gap: how many replicas have fallen behind
        rep_gap = (rf - isr) if rf > 0 and isr >= 0 else None

        # Lag ratio: fraction of stored messages the consumer is behind on
        # depth = cur - old (messages retained on disk for this partition)
        depth = max(cur - old, 0)
        lag_ratio = round((lag_msgs / depth) * 100, 1) if depth > 0 else 0.0

        if ur > 0:
            health = "UNDER-REPLICATED"
        elif isr < 1:
            health = "NO-LEADER"
        elif pl == 0:
            health = "NON-PREFERRED"
        else:
            health = "HEALTHY"

        topics.setdefault(topic, []).append({
            "partition":   partition,
            "leader":      ld,
            "isr":         isr,
            "rf":          rf,
            "rep_gap":     rep_gap,
            "under_rep":   ur,
            "pref_leader": pl,
            "lag_msgs":    lag_msgs,
            "lag_ratio":   lag_ratio,
            "depth":       depth,
            "health":      health,
        })

    return topics


def _health_color(health: str) -> str:
    return {
        "HEALTHY":          "green",
        "NON-PREFERRED":    "yellow",
        "UNDER-REPLICATED": "red",
        "NO-LEADER":        "red",
    }.get(health, "white")


def _health_icon(health: str) -> str:
    return {
        "HEALTHY":          "[green]✓[/green]",
        "NON-PREFERRED":    "[yellow]⚠[/yellow]",
        "UNDER-REPLICATED": "[red]✗[/red]",
        "NO-LEADER":        "[red]✗[/red]",
    }.get(health, "")


def _gap_str(gap) -> str:
    if gap is None:
        return "[dim]--[/dim]"
    if gap == 0:
        return "[green]0[/green]"
    if gap == 1:
        return f"[yellow]{gap}[/yellow]"
    return f"[red]{gap}[/red]"


def _lag_ratio_str(ratio: float, lag_msgs: int) -> str:
    """
    Colour the lag ratio and append the raw lag message count.
    0%      = green  (consumer fully caught up)
    0-5%    = green  (negligible lag)
    5-20%   = yellow (moderate lag)
    >20%    = red    (consumer significantly behind)
    """
    if lag_msgs == 0:
        return "[green]0.0%[/green]  [dim](0 msgs)[/dim]"
    color = "green" if ratio <= 5.0 else "yellow" if ratio <= 20.0 else "red"
    return f"[{color}]{ratio:.1f}%[/{color}]  [dim]({lag_msgs:,} msgs)[/dim]"


def display_partition_overview(timeframe: str = "1h") -> None:
    """Render the Kafka Partition Overview view."""
    console.print()
    console.rule("[bold cyan]Kafka -- Partition Overview[/bold cyan]")
    console.print()

    topics = _collect_partition_data()

    if not topics:
        console.print("[yellow]No partition data returned from Prometheus.[/yellow]")
        console.print("[dim]Check that kafka_exporter is running and reachable.[/dim]")
        return

    total_partitions = sum(len(p) for p in topics.values())
    healthy_count    = sum(1 for p in topics.values() for x in p if x["health"] == "HEALTHY")
    under_rep_count  = sum(1 for p in topics.values() for x in p if x["health"] == "UNDER-REPLICATED")
    non_pref_count   = sum(1 for p in topics.values() for x in p if x["health"] == "NON-PREFERRED")
    no_leader_count  = sum(1 for p in topics.values() for x in p if x["health"] == "NO-LEADER")

    summary_parts = [
        f"[green]HEALTHY: {healthy_count}[/green]",
        f"[yellow]NON-PREFERRED: {non_pref_count}[/yellow]" if non_pref_count else f"[dim]NON-PREFERRED: 0[/dim]",
        f"[red]UNDER-REPLICATED: {under_rep_count}[/red]"   if under_rep_count else f"[dim]UNDER-REPLICATED: 0[/dim]",
        f"[red]NO-LEADER: {no_leader_count}[/red]"          if no_leader_count else f"[dim]NO-LEADER: 0[/dim]",
    ]
    console.print(Panel(
        "  " + "   ".join(summary_parts),
        title=(
            f"[bold]Partition Health Summary[/bold] "
            f"[dim]({total_partitions} partitions across {len(topics)} topic(s))[/dim]"
        ),
        title_align="left",
        border_style="cyan",
        expand=False,
    ))
    console.print()

    issues: list[str] = []

    for topic in sorted(topics):
        partitions    = topics[topic]
        topic_has_ur  = any(p["health"] == "UNDER-REPLICATED" for p in partitions)
        topic_has_nl  = any(p["health"] == "NO-LEADER" for p in partitions)
        topic_healthy = all(p["health"] == "HEALTHY" for p in partitions)

        border_style = "red" if (topic_has_ur or topic_has_nl) else ("cyan" if not topic_healthy else None)
        header_style = "bold red" if (topic_has_ur or topic_has_nl) else "bold cyan"
        title_style  = "bold red" if (topic_has_ur or topic_has_nl) else "bold white"

        table = Table(
            box=box.ROUNDED,
            show_header=True,
            header_style=header_style,
            title=f"[bold]{topic}[/bold]  [dim]({len(partitions)} partition(s))[/dim]",
            title_style=title_style,
            expand=True,
            border_style=border_style,
        )
        table.add_column("Partition",        width=10, justify="center")
        table.add_column("Leader",           width=8,  justify="center")
        table.add_column("RF",               width=5,  justify="center")
        table.add_column("ISR",              width=6,  justify="center")
        table.add_column("Rep Gap",          width=10, justify="center")
        table.add_column("Preferred Leader", width=16, justify="center")
        table.add_column("Under-Rep",        width=11, justify="center")
        table.add_column("Lag Ratio (% of stored msgs)", width=28, justify="left")
        table.add_column("St",               width=4,  justify="center")

        for p in sorted(partitions, key=lambda x: int(x["partition"]) if x["partition"].isdigit() else 0):
            hc      = _health_color(p["health"])
            icon    = _health_icon(p["health"])
            isr_str = f"[{hc}]{p['isr']}[/{hc}]" if p["isr"] >= 0 else "[dim]--[/dim]"
            ld_str  = str(p["leader"]) if p["leader"] >= 0 else "[dim]--[/dim]"
            rf_str  = str(p["rf"])     if p["rf"]     >= 0 else "[dim]--[/dim]"
            pl_str  = (
                "[green]Yes[/green]" if p["pref_leader"] == 1
                else "[yellow]No[/yellow]" if p["pref_leader"] == 0
                else "[dim]--[/dim]"
            )
            ur_str  = f"[red]{p['under_rep']}[/red]" if p["under_rep"] > 0 else "[green]0[/green]"

            table.add_row(
                p["partition"],
                ld_str,
                rf_str,
                isr_str,
                _gap_str(p["rep_gap"]),
                pl_str,
                ur_str,
                _lag_ratio_str(p["lag_ratio"], p["lag_msgs"]),
                icon,
            )

            if p["health"] == "UNDER-REPLICATED":
                issues.append(
                    f"[red]✗[/red] [bold]{topic}[/bold] partition {p['partition']} -- "
                    f"under-replicated (ISR: {p['isr']}, RF: {p['rf']}, gap: {p['rep_gap']})"
                )
            elif p["health"] == "NO-LEADER":
                issues.append(
                    f"[red]✗[/red] [bold]{topic}[/bold] partition {p['partition']} -- no leader elected"
                )
            elif p["health"] == "NON-PREFERRED":
                issues.append(
                    f"[yellow]⚠[/yellow] [bold]{topic}[/bold] partition {p['partition']} -- "
                    "not on preferred leader"
                )
            if p["lag_ratio"] > 20.0:
                issues.append(
                    f"[red]✗[/red] [bold]{topic}[/bold] partition {p['partition']} -- "
                    f"consumer lag ratio critical ({p['lag_ratio']:.1f}% of stored messages)"
                )
            elif p["lag_ratio"] > 5.0:
                issues.append(
                    f"[yellow]⚠[/yellow] [bold]{topic}[/bold] partition {p['partition']} -- "
                    f"consumer lag ratio elevated ({p['lag_ratio']:.1f}% of stored messages)"
                )

        console.print(table)
        console.print()

    # ── Issue summary ─────────────────────────────────────────────────────────
    if issues:
        for issue in issues:
            console.print(f"  {issue}")
        console.print()

        has_critical = any("✗" in i for i in issues)
        if under_rep_count > 0 or no_leader_count > 0:
            console.print(Panel(
                f"[red]{under_rep_count} under-replicated and {no_leader_count} leaderless "
                f"partition(s) detected.[/red]\n"
                "  Replication gap > 0 means a replica has fallen behind the leader.\n"
                "  If RF=1 on all partitions, consider increasing the replication factor\n"
                "  to improve fault tolerance.",
                title="[bold red]Replication Integrity Warning[/bold red]",
                title_align="left",
                border_style="red",
                expand=False,
            ))
        elif has_critical:
            console.print(Panel(
                "  Consumer lag ratio is high on one or more partitions.\n"
                "  This means the consumer is significantly behind on stored messages.\n"
                "  Check Logstash pipeline health or increase consumer throughput.",
                title="[bold cyan]Consumer Pressure Warning[/bold cyan]",
                title_align="left",
                border_style="cyan",
                expand=False,
            ))
    else:
        console.print(
            "  [green]✓ All partitions healthy — leaders assigned, replicas in sync, "
            "consumer lag within normal range.[/green]"
        )

    # ── Legend ────────────────────────────────────────────────────────────────
    console.print()
    console.print(
        "  [dim]RF = Replication Factor   "
        "ISR = In-Sync Replicas   "
        "Rep Gap = RF − ISR   "
        "Lag Ratio = consumer lag as % of messages stored on disk[/dim]"
    )
    console.print()