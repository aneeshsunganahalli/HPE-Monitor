"""Poller JSONL history reader for monitor historical trend views."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


@dataclass
class PollerPoint:
    """Normalized cluster-level poller sample used for trend aggregation."""

    ts: int
    cpu_pct: float
    heap_used_bytes: float
    index_total: float | None


class PollerHistoryStore:
    """Read poller JSONL snapshots and build fixed-width trend buckets."""

    def __init__(self, data_dir: str | Path, bucket_seconds: int = 300) -> None:
        resolved_data_dir = Path(data_dir)
        if not resolved_data_dir.is_absolute():
            # Anchor relative paths to the repository root so monitor works from any cwd.
            repo_root = Path(__file__).resolve().parent.parent
            resolved_data_dir = repo_root / resolved_data_dir

        self._data_dir = resolved_data_dir
        self._bucket_seconds = max(1, bucket_seconds)

    def fetch_historical_trends(self, timeframe_minutes: int) -> dict[str, tuple[list[int], list[float]]]:
        """Return CPU, heap, and indexing-rate trend buckets for a timeframe."""
        if timeframe_minutes <= 0:
            return _empty_trends()

        now = int(time.time())
        start = now - (timeframe_minutes * 60)
        bucket_timestamps = list(range(start, now + 1, self._bucket_seconds))
        if not bucket_timestamps:
            bucket_timestamps = [start]

        points = self._load_recent_points(start_ts=start)
        if not points:
            return _empty_trends()

        cpu_values = [0.0] * len(bucket_timestamps)
        heap_values = [0.0] * len(bucket_timestamps)
        indexing_rate_values = [0.0] * len(bucket_timestamps)

        for point in points:
            if point.ts < start or point.ts > now:
                continue
            bucket_index = min((point.ts - start) // self._bucket_seconds, len(bucket_timestamps) - 1)
            cpu_values[bucket_index] = max(cpu_values[bucket_index], point.cpu_pct)
            heap_values[bucket_index] = max(heap_values[bucket_index], point.heap_used_bytes)

        prev_total: float | None = None
        prev_ts: int | None = None
        saw_index_total = False
        for point in points:
            if point.index_total is None:
                continue
            saw_index_total = True

            if prev_total is not None and prev_ts is not None:
                elapsed = max(1, point.ts - prev_ts)
                delta = point.index_total - prev_total
                rate = (delta / elapsed) if delta >= 0 else 0.0
                bucket_index = min(max(0, (point.ts - start) // self._bucket_seconds), len(bucket_timestamps) - 1)
                indexing_rate_values[bucket_index] = max(indexing_rate_values[bucket_index], rate)

            prev_total = point.index_total
            prev_ts = point.ts

        return {
            "cpu": (bucket_timestamps, cpu_values),
            "heap": (bucket_timestamps, heap_values),
            "indexing_rate": (bucket_timestamps, indexing_rate_values) if saw_index_total else ([], []),
        }

    def _load_recent_points(self, start_ts: int) -> list[PollerPoint]:
        points: list[PollerPoint] = []

        for path in self._candidate_files(start_ts=start_ts):
            try:
                with open(path, encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        point = _record_to_point(record)
                        if point is None or point.ts < start_ts:
                            continue
                        points.append(point)
            except OSError:
                continue

        if not points:
            return []

        # Keep the most recent record per second when duplicates exist.
        deduped: dict[int, PollerPoint] = {}
        for point in points:
            deduped[point.ts] = point

        return [deduped[ts] for ts in sorted(deduped.keys())]

    def _candidate_files(self, start_ts: int) -> list[Path]:
        if not self._data_dir.exists():
            return []

        start_day = datetime.fromtimestamp(start_ts, tz=timezone.utc).date()
        end_day = datetime.now(timezone.utc).date()
        if start_day > end_day:
            start_day = end_day

        paths: list[Path] = []
        day = start_day
        while day <= end_day:
            candidate = self._data_dir / f"metrics_{day.isoformat()}.jsonl"
            if candidate.exists():
                paths.append(candidate)
            day += timedelta(days=1)

        return paths


def _record_to_point(record: dict[str, Any]) -> PollerPoint | None:
    ts = _to_int(record.get("ts"))
    if ts is None:
        return None

    nodes = record.get("nodes", {})
    if not isinstance(nodes, dict):
        nodes = {}

    cpu_values: list[float] = []
    heap_values: list[float] = []
    index_total_sum = 0.0
    has_index_totals = False

    for node in nodes.values():
        if not isinstance(node, dict):
            continue

        cpu_pct = _to_float(node.get("cpu_pct"))
        if cpu_pct is not None:
            cpu_values.append(cpu_pct)

        heap_used = _to_float(node.get("heap_used_bytes"))
        if heap_used is not None:
            heap_values.append(heap_used)

        index_total = _to_float(node.get("index_total"))
        if index_total is not None:
            index_total_sum += index_total
            has_index_totals = True

    return PollerPoint(
        ts=ts,
        cpu_pct=max(cpu_values) if cpu_values else 0.0,
        heap_used_bytes=max(heap_values) if heap_values else 0.0,
        index_total=index_total_sum if has_index_totals else None,
    )


def _empty_trends() -> dict[str, tuple[list[int], list[float]]]:
    return {
        "cpu": ([], []),
        "heap": ([], []),
        "indexing_rate": ([], []),
    }


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
