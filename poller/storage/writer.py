"""
JSONL writer for poller metrics.

Appends one JSON line per poll cycle to a daily-rotating file under
`output_dir/metrics_YYYY-MM-DD.jsonl`. The output directory is created
automatically on first write.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path


def append_record(output_dir: str | Path, record: dict) -> Path:
    """
    Append *record* as one JSON line to today's JSONL file.

    Parameters
    ----------
    output_dir:
        Directory in which JSONL files are written.
    record:
        Fully-assembled poll-cycle dict (already contains 'ts' and
        'timestamp' keys from the orchestrator).

    Returns
    -------
    Path to the file that was written to.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = output_dir / f"metrics_{today}.jsonl"

    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, default=_json_default) + "\n")

    return path


def _json_default(obj):
    """Fallback serialiser — convert non-standard types to str."""
    return str(obj)
