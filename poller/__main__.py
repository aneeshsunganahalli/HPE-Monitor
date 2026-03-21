"""
Entry point for the OpenSearch poller.

Run as:
    python -m poller
    python -m poller --interval 30
    python -m poller --output-dir /var/log/hpe-monitor/poller --verbose
"""

from __future__ import annotations

import argparse
from pathlib import Path

from poller.config import POLL_INTERVAL_SECONDS, DEFAULT_OUTPUT_DIR


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m poller",
        description=(
            "Continuously poll OpenSearch metrics and append them to JSONL files.\n"
            "Metrics are scoped to the OpenSearch process (not system-wide):\n"
            "  CPU%        — process.cpu.percent\n"
            "  Heap%       — JVM heap used / max\n"
            "  Disk%       — OpenSearch store bytes / filesystem capacity\n"
            "  Thread pool — write/search queue depth and rejection rates\n"
            "  FD count    — open file descriptors vs ulimit\n"
            "  I/O rate    — process-level read/write bytes per second\n"
            "  GC rate     — ms of GC pause per second of wall time"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=POLL_INTERVAL_SECONDS,
        metavar="SECONDS",
        help=f"Seconds between poll cycles (default: {POLL_INTERVAL_SECONDS})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        metavar="PATH",
        help=f"Directory for JSONL output files (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Print each record to stdout in addition to writing to file",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    from poller.poller import run
    run(
        output_dir=args.output_dir,
        interval=args.interval,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
