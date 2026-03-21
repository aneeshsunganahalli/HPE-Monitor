"""
System-level collector for OpenSearch process metrics.

Collects two metrics that are NOT available from the OpenSearch API:

  fd_count          Number of open file descriptors held by the OpenSearch process.
  fd_limit          Hard limit on file descriptors for the process (ulimit -n).
  fd_pct            fd_count / fd_limit * 100

  io_read_bytes     Cumulative bytes read from storage by the OpenSearch process.
  io_write_bytes    Cumulative bytes written to storage by the OpenSearch process.
                    (The caller diffs these between polls to get a rate.)

Both are scoped to the OpenSearch JVM process — not the host system.

Sources
-------
  /proc/<pid>/fd/      — symlinks, one per open file descriptor
  resource module      — RLIMIT_NOFILE for the soft/hard fd limit
  /proc/<pid>/io       — kernel-reported process-level I/O byte counters
"""

from __future__ import annotations

import logging
import os
import resource
from typing import Any

import psutil

log = logging.getLogger(__name__)


def _find_pid(process_keyword: str) -> int | None:
    """Return PID of the process whose cmdline contains *process_keyword*."""
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmd = " ".join(proc.info["cmdline"] or [])
            if process_keyword in cmd:
                return proc.info["pid"]
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


def _fd_metrics(pid: int) -> dict[str, Any]:
    """Count open file descriptors via /proc/<pid>/fd/."""
    fd_dir = f"/proc/{pid}/fd"
    try:
        fd_count = len(os.listdir(fd_dir))
    except PermissionError:
        # Try psutil as secondary fallback (also reads /proc/<pid>/fd internally)
        try:
            fd_count = psutil.Process(pid).num_fds()
        except Exception:
            log.debug(
                "fd_count unavailable for pid %s — run poller as the opensearch OS user "
                "or with sudo to enable FD metric collection", pid
            )
            return {"permission_error": True}
    except FileNotFoundError:
        return {}

    soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    fd_pct = (fd_count / soft_limit * 100.0) if soft_limit > 0 else 0.0

    return {
        "fd_count": fd_count,
        "fd_limit": soft_limit,
        "fd_pct":   round(fd_pct, 2),
    }


def _io_metrics(pid: int) -> dict[str, Any]:
    """
    Read cumulative process-level I/O byte counters from /proc/<pid>/io.

    Returns read_bytes and write_bytes (kernel-accounted, not user-space cache).
    The orchestrator diffs consecutive readings to compute a per-interval rate.
    """
    io_path = f"/proc/{pid}/io"
    result: dict[str, Any] = {}
    try:
        with open(io_path, encoding="ascii") as fh:
            for line in fh:
                if line.startswith("read_bytes:"):
                    result["io_read_bytes"] = int(line.split(":")[1])
                elif line.startswith("write_bytes:"):
                    result["io_write_bytes"] = int(line.split(":")[1])
    except PermissionError:
        try:
            counters = psutil.Process(pid).io_counters()
            result["io_read_bytes"]  = counters.read_bytes
            result["io_write_bytes"] = counters.write_bytes
        except Exception:
            log.debug(
                "io_counters unavailable for pid %s — run poller as the opensearch OS user "
                "or with sudo to enable I/O metric collection", pid
            )
            result["permission_error"] = True
    except FileNotFoundError:
        pass

    return result


def collect(process_keyword: str) -> dict[str, Any]:
    """
    Collect FD and I/O metrics for the OpenSearch process.

    Parameters
    ----------
    process_keyword:
        String used to identify the OpenSearch JVM process in cmdline
        (e.g. 'org.opensearch.bootstrap.OpenSearch').

    Returns
    -------
    Flat dict with all available metrics plus 'pid'.
    Returns {} if the process is not found.
    """
    pid = _find_pid(process_keyword)
    if pid is None:
        log.warning("OpenSearch process not found (keyword=%r)", process_keyword)
        return {}

    metrics: dict[str, Any] = {"pid": pid}
    metrics.update(_fd_metrics(pid))
    metrics.update(_io_metrics(pid))
    return metrics
