import json
import math
import re
import time
import urllib.parse
from typing import Any

from monitor.config import (
  PROMETHEUS_HOST,
    PROMETHEUS_PORT,
    PROMETHEUS_SCHEME,
    PROMETHEUS_TIMEOUT_SECONDS,
    PA_HOST,
    PA_PORT,
    PA_SCHEME,
    PA_TIMEOUT_SECONDS,
    console
)

import urllib3

from dataclasses import dataclass
from monitor.utils import is_realtime_timeframe, timeframe_to_minutes, timeframe_to_prometheus_range

@dataclass
class TrendSeries:
    """Normalized time-series payload used by historical trend views."""

    label: str
    values: list[float]
    timestamps: list[int]
    unit: str

    @property
    def peak(self) -> float:
        return max(self.values) if self.values else 0.0

    @property
    def latest(self) -> float:
        return self.values[-1] if self.values else 0.0


class MetricsProvider:
    """Route metric requests between OpenSearch, Prometheus, and Performance Analyzer."""

    def __init__(self) -> None:
        self._http = urllib3.PoolManager()
        self._prometheus_base = f"{PROMETHEUS_SCHEME}://{PROMETHEUS_HOST}:{PROMETHEUS_PORT}"
        self._pa_base = f"{PA_SCHEME}://{PA_HOST}:{PA_PORT}"
        self._warned_contexts: set[str] = set()

    def route_source(self, timeframe: str, historical: bool = False) -> str:
        """
        Decide the metric backend from the requested timeframe.

        Rules:
        - "real-time" -> OpenSearch _nodes/stats
        - >1h          -> Prometheus PromQL
        - Historical UI views can force Prometheus regardless of timeframe.
        """
        if is_realtime_timeframe(timeframe):
            return "opensearch"
        if historical or timeframe_to_minutes(timeframe) > 60:
            return "prometheus"
        return "opensearch"

    def fetch_node_stats(self, timeframe: str = "1h") -> dict[str, Any]:
        """
        Return a node snapshot for table views.

        For >1h windows we still issue a PromQL query first (to satisfy source
        routing) but render table detail from current node stats.
        """
        source = self.route_source(timeframe=timeframe)
        if source == "prometheus":
            # Routing requirement: long windows should hit Prometheus via PromQL.
            self.fetch_prometheus_series(
                label="CPU (5m max)",
                query="max_over_time(opensearch_os_cpu_percent[5m])",
                timeframe=timeframe,
                unit="%",
            )
        return self._fetch_node_stats_from_opensearch()

    def fetch_historical_trends(self, timeframe: str) -> dict[str, TrendSeries]:
        """Fetch 5-minute bucketed max_over_time trend lines from Prometheus."""
        effective_tf = "1h" if is_realtime_timeframe(timeframe) else timeframe
        return {
            "cpu": self.fetch_prometheus_series(
                label="CPU",
                query="max_over_time(opensearch_os_cpu_percent[5m])",
                timeframe=effective_tf,
                unit="%",
            ),
            "heap": self.fetch_prometheus_series(
                label="JVM Heap",
                query="max_over_time(opensearch_jvm_mem_heap_used_bytes[5m])",
                timeframe=effective_tf,
                unit="bytes",
            ),
            "indexing_rate": self.fetch_prometheus_series(
                label="Indexing Rate",
                query="sum(rate(opensearch_indices_indexing_index_total[5m]))",
                fallback_query="sum(rate(opensearch_indices_indexing_index_count[5m]))",
                timeframe=effective_tf,
                unit="ops/s",
            ),
        }

    def fetch_prometheus_series(
        self,
        label: str,
        query: str,
        timeframe: str,
        unit: str,
        step: str = "5m",
        fallback_query: str | None = None,
    ) -> TrendSeries:
        """Run a Prometheus range query and collapse multiple series into one line."""
        payload, start, end, step_seconds = self._prometheus_query_range(
            query=query,
            timeframe=timeframe,
            step=step,
        )
        has_primary_samples = self._has_prometheus_samples(payload)
        timestamps, values = self._collapse_prometheus_result(
            payload,
            start=start,
            end=end,
            step_seconds=step_seconds,
        )

        # Preserve backwards compatibility for environments exposing alternate indexing counters.
        if fallback_query and not has_primary_samples:
            payload, start, end, step_seconds = self._prometheus_query_range(
                query=fallback_query,
                timeframe=timeframe,
                step=step,
            )
            timestamps, values = self._collapse_prometheus_result(
                payload,
                start=start,
                end=end,
                step_seconds=step_seconds,
            )

        return TrendSeries(label=label, values=values, timestamps=timestamps, unit=unit)

    def fetch_performance_analyzer_metrics(self, node_name: str) -> dict[str, float | None]:
        """
        Pull bottleneck metrics from Performance Analyzer on port 9600.

        Returns:
          - disk_utilization
          - io_tot_wait
        """
        payload, raw_text = self._request_json(
            base_url=self._pa_base,
            path="/_plugins/_performanceanalyzer/metrics",
            params={"metrics": "Disk_Utilization,IO_TotWait", "agg": "avg,1m"},
            timeout_seconds=PA_TIMEOUT_SECONDS,
            warn_context="performance analyzer metrics",
        )

        disk_util = self._extract_metric_value(payload, raw_text, "Disk_Utilization")
        io_wait = self._extract_metric_value(payload, raw_text, "IO_TotWait")

        return {
            "disk_utilization": disk_util,
            "io_tot_wait": io_wait,
        }

    def _fetch_node_stats_from_opensearch(self) -> dict[str, Any]:
        # Local import avoids module-load circular dependency with monitor.client.
        from monitor.client import get_os_client

        client = get_os_client()
        return client.nodes.stats(metric="os,jvm,fs,indices")

    def _prometheus_query_range(self, query: str, timeframe: str, step: str) -> tuple[dict[str, Any], int, int, int]:
        minutes = max(timeframe_to_minutes(timeframe), 5)
        now = int(time.time())
        start = now - (minutes * 60)
        step_seconds = self._step_to_seconds(step)

        # Keep a valid range string around for downstream logging/debugging.
        _ = timeframe_to_prometheus_range(timeframe)

        payload, _raw_text = self._request_json(
            base_url=self._prometheus_base,
            path="/api/v1/query_range",
            params={
                "query": query,
                "start": start,
                "end": now,
                "step": step,
            },
            timeout_seconds=PROMETHEUS_TIMEOUT_SECONDS,
            warn_context="prometheus query_range",
        )
        return payload, start, now, step_seconds

    @staticmethod
    def _step_to_seconds(step: str) -> int:
        """Convert Prometheus step values (e.g., '5m') into seconds."""
        m = re.fullmatch(r"\s*(\d+)\s*([smhd])\s*", step.strip().lower())
        if not m:
            return 300

        value = int(m.group(1))
        unit = m.group(2)
        multipliers = {
            "s": 1,
            "m": 60,
            "h": 3600,
            "d": 86400,
        }
        return max(1, value * multipliers.get(unit, 60))

    @staticmethod
    def _has_prometheus_samples(payload: dict[str, Any]) -> bool:
        """Return True when Prometheus matrix results include at least one datapoint."""
        if payload.get("status") != "success":
            return False

        results = payload.get("data", {}).get("result", [])
        if not isinstance(results, list):
            return False

        for series in results:
            if isinstance(series, dict) and isinstance(series.get("values"), list) and series.get("values"):
                return True
        return False

    @staticmethod
    def _collapse_prometheus_result(
        payload: dict[str, Any],
        start: int,
        end: int,
        step_seconds: int,
    ) -> tuple[list[int], list[float]]:
        """Collapse series into a max-per-timestamp line and zero-fill missing buckets."""
        if payload.get("status") != "success":
            return [], []

        results = payload.get("data", {}).get("result", [])
        if not isinstance(results, list):
            return [], []

        if start > end:
            return [], []

        expected_timestamps = list(range(start, end + 1, max(1, step_seconds)))
        if not expected_timestamps:
            expected_timestamps = [start]

        expected_set = set(expected_timestamps)

        by_timestamp: dict[int, list[float]] = {}
        for series in results:
            values = series.get("values", []) if isinstance(series, dict) else []
            for pair in values:
                if not isinstance(pair, list) or len(pair) != 2:
                    continue
                ts_raw, value_raw = pair
                value = _to_float(value_raw)
                if value is None:
                    continue
                ts = int(float(ts_raw))

                # Query results can drift by a second due to evaluation timing; snap to nearest bucket.
                if ts not in expected_set:
                    offset = round((ts - start) / max(1, step_seconds))
                    snapped = start + (offset * step_seconds)
                    if snapped < start or snapped > end:
                        continue
                    ts = snapped

                by_timestamp.setdefault(ts, []).append(value)

        collapsed = [max(by_timestamp.get(ts, [0.0])) for ts in expected_timestamps]
        return expected_timestamps, collapsed

    def _request_json(
        self,
        base_url: str,
        path: str,
        params: dict[str, Any],
        timeout_seconds: int,
        warn_context: str,
    ) -> tuple[dict[str, Any], str]:
        """Perform an HTTP GET and parse JSON, returning (payload, raw_text)."""
        query = urllib.parse.urlencode(params, doseq=True)
        url = f"{base_url}{path}?{query}" if query else f"{base_url}{path}"

        try:
            response = self._http.request(
                "GET",
                url,
                timeout=urllib3.Timeout(connect=timeout_seconds, read=timeout_seconds),
            )
        except Exception as exc:
            self._warn_once(warn_context, f"Unable to reach {warn_context}: {exc}")
            return {}, ""

        raw_text = response.data.decode("utf-8", errors="replace")

        if response.status >= 400:
            self._warn_once(warn_context, f"{warn_context} returned HTTP {response.status}.")
            return {}, raw_text

        if not raw_text.strip():
            return {}, ""

        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError:
            payload = {}

        return payload, raw_text

    def _warn_once(self, context: str, message: str) -> None:
        """Emit a warning message once per backend context to keep CLI output readable."""
        if context in self._warned_contexts:
            return
        self._warned_contexts.add(context)
        console.print(f"[yellow]{message}[/yellow]")

    def _extract_metric_value(
        self,
        payload: dict[str, Any],
        raw_text: str,
        metric_name: str,
    ) -> float | None:
        """Extract a numeric metric value from JSON payload, falling back to regex text parsing."""
        candidates: list[float] = []
        metric_key = metric_name.lower()

        def walk_json(obj: Any) -> None:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    key_l = str(key).lower()
                    if metric_key in key_l:
                        candidates.extend(_collect_numeric_values(value))
                    walk_json(value)
                return

            if isinstance(obj, list):
                for item in obj:
                    walk_json(item)

        walk_json(payload)

        if not candidates and raw_text:
            pattern = re.compile(
                rf"{re.escape(metric_name)}[^\d\-]*(-?\d+(?:\.\d+)?)",
                re.IGNORECASE,
            )
            candidates = [_to_float(match) for match in pattern.findall(raw_text)]
            candidates = [v for v in candidates if v is not None]

        if not candidates:
            return None
        return max(candidates)


def _collect_numeric_values(value: Any) -> list[float]:
    """Recursively collect numeric values from nested JSON-like structures."""
    values: list[float] = []

    if isinstance(value, dict):
        for nested in value.values():
            values.extend(_collect_numeric_values(nested))
        return values

    if isinstance(value, list):
        for nested in value:
            values.extend(_collect_numeric_values(nested))
        return values

    parsed = _to_float(value)
    if parsed is not None:
        values.append(parsed)
    return values


def _to_float(value: Any) -> float | None:
    """Safely convert a value to float; return None for non-finite or invalid numbers."""
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


_METRICS_PROVIDER = MetricsProvider()


def get_metrics_provider() -> MetricsProvider:
    """Return the shared metrics provider instance."""
    return _METRICS_PROVIDER



def fetch_historical_trends(timeframe: str) -> dict[str, TrendSeries]:
    """Fetch Prometheus-backed historical trend series for OpenSearch metrics."""
    try:
        return get_metrics_provider().fetch_historical_trends(timeframe=timeframe)
    except Exception as e:
        console.print(f"[red]Error fetching historical trends:[/red] {e}")
        return {
            "cpu": TrendSeries(label="CPU", values=[], timestamps=[], unit="%"),
            "heap": TrendSeries(label="JVM Heap", values=[], timestamps=[], unit="bytes"),
            "indexing_rate": TrendSeries(label="Indexing Rate", values=[], timestamps=[], unit="ops/s"),
        }