#!/usr/bin/env python3
import requests
import math
import urllib3
from datetime import datetime, timedelta

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Connection Config ──────────────────────────────────────────────────────────
PROMETHEUS_URL = "https://localhost:9090"
AUTH = ("admin", "password")

# ── Full Metric Catalog ────────────────────────────────────────────────────────
METRICS = {
    # ── System (node_exporter) ──────────────────────────────────────────────────
    "1":  {"name": "CPU Usage %",
           "query": '100 - (avg by(instance)(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)',
           "unit": "%",    "fmt": "pct", "service": "System",     "category": "cpu"},
    "2":  {"name": "System Load 1m",
           "query": "node_load1",
           "unit": "",     "fmt": "dec", "service": "System",     "category": "cpu"},
    "3":  {"name": "System Load 5m",
           "query": "node_load5",
           "unit": "",     "fmt": "dec", "service": "System",     "category": "cpu"},
    "4":  {"name": "System Load 15m",
           "query": "node_load15",
           "unit": "",     "fmt": "dec", "service": "System",     "category": "cpu"},
    "5":  {"name": "RAM Usage %",
           "query": "(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100",
           "unit": "%",    "fmt": "pct", "service": "System",     "category": "memory"},
    "6":  {"name": "RAM Available",
           "query": "node_memory_MemAvailable_bytes / 1024^3",
           "unit": "GB",   "fmt": "gb",  "service": "System",     "category": "memory"},
    "7":  {"name": "RAM Total",
           "query": "node_memory_MemTotal_bytes / 1024^3",
           "unit": "GB",   "fmt": "gb",  "service": "System",     "category": "memory"},
    "8":  {"name": "Swap Used",
           "query": "(node_memory_SwapTotal_bytes - node_memory_SwapFree_bytes) / 1024^3",
           "unit": "GB",   "fmt": "gb",  "service": "System",     "category": "memory"},
    "9":  {"name": "Swap Total",
           "query": "node_memory_SwapTotal_bytes / 1024^3",
           "unit": "GB",   "fmt": "gb",  "service": "System",     "category": "memory"},
    "10": {"name": "Disk Available (/)",
           "query": 'node_filesystem_avail_bytes{mountpoint="/"} / 1024^3',
           "unit": "GB",   "fmt": "gb",  "service": "System",     "category": "disk"},
    "11": {"name": "Disk Total (/)",
           "query": 'node_filesystem_size_bytes{mountpoint="/"} / 1024^3',
           "unit": "GB",   "fmt": "gb",  "service": "System",     "category": "disk"},
    "12": {"name": "Disk Read Rate",
           "query": "rate(node_disk_read_bytes_total[5m]) / 1024^2",
           "unit": "MB/s", "fmt": "dec", "service": "System",     "category": "disk"},
    "13": {"name": "Disk Write Rate",
           "query": "rate(node_disk_written_bytes_total[5m]) / 1024^2",
           "unit": "MB/s", "fmt": "dec", "service": "System",     "category": "disk"},
    "14": {"name": "Network In Rate",
           "query": 'rate(node_network_receive_bytes_total{device="enp0s3"}[5m]) / 1024^2',
           "unit": "MB/s", "fmt": "dec", "service": "System",     "category": "network"},
    "15": {"name": "Network Out Rate",
           "query": 'rate(node_network_transmit_bytes_total{device="enp0s3"}[5m]) / 1024^2',
           "unit": "MB/s", "fmt": "dec", "service": "System",     "category": "network"},

    # ── Kafka (kafka_exporter) ──────────────────────────────────────────────────
    "16": {"name": "Active Brokers",
           "query": "kafka_brokers",
           "unit": "",     "fmt": "int", "service": "Kafka",      "category": "health"},
    "17": {"name": "Consumer Lag",
           "query": 'kafka_consumergroup_lag{topic="test-topic"}',
           "unit": "msgs", "fmt": "int", "service": "Kafka",      "category": "lag"},
    "18": {"name": "Consumer Lag Sum",
           "query": 'kafka_consumergroup_lag_sum{topic="test-topic"}',
           "unit": "msgs", "fmt": "int", "service": "Kafka",      "category": "lag"},
    "19": {"name": "Consumer Offset",
           "query": 'kafka_consumergroup_current_offset{topic="test-topic"}',
           "unit": "",     "fmt": "int", "service": "Kafka",      "category": "throughput"},
    "20": {"name": "Messages Produced (Offset)",
           "query": 'kafka_topic_partition_current_offset{topic="test-topic"}',
           "unit": "",     "fmt": "int", "service": "Kafka",      "category": "throughput"},
    "21": {"name": "Oldest Offset",
           "query": 'kafka_topic_partition_oldest_offset{topic="test-topic"}',
           "unit": "",     "fmt": "int", "service": "Kafka",      "category": "throughput"},
    "22": {"name": "Consumer Members",
           "query": "kafka_consumergroup_members",
           "unit": "",     "fmt": "int", "service": "Kafka",      "category": "health"},
    "23": {"name": "Under-Replicated Partitions",
           "query": "kafka_topic_partition_under_replicated_partition",
           "unit": "",     "fmt": "int", "service": "Kafka",      "category": "health"},

    # ── Logstash (logstash_exporter) ────────────────────────────────────────────
    "24": {"name": "Logstash Up",
           "query": "logstash_info_up",
           "unit": "",     "fmt": "int", "service": "Logstash",   "category": "health"},
    "25": {"name": "Pipeline Up",
           "query": "logstash_stats_pipeline_up",
           "unit": "",     "fmt": "int", "service": "Logstash",   "category": "health"},
    "26": {"name": "Pipeline Reload Failures",
           "query": "logstash_stats_pipeline_reloads_failures",
           "unit": "",     "fmt": "int", "service": "Logstash",   "category": "health"},
    "27": {"name": "JVM Heap Used %",
           "query": "logstash_stats_jvm_mem_heap_used_percent",
           "unit": "%",    "fmt": "pct", "service": "Logstash",   "category": "memory"},
    "28": {"name": "JVM Heap Used",
           "query": "logstash_stats_jvm_mem_heap_used_bytes / 1024^3",
           "unit": "GB",   "fmt": "gb",  "service": "Logstash",   "category": "memory"},
    "29": {"name": "JVM Heap Max",
           "query": "logstash_stats_jvm_mem_heap_max_bytes / 1024^3",
           "unit": "GB",   "fmt": "gb",  "service": "Logstash",   "category": "memory"},
    "30": {"name": "Process CPU %",
           "query": "logstash_stats_process_cpu_percent",
           "unit": "%",    "fmt": "pct", "service": "Logstash",   "category": "cpu"},
    "31": {"name": "Process Load 1m",
           "query": "logstash_stats_process_cpu_load_average_1m",
           "unit": "",     "fmt": "dec", "service": "Logstash",   "category": "cpu"},
    "32": {"name": "Process Load 5m",
           "query": "logstash_stats_process_cpu_load_average_5m",
           "unit": "",     "fmt": "dec", "service": "Logstash",   "category": "cpu"},
    "33": {"name": "Events In (total)",
           "query": "logstash_stats_pipeline_events_in",
           "unit": "",     "fmt": "int", "service": "Logstash",   "category": "throughput"},
    "34": {"name": "Events Out (total)",
           "query": "logstash_stats_pipeline_events_out",
           "unit": "",     "fmt": "int", "service": "Logstash",   "category": "throughput"},
    "35": {"name": "Input Flow (current rate)",
           "query": "logstash_stats_flow_input_current",
           "unit": "e/s",  "fmt": "dec", "service": "Logstash",   "category": "throughput"},
    "36": {"name": "Output Flow (current rate)",
           "query": "logstash_stats_flow_output_current",
           "unit": "e/s",  "fmt": "dec", "service": "Logstash",   "category": "throughput"},
    "37": {"name": "Queue Events Count",
           "query": "logstash_stats_pipeline_queue_events_count",
           "unit": "",     "fmt": "int", "service": "Logstash",   "category": "lag"},
    "38": {"name": "Queue Backpressure",
           "query": "logstash_stats_flow_queue_backpressure_current",
           "unit": "",     "fmt": "dec", "service": "Logstash",   "category": "lag"},
    "39": {"name": "JVM Threads",
           "query": "logstash_stats_jvm_threads_count",
           "unit": "",     "fmt": "int", "service": "Logstash",   "category": "health"},
    "40": {"name": "Open File Descriptors",
           "query": "logstash_stats_process_open_file_descriptors",
           "unit": "",     "fmt": "int", "service": "Logstash",   "category": "health"},

    # ── OpenSearch (opensearch exporter) ────────────────────────────────────────
    "41": {"name": "Cluster Status",
           "query": "opensearch_cluster_status",
           "unit": "",     "fmt": "int", "service": "OpenSearch", "category": "health"},
    "42": {"name": "Active Shards %",
           "query": "opensearch_cluster_shards_active_percent",
           "unit": "%",    "fmt": "pct", "service": "OpenSearch", "category": "health"},
    "43": {"name": "Indexing Failed Count",
           "query": "opensearch_index_indexing_index_failed_count",
           "unit": "",     "fmt": "int", "service": "OpenSearch", "category": "health"},
    "44": {"name": "JVM Heap Used %",
           "query": "opensearch_jvm_mem_heap_used_percent",
           "unit": "%",    "fmt": "pct", "service": "OpenSearch", "category": "memory"},
    "45": {"name": "JVM Heap Used",
           "query": "opensearch_jvm_mem_heap_used_bytes / 1024^3",
           "unit": "GB",   "fmt": "gb",  "service": "OpenSearch", "category": "memory"},
    "46": {"name": "JVM Heap Max",
           "query": "opensearch_jvm_mem_heap_max_bytes / 1024^3",
           "unit": "GB",   "fmt": "gb",  "service": "OpenSearch", "category": "memory"},
    "47": {"name": "OS Memory Used %",
           "query": "opensearch_os_mem_used_percent",
           "unit": "%",    "fmt": "pct", "service": "OpenSearch", "category": "memory"},
    "48": {"name": "OS Memory Free",
           "query": "opensearch_os_mem_free_bytes / 1024^3",
           "unit": "GB",   "fmt": "gb",  "service": "OpenSearch", "category": "memory"},
    "49": {"name": "Process CPU %",
           "query": "opensearch_process_cpu_percent",
           "unit": "%",    "fmt": "pct", "service": "OpenSearch", "category": "cpu"},
    "50": {"name": "Documents Stored",
           "query": "sum(opensearch_index_indexing_index_count{index=~\"kafka-logs-.*\",context=\"primaries\"})",
           "unit": "",     "fmt": "int", "service": "OpenSearch", "category": "throughput"},
    "51": {"name": "Indexing Rate (current)",
           "query": "sum(opensearch_index_indexing_index_current_number{index=~\"kafka-logs-.*\",context=\"primaries\"})",
           "unit": "",     "fmt": "int", "service": "OpenSearch", "category": "throughput"},
    "52": {"name": "Disk Available",
           "query": "opensearch_fs_total_available_bytes / 1024^3",
           "unit": "GB",   "fmt": "gb",  "service": "OpenSearch", "category": "disk"},
    "53": {"name": "JVM Threads",
           "query": "opensearch_jvm_threads_number",
           "unit": "",     "fmt": "int", "service": "OpenSearch", "category": "health"},
    "54": {"name": "Open File Descriptors",
           "query": "opensearch_process_file_descriptors_open_number",
           "unit": "",     "fmt": "int", "service": "OpenSearch", "category": "health"},
    "55": {"name": "Circuit Breaker Trips",
           "query": "opensearch_circuitbreaker_tripped_count",
           "unit": "",     "fmt": "int", "service": "OpenSearch", "category": "health"},
}

CATEGORIES = {
    "memory":     "Memory usage across all services",
    "cpu":        "CPU & Load across all services",
    "throughput": "Data throughput across the pipeline",
    "lag":        "Pipeline lag & backpressure",
    "health":     "Health & availability of all services",
    "disk":       "Disk usage across system & OpenSearch",
}

SERVICE_ORDER = ["System", "Kafka", "Logstash", "OpenSearch"]

# ── Helpers ────────────────────────────────────────────────────────────────────
def fmt_val(v, style):
    v = float(v)
    if style == "pct": return f"{v:.2f} %"
    if style == "gb":  return f"{v:.3f} GB"
    if style == "int": return f"{int(v)}"
    return f"{v:.4f}"

def query_range(promql, minutes):
    end   = datetime.now()
    start = end - timedelta(minutes=minutes)
    step  = max(15, math.ceil((minutes * 60) / 25))
    try:
        r = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query_range",
            params={"query": promql,
                    "start": start.timestamp(),
                    "end":   end.timestamp(),
                    "step":  f"{step}s"},
            auth=AUTH, verify=False, timeout=10
        )
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        print("\n  ❌  Cannot connect to Prometheus.")
        print("      Is the CPAM stack running? Try: systemctl status prometheus")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"\n  ❌  HTTP Error: {e}")
        return None
    except Exception as e:
        print(f"\n  ❌  Unexpected error: {e}")
        return None

def display_metric(meta, data, minutes):
    W = 64
    if not data or data.get("status") != "success":
        print(f"\n  ❌  Query failed or returned an error.")
        return
    results = data["data"]["result"]
    if not results:
        print(f"\n  ⚠️   No data for: {meta['name']}")
        print(f"      The metric may not exist yet or the pipeline is idle.")
        return

    print(f"\n{'═' * W}")
    svc_tag = f"[{meta['service']}]" if "service" in meta else ""
    title   = f"  📊  {svc_tag} {meta['name']}"
    unit    = f"  ({meta['unit']})" if meta.get('unit') else ""
    print(f"{title}{unit}")
    print(f"  ⏱️   Last {minutes} minute(s)  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'─' * W}")

    for series in results:
        labels = {k: v for k, v in series.get("metric", {}).items() if k != "__name__"}
        if labels:
            lbl_str = "  🏷   " + "  ".join(f"{k}={v}" for k, v in labels.items())
            print(lbl_str[:W - 1])

        values = series.get("values", [])
        if not values:
            print("  (no values in this series)")
            continue

        print(f"\n  {'Timestamp':<26}  Value")
        print(f"  {'─' * 44}")

        if len(values) <= 12:
            rows = values
        else:
            rows = values[:5] + [None] + values[-5:]

        for item in rows:
            if item is None:
                print(f"  {'  ··· (middle values trimmed) ···':<44}")
                continue
            ts, v = item
            dt = datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d  %H:%M:%S")
            print(f"  {dt:<26}  {fmt_val(v, meta['fmt'])}")

        all_v = [float(v[1]) for v in values]
        avg   = sum(all_v) / len(all_v)
        print(f"\n  ┌─ Summary {'─' * 40}┐")
        print(f"  │  {'Latest':<12}: {fmt_val(all_v[-1], meta['fmt']):<36} │")
        print(f"  │  {'Min':<12}: {fmt_val(min(all_v),  meta['fmt']):<36} │")
        print(f"  │  {'Max':<12}: {fmt_val(max(all_v),  meta['fmt']):<36} │")
        print(f"  │  {'Average':<12}: {fmt_val(avg,       meta['fmt']):<36} │")
        print(f"  │  {'Data Points':<12}: {len(all_v):<36} │")
        print(f"  └{'─' * 51}┘")

    print(f"{'═' * W}\n")

def run_category(cat_key, minutes):
    cat_metrics = [
        (num, m) for num, m in METRICS.items() if m["category"] == cat_key
    ]
    cat_metrics.sort(key=lambda x: SERVICE_ORDER.index(x[1]["service"])
                     if x[1]["service"] in SERVICE_ORDER else 99)

    W = 64
    print(f"\n{'█' * W}")
    print(f"  🗂   CATEGORY : {cat_key.upper()}  —  {CATEGORIES[cat_key]}")
    print(f"  ⏱️   Last {minutes} minute(s)  |  {len(cat_metrics)} metrics across all services")
    print(f"{'█' * W}")

    current_service = None
    for num, meta in cat_metrics:
        if meta["service"] != current_service:
            current_service = meta["service"]
            print(f"\n  {'▶ ' + current_service + ' ':─<{W - 2}}")

        print(f"  ⏳  Querying [{num}] {meta['name']} ...", end="", flush=True)
        result = query_range(meta["query"], minutes)
        print(" done." if result else "")
        display_metric(meta, result, minutes)

# ── Menu ───────────────────────────────────────────────────────────────────────
def show_menu():
    W = 64
    print(f"\n{'═' * W}")
    print(f"{'  CPAM — Prometheus CLI Query Tool':^{W}}")
    print(f"{'═' * W}")

    sections = [
        ("💻  SYSTEM  (node_exporter)",  [str(i) for i in range(1,  16)]),
        ("📨  KAFKA   (kafka_exporter)", [str(i) for i in range(16, 24)]),
        ("⚙️   LOGSTASH (logstash_exporter)", [str(i) for i in range(24, 41)]),
        ("🔍  OPENSEARCH (opensearch exporter)", [str(i) for i in range(41, 56)]),
    ]

    for section_title, nums in sections:
        print(f"\n  {section_title}")
        for i in range(0, len(nums), 3):
            row = nums[i:i+3]
            line = ""
            for n in row:
                m = METRICS[n]
                line += f"  [{n:>2}] {m['name']:<22}"
            print(line)

    print(f"\n{'─' * W}")
    print("  🗂   CROSS-SERVICE CATEGORIES  (type the keyword):")
    for k, v in CATEGORIES.items():
        print(f"       {k:<12} → {v}")
    print(f"{'─' * W}")
    print("   [C]  Custom PromQL query          [Q]  Quit")
    print(f"{'═' * W}")

# ── Duration Input ─────────────────────────────────────────────────────────────
def get_duration():
    while True:
        d = input("  Duration in minutes (e.g. 5, 30, 60): ").strip()
        try:
            val = int(d)
            if val > 0:
                return val
            print("  ⚠️   Please enter a number greater than 0.")
        except ValueError:
            print("  ⚠️   Please enter a whole number (e.g. 10).")

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("\n  🚀  CPAM Prometheus CLI Query Tool  —  Starting up...")
    print(f"  🔗  Prometheus : {PROMETHEUS_URL}")
    print(f"  🔐  Auth       : Basic (admin:***)")
    print(f"  📦  Metrics    : {len(METRICS)} loaded  |  Categories : {len(CATEGORIES)}\n")

    while True:
        show_menu()
        choice = input("\n  Your choice: ").strip()

        if choice.upper() == "Q":
            print("\n  👋  Goodbye! CPAM session ended.\n")
            break

        elif choice.upper() == "C":
            q = input("  Enter your PromQL expression: ").strip()
            if not q:
                print("  ⚠️   No expression entered.")
                continue
            meta = {"name": f"Custom Query", "query": q,
                    "unit": "", "fmt": "dec", "service": "Custom"}
            dur = get_duration()
            print("  ⏳  Querying Prometheus...", end="", flush=True)
            result = query_range(q, dur)
            print(" done." if result else "")
            display_metric(meta, result, dur)

        elif choice.lower() in CATEGORIES:
            dur = get_duration()
            run_category(choice.lower(), dur)

        elif choice in METRICS:
            meta = METRICS[choice]
            dur  = get_duration()
            print(f"  ⏳  Querying [{choice}] {meta['name']} ...", end="", flush=True)
            result = query_range(meta["query"], dur)
            print(" done." if result else "")
            display_metric(meta, result, dur)

        else:
            print("  ⚠️   Invalid choice. Enter a number (1–55), a category keyword, C, or Q.")
            continue

        if input("\n  Press Enter to return to menu, or [Q] to quit: ").strip().upper() == "Q":
            print("\n  👋  Goodbye! CPAM session ended.\n")
            break

if __name__ == "__main__":
    main()
