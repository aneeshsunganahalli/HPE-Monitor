import psutil
import subprocess
import json
import time
import re
from datetime import datetime
from zoneinfo import ZoneInfo


# JVM process identifiers
services = {
    "kafka": "kafka.Kafka",
    "logstash": "org.logstash.Logstash",
    "opensearch": "org.opensearch.bootstrap.OpenSearch"
}

output_file = "memory_metrics.jsonl"


def find_pid(process_keyword):
    """
    Find PID of a JVM service using its process keyword
    """
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmd = " ".join(proc.info['cmdline'])
            if process_keyword in cmd:
                return proc.info['pid']
        except:
            continue
    return None


def get_heap(pid):
    try:
        output = subprocess.check_output(
            f"jstat -gc {pid}",
            shell=True,
            text=True
        )

        lines = output.split("\n")

        values = lines[1].split()

        EC = float(values[4])
        EU = float(values[5])
        OC = float(values[6])
        OU = float(values[7])

        heap_used = EU + OU
        heap_max = EC + OC
        
        heap_used /=1024
        heap_max /=1024


        heap_percent = (heap_used / heap_max) * 100

        return heap_used, heap_max, heap_percent

    except:
        return None, None, None
print("Starting JVM Memory Monitor...\n")

while True:

    data = {
        "timestamp": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
        "services": {}
    }

    for service, keyword in services.items():

        pid = find_pid(keyword)

        if pid:

            process = psutil.Process(pid)

            mem = process.memory_info()

            cpu = process.cpu_percent(interval=0.1)

            heap_used, heap_max, heap_percent = get_heap(pid)

            service_data = {
                "pid": pid,
                "rss_mb": round(mem.rss / (1024 * 1024), 2),
                "vms_mb": round(mem.vms / (1024 * 1024), 2),
                "cpu_percent": cpu,
                "heap_used_mb": heap_used,
                "heap_max_mb": heap_max,
                "heap_usage_percent": heap_percent
            }

            data["services"][service] = service_data

        else:
            data["services"][service] = "not running"

    # Print metrics live
    print(json.dumps(data, indent=2))

    # Save metrics
    with open(output_file, "a") as f:
        f.write(json.dumps(data) + "\n")

    time.sleep(5)
