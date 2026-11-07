"""Check local scrape/dashboard provisioning and a real exported trace."""
import argparse
from base64 import b64encode
import json
import os
from pathlib import Path
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("manifest", type=Path)
args = parser.parse_args()
trace_id = json.loads(args.manifest.read_text())["trace_id"]

def get(url, auth=None):
    request = Request(url)
    if auth:
        request.add_header("Authorization", "Basic " + b64encode(auth.encode()).decode())
    with urlopen(request, timeout=5) as response:
        return json.loads(response.read())

for _ in range(30):
    targets = get("http://localhost:9090/api/v1/targets")["data"]["activeTargets"]
    jobs = {target["labels"]["job"] for target in targets if target["health"] == "up"}
    if {"ingest", "consumer", "query"} <= jobs:
        break
    time.sleep(1)
else:
    raise AssertionError("Prometheus targets not healthy")
auth = "admin:" + os.environ.get("GRAFANA_ADMIN_PASSWORD", "streamforge_grafana_local_only")
dashboard = get("http://localhost:3000/api/dashboards/uid/streamforge-pipeline", auth)
assert len(dashboard["dashboard"]["panels"]) == 12
sources = get("http://localhost:3000/api/datasources", auth)
assert {"prometheus", "jaeger"} <= {source["uid"] for source in sources}
required = {"http.ingest", "kafka.publish", "consumer.partition_insert", "clickhouse.insert", "http.query.revenue", "clickhouse.revenue", "clickhouse.traffic"}
for _ in range(30):
    try:
        traces = get("http://localhost:16686/api/traces/" + trace_id)["data"]
    except HTTPError as error:
        if error.code != 404:
            raise
        traces = []
    operations = {span["operationName"] for trace in traces for span in trace["spans"]}
    if required <= operations:
        break
    time.sleep(1)
else:
    raise AssertionError("missing exported trace operations: " + str(required - operations))
print("PASS observability: Prometheus ingest/consumer/query targets up; Grafana 12-panel dashboard and both datasources provisioned; HTTP -> Kafka -> consumer -> ClickHouse and query spans found in Jaeger")
