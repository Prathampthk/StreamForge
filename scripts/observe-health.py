"""Probe actual optional service HTTP endpoints, including the Collector."""
import time
from urllib.request import urlopen

pending = {"prometheus": "http://localhost:9090/-/ready", "grafana": "http://localhost:3000/api/health", "collector": "http://localhost:13133/", "jaeger": "http://localhost:16686/api/services"}
deadline = time.monotonic() + 45
while pending and time.monotonic() < deadline:
    for name, url in list(pending.items()):
        try:
            with urlopen(url, timeout=2) as response:
                if response.status == 200:
                    del pending[name]
        except OSError:
            pass
    if pending:
        time.sleep(.5)
if pending:
    raise SystemExit("Optional HTTP endpoints unavailable: " + ", ".join(pending))
print("PASS optional HTTP health: Prometheus, Grafana, Collector, Jaeger")
