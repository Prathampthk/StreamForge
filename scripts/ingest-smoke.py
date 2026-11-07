"""Smoke the running local API; never print the credential or payload."""
import json
import os
import uuid
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen

key = os.environ.get("INGEST_API_KEY", "streamforge_demo_key_local_only_change_me")
tenant = os.environ.get("INGEST_TENANT", "merchant_demo")
base = "http://localhost:8080"

def call(path, data=None, authenticated=True):
    headers = {"Content-Type": "application/json"}
    if authenticated:
        headers["Authorization"] = "Bearer " + key
    request = Request(base + path, data=json.dumps(data).encode() if data is not None else None, headers=headers)
    try:
        with urlopen(request, timeout=10) as response:
            return response.status, response.read().decode()
    except HTTPError as error:
        with error:
            return error.code, error.read().decode()

for path in ["/livez", "/readyz"]:
    status, _ = call(path)
    assert status == 200, (path, status)

event = dict(schema_version=1, event_id=str(uuid.uuid4()), tenant_id=tenant,
             user_id="api_smoke", event_type="api_request", status_code=503,
             occurred_at=datetime.now(timezone.utc).isoformat())
assert call("/v1/events", event, authenticated=False)[0] == 401
assert call("/v1/events", {})[0] == 400
status, body = call("/v1/events", event)
assert status == 202 and json.loads(body)["accepted"] == 1, (status, body)
rows = [event | {"event_id": str(uuid.uuid4())} for _ in range(2)]
status, body = call("/v1/events/batch", rows)
assert status == 202 and json.loads(body)["accepted"] == 2, (status, body)
status, metrics = call("/metrics")
assert status == 200
assert 'streamforge_kafka_events_total{outcome="acknowledged"}' in metrics
assert 'streamforge_ingest_requests_total{outcome="accepted"}' in metrics
print("PASS live API: health=200, unauthenticated=401, invalid=400, single=202 (1), batch=202 (2), acknowledgment metrics present")
