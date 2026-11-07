"""Record truly HTTP-accepted golden deliveries; query and reconcile their scope."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import uuid

from reconcile import ClickHouse, audit, expectation, instant, STAMP

api = os.environ.get("INGEST_URL", "http://localhost:8080")
query_api = os.environ.get("QUERY_URL", "http://localhost:8082")
key = os.environ.get("INGEST_API_KEY", "streamforge_demo_key_local_only_change_me")
query_key = os.environ.get("QUERY_API_KEY", "streamforge_query_demo_key_local_only")
tenant = os.environ.get("INGEST_TENANT", "merchant_demo")
client = ClickHouse()
start = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(days=6)
for offset in range(500):
    base = start + timedelta(minutes=offset * 4)
    params = dict(tenant=tenant, start=base.strftime("%Y-%m-%d %H:%M:%S"), end=(base + timedelta(minutes=4)).strftime("%Y-%m-%d %H:%M:%S"))
    where = "tenant_id={tenant:String} AND occurred_at>={start:DateTime64(9,'UTC')} AND occurred_at<{end:DateTime64(9,'UTC')}"
    if int(client.query("SELECT count() AS n FROM events_raw WHERE " + where, params)[0]["n"]) == 0 and int(client.query("SELECT count() AS n FROM events_canonical WHERE " + where, params)[0]["n"]) == 0:
        break
else:
    raise RuntimeError("no empty manifest-owned scope found")

fixture = json.loads((Path(__file__).resolve().parents[1] / "tests/fixtures/reconciliation-golden.json").read_text())
original = instant(fixture["scope"]["from"])
ids = {}
rows = []
for accepted in fixture["accepted"]:
    event = accepted["event"].copy()
    ids.setdefault(event["event_id"], str(uuid.uuid4()))
    event["event_id"] = ids[event["event_id"]]
    event["tenant_id"] = tenant
    match = STAMP.fullmatch(event["occurred_at"])
    moment = instant(event["occurred_at"]).replace(microsecond=0)
    shifted = base + (moment - original)
    fraction = "." + match[2] if match[2] else ""
    if match[3] == "-06:00":
        event["occurred_at"] = shifted.astimezone(timezone(timedelta(hours=-6))).strftime("%Y-%m-%dT%H:%M:%S") + fraction + "-06:00"
    else:
        event["occurred_at"] = shifted.strftime("%Y-%m-%dT%H:%M:%S") + fraction + "Z"
    rows.append(event)

trace_id = uuid.uuid4().hex
headers = {"Content-Type": "application/json", "traceparent": "00-" + trace_id + "-0123456789abcdef-01"}
manifest = dict(schema_version=1, trace_id=trace_id, scope=dict(tenants=[tenant], **{"from": base.isoformat(), "to": (base + timedelta(minutes=4)).isoformat()}), accepted=[])
for _ in range(2):
    request = Request(api + "/v1/events/batch", data=json.dumps(rows).encode(), headers=headers | {"Authorization": "Bearer " + key})
    with urlopen(request, timeout=10) as response:
        body = json.loads(response.read())
        if response.status != 202 or body["accepted"] != len(rows):
            raise RuntimeError("publish not acknowledged; cannot add to accepted manifest")
    acknowledged = datetime.now(timezone.utc).isoformat()
    manifest["accepted"].extend(dict(event=event, acknowledged_at=acknowledged) for event in rows)

path = Path(tempfile.gettempdir()) / ("streamforge-accepted-" + trace_id + ".json")
path.write_text(json.dumps(manifest, indent=2) + "\n")
expected = expectation(manifest)
for _ in range(150):
    result = audit(client, expected)
    if result["ok"]:
        break
    time.sleep(.1)
else:
    raise AssertionError(result)

for kind in ["revenue", "traffic"]:
    currencies = ["USD", "EUR"] if kind == "revenue" else [None]
    for currency in currencies:
        params = dict(tenant_id=tenant, interval="1m", **{"from": base.astimezone(timezone(timedelta(hours=-6))).isoformat(), "to": (base + timedelta(minutes=4)).isoformat()})
        if currency:
            params["currency"] = currency
        request = Request(query_api + "/v1/metrics/" + kind + "?" + urlencode(params), headers=headers | {"Authorization": "Bearer " + query_key})
        with urlopen(request, timeout=5) as response:
            assert response.status == 200
            response = json.loads(response.read())
        data = response["data"]
        assert len(data) == 4
        if kind == "revenue":
            expected_cents = ["137", "0", "0", "0"] if currency == "USD" else ["0", "250", "0", "0"]
            assert [row["revenue_cents"] for row in data] == expected_cents
        else:
            assert [(r["events_per_minute"], r["rpm"], r["api_errors"], r["error_rate"]) for r in data] == [(4, 2, 1, .5), (2, 1, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0)]
print("PASS live queries/reconciliation: 18 acknowledged deliveries, 6 canonical IDs, 1 quarantined conflict; USD=137/EUR=250 cents; minute edges, offsets, zero buckets and API errors exact")
print("Accepted manifest:", path)
print("Trace ID:", trace_id)
print(json.dumps(result, sort_keys=True))
