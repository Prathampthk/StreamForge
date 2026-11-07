"""Exercise the running ingress -> Kafka -> consumer -> canonical metrics path."""
import json
import os
import time
import uuid
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

api = os.environ.get("INGEST_URL", "http://localhost:8080")
ch = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
key = os.environ.get("INGEST_API_KEY", "streamforge_demo_key_local_only_change_me")
tenant = os.environ.get("INGEST_TENANT", "merchant_demo")
user = os.environ.get("CLICKHOUSE_USER", "streamforge")
password = os.environ.get("CLICKHOUSE_PASSWORD", "streamforge_local_only")
assert tenant.replace("_", "").replace("-", "").isalnum()


def query(sql):
    request = Request(ch + "/?" + urlencode({"database": "streamforge", "wait_end_of_query": 1}), data=(sql + " FORMAT JSONEachRow").encode())
    request.add_header("Authorization", "Basic " + b64encode((user + ":" + password).encode()).decode())
    with urlopen(request, timeout=5) as response:
        return [json.loads(line) for line in response.read().decode().splitlines()]


occurred = (datetime.now(timezone.utc) - timedelta(days=1)).replace(second=1, microsecond=0)
minute = occurred.replace(second=0).strftime("%Y-%m-%d %H:%M:%S")
where = f"tenant_id='{tenant}' AND toStartOfMinute(occurred_at)=toDateTime('{minute}','UTC')"
revenue_base = {row["currency"]: (int(row["purchases"]), int(row["revenue"])) for row in query(f"SELECT currency,count() AS purchases,sum(toInt128(amount_cents)) AS revenue FROM events_canonical WHERE {where} AND event_type='purchase' GROUP BY currency")}
traffic_base = query(f"SELECT count() AS events,countIf(event_type='api_request') AS rpm,countIf(event_type='api_request' AND status_code>=500) AS errors FROM events_canonical WHERE {where}")[0]
rows = []
for index, kind in enumerate(["purchase", "purchase", "page_view", "api_request", "api_request"]):
    event = dict(schema_version=1, event_id=str(uuid.uuid4()), tenant_id=tenant, user_id="consumer_smoke", event_type=kind, occurred_at=occurred.isoformat(), attributes={"source": "consumer-smoke"})
    if kind == "purchase":
        event.update(order_id="smoke_order_" + str(index), amount_cents=137 if index == 0 else 250, currency="USD" if index == 0 else "EUR")
    if kind == "api_request":
        event["status_code"] = 200 if index == 3 else 503
    rows.append(event)

for _ in range(2):
    request = Request(api + "/v1/events/batch", data=json.dumps(rows).encode(), headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"})
    with urlopen(request, timeout=10) as response:
        assert response.status == 202 and json.loads(response.read())["accepted"] == 5

ids = ",".join("'" + row["event_id"] + "'" for row in rows)
expected_traffic = (int(traffic_base["events"]) + 5, int(traffic_base["rpm"]) + 2, int(traffic_base["errors"]) + 1)
for currency, cents in [("USD", 137), ("EUR", 250)]:
    purchases, amount = revenue_base.get(currency, (0, 0))
    revenue_base[currency] = purchases + 1, amount + cents

for _ in range(150):
    canonical = int(query(f"SELECT count() AS n FROM events_canonical WHERE event_id IN ({ids})")[0]["n"])
    revenue = {r["currency"]: (int(r["purchases"]), int(r["revenue"])) for r in query(f"SELECT currency,sum(purchase_count) AS purchases,sum(revenue_cents) AS revenue FROM revenue_1m_canonical WHERE tenant_id='{tenant}' AND minute=toDateTime('{minute}','UTC') GROUP BY currency")}
    traffic = query(f"SELECT sum(events_per_minute) AS events,sum(rpm) AS rpm,sum(api_errors) AS errors FROM traffic_1m_canonical WHERE tenant_id='{tenant}' AND minute=toDateTime('{minute}','UTC')")[0]
    if canonical == 5 and revenue == revenue_base and tuple(int(traffic[k]) for k in ["events", "rpm", "errors"]) == expected_traffic:
        break
    time.sleep(0.1)
else:
    raise AssertionError("pipeline did not reconcile within drain deadline; inspect consumer and system.view_refreshes")
raw = int(query(f"SELECT count() AS n FROM events_raw WHERE event_id IN ({ids})")[0]["n"])
assert raw >= 10
print("PASS live pipeline: 10 HTTP-acknowledged deliveries -> 5 canonical IDs; duplicate-free increments USD=137/EUR=250 cents, purchases=2, RPM=2, errors=1, events=5")
print("Raw forensic delivery rows:", raw)
