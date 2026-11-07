"""Bounded accepted-manifest audit. No traffic generation or cloud operations."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from base64 import b64encode
import uuid

IDENTIFIER = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
STAMP = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$")


def instant(value):
    if not isinstance(value, str) or not STAMP.fullmatch(value):
        raise ValueError("invalid RFC3339 timestamp")
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def normalize(event):
    required = {"schema_version", "event_id", "tenant_id", "user_id", "event_type", "occurred_at"}
    optional = {"session_id", "order_id", "amount_cents", "currency", "status_code", "attributes"}
    if not isinstance(event, dict) or not required <= event.keys() or event.keys() - required - optional:
        raise ValueError("invalid manifest event fields")
    if event["schema_version"] != 1 or isinstance(event["schema_version"], bool):
        raise ValueError("invalid schema version")
    for key in ["tenant_id", "user_id"]:
        if not isinstance(event[key], str) or not IDENTIFIER.fullmatch(event[key]):
            raise ValueError("invalid manifest identifier")
    if event["event_type"] not in {"purchase", "api_request", "page_view", "add_to_cart"}:
        raise ValueError("invalid event type")
    stamp = STAMP.fullmatch(event["occurred_at"])
    at = instant(event["occurred_at"])
    fraction = stamp[2].rstrip("0") if stamp[2] else ""
    # Preserve all nine fractional digits without Python float/microsecond rounding.
    normalized_time = at.strftime("%Y-%m-%dT%H:%M:%S") + ("." + fraction if fraction else "") + "Z"
    normalized = {k: v for k, v in event.items() if v is not None and not (k == "attributes" and not v)}
    normalized["event_id"] = str(uuid.UUID(event["event_id"]))
    normalized["occurred_at"] = normalized_time
    if event["event_type"] == "purchase":
        if type(event.get("amount_cents")) is not int or not 0 <= event["amount_cents"] <= 2**63 - 1 or not re.fullmatch(r"[A-Z]{3}", event.get("currency", "")) or not isinstance(event.get("order_id"), str):
            raise ValueError("invalid purchase in manifest")
    if event["event_type"] == "api_request" and (type(event.get("status_code")) is not int or not 100 <= event["status_code"] <= 599):
        raise ValueError("invalid status in manifest")
    return normalized


def expectation(manifest):
    if manifest.get("schema_version") != 1 or not isinstance(manifest.get("accepted"), list) or len(manifest["accepted"]) > 5000:
        raise ValueError("require schema_version=1 and <=5000 accepted entries")
    scope = manifest["scope"]
    tenants = scope["tenants"]
    start, end = instant(scope["from"]), instant(scope["to"])
    if not 1 <= len(tenants) <= 20 or len(set(tenants)) != len(tenants) or any(not IDENTIFIER.fullmatch(t) for t in tenants) or not timedelta(0) < end - start <= timedelta(days=1) or any(t.second or t.microsecond for t in [start, end]):
        raise ValueError("scope must contain 1..20 unique tenants and a minute-aligned window <=24h")
    variants = defaultdict(dict)
    deliveries = Counter()
    for entry in manifest["accepted"]:
        event = normalize(entry["event"])
        instant(entry["acknowledged_at"])
        if event["tenant_id"] not in tenants or not start <= instant(event["occurred_at"]) < end:
            raise ValueError("accepted event outside manifest-owned scope")
        encoded = json.dumps(event, sort_keys=True, separators=(",", ":"))
        variants[event["event_id"]][encoded] = event
        deliveries[event["event_id"]] += 1
    canonical = {key: next(iter(values.values())) for key, values in variants.items() if len(values) == 1}
    conflicts = {key for key, values in variants.items() if len(values) > 1}
    revenue, traffic = {}, {}
    for event in canonical.values():
        minute = instant(event["occurred_at"]).replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
        group = (event["tenant_id"], minute)
        totals = traffic.setdefault(group, [0, 0, 0])
        totals[0] += 1
        if event["event_type"] == "api_request":
            totals[1] += 1
            totals[2] += int(event["status_code"] >= 500)
        if event["event_type"] == "purchase":
            totals = revenue.setdefault(group + (event["currency"],), [0, 0])
            totals[0] += 1
            totals[1] += event["amount_cents"]
    return dict(scope=scope, canonical=canonical, conflicts=conflicts, deliveries=deliveries, revenue=revenue, traffic=traffic)


class ClickHouse:
    def __init__(self):
        self.url = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123").rstrip("/")
        credentials = os.environ.get("CLICKHOUSE_USER", "streamforge") + ":" + os.environ.get("CLICKHOUSE_PASSWORD", "streamforge_local_only")
        self.auth = "Basic " + b64encode(credentials.encode()).decode()

    def query(self, sql, parameters):
        params = dict(database="streamforge", readonly=1, wait_end_of_query=1, max_execution_time=2, max_memory_usage=134217728, max_threads=2)
        params.update({"param_" + key: value for key, value in parameters.items()})
        request = Request(self.url + "/?" + urlencode(params), data=(sql + " FORMAT JSONEachRow").encode(), headers={"Authorization": self.auth})
        with urlopen(request, timeout=3) as response:
            body = response.read((2 << 20) + 1)
            if len(body) > 2 << 20:
                raise ValueError("audit query response exceeds 2 MiB")
            return [json.loads(line) for line in body.decode().splitlines()]


def compare(expected, canonical_rows, raw_rows, conflict_rows, revenue_rows, traffic_rows):
    observed = {row["event_id"]: normalize(json.loads(row["payload"])) for row in canonical_rows}
    raw = {row["event_id"]: int(row["raw_rows"]) for row in raw_rows}
    conflicts = {row["event_id"] for row in conflict_rows}
    revenue = {(r["tenant_id"], r["minute"], r["currency"]): [int(r["purchases"]), int(r["cents"])] for r in revenue_rows}
    traffic = {(r["tenant_id"], r["minute"]): [int(r["events"]), int(r["rpm"]), int(r["errors"])] for r in traffic_rows}
    mismatches = dict(
        duplicate_canonical_rows=(len(observed) != len(canonical_rows)),
        missing_ids=sorted(expected["canonical"].keys() - observed.keys()),
        extra_ids=sorted(observed.keys() - expected["canonical"].keys()),
        payload_ids=sorted(k for k in observed.keys() & expected["canonical"].keys() if observed[k] != expected["canonical"][k]),
        raw_missing_ids=sorted(k for k, count in expected["deliveries"].items() if raw.get(k, 0) < count),
        raw_extra_ids=sorted(raw.keys() - expected["deliveries"].keys()),
        missing_conflict_ids=sorted(expected["conflicts"] - conflicts),
        unexpected_conflict_ids=sorted(conflicts - expected["conflicts"]),
    )
    if revenue != expected["revenue"]:
        mismatches["revenue_buckets"] = True
    if traffic != expected["traffic"]:
        mismatches["traffic_buckets"] = True
    ok = not any(mismatches.values())
    return dict(ok=ok, accepted_deliveries=sum(expected["deliveries"].values()), canonical_ids=len(observed), quarantined_ids=len(conflicts), raw_rows=sum(raw.values()), raw_duplicate_rows=sum(raw.values()) - len(raw), mismatches={k: (v[:20] if isinstance(v, list) else v) for k, v in mismatches.items() if v})


def audit(client, expected):
    scope = expected["scope"]
    # ClickHouse typed Array(String) uses its own quoted literals; JSON escaping
    # plus identifier validation prevents query/array syntax injection.
    array = lambda values: "[" + ",".join("'" + value + "'" for value in values) + "]"
    params = dict(tenants=array(scope["tenants"]), start=instant(scope["from"]).strftime("%Y-%m-%d %H:%M:%S"), end=instant(scope["to"]).strftime("%Y-%m-%d %H:%M:%S"), ids=array(list(expected["deliveries"])))
    where = "tenant_id IN {tenants:Array(String)} AND occurred_at>={start:DateTime64(9,'UTC')} AND occurred_at<{end:DateTime64(9,'UTC')}"
    canonical = client.query("SELECT event_id,payload FROM events_canonical WHERE " + where + " LIMIT 5001", params)
    raw = client.query("SELECT event_id,count() AS raw_rows FROM events_raw WHERE " + where + " GROUP BY event_id LIMIT 5001", params)
    conflicts = client.query("SELECT event_id FROM event_identity_conflicts WHERE event_id IN {ids:Array(UUID)} LIMIT 5001", params)
    where = "snapshot.tenant_id IN {tenants:Array(String)} AND snapshot.minute>={start:DateTime64(9,'UTC')} AND snapshot.minute<{end:DateTime64(9,'UTC')}"
    revenue = client.query("SELECT tenant_id,formatDateTime(minute,'%Y-%m-%dT%H:%i:%SZ','UTC') AS minute,currency,sum(purchase_count) AS purchases,toString(sum(revenue_cents)) AS cents FROM revenue_1m_canonical AS snapshot WHERE " + where + " GROUP BY tenant_id,snapshot.minute,currency LIMIT 5001", params)
    traffic = client.query("SELECT tenant_id,formatDateTime(minute,'%Y-%m-%dT%H:%i:%SZ','UTC') AS minute,sum(events_per_minute) AS events,sum(rpm) AS rpm,sum(api_errors) AS errors FROM traffic_1m_canonical AS snapshot WHERE " + where + " GROUP BY tenant_id,snapshot.minute LIMIT 5001", params)
    health = client.query("SELECT view,toUnixTimestamp(last_success_time) AS last_success,toUInt8(exception!='') AS failed,status FROM system.view_refreshes WHERE database='streamforge' AND view IN ('revenue_1m_refresh','traffic_1m_refresh')", {})
    result = compare(expected, canonical, raw, conflicts, revenue, traffic)
    if len(health) != 2 or any(int(r["failed"]) or r["status"] not in {"Scheduled", "Running"} or not 0 <= time.time() - int(r["last_success"]) <= 10 for r in health):
        result["ok"] = False
        result["mismatches"]["refresh_health"] = True
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--drain-timeout", type=float, default=15)
    args = parser.parse_args()
    if not 0 <= args.drain_timeout <= 60 or args.manifest.stat().st_size > 4 << 20:
        parser.error("drain timeout must be 0..60s and manifest <=4 MiB")
    try:
        manifest = json.loads(args.manifest.read_text())
        expected = expectation(manifest)
        client = ClickHouse()
        deadline = time.monotonic() + args.drain_timeout
        while True:
            result = audit(client, expected)
            if result["ok"] or time.monotonic() >= deadline:
                print(json.dumps(result, sort_keys=True))
                return 0 if result["ok"] else 1
            time.sleep(0.2)
    except Exception as error:
        # Never include a potentially credential-bearing endpoint/error URL.
        print(json.dumps({"ok": False, "error": type(error).__name__, "message": "manifest/audit failed; check configuration and local database logs"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
