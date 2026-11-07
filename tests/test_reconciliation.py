import copy
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("reconcile", ROOT / "scripts/reconcile.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)

class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.manifest = json.loads((ROOT / "tests/fixtures/reconciliation-golden.json").read_text())
        self.expected = audit.expectation(self.manifest)

    def test_golden_is_independent_of_deliveries(self):
        self.assertEqual(len(self.expected["canonical"]), 6)
        self.assertEqual(len(self.expected["conflicts"]), 1)
        self.assertEqual(self.expected["revenue"], {("golden", "2026-11-06T17:22:00Z", "USD"): [1, 137], ("golden", "2026-11-06T17:23:00Z", "EUR"): [1, 250]})
        self.assertEqual(self.expected["traffic"], {("golden", "2026-11-06T17:22:00Z"): [4, 2, 1], ("golden", "2026-11-06T17:23:00Z"): [2, 1, 0]})
        payload = self.expected["canonical"]["00000000-0000-4000-8000-000000000001"]
        self.assertEqual(payload["occurred_at"], "2026-11-06T17:22:59.999999999Z")

    def observed(self):
        canonical = [dict(event_id=k, payload=json.dumps(v)) for k, v in self.expected["canonical"].items()]
        raw = [dict(event_id=k, raw_rows=v+1) for k, v in self.expected["deliveries"].items()]
        conflicts = [dict(event_id=k) for k in self.expected["conflicts"]]
        revenue = [dict(tenant_id=k[0], minute=k[1], currency=k[2], purchases=v[0], cents=str(v[1])) for k, v in self.expected["revenue"].items()]
        traffic = [dict(tenant_id=k[0], minute=k[1], events=v[0], rpm=v[1], errors=v[2]) for k, v in self.expected["traffic"].items()]
        return [canonical, raw, conflicts, revenue, traffic]

    def test_exact_success_and_mismatch_failures(self):
        self.assertTrue(audit.compare(self.expected, *self.observed())["ok"])
        for which in range(5):
            with self.subTest(missing_category=which):
                rows = self.observed()
                rows[which].pop()
                self.assertFalse(audit.compare(self.expected, *rows)["ok"])
        rows = self.observed()
        rows[0].append(copy.deepcopy(rows[0][0]))
        self.assertFalse(audit.compare(self.expected, *rows)["ok"])
        rows = self.observed()
        rows[3][0]["cents"] = "274"
        self.assertFalse(audit.compare(self.expected, *rows)["ok"])

    def test_unacknowledged_or_out_of_scope_manifest_is_rejected(self):
        for bad in ["unacknowledged", "scope"]:
            manifest = copy.deepcopy(self.manifest)
            if bad == "unacknowledged":
                del manifest["accepted"][0]["acknowledged_at"]
            else:
                manifest["scope"]["tenants"] = ["another"]
            with self.assertRaises((ValueError, KeyError)):
                audit.expectation(manifest)
