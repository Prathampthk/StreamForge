//go:build integration

package integration

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// Own every table touched by merge controls. Never stop merges on shared data.
func TestCanonicalFinalAcrossUnmergedParts(t *testing.T) {
	c := sinkConfig(t)
	db := "final_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	query := func(sql string) string { return queryCH(t, c, sql) }
	query("CREATE DATABASE " + db)
	defer query("DROP DATABASE " + db + " SYNC")
	query("CREATE TABLE " + db + ".event_identity_values AS streamforge.event_identity_values")
	query("SYSTEM STOP MERGES " + db + ".event_identity_values")
	defer query("SYSTEM START MERGES " + db + ".event_identity_values")
	for _, name := range []string{"events_canonical", "event_identity_conflicts"} {
		ddl := query("SELECT create_table_query FROM system.tables WHERE database='streamforge' AND name='" + name + "' FORMAT TabSeparatedRaw")
		if !strings.Contains(ddl, "FINAL") {
			t.Fatal("canonical merge migration is not installed")
		}
		ddl = strings.Replace(ddl, "streamforge."+name, db+"."+name, 1)
		ddl = strings.ReplaceAll(ddl, "streamforge.event_identity_values", db+".event_identity_values")
		ddl = strings.ReplaceAll(ddl, "FROM event_identity_values", "FROM "+db+".event_identity_values")
		query(ddl)
	}
	ids := make([]string, 24)
	payloads := make([]string, len(ids))
	for i := range ids {
		ids[i] = uuid.NewString()
		value, err := json.Marshal(map[string]any{"schema_version": 1, "event_id": ids[i], "tenant_id": "before", "user_id": "user", "event_type": "purchase", "occurred_at": "2026-11-07T12:00:00Z", "amount_cents": int64(9223372036854775807), "currency": "USD", "attributes": map[string]string{"source": "regression"}})
		if err != nil {
			t.Fatal(err)
		}
		payloads[i] = string(value)
	}
	insert := func(indices []int, values []string) {
		selects := make([]string, len(indices))
		for j, i := range indices {
			payload := "unhex('" + hex.EncodeToString([]byte(values[i])) + "')"
			selects[j] = fmt.Sprintf("SELECT toUUID('%s'),%s,%s", ids[i], payload, payload)
		}
		query("INSERT INTO " + db + ".event_identity_values " + strings.Join(selects, " UNION ALL "))
	}
	all := make([]int, len(ids))
	for i := range all {
		all[i] = i
	}
	insert(all, payloads)
	if got := query("SELECT count() FROM " + db + ".events_canonical"); got != "24" {
		t.Fatal(got)
	}
	insert(all, payloads)
	insert([]int{0, 2, 4, 6, 8, 10}, payloads)
	// Global conflict identity must survive changed tenant, minute, currency, type,
	// and non-business attributes. No pushed predicate may hide a variant.
	changed := append([]string(nil), payloads...)
	for i, field := range []string{"tenant_id", "occurred_at", "currency", "event_type", "attributes"} {
		var event map[string]any
		if err := json.Unmarshal([]byte(changed[i]), &event); err != nil {
			t.Fatal(err)
		}
		switch field {
		case "tenant_id":
			event[field] = "after"
		case "occurred_at":
			event[field] = "2026-11-06T11:00:00Z"
		case "currency":
			event[field] = "EUR"
		case "event_type":
			event[field] = "page_view"
		case "attributes":
			event[field] = map[string]string{"source": "changed"}
		}
		value, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		changed[i] = string(value)
	}
	insert([]int{0, 1, 2, 3, 4}, changed)
	check := func() {
		original := "SELECT event_id,min(payload_min) AS payload,max(payload_max) AS last_payload FROM " + db + ".event_identity_values GROUP BY event_id HAVING payload=last_payload"
		expected := query("SELECT toString(event_id),payload FROM (" + original + ") ORDER BY event_id FORMAT TabSeparatedRaw")
		got := query("SELECT toString(event_id),payload FROM " + db + ".events_canonical ORDER BY event_id FORMAT TabSeparatedRaw")
		if got != expected {
			t.Fatalf("exact canonical payloads differ\ngot %s\nwant %s", got, expected)
		}
		if got := query("SELECT count() FROM " + db + ".event_identity_conflicts"); got != "5" {
			t.Fatalf("conflicts=%s", got)
		}
		for _, predicate := range []string{"tenant_id='before'", "tenant_id='after'", "currency='USD'", "currency='EUR'", "event_type='purchase'", "occurred_at>='2026-11-07'"} {
			if got := query("SELECT count() FROM " + db + ".events_canonical WHERE event_id IN ('" + strings.Join(ids[:5], "','") + "') AND " + predicate + " SETTINGS optimize_move_to_prewhere=1,optimize_move_to_prewhere_if_final=1"); got != "0" {
				t.Fatalf("predicate %s resurrected conflict: %s", predicate, got)
			}
		}
		if got := query("SELECT toString(sum(toInt128(amount_cents))) FROM " + db + ".events_canonical WHERE tenant_id='before'"); got != "175244068700240740333" {
			t.Fatalf("Int128 total=%s", got)
		}
	}
	check()
	query("SYSTEM START MERGES " + db + ".event_identity_values")
	query("OPTIMIZE TABLE " + db + ".event_identity_values FINAL")
	check()
	t.Log("exact payloads, global conflict withdrawal, predicate safety and Int128 totals hold before/after merges")
}
