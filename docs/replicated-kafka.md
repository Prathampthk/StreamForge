# Local replicated Kafka failure drill

Run `make replicated-kafka-test` or `python3 scripts/check-replicated-kafka.py --out <new-summary.json>`. Docker is required; AWS is not. The script creates a unique isolated Compose project with three combined KRaft broker/controller nodes, twelve partitions, RF3/minISR2, disabled unclean leader election and no published ports. The normal RF1 Compose stack is unchanged.

The Go fixture uses the real ingestion handler and Kafka producer. It validates per-event callback receipts, retains a private accepted-event journal, and compares every acknowledged ID and full Kafka payload. It first accepts 120 events, raises topic minISR to three, kills the partition-zero leader with SIGKILL and waits for every partition to reach ISR2. An unconfirmed twelve-event request must return 503. Restoring minISR2 while that broker remains down allows another 120 acknowledgments. After the killed broker rejoins ISR3, another 120 events are acknowledged. A different broker is then killed and the entire acknowledged cohort is read from the remaining brokers.

## Recorded result

The first local run passed in 58.163 seconds: **360 acknowledged events, 360 exact payload matches, zero missing IDs and zero duplicate acknowledged IDs**, spread across all twelve partitions. The twelve-event 503 request was treated as unknown delivery; **three of its IDs were observed**. This is direct evidence that failed HTTP acknowledgment does not prove absence from Kafka. [Small evidence](evidence/replicated-kafka.json) records topic overrides, broker IDs, phases, source hashes and cleanup. That run preceded its commit (`source_dirty=true`); clean hosted CI provides the subsequent committed-source check.

## Boundaries and cleanup

All brokers share one Docker VM, network and administrative domain. This demonstrates broker-process crash/rejoin behavior with retained disks and a controller majority; it does not prove host, disk, power or availability-zone loss resilience. It does not test ClickHouse persistence or analytical deduplication and is not a throughput benchmark. The successful physical-duplicate check applies only to this producer session and fixture.

The runner compiles a race-enabled test binary using pinned Go, then runs it on the internal-only test network. Broker heaps are 256 MiB with 768 MiB container caps; the test client is capped at 1 GiB. If retained ClickHouse refreshes are active, it pauses only their schedules outside any benchmark and restores them in cleanup. The first run found no active retained server and paused none. No retained facts, identity tables, Kafka topics or volumes are deleted. Only the unique test project is removed with its three volumes; shared Go caches remain. Private results are kept under ignored `load/results/raw/streamforge-rf3-*`.
