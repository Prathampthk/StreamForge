# ADR 014: correct global allocation drift against container accounting

## Trigger and decision

Retained refresh queries record roughly 8.6 MiB of native query memory, but some abort with a global 3.60 GiB allocation total while the error reports approximately 1.4–1.7 GiB RSS. Parsing reduction improves some refresh durations but does not remove that discrepancy. A previous correction-only trial still failed readiness, so it was reverted; that result does not establish that correction plus reduced query work will recover readiness.

Pinned ClickHouse 25.8.4.13 source confirms that memory_worker_correct_memory_tracker periodically resets the global allocation counter from the selected memory source. With cgroup v2, that source reads anon, sock and kernel from memory.stat, not filesystem page cache. Retain memory_worker_use_cgroup=true and enable correction. This corrects accounting; it does not raise the 4 GiB Docker cap, 90% automatic server cap or 64 MiB refresh query limits. Per-query limits remain independent. Allocator residency accounting was tested previously and rejected; it is not adopted here.

A disposable 1 GiB instance must prove the actual flags and automatic <=90% server cap before local rollout. The existing database-grants check now enforces those invariants. After restart, verify retained ledger/data state and observe all schedules and logging normally. No SLO or causal speedup follows from a restart. Keep the previous failed samples and disclose both configuration changes in later benchmarks.

The underlying history scans remain. Correct accounting is necessary operational work, not an incremental analytics algorithm or proof of sufficient capacity.

Sources: [pinned MemoryWorker](https://github.com/ClickHouse/ClickHouse/blob/v25.8.4.13-lts/src/Common/MemoryWorker.cpp), [pinned MemoryTracker](https://github.com/ClickHouse/ClickHouse/blob/v25.8.4.13-lts/src/Common/MemoryTracker.cpp). These version-specific sources supersede assumptions from newer documentation.
