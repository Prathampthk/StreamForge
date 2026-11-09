# ADR 017: larger bounded vectorized blocks

After the parsing and container-counter corrections, the retained sources stopped reporting global memory errors in sampled windows but concurrent scans still exceeded fifteen seconds. EXPLAIN confirmed correct hash-partition pruning (6 of 88 parts for shard zero); the arm64 container had no CPU quota or emulation. Do not attribute all remaining contention to accounting.

A quiescent single-shard comparison returned the same 1,095 metric rows with each block size. Native measurements: 128 rows/8 KiB buffer took 2,309ms and 8,590,523 bytes; 1,024 rows/64 KiB took 1,407ms and 8,606,918 bytes; 4,096 rows/64 KiB took 1,313ms and 13,278,722 bytes. All read the same 992,254 physical rows and 568,326,814 bytes. Candidates ran in that order on a shared host; no controlled speedup or sustained-health result is claimed.

Migration 010 uses 4,096-row blocks, 64 KiB read buffers and a 128 KiB preferred block size for the sixteen source projections. Keep the 64 MiB per-query cap, one thread, fifteen-second timeout, four-second schedule and exact conflict/Int128 rules. This trades some measured query memory for fewer tiny block operations; it does not increase any hard budget or change retained facts. It does not bound history growth or establish healthy ingestion at 10k/s.

The next gate is normal-operation health followed by an exact small accepted-event reconciliation before sustained load. If that gate fails, preserve the failure; do not lengthen stale/time limits or report the isolated read as an end-to-end pass. Restoring the prior source query settings is a reversible forward migration; no facts need deletion.


The normal-schedule retained trial returned only one ready response in twelve samples. Source durations remained roughly 8–15 seconds, so this change did not establish recovery. The next experiment must remove repeated payload decoding from the read path rather than keep adjusting schedules or relax staleness/time limits.
