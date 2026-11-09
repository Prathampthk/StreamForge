# ADR 016: retry metadata errors only before any records are polled

The retained-stability integration run exposed Kafka NOT_COORDINATOR while a helper joined its fresh group. The consumer exited safely, but the crash harness lost its stdout and raced on stderr while reporting the failure. A second run exposed UNKNOWN_TOPIC_OR_PARTITION during fresh-topic setup. These are preserved failed runs, not successful crash evidence.

Keep fail-closed restart/replay semantics for every failure involving collected records, inserts or commits. Only a poll containing zero records, before any record has been collected in the current epoch, may receive an in-process retry for explicitly enumerated coordinator/leader/topic metadata errors. Do not classify arbitrary errors by text or retry corruption/authorization failures. Limit retries to three, with jittered backoff and the configured rebalance time budget; empty polls do not reset that budget, only confirmed commit progress does. The client invalidates failed group sessions and resumes its group management after rebalance is released. No workers survive an epoch.

This restriction matters because simply continuing after discarding polled records could let a later offset commit skip unpersisted events. Unit tests exercise wrapped native ErrGroupSession errors, buffered/current records, mixed permanent errors, corruption and collection cancellation. A separate finite fetch-retry counter makes these retries observable.

The test-topic helper now waits for all twelve leaders/ISRs after CreateTopics. Child diagnostics use a synchronized buffer, capture stdout and report early exit immediately; an explicit concurrent diagnostic test runs under the race detector. No failure deadline or expected ID/revenue/traffic value is relaxed.
