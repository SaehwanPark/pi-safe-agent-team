# Roadmap

Actionable work is tracked here with checkboxes. Checked items mean the behavior exists and has verification evidence in the repository; unchecked items are intentionally not part of the current release unless explicitly moved into scope.

## v1 foundation

- [x] Record prior-art findings and the v1 boundary.
- [x] Define the coordinator protocol and delivery guarantees.
- [x] Implement pure identity, capability, lifecycle, and model-route types.
- [x] Implement a serialized coordinator with explicit state transitions.
- [x] Implement task creation, dependency readiness, atomic claim, result, block, and cancellation.
- [x] Implement typed mailbox messages, stable IDs, per-sender sequence, acknowledgement, deduplication, and request/reply state.
- [x] Implement scoped peer discovery and direct-message authorization.
- [x] Implement hierarchical resources, declared workspace paths, ownership, shared/mutable borrows, FIFO waiters, transfer, release, and versioned snapshots.
- [x] Implement lease heartbeats and crash/expiry reclamation.
- [x] Implement append-only transaction journal replay and malformed-tail recovery.
- [x] Implement local Unix-socket and Windows named-pipe broker transport.
- [x] Implement reconnect and unacknowledged-inbox synchronization.
- [x] Implement bounded recursive spawn limits and cancellation of managed subtrees.
- [x] Implement Pi SDK child-session creation with exact model/thinking inheritance.
- [x] Implement deadlock-free clarification pause/resume through `terminate: true` and a later reply prompt.
- [x] Implement message delivery while a child session is busy without dropping messages.
- [x] Implement child-safe capability guards for shell and repository writes at the final file-write operation.
- [x] Implement shared workspace mode and explicit Git worktree mode.
- [x] Implement compact `/agents` status/tree/tasks/resources/messages command output.
- [ ] Implement bounded root status widget and richer live fabric panel.
- [x] Add runnable examples for simple children, clarification, peers, mixed models, ownership, and recursion.
- [x] Add the required user documentation under `docs/`.

## Deterministic verification

- [x] Test lifecycle transition validity and terminal idempotence.
- [x] Test 100 concurrent task claims with exactly one winner.
- [x] Test shared-reader coexistence and writer exclusion across equal/ancestor/descendant resources.
- [x] Test FIFO mutable waiter grant after release.
- [x] Test atomic concurrent transfer and capability rejection.
- [x] Test message identity, duplicate send, per-sender order (including identical timestamps), acknowledgement, idempotent busy delivery, and reconnect recovery.
- [x] Test parent clarification with no synchronous wait/deadlock.
- [x] Test busy-recipient delivery and delayed replies.
- [x] Test child/parent/peer visibility boundaries.
- [x] Test lease heartbeat, expiry, agent crash, and cancellation release.
- [x] Test journal replay, committed transactions, incomplete final transaction, and invalid records.
- [x] Test model precedence, exact provider matching, and no silent global fallback.
- [x] Test depth/child/total/concurrent limits and recursive spawn denial.
- [x] Test capability enforcement and sender spoof resistance.
- [x] Test simultaneous child completion and structured task results.
- [x] Test workspace failure and clean/dirty Git behavior.
- [x] Test Pi child tool integration with filesystem and session doubles.
- [x] Run typecheck, unit tests, package smoke tests, and diff checks.

## Hardening completed for v0.1 release

- [x] Require mutable borrows for owner and delegated filesystem writes; declare workspace-relative file/module paths.
- [x] Make assigned-task completion explicit and derive turn-end lifecycle from durable task state.
- [x] Make semantic terminal states irreversible while allowing one explicit broker-recovery reconnect window.
- [x] Preserve pending clarification records across broker restart.
- [x] Add deterministic broker ordering and host-side in-flight message deduplication.
- [x] Tighten peer/resource capability ceilings and private worker message history.
- [x] Add adversarial regression coverage and a GitHub Actions check workflow.
- [x] Run the CI check workflow on a Linux/macOS/Windows matrix.
- [x] Make the fabric root participate in borrowing: root `edit`/`write` calls are vetoed before mutation when a live foreign hold overlaps the target, while undeclared root paths stay writable and the root shell remains documented as trusted.
- [x] Give `agent.spawn`/`task.create` durable `operationId` idempotency with journal-persisted dedup records, replay-on-retry, `IDEMPOTENCY_CONFLICT` on mismatched reuse, and a single bounded ambiguous-failure retry in the broker client.
- [x] Reserve `maxTotalAgents` capacity for reconnectable actors across broker restarts so new agents cannot evict an expected reconnection.
- [x] Resolve filesystem path identity (symlinks, junctions, alternate spellings) at the guarded write boundary so holds are enforced against the real target; unresolvable paths fail closed.
- [ ] Optional: resolve `agent_resource` declaration paths through the same real-path identity at declaration time (enforcement side is canonical today; mismatched alias declarations currently fail closed as undeclared).

## Hardening / v1.x

- [ ] Deferred write fencing: a fencing/lease epoch for guarded writes. The write-boundary `resource.check_write` already refuses expired holds, but a local filesystem write cannot enforce a fencing token against a writer that passed the check moments before its lease lapsed; genuine fencing needs cooperative file locking, which is deliberately not promised.
- [ ] Add explicit broker health diagnostics and safe stale-lock cleanup.
- [ ] Add journal compaction/checkpointing without losing audit metadata.
- [ ] Add user-confirmed worktree cleanup with fresh Git safety checks.
- [ ] Add configurable role files with validation and source diagnostics.
- [ ] Add task/resource dependency stale-version reporting.
- [ ] Add richer TUI inspector only if compact status proves insufficient.
- [ ] Add property-based/concurrency schedule tests.
- [ ] Add measured startup, memory, and message-routing benchmarks.

## Deferred / not promised in v1

- [ ] Remote or distributed agents.
- [ ] Automatic merge/rebase/commit orchestration.
- [ ] Global broadcast chat by default.
- [ ] Full transcript replication or automatic transcript summarization.
- [ ] Arbitrary child extension propagation.
- [ ] External CLI/provider adapters.
- [ ] AST-level semantic conflict detection.
- [ ] General-purpose swarm/workflow DSL.
