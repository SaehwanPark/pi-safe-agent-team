# Changelog

## 0.1.0

- Added the deterministic coordinator for recursive agents, tasks, typed mailboxes, resources, leases, capabilities, cancellation, and model-route policy.
- Added local JSONL broker transport, transaction journal replay, reconnect credentials, and durable inbox recovery.
- Added Pi SDK runtime integration, child-safe coordination tools, explicit model runtime/session creation, commands, renderers, and workspace strategies.
- Added deterministic unit, broker, journal, reconnect, routing, tool, and Git workspace tests.
- Hardened release boundaries: mutable borrows now authorize actual guarded file writes (including owners), assigned tasks require explicit completion, terminal agents cannot be revived outside one broker-recovery reattach window, pending clarification survives restart, messages have deterministic FIFO/replay and host-side duplicate suppression, capability/history visibility is narrowed, and shared-workspace shell is conservative/read-only.
- Added adversarial hardening coverage for workspace-scoped read tools, shell alias/argument escapes, in-flight broker request coalescing, and a GitHub Actions typecheck/test/build workflow.
- Made the fabric root participate in borrowing: root-session `edit`/`write` tool calls are vetoed before mutation when a live foreign hold overlaps the target (`resource.check_write` with `hostGuard: true`), while undeclared root paths stay writable and the root shell remains documented as trusted.
- Enforced path identity at the guarded write boundary: symlinks, junctions, and alternate spellings are resolved to the target's real filesystem path before `resource.check_write`, so writing through an alias can no longer bypass another actor's hold; unresolvable targets fail closed, alias-escapes stay uncoordinated for the root and denied for children.
- Reserved `maxTotalAgents` capacity for broker-recovery reconnectable actors: new registrations and spawns are refused while a reconnect window is open, so a newcomer can no longer evict an expected reconnection; the reservation ends on reconnect, turn resolution, or cancellation.
- Added durable `operationId` idempotency for `agent.spawn` and `task.create`: one `(actor, operationId)` applies exactly once, matching retries replay the original response (`replayed: true`), mismatched reuse fails `IDEMPOTENCY_CONFLICT`, records are journaled with their transaction (and restored on replay/checkpoint within a bounded window), and the broker client retries ambiguous transport failures once under the same operationId.
