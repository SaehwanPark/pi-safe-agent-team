# Changelog

## Unreleased

- **Session-Scoped Default Fabrics**: Lazily finalize the default fabric identity from the canonical workspace and root Pi session ID, isolating concurrent sessions in one repository while preserving same-session reconnects. Explicit fabric/state/endpoint options remain advanced sharing overrides.
- **Recovery-Safe Embedded Degradation**: Deactivate embedded context managers during native fallback so recovery files referenced by prior reduced tool output remain available until child shutdown; final disposal still cleans manager-owned storage.
- **Compaction Metadata Separation**: Pass only explicit `customInstructions` to child Pi compaction; diagnostic `reason` values no longer become model instructions.
- **Managed-Child Integration Smoke**: Extend the opt-in model-backed smoke to load both projects, provision disposable auth/model fixtures, spawn a real child, require the `lcm-embedded` context mode, and verify a completed child task plus root quiescence marker; credential-free load smokes remain unchanged.

## 0.2.2

- **Canonical Agent Directory Resolution**: Resolved agent directory via `@earendil-works/pi-coding-agent`'s `getAgentDir()` honoring `PI_CODING_AGENT_DIR` uniformly for root and all managed children, deprecating legacy `PI_AGENT_DIR`. Added full-isolation test verifying child session paths, auth/models paths, and broker state stay under `PI_CODING_AGENT_DIR` without leaking into `~/.pi/agent`.
- **Active Write Fence Protection in Root Shell**: Extended `evaluateRootShellGuard` to inspect active child write fences (`status.fences`) alongside mutable holds. When a child's lease expires during an in-flight write, colliding path-scoped mutations and broad mutators remain safely blocked, while mutations on distinct, unheld paths remain permitted when fence path details are known. Fails closed for mutators if fence path details are omitted.
- **Cached Filesystem Case-Sensitivity**: Cached workspace case-sensitivity detection per directory in `detectCaseInsensitivePaths` and synchronized it from broker config (`config.caseInsensitivePaths`), eliminating repeated filesystem probe file churn (`.pi-case-*`) during read-only snapshots and shell preflights.
- **Execution Wrapper Unwrapping in Root Shell**: Added automatic unwrapping of execution wrappers (`npx`, `pnpm exec`, `yarn exec`, `bunx`, `uv run`, `poetry run`, `pipenv run`, `python -m`, `python3 -m`) in `classifyRootShellCommand` so underlying mutator commands and their file targets are accurately identified.
- **Embedded Manager Disposal & Reconnect Context Mode**: Updated `ManagedChild.degradeToNativeContext()` to explicitly dispose the embedded controller (`embeddedManager.dispose()`) and clear its reference, and persisted `contextMode` (`native` vs `lcm-embedded`) across child reconnect registration (`agent.register`).
- **Simplified Resource Granted Wakeup**: Streamlined `resource_granted` delivery to unconditionally trigger an immediate root turn as `steer`, eliminating redundant `hasPendingRootRequest` broker status queries.
- **Multi-Scenario Isolated Smoke Testing**: Upgraded `scripts/smoke-isolated-pi.sh` to support explicit execution modes (`load`, `with-lcm`, `integration`, `all`), replaced hardcoded personal paths with sibling-relative/env resolution (`$LCM_DIR`), added fail-closed verification if LCM is missing in `with-lcm`, and allocated disposable temporary directories per scenario. Added npm scripts `smoke:pi:with-lcm` and `smoke:pi:integration`.
- **Extension Interop Registry & Canonical Fabric State Provider**: Registered `safe-agent-team.fabric-state.v1` in `Symbol.for("pi.extension-interop.v1")`, providing deterministic and conservative quiescence signals (`quiescent: boolean`, `state: "known" | "uncertain"`, `sessionReplacementSafe: boolean`) for companion extensions like `local-context-manager`. Idempotent and conflict-aware provider registration prevents silent cross-extension overwrites.
- **Fail-Closed Fabric State Uncertainty**: Handled broker status RPC failures while root is attached by returning `active: true, quiescent: false, state: "uncertain", sessionReplacementSafe: false`, preventing unsafe destructive session resets during outages. Included root running turn state in quiescence calculation (`rootBusy`).
- **Root Session Replacement Semantics**: Documented and verified that root Pi session shutdown/replacement cancels managed descendants, requiring companion extensions to check `sessionReplacementSafe` before swapping root sessions.
- **Embedded Context Integration & Correct Compaction Types**: Discovered and consumed `local-context-manager.embedded-context.v1` inside `ManagedChild` to apply adaptive output reduction and compaction to child tool outputs while strictly preserving `noExtensions: true`. Fixed compaction delegation to pass `customInstructions?: string` directly to `AgentSession.compact()`, eliminating type mismatches, and synchronized context degradation to coordinator via `agent.update({ contextMode: "native" })`.
- **Centralized Root Message Delivery Policy**: Implemented table-driven delivery in `src/pi/delivery.ts` (`classifyRootDelivery`), preventing routine background `progress` and `inform` messages from waking the root model turn while guaranteeing immediate turn wakes for `clarification`, `escalation`, `blocked`, `agent_failed`, and `task_result`.
- **Root Shell Mutator Preflight Guard Hardening**:
  - Reclassified arbitrary code/test execution (`cargo test`, `cargo check`, `pytest`, `npm test`, `node`, `dotnet run`) from `read-only` to `unknown`, preserving the trusted developer escape hatch without falsely labeling code execution as read-only.
  - Added support for `tee` writer commands and no-space/quoted redirections (`echo x>file.ts`, `printf x>>file.ts`).
  - Directory changes (`cd`, `pushd`) mark chained mutator scope as broad.
  - Canonical path identity resolution via `resolvePolicyPathIdentity` and filesystem case-folding when matching shell targets against child mutable holds.
- **Root/Child Capability Asymmetry Guidance**: Added explicit guidance in `agent_spawn` tool descriptions, bootstrap instructions, and `before_agent_start` indicating that external web, browser, MCP, and computer-use tools are root capabilities not inherited by managed children, directing children to request external info via parent clarification.
- **Enhanced `/agents` Diagnostics**: Surfaced child model route provenance, workspace mode, context mode (`native` vs `lcm-embedded`), and active write fences.


## 0.2.1

- Fixed Pi root reattachment by forwarding the persisted reconnect credential during root registration, eliminating the recurring `requires its reconnect credential` startup error.
- Added an integration regression test covering root reattachment across broker restart.

## 0.2.0

- Finalize the root broker turn on Pi's `agent_settled` event instead of `agent_end`, ensuring queued continuations, auto-retries, or compactions complete before turn finalization.
- Serialized root lifecycle attachment and turn-boundary requests using a generational `LifecycleQueue` so `agent_start` and `agent_settled` cannot race a pending root registration.
- Ignored stale lifecycle callbacks after session reload or shutdown, and suppressed the expected detached-root warning during teardown.
- Added a lifecycle-safe coordination failure boundary without changing broker task semantics.
- Added integration test coverage for settled turn finalization, graceful shutdown waiting for in-flight handlers, and cross-session lifecycle queue serialization.
- Clarified in documentation that session-control methods belong in commands / `withSession`, not lifecycle event handlers.
- Refined interactive GitHub Pages documentation site, navigation footer, and SVG broker architecture diagram.

## 0.1.2

- Finalize the root broker turn on Pi's `agent_settled` event instead of `agent_end`, which may be followed by automatic retry, compaction, or queued continuation.
- Added integration coverage for one settled finalization and shutdown waiting for an in-flight settled handler.
- Documented that session-control methods belong in commands/`withSession`, not lifecycle event handlers.

## 0.1.1

- Serialized root lifecycle attachment and turn-boundary requests so `agent_start` and `agent_settled` cannot race a pending root registration.
- Ignored stale lifecycle callbacks after shutdown/reload and suppressed the expected detached-root warning during teardown.
- Added a lifecycle-safe coordination failure boundary without changing broker task semantics.

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
- Extended CI to a Linux/macOS/Windows matrix (`fail-fast: false`) so the Windows-first named-pipe, junction, and path-identity behaviors and macOS filesystem assumptions are verified on every push and PR.
- Extracted the coordinator's wire-format internals (argument parsing, capability ceilings, policy-key canonicalization, idempotency keys/fingerprints, clone helpers) into an internal `src/core/coordinator-wire.ts` module, shrinking the state-transition class toward one concern; behavior-preserving, still the only writer of coordinator state, and not part of the public export surface.
- Closed the formal check-to-write race as far as a local filesystem allows: guarded writes now run `resource.begin_write` → write → `resource.end_write`, placing a short-lived coordinator fence on the target so no conflicting lease is granted to another actor while a write is in flight, even if the writer's hold lapses mid-write. Write fencing protects coordinated writes during normal broker operation (including lease expiry) and protects against root host-guard writes, but is not crash-durable across an independent broker restart (post-v0.1 recovery quarantine planned).
- Made policy-key case folding follow the filesystem instead of only the platform: the broker now probes its volume at startup using matching uppercase casing (`.MiXeD` vs `.MIXED`) against the workspace volume (`policyRoot`), folding paths into one resource on case-insensitive mounts (typical macOS default), with `FabricConfig.caseInsensitivePaths` forcing either behaviour.
- Tightened smaller-issue audit items: same-owner `resource.claim` is now an idempotent reaffirmation that never bumps the resource version, `resource.release` requires exactly one selector, and phantom operations were removed from the protocol list.
- Allowed per-request timeout overrides on `BrokerClient` (`request` and `requestIdempotent`) and made `requestTimeoutMs` mutable.
- Protected root `hostGuard` writes against active foreign write fences even after the writer's lease has expired.
- Wired `requestIdempotent` into root `agent_task(create)` via `FabricRuntime` and `lazyClient`.
- Made root write guard fail closed on `BROKER_UNAVAILABLE` for coordinated workspace writes to protect in-flight child writes during broker outages.
- Rejected indirect file-list options (`file -f`, `--files0-from`) in read-only shell inspection.
