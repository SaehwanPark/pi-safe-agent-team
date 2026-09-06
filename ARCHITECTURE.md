# pi-safe-agents-team architecture

> Status: v1 architecture baseline (2026-09-05). This document describes the implementation target; behavior is reconciled with tests as the code lands.

## North star

Agents make semantic decisions. A local coordinator makes coordination decisions. An LLM is never asked to act as a mutex, message queue, identity provider, or authority source.

## Concrete v1 scope

The first release is a local Pi extension and a small broker process:

- one authoritative coordinator per fabric;
- long-lived child `AgentSession` instances created through Pi's SDK;
- optional independent Pi sessions joining the same fabric through a local socket;
- bounded recursive spawning with explicit parent/child identity;
- typed, durable, at-least-once mailbox messages with request/reply correlation;
- parent, scoped-peer, and child-to-parent routing;
- explicit task graph with atomic claims and structured results;
- hierarchical declared resources with workspace-relative paths, ownership, shared/mutable borrows, transfer, release, leases, and versioned inspection;
- capabilities enforced by the coordinator and by child tool-operation guards at the final filesystem write boundary;
- exact model/provider/thinking resolution using the caller's in-memory Pi model for inheritance;
- compact `/agents` status views and a small tool surface;
- optional Git worktree creation for managed coding children;
- append-only broker journal with transaction markers, crash-tail recovery, and durable write-idempotency records.

The v1 implementation deliberately does not attempt a distributed service, remote agents, automatic merge/release policy, transcript replication, or unrestricted extension propagation into managed children.

## Components

```text
Pi root extension / managed child sessions
        |
        | local JSONL request/event protocol over Unix socket or Windows named pipe
        v
  BrokerServer (one process per fabric)
        |
        +-- PersistentJournal (append-only, transaction framed)
        +-- Coordinator (single serialized authority)
              +-- agent registry and lifecycle
              +-- task graph and claims
              +-- mailbox and request/reply state
              +-- resource ownership/borrows/leases
              +-- bounded event/status projections
```

### `Coordinator`

`src/core/coordinator.ts` is synchronous and owns all mutable coordination state. The broker invokes one operation at a time, so a task claim, resource acquisition, transfer, or message send is an atomic state transition. It returns a result plus internal events; it never trusts an agent-supplied sender field. The class remains a single transaction boundary intentionally: splitting the maps across services would reintroduce cross-component locking; transport, policy, and Pi-operation guards are kept outside it instead.

The coordinator uses plain serializable state internally. `exportState()`/`restoreState()` are used only for the broker's transaction rollback path and tests. Public records are defensive copies. Capability ceilings are intersections: explicit peer/resource grants cannot be widened by a child, and an empty explicit peer list means no exception.

### `BrokerServer` and `FabricClient`

`src/broker/` implements the local transport. A client connection has an authenticated actor identity after the hello/register handshake. Notifications are delivery hints, not the source of truth: unacknowledged mailbox messages are returned by `inbox` after reconnect. Mutating requests have stable request IDs and bounded in-memory idempotency caching.

A broker transaction is written as:

```text
begin(txId)
event(txId, ... [, idempotency])
event(txId, ...)
commit(txId)
```

Recovery replays only committed transactions and ignores an incomplete final transaction. When an `event` record carries a write-idempotency record, its dedup entry is restored on commit alongside the events, so an ambiguous client retry after restart still resolves to the original response. The broker is the sole journal writer; agents never edit state files.

### `FabricRuntime` and `AgentHandle`

`src/pi/runtime.ts` bridges Pi's extension/SDK lifecycle to the coordinator. The root extension registers its own stable session identity. `spawn_agent` first obtains a coordinator-created child identity, then creates a persistent Pi `AgentSession` with:

- an exact resolved `Model` object;
- a child-local `ModelRuntime` using the Pi agent auth/model stores without refreshing remote catalogs;
- the selected effective thinking level;
- a session directory under the fabric state directory;
- scoped coordination tools bound to the child identity;
- guarded Pi built-in operations for resource-authorized file writes and conservative shell access.

A handle owns at most one active `session.prompt()` call. Incoming messages are queued in the broker and then delivered through Pi's `steer`, `followUp`, or a fresh prompt. Acknowledgement follows host queue acceptance, and an in-flight/accepted ID set makes notification plus inbox replay idempotent within the host process. A clarification request is not awaited by the caller's JavaScript stack: the ask tool records a request and returns `terminate: true`; the child becomes `waiting`; a reply later starts a new prompt. This is the deadlock-free pause/resume path.

Managed children do not load the parent extension set a second time. They retain Pi's built-in tools, project context files, and skills, while the host supplies only the scoped coordination and guarded built-in operations. Arbitrary child extension inheritance is deferred because it can duplicate registrations and reintroduce unsafe orchestration paths.

### Model routing

`src/core/routing.ts` is pure and records the source of the decision:

```text
explicit spawn override
  > role configuration
  > fabric defaults
  > caller's in-memory model (inherit)
  > Pi/global default only when no caller model exists
```

`inherit` is resolved to the caller's actual `provider/model` before `createAgentSession`; the child never receives an ambiguous missing model that could read another session's global settings. Explicit unresolved models fail closed. No provider fallback is implicit.

### Resources

Resource IDs are opaque strings, with optional explicit `parentId` links and workspace-relative `path` declarations. Two resources overlap when they are equal or one is an ancestor of the other; declared file/module paths also establish overlap when links are omitted. An owner is a semantic authority, not an active lock: even the owner must hold a mutable borrow for a guarded file write. A mutable holder is exclusive across an overlapping hierarchy; shared holders conflict with a mutable holder but may coexist with one another. A holder cannot retain a shared lease while acquiring a mutable lease. Waiting requests are globally ordered by enqueue time across overlapping resources and are granted by the coordinator after release/recovery.

Each active hold has a lease. Hosts heartbeat while a session is active; the broker also reclaims expired leases. A process disconnect or terminal agent state releases runtime holds and wakes waiters. Resource `version` increments on mutable release/transfer, and `snapshot` returns a stable version token for stale-dependency checks.

Managed `edit`/`write` tools use Pi's operation override to call `resource.check_write` immediately before the final filesystem write. The target must be inside the managed workspace and match a declared file/module path. Shared-workspace shell uses a conservative read-only allowlist; a worktree shell is explicitly trusted and isolated by the Git worktree convention, so it is documented as a semantic escape hatch rather than a mechanically resource-guarded mutation path.

The root also participates in borrowing. A Pi `tool_call` veto intercepts the root session's `edit`/`write` calls before mutation and consults `resource.check_write` with `hostGuard: true`: undeclared paths remain writable (the root need not declare everything first), but any live foreign hold on an overlapping declared resource blocks the root write. The root therefore cannot mechanically race a child's borrow; only its unintercepted shell remains a trusted mutation path, and that is documented as such rather than claimed as guarded.

### Workspaces

`src/workspace.ts` is a small pluggable strategy boundary. `shared` uses the caller's cwd. Explicit `worktree` mode requires a clean Git checkout and creates a detached managed worktree from a safe base ref. Worktree paths and branches are recorded in agent metadata. v1 does not auto-delete dirty worktrees: terminal artifacts remain inspectable, and cleanup is intentionally explicit.

## State and data flow

1. A Pi extension lazily connects to the fabric on `session_start` or the first coordination tool call.
2. The broker validates the connection actor and operation against the coordinator's current state.
3. For mutations, the coordinator applies the operation, the server persists its event batch, and only then broadcasts derived notifications.
4. A notification causes a relevant recipient host to enqueue a compact custom message; unrelated agents see nothing.
5. A recipient acknowledges only after Pi has accepted the message into its mailbox/queue. Reconnect syncs anything still unacknowledged, and duplicate delivery attempts reuse the host's accepted-ID state.
6. Root UI status reads a bounded projection (`agents`, `tasks`, `resources`, recent messages); full message bodies are fetched only on request.

## Failure behavior

- model lookup/auth/session creation failure: child spawn fails and the coordinator-created identity is cancelled/released;
- broker failure: clients report a structured unavailable error and reconnect without replaying mutations automatically; a broker restart preserves pending semantic requests while marking live actors reconnectable for one matching-token reattach;
- malformed journal tail: committed transactions before the tail remain usable; the tail is ignored and surfaced in diagnostics;
- child crash: host marks it failed, releases task/resource runtime state, and sends a compact `agent_failed` notice to its parent;
- parent shutdown: the managed subtree is cancelled, leases are released, and the broker retains bounded audit metadata;
- cancellation: idempotently aborts the Pi session and releases task/resource/mailbox waits;
- limits: spawn returns a structured limit error; it never recursively retries or silently creates an unbounded worker.

## Pi-native integration decisions

- use `ExtensionAPI.registerTool`, `registerCommand`, `sendMessage`, `appendEntry`, lifecycle events, and `registerMessageRenderer`;
- use `createAgentSession`/`SessionManager` for managed child sessions rather than one-shot model calls;
- use `ctx.model` and `ctx.modelRegistry` for model identity and resolution;
- use custom session entries only for compact fabric metadata, never raw child transcripts;
- use `ctx.ui.setWidget`/notifications for progressive disclosure and keep the default prompt small.

## Important invariants

1. A resource has at most one mutable holder across its hierarchy.
2. A mutable holder cannot coexist with any shared holder across its hierarchy.
3. An exclusive task has zero or one owner, claim is atomic, and completion is an explicit task transition rather than an inference from a model turn.
4. Transfer is one serialized state transition; observers see either the old or new owner.
5. Only coordinator-issued actor identity and capabilities authorize mutations.
6. Message IDs are stable; sends are at-least-once and deduplicable by client key.
7. Per-sender message sequence is FIFO; no global ordering is promised.
8. Asking for a reply changes agent state to `waiting`; it never blocks the broker or parent turn, and broker restart does not discard the pending request.
9. Expired/dead agent leases are reclaimable and cannot permanently lock resources; broker-recovery liveness failures are reconnectable once, while semantic terminal states are not.
10. Cancellation is idempotent and releases runtime-owned task/resource state.
11. A child cannot exceed depth, child-count, total-agent, or capability limits.
12. An explicit model route wins over role/default/inheritance and never silently changes provider.
13. Model-generated payloads cannot set `from`, capabilities, ownership, or task authorship.
14. Incoming messages are retained until accepted and acknowledged; busy receivers do not drop them, and same-sender inbox replay is ordered by `senderSequence` rather than timestamps or random IDs.
15. Root status is a bounded projection and does not copy unrelated transcripts into model context.
16. `agent.spawn`/`task.create` carrying an `operationId` apply at most once per `(actor, operationId)`; a matching retry replays the original response, a mismatch fails `IDEMPOTENCY_CONFLICT`, and the record is journaled with its transaction (bounded window, oldest evicted).
17. The idempotency window is bounded and durability-scoped: it is restored on replay/checkpoint within the window, but it never reconstructs pre-hardening committed transactions that lack a record.

## Deferred by design

- remote/network transport and multi-host fabrics;
- a generalized workflow DSL or swarm scheduler;
- automatic Git merge/rebase/commit policy;
- automatic deletion of dirty worktrees;
- content-addressed file/symbol indexing and automatic AST conflict detection;
- full Pi extension/plugin propagation into children;
- external provider/CLI agent adapters;
- transcript summarization by another LLM;
- persistent broker leadership election beyond the local lock/endpoint;
- user-facing role framework beyond small JSON defaults and ad hoc spawn.

## Risk

**High** — this is a public extension with concurrency, persistence, process lifecycle, workspace, and security-boundary behavior. The risk is controlled by keeping the coordinator pure/synchronous, testing it without LLM calls, and failing closed at transport and capability boundaries.
