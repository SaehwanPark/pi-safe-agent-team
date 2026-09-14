# pi-safe-agents-team architecture

> Status: v0.1 architecture baseline (2026-09-06). This document describes the implementation target; behavior is reconciled with tests as the code lands.

## North star

Agents make semantic decisions. A local coordinator makes coordination decisions. An LLM is never asked to act as a mutex, message queue, identity provider, or authority source.

## Concrete v0.1 scope

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

The v0.1 implementation deliberately does not attempt a distributed service, remote agents, automatic merge/release policy, transcript replication, or unrestricted extension propagation into managed children. Future v1.0 milestones will address multi-host / distributed fabric scaling.

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

The coordinator uses plain serializable state internally. `exportState()`/`restoreState()` are used for the broker's transaction rollback path, journal recovery, and tests; active in-memory fence records are intentionally recreated empty, while their resource restart quarantines are serializable. Public records are defensive copies. Pure read projections skip rollback snapshots; broker mutations pass one shared snapshot into the coordinator rather than cloning the world twice. `fabric.snapshot` is a server-side aggregate projection for quiescence and stays small even when diagnostic history is large; `fabric.status` is also bounded and reports truncation/counts. Terminal agents, tasks, and resolved requests move to bounded compact tombstones after the configured retention horizon, preserving identity/dependency safety without growing hot state forever. Retained Git/session artifacts are cold records, so useful committed work does not pin terminal hot records. Historical GC is incremental with a per-maintenance record budget and one-pass relationship indexes for child, task, resource, and dependency references; a suspended laptop cannot force an unbounded cleanup transaction. Capability ceilings are intersections: explicit peer/resource grants cannot be widened by a child, and an empty explicit peer list means no exception.

### `BrokerServer` and `FabricClient`

`src/broker/` implements the local transport. A client connection has an authenticated actor identity after the hello/register handshake. Notifications are delivery hints, not the source of truth: unacknowledged mailbox messages are returned by `inbox` after reconnect. Mutating requests have stable request IDs and bounded in-memory idempotency caching.

A broker transaction is written as:

```text
begin(txId)
event(txId, ... [, idempotency])
event(txId, ...)
commit(txId)
```

Recovery replays only committed transactions and ignores an incomplete final transaction. When an `event` record carries a write-idempotency record, its dedup entry is restored on commit alongside the events, so an ambiguous client retry after restart still resolves to the original response. ACKs use the same bounded durable replay path, including after the acknowledged message has been pruned. Capacity-blocked model turns persist FIFO tickets; a grant records the exact operation/purpose and a short expiry, emits a targeted wake, and remains `ready` until the host claims it. The broker is the sole journal writer; agents never edit state files.

### `FabricRuntime` and `AgentHandle`

`src/pi/runtime.ts` bridges Pi's extension/SDK lifecycle to the coordinator. The root extension registers its own stable session identity. `spawn_agent` first obtains a coordinator-created child identity, then creates a persistent Pi `AgentSession` with:

- an exact resolved `Model` object;
- a child-local `ModelRuntime` using the Pi agent auth/model stores without refreshing remote catalogs;
- the selected effective thinking level;
- a session directory under the fabric state directory;
- scoped coordination tools bound to the child identity;
- guarded Pi built-in operations for resource-authorized file writes and conservative shell access.

The default fabric scope is `hash(canonicalWorkspace + "\0" + rootPiSessionId)`. It is
resolved lazily at root attachment so two concurrent Pi sessions in one repository
cannot alias a broker, root actor, reconnect token, or child namespace. Reattaching the
same Pi session retains the scope; explicit fabric/state/endpoint options remain the
advanced opt-in for intentional sharing.

A handle owns at most one active `session.prompt()` call. Incoming messages are queued in the broker and then delivered through Pi's `steer`, `followUp`, or a fresh prompt. Coalesced control messages retain their logical ID but advance a payload revision; host delivery state and broker ACKs carry the ID/revision pair so an older session entry cannot acknowledge a newer body. A clarification request is not awaited by the caller's JavaScript stack: the ask tool records a request and returns `terminate: true`; the child becomes `waiting`; a reply later starts a new prompt. This is the deadlock-free pause/resume path.

Managed children do not load the parent extension set a second time (`noExtensions: true`). They retain Pi's built-in tools, project context files, and skills, while the host supplies only the scoped coordination and guarded built-in operations.

### Canonical agent directory resolution

The runtime uses `@earendil-works/pi-coding-agent`'s canonical `getAgentDir()` so `PI_CODING_AGENT_DIR` is honored uniformly across the root session and all child `AgentSession` instances. Precedence is simply: `options.agentDir ?? getAgentDir()`, where `getAgentDir()` natively checks `PI_CODING_AGENT_DIR` and falls back to `~/.pi/agent`.

### Child capability boundaries and root asymmetry

Root capabilities such as web access, browser automation, MCP tools, and computer use are deliberately omitted from child toolsets. Managed children receive only safe, guarded coordination and workspace tools. When a child requires external information or actions outside its local workspace, it sends a clarification or request message to its parent rather than failing silently.

### Extension interop and embedded context

A process-local interop registry via `Symbol.for("pi.extension-interop.v1")` allows safe cooperation between extensions without hard dependencies:
- **`safe-agent-team.fabric-state.v1`**: The fabric runtime exports a deterministic and conservative state snapshot (`quiescent: boolean`, `state: "known" | "uncertain"`, `sessionReplacementSafe: boolean`, active tasks, unowned tasks, reconnecting agents, pending model-turn admissions, mutable holds, write fences, restart quarantines, pending requests in either root direction, `rootCompactionInFlight`, and root context health/diagnostic). Root manual/automatic/embedded compaction keeps replacement unsafe until its terminal hook; root session replacement (`session_shutdown`) cancels managed child agents. Companion context managers consume this to defer destructive compaction, semantic resets, or root session rewinds until `sessionReplacementSafe === true`.
- **`pi-local-context-manager.embedded-context.v1`** (legacy alias `local-context-manager.embedded-context.v1`): When present, `ManagedChild` obtains an embedded context controller. It applies adaptive output reduction and turn compaction to child tool outputs while retaining `noExtensions: true`. Options distinguish `logicalContextWindow` from an optional effective prefill budget, and compaction instructions are delegated as `string | undefined` to upstream Pi's `AgentSession.compact(customInstructions?: string)`. Any provider failure deactivates the controller while retaining its reference and manager-owned recovery files, synchronizes to the coordinator with `agent.update({ contextMode: "native" })`, and exposes a bounded diagnostic; final child shutdown calls `dispose()` for cleanup.

### Model-route capacity and turn outcomes

The coordinator's global `maxConcurrentAgents` limit is not a proxy for model-runtime
memory. `modelRoutePolicies` therefore add a provider/model capacity and optional
effective prefill budget. The process-local FIFO `ModelRouteCapacityArbiter` gates
managed-child generations and compaction on the same route; the coordinator enforces
the durable route slot for cross-process state and retains FIFO tickets when a slot is full. A
slot grant records the exact operation/purpose with a short expiry, emits a targeted wake,
and leaves the agent `ready` until the exact host claims the ticket. A lost grant returns
that ticket to FIFO on expiry; draining/recovery removes it immediately. Local providers
default to one heavy operation. Pi terminal responses are classified by `src/pi/turn-outcome.ts`, keeping
logical context overflow separate from structured prefill/KV pressure and runtime
memory pressure. Capacity gets one bounded retry; exhausted or failed recovery blocks
the task with a parent-visible diagnostic and preserves the session for explicit
reopening.

### Root message delivery policy

Root message delivery is centralized in `src/pi/delivery.ts` (`classifyRootDelivery`). Background notifications (`progress`, `inform`) append silently to the session history for the next natural model turn without waking the model (`triggerTurn: false`). High-priority notifications (`clarification`, `escalation`, `blocked`, `agent_failed`, `task_result`, `steer`, `urgent`) steer and trigger a root model turn immediately while the context is healthy. During root compaction or after an exhausted root provider/capacity outcome, deliveries remain durable, model-visible, and displayable as `nextTurn` without triggering competing work; a successful user-led recovery clears the degraded gate.

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

Each active hold has a lease. Hosts heartbeat while a session is active; the broker also reclaims expired leases. A process disconnect or terminal agent state releases runtime holds and wakes waiters. Resource `version` increments on mutable release/transfer, and `snapshot` returns a stable version token for stale-dependency checks. Resources have an explicit `active -> retired` lifecycle: only the root may retire a resource, and retirement requires no holds, waiters, quarantine, active child resources, or fences. Retired resources remain inspectable for audit but cannot be borrowed, granted, transferred, or used as a new hierarchy parent. A guarded write records a short durable restart quarantine on its resource through the fence expiry, so a surviving broker restart cannot immediately grant a conflicting writer after the ephemeral fence map is recreated.

Managed `edit`/`write` tools use Pi's operation override to execute a guarded write flow: `resource.begin_write` authorizes the write and places an active write fence on the matched resource, the filesystem mutation completes, and `resource.end_write` releases the fence. The target must be inside the managed workspace and match a declared file/module path. The write fence ensures that even if an agent's lease lapses mid-write, competing borrowers are excluded from clobbering the file. Shared-workspace shell uses a conservative read-only allowlist with argument path containment and indirect file-list exclusions (`file -f`, `--files0-from`); a worktree shell is explicitly trusted and isolated by the Git worktree convention, so it is documented as a semantic escape hatch rather than a mechanically resource-guarded mutation path.

The root also participates in borrowing. A Pi `tool_call` veto intercepts the root session's `edit`/`write` calls before mutation and invokes `resource.begin_write` with `hostGuard: true`: undeclared paths remain writable (the root need not declare everything first), but any live foreign hold or active foreign write fence on an overlapping declared resource blocks the root write. Coordinated writes fail closed if the broker is unavailable, protecting in-flight child writes during outages.

Guarded Pi writes participate in borrowing and fencing. Root shell remains a trusted escape hatch, with best-effort preflight blocking of recognized broad mutators (`git checkout`, `git restore`, `rm -rf`, `cargo fmt`, `prettier --write`, `sed -i`, `black`, `ruff format`, etc.) or pipe/redirection writes (including `tee` and no-space `>file` redirections) when live child mutable holds or active write fences exist. Execution wrappers (`npx`, `pnpm exec`, `yarn exec`, `bunx`, `uv run`, `poetry run`, `pipenv run`, `python -m`, etc.) are automatically unwrapped to classify the underlying command and its target paths. Observational commands (`git status`, `git diff`, `rg`, `cat`) are classified as read-only, while arbitrary code/test execution (`cargo test`, `npm test`, `pytest`, `node`, `dotnet run`) is classified as unknown, preserving the escape hatch without falsely labeling code execution as read-only. Path comparisons use canonical path identities and cached filesystem case folding; unrelated path mutations remain permitted when fence path identities are known, while unknown fence paths fail closed. Root shell commands are not claimed to be a fully sandboxed or formally complete barrier.


> **Durability boundary note**: The live fence object is in memory and is recreated empty after a broker restart. Its resource quarantine is journaled with the `resource.begin_write` transition and remains foreign-write exclusive until the original fence expiry, unless the authenticated original actor reconnects and completes `resource.end_write`.

### Workspaces

`src/workspace.ts` is a small pluggable strategy boundary. `shared` uses the caller's cwd. Explicit `worktree` mode requires a clean Git checkout and creates a managed worktree from a resolved base commit. Worktree paths and branches are recorded in agent metadata. Child shutdown reclaims a worktree only when it is clean **and** `HEAD === baseRef` after the session abort completes; a clean branch containing child commits, dirty state, or uncertainty remains inspectable and requires explicit cleanup. On a later root startup, the runtime asks the strategy to clean workspaces belonging to recovery-retired terminal children after their grace window, while preserving any workspace for which the strategy reports a dirty, divergent, or uncertain state and removing the matching child session directory only after workspace cleanup succeeds. A clean worktree whose `HEAD` diverged from `baseRef` is a deliberately retained artifact rather than a cleanup failure that pins the hot agent forever: workspace/branch/base/head/session metadata is recorded in cold state and `artifactDisposition: "retained"` prevents repeat GC. A durable `artifactsCleanedAt` marker (with `artifactDisposition: "cleaned"`) prevents successful cleanup from being revisited.

## State and data flow

1. A Pi extension lazily connects to the fabric on `session_start` or the first coordination tool call.
2. The broker validates the connection actor and operation against the coordinator's current state.
3. For mutations, the coordinator applies the operation, the server persists its event batch, and only then broadcasts derived notifications.
4. A notification causes a relevant recipient host to enqueue a compact custom message; unrelated agents see nothing.
5. A recipient acknowledges only after Pi has accepted the exact message revision into its mailbox/queue. Reconnect syncs anything still unacknowledged, and duplicate delivery attempts reuse the host's accepted ID/revision state.
6. Root UI status reads a bounded projection (`agents`, `tasks`, `resources`, recent messages); full message bodies are fetched only on request. Quiescence consumers use the smaller server-side `fabric.snapshot` aggregate, never historical task output.

## Failure behavior

- model lookup/auth/session creation failure: child spawn fails and the coordinator-created identity is cancelled/released;
- broker failure: clients report a structured unavailable error and reconnect; only `agent.spawn`/`task.create` retry automatically, once, under the same `operationId`, so an ambiguous failure replays instead of duplicating; a broker restart recovery-fences each live subtree, preserves pending semantic requests, marks actors reconnectable for one matching-token reattach, and keeps their capacity slots reserved until they reconnect, resolve, or are cancelled; a delayed maintenance tick after a detected event-loop pause skips new stale transitions once so live hosts can heartbeat;
- malformed journal tail: committed transactions before the tail remain usable; the tail is ignored and surfaced in diagnostics;
- child crash: a semantic provider/session failure marks it failed, releases task/resource runtime state, and sends a compact `agent_failed` notice to its parent; transport loss or broker recovery is represented as reconnectable liveness state without a semantic failure notice, while grace expiry emits the failure notice when the actor is actually retired;
- parent shutdown: the managed subtree is cancelled, leases are released, and the broker retains bounded audit metadata;
- cancellation: idempotently drains and aborts the Pi session, releases task/resource/mailbox waits, and cascades across the descendant tree;
- limits: spawn returns a structured limit error; it never recursively retries or silently creates an unbounded worker.
- recovery cleanup: grace expiry recursively retires a stale subtree, releases unfinished task ownership only at that terminal boundary, marks messages to permanently terminal actors as undeliverable for retention, and lets a fresh runtime reclaim only clean-at-base abandoned worktrees and session directories when no live or recovering actor references them.

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
9. Expired/dead agent leases are reclaimable and cannot permanently lock resources; broker-recovery liveness failures are reconnectable once, while semantic terminal states are not; a reconnectable actor is reserved/live for subtree completion, parent topology, and child capacity until its recovery right ends, and its `maxTotalAgents` slot stays reserved during the window so new agents cannot evict it. A drained actor's queued model ticket is removed immediately.
10. Cancellation is idempotent and releases runtime-owned resource state; a reconnectable actor retains semantic task ownership until its recovery grace expires or it reconnects.
11. A child cannot exceed depth, child-count, total-agent, or capability limits.
12. An explicit model route wins over role/default/inheritance and never silently changes provider.
13. Model-generated payloads cannot set `from`, capabilities, ownership, or task authorship.
14. Incoming messages are retained until the exact payload revision is accepted and acknowledged; busy receivers do not drop them, coalesced updates cannot be acknowledged by an older receipt, ACK commit/retry is replay-safe even after pruning, and same-sender inbox replay is ordered by `senderSequence` rather than timestamps or random IDs. Messages to permanently terminal actors become explicitly undeliverable and remain retention-eligible without being mislabeled as accepted.
15. Root status is a bounded projection and does not copy unrelated transcripts into model context; `fabric.snapshot` contains only bounded aggregates/summaries for quiescence; task/discovery lists use `(createdAt,id)` cursors and return `CURSOR_STALE` instead of rewinding after deletion; terminal history is archived into bounded tombstones with dependency edges.
16. `agent.spawn`/`task.create`/lifecycle turns/`message.ack` carrying an `operationId` apply at most once per `(actor, operationId)`; a matching retry replays the original response, a mismatch fails `IDEMPOTENCY_CONFLICT`, and the record is journaled with its transaction (bounded window, oldest evicted). A queued `agent.begin_turn` retains a durable FIFO ticket, transitions to an identified expiring grant, and reaches `running` only after the exact host claims it.
17. The idempotency window is bounded and durability-scoped: it is restored on replay/checkpoint within the window, but it never reconstructs pre-hardening committed transactions that lack a record.
18. Enforced path identity is the resolved filesystem path: guarded write boundaries resolve symlinks, junctions, and alternate spellings (nearest-existing-ancestor realpath, Windows case-folding in policy keys) before asking `resource.check_write`, so one writer cannot sidestep another actor's hold through an alias. The broker itself stays filesystem-agnostic; unresolvable targets fail closed, and an alias that escapes the workspace is simply not fabric-coordinated for the root while always denied for managed children.

## Deferred by design

- remote/network transport and multi-host fabrics;
- a generalized workflow DSL or swarm scheduler;
- automatic Git merge/rebase/commit policy;
- automatic deletion of dirty worktrees;
- automatic deletion of retired resources (retirement is explicit so audit references remain inspectable);
- content-addressed file/symbol indexing and automatic AST conflict detection;
- full Pi extension/plugin propagation into children;
- external provider/CLI agent adapters;
- transcript summarization by another LLM;
- persistent broker leadership election beyond the local lock/endpoint;
- user-facing role framework beyond small JSON defaults and ad hoc spawn.

## Risk

**High** — this is a public extension with concurrency, persistence, process lifecycle, workspace, and security-boundary behavior. The risk is controlled by keeping the coordinator pure/synchronous, testing it without LLM calls, and failing closed at transport and capability boundaries.
