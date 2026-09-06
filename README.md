# pi-safe-agents-team

[![CI](https://github.com/SaehwanPark/pi-safe-agent-team/actions/workflows/ci.yml/badge.svg)](https://github.com/SaehwanPark/pi-safe-agent-team/actions/workflows/ci.yml)
[![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)](https://github.com/SaehwanPark/pi-safe-agent-team/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Node](https://img.shields.io/badge/node-%3E%3D22.19.0-brightgreen.svg)](https://nodejs.org/)

**A local-first Pi extension for recursive, observable multi-agent coding with deterministic coordination.**

Agents reason about collaboration; an out-of-band broker enforces coordination correctness.

---

## The Problem: Why Multi-Agent Coding Breaks Down

Most agent frameworks and subagent extensions treat coordination as a prompt-engineering problem. Subagents are given prompt instructions ("please coordinate with other agents", "do not touch file X while agent B is working on it") and left to share the same filesystem or chatroom.

In practice, this architecture collapses under real coding workloads:

1. **Silent File Clobbering & Race Conditions**: Two agents editing the same codebase simultaneously overwrite each other's edits. Neither git nor the LLM detects the collision until code breaks or changes disappear.
2. **The Parent-Child Write Race**: While a child agent works on a subsystem, the user or parent agent continues making edits. Without mechanical enforcement, the parent inadvertently mutates files under the child's feet.
3. **Synchronous Foreground Deadlocks**: When a child asks its parent for clarification, typical implementations block the parent's turn on the JavaScript call stack. If the parent needs input or spawns another agent, execution deadlocks.
4. **Context Window Flooding**: Unscoped peer chatrooms broadcast every status message to every participant, diluting context windows with conversational noise.
5. **Ghost Duplication on Ambiguous Failures**: When a transport hiccup or timeout occurs during agent or task creation, naive retries spawn duplicate workers or duplicate tasks.
6. **Model Configuration Leaks**: When child agents spawn without an explicit model, fallback heuristics often read unvetted global configurations or cause runaway recursion costs.

---

## The Key Insight: Mechanical Coordination Invariants

> **"Agents make semantic decisions. A deterministic broker enforces coordination correctness."**

An LLM should never be asked to act as a mutex, a distributed lock manager, a message deduplicator, or an authority source.

`pi-safe-agents-team` separates semantic reasoning from mechanical safety:

```text
                  Semantic Agents
               (Pi AgentSession SDK)
              ┌──────────┴──────────┐
              │                     │
         Root Session          Child Worker
              │                     │
              └──────────┬──────────┘
                         │
             Local IPC (Socket / Pipe)
                         │
                         ▼
               Deterministic Broker
           (Append-only Journal + Replay)
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
    Task Board       Mailboxes       Coordinator
  (Atomic Claims)  (Durable FIFO)  (State Machine)
                                         │
                         ┌───────────────┴───────────────┐
                         ▼                               ▼
               Borrow Checker (Rust-style)         Write Fences
            - Shared Readers (many)             - Short-lived lock
            - Mutable Writers (exclusive)       - Excludes leases
            - Time-bounded Leases               - Intercepts Root
                         │                               │
                         └───────────────┬───────────────┘
                                         ▼
                             Guarded Filesystem Boundary
                          - Realpath (Symlinks/Junctions)
                          - Volume Case-Folding Probe
                          - Fail-Closed on Broker Outage
```

1. **Rust-Inspired Ownership & Borrowing**: Files and directories are hierarchical resources. Many agents can hold shared read leases simultaneously, but only one agent may hold an exclusive mutable borrow across an overlapping hierarchy.
2. **In-Flight Write Fencing**: Before touching disk, an authorized writer establishes a write fence (`resource.begin_write`). If its lease expires mid-mutation, the fence continues to exclude competing writers until the disk write completes (`resource.end_write`).
3. **The Root Host Guard**: The user/root Pi session participates in borrowing without needing tedious manual declarations. Root `edit`/`write` calls are intercepted before disk mutation: undeclared paths remain writable, but any declared path currently leased or fenced by a child vetoes the root write.
4. **Real Filesystem Identity**: Symlinks, junctions, and path aliases are canonicalized through the nearest existing ancestor before coordinator evaluation. An agent cannot sidestep another agent's hold through an alias.
5. **Durable Idempotency & Crash Recovery**: `agent.spawn` and `task.create` require per-actor `operationId` tracking. Transactions are journaled in an append-only transaction log. Ambiguous network retries replay original results without duplicate side-effects.

---

## How It Compares to Other Subagent Extensions

| Capability | Standard Pi Subagent Example | `nicobailon/pi-subagents` | `tmustier/pi-agent-teams` | `pi-safe-agents-team` (v0.1) |
| :--- | :--- | :--- | :--- | :--- |
| **Worker Engine** | `createAgentSession()` | `createAgentSession()` | Background processes | `createAgentSession()` |
| **Concurrency Enforcement** | None (advisory) | None (advisory) | Git branch/worktree only | **Authoritative Borrow Checker + Leases** |
| **Filesystem Mutation Guard** | None | None | None | **Write boundary hook with Write Fences** |
| **Root/Parent Write Interception** | None | None | None | **Root Host Guard (`hostGuard: true`)** |
| **Symlink / Alias Disambiguation** | None | None | None | **Canonical realpath resolution** |
| **Parent Clarification** | Not supported | Blocks JS event loop | Message poll | **Non-blocking turn termination (`waiting`)** |
| **Messaging Architecture** | Unidirectional report | Async supervisor queue | Chat / direct messages | **Durable per-actor FIFO mailboxes** |
| **Duplicate / Retry Safety** | None | None | None | **Journaled `operationId` idempotency** |
| **Broker State Durability** | In-memory | In-memory | Shared files / tmux | **Append-only transaction journal** |
| **Cross-Platform IPC** | N/A | In-process | POSIX / tmux | **Windows Named Pipes + POSIX Sockets** |

*Note on research integrity: comparisons reflect public codebases and documented issue trackers (see [`PRIOR_ART.md`](PRIOR_ART.md)).*

---

## Installation

> [!IMPORTANT]
> **npm Registry Publishing is Currently Deferred**: Due to npm publishing credential setup, direct installation from npm is temporarily deferred.
> **Please install directly from GitHub.**

### Option A: Install via Pi Package Manager (Recommended)

From your project directory:

```bash
pi install git:github.com/SaehwanPark/pi-safe-agent-team
```

### Option B: Clone into User Extensions Directory

Clone directly into your personal Pi extensions folder:

```bash
git clone https://github.com/SaehwanPark/pi-safe-agent-team ~/.pi/agent/extensions/pi-safe-agents-team
```

### Option C: Local Development

```bash
git clone https://github.com/SaehwanPark/pi-safe-agent-team.git
cd pi-safe-agent-team
npm install
npm run check
pi -e ./index.ts
```

---

## Quick Start

### 1. Launching a Managed Worker

Start Pi with the extension loaded, and prompt the root model:

```text
Spawn a researcher agent to analyze src/core/coordinator.ts and list all invariants.
Do not grant write permissions or shell access.
```

The root model calls `agent_spawn`:
```json
{
  "role": "scout",
  "taskDescription": "Analyze src/core/coordinator.ts and list invariants",
  "mayWriteRepo": false,
  "mayUseShell": false
}
```

The coordinator immediately returns the child's identity, model route, and task ID. The child runs asynchronously in its own Pi `AgentSession`.

### 2. Inspecting the Fabric

Use slash commands to inspect the state machine in real time:

- `/agents` — Summary of active agents, running counts, and health.
- `/agents tree` — Visual tree of parent-child hierarchy, roles, and status.
- `/agents tasks` — Deterministic task board showing owners, status, and outputs.
- `/agents resources` — Active ownership, shared/mutable holds, and waiters.
- `/agents inbox` — Durable mailbox inspection for unacknowledged messages.

### 3. Coordinated File Modification

When an agent needs to edit a file:

1. Agent calls `agent_resource(action="borrow", resourceId="file:src/parser.ts", mode="mutable")`.
2. The coordinator verifies no overlapping shared readers or mutable writers exist.
3. The agent calls `edit` or `write`. The guarded write hook calls `resource.begin_write`, establishes an active write fence, performs the disk write, and calls `resource.end_write`.
4. If the user or root agent attempts to edit `src/parser.ts` during this window, the root host guard blocks the write with a clear veto message:
   `safe-agents root write guard: A conflicting runtime hold prevents writing src/parser.ts`

---

## Safe Collaboration Rules

- **Durable Mailboxes**: Messages sent via `agent_send` are persisted. Receivers acknowledge only after Pi accepts the message into its prompt queue. Messages sent while an agent is busy are queued, not lost.
- **Deadlock-Free Clarifications**: When a child asks for user/parent guidance via `agent_send(type="clarification")`, it returns `terminate: true` and transitions to `waiting`. The parent turn is never blocked. Later, `agent_reply` wakes the child with a clean prompt turn.
- **Authoritative Ownership**: Prompts saying "I am modifying X" have zero authority. Only coordinator-granted mutable holds authorize disk writes.
- **Bounded Shell Access**: Shared-workspace shell access is restricted to a conservative read-only allowlist (`git`, `rg`, `grep`, `cat`, etc.) with strict argument path containment. Dangerous options that read indirect files (such as `file -f` or `wc --files0-from`) are rejected.
- **Fail-Closed Broker Guard**: If the broker process becomes unreachable during a coordinated write, the root write guard fails closed to protect in-flight child writes from being clobbered.

---

## Operational Boundaries & Known Caveats

We believe in documenting operational boundaries honestly:

1. **Write Fences Across Broker Crashes**: Write fences (`resource.begin_write`) are in-memory coordinator records. They fully protect writes during normal broker operation (including lease lapses and root writes). However, if the broker process itself crashes mid-write and restarts while the worker's disk write is still executing, the fence is not crash-durable. *(A recovery quarantine mechanism is scheduled for post-v0.1 to bridge restart survivability).*
2. **Worktree Shell Isolation**: Shared-workspace shell is mechanically verified and read-only. Worktree shell (`workspace="worktree"`) is an explicitly trusted developer escape hatch isolated by Git worktrees, not by coordinator resource locks.
3. **Local-First Architecture**: v0.1 is strictly designed for local multi-agent coordination via Unix domain sockets and Windows named pipes. Distributed network clustering is deferred to future v1.0 milestones.

---

## Configuration & Paths

State files are isolated per project workspace:

```text
~/.pi/agent/safe-agents/<cwd-hash>/
  ├── events.jsonl         # Append-only transaction journal
  ├── broker.lock          # Process single-instance lockfile
  ├── broker.sock          # Unix domain socket (POSIX)
  │   └── \\.\pipe\...     # Named pipe (Windows)
  ├── sessions/            # Persistent child session states
  └── worktrees/           # Isolated Git worktrees
```

See [`docs/configuration.md`](docs/configuration.md) for limits, role overrides, environment variables, and policy tuning.

---

## Documentation

- [`ARCHITECTURE.md`](ARCHITECTURE.md): Component diagrams, state invariants, failure recovery, and architectural decisions.
- [`PROTOCOL.md`](PROTOCOL.md): Wire format, JSONL frames, coordinator operations, and error codes.
- [`ROADMAP.md`](ROADMAP.md): Verification baseline, v0.1 release checklist, and v1.0 goals.
- [`docs/agents.md`](docs/agents.md): Agent lifecycles, recursion constraints, and session boundaries.
- [`docs/ownership.md`](docs/ownership.md): Hierarchical resources, borrow rules, leases, and write fencing.
- [`docs/messaging.md`](docs/messaging.md): Mailboxes, delivery guarantees, and non-blocking clarification.
- [`docs/models.md`](docs/models.md): Model resolution, routing precedence, and inheritance.
- [`PRIOR_ART.md`](PRIOR_ART.md): Detailed architectural analysis of preceding agent implementations.

---

## License

MIT © Saehwan Park
