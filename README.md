# pi-safe-agents-team

[![CI](https://github.com/SaehwanPark/pi-safe-agent-team/actions/workflows/ci.yml/badge.svg)](https://github.com/SaehwanPark/pi-safe-agent-team/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue.svg)](https://saehwanpark.github.io/pi-safe-agent-team/)
[![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)](https://github.com/SaehwanPark/pi-safe-agent-team/releases/tag/v0.1.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Node](https://img.shields.io/badge/node-%3E%3D22.19.0-brightgreen.svg)](https://nodejs.org/)

**A local-first Pi extension for recursive, observable multi-agent coding with deterministic coordination.**

> 📖 **[Interactive Documentation & Visual Architecture Explorer](https://saehwanpark.github.io/pi-safe-agent-team/)**  
> Visit the live documentation site for interactive component walkthroughs, sequence diagrams, live tabbed state explorers, and prior-art deep dives.

---

## Why pi-safe-agents-team?

Prompt instructions like *"please coordinate with other agents"* or *"do not edit file X while agent B is working on it"* inevitably break down under real coding workloads. Without mechanical enforcement, multi-agent systems suffer from:

- **Silent File Clobbering**: Concurrent writes overwrite each other without warning.
- **Parent-Child Write Races**: The human or root agent inadvertently mutates files under an active child's feet.
- **Foreground Deadlocks**: Asking a parent for clarification blocks the parent's turn on the JavaScript call stack.
- **Duplicate Actions on Retries**: Network hiccups or timeouts trigger retries that spawn duplicate workers or duplicate tasks.

### The Core Principle

> **"Agents make semantic coding decisions. An out-of-band broker enforces mechanical coordination correctness."**

An LLM should never be asked to act as a mutex, a distributed lock manager, a message deduplicator, or an authority source.

---

## Mechanical Coordination Invariants

1. **Rust-Inspired Hierarchical Borrowing**: Files and directories are hierarchical resources. Multiple agents can hold concurrent shared read leases, but only one agent may hold an exclusive mutable borrow across an overlapping hierarchy. Time-bounded leases automatically prevent deadlocks.
2. **Write Fencing & Root Host Guard**: Before touching disk, an authorized writer establishes an in-flight write fence (`resource.begin_write` → write → `resource.end_write`) that keeps competing leases out even if a lease expires mid-mutation. The root user/parent session participates in borrowing without manual declarations: active child reservations veto conflicting root edits.
3. **Physical Path Identity & Case Folding**: Symlinks, directory junctions, and relative path aliases are resolved to canonical real filesystem paths before coordinator checks. The broker automatically probes volume case-folding on startup (macOS APFS / Windows NTFS) to prevent cross-casing write collisions.
4. **Durable Idempotency & Crash Recovery**: `agent.spawn` and `task.create` enforce unique per-actor `operationId` records committed to an append-only transaction journal (`events.jsonl`). Ambiguous network/timeout retries replay original responses safely without duplicating agents or tasks.
5. **Non-Blocking Clarifications**: When a child asks for parent or user input via `agent_send(type="clarification")`, it yields its turn (`terminate: true`) and transitions to `waiting`. The parent is never blocked on the JavaScript event loop.
6. **Hardened Sandboxing**: Shared-workspace shell access is strictly read-only (`git`, `rg`, `grep`, `cat`, etc.) and rejects indirect file-list expansion flags (such as `file -f` or `wc --files0-from`).

---

## Comparison at a Glance

| Capability | Standard Pi Subagents | `nicobailon/pi-subagents` | `tmustier/pi-agent-teams` | `pi-safe-agents-team` (v0.1) |
| :--- | :--- | :--- | :--- | :--- |
| **Concurrency Enforcement** | None (advisory) | None (advisory) | Git branch/worktree only | **Authoritative Borrow Checker + Leases** |
| **Filesystem Write Guard** | None | None | None | **Write boundary hook with Write Fences** |
| **Root/Parent Write Veto** | None | None | None | **Root Host Guard (`hostGuard: true`)** |
| **Symlink / Alias Identity** | None | None | None | **Canonical realpath resolution** |
| **Parent Clarification** | Unsupported | Blocks JS event loop | Message polling | **Non-blocking turn yield (`waiting`)** |
| **Retry Idempotency** | None | None | None | **Journaled `operationId` deduplication** |
| **Broker State Durability** | In-memory | In-memory | Shared files / tmux | **Append-only transaction journal** |
| **Cross-Platform IPC** | N/A | In-process | POSIX / tmux | **Windows Named Pipes + POSIX Sockets** |

*For in-depth analysis, see the [interactive matrix](https://saehwanpark.github.io/pi-safe-agent-team/#comparison) or [`PRIOR_ART.md`](PRIOR_ART.md).*

---

## Installation

> [!IMPORTANT]
> **npm Publishing Deferred**: Due to account authentication setup, npm publishing is temporarily deferred. Please install directly from GitHub.

### Option A: Install via Pi Package Manager (Recommended)

```bash
pi install git:github.com/SaehwanPark/pi-safe-agent-team
```

### Option B: Clone into User Extensions Directory

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

### 1. Spawning a Worker
Prompt the root model:
```text
Spawn a researcher agent to analyze src/core/coordinator.ts and list all invariants.
Do not grant write permissions or shell access.
```
The root model invokes `agent_spawn`, returning the child's identity, model route, and task ID. The child runs asynchronously in its own Pi `AgentSession`.

### 2. Inspecting the Fabric
Use slash commands to inspect runtime state:
- `/agents` — Summary of active agents, running counts, and health.
- `/agents tree` — Visual tree of parent-child hierarchy and roles.
- `/agents tasks` — Deterministic task board showing owners and statuses.
- `/agents resources` — Active ownership, shared/mutable holds, and waiters.
- `/agents inbox` — Durable mailbox inspection for unacknowledged messages.

### 3. Coordinated File Modification
When an agent needs to edit a file:
1. Calls `agent_resource(action="borrow", resourceId="file:src/parser.ts", mode="mutable")`.
2. The coordinator verifies no overlapping shared readers or mutable writers exist.
3. The agent calls `edit` or `write`. The guarded hook sets an active write fence (`resource.begin_write`), writes to disk, and clears the fence (`resource.end_write`).
4. If the user or root agent attempts to edit `src/parser.ts` concurrently, the root host guard blocks the edit:
   ```text
   safe-agents root write guard: A conflicting runtime hold prevents writing src/parser.ts
   ```

---

## Operational Boundaries

- **Write Fences Across Broker Crashes**: Write fences (`resource.begin_write`) are in-memory coordinator records. They strictly protect writes during normal broker operation (including lease lapses and root writes). A recovery quarantine mechanism is scheduled post-v0.1 to bridge crash durability if the broker restarts mid-write.
- **Shell Isolation Scope**: Shared-workspace shell is mechanically verified and read-only. Worktree shell (`workspace="worktree"`) is an explicitly trusted developer escape hatch isolated by Git worktrees.
- **Local-First IPC**: v0.1 is designed for local multi-agent coordination via Unix domain sockets and Windows named pipes. Distributed network clustering is planned for future milestones.

---

## Documentation & Deep Dives

- 🌐 **[Interactive GitHub Pages Documentation](https://saehwanpark.github.io/pi-safe-agent-team/)** — Visual guides, diagrams, and live walkthroughs
- [`ARCHITECTURE.md`](ARCHITECTURE.md) — System design, state invariants, and failure recovery
- [`PROTOCOL.md`](PROTOCOL.md) — Wire format, JSONL frames, coordinator operations, and error codes
- [`docs/ownership.md`](docs/ownership.md) — Hierarchical resources, borrow rules, leases, and write fencing
- [`docs/messaging.md`](docs/messaging.md) — Mailboxes, delivery guarantees, and non-blocking clarification
- [`docs/agents.md`](docs/agents.md) — Agent lifecycles, recursion constraints, and session boundaries
- [`PRIOR_ART.md`](PRIOR_ART.md) — Technical breakdown of preceding agent implementations

---

## License

MIT © [Saehwan Park](https://github.com/SaehwanPark)
