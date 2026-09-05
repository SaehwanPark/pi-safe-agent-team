# pi-safe-agents-team

A local-first Pi extension for recursive, observable multi-agent work with deterministic coordination. It keeps semantic decisions in Pi sessions and puts identity, mailboxes, tasks, resources, leases, capabilities, and lifecycle transitions behind one authoritative broker.

## What v1 provides

- recursive child sessions with bounded depth/count/total limits;
- Pi SDK-backed long-lived workers (`createAgentSession`), not one-shot model calls;
- explicit provider/model/thinking routing with safe inheritance from the caller's selected model;
- durable at-least-once parent/peer mailboxes and request/reply without synchronous deadlock;
- atomic task claims and structured task results;
- hierarchical ownership, declared workspace paths, shared/mutable borrows, FIFO waiters, leases, transfer, and snapshots;
- capability checks for spawning, peer messaging, shell, repository writes, and resources;
- append-only transaction journal and reconnect recovery;
- shared workspaces or explicit clean Git worktrees;
- `/agents` status/tree/tasks/resources/messages/inbox inspection.

## Install as a Pi package

From a project directory:

```bash
pi install npm:pi-safe-agents-team
```

For local development:

```bash
npm install
npm run check
pi -e ./index.ts
```

The extension is declared through the `pi.extensions` field in `package.json`, so the package can also be loaded from a project package manifest.

## Quick start

1. Start Pi with the extension loaded.
2. Ask the root agent to use `agent_spawn` with a `taskDescription`.
3. The child starts in its own persistent Pi session and reports a bounded result to the parent.
4. Use `/agents` or `/agents tree` to inspect the fabric.

Example request to the root model:

```text
Spawn a read-only scout with role scout and taskDescription "Find the parser entry points and report file:line findings".
Do not give it write or shell capabilities. Ask it for clarification if the task is ambiguous.
```

The coordinator returns a child identity, model route source, task ID, and workspace. It does not wait for the child model turn.

## Core API example

The deterministic core has no model or network dependency:

```ts
import { Coordinator } from "pi-safe-agents-team";

const fabric = new Coordinator({ rootId: "demo" });
fabric.dispatch("root", "agent.register", {
  rootId: "demo",
  role: "root",
  route: { provider: "local", model: "qwen", thinking: "medium" },
  capabilities: { maySpawn: true, mayMessagePeers: true },
});
const task = fabric.dispatch("root", "task.create", { description: "inspect the API" }).value;
const child = fabric.dispatch("root", "agent.spawn", {
  route: { provider: "local", model: "qwen", thinking: "low" },
  taskId: task.id,
  capabilities: { mayMessagePeers: true },
}).value;
console.log(child.agent.id, child.token.length > 0);
```

Run the deterministic suite with:

```bash
npm test
npm run typecheck
```

## Safe collaboration rules

- `agent_send` is durable; busy receivers do not lose messages. A message is acknowledged only after the host accepts it into its queue, and duplicate notifications are idempotent.
- A clarification request returns `terminate: true`; the child becomes `waiting`, and a later `agent_reply` starts a fresh prompt. Pending clarification records survive broker restart; no parent turn is synchronously blocked.
- `agent_resource` is authoritative for ownership and borrows. A prompt saying “I own this file” never changes state; even the owner must hold a mutable borrow before a guarded file mutation.
- A task result is submitted explicitly with `agent_task(action=complete, result=...)` and is bounded by `maxTaskOutput`; a model turn ending never completes an assigned task.
- Managed child `read`/`grep`/`find`/`ls` tools are workspace-scoped; `edit`/`write` tools also resolve their target against a declared workspace-relative file/module resource and call the coordinator at the actual filesystem write boundary. Shared-workspace shell is a conservative read-only allowlist with workspace-relative arguments; worktree shell is explicitly trusted and isolated only by the Git worktree.
- Model inheritance means the caller's actual in-memory `provider/model` object. Missing or excluded explicit routes fail closed.
- Dirty worktrees are never silently deleted.

## Configuration

The default state directory is under Pi's agent directory:

```text
~/.pi/agent/safe-agents/<cwd-hash>/
  events.jsonl
  broker.lock
  broker.sock                 # POSIX; Windows uses a named pipe
  sessions/<agent-id>/...
  worktrees/<agent-id>/...
```

See [`docs/configuration.md`](docs/configuration.md) for limits, role routes, environment variables, and workspace policy.

## Documentation

- [`ARCHITECTURE.md`](ARCHITECTURE.md): components, data flow, invariants, risks, and deferrals.
- [`PROTOCOL.md`](PROTOCOL.md): local JSONL frames, identity, delivery, tasks, resources, and errors.
- [`ROADMAP.md`](ROADMAP.md): checked implementation baseline and remaining v1 work.
- [`docs/agents.md`](docs/agents.md): lifecycle, recursion, and session behavior.
- [`docs/messaging.md`](docs/messaging.md): mailboxes, request/reply, visibility, and busy delivery.
- [`docs/ownership.md`](docs/ownership.md): resource hierarchy, leases, borrows, and handoff.
- [`docs/models.md`](docs/models.md): exact provider/model routing and inheritance.
- [`docs/configuration.md`](docs/configuration.md): state paths, limits, roles, and workspaces.
- [`PRIOR_ART.md`](PRIOR_ART.md): Pi and agent-team lessons that shaped the design.

## Status

The deterministic coordinator, journal, local broker, model-routing adapter, guarded workspace mutation boundary, Pi extension surface, and adversarial hardening tests are implemented. The roadmap remains the source of truth for additional integration coverage; this is not a claim of production-distributed orchestration.

## License

MIT
