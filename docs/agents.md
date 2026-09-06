# Agents and sessions

An agent is an actor record plus an optional long-lived Pi `AgentSession`. Its broker identity, parent, depth, capabilities, route, task, workspace, and lifecycle state are explicit.

## Lifecycle

```text
starting -> ready -> running -> ready
                    |         -> waiting -> ready
                    |         -> blocked -> ready
                    +-> completed | failed | cancelled
```

Terminal states are idempotent and permanent. A broker restart creates one explicit reconnectable liveness window for previously live actors; a matching token can reattach that actor once, but a completed, cancelled, or non-recoverable failed actor cannot be revived. The root can cancel a descendant subtree. Cancellation and crash recovery release runtime claims; they do not silently delete worktree artifacts.

## Recursion

A child may call `agent_spawn` only when `maySpawn` is granted. The broker enforces:

- maximum depth;
- maximum child creations per parent;
- maximum active agents per fabric;
- maximum concurrent model turns;
- capability ceilings inherited from the parent.

The model chooses whether recursion is useful. The broker chooses whether another actor may be created. A denied spawn is a structured limit/capability error, not a retry invitation.

## Starting a child

The root/parent host:

1. resolves the child role/model route against its own `ModelRegistry`;
2. asks the coordinator for a child identity and reconnect credential;
3. creates shared or clean Git worktree workspace metadata;
4. configures that child record;
5. creates a Pi `AgentSession` with an exact model object, persistent session directory, context loader, and child-safe coordination tools;
6. registers the session identity and starts the first prompt without awaiting the child model turn.

The child bootstrap prompt includes identity and protocol guidance. It explicitly requires task completion through `agent_task(action=complete, result=...)` and a mutable resource borrow before `edit`/`write`. It does not inherit an in-flight parent transcript leaf, arbitrary parent extension set, or global model selection.

## Busy and waiting behavior

Each managed child has one prompt tail. New parent/peer messages are queued durably. While a model turn is active, the host uses Pi steering/follow-up queues; acknowledgement happens after queue acceptance, not after the model turn, and in-flight IDs prevent notification/inbox races from executing one message twice. While idle or waiting, it schedules a fresh prompt. Clarification requests explicitly terminate the current turn so a reply can wake a fresh prompt without a synchronous cycle.

## Shell and mutation safety

Managed `read`/`grep`/`find`/`ls` tools are scoped to the child workspace. Managed `edit`/`write` tools call `resource.check_write` immediately before their filesystem write. The target must be inside the child workspace and match a declared file/module path with a current mutable hold; logical ownership alone is insufficient. Shared-workspace `mayUseShell` is a conservative read-only allowlist with workspace-relative arguments. Worktree `mayUseShell` is an explicitly trusted shell escape limited by the Git worktree convention, not by resource locks.

The root session participates too: its ordinary Pi `edit`/`write` calls are vetoed before execution when a live foreign hold overlaps the workspace-relative target (`resource.check_write` with `hostGuard: true`). Undeclared paths stay writable for the root, targets outside the root workspace are not coordinated, and the root shell is not intercepted — it remains trusted, not sandboxed.

## Failure behavior

- session creation/auth error: child is cancelled and the error is returned;
- model turn error: child becomes `failed`, task ownership is released/requeued, and the parent receives `agent_failed`;
- lost socket: the host reconnects with its credential and syncs the inbox;
- broker restart: old live actors are marked failed/reconnectable, leases are released, pending clarification records survive, matching sessions can re-register once, and each waiting actor keeps its `maxTotalAgents` slot reserved until it reconnects, resolves, or is cancelled (so newcomers cannot evict it);
- parent cancellation: descendants are cancelled recursively.

## Workspace modes

`shared` is the caller's project cwd. `worktree` requires a clean Git checkout, creates an isolated branch/worktree, and records the path/base ref/branch. v1 never guesses how to merge changes and never removes a dirty worktree automatically.
