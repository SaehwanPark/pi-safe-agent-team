# Agents and sessions

An agent is an actor record plus an optional long-lived Pi `AgentSession`. Its broker identity, parent, depth, capabilities, route, task, workspace, and lifecycle state are explicit.

## Lifecycle

```text
starting -> ready -> running -> ready
                    |         -> waiting -> ready
                    |         -> blocked -> ready
                    +-> completed | failed | cancelled
```

Terminal states are idempotent. The root can cancel a descendant subtree. Cancellation and crash recovery release runtime claims; they do not silently delete worktree artifacts.

## Recursion

A child may call `agent_spawn` only when `maySpawn` is granted. The broker enforces:

- maximum depth;
- maximum active children per parent;
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

The child bootstrap prompt includes identity and protocol guidance. It does not inherit an in-flight parent transcript leaf, arbitrary parent extension set, or global model selection.

## Busy and waiting behavior

Each managed child has one prompt tail. New parent/peer messages are queued durably. While a model turn is active, the host uses Pi steering/follow-up queues. While idle or waiting, it schedules a fresh prompt. Clarification requests explicitly terminate the current turn so a reply can wake a fresh prompt without a synchronous cycle.

## Failure behavior

- session creation/auth error: child is cancelled and the error is returned;
- model turn error: child becomes `failed`, task ownership is released/requeued, and the parent receives `agent_failed`;
- lost socket: the host reconnects with its credential and syncs the inbox;
- broker restart: old live actors are marked stale/failed, leases are released, and matching sessions can re-register;
- parent cancellation: descendants are cancelled recursively.

## Workspace modes

`shared` is the caller's project cwd. `worktree` requires a clean Git checkout, creates an isolated branch/worktree, and records the path/base ref/branch. v1 never guesses how to merge changes and never removes a dirty worktree automatically.
