# Configuration

The extension is intentionally usable with no project configuration. Defaults are conservative and can be supplied to `FabricRuntime` or a future role/config loader.

## State layout

By default:

```text
PI_AGENT_DIR/safe-agents/<sha256(cwd)[0:24]>/
  events.jsonl       # broker transaction journal
  broker.lock        # local ownership lock
  broker.sock        # POSIX endpoint; named pipe on Windows
  root.token         # mode 0600 reconnect credential for the stable root identity
  sessions/<id>/     # child Pi session JSONL files
  worktrees/<id>/    # managed Git worktrees when selected
```

`PI_AGENT_DIR` may point at the same Pi agent directory used by the host. `FabricRuntime` options can override `cwd`, `stateDirectory`, `endpoint`, `agentDir`, `fabricId`, and whether this process starts the broker or joins an existing endpoint.

## Limits

Current defaults:

| Setting | Default |
| --- | ---: |
| `maxDepth` | 4 |
| `maxChildrenPerAgent` | 8 |
| `maxTotalAgents` | 32 active |
| `maxConcurrentAgents` | 8 running turns |
| `maxMailboxMessages` | 512 pending per recipient |
| `maxMessageBody` | 64 KiB |
| `maxTaskOutput` | 32 KiB |
| `leaseMs` | 30 minutes |
| `heartbeatMs` | 1 minute |
| `messageRetention` | 2048 recent records |
| `caseInsensitivePaths` | auto | Fold policy keys so differently-cased spellings share one resource. Undefined = probe the broker volume at startup (always true on Windows). Set `false` to keep keys case-sensitive. |

Limits fail closed. There is no automatic unbounded retry or fallback provider.

## Roles and capabilities

A role can choose a route and capability ceiling. Child requests are intersected with the parent's capabilities. Typical roles:

- `scout`: read/search, no shell/write/spawn;
- `reviewer`: read/search, peer messaging, no write;
- `worker`: read/write through declared resources, optional shell, no peer broadcast by default; shared-workspace shell is read-only and allowlisted, while worktree shell is explicitly trusted;
- `lead`: bounded spawn, peer messaging, resource transfer.

The authority fields are coordinator state. Role prompt text is guidance only.

## Workspace policy

Use `shared` for read-only investigations or when all work is intentionally serialized by resources. Use `worktree` for independent coding children. In either mode, managed `edit`/`write` requires a declared workspace-relative file/module resource and a current mutable borrow; ownership alone is not sufficient. Worktree creation fails if the base checkout is dirty or has no usable `HEAD`. Cleanup is explicit; a dirty artifact requires user confirmation/force through a future cleanup command.

## Broker startup

Normally the first root extension instance starts the local broker; later instances join the locked fabric. The runtime uses a stable per-fabric root identity and stores its reconnect credential in `root.token` with mode `0600`. A broker restart permits one matching-token reattach for live actors; completed, failed, and cancelled semantic terminal actors remain terminal. A stale lock can be removed only when its recorded PID is no longer alive. The endpoint is local-user scoped. The broker is one writer for `events.jsonl`.

## Diagnostics

Use `/agents`, `/agents tree`, `/agents tasks`, `/agents resources`, `/agents messages`, and `/agents inbox`. For tests and embedding, inspect structured `FabricError.code` values rather than matching human messages. Important categories include `CAPABILITY_DENIED`, `AGENT_LIMIT_REACHED`, `MAILBOX_FULL`, `RESOURCE_CONFLICT`, `MODEL_NOT_FOUND`, `WORKSPACE_FAILURE`, and `BROKER_UNAVAILABLE`.
