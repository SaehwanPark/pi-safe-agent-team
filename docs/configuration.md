# Configuration

The extension is intentionally usable with no project configuration. Defaults are conservative. The installed extension loads the first valid JSON configuration file below (highest precedence first); constructor options remain an explicit override for programmatic hosts and tests.

```text
PI_SAFE_AGENTS_CONFIG=/absolute/path/to/config.json
<workspace>/.pi/safe-agents.json
<workspace>/.safe-agents.json
<agentDir>/safe-agents/config.json
<agentDir>/safe-agents.json
```

The file may contain the fields directly or under a `safeAgents`, `piSafeAgents`, or `fabric` object. For example:

```json
{
  "safeAgents": {
    "modelRoutePolicies": {
      "omlx/qwen3": { "maxConcurrent": 1, "effectivePrefillBudget": 48000 }
    }
  }
}
```

Configuration errors are fail-closed: an explicitly requested path that is
missing, or any discovered file that is malformed JSON, prevents the runtime
from starting. This avoids silently relaxing safety and memory policies. The
loader returns the source and diagnostic errors for embedding hosts that want
to surface them directly.

## Canonical agent directory resolution

The runtime resolves the canonical Pi agent directory using `options.agentDir ?? getAgentDir()`.
Resolution precedence is:

1. `options.agentDir` (explicit programmatic/test override)
2. `PI_CODING_AGENT_DIR` environment variable (canonical Pi convention, read natively by `getAgentDir()`)
3. Default Pi directory (`~/.pi/agent`, canonical fallback in `getAgentDir()`)

This ensures that disposable test directories configured via `PI_CODING_AGENT_DIR` isolate both the root host and all managed child sessions consistently.

## State layout

By default:

```text
<agentDir>/safe-agents/<sha256(canonicalWorkspace + "\0" + rootPiSessionId)[0:24]>/
  events.jsonl       # broker transaction journal
  broker.lock        # local ownership lock
  broker.sock        # POSIX endpoint; named pipe on Windows
  root.token         # mode 0600 reconnect credential for the stable root identity
  sessions/<id>/     # child Pi session JSONL files
  worktrees/<id>/    # managed Git worktrees when selected
```

`FabricRuntime` options can override `cwd`, `stateDirectory`, `endpoint`, `agentDir`, `fabricId`, configuration, and whether this process starts the broker or joins an existing endpoint.

The default fabric identity is finalized when the root attaches, because Pi's session ID
is not available while the extension is being constructed. Reconnecting the same Pi
session reuses its fabric; two concurrent Pi sessions in the same workspace receive
different state directories, broker endpoints, root identities, and child namespaces.
Explicit `fabricId`, `stateDirectory`, or `endpoint` values are advanced overrides for
intentional sharing or embedding and should be used together when sharing is desired.

## Limits

Current defaults:

| Setting | Default |
| --- | ---: |
| `maxDepth` | 4 |
| `maxChildrenPerAgent` | 8 live direct children |
| `maxChildrenCreatedPerAgent` | unset (no lifetime ceiling) |
| `maxTotalAgents` | 32 active |
| `maxConcurrentAgents` | 8 running turns |
| `maxMailboxMessages` | 512 pending per recipient |
| `maxMessageBody` | 64 KiB |
| `maxTaskOutput` | 32 KiB |
| `leaseMs` | 30 minutes |
| `heartbeatMs` | 1 minute |
| `agentHeartbeatTimeoutMs` | 3 minutes | Time without an actor heartbeat before the broker marks it failed/reconnectable and releases runtime claims. |
| `reconnectGraceMs` | 10 minutes | How long a stale reconnectable actor keeps its `maxTotalAgents` slot before retirement; unfinished tasks return to `ready`. |
| `messageRetention` | 2048 recent records |
| `fenceMs` (per `resource.begin_write`) | 30s, clamped 1s-120s | Lifetime of a guarded-write fence. Not a config field: passed per call by the guarded host. The active fence map is ephemeral, while the matched resource carries a journaled restart quarantine through this expiry. |
| `caseInsensitivePaths` | auto | Fold policy keys so differently-cased spellings share one resource. Undefined = probe the broker volume at startup (always true on Windows). Set `false` to keep keys case-sensitive. |

Model-runtime coordination is configured independently from the global agent count:

```ts
modelRoutePolicies: {
  "omlx/qwen3": { maxConcurrent: 1, effectivePrefillBudget: 48_000, capacityGroup: "local-gpu-0" },
  "my-local-alias/qwen3": { maxConcurrent: 1, capacityGroup: "local-gpu-0" },
  "openai/gpt-5.6-sol": { maxConcurrent: 4 },
}
```

`modelRouteCapacity`/`modelRouteCapacities` are accepted as simple
`provider/model -> positive integer` capacity maps. Exact route entries take
precedence over provider and `*` entries. Local providers (`omlx`, `llama.cpp`,
LM Studio, Ollama, and `local`) default to one heavy operation when no capacity is
specified. `effectivePrefillBudget` is optional and is clamped to the model's
logical context window; it is the budget passed to embedded context management,
not a claim that the provider's advertised window changed.

`capacityGroup` is the physical backend identity, independent of the semantic
provider/model name. Routes that point at the same local GPU or inference
server should use one group so broker admission and the process-local arbiter
share one limit. When omitted, the route's provider/model key remains the
fallback identity and local-provider name heuristics still provide the
conservative one-turn default.

Limits fail closed. There is no automatic unbounded retry or fallback provider.

The broker periodically checkpoints `events.jsonl` (by transaction count or
file size) and checkpoints again during clean shutdown. Heartbeats without
leases update liveness in memory without creating a synchronous journal write;
lease renewals remain durable. Lifecycle writes (`agent.begin_turn`,
`agent.end_turn`, and `agent.finish_turn`) accept durable `operationId` values,
so a lost response can be retried without applying the transition twice.
`message.send` supports the same replay contract.

## Roles and capabilities

A role can choose a route and capability ceiling. Child requests are intersected with the parent's capabilities. Typical roles:

- `scout`: read/search, no shell/write/spawn;
- `reviewer`: read/search, peer messaging, no write;
- `worker`: read/write through declared resources, optional shell, no peer broadcast by default; shared-workspace shell is read-only and allowlisted, while worktree shell is explicitly trusted;
- `lead`: bounded spawn, peer messaging, resource transfer.

The authority fields are coordinator state. Role prompt text is guidance only.

## Workspace policy

Use `shared` for read-only investigations or when all work is intentionally serialized by resources. Use `worktree` for independent coding children. In either mode, managed `edit`/`write` requires a declared workspace-relative file/module resource and a current mutable borrow; ownership alone is not sufficient. Worktree creation fails if the base checkout is dirty or has no usable `HEAD`. Clean worktrees are reclaimed after a completed child shutdown or a later startup GC pass once the actor's recovery grace has expired and no live/reconnectable actor references the path; dirty or uncertain artifacts are retained for inspection and require user confirmation/force through a future cleanup command.

## Broker startup

Normally the first root extension instance starts the local broker; later instances join the locked fabric. The runtime uses a stable per-session root identity and stores its reconnect credential in `root.token` with mode `0600`. A broker restart permits one matching-token reattach for live actors and recovery-fences their live descendants; completed, failed, and cancelled semantic terminal actors remain terminal. If the broker maintenance timer was suspended past a liveness interval, its first resumed pass skips stale-agent reclamation so heartbeats can re-establish liveness. A stale lock can be removed only when its recorded PID is no longer alive. The endpoint is local-user scoped. The broker is one writer for `events.jsonl`.

## Diagnostics

Use `/agents`, `/agents tree`, `/agents tasks`, `/agents resources`, `/agents messages`, and `/agents inbox`. `/agents stop` performs a bounded graceful descendant drain; `/agents stop --budget` captures model-free handoff state for quota emergencies; `/agents stop --now` uses the shortest best-effort deadline. For tests and embedding, inspect structured `FabricError.code` values rather than matching human messages. Important categories include `CAPABILITY_DENIED`, `AGENT_LIMIT_REACHED`, `MAILBOX_FULL`, `RESOURCE_CONFLICT`, `MODEL_NOT_FOUND`, `WORKSPACE_FAILURE`, and `BROKER_UNAVAILABLE`.

## Isolated smoke modes

`npm run smoke:pi` runs the credential-free safe-agent load smoke. Use
`npm run smoke:pi:with-lcm` for an explicit joint extension-load check; it fails if the
companion checkout is missing. The opt-in model-backed managed-child check is:

```text
PI_SMOKE_MODEL=<provider/model> \
PI_SMOKE_SOURCE_AGENT_DIR=<agent-fixture-dir> \
npm run smoke:pi:integration
```

The integration mode copies only `auth.json` and `models.json` into a disposable
`PI_CODING_AGENT_DIR`, loads both extensions, spawns one child, and verifies the child
uses the embedded context provider and completes its task. `PI_SMOKE_AUTH_FILE` and
`PI_SMOKE_MODELS_FILE` can point to explicit fixture files when the source directory
layout differs.
