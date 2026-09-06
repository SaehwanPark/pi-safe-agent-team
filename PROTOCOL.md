# pi-safe-agents-team protocol

> Protocol version: `1` (local broker JSONL).

## Transport

The broker listens on a per-fabric Unix domain socket on POSIX or a per-fabric Windows named pipe. Each request and notification is one JSON object terminated by LF (`\n`). JSON strings may contain escaped newlines; clients must split on LF only.

The endpoint is local-user scoped. POSIX sockets are created with mode `0600`; Windows uses the named-pipe ACL supplied by the operating system. The broker state directory is not a public API and agents must not edit it.

## Frames

Client request:

```json
{
  "id": "request-uuid",
  "version": 1,
  "op": "message.send",
  "args": { "to": "agent-b", "type": "inform", "body": "..." }
}
```

Response:

```json
{
  "id": "request-uuid",
  "version": 1,
  "ok": true,
  "result": { "messageId": "msg-..." }
}
```

Failure:

```json
{
  "id": "request-uuid",
  "version": 1,
  "ok": false,
  "error": {
    "code": "RESOURCE_CONFLICT",
    "message": "mutable access overlaps an active shared borrow",
    "details": { "resourceId": "module:parser" }
  }
}
```

Broker notification:

```json
{
  "kind": "event",
  "version": 1,
  "event": "message.available",
  "data": { "message": { "id": "msg-...", "from": "agent-a", "to": "agent-b", "type": "inform", "body": "..." } }
}
```

The `from` field in a message is broker-created. Client operation arguments never override it.

## Handshake and identity

A client opens a transport and sends `hello` with its proposed actor ID and private join token. A new root may register once without a token. A coordinator-created child receives an unexposed token in its managed host closure. The broker binds the connection to the actor ID and uses that binding for every subsequent authorization check.

A reconnecting actor may re-register only with the matching token/session identity. After a broker restart, only a live actor marked `reconnectable` may use that credential once; semantic terminal actors cannot be resurrected by registration. Re-registering does not change parent, root, depth, or capabilities. The model cannot create or alter these fields through message text.

## Operations

The public tool/command layer maps to these operation families:

- `agent.register`, `agent.update`, `agent.configure_child`, `agent.begin_turn`, `agent.end_turn`, `agent.heartbeat`, `agent.cancel`, `agent.status`;
- `agent.spawn`;
- `message.send`, `message.reply`, `message.ack`, `message.inbox`, `message.list`;
- `discover.agents`;
- `task.create`, `task.claim`, `task.update`, `task.list`, `task.show`;
- `resource.define`, `resource.inspect`, `resource.snapshot`, `resource.borrow`, `resource.transfer`, `resource.release`, `resource.grant`, `resource.check_write`, `resource.list`;
- `fabric.status`.

The broker may add internal operations, but unknown operations fail closed.

## Idempotent durable writes

`agent.spawn` and `task.create` accept an optional `operationId` (a bounded, non-empty string, at most 128 characters, no NUL). It lets a client safely retry a write whose response was lost to an ambiguous transport failure, so a socket error after the broker already applied the mutation cannot silently produce a second child or task.

- The key is `(actor, operationId)`: one key addresses one logical request per actor, independent of the operation. Two actors may reuse the same `operationId` without collision.
- A replay of the same actor + `operationId` + arguments returns the original response with `replayed: true` and creates nothing new.
- Reusing an `operationId` for different arguments (or, for one actor, a different operation) raises `IDEMPOTENCY_CONFLICT`; it never returns the mismatched original response.
- `operationId` is rejected with `INVALID_ARGUMENT` on any other operation, which are already deduplicated by message dedupe keys or are reads/claims with their own atomicity.
- The record is journaled atomically with the transaction that applied it and is restored on replay and checkpointing, within a bounded per-coordinator window (oldest records evicted first). A legacy committed transaction with no record is not deduplicated.
- An ambiguous client failure (`BROKER_UNAVAILABLE`/`PERSISTENCE_FAILURE`) may be retried once under the same `operationId`; deterministic business errors are never retried.

## Message types and guarantees

Supported v1 types are:

```text
inform | clarification | decision_request | escalation | blocked | progress |
result | task_result | handoff | resource_request | resource_granted |
request | response | cancel | steer | agent_failed
```

Messages have stable broker IDs, a sender-local sequence number, a monotonic broker sequence for durable replay, creation timestamp, priority, optional request/reply IDs, and optional bounded metadata.

Guarantees:

- **at-least-once mailbox delivery**: a message remains available until the recipient acknowledges it;
- **deduplication**: a sender may supply a stable client dedupe key; repeated sends return the original message;
- **per-sender FIFO**: messages from one sender are returned in ascending `senderSequence`, even when timestamps and IDs would sort differently;
- **no global ordering**: messages from different senders may interleave;
- **request/reply correlation**: a request has one request ID and at most one accepted response;
- **busy safety**: notification delivery is separate from the durable inbox, so an active model turn cannot discard a message; the host tracks in-flight/accepted IDs so duplicate notifications do not execute a message twice;
- **bounded retention**: the broker keeps recent message metadata/body within configured limits and reports truncation/retention in status.

Acknowledgement means the Pi host accepted the message into its session queue. It does not mean the model read or followed it. A model response is not a broker acknowledgement.

## Clarification flow

A child calls `message.send` with a request type and `expectsReply: true`:

```text
child tool call
  -> broker creates request/message #17
  -> broker marks child waiting-capable and notifies parent
  -> tool returns requestId + terminate=true
  -> Pi finishes the current turn; host releases the child execution slot
  -> parent receives compact request #17
  -> parent calls message.reply(requestId, ...)
  -> broker creates response and resolves #17
  -> child host accepts the response as a fresh Pi prompt
  -> child resumes
```

No operation waits synchronously for the other session. If a requester or recipient explicitly terminates, unresolved requests become failed/cancelled and the status projection explains why. A broker restart changes transport liveness only: pending clarification records stay pending until a later response or explicit terminal resolution.

## Visibility

Parent/child messages are always allowed when the actor retains the required capability. Peer messages require the scoped `mayMessagePeers` capability plus one of:

- common immediate parent;
- shared task ancestry;
- root-authorized fabric visibility;

An explicit `peerIds` entry is a narrow exception for that exact recipient and may be used without the broad peer capability. Descendant `peerIds` entries are intersected with the parent's explicit list; an empty list means no explicit exceptions, never a wildcard.

`discover.agents(scope)` returns public metadata only: ID, role, task, route, status, and activity timestamps. It never returns a transcript.

## Tasks

Task claims are exclusive and atomic. A claim succeeds only if the task is ready and unowned (or already owned by the same actor). Dependencies must be completed. Completion stores a bounded structured result; child session output is not treated as an implicit task fact until the host submits it.

Cancellation releases active claims back to `pending` unless a caller explicitly marks the task failed. A task result is sent as a compact `task_result` message to the parent/creator. A model turn ending, assistant text, or session output never completes an assigned task; the worker must submit `task.update(action=complete, result=...)` explicitly. At turn end, durable task state maps to lifecycle (`completed`/`failed`/`cancelled`/`blocked`), otherwise the worker is merely `ready` or `waiting`.

## Resources

A resource can have an owner, an optional workspace-relative `path`, and active leases:

Path identity is enforced at the guarded filesystem boundary, not inside the broker: before a guarded write asks `resource.check_write`, the host resolves the target's real path (symlinks, junctions, and alternate spellings through the nearest existing ancestor; policy keys are case-folded on Windows). Declarations name real paths — a write that reaches a coordinated file through an alias is authorized only against the file's own declaration and holds. A target whose real identity escapes the workspace is denied outright for managed children and left uncoordinated for the root. Unresolvable targets (for example symlink loops) fail closed.

- `own`/`claim`: logical semantic owner; a same-owner re-claim is idempotent and does not bump the resource version;
- `borrow(shared)`: many readers if no overlapping mutable holder;
- `borrow(mutable)`: one writer, no overlapping shared/mutable holder;
- `transfer`: atomic owner change by an authorized owner/delegator;
- `release`: release one of the caller's active holds, selected by exactly one of `resourceId`, `leaseId`, or `all=true` (releasing everything is explicit, never a forgotten selector);
- `snapshot`: return `resourceId@version` for stable dependency tracking;
- `check_write`: authorize an actual file path only when the actor has a current mutable hold on a matching declared resource. With `hostGuard: true` (accepted only from the fabric root), the root instead participates in borrowing by exemption: the write is allowed when no overlapping resource carries a live foreign hold, and undeclared paths are writable without a prior declaration.

Resource hierarchy uses explicit parent links. Equality and ancestor/descendant overlap are conflict candidates. A conflict returns a structured busy/waiting result; it is never silently granted. A waiting mutable request is ordered FIFO and wakes after a release or lease reclamation.

A lease remains valid through long model turns only while the host heartbeats. The default lease is deliberately long; explicit heartbeats are the liveness signal. On expiry or terminal agent state, the coordinator releases the hold and records a recovery event.

## Lifecycle and cancellation

Agent state transitions are explicit:

```text
starting -> ready -> running -> ready
                    |         -> waiting -> ready
                    |         -> blocked -> ready
                    +-> completed | failed | cancelled
```

Terminal states are idempotent and permanently terminal. Only the explicit broker-recovery window is reconnectable: a broker restart marks live actors as reconnectable liveness failures, and a matching token may reattach them once. A cancelled/completed/non-recoverable failed actor cannot be revived by registration. Cancelling a parent cancels its descendants. Cancellation releases task/resource runtime claims and is safe to repeat.

## Persistence and recovery

Only the broker writes `events.jsonl`. Mutations are transaction-framed. Recovery applies committed transactions and ignores a partial trailing transaction. Broker restart marks previously live sessions as liveness-unknown/reconnectable, releases their active leases and requeues their non-terminal task claims, and permits matching session identities to re-register once. It does not fail pending clarification records. No model transcript is replicated in the broker journal.

A reconnectable actor keeps its `maxTotalAgents` slot reserved while its reconnect window is open: new registrations and spawns are refused rather than allowed to evict an expected reconnection, and the reservation is released when the actor reconnects, resolves its turn, or is cancelled. `maxConcurrentAgents` bounds concurrently running turns and is deliberately not reserved; a turn start that finds the fabric full is retryable and reports a structured limit error.

## Error codes

The public layer should preserve these stable categories where applicable:

```text
PROTOCOL_VERSION_UNSUPPORTED
INVALID_ARGUMENT
IDENTITY_CONFLICT
CAPABILITY_DENIED
AGENT_NOT_FOUND
AGENT_LIMIT_REACHED
MAILBOX_FULL
LIFECYCLE_CONFLICT
TASK_NOT_FOUND
TASK_BUSY
TASK_BLOCKED
TASK_NOT_OWNER
RESOURCE_NOT_FOUND
RESOURCE_CONFLICT
RESOURCE_NOT_OWNER
LEASE_EXPIRED
MESSAGE_NOT_FOUND
MESSAGE_NOT_VISIBLE
DUPLICATE_REQUEST
REQUEST_NOT_FOUND
REQUEST_ALREADY_RESOLVED
BROKER_UNAVAILABLE
PERSISTENCE_FAILURE
IDEMPOTENCY_CONFLICT
WORKSPACE_FAILURE
MODEL_NOT_FOUND
MODEL_ROUTE_INVALID
CHILD_SESSION_FAILURE
```

Error details are diagnostic metadata, not authority. Clients must not convert a failed operation into a guessed fallback operation.
