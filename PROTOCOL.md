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

A reconnecting actor may re-register only with the matching token/session identity. Re-registering does not change parent, root, depth, or capabilities. The model cannot create or alter these fields through message text.

## Operations

The public tool/command layer maps to these operation families:

- `agent.register`, `agent.update`, `agent.configure_child`, `agent.begin_turn`, `agent.end_turn`, `agent.heartbeat`, `agent.cancel`, `agent.status`;
- `agent.spawn`;
- `message.send`, `message.reply`, `message.ack`, `message.inbox`, `message.list`;
- `discover.agents`;
- `task.create`, `task.claim`, `task.update`, `task.list`, `task.show`;
- `resource.define`, `resource.inspect`, `resource.snapshot`, `resource.borrow`, `resource.transfer`, `resource.release`, `resource.grant`, `resource.list`;
- `fabric.status`, `fabric.compact`.

The broker may add internal operations, but unknown operations fail closed.

## Message types and guarantees

Supported v1 types are:

```text
inform | clarification | decision_request | escalation | blocked | progress |
result | task_result | handoff | resource_request | resource_granted |
request | response | cancel | steer | agent_failed
```

Messages have stable broker IDs, a sender-local sequence number, creation timestamp, priority, optional request/reply IDs, and optional bounded metadata.

Guarantees:

- **at-least-once mailbox delivery**: a message remains available until the recipient acknowledges it;
- **deduplication**: a sender may supply a stable client dedupe key; repeated sends return the original message;
- **per-sender FIFO**: messages from one sender are returned in sequence order;
- **no global ordering**: messages from different senders may interleave;
- **request/reply correlation**: a request has one request ID and at most one accepted response;
- **busy safety**: notification delivery is separate from the durable inbox, so an active model turn cannot discard a message;
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

No operation waits synchronously for the other session. If a requester or recipient terminates, unresolved requests become failed/stale and the status projection explains why.

## Visibility

Parent/child messages are always allowed when the actor retains the required capability. Peer messages require `mayMessagePeers` plus one of:

- common immediate parent;
- shared task ancestry;
- explicit peer grant;
- root-authorized fabric visibility.

`discover.agents(scope)` returns public metadata only: ID, role, task, route, status, and activity timestamps. It never returns a transcript.

## Tasks

Task claims are exclusive and atomic. A claim succeeds only if the task is ready and unowned (or already owned by the same actor). Dependencies must be completed. Completion stores a bounded structured result; child session output is not treated as an implicit task fact until the host submits it.

Cancellation releases active claims back to `pending` unless a caller explicitly marks the task failed. A task result is sent as a compact `task_result` message to the parent/creator.

## Resources

A resource can have an owner and active leases:

- `own`/`claim`: logical semantic owner;
- `borrow(shared)`: many readers if no overlapping mutable holder;
- `borrow(mutable)`: one writer, no overlapping shared/mutable holder;
- `transfer`: atomic owner change by an authorized owner/delegator;
- `release`: release a hold or logical owner;
- `snapshot`: return `resourceId@version` for stable dependency tracking.

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

Terminal states are idempotent. Cancelling a parent cancels its descendants. Cancellation releases task/resource runtime claims and is safe to repeat.

## Persistence and recovery

Only the broker writes `events.jsonl`. Mutations are transaction-framed. Recovery applies committed transactions and ignores a partial trailing transaction. Broker restart marks previously live sessions as liveness-unknown, releases their active leases, and permits matching session identities to re-register. No model transcript is replicated in the broker journal.

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
WORKSPACE_FAILURE
MODEL_NOT_FOUND
MODEL_ROUTE_INVALID
CHILD_SESSION_FAILURE
```

Error details are diagnostic metadata, not authority. Clients must not convert a failed operation into a guessed fallback operation.
