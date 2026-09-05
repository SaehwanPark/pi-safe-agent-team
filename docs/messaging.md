# Messaging

Messaging is actor-style and durable. A Pi session can be busy, idle, waiting for clarification, disconnected, or restarting without turning the mailbox into a lost-message channel.

## Delivery contract

Each message has a broker ID, sender-local sequence, sender, recipient, typed kind, bounded body, priority, optional request/reply correlation, and optional client dedupe key.

- delivery is **at least once** until acknowledgement;
- messages from one sender are returned FIFO;
- no global order is promised across senders;
- duplicate client sends with the same sender/key return the original message;
- a notification is only a wake-up hint; `message.inbox` is the recovery source of truth;
- acknowledgement means the host accepted the message into its queue, not that the model obeyed it.

Busy workers receive a notification while their current turn continues. The host queues a steer/follow-up or starts a later prompt, then acknowledges. If queueing fails, the broker message remains unacknowledged and reconnect recovery will retry it.

## Message kinds

Use `inform` or `progress` for findings, `result`/`task_result` for bounded outcomes, `handoff` for resource/task transfer, and `agent_failed` for lifecycle failure. Use `clarification`, `decision_request`, `escalation`, `resource_request`, or `request` only when a response is expected.

Messages are not authority. They cannot set `from`, grant capabilities, claim a task, transfer a resource, or change a lifecycle state.

## Deadlock-free clarification

A child should not call a tool that waits on the parent's current model turn. Instead:

```text
child -> agent_send(type=clarification, expectsReply=true)
     <- requestId + terminate=true
child turn ends as waiting
parent -> agent_reply(requestId, body)
child receives response as a fresh prompt and resumes
```

The coordinator never awaits the parent or child model. The request record stays pending until one response, cancellation, or terminal failure resolves it. A reply may arrive while the requester is disconnected; it remains in the mailbox.

## Visibility

Parent/child messages are always available to the relationship. Peer messages require `mayMessagePeers` plus a scoped relationship: siblings, common task ancestry, explicit peer grant, or root authorization. Discovery returns metadata only and never copies a transcript.

A root may use `/agents inbox` to inspect its pending messages. A worker can call `agent_inbox` and `agent_ack`; the child host also delivers broker notifications into the Pi session automatically.

## Cancellation and failures

Cancellation is idempotent. It does not delete unacknowledged messages or journal history. Pending requests get a visible failed/cancelled state, runtime holds are released, and parent notifications are compact. A failed child does not block its parent indefinitely.

## Practical guidance

- Keep bodies small and include stable file/task/resource IDs.
- Send one finding per message when independent delivery matters.
- Use `clientDedupeKey` for retryable reports.
- Ack only after the host has accepted the message, never merely after noticing a socket notification.
- Put semantic uncertainty in a request; put deterministic facts in task/resource operations.
