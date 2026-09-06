# Ownership, resources, and leases

Resource state is controlled by the broker. Agents may propose a handoff in prose, but only a successful coordinator operation changes authority.

## Resource IDs and hierarchy

A resource is an opaque ID such as `module:parser`, `file:parser.ts`, or `symbol:Parser.parse`. A definition may name one explicit parent and an optional workspace-relative `path`. File paths match exactly; module/directory paths match the path and descendants. The parent relationship is part of state, not inferred from filesystem spelling.

The identity a guarded write is checked against is the target's *real* filesystem path: the boundary resolves symlinks, junctions, and alternate spellings before consulting the coordinator (policy keys are case-folded on Windows). Declare the real path of a file or module; writing through a directory alias is then authorized against the real declaration and its holds, and an alias spelling of its own no longer satisfies the declaration rule.

Two resources overlap when they are equal or one is an ancestor of the other; declared file/module paths also establish overlap when hierarchy links are omitted. That makes a mutable borrow on `module:parser` conflict with a shared borrow on `file:parser.ts`, even if the two agents named different IDs.

## Ownership versus borrowing

- **Owner**: logical authority to transfer/grant the resource. Ownership is not a runtime mutex; even the owner must acquire a mutable borrow before a guarded filesystem write.
- **Shared borrow**: read/comment/test access. Multiple agents may hold it concurrently.
- **Mutable borrow**: write access. Only one overlapping mutable holder may exist, and no other agent may hold an overlapping shared borrow.
- **Version**: increments when mutable content authority is released or transferred; a same-owner re-claim is idempotent and does not bump it. Use `resource_snapshot` and retain `resourceId@version` in task results.

A parent grant applies to descendants. A child may inspect a resource only with a read grant, ownership, or a current hold.

## Typical flow

```text
root: resource.define(module:parser, path=src)
root: resource.define(file:parser.ts, path=src/parser.ts, parent=module:parser)
root: resource.grant(module:parser, child, [read, write])
child: resource.borrow(file:parser.ts, shared)
child: resource.release(leaseId)
child: resource.borrow(module:parser, mutable, wait=true)
child: edit/write(path=src/parser.ts)  # host checks the mutable hold at write time
```

If a mutable request conflicts, the broker returns `waiting` plus a request ID. It does not pretend that a write succeeded. Waiters are FIFO by enqueue time and receive a durable `resource_granted` message when a release or lease expiry makes the request grantable.

## Transfer and handoff

The owner, or an ancestor/root with `mayTransferOwnership`, may call `resource.transfer`. Transfer is one serialized transition. It fails if another agent holds an overlapping lease; the current holder must release first. The target receives write grant automatically and should still acquire a mutable borrow before editing.

A safe handoff includes:

1. release mutable/shared holds;
2. submit a task result with the current resource snapshot token;
3. transfer or grant the resource;
4. send a bounded handoff message with the resource ID and token;
5. target re-checks `resource.snapshot` before work.

## Leases and crash recovery

Every borrow has `acquiredAt`, `lastHeartbeat`, and `expiresAt`. Hosts heartbeat while a session is alive. A model can spend minutes thinking without holding a JavaScript stack lock; liveness comes from heartbeats, not from an awaited request.

On expiry, broker restart, agent cancellation, or terminal failure:

- active runtime holds are released;
- mutable release increments the resource version;
- waiting requests are reconsidered;
- dirty workspace artifacts remain for inspection.

Lease reclamation is deterministic and idempotent. It is not a merge or content-conflict detector.

## Write guard

The fabric root participates in borrowing too. The extension intercepts the root session's `edit`/`write` tool calls before execution and submits each target's resolved real workspace path (links included) to `resource.check_write` with `hostGuard: true`. The root is exempt from the declaration/mutable-hold requirement — it may keep working on undeclared files — but a live foreign hold on an overlapping declared resource blocks the root write with a visible veto, so the root cannot race a child's mutable (or shared) borrow. Targets outside the root workspace are not resource-coordinated, and while the broker is unreachable no actor can hold authorization, so the guard fails open then and only then. The root's ordinary shell is not intercepted and remains documented as trusted rather than sandboxed.

The child-safe `read`/`grep`/`find`/`ls` tools are scoped to the managed workspace. The child-safe `edit`/`write` path is exposed only when the capability allows repository writes. The managed host passes each target path to `resource.check_write` at the final filesystem write operation; the path must match a declared file resource exactly or a declared module/directory resource, and the actor must hold mutable access across the hierarchy. Ownership alone is denied. Shell access is separately gated by `mayUseShell`: shared-workspace shell is a conservative read-only allowlist whose executable arguments must stay workspace-relative (no absolute or parent paths), while worktree shell is an explicitly trusted isolated-workspace escape hatch and is not resource-enforced.
