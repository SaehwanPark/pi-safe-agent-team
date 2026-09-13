## Round 5 audit

I finished R5 against current `main` at **`ddce86ba76c20ec61ac006942c7d504c2f70831c`**. R4 was implemented comprehensively, and the current CI matrix is green on Ubuntu, macOS, Windows, plus the new joint **Pi + local-context-manager** smoke test.

For R5 I changed the failure model. Instead of mainly asking “is this state machine locally correct?”, I traced what happens if the process or connection dies at specific boundaries: after broker mutation but before response delivery, after queue acceptance but before transcript persistence, with the broker surviving the host, after a full broker+host restart, and after recursive agents partially finish.

The result is mixed in a good way: **R4's fixes hold up, but R5 found three genuinely important remaining failure classes.** The most serious one is no longer context/KV management; it is distributed exactly-once lifecycle semantics.

| ID       | Severity    | Finding                                                                                                              | Assessment                            |
| -------- | ----------- | -------------------------------------------------------------------------------------------------------------------- | ------------------------------------- |
| **R5-1** | **P1**      | Root `steer`/`followUp` messages can still be ACKed before durable Pi persistence                                    | **Confirmed crash-loss window**       |
| **R5-2** | **P1**      | Recursive agent can complete its task while descendants remain live, then wedge permanently at `end_turn`            | **Confirmed invariant mismatch**      |
| **R5-3** | **P1**      | Lifecycle RPCs are not retry-safe across “committed, response lost” failures                                         | **Confirmed distributed-systems gap** |
| **R5-4** | **P1/P2**   | A surviving broker has no heartbeat-based stale-actor reclamation                                                    | **Confirmed liveness gap**            |
| **R5-5** | **P1/P2**   | After full host restart, recovered managed children have nobody to reconnect them and may reserve slots indefinitely | **Confirmed recovery gap**            |
| **R5-6** | **P2 high** | Control-message “coalescing” preserves the oldest stale notice rather than the latest state                          | **Confirmed**                         |
| **R5-7** | **P2**      | `agent_send` is vulnerable to duplicate sends after an ambiguous broker response                                     | **Confirmed**                         |
| **R5-8** | **P2**      | Reopening a dependency does not invalidate/re-block already-unlocked dependent tasks                                 | **Confirmed task-DAG gap**            |
| **R5-9** | **P2**      | Child durable-message detection relies on textual `includes(message.id)`                                             | **Confirmed robustness weakness**     |

### R5-1 — Root delivery still has the child-message crash bug in another form

R4 correctly fixed child durability and the root's special `nextTurn` path. But ordinary root messages still have a hole.

Current root delivery does:

```text
api.sendMessage(...)
mark accepted

if nextTurn:
    retain broker message
else:
    ACK broker immediately
```

So only `nextTurn` receives delayed ACK treatment.

The problem is Pi's semantics while the root is running. For `steer` and `followUp`, `sendCustomMessage()` merely inserts the custom message into an **in-memory Agent queue**. For a `triggerTurn:false` custom message received while streaming, Pi similarly puts it into `_pendingCustomMessages`; it isn't appended to the session until the current turn reaches its flush point.

Therefore:

```text
child -> urgent message
        |
        v
root currently streaming
        |
        v
Pi steer/followUp RAM queue
        |
        v
safe-agent ACKs broker
        |
        X root process crashes
```

The broker copy is gone and Pi never persisted the message.

That means R4 fixed this:

```text
degraded/compacting -> nextTurn -> durable ACK boundary
```

but not this:

```text
healthy + currently busy -> steer/followUp -> premature ACK
```

It also affects low-signal `progress`/`inform` messages: with `triggerTurn:false` while streaming, Pi holds them in `_pendingCustomMessages` until `turn_end`.

**Fix:** make *all* root delivery modes use the same accepted → persisted → acknowledged state machine. `deliverAs` should determine model scheduling, not durability semantics. The structured root custom-message entry already contains `details.id`, so persistence detection can be exact rather than heuristic.

I would treat this as R5's first release blocker.

---

## R5-2 — Task completion and recursive-agent completion disagree

This is a particularly nasty recursive-agent bug.

`completeTask()` immediately changes the task to `completed` and emits the parent `task_result`. It does not check whether the task owner's descendants are still alive.

But later, when that worker ends its model turn, `endTurn()` derives `completed` from the task and then explicitly requires:

```text
assertNoLiveDescendants(agent.id)
```

before allowing the worker itself to become completed.

So a recursive worker can legitimately do:

```text
parent worker
    |
    +-- spawn grandchild
    |
    +-- finish its own work
    |
    +-- agent_task complete
```

and the coordinator accepts it:

```text
task = COMPLETED
parent receives task_result
```

Then the worker's model turn settles:

```text
agent.end_turn
    |
task says completed
    |
assertNoLiveDescendants()
    |
    X LIFECYCLE_CONFLICT
```

It gets worse. `ManagedChild.enqueuePrompt()` catches the error and tries a fallback:

```text
agent.end_turn(status=failed)
```

But the durable task is already completed, so `statusAfterTask()` again resolves the worker to `completed`, and the same live-descendant assertion fires again.

The broker actor can consequently remain `running`, consuming model-route/global capacity, while its parent has already been told the task completed.

This is exactly the kind of failure R5 was intended to find.

**Fix:** enforce the subtree invariant at `task.complete`, before committing the semantic fact or emitting `task_result`. A task owned by an agent with live descendants should either reject completion with a precise error or transition to an explicit `finishing/draining` state until descendants resolve.

I strongly prefer rejecting `complete` and telling the agent to collect/cancel its descendants first. It keeps the state model smaller.

The regression test should be:

```text
A owns task
A spawns B
A task.complete -> rejected
no task_result emitted
B completes/cancels
A task.complete -> succeeds
A end_turn -> completed
```

---

## R5-3 — The broker is durable, but lifecycle RPC responses are not exactly-once

This is the most fundamental R5 finding.

The broker's ordering is good:

```text
coordinator.dispatch()
journal.append() + fsync
maybe checkpoint
construct response
send response
```

So there is necessarily a valid failure point here:

```text
mutation committed durably
           |
           X connection dies
           |
client never sees response
```

The client already recognizes such ambiguity. `requestIdempotent()` specifically exists for it and retries with the same durable operation ID.

But the coordinator currently permits durable `operationId` replay **only for `agent.spawn` and `task.create`**.

Lifecycle transitions such as:

```text
agent.begin_turn
agent.end_turn
agent.finish_turn
```

use ordinary requests.

Consider `agent.finish_turn(blocked)`:

```text
broker atomically commits:
    task = blocked
    agent = blocked
    parent notice
        |
journal fsync succeeds
        |
response lost
```

The child believes `finish_turn` failed. Its outer prompt error handler then tries:

```text
agent.end_turn(status=failed)
```

But because the task is already blocked, that request can return an agent still in `blocked`. The current catch path treats *any successful `agent.end_turn` RPC* as `terminalized=true` and calls `child.stop()`.

Now the coordinator has a perfectly recoverable blocked worker, but its actual local `ManagedChild` has been destroyed.

A similar commit/response loss around a normal `agent.end_turn(ready)` can turn an otherwise successful child into a failed worker.

### Recommended design

Give each logical model turn a durable **`turnId`**.

Conceptually:

```text
agent.begin_turn(turnId)
agent.finish_turn(turnId, outcome, operationId)
agent.end_turn(turnId, outcome, operationId)
```

The coordinator should retain the active/last-finished turn ID and replay the original result when the same operation is retried.

More broadly, the operation-id machinery should not be limited to creation. Any mutation whose committed result cannot safely be reconstructed from a subsequent request should be retryable exactly once.

And the host's ambiguous-error path should first **reconcile broker state**, not blindly issue `end_turn(failed)`.

This is the most important architectural work I would take from R5.

---

## R5-4 — Heartbeats detect nothing if the broker survives

Broker restart recovery is quite good now: active agents become failed/reconnectable and runtime claims are released.

But broker **maintenance** only reclaims expired resource leases/fences. It does not inspect stale agent heartbeats.

This matters for the supported external-broker topology (`startBroker:false`) or any topology where the broker outlives an agent host.

Suppose:

```text
child = RUNNING
broker remains healthy
child host process dies
```

No more heartbeat arrives.

Yet nothing transitions the broker actor out of `running`.

The actor can therefore hold:

```text
global maxConcurrentAgents slot
capacityGroup slot
running status
```

indefinitely.

Its resource leases eventually expire, but model-route admission does not.

Even reconnect is awkward. Registering an existing non-reconnectable `running` agent leaves its status `running`; only `starting` or broker-recovery `failed+reconnectable` is reset to ready.  A recovered host can therefore immediately hit `"already running"` on its next `agent.begin_turn`.

**Fix:** add an actor-liveness timeout, probably something like 3–5 heartbeat intervals. A stale active actor should enter a recoverable/fenced state and release runtime capacity. Socket close can start the grace timer earlier, but shouldn't immediately imply death.

Importantly, heartbeat updates with no leases already update `lastActivity` in memory without journaling every minute, so the machinery needed for this is mostly present.

---

## R5-5 — A full host crash leaves durable children that no new host owns

This is related but distinct.

After a broker restart, every active child becomes:

```text
failed
reconnectable = true
```

and its slot is intentionally reserved.

That is correct when the old child host still exists and is about to reconnect.

But managed children live inside the Pi host process. After a **full Pi process crash**, they no longer exist.

On session restart, `FabricRuntime` restores/attaches the root. It does **not** reconstruct `ManagedChild` instances from durable broker records. Managed children enter the runtime map only through the normal spawn path.

So in a resumed fabric:

```text
broker:
  child-A = failed/reconnectable
  child-B = failed/reconnectable
  child-C = failed/reconnectable

new FabricRuntime:
  children Map = {}
```

Those actors can reserve `maxTotalAgents` capacity indefinitely because `reservedAgentCount()` deliberately counts reconnectable actors.

And `/agents stop` primarily derives its cancellation targets from the runtime's local `children` map, so these broker-only descendants don't naturally participate in ordinary descendant shutdown either.

This becomes a classic soak failure: after enough crash/resume cycles, apparently invisible reconnect reservations can consume the fabric's agent budget.

I see two valid solutions. The more sophisticated solution is **child rehydration** from durable session/workspace metadata through a root-authorized adoption/resume protocol. The simpler and probably safer V1 solution is: on fresh root attachment after process-level recovery, give reconnectable descendants a short grace period; if no local host claims them, retire/cancel them, preserve their sessions/worktrees as recovery artifacts, and leave unfinished tasks available for reassignment.

I would implement the simpler policy first.

---

## R5-6 — Control-message “coalescing” currently preserves stale data

R4 added a reserved urgent mailbox lane and control-message coalescing, which is a good direction.

But the dedupe semantics don't actually mean “keep the latest notice.”

When the same `clientDedupeKey` already exists and is unacknowledged, `recordMessage()` simply returns the **existing original message** unchanged.

`sendInternalMessage()` generates keys such as:

```text
control:resource_granted:<entity>
control:task_result:<taskId>
```

For `resource_granted`, the entity-selection order prefers `resourceId` before `requestId`.

Consider:

```text
grant #1:
  resource R
  lease L1
  request Q1
  message remains unACKed

worker releases R

grant #2:
  same resource R
  lease L2
  request Q2
```

The second control notification can dedupe to the old first message. The recipient can therefore wake up seeing stale `leaseId=L1/requestId=Q1` even though durable resource state now refers to L2.

The same issue exists conceptually for a task that is explicitly reopened and completed again: an old unACKed `task_result` can survive while the durable result changes.

**Fix:** distinguish event notifications from state notifications.

For true state-coalescing messages, update/replace the existing unacknowledged message atomically with the latest body/metadata while preserving its broker ordering slot. For event messages such as a particular resource grant, key on `requestId` or grant generation, not just resource ID.

---

## R5-7 — `agent_send` still has at-least-once semantics under response loss

This is the messaging counterpart to R5-3.

The tool receives a stable `_toolCallId` but ignores it:

```ts
async execute(_toolCallId, params) {
    return client.request("message.send", ...)
}
```

and `clientDedupeKey` is optional model-provided input.

If the broker durably records a clarification and its response is lost, the tool reports an error. A retry can create another clarification/request.

The immediate low-cost fix is to default:

```text
clientDedupeKey = "tool:" + toolCallId
```

unless the caller supplied a semantic key.

Longer term this should fall under the generalized operation-id framework from R5-3.

---

## R5-8 — Task dependencies don't survive `reopen` coherently

Task readiness is evaluated when a task is created/claimed and when dependency completion calls `refreshTaskReadiness()`.

But reopening a completed prerequisite simply changes that prerequisite back to active/ready. It does not revisit dependent tasks.

So:

```text
A completed
    |
    +--> unlocks B
         B = active

A reopened
```

leaves:

```text
A = active
B = active
```

even though B's declared dependency is no longer completed.

B can even complete while A is being redone.

This matters more now because `reopen` is an explicit recovery mechanism, not a theoretical task-board feature.

There are two coherent semantics: either completion is monotonic and completed tasks cannot be reopened once they have unlocked dependents, or reopening is revision-like and must invalidate/cascade dependent readiness. The current implementation is halfway between both.

For V1 I would make completion monotonic whenever downstream tasks exist. If a redo is required, create a new task/generation rather than rewriting a dependency fact that other tasks already consumed.

---

## R5-9 — Child durable ACK detection should become structured

One smaller durability weakness remains in the child path.

To determine whether a broker message reached durable Pi history, `hasPersistedChildMessage()` scans user-message text and tests:

```text
text.includes(messageId)
```

The live event observer uses the same strategy.

That works normally because the canonical fabric wrapper includes the ID. But message IDs are not secret, and another parent/peer message could legitimately quote one. A persisted unrelated message containing ID X can therefore look like durable acceptance of broker message X.

This is much less likely than R5-1–R5-5, but exactly-once recovery should not depend on substring coincidence.

Use a structured durable receipt, e.g. a custom message field or sidecar entry:

```text
fabricMessageId: <exact ID>
```

and test exact equality.

---

## Several things I specifically tried to break and now consider healthy

The R4 blocked-resource fix is correct: both task blocking and agent blocking release holds, waiters, and fences while preserving task ownership, and waiting actors are subsequently drained fairly.

Large offline inbox recovery is now cursor-based rather than capped at 100 messages for both root and children.

Journal growth is materially addressed: the broker checkpoints by transaction count or byte size, checkpoints at clean shutdown, and keeps the append log authoritative if checkpointing fails.

Capacity aliases now genuinely share a physical capacity identity: both broker admission and the process-local FIFO arbiter use the configured `capacityGroup`, and the coordinator takes the strictest applicable limit across aliases.

Abort-like provider errors are now narrowly classified as aborts rather than ordinary failures.

I also investigated a possible steered-child-message loss during ordinary provider failure. Pi's steering queue itself survives a run failure; `finishRun()` does not clear it, so I **do not count that as an R5 defect**.

And the LCM integration is now exercised in CI rather than merely unit-tested in isolation.

## Recommended Round 5 implementation sequence

1. **Unify durable root delivery.** No broker ACK until the corresponding root custom-message entry is demonstrably in Pi session history, regardless of `nextTurn`, `steer`, `followUp`, or trigger policy.
2. **Move the descendant invariant into task completion.** Never publish `task_result` for an owner that still has live descendants.
3. **Introduce retry-safe lifecycle transactions.** Give logical turns stable IDs and make begin/end/finish replayable after an ambiguous response; reconcile instead of blindly terminalizing on transport errors.
4. **Add actual actor failure detection and recovery expiry.** Heartbeat TTL for surviving brokers, plus bounded reconnect reservations and explicit handling of broker-only descendants after host restart.
5. Then fix state-notification coalescing, automatic message-send dedupe, dependency-reopen semantics, and structured child-delivery receipts.

The most valuable new fault-injection tests would literally intercept the broker immediately **after `journal.append()` but before response send** for `begin_turn`, `end_turn`, `finish_turn`, and `message.send`; kill the Pi host after root `sendCustomMessage()` queue acceptance but before `turn_end`; crash a recursive worker after completing its own task with a live grandchild; keep a broker alive while a worker disappears for several heartbeat intervals; and resume the same session after a whole-host crash containing several children.

## Round 5 verdict

The architecture has crossed an important threshold. I would no longer characterize `pi-safe-agent-team` as needing another redesign.

R1–R4 progressively removed the obvious architectural failure modes. **R5's remaining problems are classic distributed-runtime problems:** acknowledgement boundaries, orphan detection, replay identity, and semantic state transitions that need to agree across layers.

The two clearest release blockers are **R5-1 root busy-message durability** and **R5-2 recursive task completion**. I would address **R5-3 exactly-once lifecycle RPCs in the same release**, because that is the issue most likely to produce extremely confusing “it failed even though the broker says it succeeded” behavior under real crashes or flaky local infrastructure.

Once those are fixed, I think a Round 6 should be almost entirely executable chaos testing rather than another broad source audit: a deterministic fault-injection harness that crashes or disconnects at every persistence boundary and asserts convergence afterward.
