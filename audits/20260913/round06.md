## Round 6 — chaos/convergence audit

I audited the post-R5 implementation merged in PR #37, code baseline **`52d254498f72f5e13b09728247ea29c8e9906b42`**. The later commits on `main` are documentation/README changes, so this remains the relevant code baseline. PR #37 explicitly implemented the full R5 set: durable message receipts, lifecycle replay IDs, descendant checks, stale-actor reclamation, reconnect expiry, and control-message coalescing.

The normal validation picture is strong: the merge commit passes Ubuntu, macOS, Windows, and the Pi + local-context-manager smoke job.

One caveat: I attempted to independently check out and execute the repository in the execution sandbox for additional chaos tests, but outbound GitHub DNS is unavailable there. So this R6 pass is a **deterministic fault-injection/convergence audit of the actual current source plus its existing CI/tests**, rather than me claiming I ran a separate local chaos harness.

### Executive result

R5 fixed what it intended to fix. In particular, I no longer see the old premature ACK, recursive-completion wedge, basic ambiguous lifecycle replay, stale-slot leak, or substring-receipt problems in their original forms.

But R6 found an important new cluster around the **reconnectable state**. The current model treats a reconnectable actor as terminal for some invariants while simultaneously allowing it to become live again later. That creates several resurrection/orphan cases.

| ID        | Severity                 | Finding                                                                                                                 | Status                      |
| --------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------- | --------------------------- |
| **R6-1**  | **P1 / release blocker** | Reconnectable descendants are ignored by subtree and child-capacity invariants, then may resurrect                      | Confirmed                   |
| **R6-2**  | **P1 / release blocker** | Staling/retiring a recursive parent can leave active descendants orphaned                                               | Confirmed                   |
| **R6-3**  | **P1 high**              | Laptop sleep/event-loop suspension can falsely stale live managed children and strand them                              | Confirmed design race       |
| **R6-4**  | **P1 high**              | Fabric quiescence can report `sessionReplacementSafe=true` while reconnectable work and unowned ready tasks still exist | Confirmed                   |
| **R6-5**  | **P1/P2 high**           | In-place `message_updated` coalescing can ACK newest broker state after Pi persisted the old payload                    | Confirmed end-to-end race   |
| **R6-6**  | **P2 high**              | Broker notices to reconnectable parents are silently dropped                                                            | Confirmed                   |
| **R6-7**  | **P2**                   | Ambiguous lifecycle reconciliation still does not fully synchronize local child state from durable status               | Hardening gap               |
| **R6-8**  | **P2**                   | Write fences remain non-durable across external-broker restart                                                          | Known crash window persists |
| **R6-9**  | **P2 soak**              | UnACKed messages for permanently terminal actors can become unprunable                                                  | Confirmed                   |
| **R6-10** | **P2 soak**              | Crash-retired children can leave clean worktrees/session artifacts without an owner to GC them                          | Confirmed lifecycle gap     |
| **R6-11** | **P3**                   | `persistedRootMessageIds` grows for the whole root session                                                              | Confirmed small leak        |

The first four are the ones I would address before moving on.

---

# R6-1 — `reconnectable` is simultaneously dead and alive

This is the central R6 finding.

R5 introduced this recovery sequence:

```text
live child
    ↓ heartbeat expiry
failed + reconnectable=true
    ↓ within grace period
register()
    ↓
ready
```

That mechanism itself is reasonable. The problem is how the intermediate state participates in invariants.

`failed` is a terminal status. `assertNoLiveDescendants()` therefore ignores a failed reconnectable descendant:

```ts
const live = [...this.agents.values()].filter(
  candidate => !isTerminal(candidate.status) &&
               this.isAncestorAgent(agentId, candidate.id)
);
```

Likewise, `liveChildrenCount()` ignores the reconnectable child because its status is terminal:

```ts
.filter(agent =>
  agent.parentId === parentId &&
  !isTerminal(agent.status)
)
```

Yet `reservedAgentCount()` correctly treats `reconnectable === true` as still occupying a fabric slot.

And registration explicitly resurrects such an actor:

```ts
if (next.reconnectable) {
  next.status = "ready";
  next.reconnectable = false;
}
```

Critically, the reconnect path does **not** revalidate that the parent is still active or that the parent's child-capacity invariant still holds.

That gives two deterministic bad schedules.

### Resurrection beneath a completed parent

```text
P running
└── C running

C heartbeat expires
└── C = failed/reconnectable

P sees no "live" descendants
P completes task
P → completed

C reconnects within grace
C → ready

completed P
└── ready C       ← impossible tree
```

The original no-orphan invariant is broken.

### Child-limit violation after replacement

With `maxChildrenPerAgent = 1`:

```text
P
└── C

C → failed/reconnectable

P liveChildrenCount = 0
P spawns D

P
├── C [failed/reconnectable]
└── D [live]

C reconnects
```

Now P has two live children despite a limit of one.

### Fix

Introduce an explicit semantic concept such as:

```ts
isReservedLive(agent) =
  ACTIVE_STATUSES.has(agent.status) ||
  agent.reconnectable === true
```

and use it consistently for:

* descendant-completion checks;
* `maxChildrenPerAgent`;
* subtree shutdown;
* quiescence;
* parent terminalization.

Then reconnect itself must re-check:

```text
parent exists
parent is active/recoverable
parent is not draining/terminal
depth remains valid
child-capacity reservation remains valid
```

The simplest rule is: **a reconnectable child counts as live for topology invariants until either it reconnects or its grace expires.**

This should be R6's first release blocker.

---

# R6-2 — a stale recursive parent can orphan a healthy child

There is a second, independent problem in stale-agent reclamation.

When an actor misses its heartbeat, maintenance directly changes it to:

```text
failed
reconnectable = true
```

and releases its runtime claims.

It does **not** cascade the transition to descendants the way normal failed/cancelled lifecycle termination does.

That means this is possible:

```text
P
└── C
```

P's broker connection or heartbeat path fails, but C's remains healthy.

Maintenance produces:

```text
P [failed/reconnectable]
└── C [ready/running]
```

The no-orphan invariant is already violated at that point.

The grace-expiry implementation makes the same assumption. `retireReconnectableSubtree()` recursively visits only children that are themselves `reconnectable === true`:

```ts
if (
  child.parentId === agent.id &&
  child.reconnectable === true
) {
  this.retireReconnectableSubtree(child, events);
}
```

So when P's grace eventually expires:

```text
P → cancelled
C → still running
```

This is not merely theoretical in the recursive architecture: every `ManagedChild` has its own broker client and heartbeat, so transport failures can be actor-specific.

### Fix

Recovery of a parent has to be a **subtree transition**, not an individual actor transition.

When P becomes stale, either:

1. mark the entire live descendant subtree recovery-fenced/reconnectable, or
2. keep descendants active but explicitly attach them to a recovery supervisor/root, or
3. cancel descendants before P becomes irreversibly terminal.

For V1 I strongly favor option 1:

```text
stale ancestor
    ↓
recovery-fence entire subtree
    ↓
release runtime/model/write claims
    ↓
permit credentialed reconnect in topology order
```

Grace expiry can then retire the complete subtree atomically.

---

# R6-3 — the new heartbeat mechanism is hazardous on developer laptops

This matters unusually much for the intended Pi/local-LLM environment.

Current defaults are:

* heartbeat: 60 s
* stale timeout: 180 s
* reconnect grace: 600 s.

Broker maintenance uses wall-clock age:

```ts
now - agent.lastActivity >= heartbeatTimeout
```

to decide that an actor has died.

Meanwhile root and managed-child heartbeats are ordinary Node timers, and heartbeat failures are deliberately swallowed.

Now consider a MacBook/workstation sleeping for 10 minutes, or a severe event-loop suspension:

```text
t0:
broker maintenance timer armed
child heartbeat timer armed

machine sleeps > 3 minutes

resume:
overdue timers become runnable
```

The broker's maintenance interval is normally created **before** child heartbeat intervals. It can therefore run first after resume and see:

```text
Date.now() - lastActivity > 180 s
```

even though the child process never died.

It marks the child failed/reconnectable.

Then the still-live child's next ordinary heartbeat calls `agent.heartbeat`, but `requireActor()` rejects the now-terminal `failed` actor. That error is swallowed.

There is no socket close, so the child's reconnect callback isn't necessarily invoked.

The root has a partial escape hatch because future root operations call `ensureRoot()`/re-register. Managed children do not have an analogous periodic self-reconciliation path.

The result can be:

```text
model/session is alive locally
broker thinks child is failed/reconnectable
heartbeat silently fails forever
10 min later broker retires child
```

### Better liveness model

Don't infer process death solely from elapsed wall time.

At minimum:

* track broker connection state/connection generation;
* consider whether the broker's own maintenance timer experienced comparable suspension;
* after a large timer gap, give actors a fresh heartbeat opportunity before declaring them stale;
* if heartbeat gets a reconnectable/lifecycle error, trigger `agent.register` reconciliation rather than swallowing it.

A very useful rule would be:

```text
if broker maintenance itself was paused longer than the heartbeat timeout:
    do not stale actors on the first maintenance tick after resume
```

This deserves a release-blocking test because local development machines routinely sleep.

---

# R6-4 — quiescence can now lie to LCM

This one directly affects the original “don't make context management worse” goal.

The fabric snapshot counts unresolved child tasks only when they still have an owner:

```ts
const unresolvedChildTasks = status.tasks.filter(
  t =>
    t.owner &&
    t.owner !== rootAgentId &&
    ["pending", "ready", "active", "waiting", "blocked"].includes(t.status)
).length;
```

It also counts children only in `starting`, `running`, or `draining`.

But stale recovery deliberately does:

```text
child -> failed/reconnectable
task.owner -> undefined
task.status -> ready
```

`releaseAgentRuntime(..., "broker-recovery")` does exactly that.

So immediately after a stale-child recovery transition:

```text
reconnectable child = ignored
ready unfinished task with no owner = ignored
mutable holds = released
running children = 0
```

The snapshot may conclude:

```text
quiescent = true
sessionReplacementSafe = true
```

while the fabric still contains:

* a reconnect reservation;
* unfinished durable work;
* an actor allowed to return.

That is precisely a time when LCM should **not** treat session replacement as obviously safe.

### Fix

Quiescence should include at least:

```text
all nonterminal tasks, regardless of owner
all reconnectable actors
all pending recovery reservations
```

I would expose these separately:

```text
recoveringAgents
unownedUnresolvedTasks
```

and add explicit reasons:

```text
recovering_agents
unowned_unresolved_tasks
```

Also worth correcting while there: `pendingRootRequests` currently considers requests **to** the root. A root request waiting on a child is also a semantic dependency and should generally prevent session replacement.

This is another release blocker because it crosses directly into LCM/session-compaction safety.

---

# R6-5 — control-message coalescing is correct in the broker but racy at the host

R5 fixed stale coalesced notifications by replacing the unacknowledged broker message **in place** and emitting `message_updated`.

That is good at the broker level.

But both host delivery state machines are keyed only by `message.id`.

For a child:

```ts
if (state === "delivering")
  return this.deliveryTail;

if (state === "accepted")
  return this.acknowledgePersistedChildMessage(message);
```

A `message_updated` event with the same ID does not replace the already queued/persisting payload.

The root does the same conceptually: an accepted message causes an ACK attempt rather than re-delivery of the updated body.

So this race exists:

```text
broker state:
X = control message v1

host receives X/v1
host queues v1 into Pi
       |
       | before durable ACK
       v
broker coalesces:
X = control message v2
emits message_updated(X/v2)

host sees X already delivering/accepted
ignores payload change

Pi persists X/v1

host ACKs X
broker ACK applies to current X/v2
```

Final state:

```text
broker believes latest state v2 was accepted
model/session actually received v1
```

That breaks the exact property R5's coalescing change was intended to establish.

The new coordinator test verifies that the **broker inbox** contains the latest state, but it doesn't exercise this host interleaving.

### Fix

A mutable/coalesced message needs a revision.

For example:

```text
messageId = X
revision = 1
...
revision = 2
```

The durable receipt and ACK become:

```text
(X, revision)
```

and the broker rejects/stales an ACK for revision 1 when its current state is revision 2.

Alternatively, don't mutate a message once host delivery has begun; create a fresh coalesced state notification.

I prefer explicit revisions because the semantics become testable.

---

# R6-6 — reconnectable parents cannot receive important recovery notices

`sendInternalMessage()` currently does:

```ts
if (isTerminal(to.status)) return;
```

A reconnectable actor is `failed`, hence terminal.

Therefore, while a recursive parent P is within its recovery grace:

```text
P [failed/reconnectable]
└── C [still functioning]
```

C may complete or another descendant may fail, but broker-generated notices to P such as `task_result` / `agent_failed` / resource wakeups can simply disappear.

The durable task state may still be recoverable later, but the wakeup semantics are lost.

That compounds R6-2: a parent reconnects and may have no inbox indication that its subtree changed while it was away.

### Fix

The suppression condition should distinguish permanent terminality from recoverability:

```ts
if (isTerminal(to.status) && to.reconnectable !== true)
  return;
```

A reconnectable mailbox is exactly where durable control notices should accumulate.

---

# R6-7 — lifecycle replay is much better, but local reconciliation should be centralized

R5 correctly made begin/end/finish replay-safe and introduced process-unique logical turn IDs. The core coordinator replay implementation is sound, including deliberately not caching `begin_turn: started=false`. The existing tests verify basic lifecycle replay and journal restoration.

The remaining weakness is the exception-reconciliation path in `ManagedChild`.

After an ambiguous exception it queries durable `agent.status`, but only acts if the status is:

```text
starting | running | waiting
```

and needs a synthetic failure end-turn.

If status already says:

```text
blocked
completed
failed
cancelled
```

the code does not synchronize `this.record`, rebuild the blocked gate, or dispose a terminal child there.

Other event/reconnect paths may eventually repair this, but the error boundary itself should converge deterministically.

A helper like:

```text
reconcileDurableAgentState(agent)
```

should be the single path for:

* reconnect;
* ambiguous lifecycle errors;
* recovery heartbeat;
* relevant agent-updated events.

This is P2 because normal transport closure already activates stronger reconnect reconciliation, but fault injection should still cover it.

---

# R6-8 — crash-durable write fencing remains unfinished

This is the oldest meaningful residual issue from the early rounds.

Child writes do:

```text
resource.begin_write
    ↓
ephemeral fence
    ↓
filesystem write
    ↓
resource.end_write
```

The fence itself is intentionally not journaled.

With the default embedded broker, broker death normally takes the host down as well, so the exposure is limited.

But in an external/surviving-agent topology:

```text
writer obtains fence
writer starts slow FS mutation

broker crashes/restarts
recovery forgets fence + releases hold

other actor acquires resource

old writer still finishes mutation
```

Two writers can overlap across the restart boundary.

I would not redesign normal fencing. Instead add a short **restart quarantine**:

```text
on broker recovery:
resources that had mutable runtime activity
remain write-excluded for old fence TTL
or until relevant actor recovery is resolved
```

That may be simpler than persisting every tiny fence transaction.

---

# R6-9 — dead mailboxes can defeat message retention

Message pruning currently considers only messages with `acknowledgedAt`.

Once an actor becomes permanently cancelled/failed:

* it can no longer call inbox/ACK through the ordinary actor API;
* old unACKed messages to it remain unacknowledged forever;
* therefore they are never pruning candidates.

Across repeated spawn/crash/retire cycles this can make the broker snapshot/journal grow despite `messageRetention`.

Don't mark them “acknowledged,” because the actor never accepted them. Introduce an explicit terminal-delivery state such as:

```text
abandonedAt
undeliverableAt
```

which is retention-eligible but semantically distinct.

This is a soak-test concern rather than a release blocker.

---

# R6-10 — broker retirement does not clean abandoned worktrees

A clean shutdown invokes `ManagedChild.cleanupWorkspace()`. A full host crash does not.

R5 now retires abandoned broker actors after the reconnect grace, but a fresh `FabricRuntime` does not reconstruct the old `ManagedChild`, so nobody subsequently owns that old worktree cleanup lifecycle.

Repeated crash/resume cycles can therefore accumulate:

* worktree directories;
* branches;
* Pi child sessions;
* recovery metadata.

For dirty worktrees that retention is desirable. For clean abandoned ones it is leakage.

A startup/maintenance GC pass should reclaim:

```text
terminal actor
+ reconnect grace expired
+ worktree clean
+ no live/recovering actor references it
```

while preserving dirty worktrees as explicit recovery artifacts.

---

# R6-11 — small root-session memory leak

`persistedRootMessageIds` is populated after durable root receipt detection, but successful broker ACK removes the deferred maps and does not remove the ID from this set.

It is cleared only at session boundaries.

For a very long-running root session receiving tens of thousands of agent messages, that set grows monotonically.

Low severity, easy fix:

```ts
.then(() => {
  persistedRootMessageIds.delete(messageId);
  ...
})
```

Once the broker ACK has succeeded, the ID no longer needs the in-memory persistence marker.

---

## What R5 fixed successfully

I would **not** reopen the R5 architecture generally.

The following changes look sound:

* lifecycle operation IDs now cover begin/end/finish and the broker replay model is durable;
* task completion is rejected before publishing a result when live descendants exist;
* child durable receipts are exact structured receipts rather than substring matches;
* root ACK now waits for actual session persistence rather than mere `sendMessage()` acceptance;
* stale actors release runtime claims and reconnect reservations now have a finite upper bound;
* `agent_send` now has a stable tool-call identity for ambiguous retries;
* normal cross-platform CI and joint LCM smoke remain green.

R6 is therefore not saying “R5 failed.” It is exposing **composition effects introduced by making recovery stronger**.

## Recommended R6 implementation order

I would implement this as one coherent recovery-state correction rather than twelve unrelated patches:

1. **Define recovery topology semantics.** `reconnectable` must count as reserved/live for descendant and per-parent capacity invariants. Reconnect must revalidate parent topology.
2. **Make stale recovery subtree-aware.** A stale ancestor recovery-fences its descendants; grace expiry cannot terminalize a parent while leaving descendants active.
3. **Make heartbeat recovery suspension-safe.** Detect broker/event-loop pauses and make heartbeat lifecycle errors trigger re-registration rather than disappear.
4. **Fix fabric quiescence.** Count reconnectable actors and every unresolved task—including unowned tasks—as non-quiescent.
5. **Version mutable control notifications.** ACK `(messageId, revision)`, not just ID.
6. Then tighten recovery notices, local durable-state reconciliation, terminal mailbox cleanup, abandoned-worktree GC, and the root receipt set.

The key invariant I would add for R6 is:

> **Anything allowed to become live again must participate in every invariant that constrains live topology until its recovery right is irrevocably retired.**

And for messaging:

> **An ACK must identify the exact durable payload/version the recipient accepted, not merely a stable logical message ID.**

### Chaos tests I would make release-blocking

The next PR should contain explicit deterministic tests for these schedules:

```text
stale child → parent completes → child reconnect attempt
stale child → replacement child spawned → old child reconnect attempt
stale parent + healthy grandchild → parent grace expires
event-loop clock jump > heartbeatTimeout with sockets still alive
stale child → task becomes unowned-ready → fabric snapshot
control v1 queued → broker updates to v2 → v1 persists → ACK
reconnectable parent → child completes while parent absent
begin_write → broker restart → competing writer
terminal actor with hundreds of pending messages → retention/GC
```

The important difference from the existing R5 tests is that these cross **two or three subsystems at once**. The current unit tests convincingly prove individual coordinator operations; R6 needs convergence tests proving that the runtime, broker, session layer, and recovery timers agree after adversarial ordering.

### R6 verdict

The project is considerably healthier than it was in R1–R4. I don't see a reason to abandon the architecture.

But I **would not call recovery semantics finished yet**. The current R5 implementation introduced a real state-model inconsistency:

```text
failed + reconnectable
```

is presently treated as:

```text
dead for topology checks
alive for reservation
alive again on registration
```

That needs to become one coherent recovery state.

If **R6-1 through R6-5** are fixed, I expect the next round to move away from structural correctness and toward true soak properties: thousands of turns/messages, repeated suspend/resume, repeated broker restarts, bounded disk/state growth, and fairness under long-running local-model capacity contention.
