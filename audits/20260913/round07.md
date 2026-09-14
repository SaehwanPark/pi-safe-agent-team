## Round 7 audit — long-run soak and convergence

I audited current `main` at **`f01526b89193a70c7b109e0188ae30643ef45ae7`**, the merge of PR #38. The R6 fixes are genuinely present, and CI is green on Windows, macOS, Linux, and the Pi + local-context-manager smoke job. PR #38 reports **172 passing tests**.

R7 therefore shifts the question from “does recovery converge?” to **“what happens after hundreds or thousands of successful/recovered agent generations?”** This exposed several issues that weren't visible when looking at individual recovery transitions.

| ID        | Severity               | Finding                                                                                                                      |
| --------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| **R7-1**  | **P1 release blocker** | Clean committed worktree branches can be automatically deleted, dropping the only durable reference to useful child work     |
| **R7-2**  | **P1**                 | Recovery releases a stale actor's task immediately, allowing reassignment during reconnect grace                             |
| **R7-3**  | **P1/P2 high**         | Temporary broker recovery is reported as durable urgent `agent_failed`, potentially causing unnecessary orchestration        |
| **R7-4**  | **P1 soak/perf**       | Agents/tasks/requests grow indefinitely, while nearly every broker operation clones the entire coordinator state—often twice |
| **R7-5**  | **P1/P2 high**         | Model-route capacity has no broker FIFO queue; 100 ms polling permits starvation and creates an RPC storm                    |
| **R7-6**  | **P2 high**            | ACK commit + prune + lost response is not replay-safe and can wedge host delivery state                                      |
| **R7-7**  | **P2 high**            | A descendant may reconnect and run before its reconnectable parent has actually recovered                                    |
| **R7-8**  | **P2**                 | Public `agent_ack` tool cannot acknowledge coalesced messages with revision >1                                               |
| **R7-9**  | **P2 performance**     | Durable write quarantine turns each guarded file write into at least two synchronous broker journal/fsync transactions       |
| **R7-10** | **P3**                 | Abandoned-artifact GC can repeatedly revisit already-cleaned historical actors                                               |

### R7-1 — committed child work can be deleted as “clean”

This is the most important new correctness issue.

Current managed-child shutdown calls workspace cleanup after a normal stop. The code explicitly treats a clean worktree as disposable.

But `GitWorkspaceStrategy.cleanup()` defines “clean” solely as:

```text
git status --porcelain == empty
```

It then does:

```text
git worktree remove <path>
git branch -D <child-branch>
```

A child that **commits its work** has a perfectly clean worktree.

So the normal successful pattern:

```text
child edits files
child git commit
task completes
child stops
```

can result in:

```text
git status = clean
→ remove worktree
→ git branch -D pi-safe/...
```

If those commits were not merged/cherry-picked/referenced elsewhere, the extension has now removed their only named Git reference. The commit object may survive temporarily through reflogs/object retention, but the supported workflow has effectively discarded the child's checkpoint.

This is particularly problematic because the project intentionally does **not** automatically decide how child work should be integrated. The cleanup path is accidentally making that decision anyway.

The startup GC introduced in R6 has the same problem: after recovery expiry it invokes the exact same workspace cleanup function.

**Fix:** “clean” and “disposable” must become separate concepts. For V1, I would preserve any worktree whose `HEAD` differs from the recorded `baseRef`, unless there is an explicit durable indication that the branch was integrated. A safer invariant is:

```text
auto-delete iff:
  worktree has no uncommitted changes
  AND HEAD == baseRef
```

Later you could add an explicit `workspace.integratedAt` / integration acknowledgement to allow deletion after merge or cherry-pick.

This deserves a regression test where the child creates one commit, completes normally, and its branch/worktree must remain.

---

## R7-2 — recovery grace protects the actor, but not its task

R6 correctly keeps a stale actor `failed + reconnectable` as a reserved/live topology participant.

However, when the actor becomes reconnectable, `markReconnectableSubtree()` immediately calls:

```text
releaseAgentRuntime(id, "broker-recovery")
```

`releaseAgentRuntime()` immediately does this for every unfinished owned task:

```text
task.owner = undefined
task.status = "ready"
```

while deliberately leaving the recovering actor's own `taskId` intact.

That produces an inconsistent recovery window:

```text
actor A:
  failed + reconnectable
  taskId = T

task T:
  ready
  owner = undefined
```

Another worker can now claim T during A's ten-minute reconnect grace. `task.claim` freely accepts an unowned ready task.

Then A reconnects. Registration finds that its former task belongs to somebody else and clears its `taskId`.

So the result can be:

```text
A reconnects successfully, but taskless
B is now doing A's old task
```

At best A becomes an unnecessary reserved worker. At worst both A's persisted Pi session and B's new execution have partially performed the same work.

The documentation currently says unfinished work returns to the pool when an actor **never reconnects**, which suggests the task was intended to remain reserved through the grace period.

**Fix:** split “release runtime claims” from “release semantic task ownership.”

On broker recovery:

```text
release:
  mutable/shared leases
  resource waiters
  write/runtime capacity

retain:
  primary task ownership
  task state
```

Only when reconnect grace actually expires should the task become ready/unowned.

That aligns the actor reservation and task reservation into one recovery transaction.

---

## R7-3 — transient recovery looks like a semantic failure to the parent

`markReconnectableSubtree()` also immediately sends an `agent_failed` internal message to the parent whenever a child is marked reconnectable.

But broker restart or missed heartbeat does not establish that the agent semantically failed. It establishes only:

> “I temporarily don't know whether this actor's host is alive.”

Those internal messages are urgent. The root delivery policy treats urgent messages as turn-triggering steering, and `agent_failed` itself is a high-signal trigger type.

So a routine broker restart can cause:

```text
broker restart
→ child enters reconnect grace
→ root receives [agent_failed]
→ root model wakes
→ root may reason that worker died
```

Then the child reconnects 300 ms later.

There is currently no matching “agent recovered” correction replacing the semantic meaning of the old notice.

This is both a cost problem and a coordination problem, especially with frontier root models and cheap local workers: harmless local transport churn shouldn't unnecessarily wake the expensive adviser/orchestrator.

I would introduce a distinct control-plane state such as:

```text
agent_recovering
```

or keep it out of model-visible messaging entirely and expose it in fabric status.

Only emit `agent_failed` when:

* provider/session failure is semantically terminal, or
* reconnect grace expires and the actor is actually retired.

---

## R7-4 — long-running fabrics accumulate unbounded history, and every RPC pays for it

This is the largest soak/performance concern.

Coordinator persisted state contains all:

* agents,
* tasks,
* resources,
* messages,
* requests,
* sender sequence records,
* idempotency records.

Messages now have bounded retention, which is good.

But there is no corresponding deletion or archival mechanism for terminal agents, completed/cancelled tasks, or resolved requests. `exportState()` serializes every one of them forever.

That alone would be manageable for modest use.

The bigger issue is that `Coordinator.dispatch()` begins **every operation** with:

```ts
const before = this.exportState();
```

including reads such as:

```text
agent.status
message.inbox
task.list
resource.inspect
fabric.status
```

and even denied capacity probes.

Then `BrokerServer.handleRequest()` takes another complete coordinator snapshot before calling `dispatch()`.

So a single broker request can copy the complete fabric state **twice**.

After a long session with, say:

```text
10,000 completed tasks
5,000 historical agents
thousands of resolved requests
hundreds of resources/grants
```

a trivial heartbeat or inbox poll becomes O(total historical state), not O(current live state).

Task completion also scans the whole task map to refresh dependency readiness, and task listing is unbounded.

This is exactly the kind of degradation that can make a system feel excellent in unit tests and progressively sluggish during an all-day autonomous coding run.

### What I would change

You don't need to discard history. Separate **hot coordinator state** from **cold audit history**.

Terminal agents/tasks/resolved requests can be archived after a retention horizon while leaving compact tombstones for identities/dependencies that still matter. Also:

* paginate/filter `task.list` and discovery;
* remove stale per-agent grants/sequences after permanent retirement where safe;
* don't clone state for pure read operations;
* don't maintain two independent full rollback snapshots across coordinator and broker;
* use transaction-local undo/delta state for mutations rather than cloning the entire world.

This deserves an actual benchmark, not just a unit test.

---

## R7-5 — local-model capacity admission can starve and generates polling traffic

The process-local `ModelRouteCapacityArbiter` itself is FIFO.

But it is only acquired **after** broker admission.

The broker's authoritative `beginTurn()` does not enqueue a waiter. It simply returns:

```text
started: false
```

when global or route capacity is full.

Each ManagedChild then sleeps 100 ms and tries again:

```ts
while (!started) {
  begin_turn()
  if (!started) sleep(100)
}
```

For one local Qwen backend with `maxConcurrent=1`, imagine A and B:

```text
A owns capacity
B polls every 100 ms

A releases
A immediately starts next queued prompt
A calls begin_turn before B's next timer fires
A wins again
```

Nothing gives B priority merely because it has waited longer.

With N blocked workers, you also get roughly:

```text
10 × N begin_turn RPCs / second
```

while they wait.

Combined with R7-4's whole-state snapshots per request, this can become surprisingly expensive.

**Fix:** make model-route admission broker-queued just like resource borrowing.

A denied `begin_turn` should create/retain a FIFO ticket keyed by:

```text
global capacity
capacityGroup
agent/logical turn
```

When a holder exits, the broker grants the next eligible waiter through a durable wake/event.

That gives you fairness and eliminates polling.

---

## R7-6 — message ACK can succeed durably yet look permanently failed to the host

The revision mechanism itself is a good R6 fix.

But `message.ack` still uses an ordinary non-idempotent broker request.

On ACK, the coordinator can:

```text
ack message
prune old retained messages
```

in the same transaction.

The broker then:

```text
journal.append + fsync
construct response
send response
```

Consider:

```text
ACK commits durably
message gets pruned
connection drops before ACK response
```

The host believes ACK failed.

A later retry gets:

```text
MESSAGE_NOT_FOUND
```

because the message was already successfully acknowledged and pruned.

For root delivery this can leave its local state at `accepted`, meaning `pendingRootDeliveries` may remain nonzero and quiescence stays closed despite the broker having fully processed the ACK.

The lifecycle operations solved this exact distributed-systems problem with durable operation IDs. ACK should use the same pattern.

**Fix:** make ACK replay-safe using something like:

```text
ack:<recipient>:<messageId>:<revision>
```

Either include `message.ack` in durable operation-id replay or retain a bounded ACK tombstone so a known historical ACK returns success after message pruning.

---

## R7-7 — descendants can reconnect before their parent actually recovers

R6 now validates a reconnecting child's parent, but the check is:

```text
isReservedLive(parent)
```

which includes:

```text
failed + reconnectable
```

Therefore recovery order can be:

```text
P failed/reconnectable
└── C reconnects → ready/running
```

before P itself returns.

This is no longer an orphan in the permanent topology sense, but it is operationally awkward. C can consume model capacity and perform work while its parent remains unavailable. If P ultimately fails to reconnect, grace-expiry subtree retirement cancels C's fresh work anyway.

It also complicates messaging because normal actor messages cannot target a terminal/reconnectable parent through ordinary `message.send`.

I would make reconnect topological:

```text
root first
then depth 1
then depth 2
...
```

A child registration should require its parent to be an **active non-recovering** state, not merely reserved-live.

The child can simply continue its reconnect backoff until its parent returns.

---

## R7-8 — the manual ACK tool is incompatible with message revisions

The public coordination tool currently exposes:

```text
agent_ack {
  messageId
}
```

only.

But after R6 the coordinator explicitly rejects ACK without a revision when current revision >1.

Managed hosts don't suffer because their internal ACK includes the revision.

But a model explicitly using:

```text
agent_inbox
agent_ack
```

cannot acknowledge a coalesced revision-2+ message.

Straightforward fix:

```text
agent_ack {
  messageId,
  revision
}
```

and teach the tool description to echo the exact pair returned by inbox.

---

## R7-9 — restart-safe writes now impose substantial fsync overhead

R6 correctly made restart write exclusion durable.

But `resource.begin_write` now mutates and journals the resource quarantine, and `resource.end_write` clears it with another durable mutation.

Every journal transaction writes:

```text
begin
events
commit
fsync
```

So one ordinary child edit/write can require at least:

```text
begin_write → fsync
filesystem mutation
end_write → fsync
```

in addition to whatever agent/session/Git I/O occurs.

That is a very defensible correctness tradeoff, but it should be measured, particularly on Windows and laptops with slower storage.

I wouldn't weaken durability based only on source inspection. Instead add a write-heavy soak benchmark. If this is expensive, a small dedicated write-intent WAL or grouped fence persistence could preserve the restart invariant with less general-journal amplification.

---

## R6 fixes that I consider successful

I would not reopen the R6 architecture. PR #38 now correctly covers reconnectable actors in topology and quiescence, makes stale recovery subtree-aware, guards against the first maintenance tick after suspension, versions mutable messages, persists restart write quarantine, retires terminal mail safely, and adds abandoned-artifact cleanup. The new tests specifically exercise many of those interleavings.

The failures above are mostly **soak semantics and lifecycle ownership boundaries**, which is exactly where I would expect the next layer of bugs to appear after R1–R6.

## Recommended R7 implementation order

1. **Fix worktree preservation first.** Never auto-delete a child branch merely because the worktree is clean; preserve unique committed work.
2. **Keep tasks reserved during reconnect grace** and make recovery topological parent-first. Separate runtime-claim release from semantic-task release.
3. Replace temporary `agent_failed` notices with explicit recovery state; only report true semantic failure/retirement to the model.
4. Add hot-state retention/archival and remove full-state double cloning from normal broker operations.
5. Replace broker `begin_turn` polling with FIFO capacity admission.
6. Make ACK replay-safe and add revision to `agent_ack`.
7. Measure write-quarantine durability overhead under a write-heavy soak workload.

### What Round 8 should look like

After those changes, I would stop doing broad static audits.

The appropriate next gate is a **real longevity benchmark**, something like a deterministic simulated 8–24 hour fabric:

```text
10k–50k model turns
5k+ child lifecycles
10k tasks
tens of thousands of messages
hundreds of broker restart/reconnect cycles
repeated synthetic suspend/resume
single-slot local capacity contention
thousands of guarded edits
periodic checkpoints
```

Then track p50/p95/p99 latency for heartbeat, inbox, begin/end-turn, task completion, checkpoint duration, memory, journal size, retained worktrees, and fairness/max wait time.

### R7 verdict

The recovery model is now substantially coherent.

The clearest remaining correctness blocker is **R7-1: committed-but-unmerged child work can be automatically deleted**. R7-2 is the next semantic issue because reconnect grace currently protects actor identity but not work ownership.

Beyond those, the center of gravity has shifted decisively toward **long-run scaling and fairness**, especially the combination of append-only historical state, whole-state cloning, and 100 ms capacity polling.

That is a good sign: the project is now reaching the point where the dangerous questions are less “will this state machine corrupt itself?” and more “will this still behave well after ten thousand transitions?”
