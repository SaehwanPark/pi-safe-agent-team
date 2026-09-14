# Round 8 — longevity/scale audit

I audited current `main` at **`1e5375fa994aabb89239031b4b5aa36aaff9422b`**, the merge of PR #39. The R7 fixes are materially present: committed child work is preserved, reconnect grace retains task ownership, transient recovery no longer emits premature failures, model-turn admission is queued, ACKs are replay-safe, and hot-history archival was added. PR #39 reports 182 passing tests, and the merged commit is green on Windows, macOS, Ubuntu, Pi+LCM smoke, and deploy.

The center of gravity has now moved to **scale-induced failures**. I found two release-blocking issues and several longevity problems that the new R7 benchmark currently does not expose.

| ID        | Severity                 | Finding                                                                                                                       |
| --------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| **R8-1**  | **P1 / release blocker** | `fabric.status` is unbounded and can exceed the broker client's 2 MiB frame limit, breaking LCM/quiescence queries            |
| **R8-2**  | **P1 / release blocker** | Preserved committed worktrees keep terminal agents *and their completed tasks* hot forever, defeating the archival bound      |
| **R8-3**  | **P1 high**              | Queued root compaction can acquire a broker slot and then leak it permanently                                                 |
| **R8-4**  | **P1/P2 high**           | FIFO turn admission lacks a durable turn/grant identity; “granted” is immediately modeled as `running` before host acceptance |
| **R8-5**  | **P2 high**              | Archival can become quadratic and repeatedly expensive, especially with pinned records or after long suspend/resume           |
| **R8-6**  | **P2 correctness**       | Garbage collection can change task-reopen semantics because task tombstones discard dependency edges                          |
| **R8-7**  | **P2**                   | Draining an agent does not immediately remove its queued model-turn ticket                                                    |
| **R8-8**  | **P2 soak**              | Resource state is still effectively unbounded and is included wholesale in status/checkpoints                                 |
| **R8-9**  | **P2/P3**                | New pagination cursors are not stable under concurrent archival                                                               |
| **R8-10** | **Verification gap**     | `bench:r7` specifically avoids the major default-state pressure and is not a CI longevity gate                                |

## R8-1 — `fabric.status` can eventually disconnect its own client

This is the strongest R8 scale failure.

R7 paginated `task.list` and agent discovery, but `fabric.status` still returns **every hot agent, every hot task with the complete `TaskRecord`, every resource, and every pending request**.

The broker client, however, has a hard **2 MiB receive-buffer limit**; exceeding it destroys the socket.

This does not require 10,000 tasks. A task can carry a description plus bounded result summary/output, while `maxTaskOutput` defaults to 32 KiB. A few dozen unusually large completed task records can already approach a 2 MiB JSON response. Typical smaller records merely push the failure farther out. Completed records remain hot for the default **24 hours**.

This matters directly to LCM because `getFabricStateSnapshot()` obtains its quiescence information by calling the full `fabric.status` and then locally counting/filtering the returned arrays.

Once the frame becomes too large:

```text
LCM asks for fabric snapshot
→ fabric.status serializes entire hot state
→ response exceeds 2 MiB
→ BrokerClient destroys socket
→ snapshot becomes "uncertain"
→ sessionReplacementSafe = false
→ root reconnects
→ next snapshot can repeat the same failure
```

The fallback is appropriately fail-closed, so this does not create unsafe compaction. But it creates a **permanent reconnect/uncertain-state regime caused solely by historical state growth**.

### Fix

Do not increase the 2 MiB limit as the primary solution.

Add a bounded operation such as:

```text
fabric.snapshot
```

returning server-side aggregates only:

```text
root status
running/recovering counts
unresolved/unowned task counts
mutable-hold count
write-fence/quarantine counts
pending root request count
pending model-turn count
<= 50 active task summaries
<= 50 active resource summaries
```

That is exactly what `getFabricStateSnapshot()` ultimately needs anyway.

Keep detailed task/resource/history retrieval behind the paginated APIs. `fabric.status` itself should probably become a bounded diagnostic projection too.

**Release blocker.**

---

## R8-2 — useful committed work now defeats hot-state archival forever

R7 correctly fixed the dangerous behavior where a clean-but-committed child branch could be deleted. Cleanup now refuses deletion whenever child `HEAD != baseRef`.

That preservation policy is right.

But it interacts badly with the new archival scheme.

A terminal agent with a workspace is explicitly **not archived** until `artifactsCleanedAt` exists. A committed worktree intentionally never receives that cleanup marker, because cleanup fails in order to preserve the branch.

Furthermore, when a worker completed its task successfully, terminal runtime release deliberately skips completed tasks. The agent therefore keeps its `taskId`.

Task archival then refuses to archive any task still referenced by a hot agent's `taskId`.

So the common successful coding-agent lifecycle becomes:

```text
worker edits
→ worker commits useful changes
→ task completed
→ worker terminal
→ cleanup correctly preserves committed worktree
→ artifactsCleanedAt remains unset
→ agent can never archive
→ agent.taskId remains
→ completed task can never archive
→ full TaskRecord/result remains hot forever
```

This is particularly relevant to your intended workflow because **Git commits are deliberately valuable checkpoints**, not trash.

The result is that R7's hot/cold state bounding works well for disposable children but fails for precisely the useful worktree children most likely to accumulate during real agentic development.

There is a second permanent pin: terminal agents that still own a resource are also excluded from archival.

### Fix

Separate:

```text
artifact handled
```

from:

```text
artifact deleted
```

A committed branch can be deliberately retained and nevertheless considered fully handled.

I would introduce something like:

```text
artifact disposition:
  cleaned
  retained
```

and put retained recovery metadata—workspace path, branch, base/head commits, child-session location—into a **cold artifact record**, not the hot `AgentRecord`.

Then terminal agent/task records can archive while the Git branch/worktree remains available indefinitely.

Also clear or cold-store a terminal worker's completed `taskId`; there is no reason a retained Git worktree should force a 32-KiB task result to remain in the broker's hot set.

This combines badly enough with R8-1 that I consider it a **release blocker**.

---

## R8-3 — FIFO admission introduced a root-compaction slot leak

The new broker queue correctly removes polling for ordinary turns, but root compaction has a concrete bad schedule.

Suppose global/route capacity is full when an idle/manual root compaction begins:

```text
root compaction → begin_turn(purpose=compaction)
                → queued
```

When capacity becomes free, `drainModelTurnWaiters()` deletes the waiter and immediately transitions the root to `running` **before** the root host claims the wake.

The runtime wakes and retries:

```text
begin_turn(purpose=compaction)
```

But `beginTurn()` explicitly throws `LIFECYCLE_CONFLICT` whenever a compaction sees an already-running root.

`beginRootCompaction()` intentionally catches that same error because automatic nested compaction also encounters an already-running root. It therefore assumes:

> “This must be nested inside an existing root turn; I did not acquire a separate broker reservation.”

But in this case it **did** acquire one through the queue.

Consequently `brokerReservation` remains false, and `endRootCompaction()` does not send `agent.end_turn`.

Final state:

```text
compaction finished
root still broker-status = running
capacity slot permanently consumed
```

This needs a targeted regression test:

```text
capacity=1
child occupies slot
root manual compaction queues
child ends
root compaction wakes and runs
root compaction ends
assert root == ready
assert next waiter can run
```

---

## R8-4 — admission tickets need identity and a claim phase

R8-3 exposes a more general problem.

`ModelTurnWaiter` currently knows:

```text
id
sequence
agentId
route
capacityKey
timestamp
```

but not the logical `operationId`, turn ID, purpose, or a grant lease.

When capacity becomes available the broker directly changes the agent to `running`, deletes the ticket, and then emits `slot_available`.

That conflates three distinct states:

```text
queued
granted/reserved
actually running provider work
```

It also explains why ordinary `begin_turn` now simply returns `{started:true}` for **any** new normal begin operation if the actor is already `running`. There is no durable identity proving that the current running state belongs to that particular logical turn.

Your host serialization prevents most practical double starts, but the broker invariant itself has weakened.

A more robust protocol is:

```text
QUEUED
  ↓ capacity available
GRANTED(ticketId, turnOperationId, purpose, expiresAt)
  ↓ exact host claim
RUNNING(turnOperationId)
```

The granted ticket should reserve capacity but have a short TTL. If the host disappears after the wake, capacity automatically moves to the next waiter.

This would simultaneously solve:

* the queued-compaction ambiguity;
* lost wake/granted-but-never-used slots;
* distinguishing retries from different logical turns;
* future multi-host correctness;
* normal `begin_turn` no longer needing to treat arbitrary `running` as success.

I would make this part of the R8 capacity fix rather than patching compaction with another special-case boolean.

---

## R8-5 — archival can become persistently quadratic

The history GC contains several nested whole-map scans.

For every eligible terminal agent it may scan all agents, tasks, and resources. For every eligible terminal task it scans agents and tasks again.

Under the benchmark's configuration this looks fine because `historyRetentionMs=1`, so each iteration archives almost immediately while the hot set remains tiny.

The default is 24 hours.

The nasty practical case is R8-2: retained committed worktrees keep N terminal agents hot, and each completed task remains pinned by `agent.taskId`. Once those records become older than 24 hours, every maintenance pass can repeatedly scan the same large pinned set without making progress.

The broker also takes a complete rollback snapshot before every maintenance run.

A long laptop suspend is another interesting edge. The first post-resume maintenance skips stale-agent reclamation, but **does not skip archival**, so a machine waking after records cross the retention boundary can perform a very large GC pass immediately.

### Fix

Make GC incremental and indexed:

```text
terminalAgentIds ordered by terminalAt
terminalTaskIds ordered by updatedAt
task refs by agentId
dependentsByTaskId
resourcesOwnedByAgent
```

Then process a bounded budget per maintenance tick, e.g. at most:

```text
100–500 records
or 5–10 ms wall-clock
```

Maintenance should never monopolize the broker because a machine accumulated a day's history.

---

## R8-6 — GC can change legal task transitions

This is a more subtle archival correctness issue.

`task.update(... reopen ...)` determines whether a completed prerequisite has downstream dependents by traversing the **hot task map**.

But archived task tombstones retain only:

```text
id
status
updatedAt
```

They do not retain dependencies.

And terminal dependent tasks are allowed to archive even while their prerequisite remains hot; only a **non-terminal** dependent prevents archival.

Therefore, whether reopening a prerequisite is accepted can change simply because maintenance archived a downstream completed task.

A concrete schedule is possible when prerequisite A stays hot because its worker/worktree is retained, while completed dependent B gets archived:

```text
before GC:
A completed
B completed, depends on A
taskDependents(A) sees B

after GC:
A still hot
B tombstone has no dependencies
taskDependents(A) no longer sees B
```

GC should not alter the set of state transitions the coordinator permits.

You have two coherent choices:

* retain dependency edges in task tombstones and traverse them, or
* explicitly define completed downstream tasks as irrelevant to reopen semantics and make that behavior identical before and after archival.

The key requirement is **GC-semantic transparency**.

---

## R8-7 — draining leaves capacity waiters alive

Cancellation correctly removes model-turn waiters, but `agent.drain` merely changes descendants to `draining`; it does not remove their queued turn tickets.

`drainModelTurnWaiters()` would eventually remove a waiter once some later operation invokes it, because it recognizes `draining`, but a worker currently waiting for capacity may remain asleep until another capacity transition occurs.

That means a graceful `/agents stop` can unnecessarily run into its shutdown deadline instead of immediately waking/cancelling a worker that is forbidden from starting another turn anyway.

When entering `draining`, remove any model-turn waiter and wake the host so its admission wait exits.

This is not a data-corruption issue, but it works against the deterministic graceful-shutdown behavior established in earlier rounds.

---

## R8-8 — resources remain an unbounded hot collection

Agents, tasks, requests, messages, and idempotency now have some form of retention/bounding.

Resources do not.

There is no corresponding retirement/archive operation, and `fabric.status`, coordinator checkpoints, rollback snapshots, overlap scans, and permission scans include the entire live resource map.

If resources represent a modest stable module hierarchy, that's fine.

If agents define dynamic file/symbol/task-scoped resources across a large repository, this becomes another permanent state-growth vector, particularly because terminal resource ownership already blocks agent archival.

I would add an explicit resource lifecycle rather than automatic deletion:

```text
active → retired
```

with root-authorized retirement only when there are no holds, waiters, quarantine, or required child resources.

---

## R8-9 — pagination cursors aren't stable under archival

The new task/discovery pagination uses:

```text
after = record ID
findIndex(after)
if not found → start at 0
```

If maintenance archives the page's final record between requests, the next request silently starts from page 1 again.

For human CLI use this is mostly annoying. For an agent automatically paginating, it can produce duplicates or an accidental loop.

Use a stable cursor containing the ordering tuple, e.g.:

```text
(createdAt, id)
```

or explicitly return `CURSOR_STALE` rather than silently rewinding.

---

# The R7 benchmark is useful, but it is not yet a longevity gate

`bench:r7` is a useful microbenchmark, but it currently makes the coordinator problem unusually easy:

```ts
historyRetentionMs: 1
maxArchivedRecords: 1024
```

and immediately maintenance-archives each generated child/task. It does not exercise worktree retention, messages, resolved requests, growing resources, broker restarts, suspend/resume, capacity contention, or a large hot task set.

PR #39's stated verification ran it with only **20 iterations**, and the normal CI workflow does not invoke `bench:r7` at all.

So I would not yet interpret R7 as demonstrating 8–24-hour stability.

The next benchmark should specifically keep history hot rather than aggressively collecting it.

## Recommended R8 implementation order

1. **Bound the status protocol first.** Create a small server-side `fabric.snapshot`/quiescence projection so historical state can never break LCM via frame size.
2. **Decouple retained Git artifacts from hot coordinator records.** A preserved committed branch should produce a cold artifact reference, not permanently pin an AgentRecord + completed TaskRecord.
3. **Replace direct waiter→running admission with identified, leased grants.** Store logical turn ID/operation ID/purpose; host claims the exact grant before `running`. This fixes R8-3 and R8-4 together.
4. **Make historical GC incremental and semantically transparent.** Preserve whatever dependency metadata is needed so archive timing never changes legal transitions.
5. Remove capacity tickets immediately on `draining`, then add resource retirement and stable pagination cursors.
6. Finally, make longevity testing an actual gate.

For that last step I would use at least two scenarios: a fast CI soak of perhaps 2k–5k transitions with strict state/frame bounds, and a larger manual/nightly 10k–50k scenario using **default retention settings**, committed worktree artifacts, capacity contention, broker restarts, suspend/resume jumps, large task results, and repeated LCM snapshot calls.

## Verdict

The R7 work is a meaningful improvement. I consider the basic recovery and message-durability model mature enough that I would **not redesign those layers**.

Round 8 does, however, expose a new hard boundary:

> **The control plane itself must remain bounded even when the history it coordinates is not.**

Right now `fabric.status` violates that principle, and committed worktree preservation accidentally defeats hot-state retirement. Those are the two release blockers I would address first.

The FIFO admission work is also promising, but it needs one more protocol refinement: **a capacity grant must be an identified, expiring reservation—not indistinguishable from an already-running model turn.**

After those fixes, Round 9 should genuinely be an executable soak/fault campaign rather than another broad source audit.
