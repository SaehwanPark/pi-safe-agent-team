# Round 9 — executable soak/fault audit

I audited current `main` at **`1589a951d86e031333a192e284022701af4ca056`**, the merge of PR #40. The R8 changes are materially present, and all six current checks are green, including Windows/macOS/Linux, Pi + LCM smoke, and the new R8 longevity job. PR #40 also reports a successful 2,000-iteration benchmark.

The previous two R8 blockers are fixed well. `fabric.snapshot` is now genuinely small and server-side; retained Git work no longer has to keep the full agent/task hot; and model capacity now has an explicit `queued → granted → claimed/running` distinction. The architecture is substantially healthier.

Round 9 does uncover another layer, though. The most important theme is:

> **A bounded mechanism must remain correct after its retention window expires.**

That applies especially to ACK replay, grant leases, retained artifacts, and retired resources.

| ID        | Severity                 | Finding                                                                                                                                                         |
| --------- | ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **R9-1**  | **P1 / release blocker** | Successful message ACK can become unrecoverable after both message pruning and the 256-entry idempotency window expire, permanently wedging delivery/quiescence |
| **R9-2**  | **P1 high, conditional** | Independent broker restart can forget capacity consumed by a provider call that is still physically running                                                     |
| **R9-3**  | **P1 high**              | Expired model-turn grant is immediately re-granted to the same dead/nonclaiming head waiter, so TTL does not actually prevent head-of-line starvation           |
| **R9-4**  | **P2 high**              | `retainedArtifacts` is called cold state but is unbounded and copied into every coordinator snapshot/checkpoint                                                 |
| **R9-5**  | **P2 high**              | Preserving a committed worktree also preserves its child Pi session unnecessarily                                                                               |
| **R9-6**  | **P2 high**              | Retired resources are never compacted; dynamic resource usage therefore remains unbounded in production coordinator state                                       |
| **R9-7**  | **P2 high**              | Abandoned-artifact GC silently stops at the first 50 agents because its status-page cursor is always absent                                                     |
| **R9-8**  | **P2**                   | Pagination cursor encoding is incompatible with valid long resource IDs                                                                                         |
| **R9-9**  | **P2 UX/operations**     | `/agents` diagnostics silently show truncated counts/data as if complete                                                                                        |
| **R9-10** | **Verification blocker** | The new CI “longevity soak” deliberately bypasses the production broker/journal/snapshot path and is not yet the chaos campaign Round 9 needs                   |

## R9-1 — ACK durability still has a finite correctness horizon

This is the main release blocker.

`message.ack` is now idempotent, which fixed the R7 ambiguity. But durable replay is stored in the coordinator's **global 256-entry idempotency cache**. The record contains the operation response, and ACK itself can prune the acknowledged broker message.

That creates this deterministic schedule:

```text
root receives message M
→ Pi durably persists M

root sends ACK(M, rev=1)
→ broker commits ACK
→ M eventually gets pruned
→ ACK response is lost

256+ unrelated idempotent writes happen
→ ACK replay record is evicted

root retries the exact ACK
→ M no longer exists
→ MESSAGE_NOT_FOUND
```

The root-side recovery path handles `MESSAGE_REVISION_CONFLICT`, but a generic failure—including `MESSAGE_NOT_FOUND`—does not transition the local delivery to acknowledged.

The child path behaves equivalently: it intentionally keeps the accepted delivery for later retry, but nothing turns a permanently missing broker record into success.

For the root this is particularly nasty because `pendingRootDeliveriesCount > 0` is explicitly a non-quiescence reason. A single old ACK caught in this state can keep LCM/session replacement closed indefinitely.

The replay window is not huge in memory terms—the current constant is 256, not 4,096 as I initially said while investigating—but with the default 64 KiB message-body ceiling even that can still mean on the order of ~16 MiB of replay response payloads before other state is counted.

The better fix is **not** to make the generic idempotency LRU enormous. ACK needs a tiny durable tombstone independent of ordinary replay:

```text
(toAgent, messageId, revision, acknowledgedAt)
```

When the original message has been pruned, the broker should recognize that exact ACK tombstone and return successful already-acknowledged semantics. The tombstone can be much smaller than retaining the whole `AgentMessage`, and its lifetime can safely exceed ordinary message/idempotency retention.

I would not simply treat every `MESSAGE_NOT_FOUND` as success on the host—that loses the proof that this particular recipient/revision was ever acknowledged.

---

## R9-2 — broker restart can forget a provider call that is still consuming the GPU

This affects the advanced/shared broker topology rather than the normal single embedded process, but it matters because `capacityGroup` is explicitly meant to coordinate a physical backend across semantic aliases.

Consider an external broker:

```text
Host A / Qwen process
    │
    └── provider generation currently running

broker process crashes independently
```

The provider generation does not inherently stop just because its broker socket died. The ManagedChild executes `session.prompt()` under the process-local model-capacity permit.

On broker recovery, however, active agents are recovery-fenced, their `activeTurnOperationId` is cleared, and they become reconnectable.

When A reconnects, it reconciles durable task/lifecycle state, but there is no corresponding protocol saying:

```text
the provider call from turn X is still physically in flight
```

The restarted broker can therefore regard that route/capacity group as available.

Within the **same process**, the process-local arbiter still protects you. But another host/process attached to that broker can now obtain a broker grant against the same physical `capacityGroup` while A's old provider call is still using it.

That is exactly the scenario the broker-level capacity abstraction is supposed to prevent.

The conservative design is to preserve a **recovery route reservation** whenever the broker restarts an actor that had status `running`. Keep that capacity occupied through the reconnect window until the authenticated host says either “the old provider operation is still running” or “the provider operation has definitively stopped.” If the host never returns, release it at bounded recovery expiry.

For local AI, temporarily underutilizing the GPU after a broker crash is preferable to accidentally double-prefilling a memory-constrained backend.

---

## R9-3 — the new grant TTL does not actually break head-of-line starvation

The new grant/claim split is a good R8 improvement. The remaining problem is the expiry policy.

When a grant expires, `expireModelTurnGrants()` changes the ticket back to `queued` but leaves its original FIFO sequence unchanged. `drainModelTurnWaiters()` then sorts by that sequence and immediately grants the same ticket again if capacity remains available.

So with A ahead of B:

```text
A → granted
A never claims
B → queued

30 sec:
A grant expires
A → same queue position
A → immediately granted again

30 sec:
repeat
```

B never receives the slot.

If A's entire host has died, heartbeat reclamation eventually removes it after the default three-minute stale timeout. But if A is alive enough to heartbeat while its admission waiter is wedged—for example because of a host-side logic fault—the starvation can be indefinite. Defaults are 30 seconds for grant TTL versus three minutes for actor stale detection.

A grant lease should mean that failure to claim **forfeits priority**.

On expiry, preserve the logical turn but assign it a new queue sequence at the tail, or use a bounded retry policy before demotion. Then B gets a chance while A remains recoverable.

This is important for the single-slot local backend configuration you care about: one damaged worker should not monopolize the entire Qwen/oMLX queue without ever performing inference.

---

## R9-4 — retained artifacts were moved out of hot agents, but not out of hot coordinator snapshots

R8 correctly introduced `retainedArtifacts`. That lets an AgentRecord and completed task archive even when a useful Git worktree remains.

But `retainedArtifacts` itself is currently an unbounded coordinator map, and every entry is included in `exportState()` and restored into memory. I found no corresponding max count, retention policy, resolved state, or pruning path.

This matters because every production broker mutation still begins by exporting the whole coordinator state for transactional rollback. Maintenance does the same.

So “cold” currently means:

```text
not in AgentRecord
```

rather than:

```text
not on the broker's hot transaction-copy path
```

Over a development repo that deliberately retains useful child commits, thousands of retained artifacts are plausible. Each can contain workspace paths, session path, base/head refs, and a reason.

I would make retained artifact records genuinely external/cold: a separate small manifest/WAL under the state directory, with the coordinator retaining only a compact artifact ID/reference. It should also support a lifecycle such as `retained → integrated/resolved`, so a user or root can eventually acknowledge that a branch was merged/cherry-picked/discarded.

That also gives you a natural `/agents artifacts` recovery surface.

---

## R9-5 — keeping useful Git work also keeps unnecessary Pi transcripts

The normal ManagedChild cleanup currently puts workspace cleanup and child-session cleanup in a single `try`.

A committed child worktree intentionally makes `workspaceStrategy.cleanup()` fail, because deleting the branch would be unsafe. But that exception is caught before `cleanupSessionArtifacts()` runs. The code then marks both the workspace **and the session path** as retained and returns.

So:

```text
child writes good code
→ commits it
→ task completes
→ branch correctly retained
→ session JSONL also retained
```

The branch may be small. The accumulated Pi session can be much larger.

The abandoned-artifact recovery path has the same coupling: if worktree cleanup fails, it records the session path and immediately continues rather than trying to delete the session independently.

These should be independent artifact decisions.

A committed worktree should normally become:

```text
workspace = retained
session = cleaned
```

Only retain the child session when session deletion itself fails, or when there is an explicit policy saying completed-agent transcripts should be preserved.

That would substantially improve disk behavior without compromising the Git checkpoint workflow.

---

## R9-6 — resource retirement does not solve resource-state growth yet

R8 added a sensible explicit resource lifecycle. Retired resources stop participating in overlap/borrow safety, while remaining inspectable.

But retirement never removes or compacts the actual `ResourceRecord`.

All resources—active and retired—are still exported into every persisted coordinator state.

And production mutations clone that entire state for rollback.

Interestingly, the R8 benchmark itself demonstrates the usage pattern: every iteration defines a unique dynamic resource and then retires it. After 2,000 iterations all those retired records remain.

The benchmark still passes partly because it deliberately reuses one initial rollback image instead of performing the fresh production `exportState()` copy on every request. Its own comment explicitly says it is excluding broker snapshot-copy throughput.

So resource lifecycle needs one more layer, analogous to tasks:

```text
active ResourceRecord
→ retired hot record
→ compact ResourceTombstone
```

A tombstone likely needs only identity, final version, status, retired time, and perhaps path/kind if inspectability is part of the API contract. Runtime grants, waiters and ownership maps no longer matter after retirement.

---

## R9-7 — abandoned-artifact cleanup breaks once status contains more than 50 agents

This is a concrete R8 implementation bug.

`fabric.status` now correctly bounds its agent array to 50. Those entries are created with `toBoundedSummary()` and do **not** receive pagination cursors.

But `discoverAllAgentsForCleanup()` does this when status reports truncation:

```text
after = status.agents.at(-1)?.cursor
```

and only enters its paging loop when `after` exists.

Therefore:

```text
status.truncated.agents = true
status.agents.length = 50
status.agents[49].cursor = undefined

→ no page request happens
→ cleanup sees only first 50
```

Because status sorts agents oldest-first, newer terminal worktree actors can be omitted indefinitely from artifact classification. Those unclassified workspaces in turn prevent their terminal agent records from being archived.

This is straightforward to fix: if status is truncated, start `discover.agents(scope=all, limit=100)` from the beginning and page using **that API's** returned cursors. Do not use a cursor from the diagnostic status projection.

A regression test should put the retained/cleanup candidate after position 50.

---

## R9-8 — valid resource IDs can generate unusable cursors

`resource.define` permits IDs up to **1,024 characters**.

The generic pagination cursor decoder only accepts decoded IDs up to **512 characters**.

Therefore a resource whose ID is 600 ASCII characters is legal:

```text
resource.list → returns resource + cursor
```

but feeding that exact cursor into the next page causes cursor decoding to fail.

Unicode makes the length mismatch worse because `encodeURIComponent()` can expand the opaque cursor substantially.

I would stop tying cursor wire length to the underlying identifier's textual form. A base64url-encoded compact tuple or another opaque format is preferable, with a cursor length limit chosen independently from individual entity-ID limits.

At minimum, decoder and `after` limits need to cover the largest valid resource identity.

---

## R9-9 — `/agents` now silently lies by omission when status is truncated

The bounded diagnostic projection itself is good.

The CLI formatter has not caught up with it.

`FabricStatus` exposes authoritative `totalAgents`, `totalTasks`, `totalResources`, and truncation flags. The returned arrays contain only the first 50.

But the command formatter still reports:

```text
agents: status.agents.length
tasks: status.tasks.length
resources: status.resources.length
```

and `/agents tree`, `/agents tasks`, and `/agents resources` simply print those bounded arrays without any “showing 50 of 347” notice.

That means the operator can see:

```text
agents: 50
```

when the broker actually has 200.

Worse, because agents are sorted oldest-first, the omitted records may include the worker that is currently relevant to a long-running operation.

At minimum, the commands should show authoritative totals and an explicit truncation marker. Better still, tree/tasks/resources commands should page their corresponding APIs when explicitly requested by the human.

This isn't an internal consistency failure, but it is exactly the kind of bad monitoring experience a remote-control/operator-oriented setup should avoid.

---

# The biggest Round 9 issue is actually the test boundary

The new CI longevity check is useful and green.

But it is **not yet the executable fault campaign we planned for Round 9**.

The benchmark directly invokes `Coordinator.dispatch()`, deliberately supplies one reusable rollback image, and says that this is done specifically to exclude production snapshot-copy throughput. It has no BrokerClient/BrokerServer round trips, no JSONL fsync path, no dropped socket, no abrupt broker restart, no suspend/resume cycle, no real workspace/session artifact lifecycle, and only one capacity-contention episode.

So I would keep the existing benchmark as a good **coordinator micro-soak**, but not use its green result as evidence for end-to-end multi-hour stability.

The next implementation sequence I recommend is:

1. **Fix R9-1 first:** introduce durable compact ACK tombstones independent of the general 256-entry idempotency LRU, and test ACK commit → message prune → replay eviction → response retry.
2. Fix grant liveness: an expired grant must yield its queue position. In parallel, preserve route capacity across an independent broker restart when the host may still have an in-flight provider call.
3. Make retained artifacts genuinely cold and separately resolveable, then decouple session cleanup from worktree retention.
4. Compact retired resources into tombstones and fix the >50-agent cleanup paginator plus cursor length contract.
5. Fix `/agents` truncation reporting.
6. Add a true `bench:r9-chaos` using `BrokerServer` + `BrokerClient` + a real temporary journal. The CI-sized version should repeatedly restart the broker, drop connections after commits, expire grants, jump the clock, create/prune large message sets, and exercise retained worktrees. A larger nightly/manual version can scale that to thousands or tens of thousands of cycles.

## What I would require before calling Round 9 passed

I would not require another architecture redesign. R8's basic direction is right.

The acceptance criterion now should be **convergence after information ages out**:

```text
no old ACK can permanently wedge quiescence
no dead grant can monopolize a capacity group
no independent broker restart can over-admit a physical backend
no retained artifact/resource collection grows on every hot transaction forever
no bounded diagnostic silently becomes an incomplete operational truth
```

The only issue I would treat as a general release blocker is **R9-1**. R9-2 is equivalently serious if external/shared-broker mode is considered supported rather than experimental. R9-3 should land in the same hardening release because single-slot local models make that liveness failure particularly visible.

### Verdict

R8 successfully moved the project past the earlier state-machine problems. The remaining findings are now mostly consequences of **bounded retention and real long-run operation**, which is encouraging.

The next milestone should not be another broad audit after source fixes. It should be the first genuine broker-level chaos harness. Once that exists and these R9 schedules pass under repeated restarts, pruning, expiry, and artifact retention, I would expect Round 10 to become much more about measured latency/disk/memory behavior than discovering new coordination invariants.
