## Round 3 audit

I audited current `main` at **`50b32af`**, the merge of PR #30 addressing R2. The R2 patch is substantial, and its CI checks are green across Linux, macOS, and Windows.

My overall assessment has improved again: **R1's coordination/shutdown layer and much of R2's context-failure machinery are now solid. R3 is exposing fewer but deeper integration bugs, mostly at transaction boundaries between Pi, the broker, and LCM.** I found three issues I would treat as clear release blockers, three more important correctness gaps, and several lower-priority hardening items.

### R3 findings

| ID       | Severity                      | Finding                                                                                                                         | Assessment                         |
| -------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------- |
| **R3-1** | **P1 / critical correctness** | A task that already completed can be changed back to `blocked`/`failed` by a later model/compaction failure                     | **Confirmed**                      |
| **R3-2** | **P1**                        | Root route-capacity admission does not actually gate the root provider call                                                     | **Confirmed race**                 |
| **R3-3** | **P1**                        | Deferred root messages are ACKed before they are durably persisted into the Pi session                                          | **Confirmed message-loss window**  |
| **R3-4** | **P1**                        | Child `prefill_capacity` "same-context retry" actually submits the user prompt a second time                                    | **Confirmed semantic bug**         |
| **R3-5** | **P1**                        | `effectivePrefillBudget` is not actually honored by current LCM, and normal extension users cannot configure route policies yet | **Confirmed end-to-end gap**       |
| **R3-6** | **P1**                        | Manual root compaction can proceed without a broker route reservation; naïvely fixing it creates lock-order deadlock            | **Confirmed architectural issue**  |
| R3-7     | P2                            | Outcome classifier can misclassify prefill pressure as generic runtime memory pressure                                          | Confirmed pattern-order issue      |
| R3-8     | P2                            | Recovery transition is non-atomic across task + agent + parent notification                                                     | Crash-consistency gap              |
| R3-9     | P2                            | Blocked-state recovery can remain stale across broker reconnect                                                                 | Confirmed state-reconciliation gap |
| R3-10    | P2                            | Route capacity is keyed only by `provider/model`, not actual backend/runtime identity                                           | Design limitation                  |
| R3-11    | P2                            | safe-agent interop registry overwrites unknown future registry versions                                                         | Forward-compatibility bug          |
| R3-12    | P3                            | Blocked children mirror unacked broker messages in memory; whole-turn capacity locking can hurt fairness                        | Robustness/performance             |

The first six deserve attention before I'd consider the KV/compaction robustness goal satisfied.

---

## R3-1: completed tasks are no longer terminal in practice

This is the highest-priority new finding.

R2 correctly made context/provider outcomes explicit. A blocked outcome now executes roughly:

```text
task.update(action=block)
agent.end_turn(status=blocked)
notify parent
```

and a fatal outcome similarly calls `task.update(action=fail)`.

The problem is that the coordinator's task state machine allows `block` and `fail` on a task **without checking whether it is already terminal**. Only `ready/reopen` performs an explicit terminal-state check.

That creates this entirely plausible sequence:

```text
child performs work
        |
        v
agent_task complete
task = COMPLETED
parent receives task_result
        |
        v
child reaches final response / settled boundary
        |
        v
threshold compaction runs
        |
        v
compaction fails / provider memory fails
        |
        v
R2 outcome handler
task.update(block)
        |
        v
task = BLOCKED
```

So the fabric can tell the parent "task completed" and later mutate that same durable fact into "task blocked."

That violates one of the best architectural principles already present in this project: **model-runtime state must not override durable semantic task facts.**

The fix should be two-layered. Coordinator terminal task states should be immutable under `block`, `fail`, and ordinary `cancel`; only an explicit, deliberately authorized `reopen` should cross a terminal boundary. Separately, `finishTurnWithOutcome()` should inspect the current task first. If the task is already completed, that completion wins; a late context problem may become a diagnostic about the worker/session, but must never rewrite the completed task.

A regression test should specifically exercise:

```text
task complete
→ parent task_result committed
→ embedded/threshold compaction fails
→ task remains completed
→ no contradictory "blocked task" notification
```

This is more important than adding further context heuristics.

---

## R3-2: route capacity doesn't gate the root's actual model request

The R2 route-capacity system works at the coordinator level. `agent.begin_turn` returns `started:false` when another actor already consumes the route's configured capacity.

Managed children wait for that result before calling their model.

The root does not.

Its `agent_start` handler currently launches the lifecycle operation with:

```ts
void enqueueLifecycle(...)
```

and returns immediately. Inside that detached operation it eventually loops until `agent.begin_turn` succeeds.

Pi, meanwhile, emits `agent_start`, waits for extension handling to return, and then proceeds toward the assistant request.

Therefore:

```text
child owns omlx/qwen route slot
             |
root Pi agent_start
             |
safe-agent starts async broker admission ───────┐
             |                                   |
handler returns                                 |
             |                                   |
Pi starts root provider request                 |
             |                                   |
             |                         broker says capacity full
             |                         and starts polling
             v
ROOT + CHILD NOW HIT LOCAL MODEL CONCURRENTLY
```

The capacity control is recording the contention rather than preventing it.

### Correct fix

The first `agent_start` of a logical root run must **return/await** broker admission before Pi may continue.

The R2 logical-run latch still solves repeated `agent_start` during Pi retry/compaction:

```text
first agent_start:
    await admission

retry agent_start:
    logicalRootRunActive => immediate return
```

This makes the root follow the same admission semantics as a managed child.

This deserves an integration test using a deliberately blocked route slot and a provider stub that asserts the root provider callback is not entered until the broker admits it. The existing R2 tests test coordinator capacity and local-arbiter capacity independently, but not this host-to-provider ordering.

---

## R3-3: "deferred durable delivery" has a crash-loss window

The idea introduced in R2 is good: while the root is compacting or its provider is degraded, child messages should remain visible without waking another model request. `classifyRootDelivery()` therefore changes them to:

```text
triggerTurn = false
deliverAs = nextTurn
```

The problem is the acknowledgement point.

Safe-agent currently:

1. sends the message to Pi as `nextTurn`;
2. records it in the in-memory `deferredRootMessages`;
3. **ACKs the broker message immediately**.

But Pi's `nextTurn` behavior does not immediately persist the custom message into session history. It puts it into an in-memory pending-next-turn queue and injects it only when a real subsequent prompt starts.

Meanwhile the broker's inbox only returns **unacknowledged** messages.

So:

```text
broker durable message
       |
       v
Pi pendingNextTurn RAM
       |
       +--> broker ACK
       |
       X process crashes / session switches here
```

On restart:

* Pi's transient next-turn queue is gone;
* `deferredRootMessages` is gone;
* the broker considers the message acknowledged;
* inbox recovery will not return it.

That is actual message loss.

### Better ownership rule

For deferred delivery, the broker should remain the durable source of truth until Pi has crossed a durable acceptance boundary.

The simplest design is:

```text
normal immediate delivery:
    Pi accepts/persists -> ACK broker

deferred delivery:
    leave broker UNACKED
    remember only message ID / state locally
    after compaction/recovery:
        deliver into actual prompt/session
        once persisted -> ACK broker
```

The in-memory map can remain an optimization, but must not be the sole surviving copy.

There is a second smaller issue here: `wakeDeferredRoot()` clears its map before invoking the wake operation, so a synchronous wake failure also loses the local retry marker.

---

## R3-4: capacity retry is not a same-context retry

This is particularly relevant to your original KV-cache requirement.

R2's child recovery says:

> structured prefill failure → release route capacity → retry same context once.

The implementation loops twice around:

```ts
session.prompt(prompt, ...)
```

But calling public `AgentSession.prompt()` again creates a **new user turn**. Pi constructs a fresh user message and runs a fresh prompt operation on every call.

So the effective history is:

```text
user:      "do X"
assistant: prefill error
user:      "do X"       <- duplicated
assistant: ...
```

It is not:

```text
same user turn
    provider attempt 1 fails due capacity
    provider attempt 2 retries same context
```

This can actually make a prefill failure worse because the second request has a larger transcript.

The current R2 test does not reveal this because it replaces `session.prompt()` with a mock that merely increments a counter; it contains no real Pi session history.

### Recommendation

Do not implement a capacity retry by invoking public `prompt()` twice.

A genuine solution needs a Pi-level seam for **retrying the current generation without adding another user message**. Ideally this should live close to Pi's own retry machinery, because Pi already knows how to retry a generation while maintaining transcript invariants.

Until such a seam exists, I would prefer:

```text
capacity failure
→ release competing capacity
→ if safe same-generation retry primitive exists: retry once
→ otherwise block gracefully
```

rather than silently duplicate the user turn.

---

## R3-5: the effective prefill budget is currently mostly aspirational

R2 added a good API distinction between:

```text
logical context window
effective hardware-safe context budget
```

Safe-agent now passes both effective-budget fields to its embedded LCM provider.

However, **current `pi-local-context-manager/main` does not consume them.**

Its current `EmbeddedContextManagerOptions` only defines `contextWindow`; there is no `effectiveContextBudget` or `effectivePrefillBudget`.

And the embedded controller computes its thresholds directly from `options.contextWindow` / `usage.contextWindow`.

Extra JS properties are therefore simply ignored.

So with:

```text
Qwen model logical window = 131k
safe prefill budget        = 48k
```

safe-agent may successfully calculate 48k, but LCM still sees 131k as the threshold basis.

There is also a second end-to-end issue: the installed extension creates:

```ts
new FabricRuntime()
```

with no configuration loader.

The configuration documentation itself currently says policies can be supplied programmatically to `FabricRuntime` **or a future role/config loader**.

So an ordinary extension user cannot presently configure:

```text
effectivePrefillBudget = 48000
```

through the normal installed-extension path anyway.

### This needs a coordinated two-repo fix

Safe-agent and LCM should agree on an interop contract where LCM explicitly understands:

```text
logicalContextWindow
effectiveContextBudget
```

and uses the latter for threshold policy while retaining the former for provider semantics.

Then safe-agent needs a normal configuration-loading path rather than only constructor-level configuration.

The important joint test is not "did safe-agent pass `48000` in an options object?" It should be:

```text
advertised model window = 131072
effective budget         = 48000
active context           = e.g. 40-45k

=> LCM reaches its configured proactive threshold based on 48k,
   not based on 131k
```

Also, CI currently runs the ordinary checks/build on all three OSes but does **not** exercise the joint LCM smoke test.  Given how tightly coupled context safety now is to that interop, I would add that combination to CI.

---

## R3-6: root compaction has a distributed-capacity hole and a lock-order trap

This is the most subtle finding.

For manual root compaction, `beginRootCompaction()` currently:

```text
1. acquire process-local model-capacity permit
2. ask broker agent.begin_turn
3. if broker says started=true, remember reservation
4. if started=false ... continue anyway
```

The broker deliberately returns `started:false` when either global or route capacity is unavailable.

Because the local arbiter only knows about one process, another process can legitimately occupy the same broker-controlled route. The current root then performs its compaction despite broker refusal.

It would be tempting to "fix" that by simply waiting until broker admission succeeds.

Don't do only that, because the acquisition order is already inconsistent:

```text
ManagedChild:
    broker begin_turn
    → local capacity permit
    → provider

Root manual compaction:
    local capacity permit
    → broker begin_turn
    → provider
```

If root starts waiting on broker admission while retaining its local permit, this can become a textbook lock inversion:

```text
child:
    holds broker route slot
    waits for local permit

root compaction:
    holds local permit
    waits for broker route slot
```

Neither can progress.

### Better architecture

This suggests route capacity should eventually become a **first-class model-operation lease**, not be inferred indirectly from agent `running` state plus a second local mutex.

Conceptually:

```text
model_operation.acquire(routeKey, kind)
model_operation.release(lease)
```

where `kind` can be:

```text
generation
compaction
recovery
```

The broker is authoritative. A process-local gate may still optimize same-process scheduling, but it should not form an independent lock hierarchy.

If you want a smaller R3 change, at minimum make all paths acquire capacities in the same order and never continue on `started:false`.

---

## Additional R3 hardening

The outcome classifier is directionally good but its ordering needs refinement. It tests broad runtime-memory patterns before prefill-capacity patterns. In particular `/metal.*memory/i` is broad enough that a prefill-guard error mentioning Metal memory can become `runtime_memory_pressure` rather than `prefill_capacity`, losing the intended bounded capacity retry.  Structured provider codes should take priority, then explicit prefill/KV patterns, then generic OOM/eviction/runtime-pressure heuristics.

The recovery commit is also not atomic. `finishTurnWithOutcome()` mutates the task, then ends the agent turn, then sends the parent notice in separate broker operations.  A disconnect between those operations can leave combinations such as `task=blocked` while the agent remains `running`, potentially holding route capacity until liveness recovery. Long term, I would make "finish model turn with outcome" one coordinator transaction.

Reconnect deserves another synchronization pass as well. Initial child startup reconstructs `blockedByOutcome` when the coordinator says the agent is blocked, but the reconnect path updates `record`/`taskId` and resumes work without equivalently rebuilding that local recovery gate from durable agent + task state.  That makes missed `task_changed` events around disconnect capable of leaving local and broker recovery states inconsistent.

There are two architectural limitations rather than outright defects. Route identity is currently just `provider/model`, and local-runtime detection depends on provider-name substrings such as `omlx`, `ollama`, or `llamacpp`.  Eventually the capacity key should identify the **actual backend instance**—for example endpoint/runtime identity + provider + model—because two aliases may hit the same GPU, while two endpoints may legitimately serve the same model independently.

Finally, safe-agent's interop registry handling is now weaker than LCM's. If `Symbol.for("pi.extension-interop.v1")` contains an unknown version, safe-agent replaces it with a new v1 registry.  LCM already uses the safer design: preserve a well-formed foreign version, isolate locally, and fail closed instead of overwriting another extension's state. That behavior should be shared.

---

## What R2 clearly fixed well

I would **keep** several R2 changes essentially as designed. The child outcome classifier is a strong improvement over treating every provider failure as an exception. The coordinator route-capacity mechanism itself behaves correctly for actors that actually await it. The blocked child gate prevents ordinary inbox traffic from creating infinite provider retries. Root compaction is now visible in fabric quiescence. Emergency draining now begins local cancellation before potentially slow broker operations and can be escalated from graceful/budget to `--now`.

The child lifecycle cleanup is also better: terminal/disposed `ManagedChild` instances remove themselves from the runtime's live map.  And replacing transcript-scanning abort detection with actual `agent_end` observation was the correct R2 direction.

So I would **not** redesign those pieces. R3 should tighten their transactional boundaries.

## Recommended R3 implementation order

1. **Restore hard invariants first:** make terminal task facts immutable; ensure late context failures cannot undo completion. Fix root model admission so provider execution cannot outrun broker capacity.
2. **Fix durability and retry semantics:** keep deferred root messages broker-unacked until persistent acceptance, and remove the double-`prompt()` capacity retry.
3. **Make KV protection genuinely end-to-end:** update LCM to consume effective budgets, provide a real user configuration path, and add a joint integration test to CI.
4. **Unify capacity ownership:** resolve root-compaction admission and lock ordering, preferably toward a broker model-operation lease.
5. **Then harden recovery:** atomic outcome commits, reconnect-state reconciliation, structured-code-first error classification, backend-instance capacity identities, and non-destructive interop version negotiation.

### R3 verdict

After R1 I was worried about coordination correctness. After R2 I was worried about model-runtime/context correctness. **After R3, I think both underlying designs are basically sound; the main remaining risk is that boundaries between them are not yet transactional enough.**

The most important invariant for the next patch is:

> **A model/runtime recovery mechanism must never alter an already-established semantic fact, start compute before admission, acknowledge data before durable acceptance, or retry by changing the conversation being retried.**

If R3 closes those four classes of boundary violation, I would expect a Round 4 audit to move away from major architectural faults and toward adversarial races, crash consistency, fairness, and long-duration soak behavior rather than finding another fundamental layer of problems.
