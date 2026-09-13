I audited the post-R1 `main` at `e021c03` and also traced the relevant current Pi `AgentSession` compaction/retry behavior and the current `pi-local-context-manager` embedded path.

The R1 work looks materially successful. Recursive drain/cancel, lifecycle epochs around in-flight spawning, bounded child aborts, deterministic handoff capture, concurrent-child rather than lifetime limits, unknown-root-shell fail-closed behavior under active child writes, and worktree cleanup all address the earlier findings in the right architectural layer. I do **not** see another R1-scale flaw in the basic cancellation tree.

Round 2 does expose a different cluster of issues, though: **the fabric lifecycle currently assumes that `session.prompt()` returning means the model turn was semantically successful. That assumption is false for Pi's retry/compaction model, and becomes especially problematic with local-model prefill/KV failures.**

## R2 findings

| ID    | Severity      | Finding                                                                                                         | Assessment                       |
| ----- | ------------- | --------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| R2-1  | **P1 high**   | A child can suffer a terminal model/context error and be returned to `ready` with its task still active         | **Confirmed**                    |
| R2-2  | **P1 high**   | Root broker `agent.begin_turn` is not idempotent across Pi automatic retries / compact-and-retry                | **Confirmed**                    |
| R2-3  | **P1 high**   | Root manual/LCM compaction is invisible to fabric quiescence and can overlap root message-triggered turns       | **Confirmed architectural race** |
| R2-4  | **P1 high**   | KV/prefill capacity failures are not the same thing as context overflow; blindly compacting is unsafe           | **Important for local AI**       |
| R2-5  | **P1 high**   | No model-route capacity coordination; subagents can collectively create the KV/prefill failure                  | **Architectural gap**            |
| R2-6  | **P1 high**   | Child proactive context management uses advertised context window, not effective hardware-safe prefill capacity | **Confirmed design mismatch**    |
| R2-7  | **P1 medium** | `--now` still waits on broker/status work before locally aborting children                                      | **R1 second-order issue**        |
| R2-8  | **P2**        | Root abort detection should use the actual final `agent_end`, not scan the transcript at settlement             | **Brittle**                      |
| R2-9  | **P2**        | Terminal ManagedChild objects remain in the runtime's live `children` map                                       | **Leak/hygiene issue**           |
| R2-10 | **P2**        | LCM embedded-provider naming/config/diagnostics are more fragile than they need to be                           | **Interop hardening**            |

The first six are worth addressing before I would call the extension robust against long-running local-agent workloads.

---

## R2-1 — terminal child errors can be laundered into `ready`

This is the most consequential finding.

`ManagedChild.executePrompt()` does:

```ts
await this.session.prompt(prompt, ...);

// ...

const task = ...
const status = taskAwareTurnStatus(task, ...);
await this.client.request("agent.end_turn", { status });
```

There is **no inspection of the final assistant message** after `session.prompt()`.

That would be fine if `AgentSession.prompt()` rejected whenever the model ultimately failed. But Pi deliberately does not work that way. It handles retryable errors, overflow recovery, compaction, and retry internally. Once those mechanisms are exhausted, an assistant message with `stopReason: "error"` can be the final result and `prompt()` can return normally. Pi's overflow path attempts compact-and-retry once; after another overflow it emits a compaction failure and settles.

So this sequence is possible:

```text
child task = active
      |
model call
      |
context/prefill error
      |
Pi retry / compaction recovery
      |
recovery exhausted
      |
session.prompt() resolves
      |
safe-agent-team checks only task state
      |
task still active
      |
agent.end_turn("ready")
```

The child now looks healthy even though it failed to make progress.

Worse, another parent message can wake it again and repeat the same cycle.

### Fix

Add a shared **turn-outcome classifier** and have `ManagedChild` subscribe to its `AgentSession` events. After `session.prompt()` settles, distinguish at least:

```text
success
aborted
transient-error-exhausted
context-overflow-exhausted
compaction-failed
capacity/prefill failure
fatal provider/auth error
```

Do not derive lifecycle solely from task state.

For a recoverable context/capacity problem, I would usually transition the child/task to `blocked`, not `failed`, and send a deterministic bounded diagnostic to its parent:

```text
agent blocked:
  cause = prefill_capacity
  provider/model = ...
  context tokens ~= ...
  native recovery attempted = yes
  compaction attempted = yes/no
```

That preserves the session and worktree for recovery without presenting it as healthy.

This same logic automatically applies to **recursive parent agents**, because they are also `ManagedChild` instances.

---

## R2-2 — automatic retry/overflow continuation conflicts with root broker lifecycle

This is directly connected to compaction.

Current root wiring calls:

```ts
pi.on("agent_start", ... {
  await runtime.request("agent.begin_turn", {});
});
```

and does not end the broker turn until `agent_settled`.

That sounds right, except Pi emits a fresh `agent_start` for a continued agent loop too. `agentLoopContinue()` explicitly emits `agent_start`, and Pi uses continuation for automatic retry and overflow compact-and-retry.

Therefore:

```text
agent_start #1
 -> broker root running

agent_end #1, willRetry=true
 -> broker deliberately remains running

Pi compact/retry

agent_start #2
 -> safe-agent-team calls agent.begin_turn again
 -> coordinator: "already running" LIFECYCLE_CONFLICT
```

Pi explicitly documents `agent_end` as a low-level boundary and `agent_settled` as the final boundary after retries/compactions/continuations.

This probably manifests mostly as lifecycle warnings today rather than corruption, but it is exactly the kind of behavior that will become noisy under local models where retry/compaction is common.

### Fix

Make root turn tracking idempotent at the host layer:

```text
first agent_start since last agent_settled
    -> agent.begin_turn

subsequent agent_start while logical root run active
    -> no-op

agent_settled
    -> agent.end_turn
    -> logicalRootRunActive = false
```

Alternatively add a coordinator `agent.ensure_running` operation, but a local logical-run latch is simpler.

And add an explicit test:

```text
agent_start
agent_end(willRetry=true)
agent_start
agent_end(willRetry=false)
agent_settled
```

Expected broker operations:

```text
agent.begin_turn
agent.end_turn
```

exactly once each.

---

## R2-3 — root compaction is currently invisible to fabric quiescence

Managed-child compaction is mostly safe from this problem: `ManagedChild.executePrompt()` keeps the broker child running until embedded `observeSettled()` finishes.

The **root** is different.

LCM can schedule threshold/semantic compaction from `agent_settled`.  Safe-agent-team concurrently turns the broker root back to `ready` at that same settlement boundary.

Manual `/compact` has the same issue.

Safe-agent-team does not currently listen to:

* `session_before_compact`
* `session_compact`
* `session_compact_failed`

even though Pi exposes all three.

So there is a period where:

```text
Pi root: actively compacting
broker root: ready
children: none
holds: none
requests: none

=> fabric snapshot: quiescent=true
=> sessionReplacementSafe=true
```

That is semantically wrong.

It can also interact with fabric message delivery. High-signal child messages currently have `triggerTurn: true`.  During an out-of-band/manual root compaction, that risks starting or queuing root work at precisely the wrong time.

### Fix

Track root context mutation explicitly:

```ts
rootCompactionInFlight += 1  // session_before_compact

rootCompactionInFlight -= 1  // session_compact
rootCompactionInFlight -= 1  // session_compact_failed
```

and add:

```text
quiescenceReasons += "root_compaction_in_flight"
sessionReplacementSafe = false
```

No interop schema bump is really required; the existing conservative quiescence contract can represent it.

While that flag is set, root-delivery policy should **store messages durably but not trigger a new turn**. Deliver them as `nextTurn`/context-only and wake the root only after compaction settles.

This is especially important for local models because simultaneous summarization prefill + ordinary prefill is exactly what you do **not** want under tight memory.

---

# KV cache / prefill failures need their own category

The most important conceptual change I recommend is:

> **Do not model every “prompt/context too large” looking error as context overflow.**

Current Pi already does a respectable job with true context-window overflow. Its detector recognizes many provider-specific messages, including llama.cpp-style context-size errors, and its built-in response is one compact-and-retry attempt.

But local inference introduces a different failure:

```text
logical model context window: 128k
actual prompt:              46k
server memory available:    insufficient for transient prefill/KV
```

That is **capacity pressure**, not logical context overflow.

This is happening in current oMLX deployments. One recent Qwen3.8-27B report shows the prefill guard becoming restrictive around roughly 44–51K tokens on a 48 GB machine despite a much larger logical model context. ([GitHub][1]) Another oMLX failure mode can occur because process-memory pressure causes an abort/model eviction around a long prefill. ([GitHub][2])

More importantly, treating these failures as ordinary overflow can create a loop: the compression request itself needs a substantial prefill and hits the same memory guard. That failure mode has already been documented in another agent harness using oMLX. ([GitHub][3])

So I recommend a taxonomy such as:

| Failure class                              | Correct first reaction                                                        |
| ------------------------------------------ | ----------------------------------------------------------------------------- |
| `context_overflow`                         | Pi native compact + one retry                                                 |
| `recoverable_length`                       | Pi native recovery                                                            |
| `transient_provider`                       | bounded retry/backoff                                                         |
| `prefill_capacity`                         | **free capacity first**, retry once                                           |
| `runtime_memory_pressure`                  | route circuit-break / cooldown                                                |
| `compaction_failed`                        | stop retry loop; preserve state                                               |
| `cache_corruption/suspected_backend_fault` | fresh/cache-bypassed request if provider supports it, otherwise circuit-break |
| `auth/config/fatal`                        | block/fail clearly                                                            |

For oMLX specifically, structured `omlx_code: prefill_memory_exceeded` should be classified as `prefill_capacity`, not rewritten to `context_overflow`. ([GitHub][3])

---

# R2-5 — agents sharing a local model have no capacity coordination

The fabric has concurrency controls, but they operate on **agent counts**, not model-runtime memory.

That is inadequate for local inference.

Suppose:

```text
Qwen3.8-27B / oMLX / one GPU-memory pool

root
 ├─ child A prefill 35k
 ├─ child B prefill 42k
 ├─ child C compaction summary
 └─ child D prefill 20k
```

Every session can individually fit the nominal model window and yet collectively push the server into prefill/KV failure.

The current fabric has no concept analogous to:

```text
capacity:omlx://machine/model/Qwen3.8-27B
```

The existing general `maxConcurrentAgents` cannot express this distinction.

### Recommended design

Add a separate **model-route capacity arbiter**, conceptually similar to the resource manager but simpler.

A capacity key should ideally identify the actual backend instance, not merely the model name:

```text
backend endpoint + provider + model
```

Then allow configuration like:

```text
cloud OpenAI route       -> unlimited/default
local oMLX Qwen3.8-27B   -> 1 heavy operation
local llama.cpp model    -> 1 or 2
```

For V1 I would gate the whole model request. Later, if Pi exposes a clean prefill/decode boundary, this can become more sophisticated.

Crucially, **compaction must acquire the same capacity token**. A compaction summary is itself a large model request.

This gives one invariant:

> Normal generations, retries, and compaction summaries targeting the same constrained runtime must all participate in the same capacity policy.

That would remove a substantial fraction of KV failures before recovery is needed.

---

# R2-6 — advertised context window is not a safe prefill budget

The managed-child integration currently passes:

```ts
contextWindow: this.model.contextWindow
```

to the embedded context controller.

LCM then derives its thresholds from that context window.

That makes sense for cloud APIs whose logical context limit is the binding constraint.

It is often wrong for local inference.

For example:

```text
advertised contextWindow = 131072
machine-safe prefill      = perhaps ~45k under current conditions
```

The proactive context manager will therefore wait far too long.

### Introduce an effective context budget

I suggest keeping two concepts explicitly:

```text
logicalContextWindow
effectivePrefillBudget
```

and using:

$$
\text{context-management budget}
=
\min(
\text{logicalContextWindow},
\text{configuredSafePrefill},
\text{learnedSafePrefill}
)
$$

For cloud routes, `effectivePrefillBudget` usually equals the logical window.

For local routes, it can come from:

* explicit model/runtime configuration;
* provider-reported capabilities;
* observed structured capacity failures;
* optionally a conservative auto profile.

Do **not** permanently learn a small context ceiling just because one failure occurred during transient memory pressure. A useful adaptive rule is to keep failure observations session/runtime-instance scoped and distinguish:

```text
failed at 48k while no competing load
    -> evidence of context/prefill ceiling

failed at 8k while server memory already saturated
    -> evidence of backend pressure, NOT an 8k context limit
```

That distinction matters a lot.

---

# A recovery ladder that works for both parent and child agents

I would implement one common state machine and use it from both root hooks and `ManagedChild`.

For true context overflow, keep Pi's existing behavior: native compact-and-retry once. Pi already intentionally prevents unlimited overflow retry.

For `prefill_capacity`, I recommend:

```text
prefill/KV capacity failure
        |
        v
freeze new calls on same capacity route
        |
        v
allow/abort competing same-route work as policy permits
        |
        v
retry SAME context once
        |
        +---- success -> healthy
        |
        v
still fails
        |
        +---- context is genuinely large
        |        |
        |        v
        |   compact once
        |   preferably via alternate/lightweight summarizer
        |        |
        |        +---- success -> retry once
        |
        v
block agent/task + deterministic handoff
```

There should be hard counters such as:

```text
max ordinary provider retries: Pi's configured limit
max context recovery:          1
max capacity retry:            1
max capacity-driven compact:   1
```

Never recursively compact because compaction failed.

And if the same local backend is obviously unhealthy—even tiny requests fail with memory pressure—open a route-level circuit breaker instead of burning tokens and repeatedly touching the same model.

---

## Root-specific circuit breaker

The root needs one additional protection because child messages can wake it automatically.

Today almost every high-signal message triggers a root model turn.

If the root's local provider has entered a persistent KV/prefill failure state, this can become:

```text
root fails
child A sends result -> wake -> fail
child B asks question -> wake -> fail
child C sends blocked -> wake -> fail
...
```

So after an exhausted root context/capacity failure:

```text
rootContextHealth = degraded
```

should temporarily change delivery from:

```text
triggerTurn=true
```

to:

```text
persist + model-visible + display
triggerTurn=false
```

until one of these happens:

* successful recovery/compaction;
* user initiates a new run;
* model/runtime switch;
* explicit retry/reset.

Nothing is lost because your broker mailbox is already durable.

---

# R2-7 — `--now` is bounded, but not actually immediate

R1 made emergency shutdown bounded, which is good, but the sequencing can still be improved.

`abortDescendants()` currently:

1. awaits broker-backed handoff capture;
2. awaits all `agent.drain` RPCs;
3. only then calls local `child.stop()`.

Each broker operation can consume the shutdown RPC timeout.

Thus an unhealthy broker can delay local model cancellation before `--now` has even begun stopping the expensive work.

For an emergency stop, ordering should be inverted:

```text
set local draining flag
snapshot local state synchronously
START every local session.abort() immediately
START broker drain/cancel concurrently
enrich handoff snapshot concurrently
dispose at deadline
```

The broker is authoritative for durable lifecycle bookkeeping, but it should not sit in front of the local compute kill switch.

Also:

```ts
if (this.draining) return this.handoffSnapshots;
```

means a second `--now` cannot escalate an already-running graceful stop.

Use a shared `drainPromise` plus an escalation controller:

```text
graceful -> budget -> now
```

where a later stricter request tightens the current deadline.

---

# R2-8 — capture root termination from `agent_end`, not history

R1 currently determines abort at settlement by scanning backward through session entries for the latest assistant message.

Pi gives you a more precise primitive: each `agent_end` corresponds to the actual low-level run, and has `willRetry`; tests show retry sequences such as `[true, false]`.

I would store:

```ts
lastFinalRootOutcome
```

from:

```text
agent_end where willRetry == false
```

and consume that once at `agent_settled`.

This simultaneously solves:

* abort classification;
* terminal error classification;
* context/KV error classification;
* avoiding stale transcript inference.

Interestingly, the new R1 lifecycle test explicitly asserts that safe-agent-team has no `agent_end` handler.  For R2 I would reverse that choice: use `agent_end` for **observation only**, but retain `agent_settled` as the point where lifecycle decisions are committed.

That preserves the good R1 design.

---

# Two smaller lifecycle findings

**Managed child retention.** The runtime removes a child from `this.children` when startup fails, but normal terminal `ManagedChild.stop()` does not notify the runtime to remove itself. The broker remains the durable history, so the runtime map should represent only local live sessions.  I would add an idempotent `onChildStopped(child)` callback and delete only if the map still points to that instance.

**LCM diagnostics are currently thrown away.** The embedded host installs `onDiagnostic: (_diagnostic) => {}`.  Yet the embedded controller reports precisely the information you care about, including compaction failure.  Store a bounded last diagnostic on the child, surface it through `/agents`, and promote repeated critical context failures to the parent.

---

# LCM interop should get a small V2 hardening

There are two concrete issues.

Safe-agent-team currently looks only for:

```text
local-context-manager.embedded-context.v1
```

The current LCM's canonical name is now:

```text
pi-local-context-manager.embedded-context.v1
```

with the old name retained only as a compatibility alias.

Nothing is broken today, but safe-agent-team should prefer the canonical name and then fall back to the legacy alias.

More significantly, `EmbeddedContextController` starts from its own `DEFAULT_CONFIG` unless `options.config` is supplied.  Safe-agent-team supplies only mode + context window.

Thus the root's loaded LCM profile/configuration is not necessarily the policy children use.

For V2 I would let the LCM provider expose:

```ts
createEmbeddedContextManager(host, {
  mode,
  logicalContextWindow,
  effectiveContextBudget,
})
```

while **LCM itself injects its current effective configuration**. Safe-agent-team should not need to parse another extension's config files.

---

# Tests I would consider release-blocking for R2

1. **Child true overflow:** first call overflows, Pi compacts, retry succeeds; broker child stays logically running throughout and finishes normally.

2. **Child exhausted overflow recovery:** second overflow settles; child must become `blocked`/failed-with-diagnostic, never `ready + active`.

3. **Child compaction failure:** summarization itself fails; no repeated compaction loop on subsequent fabric messages.

4. **oMLX-style `prefill_memory_exceeded`:** classified as capacity, not context overflow; capacity relief retry occurs before compaction.

5. **Same-route local concurrency:** two or more Qwen workers cannot simultaneously exceed the configured route-capacity policy.

6. **Advertised 128K / effective 48K:** embedded controller sees 48K as its operational budget while model metadata still remains 128K.

7. **Root automatic compact-and-retry:** two `agent_start`s, only one broker `begin_turn`, one final `end_turn`.

8. **Root manual/LCM compaction:** fabric snapshot must return `sessionReplacementSafe=false`.

9. **Urgent child message during root compaction:** message stays durable and visible but does not start a competing root model call; it wakes the root after compaction.

10. **Root persistent capacity failure:** subsequent child results/clarifications do not repeatedly auto-wake the broken root provider.

11. **`/agents stop --now` with frozen broker:** local `session.abort()` begins immediately rather than after RPC timeout.

12. **Graceful stop escalated to `--now`:** second request tightens the existing stop rather than returning immediately.

---

## Recommended implementation order

I would make **R2-1 + R2-2 + R2-3** the first patch: introduce shared model-turn outcome tracking, make root logical turn-start idempotent, and make root compaction part of fabric quiescence.

Then implement **capacity-aware recovery** as a coherent feature rather than another set of error-message special cases: structured failure classification, same-route capacity arbitration, effective prefill budgets, and a root/child circuit breaker. The recent oMLX evidence makes this worthwhile: the same model may safely accept one context and reject a substantially shorter one depending on current memory state, so a token-count-only solution will never be robust. ([GitHub][1])

Finally I would clean up the smaller lifecycle/interop issues: immediate emergency-abort ordering, `agent_end`-based root outcome capture, live-child map cleanup, canonical LCM provider discovery, and diagnostic propagation.

### Overall R2 assessment

The **coordination layer itself is now strong** after R1. I would no longer focus the next hardening round primarily on locks, cancellation trees, or durable broker semantics.

The biggest remaining reliability boundary is now:

```text
model runtime / context state
          ↕
Pi retry + compaction
          ↕
safe-agent logical lifecycle
```

At present those layers are only loosely coupled.

The right R2 goal is therefore not “add more compaction.” It is:

> **Make context overflow, compaction failure, KV/prefill capacity pressure, transient provider failure, and genuine task completion first-class deterministic lifecycle facts—and apply the same recovery semantics to the root, children, and recursive parent agents.**

With that change, I think the extension would be substantially better positioned for long-running Qwen/oMLX/llama.cpp workloads rather than merely surviving the happy path.

[1]: https://github.com/jundot/omlx/issues/2865 "https://github.com/jundot/omlx/issues/2865"
[2]: https://github.com/jundot/omlx/issues/2841 "https://github.com/jundot/omlx/issues/2841"
[3]: https://github.com/NousResearch/hermes-agent/issues/60383 "https://github.com/NousResearch/hermes-agent/issues/60383"
