## Round 4 audit

I audited current `main` at **`acc97f6a`**. This is meaningfully stronger than the R3 baseline: PRs #31–#34 landed the R3 fixes, including atomic turn finishing, root admission ordering, durable root deferred delivery, fail-closed reconnect reconciliation, and compaction reservation cleanup.

Current CI is also green on **Ubuntu, macOS, and Windows**.

My Round 4 conclusion is encouraging: **I no longer see another broad architectural flaw comparable to R1–R3.** The important remaining failures are now mostly crash-durability, long-running liveness, backpressure, and recovery-edge problems. There are still two findings I would fix before calling this robust for unattended local-agent work.

### Findings

| ID        | Severity                        | Finding                                                                  | Status                                                  |
| --------- | ------------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------- |
| **R4-1**  | **P1**                          | Child messages are ACKed before durable Pi-session acceptance            | Confirmed                                               |
| **R4-2**  | **P1**                          | Blocked children can renew mutable resource leases forever               | Confirmed                                               |
| **R4-3**  | P1 guarantee / P2 compatibility | Abort-shaped `error` outcomes can evade root descendant shutdown         | Defensive gap; historical upstream regression confirmed |
| **R4-4**  | P2                              | Reopening a blocked task may clear the gate but never wake the child     | Confirmed                                               |
| **R4-5**  | P2                              | Root quiescence undercounts deferred-but-unacknowledged messages         | Confirmed                                               |
| **R4-6**  | P2                              | Reconnect recovery drains only the first 100 inbox messages              | Confirmed                                               |
| **R4-7**  | P2                              | Broker journal grows without bound; heartbeats amplify fsync traffic     | Confirmed soak issue                                    |
| **R4-8**  | P2                              | Malformed configuration silently falls back to safer-looking defaults    | Confirmed                                               |
| **R4-9**  | P2                              | Local capacity inference depends on provider-name heuristics             | Design weakness                                         |
| **R4-10** | P2/P3                           | Normal root generation lacks a process-local capacity permit             | Broker-restart edge                                     |
| **R4-11** | P2                              | Detached/background worktree processes can outlive their agent           | Residual process-lifecycle gap                          |
| **R4-12** | P2                              | Critical broker-generated messages can be discarded when mailbox is full | Confirmed backpressure gap                              |

---

### R4-1 — Child message acknowledgement still has a crash-loss window

R3 fixed this correctly for the **root**, but the equivalent child path still has the old problem.

When a child is idle, `acceptMessage()` calls:

```text
enqueuePrompt(message)
ACK broker message
```

`enqueuePrompt()` merely appends work onto the in-memory `promptTail`. The broker ACK occurs immediately afterward.

Therefore this remains possible:

```text
broker durable message
        |
        v
child promptTail RAM
        |
        +---- ACK broker
        |
        X process crash
```

The prompt may never have entered the child Pi session, but the authoritative broker copy has already been acknowledged. Reconnect only recovers unacknowledged inbox messages, so this message is gone.

This deserves **P1** because it is silent coordination-message loss.

The root's new implementation already provides the right pattern: distinguish `delivering`, `accepted-but-not-durable`, and `persisted/acknowledged`; retain the broker copy until Pi has crossed a durable transcript boundary; and on recovery detect the message ID in the existing session before deciding whether to inject it again.

I would implement the same semantics for `ManagedChild`. The essential regression test is a forced crash after `enqueuePrompt()` but before the queued prompt starts; after restart the message must be delivered **exactly once logically**—neither lost nor duplicated.

---

### R4-2 — A blocked child can monopolize a mutable resource indefinitely

This is the most important long-running liveness problem.

`heartbeat()` extends the expiration of **every resource hold belonging to the agent**:

```text
expiresAt = now + leaseMs
```

including mutable holds.

At the same time, a provider/context failure intentionally leaves the child alive in `blocked` state so that it can later recover. Blocked is non-terminal, so normal terminal cleanup does not call `releaseAgentRuntime()`.

The child heartbeat timer also keeps running while blocked.

That gives:

```text
child acquires mutable file lease
          |
          v
model hits prefill/OOM/compaction failure
          |
          v
agent = BLOCKED
          |
          v
heartbeat every minute
          |
          v
lease extended forever
```

A sibling can consequently remain stuck forever even though the holder is doing no work.

This defeats the useful meaning of a lease: the holder is alive, but it is no longer making progress.

I would change resource retention to be status-aware. Entering `blocked` should release **mutable holds**, active write claims, and probably outstanding resource waiters immediately. Task ownership can remain intact. On explicit recovery, the child simply reacquires the resource before writing; your existing guarded-write layer already enforces that.

I would probably also avoid extending mutable holds for `waiting`. Shared/read holds are less dangerous and could follow a looser policy.

The critical test is a blocked child that owns a mutable resource while continuing to heartbeat: another actor must become able to acquire that resource without killing the blocked child.

---

### R4-3 — Abort detection should be hardened around the exact use case that motivated this work

Current root shutdown behavior is now good **when the final outcome is classified as `aborted`**: `agent_settled` performs the budget-mode descendant drain.

But `classifyAssistantMessage()` recognizes an abort only from:

```ts
stopReason === "aborted"
```

An `error` result containing an abort-specific error message is treated as an ordinary provider/runtime failure.

There is concrete upstream precedent for this. Pi issue #8409 documented a timing-dependent path in 0.84.2 where an aborted tool turn ended as:

```text
stopReason: error
errorMessage: This operation was aborted
```

rather than `stopReason: aborted`. The issue was closed as not planned.

Your dependency floor is now Pi `>=0.85.0`, so I would **not claim that this exact bug still reproduces on the current dependency**.  But given that rapid root abort is specifically a cost-control/safety invariant for this project, relying on one upstream representation is unnecessarily fragile.

Use an `abort-like` classifier with very narrow signatures, for example an explicit abort stop reason, `AbortError`/`ABORT_ERR`, or known messages such as `"This operation was aborted"`, while refusing to reinterpret real provider errors as user aborts. Better still, if Pi exposes an explicit abort signal/intention at the lifecycle layer, latch that independently of assistant-message classification.

For this particular feature I would optimize for **false negatives being much worse than false positives**: if the user definitely hit abort, descendants should die.

---

### R4-4 — Explicit task recovery can clear the block but never resume work

R3 correctly reconciles durable task state before clearing `blockedByOutcome`. The event path does this when a blocked task becomes active/ready:

```text
blockedByOutcome = undefined
drainPendingMessages()
```

That works if something accumulated in the child inbox while it was blocked.

If nothing did, however, `drainPendingMessages()` has nothing to deliver and **no model turn starts**.

So:

```text
provider failure
→ task blocked
→ human/root fixes condition
→ task reopen
→ local gate clears
→ ...nothing...
```

The worker can remain idle indefinitely.

A blocked → reopened transition should enqueue exactly one deterministic recovery turn when the session is idle:

> Task X was explicitly reopened. Resume from durable task state. Reacquire any required mutable resources before writing.

Use a recovery generation/epoch so simultaneous `task_changed` and `agent_updated` events cannot produce two wakeups.

---

### R4-5 — Deferred root messages don't participate fully in quiescence

R3 correctly stopped ACKing `nextTurn` root messages prematurely.

But `pendingRootDeliveriesCount` currently counts only states equal to `"delivering"`. Once a message is accepted into deferred state, it becomes `"accepted"` even though it remains broker-unacknowledged and has not necessarily been consumed by a root turn.

The fabric snapshot uses that counter to determine whether root deliveries prevent quiescence/session replacement.

Consequently:

```text
deferred coordination message exists
broker still has it unACKed
root has not consumed it
            |
snapshot may say:
pendingRootDeliveries = 0
sessionReplacementSafe = true
```

The data is no longer lost, which is good, but the **quiescence contract is wrong**.

All non-acknowledged root deliveries should count, including accepted-deferred messages and persisted messages awaiting ACK. Ideally the authoritative count should come from broker state rather than the host-side map.

---

### R4-6 — Reconnection only recovers 100 messages

Both managed-child recovery and root recovery issue one:

```text
message.inbox(limit: 100)
```

request.

But normal mailbox capacity is substantially larger than that. During a prolonged disconnect, more than 100 durable messages can accumulate.

After reconnect, the first 100 are recovered. The remainder do not necessarily produce new live `message_sent` events—they already happened while the connection was down—so they can simply remain in the broker indefinitely.

Recovery should drain inbox batches until a batch returns fewer than the maximum, ideally using a stable broker-sequence cursor. For fairness, yield between batches or cap each recovery cycle, but schedule another cycle until empty.

A 250-message offline test should be part of the fault suite.

---

### R4-7 — The broker journal needs lifecycle compaction

The journal is admirably careful about transaction framing and fsync:

```text
begin
events
commit
fsync
```

and it already has an atomic checkpoint implementation.

But I found no production caller of `Journal.checkpoint()`.

Every event-producing broker operation goes through `journal.append()`, including heartbeats.  A heartbeat itself always updates the agent, and if leases exist it updates those resources as well.

At the default one-minute heartbeat, 32 long-lived agents can generate roughly:

$$
32 \times 60 \times 24 = 46{,}080
$$

heartbeat transactions per day before counting actual work.

Each transaction is three journal records plus an fsync.

Over long-running development sessions this produces three undesirable trends: steadily growing `events.jsonl`, increasingly expensive broker startup/replay, and unnecessary synchronous disk pressure.

Use periodic checkpoints based on both transaction count and file size—for example every several thousand transactions or tens of MB—and checkpoint during clean broker shutdown. Once the checkpoint is durably renamed, the old log history is no longer necessary.

For POSIX power-loss durability, I would additionally fsync the containing directory after the checkpoint rename.

Heartbeat journaling can also probably be reduced: if an actor has no lease to maintain, persisting `lastActivity` every minute is not worth a synchronous transaction.

---

### R4-8 — Configuration errors currently fail invisibly

Adding an ordinary installed-extension configuration loader was an important R3 improvement. It now supports project and user config locations plus `PI_SAFE_AGENTS_CONFIG`.

But `loadFabricConfig()` returns an `errors` array when parsing fails, and `FabricRuntime` ignores it:

```ts
const loadedConfig = loadFabricConfig(...)
this.config = {
  ...loadedConfig.config,
  ...options.config
}
```

Consider a local oMLX user intending:

```json
{
  "modelRoutePolicies": {
    "omlx/qwen": {
      "maxConcurrent": 1,
      "effectivePrefillBudget": 48000
    }
  }
}
```

A JSON typo can silently eliminate the entire policy and continue using defaults. That's especially undesirable for settings whose purpose is preventing local-backend memory failure.

For an explicitly named `PI_SAFE_AGENTS_CONFIG`, malformed/missing configuration should fail closed.

For auto-discovered project/user files, at minimum show a persistent warning. I would lean toward refusing to activate the fabric when a discovered config exists but cannot be validated, because silently relaxing a coordination/memory policy is the more surprising behavior.

---

### R4-9 — Backend capacity needs an identity stronger than `provider/model`

The current key is:

```text
provider/model
```

and automatic "local provider" detection depends on names containing strings such as `omlx`, `llamacpp`, `lmstudio`, or `ollama`.

This is useful as a zero-config heuristic, but it cannot be the final resource identity.

A provider called:

```text
qwen-home
openai-compatible
my-local
```

may point to exactly one constrained local GPU while escaping the automatic serialization rule.

The reverse is also possible: two independent inference servers can expose the same provider/model name even though they have completely independent memory capacity.

I would eventually introduce an explicit:

```text
capacityGroup / runtimeKey / backendId
```

orthogonal to the semantic model route.

Then two different aliases targeting one Ryzen/MLX/llama.cpp runtime share a capacity bucket, while two genuinely separate machines do not.

Keep provider-name inference as a convenient fallback.

---

### R4-10 — Normal root generation still doesn't occupy the local arbiter

Managed children use both coordinator admission and the process-local `ModelRouteCapacityArbiter` around actual provider generation. Root normal generation now correctly awaits broker admission, but it does not hold a corresponding local arbiter permit for the duration of its provider call.

Normally the broker remains authoritative, so that is fine.

It becomes relevant during **external-broker restart**. Broker recovery marks live actors failed/reconnectable and releases their runtime claims.  If the root provider request is still running in a separate host process, it may reconnect as ready; another local child can then be admitted, and nothing in the process-local arbiter records that the root is still consuming the backend.

This is P3 for the normal single-process deployment, but P2 if you support separated/embedded broker topology.

The clean symmetry is:

```text
root:
broker admission
→ local route permit
→ provider generation
→ release local permit
→ broker end_turn
```

using the same broker-before-local lock ordering adopted for compaction.

---

### R4-11 — Process-tree cleanup is better than before, but detached daemons remain

One earlier concern has actually improved upstream: Pi's local Bash implementation explicitly kills the **active process tree** when its AbortSignal fires.

So I would retire the broad claim that aborting a foreground Bash tool leaves its subprocess tree running.

The residual case is narrower:

```text
nohup server &
some-daemon --fork
dev-server & disown
```

If the command has already returned, there is no active Bash invocation left to abort. Safe-agent currently gives trusted worktree children standard `createLocalBashOperations()` and keeps no per-agent process registry.

Those background processes can therefore survive `/agents stop --now`.

For a strong quota-emergency/cleanup guarantee, either disallow background/daemonized shell commands by default in managed worktrees, or give every agent an OS-level process ownership container—POSIX process group/cgroup and Windows Job Object being the ideal forms.

---

### R4-12 — Critical internal notifications lose against mailbox exhaustion

Broker-internal notices such as task/result/resource notifications eventually use the same mailbox machinery. If the recipient mailbox is full, `sendInternalMessage()` catches `MAILBOX_FULL` and emits a diagnostic instead of preserving the critical message.

The underlying semantic task state remains durable, so this is not state corruption. But the parent may never learn that the state changed without polling.

This becomes more plausible precisely when a root has been degraded/disconnected and coordination traffic is accumulating.

I'd reserve some mailbox capacity for control-plane messages or maintain a separate bounded control lane for `task_result`, `agent_failed`, `blocked`, `resource_granted`, responses to outstanding requests, and cancellation notices. Another option is state-coalescing rather than message accumulation: one latest durable notification per `(type, entity)`.

---

## R3 fixes that survived Round 4

Several areas now look good enough that I would stop redesigning them.

Terminal task facts correctly dominate late model failures, and `agent.finish_turn` commits task, agent, and parent-notice effects inside one coordinator transaction.  The duplicate public `prompt()` retry is gone; the tests now explicitly require only one prompt attempt.  Root provider admission is actually awaited before Pi continues.

The LCM integration is also now real rather than aspirational: current `pi-local-context-manager` explicitly accepts `logicalContextWindow`, `effectiveContextBudget`, and `effectivePrefillBudget`, and its threshold computation uses the effective budget.

The future-version interop clobbering problem was fixed as well: safe-agent now preserves an unknown future registry and uses a private v1 registry rather than overwriting foreign state.

### What I would implement for R4

1. **Fix R4-1 and R4-2 first:** make child message ACK transactional with durable session acceptance, and release mutable runtime claims when an agent becomes blocked.
2. Harden root abort recognition, then make explicit `task.reopen` produce one deterministic recovery wake.
3. Make deferred root deliveries participate correctly in quiescence and drain reconnect inboxes completely.
4. Add journal checkpoint/compaction and reduce heartbeat write amplification; simultaneously make malformed safety configuration visible/fail-closed.
5. Introduce an explicit backend-capacity identity, then make root generation occupy the local permit as children already do.
6. Finish process lifecycle/backpressure hardening: owned process groups for worktree children and reserved/coalesced control-plane mailbox capacity.
7. Add a **fault/soak suite**, not merely more ordinary unit tests: kill the broker/process between every meaningful persistence boundary, queue hundreds of messages during disconnection, leave workers blocked for simulated hours, restart brokers during generations, and execute tens or hundreds of thousands of coordinator transitions. Also put `smoke:pi:with-lcm` into CI; the current regular CI still concentrates on `check` and `build`, while the safe-agent/LCM contract has become important enough to test jointly.

## Round 4 verdict

The trajectory is good:

**R1:** orchestration safety
**R2:** model/context failure handling
**R3:** transaction boundaries
**R4:** crash durability and long-duration liveness

I would **not introduce another large abstraction layer at this point**. The coordinator/runtime split is holding together. The best return now comes from making the existing state machines survive hostile timing.

The two things I would still regard as release-blocking for a system expected to run unattended for hours are **R4-1 child-message durability** and **R4-2 blocked-agent lease retention**. Everything after those is increasingly hardening rather than a fundamental design correction.

Once those R4 items are addressed, a Round 5 audit should probably change methodology: rather than another static review, I would treat it as a **failure-injection + long-running soak audit** and deliberately try to kill/restart/stall components at adversarial boundaries. That is now much more likely to find the next real problems than another ordinary source-code pass.
