I audited the current `main` at commit `c5dadda` (September 13, 2026), including the recent cancellation and interop fixes, and traced the root/child lifecycle through the Pi host, runtime, broker, coordinator, and tests.

The project is substantially stronger than during the earlier audits. The borrowing/fencing model, durable broker state, child isolation, message ordering, explicit task completion, recovery behavior, and LCM interop are generally well designed. The two newest fixes also address real prior problems: provider re-registration now happens on every `session_start`, and broker status/snapshot requests support `AbortSignal`.

However, your new abort requirement exposes **two important lifecycle holes**, one of which I would treat as release-blocking for the behavior you want.

## Executive assessment

| Severity       | Finding                                                                        | Current state                   |
| -------------- | ------------------------------------------------------------------------------ | ------------------------------- |
| **P0**         | Root run abort does not terminate descendants                                  | **Real gap**                    |
| **P0**         | A failed/terminal non-root parent can leave grandchildren running              | **Real gap**                    |
| **P0/P1**      | Emergency shutdown is sequential and potentially very slow                     | **Real gap**                    |
| **P1**         | No true hard-kill boundary for child shell/background processes                | **Architectural limitation**    |
| **P1**         | Current cancellation has no graceful `draining` phase                          | **Real gap**                    |
| **P1**         | Unknown root shell commands bypass coordination                                | **Earlier issue still present** |
| **P1/P2**      | `maxChildrenPerAgent=8` is a lifetime rather than concurrent limit             | **Likely practical UX failure** |
| **P2**         | Worktree cleanup is implemented but never invoked                              | **Resource accumulation risk**  |
| **P2**         | Session-scoped delivery/fence tails can survive asynchronously across shutdown | **Race/hygiene issue**          |
| Known boundary | Write fences disappear on independent broker crash                             | **Documented limitation**       |
| Fixed          | Interop provider disappears after session replacement                          | **Fixed by PR #28**             |
| Fixed          | Status/snapshot request ignores cancellation                                   | **Fixed by PR #27**             |

The first five are where I would focus next.

---

# 1. P0: aborting the root run does not currently abort children

This is the main issue corresponding to your practical observation.

Current lifecycle wiring is essentially:

```text
root agent starts
  -> agent_start
  -> root broker begin_turn

root agent finishes/interrupted
  -> agent_end
  -> agent_settled
  -> root broker end_turn("ready")

session itself closes/replaces
  -> session_shutdown
  -> runtime.stop()
  -> cancel children
```

`agent_settled` currently just sends:

```ts
agent.end_turn({ status: "ready" })
```

while descendant cancellation happens only in `runtime.stop()`, which is called from `session_shutdown`.

That distinction matters because Pi considers a model run and a session separate lifecycle scopes. `agent_settled` means there is no automatic retry/compaction/follow-up left, while `session_shutdown` is associated with quitting, reloading, `/new`, `/resume`, `/fork`, etc. An interrupted run therefore does not imply session shutdown. ([Pi][1])

Your lifecycle tests actually encode this behavior: one test verifies that `agent_settled` only finalizes the root turn, whereas `runtime.stop()` happens on `session_shutdown`.

### Concrete failure

Suppose:

```text
root GPT-5.6 Sol
 ├─ child A: Sol
 ├─ child B: Sol
 └─ child C
      ├─ grandchild D
      └─ grandchild E
```

You see subscription quota approaching its threshold and hit abort/Ctrl+C on the root generation.

The root model stops.

**A-E can continue generating.**

That is almost exactly the opposite of what one wants in a quota emergency: the visible expensive call stops while several invisible expensive calls remain active.

### Fix

Do **not** equate ordinary `agent_settled` with cancellation, because successful root turns are intentionally allowed to launch asynchronous children.

Instead, observe the cause of the finished root run:

```text
agent_end
  -> inspect final assistant stop reason
  -> remember whether the run was aborted

agent_settled
  if normal:
    root -> ready
    children continue

  if aborted:
    cascade-abort descendant tree
    root -> ready
```

The cancellation decision belongs at `agent_settled`, because Pi may perform automatic retry/compaction after a low-level `agent_end`. The extension should not kill children during an intermediate retry.

I would add a dedicated runtime API:

```ts
runtime.abortDescendants({
  reason: "root-aborted",
  mode: "graceful",
});
```

rather than abusing `runtime.stop()`, because the latter destroys the whole fabric attachment. After aborting a run, the user should still be able to talk to the root.

---

# 2. P0: a failed subagent can orphan its own descendants

This is a separate problem I found while tracing your new requirement.

`agent.cancel` is good: the coordinator recursively walks descendants, terminalizes them child-first, cancels pending requests, and releases runtime claims.

But ordinary terminal transition through `agent.end_turn()` does **not** cascade.

For a failed worker, `endTurn()`:

* marks that worker failed;
* releases that worker's resources;
* cancels that worker's requests;
* informs its parent;
* stops the local `ManagedChild`.

It does not cancel its children.

Therefore:

```text
root
  └─ researcher A
       ├─ worker B
       └─ worker C
```

If A hits an unrecoverable model/provider/session error, A becomes `failed` and its `ManagedChild` is stopped. B and C can remain live.

They now have a terminal parent. Messages back to A cannot work normally because normal messaging explicitly rejects terminal senders/recipients.

This gives you both an **orphan-agent problem and a cost leak**.

### Recommended invariant

I would make this a coordinator invariant:

> **No terminal agent may have a live descendant unless an explicit detach/reparent operation has occurred first.**

For V1, there is little reason to support detachment. So use simple cascading semantics:

```text
parent cancelled -> cancel descendants
parent failed    -> cancel descendants
parent completed -> require descendants terminal first
                    OR explicitly transfer them to parent/root
```

`cancelled` and `failed` should definitely cascade.

For `completed`, I prefer refusing the transition while live children exist rather than silently killing useful work. Something like:

```text
LIFECYCLE_CONFLICT:
agent cannot complete with 2 active descendants;
wait, cancel, or transfer them first
```

That gives much cleaner recursive-agent semantics.

---

# 3. P0/P1: `runtime.stop()` is not an emergency-stop implementation

The current stop path is safe-minded, but it is optimized for orderly shutdown rather than **stop spending now**.

It currently does roughly:

```ts
await root.client.request("agent.end_turn", ...);

for (const child of children.values()) {
  await root.client.request("agent.cancel", ...);
  await child.stop();
}
```

in sequence.

And each broker request has a default timeout of **60 seconds**.

Meanwhile `ManagedChild.stop()` itself awaits:

```ts
await session.abort();
```

without a deadline before disposing the session.

This creates a nasty worst case:

```text
broker/request hangs
  -> wait
  -> child 1 abort hangs
  -> wait
  -> only then child 2 gets told to stop
  -> ...
```

For cost control, stopping N children must not be serialized behind the least responsive child.

### Change the ordering

Emergency termination should prioritize **stopping computation first**, bookkeeping second:

```text
1. Freeze the fabric
   - reject new spawns
   - reject new begin_turn
   - reject new resource acquisition

2. Signal every live AgentSession.abort() concurrently

3. Simultaneously issue one recursive coordinator cancellation

4. Give cooperative shutdown a short bounded grace period

5. Dispose anything remaining

6. Kill owned OS process trees if any remain

7. Close broker/client resources

8. Persist final shutdown/handoff snapshot
```

A single recursive `agent.cancel` is already sufficient at the coordinator layer; its implementation recursively terminalizes the entire target subtree.

So there is no benefit in issuing a separate serialized broker cancellation for every entry in `runtime.children`.

For example:

```ts
const aborts = [...children.values()].map(child =>
  child.abortNow(reason)
);

const durableCancel = client.request(
  "agent.cancel",
  { agentId: topLevelChildId },
  shortTimeout,
  shutdownSignal,
);

await Promise.race([
  Promise.allSettled([...aborts, durableCancel]),
  hardDeadline,
]);

for (const child of children.values()) {
  child.disposeNow();
}
```

I would make shutdown requests use a special timeout measured in a few seconds, not the normal 60-second RPC timeout.

---

# 4. Graceful wrap-up needs to precede terminal cancellation

This part is subtle.

Today, `agent.cancel` immediately makes the whole subtree terminal. Once terminal, agents cannot use the normal message fabric to report a handoff.

So this sequence cannot work:

```text
cancel child
-> ask child to summarize what it was doing
```

The child is already dead from the coordinator's perspective.

You need a small **two-phase cancellation protocol**.

I suggest adding:

```text
ready/running/waiting/blocked
        |
        v
     draining
        |
        +------> cancelled
        |
        +------> completed
```

`draining` should be mechanically restrictive:

| During `draining`                          |     Allowed? |
| ------------------------------------------ | -----------: |
| New child spawn                            |           No |
| New task                                   |           No |
| New mutable borrow                         |           No |
| Start another ordinary model turn          |           No |
| Finish current atomic write/tool operation | Yes, bounded |
| Persist handoff/checkpoint                 |          Yes |
| Send final handoff                         |          Yes |
| Release resources                          |          Yes |
| Spawn new work because model “needs it”    |           No |

This prevents "please wrap up" from accidentally turning into another five-minute agent task.

---

# 5. For quota emergencies, the wrap-up should normally use **zero LLM tokens**

This is particularly important for your example.

When you are aborting because the subscription allowance is exhausted, asking every subagent:

> “Please summarize your current state before stopping.”

can itself consume a meaningful chunk of the remaining quota.

The safe-agent extension already knows most of what is needed mechanically:

* agent/task ID;
* task description and durable task status;
* workspace path;
* worktree branch/base;
* model route;
* leases and write fences;
* pending requests;
* recent durable messages;
* current session ID;
* last activity;
* completed task result if present.

So I would create a deterministic `HandoffSnapshot` from broker/runtime state.

Something like:

```ts
interface HandoffSnapshot {
  agentId: string;
  parentId?: string;
  task?: {
    id: string;
    description: string;
    status: string;
  };
  workspace?: {
    mode: "shared" | "worktree";
    path: string;
    branch?: string;
    baseRef?: string;
  };
  lastActivity: number;
  pendingRequests: string[];
  mutableResources: string[];
  reason: string;
}
```

Then model-generated wrap-up becomes an optional enhancement:

```text
graceful
  -> allow tiny final handoff turn if budget permits

budget-emergency
  -> deterministic snapshot only

now
  -> immediate abort, no semantic wrap-up
```

I would actually expose these as three user-facing modes:

| Mode                    | Behavior                                                        |
| ----------------------- | --------------------------------------------------------------- |
| `/agents stop`          | Short graceful drain, then abort                                |
| `/agents stop --budget` | No new model calls; checkpoint state, abort immediately         |
| `/agents stop --now`    | Abort everything immediately, best-effort bookkeeping afterward |

For your subscription-limit case, `--budget` is the important one.

---

# 6. Hard-kill guarantee currently does not exist for child processes

There is a limit to how strong the current in-process architecture can be.

Managed children are `AgentSession`s inside the same Node process. `session.abort()` provides cooperative cancellation.

But worktree children can receive `createLocalBashOperations()`.

If an agent launches something that creates its own long-running or detached OS process, disposing the `AgentSession` does not necessarily prove that descendant process is gone.

Examples include a test server, compiler daemon, watcher, background script, or something that deliberately daemonizes.

That produces:

```text
agent aborted
Node AgentSession disposed
broker says cancelled
BUT
python/node/server subprocess still alive
```

This is less about token spend and more about correctness and resource leakage, but it violates the semantics of "kill the subagent."

### Strong fix

Make every child shell invocation belong to a killable execution scope.

On POSIX:

```text
child AgentSession
  -> dedicated process group
       -> shell
          -> grandchildren
```

then escalation can terminate the whole process group.

On Windows, use the equivalent job-object/process-tree mechanism.

A simpler first version is:

* track every process PID started for each ManagedChild;
* reject obvious daemonization/backgrounding;
* terminate the tracked process tree during child shutdown.

If you eventually want an absolute isolation boundary, **subprocess-per-agent** is stronger than today's in-process `AgentSession`. I would not require that rewrite now, but I would design the shutdown API so a future backend can implement true `SIGKILL` semantics.

---

# 7. Earlier failure-mode audit: what remains

Several earlier concerns have been resolved well.

The recent session/provider lifecycle bug is fixed: the interop provider is re-registered from `session_start`, and there is now a regression test that exercises shutdown followed by another start.

The new `AbortSignal` plumbing through `BrokerClient.request()` is also structurally sound: aborted requests remove themselves from the pending map, clear their timer, and remove the abort listener.

The path/borrowing side remains quite strong. Child mutations authorize at the actual filesystem operation, symlink traversal is checked, and writes get fences around the mutation rather than relying only on prompt-level discipline.

There are, however, several remaining issues worth preserving from the earlier analysis.

### Root shell remains a meaningful escape hatch

This one is still present.

`evaluateRootShellGuard()` explicitly does:

```ts
if (risk.kind === "read-only" || risk.kind === "unknown") {
  return undefined;
}
```

meaning **unknown commands are allowed**, even when children are actively working.

The classifier deliberately returns `unknown` for things such as arbitrary npm scripts, `cargo test/build`, Go commands that execute code, and numerous other executable workloads.

Therefore:

```bash
npm run generate
python scripts/rewrite.py
make regenerate
./custom-build-tool
```

can potentially mutate a child-held file while bypassing the borrowing check.

Your README accurately describes root shell as a trusted escape hatch rather than a mechanically guaranteed sandbox.

I would keep the escape hatch, but invert behavior whenever live mutable coordination exists:

```text
no child mutable holds/fences
  unknown shell -> allow

child mutable hold/fence exists
  unknown shell -> block or explicit user confirmation
```

That is much safer without making ordinary solo usage annoying.

---

# 8. `maxChildrenPerAgent` is unexpectedly a lifetime quota

This is another concrete failure mode I did not like in the current implementation.

The default is:

```ts
maxChildrenPerAgent: 8
```

But the implementation increments `parent.childrenCreated` whenever a child is registered and never decrements it. Future spawning checks that cumulative number.

So a root can do:

```text
spawn #1 -> finishes
spawn #2 -> finishes
...
spawn #8 -> finishes
spawn #9 -> AGENT_LIMIT_REACHED
```

even with **zero active children**.

For a long Pi coding session, eight lifetime delegations is quite small.

I would split the concepts:

```ts
maxLiveChildrenPerAgent: 8
maxTotalLiveAgents: 32
```

and retain `childrenCreated` purely as telemetry/audit history.

If you want an abuse ceiling on cumulative creation, make that a distinct and much larger `maxChildrenCreatedPerSession`, so the configuration means what users expect.

---

# 9. Worktree lifecycle leaks artifacts

`GitWorkspaceStrategy` implements a proper `cleanup()` that removes a worktree and branch.

But the runtime stop paths do not call `workspaceStrategy.cleanup()`.

That means repeated worktree workers can accumulate:

* `.git/worktrees` entries;
* physical worktree directories;
* `pi-safe/...` branches.

I would **not** blindly delete dirty worktrees when cancelling; those are valuable recovery artifacts.

Better policy:

```text
clean completed worktree
  -> auto-clean

clean cancelled worktree
  -> auto-clean or short retention

dirty completed/cancelled worktree
  -> preserve
  -> expose in handoff snapshot
  -> garbage-collect later
```

An `/agents gc` command would be useful.

---

# 10. Broker-crash fencing remains a known correctness hole

Write fences are deliberately ephemeral and are not restored after independent broker restart. The type documentation and README both acknowledge this.

The bad case is narrow but real:

```text
child begins fenced write
      |
broker crashes
      |
broker restarts and forgets fence
      |
another actor is allowed to write
      |
original OS write has not actually finished
```

I still recommend a **recovery quarantine** rather than persisting every fence transaction:

```text
broker recovers from crash
  -> resources associated with formerly-active writers = uncertain
  -> temporarily reject conflicting writes
  -> clear quarantine after:
       writer re-registers and reconciles
       OR bounded liveness/lease timeout expires
```

That handles the uncertain-world problem more honestly than pretending an old fence can simply be replayed.

---

# Recommended shutdown architecture

I would make the next patch centered around one unified concept:

```text
                   ┌──────── normal root settles ────────> keep children
root run ends ─────┤
                   └──────── aborted ───────┐
                                             v
                                   BEGIN FABRIC DRAIN
                                             |
                          ┌──────────────────┴─────────────────┐
                          v                                    v
                 freeze new work                    deterministic snapshots
                          |                                    |
                          └──────────────────┬─────────────────┘
                                             v
                             abort all AgentSessions concurrently
                                             |
                                   short bounded grace
                                             |
                                  recursive broker cancel
                                             |
                                  kill owned processes
                                             |
                                      release/close
                                             |
                                    report handoff state
```

I would implement it in this order:

1. **Add coordinator-level descendant lifecycle invariant and a `draining` state.** Terminal failure/cancellation of a parent must not leave live descendants.
2. **Add `FabricRuntime.abortDescendants()` distinct from `stop()`.** It should freeze spawning and abort all live local sessions concurrently.
3. **Detect root abort at `agent_end` and act after `agent_settled`.** Normal root completion must continue to allow asynchronous workers.
4. **Make shutdown deadline-bounded.** Never serialize emergency stopping behind ordinary 60-second broker timeouts or an indefinitely awaited `session.abort()`.
5. **Add `/agents stop`, `--budget`, and `--now`.** `--budget` should be the quota-emergency path and perform zero additional LLM calls.
6. **Track and terminate child-owned OS process trees.** Otherwise "everything stopped" cannot be a strong guarantee.
7. **Add deterministic handoff snapshots and preserve dirty worktrees.** Semantic LLM summarization should be optional, not required for graceful recovery.
8. **Then address the remaining older issues:** unknown root-shell commands, cumulative child limit, worktree GC, and crash quarantine.

The most important design principle here is that **graceful shutdown should not mean “ask every model nicely to stop.”** It should mean that the deterministic control plane captures enough recovery state, prevents any new work, gives already-started atomic operations a very short safe boundary, then mechanically cancels computation. That is both more reliable and exactly what you want when the reason for cancellation is an impending usage/cost transition.

Overall, I would rate the current codebase as **structurally strong but not yet safe to assume “root abort = whole agent tree abort.”** The coordinator already contains most of the machinery needed to get there; the missing piece is making cancellation a first-class **tree lifecycle protocol**, rather than something that primarily happens during session teardown.

[1]: https://pi.dev/docs/latest/extensions?utm_source=chatgpt.com "Extensions · Documentation · Pi"
