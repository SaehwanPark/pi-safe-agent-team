# Audit Feedback: `pi-safe-agent-team`

**Repository:** `SaehwanPark/pi-safe-agent-team`  
**Audited branch:** `main`  
**Audited HEAD:** `d25057c35ceb832924d443d8e4ef097406c60904`  
**Primary implementation reviewed:** merged PR #22, `feat/integration-hardening`  
**Audit date:** 2026-09-08  
**Companion architecture considered:** `local-context-manager`, `pi-mono-context-guard`, `pi-computer-use`, `pi-web-access`, `pi-chrome`, `pi-mcp-adapter`, and `@narumitw/pi-goal`.

## Executive summary

PR #22 is a substantial improvement. It implements most of the requested ecosystem-hardening architecture while preserving the project's most important invariant: managed children still use `noExtensions: true` and receive explicit guarded capabilities rather than ambient extension inheritance.

The new process-local interop mechanism, capability guidance, root delivery policy, and root-shell preflight are directionally strong. Several implementation defects remain, however, and two should be treated as release-blocking for the LCM integration:

1. the embedded-context host passes an object to Pi's `AgentSession.compact()` even though the current SDK accepts `customInstructions?: string`;
2. the fabric-state provider can report `active: false` when an attached fabric merely fails to answer a status query, which is unsafe for a consumer deciding whether session replacement is safe.

A third cross-project issue is lifecycle-critical: LCM's `/checkpoint-reset` replaces the root Pi session, and safe-agent-team currently cancels all managed descendants on root `session_shutdown`. This behavior should be made explicit in the protocol and tested with LCM's quiescence gate.

### Overall assessment

| Area | Assessment |
| --- | --- |
| Ownership/borrowing core | Strong |
| Child capability isolation | Strong |
| Canonical Pi directory intent | Good, minor precedence cleanup |
| Interop architecture | Good design, contract inconsistencies |
| Fabric quiescence | Promising, needs correctness fixes |
| Embedded LCM integration | **P0 adapter defect** |
| Root message scheduling | Good, one integration bug |
| Root shell preflight | Useful best-effort layer, several bypasses/overclaims |
| Test suite breadth | Good unit coverage |
| Reproducible isolated Pi SUT testing | Not yet codified in CI/scripts |

---

# What is already done well

## 1. `noExtensions: true` was preserved

This is the right decision.

Managed children still receive:

- guarded read-only filesystem tools;
- guarded edit/write;
- controlled shell behavior;
- safe-agent coordination tools;
- optional embedded context policy through a narrow adapter.

They do not inherit arbitrary root extensions.

That preserves the ownership/borrowing model and avoids duplicated or untracked mutation paths.

## 2. The process-local registry is an appropriate interoperability boundary

Using:

```ts
Symbol.for("pi.extension-interop.v1")
```

is a good fit for this architecture.

Both companion extensions run in the same Pi process, while the external developer harness (Codex CLI or Antigravity CLI) remains outside the SUT. No filesystem/network IPC is needed merely for extension cooperation.

## 3. Non-triggering root messages match current Pi semantics

The new delivery policy sends routine `progress`/`inform` messages with `triggerTurn: false`.

Current upstream Pi behavior supports this correctly:

- when not streaming, the custom message is appended to session/model state without starting a new turn;
- when streaming and `triggerTurn` is false, Pi defers the custom message until the turn boundary to preserve provider message ordering.

So the core idea—make informational fabric messages model-visible without waking the model—is sound.

## 4. Root write fencing remains strong

The existing edit/write path still:

- resolves real filesystem identity;
- coordinates through `resource.begin_write`;
- takes a write fence around the actual mutation;
- releases the fence after the tool result;
- fails closed on broker failure for coordinated writes.

This is the strongest part of the project and should remain separate from the intentionally best-effort root-shell classifier.

## 5. Capability asymmetry is now communicated explicitly

The new root and child instructions explain that web, Chrome, MCP, computer-use, and arbitrary root extensions are not inherited by managed children.

This should materially reduce tool hallucination and inefficient delegation, especially for smaller local Qwen models.

---

# Findings

## P0 — Embedded compaction calls Pi `AgentSession.compact()` with the wrong argument type

### Current implementation

The embedded host in `ManagedChild.start()` exposes:

```ts
compact: async (request) => {
  ...
  if (typeof this.session.compact === "function") {
    await this.session.compact(request);
  }
}
```

The call is hidden behind `any`.

### Upstream Pi contract

Current Pi `AgentSession` exposes:

```ts
async compact(customInstructions?: string): Promise<CompactionResult>
```

The object-shaped `EmbeddedCompactionRequest` is therefore not the SDK's argument type.

### Failure mode

When an embedded LCM controller eventually requests compaction, safe-agent-team can pass an object where Pi expects a string. Depending on downstream handling, this can:

- throw;
- stringify incorrectly;
- be treated as invalid custom instructions;
- cause safe-agent-team to degrade the child to native context mode.

The happy-path unit tests do not exercise the actual `ManagedChild -> AgentSession.compact()` adapter.

### Required fix

Make the interop contract match what the host can really perform.

For example:

```ts
export interface EmbeddedCompactionRequest {
  customInstructions?: string;
  reason?: string;
}
```

Then:

```ts
await this.session.compact(
  request.customInstructions ?? request.reason
);
```

If `targetTokens` cannot be honored by Pi's SDK, remove it from the provider contract rather than silently ignoring or forwarding it.

Eliminate `any` around this path.

### Required test

Instantiate the adapter with a fake session whose `compact` method is strongly typed as:

```ts
compact(customInstructions?: string)
```

Assert that an embedded request results in a string/undefined argument, never an object.

---

## P0 — Status-query failure is reported as “fabric inactive”

### Current behavior

`getFabricStateSnapshot()` returns:

```text
active: false
quiescent: false
```

both when:

- the runtime is genuinely stopped/unattached; and
- the runtime is attached but `fabric.status` throws.

Those states are not equivalent.

### Why this is dangerous

A context manager needs to distinguish:

```text
no fabric exists
```

from:

```text
fabric exists, but current state is unknown
```

A future LCM consumer can reasonably use:

```text
active == false
  -> standalone behavior
```

If a broker/status RPC transiently fails during active delegated work, the current provider can therefore accidentally tell LCM that no fabric exists.

For semantic compaction this can produce stale boundaries. For `/checkpoint-reset`, it is worse: replacing the Pi session causes safe-agent-team to cancel active descendants.

### Required fix

Fail closed on uncertainty.

Recommended states:

```text
unattached/stopped
  -> active=false, quiescent=false

attached + status succeeds
  -> active=true, quiescent=derived

attached + status fails
  -> active=true, quiescent=false
  -> optionally uncertainty/reason field
```

Adding:

```ts
state: "known" | "uncertain"
```

or bounded `quiescenceReasons` would make consumers safer.

### Required test

Mock an attached root whose `status()` throws and assert:

```text
active == true
quiescent == false
```

---

## P0 — The documented fabric-state protocol does not match the actual TypeScript contract

### Documentation currently describes

`PROTOCOL.md` documents a snapshot with fields such as:

```text
version
rootSessionId
cwd
activeAgents
runningAgents
nonTerminalChildTasks
activeMutableHolds
activeWriteFences
pendingRequestsToRoot
quiescenceReasons
```

### Actual `src/pi/interop.ts` exports

The implementation instead provides:

```text
active
quiescent
capturedAt
runningChildren
unresolvedChildTasks
mutableHolds
pendingRootRequests
pendingRootDeliveries
activeTasks[]
mutableResources[]
```

### Consequence

LCM is not implemented yet. Its implementer is likely to use the public protocol documentation as the contract and can build an incompatible consumer even though safe-agent-team's code is internally consistent.

This is exactly the kind of cross-project drift the versioned interop registry was intended to prevent.

### Required fix

Pick one canonical V1 interface and make all of these agree:

- `src/pi/interop.ts`;
- `PROTOCOL.md`;
- `ARCHITECTURE.md`;
- LCM implementation plan;
- tests.

Prefer exporting the exact public TypeScript shape in documentation.

Do not keep two “equivalent” V1 schemas.

---

## P0 cross-project — Root session replacement cancels managed descendants

This behavior is not newly introduced, but the new LCM integration makes it important to formalize.

### Current safe-agent lifecycle

On root `session_shutdown`:

```text
runtime.stop()
```

and `runtime.stop()` explicitly requests cancellation of each managed child and stops it.

### LCM behavior

`/checkpoint-reset` eventually calls Pi `newSession()`.

### Combined result

A reset while workers are active can terminate them.

### Recommendation for safe-agent-team

The primary gate belongs in LCM, but safe-agent-team should also:

- document this lifecycle consequence explicitly;
- expose sufficient quiescence/uncertainty information for LCM to gate safely;
- add a cross-project contract test or fixture describing the behavior;
- consider a bounded snapshot field such as `sessionReplacementSafe` derived conservatively from quiescence.

Do not silently change `session_shutdown` to preserve children across an unrelated root session without designing root identity/session ownership carefully. Cancellation is currently the safer lifecycle behavior; the reset command should respect it.

---

## P1 — Quiescence does not include the root's own running state

`PROTOCOL.md` says quiescence requires:

> Root must be active and not currently executing a turn.

The implementation computes quiescence from:

- running children;
- unresolved child tasks;
- mutable holds;
- pending root requests;
- pending root deliveries;
- fences.

It does not check the root agent's broker status.

### Failure mode

If a snapshot is requested while the root is running but children are idle, the provider may report `quiescent: true`.

This may be rare for LCM because LCM normally checks at settled boundaries, but the provider contract should be correct independently of the consumer's timing.

### Fix

Include root status:

```ts
const rootAgent = status.agents.find(a => a.id === status.rootId);

const rootBusy =
  rootAgent?.status === "starting" ||
  rootAgent?.status === "running";
```

Require `rootBusy === false` for quiescence.

If root state is missing/uncertain, prefer non-quiescent.

Add a regression test.

---

## P1 — CWD scope matching lowercases paths unconditionally

Current provider scope check effectively compares:

```ts
resolve(request.cwd).toLowerCase()
```

against:

```ts
resolve(this.cwd).toLowerCase()
```

### Problem

On a case-sensitive Linux filesystem:

```text
/repo/Foo
/repo/foo
```

can be different workspaces.

The provider treats them as the same scope.

This undermines the promise that process-global interop data cannot leak across project/session boundaries.

### Fix

Reuse the project's existing filesystem-case-sensitivity policy rather than hard-coding lowercase comparison.

At minimum:

- Windows / detected case-insensitive volume -> folded identity;
- case-sensitive volume -> exact canonical path.

Ideally reuse the same root identity/case-probe semantics already used by resource coordination.

### Tests

Add a Linux/case-sensitive test where differently cased paths do not match.

---

## P1 — `resource_granted` conditional wake logic is never supplied its context

`classifyRootDelivery()` supports:

```ts
resource_granted:
  triggerTurn = state.hasPendingRootRequest
```

The unit test verifies both cases.

But actual root delivery calls:

```ts
classifyRootDelivery(message)
```

with no `RootDeliveryContext`.

Therefore normal-priority `resource_granted` currently never triggers a root turn, regardless of whether it resolves a root request.

### Fix options

Either:

1. compute the pending-request relationship before classification and pass the state; or
2. derive it directly from message/request metadata in the pure classifier input.

Then add an integration-level test around `rootDelivery`, not only the pure function.

---

## P1 — Root shell classifier has several false-“read-only” and parsing gaps

The root shell guard is correctly documented as best-effort, not a sandbox. Still, some current classifications give stronger confidence than the implementation warrants.

### A. General code execution is marked read-only

The classifier marks commands such as:

```text
node
pytest
dotnet run
```

as read-only.

Those execute arbitrary project code, which can modify held repository files.

Other test/build commands can also run arbitrary hooks.

Because `evaluateRootShellGuard()` immediately permits `"read-only"`, this classification bypasses the warning/block layer.

### Recommendation

Reserve `"read-only"` for commands whose filesystem behavior is intrinsically observational.

Classify arbitrary code/test execution as:

```text
unknown
```

and document that unknown commands remain the trusted-root escape hatch.

If desired, add a separate semantic label:

```text
executes-project-code
```

rather than pretending it is read-only.

### B. No-whitespace redirection can bypass detection

The redirection regex recognizes forms such as:

```bash
echo x > src/a.ts
```

but not reliably:

```bash
echo x>src/a.ts
```

Since `echo` is otherwise read-only, this can escape the preflight.

Add tests for no-space redirections, descriptor variants, and quoted targets.

### C. Path-scoped mutations compare textual paths, not canonical identities

`evaluateRootShellGuard()` normalizes slashes and `./`, then compares strings.

This can miss equivalent paths involving:

- absolute paths;
- `cd subdir && ...`;
- `..`;
- symlink aliases;
- platform case folding.

Use the same policy-path identity machinery as guarded writes where possible. If a shell construct makes reliable path resolution difficult, conservatively classify it as broad while live child mutation exists.

### D. `tee`/similar writers are unknown

For example:

```bash
cat input | tee src/held.ts
```

is not recognized as a mutator by the current command classifier.

Given the documented best-effort model, unknown may remain allowed, but docs should not broadly claim that “pipe writes” are blocked unless the actual writer set supports that statement.

### Required test expansion

Add:

```text
echo x>src/a.ts
printf x>>src/a.ts
cat x | tee src/a.ts
node script-that-writes.js
pytest
absolute path formatter target
cd subdir && formatter --write file
```

The key is to test the classifier's **claimed protection boundary**, not to turn it into a full shell parser.

---

## P1 — The canonical-agent-directory isolation test does not prove its title/PR claim

`test/canonical-agent-dir.test.ts` is named as if it verifies:

> all child-derived state stays under `PI_CODING_AGENT_DIR` and `~/.pi/agent` remains untouched.

What it actually verifies is mostly:

- `runtime.agentDir === testDir`;
- `stateDirectory` starts under `testDir`;
- `stateDirectory` does not start under the default agent path.

It does not:

- spawn a managed child;
- create child session state;
- instantiate the child model runtime;
- start broker state;
- compare default-agent-dir contents before/after.

It even computes `realDirHasSafeAgents` without asserting it.

### Fix

Split into two tests:

**Pure resolver test**

- verifies precedence only.

**Real isolation integration test**

- disposable `PI_CODING_AGENT_DIR`;
- start runtime/broker if feasible;
- create representative session/broker state;
- ideally spawn a child with a fake/local model adapter;
- snapshot default `~/.pi/agent` relevant paths before/after;
- assert all new paths are under the disposable root.

The PR's manual smoke-test claim is useful, but codifying it will make future regressions much harder.

---

## P1 — Isolated Pi smoke tests are claimed manually but not reproducible from CI

PR #22 reports:

> Isolated smoke tests passed ... under disposable `PI_CODING_AGENT_DIR` with `--no-extensions`.

The repository CI currently runs `npm run check` and `npm run build` on Ubuntu, macOS, and Windows. There is no checked-in script/job that reproduces the Pi invocation.

### Recommendation

Add a script such as:

```text
scripts/smoke-isolated-pi.*
```

that runs the exact command line and is usable by Codex CLI or Antigravity CLI.

A credential-free smoke can simply verify extension load/startup. Model-backed cases can remain an opt-in local job.

This is more important than forcing paid-model credentials into GitHub Actions.

---

## P2 — Child `contextMode` can become stale after embedded-provider failure

The child registers with the broker using its initial:

```text
contextMode: "lcm-embedded"
```

If a later transform/turn callback throws, runtime sets:

```text
this.contextMode = "native"
```

but does not update the broker record.

`/agents` obtains `contextMode` from broker status, so diagnostics can continue showing `lcm-embedded` even after the live child has degraded.

### Fix

On degradation:

- update child broker metadata through an explicit bounded operation; or
- derive live diagnostics from the runtime child object when local.

Prefer an explicit coordinator update so reconnect/status remains truthful.

Test the state transition.

---

## P2 — Legacy `PI_AGENT_DIR` precedence still weakens canonical-directory semantics

Current constructor logic is equivalent to:

```text
options.agentDir
  -> PI_CODING_AGENT_DIR via getAgentDir()
  -> PI_AGENT_DIR
  -> getAgentDir()
```

So when `PI_CODING_AGENT_DIR` is absent but `PI_AGENT_DIR` is present, the legacy variable overrides Pi's canonical default.

That may be intentional backward compatibility, but it does not match a strict reading of:

> use Pi's canonical resolver as the default source of truth.

### Recommendation

Prefer:

```ts
this.agentDir = options.agentDir ?? getAgentDir();
```

If legacy `PI_AGENT_DIR` must remain supported, document it as a temporary migration alias and consider reading/migrating it outside the core resolver rather than maintaining two public directory contracts indefinitely.

---

## P2 — Interop provider registration silently overwrites conflicts

`registerInteropProvider(name, provider)` currently does:

```ts
registry.providers.set(name, provider)
```

If two copies/versions of an extension register the same V1 provider name, the later one silently replaces the earlier one.

### Recommendation

Make registration idempotent and conflict-aware:

- same provider reference -> no-op;
- empty name -> reject;
- existing different provider -> keep first or fail with a diagnostic;
- explicit unregister should remove only the provider instance that owns the registration, if practical.

This avoids load-order-dependent cross-extension behavior.

---

## P2 — Active fence count is likely double-counted in snapshot diagnostics

Snapshot logic computes roughly:

```text
status.activeFences + pendingRootFencesCount
```

The coordinator's `activeFences` already counts active fence records, including root fences created through `resource.begin_write`. `pendingRootFencesCount` appears to track the same in-flight root fence IDs at the host boundary.

This does not make quiescence unsafe—the result remains non-zero—but it can inflate diagnostics.

### Fix

Use coordinator fence count as canonical. Keep host pending-fence count only if it represents a distinct pre-registration window; if so, name and combine it explicitly without overlap.

---

# Documentation corrections

## 1. Make interop types canonical

Synchronize `PROTOCOL.md` with `src/pi/interop.ts`.

## 2. Correct quiescence claims

If the root's running status is not checked yet, do not claim it is part of the invariant until the implementation does so.

## 3. Tighten shell wording

Prefer:

> blocks recognized shell mutators while coordinated child writes are active

over:

> blocks pipe/redirection writes

unless every documented form is actually recognized.

## 4. Document session replacement semantics

Add:

> Root Pi session shutdown/replacement cancels managed descendants. Companion extensions that replace sessions must require fabric quiescence or an explicit destructive override.

That is an important contract for LCM and any future session-management extension.

---

# Recommended regression matrix

## Interop

- [ ] duplicate provider registration cannot silently replace an incompatible V1 provider;
- [ ] fabric provider scope is case-correct for the underlying filesystem;
- [ ] attached status failure -> active but non-quiescent/uncertain;
- [ ] root running -> non-quiescent;
- [ ] root ready + all child work terminal -> quiescent.

## Embedded context

- [ ] actual host adapter calls `AgentSession.compact(string | undefined)`;
- [ ] child context window is used, not root window;
- [ ] tool reduction failure degrades ManagedChild itself to native;
- [ ] broker `contextMode` updates after degradation;
- [ ] compaction is deferred while streaming/tool/write activity is in flight.

## Delivery

- [ ] `progress`/`inform` persist without waking root;
- [ ] urgent always steers;
- [ ] `resource_granted` resolving a root request wakes root;
- [ ] `resource_granted` unsolicited does not;
- [ ] acknowledgement still follows host acceptance.

## Shell

- [ ] no-space redirection;
- [ ] `tee`;
- [ ] arbitrary interpreter/test execution is not labeled intrinsically read-only;
- [ ] absolute path collision;
- [ ] `cd` + relative target collision;
- [ ] symlink/case-alias behavior follows policy identity.

## Isolation

- [ ] real disposable `PI_CODING_AGENT_DIR`;
- [ ] explicit `--no-extensions`;
- [ ] safe-agent alone;
- [ ] safe-agent + LCM;
- [ ] no new state under the user's default agent directory.

---

# Suggested fix order

- [ ] **P0:** fix embedded `AgentSession.compact()` adapter and contract.
- [ ] **P0:** make attached-but-uncertain fabric snapshots active/non-quiescent.
- [ ] **P0:** reconcile `PROTOCOL.md` interop schema with actual code.
- [ ] **P0 cross-project:** document/test that session replacement cancels descendants; coordinate with LCM quiescence gate.
- [ ] Add root-running state to quiescence.
- [ ] Fix case-sensitive CWD scope identity.
- [ ] Pass pending-request context into `classifyRootDelivery`.
- [ ] Harden shell classifier claims and path identity.
- [ ] Strengthen canonical-agent-dir isolation test.
- [ ] Check in isolated Pi smoke harness.
- [ ] Keep broker `contextMode` synchronized after degradation.
- [ ] Simplify/deprecate legacy `PI_AGENT_DIR`.
- [ ] Make interop provider registration conflict-aware.
- [ ] Remove active-fence diagnostic double-counting.
- [ ] Update docs/changelog after fixes.

# Release recommendation

PR #22 is a good architectural step and most of its direction should be retained.

Before treating v0.2.2-style integration as production-ready with LCM, I would fix these four items first:

1. embedded compaction type mismatch;
2. attached-but-uncertain fabric state;
3. documented-vs-actual interop schema mismatch;
4. reset/session-replacement lifecycle contract.

The remaining findings are mostly hardening and truthfulness improvements rather than reasons to redesign the system.

# Audit scope and verification note

This review inspected the merged PR #22 implementation, current `main` source, unit tests, protocol/architecture documentation, and the relevant current upstream Pi SDK behavior. PR #22 reports 111 passing tests and successful manual isolated smoke tests; those claims were considered as project evidence, but the smoke workflow is not yet codified in repository CI/scripts. The audit is therefore primarily static/code-level plus upstream API contract verification.
