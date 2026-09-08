# Re-audit Feedback: `pi-safe-agent-team`

**Repository:** `SaehwanPark/pi-safe-agent-team`  
**Audited branch:** `main`  
**Audited HEAD:** `13a407c769388e3a1ce75db7b1353a8842d8032b`  
**Primary update reviewed:** PR #23, `fix/audit-feedback-hardening`  
**Audit date:** 2026-09-08

## Executive summary

The update resolves the major correctness and contract problems from the previous audit. The architecture now looks substantially closer to something I would be comfortable running continuously with `local-context-manager` and root-only web/browser/MCP/computer-use extensions.

The following prior findings are now materially fixed:

- embedded LCM compaction now maps to Pi's actual `AgentSession.compact(string | undefined)` contract;
- attached-but-unreadable fabric state fails closed as `active: true`, `quiescent: false`, `state: "uncertain"`;
- quiescence now includes root-running state;
- the public V1 fabric-state schema is synchronized between code and protocol docs;
- `sessionReplacementSafe` is explicit;
- `options.agentDir ?? getAgentDir()` is now canonical;
- child `contextMode` can be updated in broker state;
- arbitrary code/test execution is no longer falsely labeled read-only by the root shell classifier;
- `tee`, no-space redirection, and directory-changing shell chains are recognized more accurately;
- provider registration is conflict-aware;
- a reproducible isolated-Pi load smoke script now exists.

I found no new architectural reason to abandon the current design.

There are, however, several remaining hardening issues. The most important is a **path-scoped root-shell gap when only an active write fence remains after a mutable lease has lapsed**. I would fix that before calling the root shell preflight "fence-aware" in a strong sense.

## Resolution matrix from the prior audit

| Prior finding | Current status | Notes |
| --- | --- | --- |
| Embedded `AgentSession.compact()` object/string mismatch | **Fixed** | Host now passes `customInstructions ?? reason` as `string | undefined`. |
| Attached status failure reported inactive | **Fixed** | Returns active + uncertain + non-quiescent + unsafe. |
| Interop docs/schema mismatch | **Fixed** | `FabricStateSnapshotV1` is now aligned. |
| Root session replacement cancels descendants undocumented | **Fixed/documented** | LCM still needs to consume this correctly. |
| Root running omitted from quiescence | **Fixed** | Missing root record also fails non-quiescent. |
| CWD lowercase comparison on case-sensitive filesystems | **Mostly fixed** | Filesystem-aware folding is used, but the implementation re-probes by mutating the workspace on each snapshot. |
| `resource_granted` conditional state not passed | **Mechanically fixed, semantically redundant** | Broker-generated `resource_granted` is urgent, so urgent handling wins before the conditional logic. |
| Shell code/test execution falsely called read-only | **Fixed** | Such commands are now `unknown`, preserving the trusted-root escape hatch honestly. |
| No-space redirection / `tee` / `cd` gaps | **Fixed for covered cases** | Good regression expansion. |
| Canonical agent-directory resolver | **Fixed** | Legacy `PI_AGENT_DIR` removed from core resolution. |
| Context-mode diagnostics stale after degradation | **Mostly fixed** | Broker update exists, but degraded-manager disposal/reconnect synchronization can improve. |
| Interop provider overwrite | **Fixed** | Different provider instance under the same name now fails rather than silently replacing. |
| Fence diagnostic double count | **Fixed** | Snapshot uses broker active-fence count. |
| Isolated Pi smoke harness absent | **Partially fixed** | A load/startup smoke exists; joint LCM coverage can silently skip and it does not exercise a managed child. |

---

# Remaining findings

## P1 — Path-scoped root shell mutation can bypass an active write fence after the mutable hold disappears

### Current behavior

`evaluateRootShellGuard()` fetches:

- `childHolds`, derived from current mutable holds;
- `activeFences`, a count only.

It correctly avoids the fast-path return when `activeFences > 0`.

For a **broad** known mutator, the command is blocked.

For a **path-scoped** mutator, however, collision testing iterates only over `childHolds`. If the hold has disappeared but the short-lived write fence is still active, there is no path record to match and the function returns `undefined`.

### Concrete failure mode

This is exactly the edge case write fencing is designed to protect:

```text
1. Child owns mutable lease for src/parser.ts.
2. Child calls guarded write; resource.begin_write creates a write fence.
3. Lease expires while filesystem mutation is still in progress.
4. Coordinator releases the mutable hold, but the write fence remains active.
5. Root invokes:
     prettier --write src/parser.ts
6. root shell guard sees:
     childHolds = []
     activeFences = 1
7. Command is path-scoped.
8. No childHolds match because the lease already disappeared.
9. Root shell command is allowed despite the active fence.
```

Ordinary root `edit`/`write` remains safe because it goes through `resource.begin_write`, which checks fences. The gap is specifically the best-effort root shell layer.

### Minimal, low-friction fix

Because active fences are intentionally short-lived, the simplest safe behavior is:

```text
if activeFences > 0 and the command is a known mutator:
  block unless exact active-fence path identity is available and proven unrelated
```

That is conservative but should create little UX friction.

A more precise future design can expose active fence resource/path identities in `fabric.status` and compare them like mutable holds.

### Regression test

Add:

```text
activeFences = 1
childHolds = []
prettier --write src/parser.ts
=> blocked
```

Also test an unrelated path and explicitly choose/document whether the conservative policy blocks it.

---

## P1 — Fabric-state queries repeatedly mutate the project filesystem to probe case sensitivity

### Current behavior

`getFabricStateSnapshot()` calls:

```ts
detectCaseInsensitivePaths(this.cwd)
```

on every snapshot.

`detectCaseInsensitivePaths()` creates a mixed-case probe file in the target directory, tests a differently cased name, then deletes the file.

`evaluateRootShellGuard()` similarly calls a fresh probe after already retrieving `fabric.status`.

### Why this matters

LCM is expected to query fabric state at safe lifecycle boundaries. With both extensions always enabled, a nominally read-only state query can therefore repeatedly:

- synchronously create and delete `.pi-case-*` files in the repository;
- trigger IDE/file-watcher/build-system events;
- transiently affect `git status`;
- leave a probe behind if cleanup fails;
- perform synchronous I/O on frequent orchestration boundaries.

It also bypasses the spirit of the borrowing model: a coordination snapshot should not need an uncoordinated filesystem mutation.

### Recommended fix

Resolve case sensitivity once and cache it.

For example:

```ts
private caseInsensitivePaths?: boolean;
```

Populate from, in preference order:

1. explicit `FabricConfig.caseInsensitivePaths`;
2. broker/coordinator resolved configuration;
3. one fallback probe only if no resolved value exists.

For `evaluateRootShellGuard()`, `fabric.status.config.caseInsensitivePaths` is already available from the status request and should be preferred over a fresh filesystem probe.

### Acceptance test

Count calls to the probe helper and verify repeated snapshot/guard operations do not create new probe files after initialization.

---

## P1 — The isolated Pi smoke script can silently skip the LCM scenario and still report success

### Current script

The script defaults to:

```bash
LCM_DIR="${LCM_DIR:-/Users/saehwan/repos/local-context-manager}"
```

and executes the joint scenario only if that path exists.

On Fedora, another macOS account, CI, or another developer machine, that path normally does not exist. The script simply skips Scenario 2 and still prints:

```text
Isolated Pi smoke tests completed successfully.
```

### Failure mode

An external Codex/Antigravity implementation agent runs:

```bash
npm run smoke:pi
```

on Fedora and reports that safe-agent + LCM integration passed, when only safe-agent-team alone actually ran.

### Recommended fix

Make the modes explicit:

```text
smoke:pi              -> safe-agent load smoke only
smoke:pi:with-lcm     -> joint smoke; fail if LCM cannot be found
```

For joint mode:

- use `LCM_DIR` when supplied;
- otherwise try a sibling checkout such as `../local-context-manager`;
- if still absent, **fail with an actionable message** rather than silently skipping.

Use a separate disposable `PI_CODING_AGENT_DIR` per scenario.

Also remove the personal `/Users/saehwan/...` default from the public script.

---

## P1/P2 — The current smoke is a good extension-load test, not yet an end-to-end managed-child integration test

The use of `--list-models` is not meaningless: current Pi constructs the runtime and loads explicit extensions before handling `--list-models`, so the script does verify that the extension can be loaded in an isolated Pi process.

However, it does not prove:

- root fabric attachment with a usable selected model;
- actual child creation;
- `noExtensions: true` child construction;
- LCM embedded provider discovery by a child;
- `contextMode=lcm-embedded`;
- child state under `PI_CODING_AGENT_DIR`;
- quiescence behavior after child completion.

### Recommendation

Keep the current credential-free smoke as a fast **load smoke**.

Add an optional model-backed integration mode for developer machines:

```text
PI_SMOKE_MODEL=<configured model> npm run smoke:pi:integration
```

The mode should use the user's explicitly provisioned isolated auth/model fixtures and verify at least one real managed child lifecycle.

This is particularly useful for testing:

- Codex subscription routes;
- local Qwen provider entries;
- Apple Silicon vs. Fedora local-AI setups.

---

## P2 — Canonical-agent-directory test still does not instantiate a managed child

The updated test is better than before:

- it starts the broker;
- verifies state under the disposable directory;
- compares the real agent directory before/after.

But the title says:

> `PI_CODING_AGENT_DIR contains all state`

while no managed child session or child model runtime is created.

That leaves the most important derived paths unexercised:

```text
<state>/sessions/<child>
<agentDir>/auth.json
<agentDir>/models.json
```

### Recommendation

Either:

- narrow the test title to "broker/runtime state"; or
- add a fake-model child integration fixture and prove child session paths also remain isolated.

---

## P2 — Degrading from LCM to native mode should dispose the failed embedded manager

`degradeToNativeContext()` currently changes the mode and updates broker state, but leaves `embeddedManager` alive until child shutdown.

If the embedded manager owns timers, caches, listeners, or file handles, a failed provider can remain resident even though it is never used again.

### Fix

On first degradation:

```text
1. set contextMode = native;
2. best-effort dispose embeddedManager;
3. set embeddedManager = undefined;
4. best-effort update broker contextMode.
```

This also makes the state transition easier to reason about.

---

## P2 — Reconnect should carry the current `contextMode`

If degradation occurs while broker transport is unavailable:

```text
local child mode -> native
agent.update(contextMode=native) -> fails
```

The child later reconnects with `agent.register(...)`, but the reconnect registration currently omits `contextMode`.

The coordinator may therefore retain stale `lcm-embedded` diagnostics even though the live child is native.

### Fix

Include:

```ts
contextMode: this.contextMode
```

in reconnect registration.

This is a small change with useful diagnostic truthfulness.

---

## P2 — `resource_granted` conditional delivery logic is effectively dead under current broker semantics

The delivery classifier has a conditional path:

```text
resource_granted
  -> wake only if hasPendingRootRequest
```

But broker-generated internal messages are recorded with urgent priority, and `resource_granted` is an internal message type. `classifyRootDelivery()` handles urgent priority first, so a real broker `resource_granted` always steers/wakes regardless of the conditional state.

The added `hasPendingRootRequest()` broker status query therefore adds latency/complexity without affecting the real decision.

### Recommendation

Choose one clear contract.

The simpler current-semantics version is:

```text
resource_granted is an internal urgent wakeup
```

Then:

- remove the conditional root-request lookup;
- update the protocol table;
- keep the pure classifier simpler.

If conditional grants are desired instead, change broker priority semantics and correlate against the resource waiter rather than the ordinary message-request table.

---

## P2 — Common wrapper invocations remain trusted-shell bypasses

The shell classifier intentionally treats unknown commands as the trusted-root escape hatch. That is reasonable.

Be aware that common wrapper forms remain unknown, for example:

```text
npx prettier --write src
pnpm exec prettier --write src
uv run ruff format .
poetry run black .
python -m black .
```

This is not a contradiction if the docs continue to describe the shell guard as best-effort.

A useful incremental improvement would be to unwrap only a small set of obvious execution wrappers and reuse the inner-command classifier, without trying to build a shell interpreter.

---

# Cross-project implications for LCM

Once the LCM update is visible, verify these exact contracts:

```text
safe-agent-team.fabric-state.v1
  version = 1
  active
  quiescent
  state = known | uncertain
  sessionReplacementSafe
  activeWriteFences
  pendingRootRequests
  pendingRootDeliveries
  activeTasks
  mutableResources
  quiescenceReasons
```

LCM reset policy should use **`sessionReplacementSafe`**, not infer safety independently from individual counters.

For uncertainty:

```text
active=true
state=uncertain
sessionReplacementSafe=false
```

must fail closed for root session replacement.

For semantic compaction, threshold safety and semantic-fabric safety should remain separate:

```text
threshold pressure
  -> protect context even if workers remain active

semantic phase boundary
  -> defer while active fabric is non-quiescent/uncertain
```

---

# Recommended implementation order

- [ ] **P1:** close the path-scoped shell-vs-active-fence gap.
- [ ] **P1:** cache case-sensitivity policy; stop probing the filesystem on each snapshot/guard.
- [ ] **P1:** make joint LCM smoke explicit and fail if requested LCM checkout is absent.
- [ ] Add optional model-backed managed-child integration smoke.
- [ ] Extend/narrow canonical-agent-dir isolation test appropriately.
- [ ] Dispose failed embedded manager on degradation.
- [ ] Include `contextMode` on reconnect registration.
- [ ] Simplify `resource_granted` delivery semantics/documentation.
- [ ] Optionally unwrap common formatter/linter execution wrappers.

# Release assessment

The update is a clear improvement and fixes the previous audit's major blockers.

I would characterize the current state as:

```text
core fabric / borrowing / child isolation:
  strong

LCM interop host contract:
  strong enough for LCM implementation

root shell guard:
  useful best-effort protection, but one fence-specific correctness gap remains

isolated external-harness workflow:
  workable, but joint-test success reporting should be made stricter
```

I would fix the two P1 correctness/side-effect issues (active-fence shell gap and repeated case-probe mutation) before declaring the integration hardening complete.
