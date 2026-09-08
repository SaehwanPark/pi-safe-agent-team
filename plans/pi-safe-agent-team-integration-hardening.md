# pi-safe-agent-team: Ecosystem Integration Hardening Implementation Plan

Repository: `SaehwanPark/pi-safe-agent-team`  
Target branch baseline: current `main` audited 2026-09-08  
Primary companion: `SaehwanPark/local-context-manager`  
Assumption: this is the only subagent/orchestration extension visible to the Pi harness.  
Expected always-on root extensions: `pi-mono-context-guard`, `pi-computer-use`, `pi-web-access`, `pi-chrome`, `pi-mcp-adapter`, `@narumitw/pi-goal`, and `local-context-manager`.  
Expected models: OpenAI Codex subscription models (GPT-5.6 Luna/Sol, GPT-6 Astra) and local Qwen 3.8 Flash Next / Qwen 3.8-27B.

## 1. Purpose

Make `pi-safe-agent-team` the unambiguous, low-friction orchestration layer for diverse agentic-development projects while preserving its strongest design property: managed children run in a deliberately constrained capability environment.

The implementation must address five integration issues:

1. Managed children use `noExtensions: true` and therefore miss root context-management benefits.
2. The root cannot currently expose a deterministic "fabric is quiescent" signal to context-management extensions.
3. Every delivered root message currently requests a model turn, which can create unnecessary turn churn and interfere with goal-driven continuation.
4. Root `edit`/`write` mutations are coordinated, but root shell commands can still mutate child-held files.
5. Root models need explicit guidance that web/browser/MCP/computer-use capabilities are root capabilities unless separately brokered; otherwise they may delegate impossible tasks.

Do not solve these by enabling arbitrary extension inheritance in children.

---

# 2. Current implementation anchors

Preserve the current strengths.

## `index.ts`

Already provides:

- ordered root message delivery;
- durable acknowledgement after host acceptance;
- root `edit`/`write` interception;
- short-lived write fences;
- lifecycle generation handling;
- broker start/stop coordination;
- root session registration;
- `/agents` diagnostics.

## `src/pi/runtime.ts`

Already provides:

- explicit managed child `AgentSession`;
- `noExtensions: true`;
- guarded read-only and mutation tools;
- child-specific model/provider/thinking routing;
- worktree/shared-workspace strategies;
- durable broker identity/reconnect;
- serialized child prompt/message delivery;
- bounded child lifecycle;
- bootstrap coordination instructions.

## `src/pi/guards.ts`

Already provides:

- real-path workspace identity;
- symlink/junction escape protection;
- child write authorization at the operation performing the write;
- write fencing;
- shared-workspace read-only shell;
- explicitly trusted worktree shell;
- root `edit`/`write` guard.

## `src/core/types.ts`

Already has:

- explicit lifecycle;
- tasks;
- resources/borrows/leases;
- requests;
- durable messages;
- message priority;
- bounded fabric configuration.

Build integration on top of these rather than adding an independent orchestration layer.

---

# 3. Non-goals

Do not:

- set `noExtensions: false` for managed children;
- load arbitrary root npm extensions into children;
- allow child extensions to bypass guarded FS/shell tools;
- make safe-agent-team depend directly on local-context-manager;
- promise that arbitrary root shell commands are fully sandboxed;
- suppress clarification, escalation, failure, or required-result delivery merely to save turns;
- make browser/MCP/computer-use capabilities implicitly available to children;
- create a second task/resource truth outside the broker;
- add a second subagent orchestration mechanism.


## 1.1 Development and test execution contract

Implementation is expected to be performed by an **external coding harness** such as Codex CLI or Antigravity CLI. The coding harness must not rely on a normal Pi process in which the user's installed extensions are already active.

Treat Pi as the **system under test (SUT)**:

```text
Codex CLI / Antigravity CLI
  |
  +-- edits this repository
  +-- runs npm/unit tests directly
  |
  `-- spawns short-lived isolated Pi processes
        |
        +-- global extension discovery disabled
        +-- only explicit extension entrypoints loaded
        +-- disposable Pi state for full-isolation tests
        `-- may run real Pi/model turns when an integration test requires them
```

A passing integration test must not depend on a globally installed extension, skill, prompt template, theme, or project context file unless that resource is explicitly part of the test.

### A. Default extension-isolated Pi invocation

Safe-agent-team alone:

```bash
pi \
  --no-extensions \
  -e "$SAFE_AGENT_REPO/index.ts"
```

Safe-agent-team + LCM:

```bash
pi \
  --no-extensions \
  -e "$SAFE_AGENT_REPO/index.ts" \
  -e "$LCM_REPO/src/index.ts"
```

Use explicit development entrypoint paths. Do not install the development checkout into normal Pi settings merely to test it.

### B. Strict deterministic invocation

For tests where ambient prompt resources could affect model behavior:

```bash
pi \
  --no-extensions \
  --no-skills \
  --no-prompt-templates \
  --no-themes \
  --no-context-files \
  -e "$SAFE_AGENT_REPO/index.ts" \
  -e "$LCM_REPO/src/index.ts"
```

Keep Pi built-in tools enabled unless the test explicitly requires otherwise.

### C. Full Pi-state isolation

Use a disposable canonical Pi agent directory:

```bash
TEST_PI_DIR="$(mktemp -d)"

PI_CODING_AGENT_DIR="$TEST_PI_DIR" \
pi \
  --no-extensions \
  --no-skills \
  --no-prompt-templates \
  --no-themes \
  --no-context-files \
  -e "$SAFE_AGENT_REPO/index.ts" \
  -e "$LCM_REPO/src/index.ts"
```

If a model-backed test requires existing credentials or a custom model definition, provision only the minimum required opaque files into the disposable directory. Do not copy `settings.json`, installed-extension state, package state, or unrelated project configuration.

Never print, parse, modify, or commit credential fixtures.

### D. Mandatory P0: use Pi's canonical agent directory

The current runtime must not maintain a separate config-directory convention for managed children.

Replace behavior equivalent to:

```ts
options.agentDir ??
process.env.PI_AGENT_DIR ??
join(homedir(), ".pi", "agent")
```

with Pi's canonical resolver:

```ts
import { getAgentDir } from "@earendil-works/pi-coding-agent";

this.agentDir =
  options.agentDir ??
  getAgentDir();
```

or an equivalent implementation that is explicitly proven to honor `PI_CODING_AGENT_DIR`.

Requirements:

- `PI_CODING_AGENT_DIR` must affect root Pi and all managed child sessions consistently.
- `options.agentDir` remains the highest-precedence explicit dependency-injection override for tests.
- do not introduce or document `PI_AGENT_DIR` as a competing public setting;
- remove legacy `PI_AGENT_DIR` behavior unless backward compatibility is intentionally retained as a lower-priority deprecated alias;
- if retained temporarily, precedence must be:
  `options.agentDir` -> canonical Pi `getAgentDir()` -> no independent hard-coded branch.
- child `auth.json`, `models.json`, session directories, broker state roots, and any other agent-directory-derived paths must resolve from the same canonical test directory when full isolation is enabled.

Add a regression test that sets `PI_CODING_AGENT_DIR` to a temporary directory and proves that no safe-agent child state is created under the real `~/.pi/agent`.

This is a release-blocking requirement for the isolated Codex/Antigravity development workflow.

### E. Test pyramid

Prefer:

```text
many pure/unit tests
  -> coordinator/runtime tests
  -> fake LCM-provider tests
  -> spawned-child tests with fake model/session adapters where possible
  -> a small number of real isolated Pi process tests
  -> selected model-backed smoke tests
```

Do not use expensive LLM-backed runs for deterministic routing, classification, quiescence, or guard logic.

### F. Required isolated smoke matrix

Before completion, run:

```text
1. safe-agent-team alone
2. local-context-manager alone
3. safe-agent-team + local-context-manager
4. safe-agent-team + local-context-manager
   + pi-mono-context-guard + @narumitw/pi-goal
```

Load every companion explicitly with `-e`; never rely on global discovery in this matrix.

Browser/web/computer-use/MCP extensions should receive selected compatibility smoke tests only where root-vs-child capability boundaries are being verified.

### G. Process-local interop implication

`Symbol.for("pi.extension-interop.v1")` is intentionally process-local and is correct for this architecture:

```text
external coding harness
        |
        `-- isolated Pi process
              +-- safe-agent-team
              +-- local-context-manager
              `-- shared process-local interop registry
```

Managed children are constructed by safe-agent-team inside that Pi runtime. Do not replace this narrow registry with disk/network IPC merely because Codex CLI or Antigravity itself is a separate process.

---

# 4. P0: Publish a deterministic fabric-state/quiescence provider

## 4.1 Goal

Allow context-management extensions to determine whether a root semantic checkpoint/reset represents a genuine coordination boundary.

Publish through the same optional process-local interop registry used by LCM.

## 4.2 Registry

Use:

```ts
const PI_EXTENSION_INTEROP = Symbol.for("pi.extension-interop.v1");
```

Provider name:

```text
safe-agent-team.fabric-state.v1
```

No direct import of LCM.

## 4.3 Public provider interface

Suggested structural interface:

```ts
export interface FabricSnapshotRequest {
  cwd: string;
  sessionId?: string;
}

export interface FabricStateSnapshotV1 {
  active: boolean;
  quiescent: boolean;
  capturedAt: number;

  runningChildren: number;
  unresolvedChildTasks: number;
  mutableHolds: number;
  pendingRootRequests: number;
  pendingRootDeliveries: number;

  activeTasks: Array<{
    id: string;
    status: string;
    owner?: string;
    description?: string;
  }>;

  mutableResources: Array<{
    id: string;
    path?: string;
    holder?: string;
  }>;
}
```

Keep detail lists bounded.

Never expose:

- auth tokens;
- broker credentials;
- full message bodies;
- arbitrary tool output;
- child private prompts.

## 4.4 Quiescence definition

Make `quiescent` deterministic and conservative.

Recommended rule:

```text
quiescent = true only when ALL are true:

- no child is starting or running;
- no unresolved child-owned task is pending/ready/active/waiting/blocked;
- no mutable resource hold is live;
- no root-directed request remains pending;
- no accepted-but-undelivered root fabric message remains;
- no root write fence or child write fence is currently in flight.
```

Terminal child tasks (`completed`, `failed`, `cancelled`) do not prevent quiescence.

If a child is "ready" but owns an unfinished task, the fabric is not quiescent.

This intentionally answers:

> "Is coordinated delegated work resolved enough to call the episode complete?"

not merely:

> "Is a model streaming right now?"

## 4.5 Scope

The provider must reject cross-session/cross-workspace leakage.

When request `cwd` or `sessionId` does not match the attached root:

- return inactive/null;
- never return another project's fabric state.

## 4.6 Lifecycle

Registration should be idempotent.

After shutdown:

- provider may remain registered;
- it must return inactive while no root is attached.

During reconnect:

- return the best durable snapshot available;
- if state certainty is unavailable, prefer `quiescent: false`.

---

# 5. P0: Consume LCM's embedded child context policy

## 5.1 Goal

Give long-lived managed children context protection without enabling root extension inheritance.

Discover:

```text
local-context-manager.embedded-context.v1
```

Provider absence is normal.

## 5.2 Child runtime behavior

Keep:

```ts
noExtensions: true
```

unchanged.

After creating the child session and guarded tools, optionally instantiate the embedded context policy.

Desired child stack:

```text
ManagedChild
  + guarded read/grep/find/ls
  + guarded edit/write
  + guarded shell
  + coordination tools
  + optional embedded context policy
```

Not:

```text
ManagedChild
  + globally installed extensions
```

## 5.3 Safe child feature subset

When requesting embedded mode, explicitly select managed-child semantics.

Expected enabled capabilities:

- context telemetry;
- adaptive thresholds;
- tool-result reduction;
- evidence-completeness markers;
- threshold compaction at safe boundaries.

Expected disabled capabilities:

- checkpoint-reset files;
- handoff commands;
- semantic cold-memory reset;
- unrelated slash commands;
- root TUI status;
- arbitrary extension loading.

## 5.4 Integration adapter

Implement a small adapter owned by safe-agent-team.

Suggested lifecycle mapping:

```text
before child model prompt
  -> context.observeTurnStart()

after child AgentSession.prompt() fully settles
  -> context.observeTurnEnd()
  -> context.observeSettled()
```

Because managed child tool definitions are constructed directly by safe-agent-team, wrap their `execute` methods through the embedded controller so every large child tool result can be transformed before it is retained by the child session.

Pseudo-shape:

```ts
function wrapToolWithContextPolicy(
  tool: ToolDefinition,
  context: EmbeddedContextManager,
): ToolDefinition {
  const execute = tool.execute;

  tool.execute = async (...args) => {
    const result = await execute(...args);
    return context.transformToolResult({
      toolName: tool.name,
      input: args[1],
      content: result.content,
      details: result.details,
      isError: /* derive from result */,
    });
  };

  return tool;
}
```

Do not double-wrap.

Do not alter broker coordination result semantics.

## 5.5 Safe compaction boundary

The child host remains authoritative about when compaction is legal.

Never initiate an embedded compaction while:

- `session.isStreaming` is true;
- a guarded write operation is in flight;
- a tool call is still resolving.

Preferred point: after `session.prompt()` settles and before the next queued prompt begins.

Native Pi context protection remains the fallback during a single long tool loop.

## 5.6 Failure behavior

If the embedded provider:

- is absent;
- is incompatible;
- throws;
- fails during compaction;

then:

- emit a bounded broker diagnostic/debug message;
- mark child context mode as native;
- continue the task;
- do not terminalize the child merely because LCM integration failed.

Context optimization is not task correctness authority.

## 5.7 Diagnostics

Expose in `/agents` output:

```text
agent-4 [running] implementer openai/gpt-5.6-sol
  workspace=worktree
  context=lcm-embedded
```

or:

```text
context=native
```

Detailed status should also expose the advertised context window and whether current usage is reported/estimated when supplied by LCM.

---

# 6. P0: Reduce unnecessary root model-turn injection

## 6.1 Current issue

Root delivery currently sends every fabric message with `triggerTurn: true`.

This is safe but can make a chatty fabric repeatedly wake the root model, especially under `@narumitw/pi-goal`.

The fix must reduce only clearly informational traffic first.

## 6.2 Centralize delivery policy

Create a pure function:

```ts
interface RootDeliveryDecision {
  triggerTurn: boolean;
  deliverAs: "steer" | "followUp";
  modelVisible: boolean;
  display: boolean;
}

function classifyRootDelivery(
  message: AgentMessage,
  state: RootDeliveryContext,
): RootDeliveryDecision
```

Unit-test it exhaustively.

## 6.3 Conservative first policy

Always trigger immediately:

- `priority === "urgent"` -> `steer`;
- `clarification`;
- `decision_request`;
- `escalation`;
- `blocked`;
- `resource_request`;
- `request`;
- `response`;
- `agent_failed`;
- `cancel`;
- `steer`.

Normally trigger a follow-up turn:

- `result`;
- `task_result`;
- `handoff`.

Do not trigger a turn by default:

- `progress`;
- routine `inform`;
- unsolicited `resource_granted` that does not resolve a pending root request.

This conservative first pass captures most avoidable chatter without risking lost decisions.

## 6.4 Non-triggering message requirements

A non-triggering message must still:

- be displayed appropriately;
- remain durably available from broker inbox/history;
- be acknowledged only after the host has accepted it;
- be visible to the root model on the next natural turn, if Pi supports non-triggering `sendMessage` context insertion.

Add an integration test for this exact Pi behavior.

If Pi does not preserve a non-triggering custom message in later model context, use:

```text
UI display now
+
durable broker acknowledgement
+
bounded "fabric updates since last turn" injection at next root agent_start
```

Do not silently lose model visibility.

## 6.5 Optional P1 batching

After the conservative mapping is stable, consider coalescing multiple normal terminal results that arrive before the next natural root boundary into one root follow-up turn.

Do not timer-batch urgent/request messages.

---

# 7. P0: Make root/child capability asymmetry explicit to the model

## 7.1 Problem

The root may have:

- web access;
- Chrome;
- computer-use;
- MCP;
- goal;
- LCM;

while managed children deliberately do not.

The root model should know this before assigning a task.

## 7.2 `agent_spawn` description

Update the tool description/guidelines to say clearly:

```text
Managed children run in a constrained tool environment and do not inherit
arbitrary root extensions. Delegate repository analysis/coding/testing directly.
For root-only web/browser/MCP/computer-use work, gather the external information
at the root and send the findings to the child unless an explicit brokered
capability is available.
```

Keep it concise enough for smaller local models.

## 7.3 Root instruction

Add a root-side prompt guideline through the least intrusive Pi-supported mechanism.

Desired policy:

```text
Delegation policy:
- Use safe-agent children for repository analysis, implementation, tests, and coordinated parallel work.
- Managed children do not automatically inherit root extensions.
- If a delegated task requires web, Chrome, computer-use, or MCP and the child
  does not have an explicitly brokered equivalent, perform that external I/O
  at the root and send the result to the child.
- Do not infer child capabilities from root tool availability.
```

Prefer tool prompt guidelines or a small extension system-prompt addition over a visible user message every session.

## 7.4 Child bootstrap instruction

Extend `bootstrapInstructions()`:

```text
- Global root extensions are not inherited. If required information is unavailable
  through your granted tools, send a bounded clarification/request to the parent
  describing the exact missing capability or data.
```

This turns capability mismatch into an intentional escalation instead of tool hallucination/retry.

---

# 8. P0/P1: Guard obvious root shell mutations during live child holds

## 8.1 Goal

Reduce accidental violation of borrowing semantics through common root shell commands without pretending arbitrary shell can be perfectly sandboxed.

Current root `edit`/`write` guard remains authoritative for those tools.

## 8.2 Add a root shell preflight classifier

Intercept root `bash`/shell tool calls when the fabric has live mutable holds or in-flight write fences.

Pure API:

```ts
type RootShellRisk =
  | { kind: "read-only" }
  | { kind: "known-mutator"; scope: "broad" | "path"; paths?: string[]; reason: string }
  | { kind: "unknown" };

function classifyRootShellCommand(command: string): RootShellRisk;
```

## 8.3 Known broad mutators

Recognize common accidental collision sources, including:

```text
cargo fmt
ruff format
black
prettier --write
eslint --fix
go fmt ./...
gofmt -w
dotnet format
mix format
npm/pnpm/yarn format scripts when known to mutate
code-generation commands
git checkout
git restore
git reset
git clean
sed -i
perl -pi
shell redirection to repository files
```

Do not attempt a complete shell parser in the first implementation.

## 8.4 Policy

When no live child mutable hold/fence exists:

- preserve current root shell behavior.

When live child write coordination exists:

```text
known read-only
  -> allow

known broad mutator
  -> block or require explicit retry/force, with exact reason

known path-scoped mutator
  -> compare affected path(s) to live coordinated resources when feasible

unknown
  -> preserve trusted-root behavior, but optionally emit one concise warning
```

A strong default is to block known broad mutators only.

Do not falsely advertise this as a shell sandbox.

## 8.5 Root error UX

Example:

```text
safe-agents blocked `cargo fmt` while child agent-4 holds mutable resource
src/parser. Wait for/release the hold, or explicitly perform the operation after
coordinated child work completes.
```

Do not expose internal tokens or verbose broker state.

## 8.6 Tests

Include:

- `cargo test` allowed;
- `git diff` allowed;
- `rg` allowed;
- `cargo fmt` blocked with live hold;
- `ruff format .` blocked with live hold;
- `git restore .` blocked;
- root `edit` remains fenced as before;
- unknown shell command retains documented trusted-root semantics.

---

# 9. P1: Extend ownership beyond files only through explicit abstract resources

Do not make this a blocker for the integration hardening release.

The current resource model already has a generic `kind`, so document and test future use for:

```text
process:dev-server:3000
browser:staging-session
database:test-schema
service:docker-compose
```

Do not attempt to automatically lock Chrome, MCP, or desktop state yet.

The immediate requirement is documentation:

> file borrowing guarantees do not automatically coordinate external browser/process/MCP state unless that state is explicitly represented as a fabric resource.

---

# 10. P1: Improve `/agents` diagnostics

Current `/agents` output is useful but should make capability topology obvious.

Suggested detailed child output:

```text
agent-4 [running] role=implementer
  model=openai/gpt-5.6-sol thinking=high
  task=T-12
  workspace=worktree:/...
  repo-write=yes shell=workspace
  spawn=no peers=yes escalate=yes
  external-root-extensions=not-inherited
  context=lcm-embedded
```

Add status summaries for:

```text
quiescent: yes|no
running children: N
unresolved child tasks: N
mutable holds: N
pending root requests: N
pending root deliveries: N
```

This should be the same data supplied to the fabric-state provider.

Avoid dumping full messages or large task outputs in ordinary status.

---

# 11. P1: Model-routing UX

Keep current routing precedence and inheritance.

Improve provenance so every spawn result clearly reports:

```text
agent id
role
provider/model
thinking
route source
workspace mode
context mode
```

Example:

```text
agent-7 · reviewer · openai/gpt-5.6-sol · high · role-default · worktree · lcm-embedded
```

This matters when a root uses local Qwen and delegates selected reviews to Codex, or vice versa.

Do not add model-specific policy magic.

---

# 12. Cross-project interoperability helper

Implement the same registry protocol structurally as LCM, but do not create a package dependency solely for a few interfaces.

Provider names:

```text
safe-agent-team.fabric-state.v1
local-context-manager.embedded-context.v1
```

Requirements:

- no direct imports;
- provider absence is ordinary;
- version mismatch is ordinary;
- failures degrade safely;
- multiple Pi sessions/processes cannot leak fabric state to each other;
- registry does not become a new source of task/resource truth.

If Pi later gains an official cross-extension service API, migrate behind a compatibility adapter.

Isolation compatibility requirement:

- real-Pi tests must use `--no-extensions` plus explicit `-e` entrypoints;
- full-state tests must set a disposable `PI_CODING_AGENT_DIR`;
- safe-agent managed children must resolve the same canonical Pi agent directory as the root;
- no integration test may pass only because a globally installed extension was discovered.

---

# 13. Root message-delivery tests

Add a table-driven test over all `MESSAGE_TYPES`.

At minimum verify:

| Type | Default root turn |
|---|---|
| progress | no |
| inform | no |
| clarification | yes |
| decision_request | yes |
| escalation | yes |
| blocked | yes |
| request | yes |
| response | yes |
| resource_request | yes |
| resource_granted | normally no |
| result | yes |
| task_result | yes |
| handoff | yes |
| agent_failed | yes |
| cancel | yes |
| steer | yes |

Also test:

- urgent always becomes steer + trigger;
- broker order remains preserved;
- duplicate delivery remains idempotent;
- acknowledgement still happens only after host acceptance;
- non-triggering messages survive for the next natural root model turn;
- session shutdown/reload does not deliver stale messages into a new generation.

---

# 14. Embedded-context integration tests

Use a fake LCM provider in safe-agent-team's unit tests.

Required cases:

### Provider absent

Expected: child starts with native context behavior.

### Provider available

Expected: child reports `context=lcm-embedded`.

### Tool output reduction

A child build emits a very large output.

Expected:

- transformed output is retained;
- recovery access survives;
- coordination tool result shape remains valid.

### Provider throws during transform

Expected:

- task continues;
- diagnostic recorded;
- context mode degrades to native.

### Compaction requested while streaming

Expected: defer until safe boundary.

### Child model context windows differ

Root and child use different model routes.

Expected: child policy receives the child's model context window, not the root's.

### Local model child

Use a fake Qwen-like route with large advertised context.

Expected: no model-name branch; same adaptive policy path.

---

# 15. Fabric-state provider tests

Required:

- newly attached idle root -> active and quiescent;
- running child -> non-quiescent;
- child ready but unfinished owned task -> non-quiescent;
- blocked child task -> non-quiescent;
- pending clarification to root -> non-quiescent;
- mutable hold -> non-quiescent;
- in-flight write fence -> non-quiescent;
- all child tasks terminal + no holds/requests -> quiescent;
- stale cwd -> no state;
- stale session id -> no state;
- shutdown -> inactive;
- reconnect uncertainty -> conservative non-quiescent;
- details are bounded;
- snapshot contains no auth credentials/message bodies.

---

# 16. Root-shell tests

Build a dedicated classifier test suite independent of live shell execution.

Test:

```text
cargo test                 -> read-only
git diff                   -> read-only
git status                 -> read-only
rg foo src                 -> read-only
cargo fmt                  -> known-mutator/broad
ruff format .              -> known-mutator/broad
prettier --write src       -> known-mutator
git restore .              -> known-mutator/broad
git reset --hard           -> known-mutator/broad
sed -i ...                 -> known-mutator
echo x > src/a.ts          -> known-mutator/path if recognized
unknown-tool --foo         -> unknown
```

Then integration-test blocking only when live mutable coordination exists.

---

# 17. Documentation updates

Update `README.md`, `ARCHITECTURE.md`, and `PROTOCOL.md`.

Explicitly document:

## Child capability boundary

Managed children:

- do not inherit arbitrary root extensions;
- receive only guarded tools and explicitly integrated safe capabilities;
- should request missing external information from parent.

## Root external I/O

Root web/browser/MCP/computer-use actions are outside child tool inheritance.

## Borrowing guarantee

Clarify:

> guarded Pi writes participate in borrowing/fencing. Root shell remains a trusted escape hatch, with best-effort blocking of recognized broad mutators when live child holds exist.

Do not describe shell ownership as formally complete.

## Context integration

If LCM embedded provider is present, managed children can use its child-safe policy without loading the LCM extension.

If absent, children use native Pi context behavior.

---

# 18. Suggested implementation order

- [ ] **P0: replace independent `PI_AGENT_DIR`/hard-coded child config resolution with Pi's canonical `getAgentDir()` / `PI_CODING_AGENT_DIR` behavior.**
- [ ] Add regression test proving a temporary `PI_CODING_AGENT_DIR` contains all child-derived state and the real `~/.pi/agent` remains untouched.
- [ ] Add process-local interop registry helper.
- [ ] Implement and register `safe-agent-team.fabric-state.v1`.
- [ ] Define/test conservative fabric quiescence.
- [ ] Discover `local-context-manager.embedded-context.v1`.
- [ ] Integrate child-safe context controller without changing `noExtensions: true`.
- [ ] Add child context-mode diagnostics.
- [ ] Centralize root message-delivery classification.
- [ ] Stop routine progress/inform messages from triggering root turns.
- [ ] Verify non-triggering messages remain model-visible on next natural turn.
- [ ] Add explicit root delegation/capability guidance.
- [ ] Extend child bootstrap instructions for missing root capabilities.
- [ ] Implement root shell mutator classifier.
- [ ] Block known broad root mutators while conflicting child coordination is live.
- [ ] Extend `/agents` diagnostics and route provenance.
- [ ] Add cross-project fake-provider tests.
- [ ] Add message-delivery table tests.
- [ ] Add shell-classifier tests.
- [ ] Update architecture/protocol/README documentation.
- [ ] Add changelog entry.

---

# 19. Definition of done

The work is complete when:

1. `pi-safe-agent-team` remains the only orchestration/subagent mechanism assumed by its prompt/tool surface.
2. Managed children still use `noExtensions: true`.
3. Children can optionally receive LCM context policy through an embedded provider without loading global extensions.
4. Failure of that optional integration never fails the child task.
5. Safe-agent-team publishes bounded quiescence state for LCM semantic-reset decisions.
6. Routine progress/inform traffic no longer wakes the root model unnecessarily.
7. Clarification, decisions, failures, blocking conditions, and task results still reach the root reliably.
8. Message order, deduplication, and acknowledgement guarantees remain intact.
9. Root models are explicitly told which capabilities children do and do not inherit.
10. Common broad shell mutators are protected during live child mutable work, without falsely claiming a complete shell sandbox.
11. `/agents` clearly displays child model/workspace/context/capability provenance.
12. The full stack works without user model-specific tuning on OpenAI Codex and local Qwen routes.
13. All compatibility and integration scenarios above are covered by CI.
14. Safe-agent child config/state resolution honors `PI_CODING_AGENT_DIR` through Pi's canonical resolver.
15. Real-Pi integration tests run with global extension discovery disabled and only explicit `-e` development entrypoints.
16. Full-isolation tests prove no child session/broker/config state leaks into the user's normal `~/.pi/agent`.

## Final engineering principle

`pi-safe-agent-team` owns actors, tasks, resources, borrowing, and execution coordination. Other extensions may provide narrow capabilities, but they must enter managed children through explicit safe adapters rather than ambient extension inheritance. The root may be powerful; children should be powerful only where the fabric can reason about that power.
