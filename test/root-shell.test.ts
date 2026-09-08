import test from "node:test";
import assert from "node:assert/strict";
import { evaluateRootShellGuard } from "../src/pi/guards.ts";
import type { FabricStatus } from "../src/core/types.ts";

function makeMockClient(status: FabricStatus) {
  return {
    async request<T>(_operation: string): Promise<T> {
      return status as unknown as T;
    },
  };
}

test("root-shell: allowed when no live child mutable hold exists", async () => {
  const status: FabricStatus = {
    rootId: "root",
    agents: [],
    tasks: [],
    resources: [],
    pendingRequests: [],
    recentMessages: [],
    runningChildren: 0,
    config: {} as any,
    activeFences: 0,
  };

  const client = makeMockClient(status);
  const options = { client, workspacePath: "/test/workspace" };

  // Even broad mutators are allowed when there is no conflicting child hold
  const outcome1 = await evaluateRootShellGuard(options, { command: "cargo fmt" });
  assert.equal(outcome1, undefined);

  const outcome2 = await evaluateRootShellGuard(options, { command: "git restore ." });
  assert.equal(outcome2, undefined);

  const outcome3 = await evaluateRootShellGuard(options, { command: "cargo test" });
  assert.equal(outcome3, undefined);

  const outcome4 = await evaluateRootShellGuard(options, { command: "unknown-tool --foo" });
  assert.equal(outcome4, undefined);
});

test("root-shell: blocked with exact UX when child holds mutable resource", async () => {
  const status: FabricStatus = {
    rootId: "root",
    agents: [],
    tasks: [],
    resources: [
      {
        id: "res-parser",
        kind: "file",
        path: "src/parser",
        version: 1,
        owner: "root",
        grants: {},
        sharedHolds: [],
        createdAt: Date.now(),
        updatedAt: Date.now(),
        mutableHold: {
          leaseId: "lease-1",
          agentId: "agent-4",
          mode: "mutable",
          acquiredAt: Date.now(),
          expiresAt: Date.now() + 30000,
          lastHeartbeat: Date.now(),
          leaseMs: 30000,
        },
        waiters: [],
      },
    ],
    pendingRequests: [],
    recentMessages: [],
    runningChildren: 1,
    config: {} as any,
    activeFences: 0,
  };

  const client = makeMockClient(status);
  const options = { client, workspacePath: "/test/workspace" };

  // 1. Read-only command is allowed
  const readOnlyOutcome = await evaluateRootShellGuard(options, { command: "cargo test" });
  assert.equal(readOnlyOutcome, undefined);

  const gitDiffOutcome = await evaluateRootShellGuard(options, { command: "git diff" });
  assert.equal(gitDiffOutcome, undefined);

  // 2. Broad mutator is blocked with exact reason
  const fmtOutcome = await evaluateRootShellGuard(options, { command: "cargo fmt" });
  assert.ok(fmtOutcome?.block);
  assert.equal(
    fmtOutcome?.reason,
    "safe-agents blocked `cargo fmt` while child agent-4 holds mutable resource src/parser. Wait for/release the hold, or explicitly perform the operation after coordinated child work completes."
  );

  const restoreOutcome = await evaluateRootShellGuard(options, { command: "git restore ." });
  assert.ok(restoreOutcome?.block);
  assert.ok(restoreOutcome?.reason?.includes("while child agent-4 holds mutable resource src/parser"));

  // 3. Path-scoped mutator on unrelated path is allowed
  const otherPathOutcome = await evaluateRootShellGuard(options, { command: "prettier --write docs/readme.md" });
  assert.equal(otherPathOutcome, undefined);

  // 4. Path-scoped mutator overlapping the held path is blocked
  const collidingOutcome = await evaluateRootShellGuard(options, { command: "prettier --write src/parser/ast.ts" });
  assert.ok(collidingOutcome?.block);
  assert.ok(collidingOutcome?.reason?.includes("while child agent-4 holds mutable resource src/parser"));

  // 5. Unknown command retains trusted-root semantics (allowed)
  const unknownOutcome = await evaluateRootShellGuard(options, { command: "custom-script --check" });
  assert.equal(unknownOutcome, undefined);

  // 6. Output redirections targeting held path are blocked
  const echoRedir = await evaluateRootShellGuard(options, { command: "echo x>src/parser/ast.ts" });
  assert.ok(echoRedir?.block);
  assert.ok(echoRedir?.reason?.includes("while child agent-4 holds mutable resource src/parser"));

  const appendRedir = await evaluateRootShellGuard(options, { command: "printf 'more'>>src/parser/ast.ts" });
  assert.ok(appendRedir?.block);
  assert.ok(appendRedir?.reason?.includes("while child agent-4 holds mutable resource src/parser"));

  // 7. tee pipeline targeting held path is blocked
  const teeOutcome = await evaluateRootShellGuard(options, { command: "cat input.ts | tee src/parser/ast.ts" });
  assert.ok(teeOutcome?.block);
  assert.ok(teeOutcome?.reason?.includes("while child agent-4 holds mutable resource src/parser"));

  // 8. Chained cd command turns mutator broad -> blocked
  const cdOutcome = await evaluateRootShellGuard(options, { command: "cd src && prettier --write other.ts" });
  assert.ok(cdOutcome?.block);
  assert.ok(cdOutcome?.reason?.includes("while child agent-4 holds mutable resource src/parser"));

  // 9. Path-scoped mutator with relative navigation (..) resolving into held path is blocked
  const relativeDotDot = await evaluateRootShellGuard(options, { command: "prettier --write src/other/../parser/ast.ts" });
  assert.ok(relativeDotDot?.block);
  assert.ok(relativeDotDot?.reason?.includes("while child agent-4 holds mutable resource src/parser"));
});

test("root-shell: active write fences block colliding mutators even if mutable hold expired", async () => {
  // Scenario: Child agent-4 had a mutable hold on src/parser, but the hold expired
  // while an in-flight write fence remains active on that path.
  const statusWithFence: FabricStatus = {
    rootId: "root",
    agents: [],
    tasks: [],
    resources: [],
    pendingRequests: [],
    recentMessages: [],
    runningChildren: 1,
    config: {} as any,
    activeFences: 1,
    fences: [
      {
        id: "fence-1",
        resourceId: "res-parser",
        path: "src/parser",
        actorId: "agent-4",
      },
    ],
  };

  const client = makeMockClient(statusWithFence);
  const options = { client, workspacePath: "/test/workspace" };

  // 1. Read-only command is allowed
  const readOnlyOutcome = await evaluateRootShellGuard(options, { command: "cargo test" });
  assert.equal(readOnlyOutcome, undefined);

  // 2. Broad mutator is blocked by active fence
  const broadOutcome = await evaluateRootShellGuard(options, { command: "cargo fmt" });
  assert.ok(broadOutcome?.block);
  assert.ok(broadOutcome?.reason?.includes("active write fence"));

  // 3. Path-scoped mutator colliding with fenced path is blocked
  const collidingOutcome = await evaluateRootShellGuard(options, { command: "prettier --write src/parser/ast.ts" });
  assert.ok(collidingOutcome?.block);
  assert.ok(collidingOutcome?.reason?.includes("while child agent-4 has an active write fence on src/parser"));

  // 4. Path-scoped mutator on unrelated path is allowed when fence path is known
  const unrelatedOutcome = await evaluateRootShellGuard(options, { command: "prettier --write docs/readme.md" });
  assert.equal(unrelatedOutcome, undefined);

  // 5. When active fences exist but path details are omitted (fail closed for mutators)
  const statusOmittedPaths: FabricStatus = {
    ...statusWithFence,
    fences: undefined,
  };
  const clientOmitted = makeMockClient(statusOmittedPaths);
  const optionsOmitted = { client: clientOmitted, workspacePath: "/test/workspace" };

  const blockedFailClosed = await evaluateRootShellGuard(optionsOmitted, { command: "prettier --write docs/readme.md" });
  assert.ok(blockedFailClosed?.block);
  assert.ok(blockedFailClosed?.reason?.includes("active write fence"));

  // But read-only still succeeds
  const readOnlyOmitted = await evaluateRootShellGuard(optionsOmitted, { command: "git diff" });
  assert.equal(readOnlyOmitted, undefined);
});

