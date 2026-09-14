import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BrokerClient } from "../src/broker/client.ts";
import { Coordinator } from "../src/core/coordinator.ts";
import { FabricError } from "../src/core/errors.ts";
import type { AgentRecord, ModelRoute } from "../src/core/types.ts";
import { FabricRuntime } from "../src/pi/runtime.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

function makeCoordinator(config: Record<string, unknown> = {}, now = { value: 1_000 }): Coordinator {
  let sequence = 0;
  return new Coordinator({
    rootId: "fabric",
    config,
    clock: () => now.value,
    idFactory: (prefix) => `${prefix}-${++sequence}`,
  });
}

function registerRoot(coordinator: Coordinator): { agent: AgentRecord; token: string } {
  return coordinator.dispatch("root", "agent.register", {
    rootId: "fabric",
    route,
    capabilities: { maySpawn: true, mayMessagePeers: true, mayWriteRepo: true, mayTransferOwnership: true },
  }).value as { agent: AgentRecord; token: string };
}

function registerChild(coordinator: Coordinator, id: string): { agent: AgentRecord; token: string } {
  return coordinator.dispatch(id, "agent.register", {
    rootId: "fabric",
    parentId: "root",
    route,
    capabilities: { mayMessagePeers: true, mayWriteRepo: true },
  }).value as { agent: AgentRecord; token: string };
}

function expectCode(fn: () => unknown, code: string): void {
  assert.throws(fn, (error: unknown) => error instanceof FabricError && error.code === code);
}

test("R8 status and snapshot projections stay bounded under large resource state", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  for (let index = 0; index < 100; index += 1) {
    coordinator.dispatch("root", "resource.define", {
      resourceId: `${"resource-".repeat(100)}-${index}`,
      kind: "file",
      path: `${"nested/".repeat(500)}file-${index}.ts`,
    });
  }

  const status = coordinator.dispatch("root", "fabric.status", {}).value as any;
  const snapshot = coordinator.dispatch("root", "fabric.snapshot", {}).value as any;
  assert.equal(status.resources.length, 50);
  assert.equal(status.truncated.resources, true);
  assert.equal(snapshot.activeTasks.length <= 50, true);
  assert.equal(snapshot.mutableResources.length <= 50, true);
  assert.ok(Buffer.byteLength(JSON.stringify(status)) < 2 * 1024 * 1024);
  assert.ok(Buffer.byteLength(JSON.stringify(snapshot)) < 2 * 1024 * 1024);
});

test("R8 model-turn grants remain durable, claimable, and FIFO across expiry", () => {
  const now = { value: 1_000 };
  const coordinator = makeCoordinator({ maxConcurrentAgents: 1, modelTurnGrantTtlMs: 100 }, now);
  registerRoot(coordinator);
  const first = registerChild(coordinator, "first");
  const second = registerChild(coordinator, "second");

  coordinator.dispatch("root", "agent.begin_turn", { operationId: "root-turn" });
  const firstQueued = coordinator.dispatch(first.agent.id, "agent.begin_turn", { operationId: "first-turn" }).value as any;
  const secondQueued = coordinator.dispatch(second.agent.id, "agent.begin_turn", { operationId: "second-turn" }).value as any;
  assert.equal(firstQueued.queued, true);
  assert.equal(secondQueued.queued, true);

  coordinator.dispatch("root", "agent.end_turn", { status: "ready" });
  let state = coordinator.exportState();
  const granted = state.modelWaiters?.find((waiter) => waiter.agentId === first.agent.id);
  assert.equal(granted?.state, "granted");
  assert.equal(granted?.operationId, "first-turn");
  assert.ok((granted?.grantExpiresAt ?? 0) > now.value);
  assert.equal((coordinator.dispatch("root", "agent.status", { agentId: first.agent.id }).value as AgentRecord).status, "ready");

  // A missed grant event does not discard the logical turn. Expiry returns the
  // same ticket to the queue and the normal drain can grant it again.
  const oldExpiry = granted?.grantExpiresAt;
  now.value = (oldExpiry ?? now.value) + 1;
  const maintenance = coordinator.maintenance();
  assert.ok(maintenance.events.some((event) => event.type === "model_turn_waiting" && event.waiter.agentId === first.agent.id));
  state = coordinator.exportState();
  const regranted = state.modelWaiters?.find((waiter) => waiter.agentId === first.agent.id);
  assert.equal(regranted?.state, "granted");
  assert.ok((regranted?.grantExpiresAt ?? 0) > (oldExpiry ?? 0));
  assert.equal((coordinator.dispatch(first.agent.id, "agent.begin_turn", { operationId: "first-turn" }).value as any).started, true);
  coordinator.dispatch(first.agent.id, "agent.end_turn", { status: "ready" });
  assert.equal((coordinator.dispatch(second.agent.id, "agent.status", {}).value as AgentRecord).status, "ready");
  assert.equal((coordinator.dispatch(second.agent.id, "agent.begin_turn", { operationId: "second-turn" }).value as any).started, true);
});

test("R8 queued root compaction claims and releases its durable broker grant", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-r8-compaction-"));
  const runtime = new FabricRuntime({
    cwd: directory,
    fabricId: "r8-compaction-fabric",
    sessionId: "r8-compaction-session",
    stateDirectory: directory,
    config: { maxConcurrentAgents: 1, maintenanceMs: 60_000 } as any,
  });
  let child: BrokerClient | undefined;
  try {
    const context = {
      cwd: directory,
      model: { provider: "test", id: "small", reasoning: true },
      thinkingLevel: "medium",
      sessionManager: { getSessionId: () => "r8-compaction-session" },
    } as any;
    await runtime.attachRoot({} as any, context);
    const rootClient = runtime.client!;
    const rootAgentId = runtime.rootAgentId!;
    const spawned = await rootClient.request<{ agent: AgentRecord; token: string }>("agent.spawn", { route });
    child = new BrokerClient({ endpoint: runtime.endpoint, agentId: spawned.agent.id, token: spawned.token });
    await child.connect();
    await child.request("agent.register", { rootId: runtime.fabricId, parentId: rootAgentId, route, token: spawned.token });
    await child.requestIdempotent("agent.begin_turn", {}, "child-turn");

    const queued = new Promise<void>((resolve) => {
      const unsubscribe = rootClient.onEvent((event) => {
        if (event.event !== "model_turn_waiting") return;
        unsubscribe();
        resolve();
      });
    });
    const compaction = runtime.beginRootCompaction();
    await queued;
    await child.requestIdempotent("agent.end_turn", { status: "ready" }, "child-end");
    await compaction;

    const during = await rootClient.request<AgentRecord>("agent.status", { agentId: rootAgentId });
    assert.equal(during.status, "running");
    await runtime.endRootCompaction();
    const after = await rootClient.request<AgentRecord>("agent.status", { agentId: rootAgentId });
    assert.equal(after.status, "ready");
    assert.equal((await child.requestIdempotent<{ started: boolean }>("agent.begin_turn", {}, "child-next")).started, true);
    await child.requestIdempotent("agent.end_turn", { status: "ready" }, "child-next-end");
  } finally {
    child?.close();
    await runtime.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("R8 resource retirement preserves inspectability while removing active borrowability", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const child = registerChild(coordinator, "child");
  coordinator.dispatch("root", "resource.define", { resourceId: "module", kind: "module", path: "src" });
  coordinator.dispatch("root", "resource.define", { resourceId: "file", kind: "file", parentId: "module", path: "src/file.ts" });

  expectCode(() => coordinator.dispatch("root", "resource.retire", { resourceId: "module" }), "RESOURCE_CONFLICT");
  coordinator.dispatch("root", "resource.retire", { resourceId: "file" });
  const retired = coordinator.dispatch("root", "resource.inspect", { resourceId: "file" }).value as any;
  assert.equal(retired.status, "retired");
  expectCode(() => coordinator.dispatch(child.agent.id, "resource.borrow", { resourceId: "file", mode: "shared" }), "LIFECYCLE_CONFLICT");
  coordinator.dispatch("root", "resource.retire", { resourceId: "module" });
  assert.equal((coordinator.dispatch("root", "resource.inspect", { resourceId: "module" }).value as any).status, "retired");
});

test("R8 retained worktree metadata lets terminal agent and task history archive", () => {
  const now = { value: 1_000 };
  const coordinator = makeCoordinator({ historyRetentionMs: 1, historyGcBatchSize: 100 }, now);
  registerRoot(coordinator);
  const spawned = coordinator.dispatch("root", "agent.spawn", { route, taskDescription: "committed child work" }).value as { agent: AgentRecord; taskId: string };
  coordinator.dispatch("root", "agent.configure_child", {
    agentId: spawned.agent.id,
    workspace: { mode: "worktree", root: "/repo", path: "/state/worktrees/child", baseRef: "base-sha", branch: "pi-safe/child" },
  });
  coordinator.dispatch(spawned.agent.id, "agent.begin_turn", { operationId: "child-turn" });
  coordinator.dispatch("root", "task.update", { taskId: spawned.taskId, action: "complete", result: { summary: "committed" } });
  coordinator.dispatch(spawned.agent.id, "agent.end_turn", { status: "completed" });
  coordinator.dispatch("root", "agent.mark_artifacts_retained", {
    agentId: spawned.agent.id,
    artifact: {
      workspace: { mode: "worktree", root: "/repo", path: "/state/worktrees/child", baseRef: "base-sha", branch: "pi-safe/child" },
      sessionPath: "/state/sessions/child",
      headRef: "child-sha",
      reason: "clean worktree contains committed child changes",
    },
  });
  now.value += 10;
  coordinator.maintenance();
  const state = coordinator.exportState();
  assert.equal(state.agents.some((agent) => agent.id === spawned.agent.id), false);
  assert.equal(state.tasks.some((task) => task.id === spawned.taskId), false);
  assert.equal(state.archivedAgents?.some((agent) => agent.id === spawned.agent.id && agent.artifactDisposition === "retained"), true);
  assert.equal(state.archivedTasks?.some((task) => task.id === spawned.taskId), true);
  assert.equal(state.retainedArtifacts?.some((artifact) => artifact.agentId === spawned.agent.id && artifact.headRef === "child-sha"), true);
});

test("R8 archival keeps dependency edges needed for live tasks without pinning terminal history", () => {
  const now = { value: 1_000 };
  const coordinator = makeCoordinator({ historyRetentionMs: 1, historyGcBatchSize: 100, maxArchivedRecords: 10 }, now);
  registerRoot(coordinator);
  const prerequisite = coordinator.dispatch("root", "task.create", { description: "prerequisite" }).value as any;
  coordinator.dispatch("root", "task.update", { taskId: prerequisite.id, action: "complete", result: { summary: "done" } });
  const liveDependent = coordinator.dispatch("root", "task.create", { description: "live dependent", dependencies: [prerequisite.id] }).value as any;

  now.value += 10;
  coordinator.maintenance();
  assert.ok(coordinator.exportState().tasks.some((task) => task.id === prerequisite.id), "a live dependent keeps its prerequisite hot");
  coordinator.dispatch("root", "task.update", { taskId: liveDependent.id, action: "complete", result: { summary: "done" } });
  now.value += 10;
  coordinator.maintenance();
  const state = coordinator.exportState();
  const archivedPrerequisite = state.archivedTasks?.find((task) => task.id === prerequisite.id);
  const archivedDependent = state.archivedTasks?.find((task) => task.id === liveDependent.id);
  assert.ok(archivedPrerequisite);
  assert.ok(archivedDependent);
  assert.deepEqual(archivedDependent?.dependencies, [prerequisite.id]);
});

test("R8 pagination rejects malformed or deleted cursors instead of restarting", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  registerChild(coordinator, "child");
  expectCode(() => coordinator.dispatch("root", "discover.agents", { scope: "all", after: "v1:not-a-number:child" }), "INVALID_ARGUMENT");
  expectCode(() => coordinator.dispatch("root", "discover.agents", { scope: "all", after: "v1:1000:deleted-agent" }), "CURSOR_STALE");
  expectCode(() => coordinator.dispatch("root", "task.list", { after: "deleted-task" }), "CURSOR_STALE");
});
