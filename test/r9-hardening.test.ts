import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Coordinator } from "../src/core/coordinator.ts";
import { FabricError } from "../src/core/errors.ts";
import type { AgentRecord, AgentSummary, FabricConfig, ModelRoute, ResourceRecord } from "../src/core/types.ts";
import { FabricRuntime, ManagedChild } from "../src/pi/runtime.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

function makeCoordinator(config: Partial<FabricConfig> = {}, now = { value: 1_000 }): Coordinator {
  let sequence = 0;
  return new Coordinator({
    rootId: "fabric",
    rootAgentId: "root",
    config,
    clock: () => now.value,
    idFactory: (prefix) => `${prefix}-${++sequence}`,
  });
}

function registerRoot(coordinator: Coordinator): { agent: AgentRecord; token: string } {
  return coordinator.dispatch("root", "agent.register", {
    rootId: "fabric",
    route,
    capabilities: { maySpawn: true, mayMessagePeers: true, mayEscalate: true, mayTransferOwnership: true, mayWriteRepo: true, mayUseShell: true },
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

test("ACK tombstones survive message pruning and eviction of the generic idempotency window", () => {
  const coordinator = makeCoordinator({ messageRetention: 1 });
  registerRoot(coordinator);
  registerChild(coordinator, "child");
  const first = coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "first" }).value as { message: { id: string } };
  coordinator.dispatch("child", "message.ack", { messageId: first.message.id, revision: 1, operationId: "ack-first" });
  coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "second" });

  // Evict the ordinary 256-entry replay cache. The compact acknowledgement
  // proof is deliberately independent of that bounded response cache.
  for (let index = 0; index < Coordinator.maxIdempotencyEntries + 8; index += 1) {
    coordinator.dispatch("root", "task.create", { description: `eviction-${index}`, operationId: `eviction-${index}` });
  }

  const replay = coordinator.dispatch("child", "message.ack", { messageId: first.message.id, revision: 1 }).value as any;
  assert.equal(replay.id, first.message.id);
  assert.equal(replay.to, "child");
  assert.equal(replay.revision, 1);
  assert.equal(replay.acknowledged, true);
  assert.equal(replay.alreadyAcknowledged, true);

  const restored = makeCoordinator({ messageRetention: 1 });
  restored.restoreState(coordinator.exportState());
  const restoredReplay = restored.dispatch("child", "message.ack", { messageId: first.message.id, revision: 1 }).value as any;
  assert.equal(restoredReplay.alreadyAcknowledged, true);
  expectCode(() => restored.dispatch("child", "message.ack", { messageId: first.message.id, revision: 2 }), "MESSAGE_REVISION_CONFLICT");
});

test("recovery reservations keep a running provider route occupied until the host reconciles it", () => {
  const now = { value: 1_000 };
  const coordinator = makeCoordinator({ maxConcurrentAgents: 1, reconnectGraceMs: 500 }, now);
  const root = registerRoot(coordinator);
  const running = registerChild(coordinator, "running");
  const waiting = registerChild(coordinator, "waiting");
  coordinator.dispatch(running.agent.id, "agent.begin_turn", { operationId: "running-turn" });

  const recovery = coordinator.recover();
  assert.ok(recovery.events.some((event) => event.type === "model_turn_recovery_reserved" && event.reservation.agentId === running.agent.id));
  assert.equal(coordinator.exportState().recoveryTurnReservations?.length, 1);

  coordinator.dispatch(root.agent.id, "agent.register", { rootId: "fabric", route, token: root.token });
  coordinator.dispatch(running.agent.id, "agent.register", { rootId: "fabric", parentId: root.agent.id, route, token: running.token });
  coordinator.dispatch(waiting.agent.id, "agent.register", { rootId: "fabric", parentId: root.agent.id, route, token: waiting.token });

  const queued = coordinator.dispatch(waiting.agent.id, "agent.begin_turn", { operationId: "waiting-turn" }).value as any;
  assert.equal(queued.queued, true);
  const stillRunning = coordinator.dispatch(running.agent.id, "agent.reconcile_turn", { state: "running" }).value as any;
  assert.equal(stillRunning.reconciled, true);
  assert.equal(stillRunning.agent.status, "running");
  coordinator.dispatch(running.agent.id, "agent.end_turn", { status: "ready" });
  const waiter = coordinator.exportState().modelWaiters?.find((candidate) => candidate.agentId === waiting.agent.id);
  assert.equal(waiter?.state, "granted");
  assert.equal(coordinator.exportState().recoveryTurnReservations?.length, 0);

  // A host that confirms the old provider call stopped releases the route
  // without converting the actor back to running.
  const stopped = coordinator.dispatch(waiting.agent.id, "agent.begin_turn", { operationId: "waiting-turn" }).value as any;
  assert.equal(stopped.started, true);
  coordinator.dispatch(waiting.agent.id, "agent.end_turn", { status: "ready" });
});

test("an unclaimed grant gets one notification-loss retry, then yields FIFO priority", () => {
  const now = { value: 1_000 };
  const coordinator = makeCoordinator({ maxConcurrentAgents: 1, modelTurnGrantTtlMs: 10 }, now);
  registerRoot(coordinator);
  const first = registerChild(coordinator, "first");
  const second = registerChild(coordinator, "second");
  coordinator.dispatch("root", "agent.begin_turn", {});
  coordinator.dispatch(first.agent.id, "agent.begin_turn", { operationId: "first-turn" });
  coordinator.dispatch(second.agent.id, "agent.begin_turn", { operationId: "second-turn" });
  coordinator.dispatch("root", "agent.end_turn", { status: "ready" });

  let state = coordinator.exportState();
  let firstGrant = state.modelWaiters?.find((waiter) => waiter.agentId === first.agent.id);
  assert.equal(firstGrant?.state, "granted");
  now.value = (firstGrant?.grantExpiresAt ?? now.value) + 1;
  coordinator.maintenance();
  state = coordinator.exportState();
  firstGrant = state.modelWaiters?.find((waiter) => waiter.agentId === first.agent.id);
  assert.equal(firstGrant?.state, "granted", "one lost notification is retried for compatibility");

  now.value = (firstGrant?.grantExpiresAt ?? now.value) + 1;
  coordinator.maintenance();
  state = coordinator.exportState();
  assert.equal(state.modelWaiters?.find((waiter) => waiter.agentId === first.agent.id)?.state, "queued");
  assert.equal(state.modelWaiters?.find((waiter) => waiter.agentId === second.agent.id)?.state, "granted");
});

test("retired resources compact into bounded inspectable tombstones", () => {
  const now = { value: 1_000 };
  const coordinator = makeCoordinator({ historyRetentionMs: 1, maxArchivedRecords: 2, historyGcBatchSize: 100 }, now);
  registerRoot(coordinator);
  for (let index = 0; index < 5; index += 1) {
    coordinator.dispatch("root", "resource.define", { resourceId: `resource-${index}`, kind: "file", path: `src/${index}.ts` });
    coordinator.dispatch("root", "resource.retire", { resourceId: `resource-${index}` });
  }
  now.value += 10;
  coordinator.maintenance();
  const state = coordinator.exportState();
  assert.equal(state.resources.length, 0);
  assert.equal(state.archivedResources?.length, 2);
  const listed = coordinator.dispatch("root", "resource.list", { limit: 100 }).value as ResourceRecord[];
  assert.equal(listed.length, 2);
  assert.ok(listed.every((resource) => resource.status === "retired" && resource.sharedHolds.length === 0 && resource.waiters.length === 0));
  const inspect = coordinator.dispatch("root", "resource.inspect", { resourceId: listed[0].id }).value as ResourceRecord;
  assert.equal(inspect.status, "retired");
  expectCode(() => coordinator.dispatch("root", "resource.define", { resourceId: listed[0].id, kind: "file", path: "reuse.ts" }), "IDENTITY_CONFLICT");
});

test("resource cursors remain usable for IDs at the maximum valid length", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const longId = "a".repeat(1_024);
  coordinator.dispatch("root", "resource.define", { resourceId: longId, kind: "file", path: "long.ts" });
  coordinator.dispatch("root", "resource.define", { resourceId: "z", kind: "file", path: "z.ts" });
  const first = coordinator.dispatch("root", "resource.list", { limit: 1 }).value as Array<ResourceRecord & { cursor?: string }>;
  assert.equal(first.length, 1);
  assert.ok(first[0].cursor?.startsWith("v2:"));
  const next = coordinator.dispatch("root", "resource.list", { limit: 1, after: first[0].cursor }).value as Array<ResourceRecord & { cursor?: string }>;
  assert.equal(next.length, 1);
  assert.notEqual(next[0].id, first[0].id);
});

test("cleanup discovery starts its own cursor when bounded status omits one", async () => {
  const runtime = new FabricRuntime({ cwd: process.cwd(), fabricId: "r9-discovery", sessionId: "r9-session", startBroker: false });
  const firstPage = Array.from({ length: 100 }, (_, index) => ({ id: `agent-${index}`, cursor: `v2:${index}:YQ`, depth: 1 } as unknown as AgentSummary));
  const secondPage = [{ id: "late-agent", cursor: "v2:100:bGF0ZS1hZ2VudA", depth: 1 } as unknown as AgentSummary];
  const calls: Array<Record<string, unknown>> = [];
  (runtime as any).root = { client: { request: async (_operation: string, args: Record<string, unknown>) => {
    calls.push(args);
    return calls.length === 1 ? firstPage : secondPage;
  } } };
  const status = {
    agents: firstPage.slice(0, 50).map(({ cursor: _cursor, ...agent }) => agent),
    truncated: { agents: true },
  } as any;
  const discovered = await (runtime as any).discoverAllAgentsForCleanup(status) as AgentSummary[];
  assert.equal(calls[0].after, undefined);
  assert.equal(discovered.some((agent) => agent.id === "late-agent"), true);
});

test("retaining a committed worktree does not retain its child session transcript", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-r9-cleanup-"));
  const sessionPath = join(directory, "sessions", "child");
  await mkdir(sessionPath, { recursive: true });
  const workspace = { mode: "worktree", root: directory, path: join(directory, "worktree"), baseRef: "base", branch: "branch" } as const;
  const strategy = { create: async () => workspace, cleanup: async () => { throw new FabricError("WORKSPACE_FAILURE", "committed worktree"); } };
  const runtime = new FabricRuntime({ cwd: directory, fabricId: "r9-cleanup", sessionId: "r9-cleanup-session", stateDirectory: directory, workspaceStrategy: strategy });
  let retained: Record<string, unknown> | undefined;
  (runtime as any).markArtifactsRetained = async (_agentId: string, artifact: Record<string, unknown>) => { retained = artifact; };
  let cleaned = false;
  (runtime as any).markArtifactsCleaned = async () => { cleaned = true; };
  const child = new ManagedChild(runtime, {
    agentId: "child",
    token: "token",
    parentId: "root",
    role: "worker",
    route,
    workspace,
    cwd: workspace.path,
    stateDirectory: directory,
    agentDir: directory,
    endpoint: "unused",
    model: {} as any,
    capabilities: { maySpawn: false, mayMessagePeers: false, mayEscalate: true, mayTransferOwnership: false, mayWriteRepo: true, mayUseShell: false, peerIds: [], resourceGrants: {} },
  });
  try {
    await (child as any).cleanupWorkspace();
    assert.equal(retained?.workspace, workspace);
    assert.equal(retained?.sessionPath, undefined);
    assert.equal(retained?.workspaceRetained, true);
    assert.equal(retained?.sessionRetained, false);
    assert.equal(cleaned, false);
    await assert.rejects(() => stat(sessionPath));
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
