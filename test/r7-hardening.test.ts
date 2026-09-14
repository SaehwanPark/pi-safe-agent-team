import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { Coordinator } from "../src/core/coordinator.ts";
import { FabricError } from "../src/core/errors.ts";
import type { AgentMessage, AgentRecord, ModelRoute } from "../src/core/types.ts";
import { GitWorkspaceStrategy } from "../src/workspace.ts";
import { BrokerClient } from "../src/broker/client.ts";
import { BrokerServer } from "../src/broker/server.ts";

const exec = promisify(execFile);
const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

function makeCoordinator(config: Record<string, number> = {}, now?: { value: number }): Coordinator {
  let sequence = 0;
  return new Coordinator({
    rootId: "fabric",
    config,
    clock: () => now?.value ?? 1_000,
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

function registerChild(coordinator: Coordinator, id: string, parentId = "root"): { agent: AgentRecord; token: string } {
  return coordinator.dispatch(id, "agent.register", {
    rootId: "fabric",
    parentId,
    route,
    capabilities: { mayMessagePeers: true },
  }).value as { agent: AgentRecord; token: string };
}

function expectCode(fn: () => unknown, code: string): void {
  assert.throws(fn, (error: unknown) => error instanceof FabricError && error.code === code);
}

test("clean committed child work remains available instead of being auto-deleted", async () => {
  const repo = await mkdtemp(join(tmpdir(), "safe-agents-r7-worktree-"));
  try {
    await exec("git", ["init", "-q"], { cwd: repo });
    await exec("git", ["config", "user.email", "test@example.invalid"], { cwd: repo });
    await exec("git", ["config", "user.name", "Test"], { cwd: repo });
    await writeFile(join(repo, "README"), "base\n");
    await exec("git", ["add", "README"], { cwd: repo });
    await exec("git", ["commit", "-qm", "initial"], { cwd: repo });

    const strategy = new GitWorkspaceStrategy();
    const worktree = await strategy.create({ mode: "worktree", cwd: repo, stateDirectory: repo, agentId: "child" });
    await writeFile(join(worktree.path, "child.txt"), "committed child work\n");
    await exec("git", ["add", "child.txt"], { cwd: worktree.path });
    await exec("git", ["commit", "-qm", "child work"], { cwd: worktree.path });

    await assert.rejects(() => strategy.cleanup(worktree), /committed child work/);
    assert.match((await exec("git", ["branch", "--list", worktree.branch!], { cwd: repo })).stdout, /pi-safe/);
    await strategy.cleanup(worktree, true);
  } finally {
    await rm(repo, { recursive: true, force: true });
  }
});

test("recovery keeps semantic task ownership until grace expires", () => {
  const now = { value: 1_000 };
  const coordinator = makeCoordinator({ agentHeartbeatTimeoutMs: 100, reconnectGraceMs: 200 }, now);
  registerRoot(coordinator);
  const child = coordinator.dispatch("root", "agent.spawn", { route, taskDescription: "recover once" }).value as { agent: AgentRecord; token: string; taskId: string };
  const other = registerChild(coordinator, "other");

  now.value = 1_101;
  coordinator.dispatch("root", "agent.heartbeat", {});
  coordinator.dispatch(other.agent.id, "agent.heartbeat", {});
  coordinator.maintenance();

  const stale = coordinator.dispatch("root", "agent.status", { agentId: child.agent.id }).value as AgentRecord;
  assert.equal(stale.reconnectable, true);
  assert.equal(coordinator.dispatch("root", "task.show", { taskId: child.taskId }).value.owner, child.agent.id);
  expectCode(() => coordinator.dispatch(other.agent.id, "task.claim", { taskId: child.taskId }), "TASK_BUSY");

  coordinator.dispatch(child.agent.id, "agent.register", { rootId: "fabric", parentId: "root", route, token: child.token });
  assert.equal(coordinator.dispatch("root", "task.show", { taskId: child.taskId }).value.owner, child.agent.id);
});

test("recovery is quiet during reconnect grace and reports failure only after retirement", () => {
  const now = { value: 1_000 };
  const coordinator = makeCoordinator({ agentHeartbeatTimeoutMs: 100, reconnectGraceMs: 200 }, now);
  registerRoot(coordinator);
  const child = registerChild(coordinator, "child");

  now.value = 1_101;
  coordinator.dispatch("root", "agent.heartbeat", {});
  coordinator.maintenance();
  let messages = coordinator.dispatch("root", "message.list", { scope: "all" }).value as AgentMessage[];
  assert.equal(messages.some((message) => message.type === "agent_failed"), false);

  now.value = 1_302;
  coordinator.dispatch("root", "agent.heartbeat", {});
  coordinator.maintenance();
  messages = coordinator.dispatch("root", "message.list", { scope: "all" }).value as AgentMessage[];
  assert.ok(messages.some((message) => message.type === "agent_failed" && message.from === child.agent.id));
});

test("reconnecting descendants wait for an active parent", () => {
  const coordinator = makeCoordinator();
  const root = registerRoot(coordinator);
  const parent = registerChild(coordinator, "parent");
  const child = registerChild(coordinator, "child", parent.agent.id);
  coordinator.recover();

  expectCode(() => coordinator.dispatch(child.agent.id, "agent.register", {
    rootId: "fabric",
    parentId: parent.agent.id,
    route,
    token: child.token,
  }), "LIFECYCLE_CONFLICT");

  coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, token: root.token });
  coordinator.dispatch(parent.agent.id, "agent.register", { rootId: "fabric", parentId: "root", route, token: parent.token });
  const reconnected = coordinator.dispatch(child.agent.id, "agent.register", { rootId: "fabric", parentId: parent.agent.id, route, token: child.token }).value as { agent: AgentRecord };
  assert.equal(reconnected.agent.status, "ready");
});

test("broker turn admission is durable FIFO and wakes the next waiter", () => {
  const coordinator = makeCoordinator({ maxConcurrentAgents: 1 });
  registerRoot(coordinator);
  const first = registerChild(coordinator, "first");
  const second = registerChild(coordinator, "second");
  assert.deepEqual(coordinator.dispatch("root", "agent.begin_turn", {}).value, { started: true });
  assert.deepEqual(coordinator.dispatch(first.agent.id, "agent.begin_turn", { operationId: "first-turn" }).value, { started: false, reason: "maxConcurrentAgents reached", queued: true });
  assert.deepEqual(coordinator.dispatch(second.agent.id, "agent.begin_turn", { operationId: "second-turn" }).value, { started: false, reason: "maxConcurrentAgents reached", queued: true });
  const queued = coordinator.exportState();
  assert.deepEqual(queued.modelWaiters?.map((waiter) => waiter.agentId), [first.agent.id, second.agent.id]);

  const released = coordinator.dispatch("root", "agent.end_turn", { status: "ready" });
  assert.ok(released.events.some((event) => event.type === "slot_available" && event.agentId === first.agent.id));
  // The grant is durable but does not start work on behalf of a host whose
  // response/event may have been lost. The matching retry claims it.
  assert.equal((coordinator.dispatch("root", "agent.status", { agentId: first.agent.id }).value as AgentRecord).status, "ready");
  assert.deepEqual(coordinator.dispatch(first.agent.id, "agent.begin_turn", { operationId: "first-turn" }).value, { started: true });
  assert.equal(coordinator.dispatch(first.agent.id, "agent.begin_turn", { operationId: "first-turn" }).value.replayed, true);
  coordinator.dispatch(first.agent.id, "agent.end_turn", { status: "ready" });
  assert.equal((coordinator.dispatch("root", "agent.status", { agentId: second.agent.id }).value as AgentRecord).status, "ready");
  assert.deepEqual(coordinator.dispatch(second.agent.id, "agent.begin_turn", { operationId: "second-turn" }).value, { started: true });
});

test("the broker wakes a queued turn without polling and preserves its ticket", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-r7-turn-"));
  const server = new BrokerServer({ directory, rootId: "fabric", config: { maxConcurrentAgents: 1 }, maintenanceMs: 60_000 });
  const root = new BrokerClient({ endpoint: server.endpoint, agentId: "root" });
  let child: BrokerClient | undefined;
  try {
    await server.start();
    await root.connect();
    const registration = await root.request<{ token: string }>("agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true } });
    root.setIdentity("root", registration.token);
    await root.requestIdempotent("agent.begin_turn", {}, "root-turn");
    const spawned = await root.request<{ agent: AgentRecord; token: string }>("agent.spawn", { route });
    child = new BrokerClient({ endpoint: server.endpoint, agentId: spawned.agent.id, token: spawned.token });
    await child.connect();
    await child.request("agent.register", { rootId: "fabric", parentId: "root", route, token: spawned.token });
    const queued = await child.requestIdempotent<{ started: boolean; queued?: boolean }>("agent.begin_turn", {}, "child-turn");
    assert.equal(queued.queued, true);

    const wake = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("queued turn was not woken")), 1_000);
      child!.onEvent((event) => {
        if (event.event !== "slot_available") return;
        clearTimeout(timer);
        resolve();
      });
    });
    await root.requestIdempotent("agent.end_turn", { status: "ready" }, "root-end");
    await wake;
    const started = await child.requestIdempotent<{ started: boolean }>("agent.begin_turn", {}, "child-turn");
    assert.equal(started.started, true);
  } finally {
    child?.close();
    root.close();
    await server.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("message ACK replays after the acknowledged record is pruned", () => {
  const coordinator = makeCoordinator({ messageRetention: 1 });
  registerRoot(coordinator);
  registerChild(coordinator, "child");
  const first = coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "one" }).value.message as AgentMessage;
  const operationId = "ack:child:first:1";
  coordinator.dispatch("child", "message.ack", { messageId: first.id, revision: 1, operationId });
  coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "two" });
  const replay = coordinator.dispatch("child", "message.ack", { messageId: first.id, revision: 1, operationId }).value as AgentMessage & { replayed?: boolean };
  assert.equal(replay.id, first.id);
  assert.equal(replay.replayed, true);
});

test("read projections avoid rollback snapshots while mutations use one", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  let snapshots = 0;
  const original = coordinator.exportState.bind(coordinator);
  (coordinator as any).exportState = () => {
    snapshots += 1;
    return original();
  };
  coordinator.dispatch("root", "fabric.status", {});
  coordinator.dispatch("root", "task.list", {});
  assert.equal(snapshots, 0);
  coordinator.dispatch("root", "task.create", { description: "one" });
  assert.equal(snapshots, 1);
});

test("terminal history moves to bounded tombstones and remains safe for dependencies", () => {
  const now = { value: 1_000 };
  const coordinator = makeCoordinator({ historyRetentionMs: 100, maxArchivedRecords: 2 }, now);
  registerRoot(coordinator);
  const child = registerChild(coordinator, "child");
  const task = coordinator.dispatch("root", "task.create", { description: "finished prerequisite" }).value;
  coordinator.dispatch("root", "task.update", { taskId: task.id, action: "complete", result: { summary: "done" } });
  const request = coordinator.dispatch("root", "message.send", { to: child.agent.id, type: "clarification", body: "answer", expectsReply: true }).value as { request: { id: string } };
  coordinator.dispatch("root", "agent.cancel", { agentId: child.agent.id });
  now.value = 1_101;
  const maintenance = coordinator.maintenance();
  assert.ok(maintenance.events.some((event) => event.type === "agent_archived"));
  assert.ok(maintenance.events.some((event) => event.type === "task_archived"));
  assert.ok(maintenance.events.some((event) => event.type === "request_archived"));
  const state = coordinator.exportState();
  assert.equal(state.agents.some((agent) => agent.id === child.agent.id), false);
  assert.ok(state.archivedAgents?.some((agent) => agent.id === child.agent.id));
  assert.ok(state.archivedTasks?.some((archived) => archived.id === task.id));
  assert.ok(state.archivedRequests?.some((archived) => archived.id === request.request.id));
  expectCode(() => coordinator.dispatch("child", "agent.register", { rootId: "fabric", parentId: "root", route, token: child.token }), "IDENTITY_CONFLICT");
  const dependent = coordinator.dispatch("root", "task.create", { description: "uses archived prerequisite", dependencies: [task.id] }).value as { status: string };
  assert.equal(dependent.status, "ready");
});

test("workspace cleanup markers identify handled historical artifacts", async () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const child = registerChild(coordinator, "child");
  coordinator.dispatch("root", "agent.cancel", { agentId: child.agent.id });
  const result = coordinator.dispatch("root", "agent.mark_artifacts_cleaned", { agentId: child.agent.id }).value as AgentRecord;
  assert.equal(typeof result.artifactsCleanedAt, "number");
  const replay = coordinator.dispatch("root", "agent.mark_artifacts_cleaned", { agentId: child.agent.id });
  assert.deepEqual(replay.events, []);
});

