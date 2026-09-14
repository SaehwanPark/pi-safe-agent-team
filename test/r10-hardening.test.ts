import test from "node:test";
import assert from "node:assert/strict";
import { appendFile, copyFile, readFile, mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { BrokerClient } from "../src/broker/client.ts";
import { BrokerServer } from "../src/broker/server.ts";
import { Coordinator } from "../src/core/coordinator.ts";
import { AckProofStore } from "../src/broker/ack-proof-store.ts";
import type { AgentRecord, FabricConfig, MessageAckTombstone, ModelRoute } from "../src/core/types.ts";

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
    capabilities: { maySpawn: true, mayMessagePeers: true, mayWriteRepo: true, mayTransferOwnership: true },
  }).value as { agent: AgentRecord; token: string };
}

test("broker keeps ACK proofs in an append-only cold store, not hot checkpoint state", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-r10-ack-"));
  let server: BrokerServer | undefined;
  let restarted: BrokerServer | undefined;
  const clients: BrokerClient[] = [];
  try {
    server = new BrokerServer({
      directory,
      rootId: "fabric",
      rootAgentId: "root",
      config: { messageRetention: 1 },
      checkpointTransactions: 1,
      maintenanceMs: 60_000,
    });
    await server.start();
    const root = new BrokerClient({ endpoint: server.endpoint, agentId: "root" });
    clients.push(root);
    await root.connect();
    const registration = await root.request<{ token: string }>("agent.register", {
      rootId: "fabric",
      route,
      capabilities: { maySpawn: true, mayMessagePeers: true },
    });
    root.setIdentity("root", registration.token);
    const spawned = await root.request<{ agent: AgentRecord; token: string }>("agent.spawn", { route });
    const child = new BrokerClient({ endpoint: server.endpoint, agentId: spawned.agent.id, token: spawned.token });
    clients.push(child);
    await child.connect();
    await child.request("agent.register", { rootId: "fabric", parentId: "root", route, token: spawned.token });

    const first = await root.request<{ message: { id: string } }>("message.send", { to: spawned.agent.id, type: "inform", body: "first" });
    await child.request("message.ack", { messageId: first.message.id, revision: 1 });
    await root.request("message.send", { to: spawned.agent.id, type: "inform", body: "second" });

    const checkpointBeforeRestart = await readFile(join(directory, "events.jsonl"), "utf8");
    assert.doesNotMatch(checkpointBeforeRestart, /"acknowledgedMessages"/);
    const coldProof = JSON.parse((await readFile(join(directory, "ack-proofs.jsonl"), "utf8")).trim()) as { id: string; to: string; revision: number };
    assert.deepEqual({ id: coldProof.id, to: coldProof.to, revision: coldProof.revision }, { id: first.message.id, to: spawned.agent.id, revision: 1 });

    await server.stop();
    restarted = new BrokerServer({ directory, rootId: "fabric", rootAgentId: "root", config: { messageRetention: 1 }, checkpointTransactions: 1, maintenanceMs: 60_000 });
    const rootAfterRestart = new BrokerClient({ endpoint: restarted.endpoint, agentId: "root", token: registration.token });
    const childAfterRestart = new BrokerClient({ endpoint: restarted.endpoint, agentId: spawned.agent.id, token: spawned.token });
    clients.push(rootAfterRestart, childAfterRestart);
    await restarted.start();
    await rootAfterRestart.connect();
    await rootAfterRestart.request("agent.register", { rootId: "fabric", route, token: registration.token, capabilities: { maySpawn: true, mayMessagePeers: true } });
    await childAfterRestart.connect();
    await childAfterRestart.request("agent.register", { rootId: "fabric", parentId: "root", route, token: spawned.token });

    const replay = await childAfterRestart.request<any>("message.ack", { messageId: first.message.id, revision: 1 });
    assert.equal(replay.alreadyAcknowledged, true);
    assert.equal(replay.revision, 1);
  } finally {
    for (const client of clients) client.close();
    if (restarted?.isStarted()) await restarted.stop();
    if (server?.isStarted()) await server.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("ACK proof store repairs a partial final record before future appends", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-r10-ack-tail-"));
  try {
    const store = new AckProofStore({ directory });
    await store.open();
    const first: MessageAckTombstone = { id: "message:first", to: "child", revision: 1, acknowledgedAt: 1_000, acknowledged: true };
    store.queue([first]);
    await store.flush();
    await appendFile(store.filePath, '{"version":1,"id":"partial');

    const reopened = new AckProofStore({ directory });
    await reopened.open();
    assert.equal(reopened.findExact("child", first.id, 1)?.acknowledged, true);
    const second: MessageAckTombstone = { id: "message:second", to: "child", revision: 1, acknowledgedAt: 1_001, acknowledged: true };
    reopened.queue([second]);
    await reopened.flush();
    const records = (await readFile(store.filePath, "utf8")).trim().split("\n").map((line) => JSON.parse(line) as { id: string });
    assert.deepEqual(records.map((record) => record.id), [first.id, second.id]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("resource snapshot identity changes when a pruned retired ID is reused", () => {
  const now = { value: 1_000 };
  const coordinator = makeCoordinator({ historyRetentionMs: 1, maxArchivedRecords: 1, historyGcBatchSize: 100 }, now);
  registerRoot(coordinator);

  coordinator.dispatch("root", "resource.define", { resourceId: "parser", kind: "module", path: "src/parser" });
  const oldSnapshot = coordinator.dispatch("root", "resource.snapshot", { resourceId: "parser" }).value as { resourceId: string; incarnation: string; version: number; token: string };
  coordinator.dispatch("root", "resource.retire", { resourceId: "parser" });
  now.value += 10;
  coordinator.maintenance();

  coordinator.dispatch("root", "resource.define", { resourceId: "other", kind: "module", path: "src/other" });
  coordinator.dispatch("root", "resource.retire", { resourceId: "other" });
  now.value += 10;
  coordinator.maintenance();
  assert.equal(coordinator.exportState().archivedResources?.some((resource) => resource.id === "parser"), false);

  const replacement = coordinator.dispatch("root", "resource.define", { resourceId: "parser", kind: "module", path: "src/parser-v2" }).value as { incarnation: string; version: number };
  const newSnapshot = coordinator.dispatch("root", "resource.snapshot", { resourceId: "parser" }).value as { incarnation: string; version: number; token: string };
  assert.equal(replacement.version, 1);
  assert.notEqual(newSnapshot.incarnation, oldSnapshot.incarnation);
  assert.notEqual(newSnapshot.token, oldSnapshot.token);
});

test("retained artifact admission frees a resolved slot at the exact limit", () => {
  const coordinator = makeCoordinator({ maxRetainedArtifacts: 2 });
  registerRoot(coordinator);
  for (const id of ["one", "two", "three"]) {
    coordinator.dispatch(id, "agent.register", { rootId: "fabric", parentId: "root", route });
    coordinator.dispatch("root", "agent.cancel", { agentId: id });
  }
  coordinator.dispatch("root", "agent.mark_artifacts_retained", { agentId: "one", artifact: { reason: "one" } });
  coordinator.dispatch("root", "agent.mark_artifacts_retained", { agentId: "two", artifact: { reason: "two" } });
  const first = coordinator.dispatch("root", "agent.artifacts", {}).value as { artifacts: Array<{ id: string; agentId: string }> };
  coordinator.dispatch("root", "agent.resolve_artifact", { artifactId: first.artifacts[0].id, resolution: "merged" });
  coordinator.dispatch("root", "agent.mark_artifacts_retained", { agentId: "three", artifact: { reason: "three" } });
  const final = coordinator.dispatch("root", "agent.artifacts", {}).value as { artifacts: Array<{ agentId: string; status?: string }> };
  assert.equal(final.artifacts.length, 2);
  assert.deepEqual(final.artifacts.map((artifact) => artifact.agentId).sort(), ["three", "two"]);
});

test("a real v0.2.3 checkpoint and journal tail migrate to v2 before recovery", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-r10-migration-"));
  const fixturePath = join(process.cwd(), "test/fixtures/v0.2.3/events.jsonl");
  await copyFile(fixturePath, join(directory, "events.jsonl"));
  const legacyRecord = JSON.parse((await readFile(fixturePath, "utf8")).split("\n", 1)[0]!) as { kind: string; state: any };
  assert.equal(legacyRecord.kind, "checkpoint");
  assert.equal(legacyRecord.state.version, 1);
  const rootState = legacyRecord.state.agents.find((agent: AgentRecord) => agent.depth === 0) as AgentRecord;
  const childState = legacyRecord.state.agents.find((agent: AgentRecord) => agent.parentId === rootState.id) as AgentRecord;
  const rootToken = rootState.authToken as string;
  const childToken = childState.authToken as string;
  let server: BrokerServer | undefined;
  const clients: BrokerClient[] = [];
  try {
    server = new BrokerServer({ directory, rootId: "fixture-fabric", rootAgentId: "root", maintenanceMs: 60_000 });
    await server.start();
    const root = new BrokerClient({ endpoint: server.endpoint, agentId: rootState.id, token: rootToken });
    const child = new BrokerClient({ endpoint: server.endpoint, agentId: childState.id, token: childToken });
    clients.push(root, child);
    await root.connect();
    await root.request("agent.register", { rootId: "fixture-fabric", route, token: rootToken });
    await child.connect();
    await child.request("agent.register", { rootId: "fixture-fabric", parentId: rootState.id, route, token: childToken });

    const inbox = await child.request<Array<{ body: string }>>("message.inbox", { limit: 10 });
    assert.ok(inbox.some((message) => message.body.includes("journal tail after the v0.2.3 checkpoint")));
    const task = await root.request<{ owner?: string }>("task.show", { taskId: childState.taskId });
    assert.equal(task.owner, childState.id);
    const resource = await root.request<{ incarnation?: string; status?: string }>("resource.inspect", { resourceId: "module:fixture" });
    assert.equal(resource.status, "active");
    assert.equal(typeof resource.incarnation, "string");

    root.close();
    const checkpoint = JSON.parse((await readFile(join(directory, "events.jsonl"), "utf8")).split("\n", 1)[0]!) as { kind: string; state: any };
    assert.equal(checkpoint.kind, "checkpoint");
    assert.equal(checkpoint.state.version, 2);
    assert.equal(typeof checkpoint.state.resources[0].incarnation, "string");
  } finally {
    for (const client of clients) client.close();
    if (server?.isStarted()) await server.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("new coordinator checkpoints use state version 2 and migrate legacy resources", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  coordinator.dispatch("root", "resource.define", { resourceId: "legacy", kind: "file", path: "legacy.ts" });
  const current = coordinator.exportState();
  assert.equal(current.version, 2);
  const legacy = structuredClone(current) as any;
  legacy.version = 1;
  delete legacy.resources[0].incarnation;

  const restored = makeCoordinator();
  restored.restoreState(legacy);
  const migrated = restored.exportState();
  assert.equal(migrated.version, 2);
  assert.equal(typeof migrated.resources[0].incarnation, "string");
  const restoredAgain = makeCoordinator();
  restoredAgain.restoreState(legacy);
  assert.equal(migrated.resources[0].incarnation, restoredAgain.exportState().resources[0].incarnation);
});
