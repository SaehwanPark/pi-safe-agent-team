import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BrokerClient } from "../src/broker/client.ts";
import { BrokerServer } from "../src/broker/server.ts";
import { Coordinator } from "../src/core/coordinator.ts";
import type { AgentRecord, FabricConfig, FabricStatus, ModelRoute, RetainedArtifactRecord } from "../src/core/types.ts";

const iterations = parsePositiveInt(process.env.R9_ITERATIONS ?? process.argv[2] ?? "120");
const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };
const now = { value: 1_000 };
const config: Partial<FabricConfig> = {
  maxTotalAgents: 32,
  maxChildrenPerAgent: 16,
  maxMailboxMessages: 64,
  messageRetention: 8,
  maxConcurrentAgents: 1,
  modelTurnGrantTtlMs: 10,
  reconnectGraceMs: 100,
  heartbeatMs: 100,
  agentHeartbeatTimeoutMs: 10_000,
  historyRetentionMs: 10,
  maxArchivedRecords: 32,
  maxRetainedArtifacts: 8,
  historyGcBatchSize: 256,
};

function parsePositiveInt(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`iteration count must be a positive integer: ${value}`);
  return parsed;
}

function makeCoordinator(): Coordinator {
  return new Coordinator({ rootId: "r9-chaos", rootAgentId: "root", config, clock: () => now.value });
}

async function pause(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

const directory = await mkdtemp(join(tmpdir(), "safe-agents-r9-chaos-"));
let server: BrokerServer | undefined;
let root: BrokerClient | undefined;
let rootToken = "";
let first: BrokerClient | undefined;
let firstId = "";
let firstToken = "";
let second: BrokerClient | undefined;
let secondId = "";
let secondToken = "";
let artifactId = "";
let artifactToken = "";
let restartCount = 0;
let droppedConnectionCount = 0;
let abruptRestartCount = 0;
const maintenanceSamples: number[] = [];
const restartSamples: number[] = [];

async function startBroker(): Promise<void> {
  server = new BrokerServer({
    directory,
    rootId: "r9-chaos",
    rootAgentId: "root",
    coordinator: makeCoordinator(),
    maintenanceMs: 60_000,
    checkpointTransactions: 32,
    checkpointBytes: 2 * 1024 * 1024,
    clock: () => now.value,
  });
  await server.start();
}

function newClient(agentId: string, token?: string): BrokerClient {
  if (!server) throw new Error("broker is not running");
  return new BrokerClient({ endpoint: server.endpoint, agentId, token, requestTimeoutMs: 10_000 });
}

async function registerRoot(reconcileOperationId?: string): Promise<void> {
  root = newClient("root", rootToken || undefined);
  await root.connect();
  const registered = await root.request<{ token: string }>("agent.register", {
    rootId: "r9-chaos",
    role: "root",
    route,
    token: rootToken || undefined,
    capabilities: { maySpawn: true, mayMessagePeers: true, mayEscalate: true, mayTransferOwnership: true, mayWriteRepo: true, mayUseShell: true },
  });
  rootToken = registered.token;
  root.setIdentity("root", rootToken);
  if (reconcileOperationId) {
    await root.request("agent.reconcile_turn", { state: "stopped", operationId: reconcileOperationId });
  }
}

async function registerFixedChild(id: string, token: string): Promise<BrokerClient> {
  const client = newClient(id, token);
  await client.connect();
  await client.request("agent.register", { rootId: "r9-chaos", parentId: "root", route, token });
  return client;
}

async function closeClients(): Promise<void> {
  root?.close();
  first?.close();
  second?.close();
  await pause();
}

async function dropConnectionsAfterCommit(): Promise<void> {
  await closeClients();
  droppedConnectionCount += 1;
  root = newClient("root", rootToken);
  first = newClient(firstId, firstToken);
  second = newClient(secondId, secondToken);
  await Promise.all([root.connect(), first.connect(), second.connect()]);
}

async function crashBroker(): Promise<void> {
  if (!server?.isStarted()) return;
  // Simulate the process dying after committed requests but before its normal
  // shutdown checkpoint. The journal and cold manifest must be sufficient for
  // the replacement BrokerServer to recover.
  const raw = server as any;
  raw.stopping = true;
  raw.started = false;
  if (raw.maintenanceTimer) clearInterval(raw.maintenanceTimer);
  for (const connection of raw.connections as Set<any>) connection.socket.destroy();
  raw.connections.clear();
  await new Promise<void>((resolve) => {
    try {
      raw.server.close(() => resolve());
    } catch {
      resolve();
    }
  });
  await rm(raw.endpoint, { force: true }).catch(() => undefined);
  await raw.releaseLock();
  abruptRestartCount += 1;
}

async function restartBroker(activeOperationId?: string, abrupt = false): Promise<void> {
  const started = performance.now();
  await closeClients();
  if (abrupt) await crashBroker();
  else if (server?.isStarted()) await server.stop();
  await startBroker();
  await registerRoot(activeOperationId);
  first = await registerFixedChild(firstId, firstToken);
  second = await registerFixedChild(secondId, secondToken);
  restartCount += 1;
  restartSamples.push(performance.now() - started);
}

async function maintenance(): Promise<void> {
  if (!server) throw new Error("broker is not running");
  const started = performance.now();
  await server.maintenanceNow();
  maintenanceSamples.push(performance.now() - started);
}

async function status(): Promise<FabricStatus> {
  if (!root) throw new Error("root client is not connected");
  return root.request<FabricStatus>("fabric.status");
}

async function waiterFor(agentId: string): Promise<{ state?: string; grantExpiresAt?: number } | undefined> {
  const current = await status();
  return current.pendingModelTurns?.find((waiter) => waiter.agentId === agentId);
}

async function exerciseGrantExpiry(): Promise<void> {
  if (!root || !first || !second) throw new Error("fixed clients are not connected");
  const operationStamp = now.value;
  const rootOperationId = `root-capacity-${operationStamp}`;
  const firstOperationId = `first-capacity-${operationStamp}`;
  const secondOperationId = `second-capacity-${operationStamp}`;
  await root.request("agent.begin_turn", { operationId: rootOperationId });
  await first.request("agent.begin_turn", { operationId: firstOperationId });
  await second.request("agent.begin_turn", { operationId: secondOperationId });
  await root.request("agent.end_turn", { status: "ready" });

  const firstGrant = await waiterFor(firstId);
  if (firstGrant?.grantExpiresAt === undefined || firstGrant.state !== "granted") throw new Error("first capacity waiter was not granted");
  now.value = firstGrant.grantExpiresAt + 1;
  await maintenance();
  const retried = await waiterFor(firstId);
  if (retried?.state !== "granted") throw new Error("expired grant was not retried once");

  now.value = (retried.grantExpiresAt ?? now.value) + 1;
  await maintenance();
  const secondGrant = await waiterFor(secondId);
  if (secondGrant?.state !== "granted") throw new Error("expired grant did not yield FIFO priority");
  await second.request("agent.begin_turn", { operationId: secondOperationId });
  await second.request("agent.end_turn", { status: "ready" });
  const firstAfterSecond = await waiterFor(firstId);
  if (firstAfterSecond?.state === "granted") {
    await first.request("agent.begin_turn", { operationId: firstOperationId });
    await first.request("agent.end_turn", { status: "ready" });
  }
}

async function exerciseMessagesAndTasks(evictIdempotency = false): Promise<string> {
  if (!root || !first) throw new Error("fixed clients are not connected");
  let firstMessageId = "";
  for (let index = 0; index < 12; index += 1) {
    const sent = await root.request<{ message: { id: string } }>("message.send", {
      to: firstId,
      type: "inform",
      body: `chaos message ${index}`,
      clientDedupeKey: `chaos-${now.value}-${index}`,
    });
    firstMessageId ||= sent.message.id;
    await first.request("message.ack", { messageId: sent.message.id, revision: 1, operationId: `ack-${sent.message.id}` });
  }
  if (evictIdempotency) {
    for (let index = 0; index < Coordinator.maxIdempotencyEntries + 8; index += 1) {
      const task = await root.request<{ id: string }>("task.create", { description: `chaos eviction ${index}`, operationId: `task-eviction-${index}` });
      await root.request("task.update", { taskId: task.id, action: "complete", result: { summary: "done" } });
    }
  }
  const replay = await first.request<any>("message.ack", { messageId: firstMessageId, revision: 1 });
  if (replay.alreadyAcknowledged !== true) throw new Error("ACK did not converge after mailbox/idempotency retention");
  return firstMessageId;
}

async function exerciseResourceAndArtifact(iteration: number): Promise<void> {
  if (!root) throw new Error("root client is not connected");
  const resourceId = `chaos-resource-${iteration}`;
  await root.request("resource.define", { resourceId, kind: "file", path: `bench/r9/${resourceId}.ts` });
  await root.request("resource.retire", { resourceId });
  if (!artifactId) {
    const spawned = await root.request<{ agent: AgentRecord; token: string }>("agent.spawn", { route });
    artifactId = spawned.agent.id;
    artifactToken = spawned.token;
    const artifactClient = newClient(artifactId, artifactToken);
    await artifactClient.connect();
    await artifactClient.request("agent.register", { rootId: "r9-chaos", parentId: "root", route, token: artifactToken });
    await root.request("agent.cancel", { agentId: artifactId });
    await root.request("agent.mark_artifacts_retained", {
      agentId: artifactId,
      artifact: {
        workspace: { mode: "worktree", root: directory, path: join(directory, "retained-worktree"), baseRef: "main", branch: `r9-${iteration}` },
        sessionPath: join(directory, "sessions", artifactId),
        reason: "chaos retained branch",
      },
    });
    artifactClient.close();
  }
  now.value += 20;
  await maintenance();
}

try {
  await startBroker();
  await registerRoot();
  const firstSpawn = await root!.request<{ agent: AgentRecord; token: string }>("agent.spawn", { route });
  firstId = firstSpawn.agent.id;
  firstToken = firstSpawn.token;
  first = await registerFixedChild(firstId, firstToken);
  const secondSpawn = await root!.request<{ agent: AgentRecord; token: string }>("agent.spawn", { route });
  secondId = secondSpawn.agent.id;
  secondToken = secondSpawn.token;
  second = await registerFixedChild(secondId, secondToken);

  // Leave a real durable running-turn state in the checkpoint, then restart
  // the broker. The provider is simulated by this harness; reconciliation is
  // explicitly "stopped" so the reservation must be released before reuse.
  await root!.request("agent.begin_turn", { operationId: "restart-provider-turn" });
  await restartBroker("restart-provider-turn", true);

  let firstMessageId = "";
  for (let iteration = 0; iteration < iterations; iteration += 1) {
    await exerciseGrantExpiry();
    firstMessageId = await exerciseMessagesAndTasks(iteration === 0);
    await exerciseResourceAndArtifact(iteration);
    if (iteration === 0) {
      // Force one successful ACK replay after a broker restart, not just after
      // ordinary connection churn.
      await restartBroker();
      const replay = await first!.request<any>("message.ack", { messageId: firstMessageId, revision: 1 });
      if (replay.alreadyAcknowledged !== true) throw new Error("ACK replay did not survive broker restart");
    } else if (iteration % 10 === 0) {
      await restartBroker(undefined, iteration % 20 === 0);
    } else if (iteration % 3 === 0) {
      await dropConnectionsAfterCommit();
    }
  }

  const artifactPage = await root!.request<{ artifacts: RetainedArtifactRecord[] }>("agent.artifacts", { limit: 100 });
  if (!artifactPage.artifacts.some((artifact) => artifact.id && artifact.agentId === artifactId)) throw new Error("retained artifact was not recoverable through the broker API");
  const artifact = artifactPage.artifacts.find((candidate) => candidate.agentId === artifactId)!;
  if (artifact.status !== "resolved") {
    await root!.request("agent.resolve_artifact", { artifactId: artifact.id, resolution: "verified by r9 chaos" });
  }
  const finalStatus = await status();
  if ((finalStatus.pendingModelTurns?.length ?? 0) !== 0) throw new Error("capacity queue did not converge");
  if ((finalStatus.recoveryTurnReservations?.length ?? 0) !== 0) throw new Error("recovery reservation did not converge");
  if ((finalStatus.archivedCounts?.resources ?? 0) === 0) throw new Error("retired resources were not compacted");

  await closeClients();
  await server!.stop();
  const manifest = JSON.parse(await readFile(join(directory, "retained-artifacts.json"), "utf8")) as { records: RetainedArtifactRecord[] };
  const checkpoint = await readFile(join(directory, "events.jsonl"), "utf8");
  if (!manifest.records.some((record) => record.status === "resolved")) throw new Error("resolved artifact was not durable");
  if (checkpoint.includes('"retainedArtifacts"')) throw new Error("hot checkpoint still contains full retained artifact metadata");
  console.log(JSON.stringify({ iterations, restartCount, abruptRestartCount, droppedConnectionCount, maintenanceP95: percentile(maintenanceSamples, 0.95), restartP95: percentile(restartSamples, 0.95), manifestRecords: manifest.records.length, checkpointBytes: Buffer.byteLength(checkpoint) }, null, 2));
} finally {
  await closeClients();
  if (server?.isStarted()) await server.stop();
  await rm(directory, { recursive: true, force: true });
}

function percentile(values: number[], percentage: number): number {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * percentage))] ?? 0;
}
