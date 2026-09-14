import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BrokerClient } from "../src/broker/client.ts";
import { BrokerServer } from "../src/broker/server.ts";
import type { ModelRoute } from "../src/core/types.ts";

const itemCount = parsePositiveInt(process.env.R10_ITEMS ?? process.argv[2] ?? "2000");
const route: ModelRoute = { provider: "benchmark", model: "small", thinking: "medium" };
const directory = await mkdtemp(join(tmpdir(), "safe-agents-r10-production-"));
const server = new BrokerServer({
  directory,
  rootId: "r10-production-soak",
  rootAgentId: "root",
  // Deliberately use the normal production retention defaults. The item count
  // controls the workload, not the coordinator's history horizon.
  maintenanceMs: 60_000,
  checkpointTransactions: 4_096,
  checkpointBytes: 16 * 1024 * 1024,
});
const root = new BrokerClient({ endpoint: server.endpoint, agentId: "root", requestTimeoutMs: 120_000 });
const latencySamples = new Map<string, number[]>();
const rollbackImageBytes: number[] = [];
const rollbackSnapshotMs: number[] = [];
let rollbackSnapshotCount = 0;
const memoryBefore = process.memoryUsage();
let stallTimer: NodeJS.Timeout | undefined;
const eventLoopStalls: number[] = [];

function parsePositiveInt(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`item count must be a positive integer: ${value}`);
  return parsed;
}

function percentile(values: readonly number[], fraction: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)] ?? 0;
}

async function timed<T>(operation: string, action: () => Promise<T>): Promise<T> {
  const started = performance.now();
  try {
    return await action();
  } finally {
    const samples = latencySamples.get(operation) ?? [];
    samples.push(performance.now() - started);
    latencySamples.set(operation, samples);
  }
}

function installRollbackInstrumentation(): void {
  const coordinator = server.coordinator as any;
  const original = coordinator.exportState.bind(coordinator);
  coordinator.exportState = (options?: { includeRetainedArtifacts?: boolean }) => {
    const started = performance.now();
    const state = original(options);
    const elapsed = performance.now() - started;
    rollbackSnapshotCount += 1;
    rollbackSnapshotMs.push(elapsed);
    // JSON size is the durable image's wire-equivalent size. Sampling every
    // eighth image avoids making the diagnostic itself dominate the soak.
    if (rollbackSnapshotCount % 8 === 0) rollbackImageBytes.push(Buffer.byteLength(JSON.stringify(state)));
    return state;
  };
}

function startEventLoopWatch(): void {
  const started = performance.now();
  let expected = started + 10;
  stallTimer = setInterval(() => {
    const now = performance.now();
    const late = now - expected;
    if (late > 1) eventLoopStalls.push(late);
    expected = now + 10;
  }, 10);
  stallTimer.unref();
}

function stopEventLoopWatch(): void {
  if (stallTimer) clearInterval(stallTimer);
  stallTimer = undefined;
}

function summarizeLatency(): Record<string, { count: number; p50Ms: number; p95Ms: number; p99Ms: number; maxMs: number }> {
  return Object.fromEntries([...latencySamples.entries()].map(([operation, values]) => [operation, {
    count: values.length,
    p50Ms: percentile(values, 0.50),
    p95Ms: percentile(values, 0.95),
    p99Ms: percentile(values, 0.99),
    maxMs: Math.max(...values, 0),
  }]));
}

try {
  installRollbackInstrumentation();
  await server.start();
  await root.connect();
  const registration = await timed("agent.register", () => root.request<{ token: string }>("agent.register", {
    rootId: "r10-production-soak",
    route,
    capabilities: { mayWriteRepo: true, mayTransferOwnership: true, mayMessagePeers: true },
  }));
  root.setIdentity("root", registration.token);
  startEventLoopWatch();

  const taskIds: string[] = [];
  for (let index = 0; index < itemCount; index += 1) {
    const task = await timed("task.create", () => root.request<{ id: string }>("task.create", {
      description: `Production soak task ${index}: coordinate a realistically sized hot task record and preserve its diagnostic history.`,
    }));
    taskIds.push(task.id);

    const resourceId = `bench:r10:${index}`;
    await timed("resource.define", () => root.request("resource.define", {
      resourceId,
      kind: "file",
      path: `bench/r10/generated/resource-${index}.ts`,
    }));

    const message = await timed("message.send", () => root.request<{ message: { id: string } }>("message.send", {
      to: "root",
      type: "inform",
      body: `Production soak message ${index}: verify broker ordering, durable acceptance, and checkpoint behavior under the default 24-hour hot history.`,
      metadata: { benchmark: "r10-production-soak", index },
    }));
    await timed("message.ack", () => root.request("message.ack", { messageId: message.message.id, revision: 1 }));

    if (index % 16 === 0) {
      await timed("resource.borrow", () => root.request("resource.borrow", { resourceId, mode: "mutable", leaseMs: 60_000 }));
      await timed("resource.release", () => root.request("resource.release", { resourceId }));
    }
    if (index % 20 === 0) {
      await timed("task.update", () => root.request("task.update", {
        taskId: task.id,
        action: "complete",
        result: { summary: `Completed production soak task ${index} with a bounded result record.` },
      }));
    }
    if (index % 25 === 0) {
      await timed("agent.heartbeat", () => root.request("agent.heartbeat"));
      await timed("fabric.status", () => root.request("fabric.status"));
      await timed("fabric.snapshot", () => root.request("fabric.snapshot"));
    }
  }

  // Keep a small live task set in the hot state while measuring a final round
  // of transitions and diagnostics rather than measuring only terminal GC.
  for (const taskId of taskIds.slice(-Math.min(10, taskIds.length))) {
    await timed("task.show", () => root.request("task.show", { taskId }));
  }
  await timed("agent.heartbeat", () => root.request("agent.heartbeat"));
  await timed("fabric.status", () => root.request("fabric.status"));
  await timed("fabric.snapshot", () => root.request("fabric.snapshot"));

  const memoryAfter = process.memoryUsage();
  const journalBytesBeforeStop = await server.journal.size();
  const ackProofBytesBeforeStop = await fileSize(server.ackProofStore.filePath);
  root.close();
  stopEventLoopWatch();
  await server.stop();
  const checkpointBytes = await server.journal.size();
  const memoryAfterCheckpoint = process.memoryUsage();
  console.log(JSON.stringify({
    benchmark: "r10-production-soak",
    itemCount,
    retention: {
      historyRetentionMs: server.coordinator.config.historyRetentionMs,
      messageRetention: server.coordinator.config.messageRetention,
      maxArchivedRecords: server.coordinator.config.maxArchivedRecords,
    },
    latencyMs: summarizeLatency(),
    rollbackSnapshots: {
      count: rollbackSnapshotCount,
      p50Ms: percentile(rollbackSnapshotMs, 0.50),
      p95Ms: percentile(rollbackSnapshotMs, 0.95),
      p99Ms: percentile(rollbackSnapshotMs, 0.99),
      imageSamples: rollbackImageBytes.length,
      p50ImageBytes: percentile(rollbackImageBytes, 0.50),
      p95ImageBytes: percentile(rollbackImageBytes, 0.95),
      p99ImageBytes: percentile(rollbackImageBytes, 0.99),
      maxImageBytes: Math.max(...rollbackImageBytes, 0),
    },
    memoryBytes: {
      before: memoryBefore,
      afterWorkload: memoryAfter,
      afterCheckpoint: memoryAfterCheckpoint,
    },
    eventLoopStallsMs: {
      count: eventLoopStalls.length,
      p95Ms: percentile(eventLoopStalls, 0.95),
      maxMs: Math.max(...eventLoopStalls, 0),
    },
    storageBytes: {
      journalBeforeStop: journalBytesBeforeStop,
      checkpoint: checkpointBytes,
      ackProofsBeforeStop: ackProofBytesBeforeStop,
    },
  }, null, 2));
} finally {
  stopEventLoopWatch();
  root.close();
  if (server.isStarted()) await server.stop();
  await rm(directory, { recursive: true, force: true });
}

async function fileSize(path: string): Promise<number> {
  try {
    return (await stat(path)).size;
  } catch {
    return 0;
  }
}
