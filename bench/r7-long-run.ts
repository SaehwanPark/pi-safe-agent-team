import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BrokerClient } from "../src/broker/client.ts";
import { BrokerServer } from "../src/broker/server.ts";
import { Coordinator } from "../src/core/coordinator.ts";
import type { AgentRecord, ModelRoute } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };
const iterations = parsePositiveInt(process.env.R7_ITERATIONS ?? process.argv[2] ?? "1000");

function parsePositiveInt(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`iteration count must be a positive integer: ${value}`);
  return parsed;
}

function percentile(values: number[], percentage: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * percentage))] ?? 0;
}

function durationSamples(): Record<string, number[]> {
  return { heartbeat: [], status: [], spawn: [], taskComplete: [], maintenance: [] };
}

function record(samples: number[], operation: () => void): void {
  const started = performance.now();
  operation();
  samples.push(performance.now() - started);
}

async function runCoordinatorSoak(): Promise<Record<string, unknown>> {
  const now = { value: 1_000 };
  const samples = durationSamples();
  const coordinator = new Coordinator({
    rootId: "r7-benchmark",
    config: { historyRetentionMs: 1, maxArchivedRecords: 1_024, maxTotalAgents: 8, maxChildrenPerAgent: 8 },
    clock: () => now.value,
  });
  coordinator.dispatch("root", "agent.register", {
    rootId: "r7-benchmark",
    route,
    capabilities: { maySpawn: true, mayWriteRepo: true, mayMessagePeers: true },
  });

  for (let index = 0; index < iterations; index += 1) {
    record(samples.spawn, () => {
      const spawned = coordinator.dispatch("root", "agent.spawn", { route }).value as { agent: AgentRecord };
      coordinator.dispatch(spawned.agent.id, "agent.begin_turn", {});
      coordinator.dispatch(spawned.agent.id, "agent.end_turn", { status: "completed" });
    });
    const task = coordinator.dispatch("root", "task.create", { description: `benchmark task ${index}` }).value as { id: string };
    record(samples.taskComplete, () => {
      coordinator.dispatch("root", "task.update", { taskId: task.id, action: "complete", result: { summary: "done" } });
    });
    record(samples.heartbeat, () => {
      coordinator.dispatch("root", "agent.heartbeat", {});
    });
    record(samples.status, () => {
      coordinator.dispatch("root", "fabric.status", {});
    });
    now.value += 2;
    record(samples.maintenance, () => {
      coordinator.maintenance();
    });
  }

  const state = coordinator.exportState();
  return {
    iterations,
    hotAgents: state.agents.length,
    hotTasks: state.tasks.length,
    archivedAgents: state.archivedAgents?.length ?? 0,
    archivedTasks: state.archivedTasks?.length ?? 0,
    samplesMs: Object.fromEntries(Object.entries(samples).map(([name, values]) => [name, {
      p50: percentile(values, 0.50),
      p95: percentile(values, 0.95),
      p99: percentile(values, 0.99),
    }])),
  };
}

async function runBrokerWriteSoak(): Promise<Record<string, unknown>> {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-r7-bench-"));
  const server = new BrokerServer({
    directory,
    rootId: "r7-broker-benchmark",
    maintenanceMs: 60_000,
    checkpointTransactions: iterations + 10,
  });
  const client = new BrokerClient({ endpoint: server.endpoint, agentId: "root" });
  const beginSamples: number[] = [];
  const endSamples: number[] = [];
  try {
    await server.start();
    await client.connect();
    const registration = await client.request<{ token: string }>("agent.register", {
      rootId: "r7-broker-benchmark",
      route,
      capabilities: { mayWriteRepo: true, maySpawn: true },
    });
    client.setIdentity("root", registration.token);
    await client.request("resource.define", { resourceId: "r7-bench-file", kind: "file", path: "bench.ts" });
    for (let index = 0; index < iterations; index += 1) {
      const beginAt = performance.now();
      const decision = await client.request<{ fenceId?: string }>("resource.begin_write", { path: "bench.ts", hostGuard: true });
      beginSamples.push(performance.now() - beginAt);
      if (!decision.fenceId) throw new Error("benchmark write fence was not granted");
      const endAt = performance.now();
      await client.request("resource.end_write", { fenceId: decision.fenceId });
      endSamples.push(performance.now() - endAt);
    }
    return {
      iterations,
      beginWriteMs: { p50: percentile(beginSamples, 0.50), p95: percentile(beginSamples, 0.95), p99: percentile(beginSamples, 0.99) },
      endWriteMs: { p50: percentile(endSamples, 0.50), p95: percentile(endSamples, 0.95), p99: percentile(endSamples, 0.99) },
      journalBytes: await server.journal.size(),
    };
  } finally {
    client.close();
    await server.stop();
    await rm(directory, { recursive: true, force: true });
  }
}

const [coordinator, broker] = await Promise.all([runCoordinatorSoak(), runBrokerWriteSoak()]);
console.log(JSON.stringify({ coordinator, broker }, null, 2));
