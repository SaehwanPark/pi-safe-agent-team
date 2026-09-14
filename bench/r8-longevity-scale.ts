import { Coordinator } from "../src/core/coordinator.ts";
import type { AgentRecord, ModelRoute, PersistedCoordinatorState } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };
const iterations = parsePositiveInt(process.env.R8_ITERATIONS ?? process.argv[2] ?? "2000");
const twoMiB = 2 * 1024 * 1024;

function parsePositiveInt(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`iteration count must be a positive integer: ${value}`);
  return parsed;
}

function percentile(values: number[], percentage: number): number {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * percentage))] ?? 0;
}

function timed<T>(samples: number[], operation: () => T): T {
  const started = performance.now();
  const result = operation();
  samples.push(performance.now() - started);
  return result;
}

function dispatch<T>(coordinator: Coordinator, rollback: PersistedCoordinatorState, actorId: string, operation: string, args: Record<string, unknown> = {}): T {
  // This is a valid-operation soak, so reuse one initial rollback image. The
  // broker's production path still takes a fresh image per request; keeping
  // it out of this loop isolates coordinator/GC growth from snapshot-copy
  // throughput and lets the gate exercise thousands of state transitions.
  return coordinator.dispatch(actorId, operation, args, rollback).value as T;
}

const coordinator = new Coordinator({
  rootId: "r8-benchmark",
  config: {
    maxTotalAgents: 64,
    maxChildrenPerAgent: 64,
    maxMailboxMessages: Math.max(8_192, iterations + 10),
    maxConcurrentAgents: 1,
    historyRetentionMs: 24 * 60 * 60 * 1000,
    maxArchivedRecords: 4_096,
  },
});
const root = dispatch<{ agent: AgentRecord; token: string }>(coordinator, coordinator.exportState(), "root", "agent.register", {
  rootId: "r8-benchmark",
  route,
  capabilities: { maySpawn: true, mayMessagePeers: true, mayWriteRepo: true, mayTransferOwnership: true },
});
const child = dispatch<{ agent: AgentRecord; token: string }>(coordinator, coordinator.exportState(), "child", "agent.register", {
  rootId: "r8-benchmark",
  parentId: root.agent.id,
  route,
  capabilities: { mayMessagePeers: true },
});
const rollback = coordinator.exportState();
const taskSamples: number[] = [];
const resourceSamples: number[] = [];
const messageSamples: number[] = [];

for (let index = 0; index < iterations; index += 1) {
  const task = timed(taskSamples, () => dispatch<{ id: string }>(coordinator, rollback, root.agent.id, "task.create", {
    description: `long-lived task ${index}`,
  }));
  dispatch(coordinator, rollback, root.agent.id, "task.update", {
    taskId: task.id,
    action: "complete",
    result: { summary: "completed", output: `result-${index}-`.padEnd(4096, "x") },
  });
  const resourceId = `dynamic-resource-${index}`;
  timed(resourceSamples, () => dispatch(coordinator, rollback, root.agent.id, "resource.define", {
    resourceId,
    kind: "file",
    path: `bench/r8/${resourceId}.ts`,
  }));
  dispatch(coordinator, rollback, root.agent.id, "resource.retire", { resourceId });
  timed(messageSamples, () => dispatch(coordinator, rollback, root.agent.id, "message.send", {
    to: child.agent.id,
    type: "inform",
    body: `long-lived message ${index} `.padEnd(4096, "m"),
  }));
}

// Exercise durable grant state and the restart/checkpoint shape without
// retaining an active turn in the final measurements.
dispatch(coordinator, rollback, root.agent.id, "agent.begin_turn", { operationId: "r8-root-turn" });
const queued = dispatch<{ queued?: boolean }>(coordinator, rollback, child.agent.id, "agent.begin_turn", { operationId: "r8-child-turn" });
if (queued.queued !== true) throw new Error("capacity contention did not queue the child turn");
dispatch(coordinator, rollback, root.agent.id, "agent.end_turn", { status: "ready" });
dispatch(coordinator, rollback, child.agent.id, "agent.begin_turn", { operationId: "r8-child-turn" });
dispatch(coordinator, rollback, child.agent.id, "agent.end_turn", { status: "ready" });

const status = coordinator.dispatch(root.agent.id, "fabric.status", {}).value as Record<string, unknown>;
const snapshot = coordinator.dispatch(root.agent.id, "fabric.snapshot", {}).value as Record<string, unknown>;
const statusBytes = Buffer.byteLength(JSON.stringify(status));
const snapshotBytes = Buffer.byteLength(JSON.stringify(snapshot));
if (statusBytes >= twoMiB || snapshotBytes >= twoMiB) throw new Error(`bounded projection exceeded 2 MiB: status=${statusBytes}, snapshot=${snapshotBytes}`);

const maintenanceSamples: number[] = [];
const maintenanceStarted = performance.now();
timed(maintenanceSamples, () => coordinator.maintenance());
const state = coordinator.exportState();
const restored = new Coordinator({
  rootId: "r8-benchmark",
  config: {
    maxTotalAgents: 64,
    maxChildrenPerAgent: 64,
    maxMailboxMessages: Math.max(8_192, iterations + 10),
    maxConcurrentAgents: 1,
    historyRetentionMs: 24 * 60 * 60 * 1000,
    maxArchivedRecords: 4_096,
  },
});
restored.restoreState(state);
const restoredStatus = restored.dispatch(root.agent.id, "fabric.status", {}).value as Record<string, unknown>;
const restoredBytes = Buffer.byteLength(JSON.stringify(restoredStatus));
if (restoredBytes >= twoMiB) throw new Error(`restored bounded projection exceeded 2 MiB: ${restoredBytes}`);

console.log(JSON.stringify({
  iterations,
  transitionMs: {
    taskCreateP50: percentile(taskSamples, 0.5),
    taskCreateP95: percentile(taskSamples, 0.95),
    resourceDefineP95: percentile(resourceSamples, 0.95),
    messageSendP95: percentile(messageSamples, 0.95),
  },
  maintenanceMs: performance.now() - maintenanceStarted,
  hotState: {
    agents: state.agents.length,
    tasks: state.tasks.length,
    resources: state.resources.length,
    messages: state.messages.length,
    archivedTasks: state.archivedTasks?.length ?? 0,
  },
  boundedBytes: { status: statusBytes, snapshot: snapshotBytes, restoredStatus: restoredBytes },
}, null, 2));
