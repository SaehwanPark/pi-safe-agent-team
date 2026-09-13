import test from "node:test";
import assert from "node:assert/strict";
import { Coordinator } from "../src/core/coordinator.ts";
import { DEFAULT_FABRIC_CONFIG, type AgentRecord, type FabricConfig, type ModelRoute, type TaskRecord } from "../src/core/types.ts";
import { effectivePrefillBudget, modelRouteCapacity, modelRouteCapacityKey, modelRoutePolicy } from "../src/core/coordinator-wire.ts";
import { classifyRootDelivery } from "../src/pi/delivery.ts";
import { ModelRouteCapacityArbiter } from "../src/pi/model-capacity.ts";
import { classifyAssistantMessage, classifyCompactionFailure, isBlockingOutcome, isAbortLikeMessage } from "../src/pi/turn-outcome.ts";
import { FabricRuntime, ManagedChild } from "../src/pi/runtime.ts";

const localRoute: ModelRoute = { provider: "omlx", model: "qwen3", thinking: "medium" };

function capabilities(): AgentRecord["capabilities"] {
  return {
    maySpawn: true,
    mayMessagePeers: true,
    mayEscalate: true,
    mayTransferOwnership: true,
    mayWriteRepo: true,
    mayUseShell: true,
    peerIds: [],
    resourceGrants: {},
  };
}

function registerRoot(coordinator: Coordinator, route = localRoute): AgentRecord {
  return coordinator.dispatch("root", "agent.register", {
    rootId: "fabric",
    role: "root",
    route,
    capabilities: capabilities(),
  }).value.agent;
}

test("R2 outcome classifier separates logical overflow, prefill capacity, runtime pressure, and transient errors", () => {
  const base = { role: "assistant", usage: { input: 47_000, cacheRead: 0, output: 0 } };
  assert.equal(classifyAssistantMessage({ ...base, stopReason: "error", errorMessage: "omlx_code: prefill_memory_exceeded" }, 131_072).kind, "prefill_capacity");
  assert.equal(classifyAssistantMessage({ ...base, stopReason: "error", diagnostics: [{ error: { code: "prefill_memory_exceeded", message: "backend rejected prefill" } }] }, 131_072).kind, "prefill_capacity");
  assert.equal(classifyAssistantMessage({ ...base, stopReason: "error", errorMessage: "prefill rejected after Metal memory pressure" }, 131_072).kind, "prefill_capacity");
  assert.equal(classifyAssistantMessage({ ...base, stopReason: "error", errorMessage: "CUDA out of memory during decode" }, 131_072).kind, "runtime_memory_pressure");
  assert.equal(classifyAssistantMessage({ ...base, stopReason: "error", errorMessage: "prompt is too long: 140000 tokens" }, 131_072).kind, "context_overflow");
  assert.equal(classifyAssistantMessage({ ...base, stopReason: "error", errorMessage: "service unavailable (503)" }, 131_072).kind, "transient_error_exhausted");
  assert.equal(classifyCompactionFailure("summary backend failed").kind, "compaction_failed");
  assert.equal(isBlockingOutcome(classifyCompactionFailure()), true);
});

test("abort-like provider errors are recognized without broad substring matching", () => {
  const base = { role: "assistant", usage: { input: 1, cacheRead: 0, output: 0 } };
  assert.equal(classifyAssistantMessage({ ...base, stopReason: "error", errorMessage: "This operation was aborted" }).kind, "aborted");
  assert.equal(classifyAssistantMessage({ ...base, stopReason: "error", errorMessage: "AbortError: request cancelled by user" }).kind, "aborted");
  assert.equal(isAbortLikeMessage("error", "provider reported an aborted upstream request"), false);
  assert.equal(classifyAssistantMessage({ ...base, stopReason: "error", errorMessage: "provider reported an aborted upstream request" }).kind, "fatal_provider");
});
test("R2 route policy coordinates broker turns and derives an effective prefill budget", () => {
  const config: FabricConfig = {
    ...DEFAULT_FABRIC_CONFIG,
    modelRoutePolicies: { "omlx/qwen3": { maxConcurrent: 1, effectivePrefillBudget: 48_000 } },
  };
  assert.deepEqual(modelRoutePolicy(config, localRoute), { maxConcurrent: 1, effectivePrefillBudget: 48_000 });
  assert.equal(modelRouteCapacity(config, localRoute), 1);
  assert.equal(effectivePrefillBudget(config, localRoute, 131_072), 48_000);

  const coordinator = new Coordinator({ rootId: "fabric", config });
  registerRoot(coordinator);
  const child = coordinator.dispatch("root", "agent.spawn", { role: "worker", route: localRoute, capabilities: { mayMessagePeers: true } }).value.agent as AgentRecord;
  assert.deepEqual(coordinator.dispatch("root", "agent.begin_turn", {}).value, { started: true });
  assert.deepEqual(coordinator.dispatch(child.id, "agent.begin_turn", {}).value, {
    started: false,
    reason: "model route capacity reached for omlx/qwen3",
  });
  coordinator.dispatch("root", "agent.end_turn", { status: "ready" });
  assert.deepEqual(coordinator.dispatch(child.id, "agent.begin_turn", {}).value, { started: true });
});

test("R2 local route arbiter serializes heavy operations and releases FIFO waiters", async () => {
  const arbiter = new ModelRouteCapacityArbiter({
    ...DEFAULT_FABRIC_CONFIG,
    modelRoutePolicies: { "omlx/qwen3": { maxConcurrent: 1 } },
  });
  const first = await arbiter.acquire(localRoute);
  let secondEntered = false;
  const second = arbiter.acquire(localRoute).then((release) => {
    secondEntered = true;
    return release;
  });
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(secondEntered, false);
  first();
  const releaseSecond = await second;
  assert.equal(secondEntered, true);
  releaseSecond();
});

test("explicit capacity groups share the strictest alias limit", async () => {
  const routeA: ModelRoute = { provider: "home-a", model: "qwen", thinking: "medium", capacityGroup: "gpu-0" };
  const routeB: ModelRoute = { provider: "home-b", model: "qwen", thinking: "medium", capacityGroup: "gpu-0" };
  const config: FabricConfig = {
    ...DEFAULT_FABRIC_CONFIG,
    modelRoutePolicies: {
      "home-a/qwen": { maxConcurrent: 2, capacityGroup: "gpu-0" },
      "home-b/qwen": { maxConcurrent: 1, capacityGroup: "gpu-0" },
    },
  };
  assert.equal(modelRouteCapacityKey(config, routeA), "gpu-0");
  assert.equal(modelRouteCapacityKey(config, routeB), "gpu-0");
  const arbiter = new ModelRouteCapacityArbiter(config);
  const first = await arbiter.acquire(routeA);
  let entered = false;
  const queued = arbiter.acquire(routeB).then((release) => {
    entered = true;
    return release;
  });
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(entered, false);
  first();
  const release = await queued;
  assert.equal(entered, true);
  release();
});

test("R2 root delivery remains durable/context-only during compaction or degraded context", () => {
  const message = {
    id: "message-1",
    from: "child-1",
    to: "root",
    type: "blocked" as const,
    body: "prefill capacity",
    senderSequence: 1,
    priority: "urgent" as const,
    createdAt: Date.now(),
  };
  for (const state of [{ rootCompactionInFlight: true }, { rootContextHealth: "degraded" as const }]) {
    assert.deepEqual(classifyRootDelivery(message, state), {
      triggerTurn: false,
      deliverAs: "nextTurn",
      modelVisible: true,
      display: true,
    });
  }
});

test("R2 fabric snapshots fail closed while root compaction is in flight", async () => {
  const runtime = new FabricRuntime({ cwd: "/test/repo", startBroker: false });
  (runtime as any).root = { agentId: "root-1", ctx: { cwd: "/test/repo", sessionId: "sess-1" } };
  (runtime as any).status = async () => ({
    rootId: "root-1",
    agents: [{ id: "root-1", depth: 0, role: "root", route: { provider: "openai", model: "gpt", thinking: "off" }, status: "ready", lastActivity: Date.now() }],
    tasks: [],
    resources: [],
    pendingRequests: [],
    recentMessages: [],
    runningChildren: 0,
    config: DEFAULT_FABRIC_CONFIG,
    activeFences: 0,
  });
  (runtime as any).rootCompactionInFlight = 1;
  const snapshot = await runtime.getFabricStateSnapshot({ cwd: "/test/repo" });
  assert.ok(snapshot);
  assert.equal(snapshot.rootCompactionInFlight, true);
  assert.equal(snapshot.quiescent, false);
  assert.ok(snapshot.quiescenceReasons.includes("root_compaction_in_flight"));
  assert.equal(snapshot.sessionReplacementSafe, false);
});

function makePromptChild(prompt: () => Promise<void>, calls: Array<{ operation: string; args: any }>): ManagedChild {
  const runtime = {
    fabricId: "fabric",
    isDraining: false,
    config: { heartbeatMs: 60_000 },
    withModelRouteCapacity: async (_route: ModelRoute, operation: () => Promise<void>) => operation(),
    getEffectivePrefillBudget: (_route: ModelRoute, logical?: number) => logical,
    workspaceStrategy: { cleanup: async () => {} },
    onChildStopped() {},
  } as never;
  const child = new ManagedChild(runtime, {
    agentId: "child-1",
    token: "token",
    parentId: "root",
    role: "worker",
    route: localRoute,
    taskId: "task-1",
    cwd: "/test/repo",
    stateDirectory: "/test/repo",
    agentDir: "/test/repo",
    endpoint: "unused",
    model: { provider: localRoute.provider, id: localRoute.model, contextWindow: 131_072 } as never,
    capabilities: capabilities(),
  });
  const record: AgentRecord = {
    id: "child-1",
    rootId: "fabric",
    parentId: "root",
    depth: 1,
    role: "worker",
    taskId: "task-1",
    route: localRoute,
    capabilities: capabilities(),
    status: "running",
    createdAt: 1,
    lastActivity: 1,
    childrenCreated: 0,
  };
  (child.client as any).request = async (operation: string, args: any = {}) => {
    calls.push({ operation, args });
    if (operation === "agent.begin_turn") return { started: true };
    if (operation === "agent.status") return record;
    if (operation === "task.show") return { id: "task-1", status: "active", owner: "child-1" };
    if (operation === "task.update") return {};
    if (operation === "message.send") return {};
    if (operation === "agent.update") return { ...record, contextDiagnostic: args.contextDiagnostic };
    if (operation === "agent.finish_turn") return { agent: { ...record, status: args.status, statusReason: args.statusReason } };
    if (operation === "agent.end_turn") return { agent: { ...record, status: args.status, statusReason: args.statusReason } };
    return {};
  };
  (child as any).session = {
    isStreaming: false,
    prompt,
    subscribe: () => () => {},
  };
  (child as any).record = record;
  return child;
}

test("R3 child terminal provider error blocks the task and does not become ready or loop", async () => {
  let promptCalls = 0;
  const calls: Array<{ operation: string; args: any }> = [];
  let child!: ManagedChild;
  child = makePromptChild(async () => {
    promptCalls += 1;
    (child as any).observeSessionEvent({
      type: "agent_end",
      willRetry: false,
      messages: [{ role: "assistant", provider: "omlx", model: "qwen3", stopReason: "error", errorMessage: "omlx_code: prefill_memory_exceeded", usage: { input: 47_000, cacheRead: 0, output: 0 } }],
    });
  }, calls);

  await (child as any).executePrompt("first");
  await (child as any).executePrompt("second");
  assert.equal(promptCalls, 1, "public prompt() is never called twice for a same-context retry");
  assert.equal(calls.filter((call) => call.operation === "agent.finish_turn")[0]?.args.taskAction, "block");
  assert.equal(calls.filter((call) => call.operation === "agent.finish_turn")[0]?.args.status, "blocked");
  assert.equal(calls.some((call) => call.operation === "agent.end_turn" && call.args.status === "ready"), false);
  assert.equal(calls.some((call) => call.operation === "agent.finish_turn" && call.args.metadata?.cause === "prefill_capacity"), true);
});

test("R3 child capacity failure blocks without duplicating the user turn", async () => {
  let promptCalls = 0;
  const calls: Array<{ operation: string; args: any }> = [];
  let child!: ManagedChild;
  child = makePromptChild(async () => {
    promptCalls += 1;
    if (promptCalls === 1) throw new Error("omlx_code: prefill_memory_exceeded");
    (child as any).observeSessionEvent({
      type: "agent_end",
      willRetry: false,
      messages: [{ role: "assistant", provider: "omlx", model: "qwen3", stopReason: "stop", usage: { input: 12_000, cacheRead: 0, output: 32 } }],
    });
  }, calls);

  await (child as any).executePrompt("retryable");
  assert.equal(promptCalls, 1);
  assert.equal(calls.some((call) => call.operation === "agent.finish_turn" && call.args.status === "blocked"), true);
  assert.equal(calls.some((call) => call.operation === "agent.finish_turn" && call.args.taskAction === "block"), true);
});

test("R2 embedded compaction diagnostics block instead of laundering the turn to ready", async () => {
  const calls: Array<{ operation: string; args: any }> = [];
  let child!: ManagedChild;
  child = makePromptChild(async () => {
    (child as any).observeSessionEvent({
      type: "agent_end",
      willRetry: false,
      messages: [{ role: "assistant", provider: "omlx", model: "qwen3", stopReason: "stop", usage: { input: 12_000, cacheRead: 0, output: 32 } }],
    });
    (child as any).compactionFailure = classifyCompactionFailure("Embedded compaction failed: summary backend unavailable");
  }, calls);

  await (child as any).executePrompt("compaction-failed");
  assert.equal(calls.some((call) => call.operation === "agent.finish_turn" && call.args.status === "blocked"), true);
  assert.equal(calls.some((call) => call.operation === "agent.finish_turn" && call.args.status === "ready"), false);
});
