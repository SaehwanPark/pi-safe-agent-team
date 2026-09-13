import test from "node:test";
import assert from "node:assert/strict";
import { Coordinator } from "../src/core/coordinator.ts";
import { DEFAULT_FABRIC_CONFIG, type AgentRecord, type FabricConfig, type ModelRoute, type TaskRecord } from "../src/core/types.ts";
import { effectivePrefillBudget, modelRouteCapacity, modelRoutePolicy } from "../src/core/coordinator-wire.ts";
import { classifyRootDelivery } from "../src/pi/delivery.ts";
import { ModelRouteCapacityArbiter } from "../src/pi/model-capacity.ts";
import { classifyAssistantMessage, classifyCompactionFailure, isBlockingOutcome } from "../src/pi/turn-outcome.ts";
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
  assert.equal(classifyAssistantMessage({ ...base, stopReason: "error", errorMessage: "CUDA out of memory during decode" }, 131_072).kind, "runtime_memory_pressure");
  assert.equal(classifyAssistantMessage({ ...base, stopReason: "error", errorMessage: "prompt is too long: 140000 tokens" }, 131_072).kind, "context_overflow");
  assert.equal(classifyAssistantMessage({ ...base, stopReason: "error", errorMessage: "service unavailable (503)" }, 131_072).kind, "transient_error_exhausted");
  assert.equal(classifyCompactionFailure("summary backend failed").kind, "compaction_failed");
  assert.equal(isBlockingOutcome(classifyCompactionFailure()), true);
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
    if (operation === "task.update") return {};
    if (operation === "message.send") return {};
    if (operation === "agent.update") return { ...record, contextDiagnostic: args.contextDiagnostic };
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

test("R2 child terminal provider error blocks the task and does not become ready or loop", async () => {
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
  assert.equal(promptCalls, 2, "one capacity relief retry is allowed, then the blocked gate prevents later wakes");
  assert.equal(calls.filter((call) => call.operation === "task.update")[0]?.args.action, "block");
  assert.equal(calls.filter((call) => call.operation === "agent.end_turn")[0]?.args.status, "blocked");
  assert.equal(calls.some((call) => call.operation === "agent.end_turn" && call.args.status === "ready"), false);
  assert.equal(calls.some((call) => call.operation === "message.send" && call.args.type === "blocked"), true);
});

test("R2 child capacity failure retries once before a successful turn", async () => {
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
  assert.equal(promptCalls, 2);
  assert.equal(calls.some((call) => call.operation === "agent.end_turn" && call.args.status === "ready"), true);
  assert.equal(calls.some((call) => call.operation === "task.update"), false);
});

