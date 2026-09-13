import test from "node:test";
import assert from "node:assert/strict";
import { Coordinator } from "../src/core/coordinator.ts";
import { FabricError } from "../src/core/errors.ts";
import type { AgentMessage, AgentRecord, ModelRoute, ResourceRecord } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

function makeCoordinator(config: Record<string, number> = {}, nowRef?: { value: number }): Coordinator {
  let sequence = 0;
  return new Coordinator({
    rootId: "fabric",
    config,
    clock: () => nowRef?.value ?? 1_000,
    idFactory: (prefix) => `${prefix}-${++sequence}`,
  });
}

function registerRoot(coordinator: Coordinator, capabilities: Partial<AgentRecord["capabilities"]> = {}, routeValue = route): AgentRecord {
  return coordinator.dispatch("root", "agent.register", {
    rootId: "fabric",
    role: "root",
    route: routeValue,
    capabilities: {
      maySpawn: true,
      mayMessagePeers: true,
      mayEscalate: true,
      mayTransferOwnership: true,
      mayWriteRepo: true,
      mayUseShell: true,
      ...capabilities,
    },
  }).value.agent;
}

function registerChild(coordinator: Coordinator, id: string, parentId = "root", capabilities: Partial<AgentRecord["capabilities"]> = {}, routeValue = route): AgentRecord {
  return coordinator.dispatch(id, "agent.register", {
    rootId: "fabric",
    parentId,
    role: "worker",
    route: routeValue,
    capabilities: { mayMessagePeers: true, ...capabilities },
  }).value.agent;
}

function expectCode(fn: () => unknown, code: string): void {
  assert.throws(fn, (error: unknown) => error instanceof FabricError && error.code === code);
}

test("recursive spawn is bounded and preserves parent identity", () => {
  const coordinator = makeCoordinator({ maxDepth: 1, maxChildrenPerAgent: 1, maxTotalAgents: 4 });
  registerRoot(coordinator);
  const spawned = coordinator.dispatch("root", "agent.spawn", { role: "worker", route, capabilities: { maySpawn: true } }).value as { agent: AgentRecord; token: string };
  assert.equal(spawned.agent.parentId, "root");
  assert.equal(spawned.agent.depth, 1);
  assert.ok(spawned.token);
  expectCode(() => coordinator.dispatch("root", "agent.spawn", { role: "second", route }), "AGENT_LIMIT_REACHED");
  expectCode(() => coordinator.dispatch(spawned.agent.id, "agent.spawn", { route }), "AGENT_LIMIT_REACHED");
});

test("maxChildrenPerAgent limits live children, not historical creations", () => {
  const coordinator = makeCoordinator({ maxChildrenPerAgent: 1, maxTotalAgents: 8 });
  registerRoot(coordinator);
  const first = coordinator.dispatch("root", "agent.spawn", { route }).value.agent as AgentRecord;
  expectCode(() => coordinator.dispatch("root", "agent.spawn", { route }), "AGENT_LIMIT_REACHED");
  coordinator.dispatch("root", "agent.cancel", { agentId: first.id });
  const replacement = coordinator.dispatch("root", "agent.spawn", { route }).value.agent as AgentRecord;
  assert.notEqual(replacement.id, first.id);
});

test("failed parents cascade cancellation to every descendant", () => {
  const coordinator = makeCoordinator({ maxChildrenPerAgent: 4, maxTotalAgents: 8 });
  registerRoot(coordinator);
  const child = coordinator.dispatch("root", "agent.spawn", { route, capabilities: { maySpawn: true } }).value.agent as AgentRecord;
  const grandchild = coordinator.dispatch(child.id, "agent.spawn", { route }).value.agent as AgentRecord;
  const failed = coordinator.dispatch(child.id, "agent.end_turn", { status: "failed", statusReason: "provider failed" });
  assert.deepEqual(
    failed.events.filter((event) => event.type === "agent_updated").map((event) => [event.agent.id, event.agent.status]),
    [[grandchild.id, "cancelled"], [child.id, "failed"]],
  );
  assert.equal(coordinator.dispatch("root", "agent.status", { agentId: child.id }).value.status, "failed");
  assert.equal(coordinator.dispatch("root", "agent.status", { agentId: grandchild.id }).value.status, "cancelled");
});

test("an agent cannot complete while it still has live descendants", () => {
  const coordinator = makeCoordinator({ maxChildrenPerAgent: 4, maxTotalAgents: 8 });
  registerRoot(coordinator);
  const child = coordinator.dispatch("root", "agent.spawn", { route, capabilities: { maySpawn: true } }).value.agent as AgentRecord;
  coordinator.dispatch(child.id, "agent.spawn", { route });
  expectCode(() => coordinator.dispatch(child.id, "agent.end_turn", { status: "completed" }), "LIFECYCLE_CONFLICT");
  assert.equal(coordinator.dispatch("root", "agent.status", { agentId: child.id }).value.status, "starting");
});

test("draining freezes new work until the subtree is cancelled", () => {
  const coordinator = makeCoordinator({ maxChildrenPerAgent: 4, maxTotalAgents: 8 });
  registerRoot(coordinator);
  const child = coordinator.dispatch("root", "agent.spawn", { route, capabilities: { maySpawn: true } }).value.agent as AgentRecord;
  coordinator.dispatch("root", "agent.drain", { agentId: child.id, reason: "quota emergency" });
  assert.equal(coordinator.dispatch("root", "agent.status", { agentId: child.id }).value.status, "draining");
  expectCode(() => coordinator.dispatch(child.id, "agent.begin_turn", {}), "LIFECYCLE_CONFLICT");
  expectCode(() => coordinator.dispatch(child.id, "agent.spawn", { route }), "LIFECYCLE_CONFLICT");
  coordinator.dispatch("root", "agent.cancel", { agentId: child.id });
  assert.equal(coordinator.dispatch("root", "agent.status", { agentId: child.id }).value.status, "cancelled");
});

test("one of 100 concurrently scheduled task claimers wins atomically", async () => {
  const coordinator = makeCoordinator({ maxTotalAgents: 128, maxChildrenPerAgent: 128 });
  registerRoot(coordinator);
  const workers = Array.from({ length: 100 }, (_, index) => registerChild(coordinator, `worker-${index}`));
  const task = coordinator.dispatch("root", "task.create", { description: "single winner" }).value;
  let winners = 0;
  await Promise.all(workers.map(async (worker) => {
    await Promise.resolve();
    try {
      coordinator.dispatch(worker.id, "task.claim", { taskId: task.id });
      winners += 1;
    } catch (error) {
      assert.ok(error instanceof FabricError);
      assert.equal(error.code, "TASK_BUSY");
    }
  }));
  assert.equal(winners, 1);
  assert.equal(coordinator.dispatch("root", "task.show", { taskId: task.id }).value.owner, "worker-0");
});

test("shared readers coexist while mutable access waits across resource hierarchy", () => {
  const coordinator = makeCoordinator({ maxTotalAgents: 10 });
  registerRoot(coordinator);
  registerChild(coordinator, "reader-1", "root");
  registerChild(coordinator, "reader-2", "root");
  registerChild(coordinator, "writer", "root");
  coordinator.dispatch("root", "resource.define", { resourceId: "module:parser", kind: "module" });
  coordinator.dispatch("root", "resource.define", { resourceId: "file:parser.ts", kind: "file", parentId: "module:parser" });
  for (const id of ["reader-1", "reader-2", "writer"]) coordinator.dispatch("root", "resource.grant", { resourceId: "module:parser", agentId: id, permissions: ["read", "write"] });
  const first = coordinator.dispatch("reader-1", "resource.borrow", { resourceId: "file:parser.ts", mode: "shared" }).value;
  const second = coordinator.dispatch("reader-2", "resource.borrow", { resourceId: "module:parser", mode: "shared" }).value;
  assert.equal(first.status, "granted");
  assert.equal(second.status, "granted");
  const waiting = coordinator.dispatch("writer", "resource.borrow", { resourceId: "module:parser", mode: "mutable", wait: true }).value;
  assert.equal(waiting.status, "waiting");
  coordinator.dispatch("reader-1", "resource.release", { leaseId: first.leaseId });
  assert.equal(coordinator.dispatch("writer", "resource.inspect", { resourceId: "module:parser" }).value.mutableHold?.agentId, undefined);
  coordinator.dispatch("reader-2", "resource.release", { leaseId: second.leaseId });
  const inspect = coordinator.dispatch("writer", "resource.inspect", { resourceId: "module:parser" }).value;
  assert.equal(inspect.mutableHold?.agentId, "writer");
  assert.equal(inspect.waiters.length, 0);
  assert.ok((coordinator.dispatch("writer", "message.inbox", {}).value as AgentMessage[]).some((message) => message.type === "resource_granted"));
});

test("durable mailbox retains busy messages and request/reply never blocks", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  registerChild(coordinator, "child");
  const first = coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "one" }).value.message;
  const second = coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "two" }).value.message;
  const inbox = coordinator.dispatch("child", "message.inbox", {}).value as AgentMessage[];
  assert.deepEqual(inbox.map((message) => message.body), ["one", "two"]);
  coordinator.dispatch("child", "message.ack", { messageId: first.id });
  assert.deepEqual((coordinator.dispatch("child", "message.inbox", {}).value as AgentMessage[]).map((message) => message.body), ["two"]);

  const request = coordinator.dispatch("root", "message.send", { to: "child", type: "clarification", body: "which parser?", expectsReply: true }).value;
  assert.ok(request.request?.id);
  assert.equal(coordinator.dispatch("child", "message.reply", { requestId: request.request?.id, body: "parser-v2" }).value.request.status, "resolved");
  assert.ok((coordinator.dispatch("root", "message.inbox", {}).value as AgentMessage[]).some((message) => message.replyTo === request.message.id));
});

test("peer visibility is scoped and sender identity cannot be spoofed", () => {
  const coordinator = makeCoordinator({ maxTotalAgents: 10 });
  registerRoot(coordinator);
  registerChild(coordinator, "peer-a", "root");
  registerChild(coordinator, "peer-b", "root");
  const sent = coordinator.dispatch("peer-a", "message.send", { to: "peer-b", type: "inform", body: "finding" }).value.message;
  assert.equal(sent.from, "peer-a");
  const parentMessage = coordinator.dispatch("peer-a", "message.send", { to: "root", type: "inform", body: "allowed parent", from: "root" }).value.message as typeof sent;
  assert.equal(parentMessage.from, "peer-a");
  assert.equal(coordinator.dispatch("peer-a", "discover.agents", { scope: "siblings" }).value.length, 2);
});

test("lease expiry and cancellation release runtime claims", () => {
  const now = { value: 1000 };
  const coordinator = makeCoordinator({ leaseMs: 100 }, now);
  registerRoot(coordinator);
  registerChild(coordinator, "child");
  coordinator.dispatch("root", "resource.define", { resourceId: "file:a", kind: "file" });
  coordinator.dispatch("root", "resource.grant", { resourceId: "file:a", agentId: "child", permissions: ["read", "write"] });
  coordinator.dispatch("child", "resource.borrow", { resourceId: "file:a", mode: "mutable" });
  now.value = 2500;
  coordinator.maintenance();
  assert.equal(coordinator.dispatch("root", "resource.inspect", { resourceId: "file:a" }).value.mutableHold, undefined);
  coordinator.dispatch("child", "resource.borrow", { resourceId: "file:a", mode: "mutable" });
  coordinator.dispatch("root", "agent.cancel", { agentId: "child" });
  assert.equal(coordinator.dispatch("root", "resource.inspect", { resourceId: "file:a" }).value.mutableHold, undefined);
  assert.equal(coordinator.dispatch("root", "agent.status", { agentId: "child" }).value.status, "cancelled");
});

test("blocked children release mutable holds while retaining task ownership", () => {
  const coordinator = makeCoordinator({ maxTotalAgents: 8 }, { value: 1_000 });
  registerRoot(coordinator);
  const blocked = registerChild(coordinator, "blocked");
  const waiter = registerChild(coordinator, "waiter");
  const task = coordinator.dispatch("root", "task.create", { description: "recoverable task" }).value;
  coordinator.dispatch(blocked.id, "task.claim", { taskId: task.id });
  coordinator.dispatch("root", "resource.define", { resourceId: "file:shared.ts", kind: "file", path: "shared.ts" });
  for (const agentId of [blocked.id, waiter.id]) coordinator.dispatch("root", "resource.grant", { resourceId: "file:shared.ts", agentId, permissions: ["read", "write"] });
  coordinator.dispatch(blocked.id, "resource.borrow", { resourceId: "file:shared.ts", mode: "mutable" });
  coordinator.dispatch(blocked.id, "agent.begin_turn", {});
  const waiting = coordinator.dispatch(waiter.id, "resource.borrow", { resourceId: "file:shared.ts", mode: "mutable", wait: true }).value as { status: string };
  assert.equal(waiting.status, "waiting");

  coordinator.dispatch(blocked.id, "agent.end_turn", { status: "blocked", statusReason: "provider unavailable" });
  const resource = coordinator.dispatch("root", "resource.inspect", { resourceId: "file:shared.ts" }).value as ResourceRecord;
  assert.equal(resource.mutableHold?.agentId, waiter.id);
  assert.equal(resource.waiters.length, 0);
  assert.equal(coordinator.dispatch("root", "task.show", { taskId: task.id }).value.owner, blocked.id);
  assert.equal(coordinator.dispatch("root", "agent.status", { agentId: blocked.id }).value.status, "blocked");
});

test("inbox cursor drains durable messages beyond one recovery batch", () => {
  const coordinator = makeCoordinator({ maxMailboxMessages: 512, maxTotalAgents: 4 });
  registerRoot(coordinator);
  registerChild(coordinator, "child");
  for (let index = 0; index < 205; index += 1) {
    coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: `message-${index}` });
  }
  const received: AgentMessage[] = [];
  let afterBrokerSequence = 0;
  while (received.length < 205) {
    const batch = coordinator.dispatch("child", "message.inbox", { limit: 100, afterBrokerSequence }).value as AgentMessage[];
    assert.ok(batch.length > 0);
    received.push(...batch);
    afterBrokerSequence = batch.at(-1)?.brokerSequence ?? afterBrokerSequence;
  }
  assert.equal(received.length, 205);
  assert.equal(new Set(received.map((message) => message.id)).size, 205);
  assert.deepEqual(received.map((message) => message.body), Array.from({ length: 205 }, (_, index) => `message-${index}`));
  assert.deepEqual((coordinator.dispatch("child", "message.inbox", { limit: 100, afterBrokerSequence }).value as AgentMessage[]), []);
});

test("capacity groups serialize semantic route aliases", () => {
  const routeA: ModelRoute = { provider: "alias-a", model: "model", thinking: "medium", capacityGroup: "gpu-0" };
  const routeB: ModelRoute = { provider: "alias-b", model: "model", thinking: "medium", capacityGroup: "gpu-0" };
  const coordinator = new Coordinator({
    rootId: "fabric",
    config: {
      modelRoutePolicies: {
        "alias-a/model": { maxConcurrent: 2, capacityGroup: "gpu-0" },
        "alias-b/model": { maxConcurrent: 1, capacityGroup: "gpu-0" },
      },
    },
  });
  registerRoot(coordinator, {}, routeA);
  const child = registerChild(coordinator, "alias-child", "root", {}, routeB);
  const secondAliasChild = registerChild(coordinator, "alias-child-2", "root", {}, routeA);
  assert.equal(coordinator.dispatch("root", "agent.begin_turn", {}).value.started, true);
  assert.deepEqual(coordinator.dispatch(child.id, "agent.begin_turn", {}).value, {
    started: false,
    reason: "model route capacity reached for alias-b/model",
  });
  coordinator.dispatch("root", "agent.end_turn", { status: "ready" });
  assert.equal(coordinator.dispatch(child.id, "agent.begin_turn", {}).value.started, true);
  assert.equal(coordinator.dispatch(secondAliasChild.id, "agent.begin_turn", {}).value.started, false);
});

test("message dedupe returns the original message", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  registerChild(coordinator, "child");
  const first = coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "same", clientDedupeKey: "k" }).value.message;
  const second = coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "different", clientDedupeKey: "k" }).value.message;
  assert.equal(second.id, first.id);
  assert.equal(coordinator.dispatch("child", "message.inbox", {}).value.length, 1);
});

test("same-owner claim is idempotent and release requires exactly one selector", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  coordinator.dispatch("root", "resource.define", { resourceId: "file:a.ts", kind: "file", path: "a.ts" });
  coordinator.dispatch("root", "resource.define", { resourceId: "file:b.ts", kind: "file", path: "b.ts" });
  const defined = coordinator.dispatch("root", "resource.define", { resourceId: "file:c.ts", kind: "file", path: "c.ts" });
  registerChild(coordinator, "worker");
  // define makes the definer the owner at version 1; a same-owner claim must be
  // an idempotent reaffirmation that never bumps the version.
  assert.equal((defined.value as { version: number }).version, 1);
  const reclaim = coordinator.dispatch("root", "resource.claim", { resourceId: "file:a.ts" });
  assert.equal((reclaim.value as { version: number; owner: string }).owner, "root");
  assert.equal((reclaim.value as { version: number }).version, 1);
  assert.deepEqual(reclaim.events, []);
  // Ownership transitions still bump, and a claim by the new owner is idempotent again.
  coordinator.dispatch("root", "resource.transfer", { resourceId: "file:a.ts", agentId: "worker" });
  const workerClaim = coordinator.dispatch("worker", "resource.claim", { resourceId: "file:a.ts" });
  assert.equal((workerClaim.value as { version: number }).version, 2);
  assert.deepEqual(workerClaim.events, []);
  const rootReclaim = coordinator.dispatch("root", "resource.claim", { resourceId: "file:a.ts" });
  assert.equal((rootReclaim.value as { version: number }).version, 3);

  coordinator.dispatch("root", "resource.borrow", { resourceId: "file:a.ts", mode: "mutable" });
  coordinator.dispatch("root", "resource.borrow", { resourceId: "file:b.ts", mode: "shared" });
  expectCode(() => coordinator.dispatch("root", "resource.release", {}), "INVALID_ARGUMENT");
  expectCode(() => coordinator.dispatch("root", "resource.release", { resourceId: "file:a.ts", all: true }), "INVALID_ARGUMENT");
  expectCode(() => coordinator.dispatch("root", "resource.release", { all: "yes" }), "INVALID_ARGUMENT");
  const releasedAll = coordinator.dispatch("root", "resource.release", { all: true });
  assert.equal((releasedAll.value as { released: boolean }).released, true);
  const again = coordinator.dispatch("root", "resource.release", { all: true });
  assert.equal((again.value as { released: boolean }).released, false);
});

test("case-insensitive policy config folds declarations and checks together", () => {
  const registerFabric = (config: Record<string, unknown>) => {
    const coordinator = new Coordinator({ rootId: "fabric", config: config as never, clock: () => 1_000 });
    registerRoot(coordinator);
    registerChild(coordinator, "worker", "root", { mayWriteRepo: true });
    coordinator.dispatch("root", "resource.define", { resourceId: "file:foo", kind: "file", path: "src/Foo.ts", permissions: ["write"] });
    coordinator.dispatch("root", "resource.grant", { resourceId: "file:foo", agentId: "worker", permissions: ["write"] });
    coordinator.dispatch("worker", "resource.borrow", { resourceId: "file:foo", mode: "mutable" });
    return coordinator;
  };
  const folding = registerFabric({ caseInsensitivePaths: true });
  const hit = folding.dispatch("worker", "resource.check_write", { path: "SRC/FOO.TS" });
  assert.equal((hit.value as { allowed: boolean }).allowed, true);
  assert.equal((hit.value as { resourceId?: string }).resourceId, "file:foo");
  // A strictly case-sensitive coordinator must keep the two spellings apart.
  const strict = registerFabric({ caseInsensitivePaths: false });
  const miss = strict.dispatch("worker", "resource.check_write", { path: "SRC/FOO.TS" });
  assert.equal((miss.value as { allowed: boolean }).allowed, false);
  const exact = strict.dispatch("worker", "resource.check_write", { path: "src/Foo.ts" });
  assert.equal((exact.value as { allowed: boolean }).allowed, true);
});
