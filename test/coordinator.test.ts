import test from "node:test";
import assert from "node:assert/strict";
import { Coordinator } from "../src/core/coordinator.ts";
import { FabricError } from "../src/core/errors.ts";
import type { AgentMessage, AgentRecord, ModelRoute } from "../src/core/types.ts";

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

function registerRoot(coordinator: Coordinator, capabilities: Partial<AgentRecord["capabilities"]> = {}): AgentRecord {
  return coordinator.dispatch("root", "agent.register", {
    rootId: "fabric",
    role: "root",
    route,
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

function registerChild(coordinator: Coordinator, id: string, parentId = "root", capabilities: Partial<AgentRecord["capabilities"]> = {}): AgentRecord {
  return coordinator.dispatch(id, "agent.register", {
    rootId: "fabric",
    parentId,
    role: "worker",
    route,
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
