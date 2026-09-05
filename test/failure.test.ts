import test from "node:test";
import assert from "node:assert/strict";
import { Coordinator } from "../src/core/coordinator.ts";
import { FabricError } from "../src/core/errors.ts";
import type { ModelRoute } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

function code(fn: () => unknown, expected: string): void {
  assert.throws(fn, (error: unknown) => error instanceof FabricError && error.code === expected);
}

test("capability ceilings and concurrency limits fail closed", () => {
  const coordinator = new Coordinator({ rootId: "fabric", config: { maxConcurrentAgents: 1, maxTotalAgents: 4 }, idFactory: (() => { let n = 0; return (prefix: string) => `${prefix}-${++n}`; })() });
  coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true, mayMessagePeers: true } });
  const a = coordinator.dispatch("root", "agent.spawn", { route, capabilities: { mayMessagePeers: false } }).value.agent;
  const b = coordinator.dispatch("root", "agent.spawn", { route, capabilities: { mayMessagePeers: true } }).value.agent;
  coordinator.dispatch(a.id, "agent.begin_turn", {});
  assert.equal(coordinator.dispatch(b.id, "agent.begin_turn", {}).value.started, false);
  code(() => coordinator.dispatch(a.id, "agent.update", { status: "completed" }), "LIFECYCLE_CONFLICT");
  code(() => coordinator.dispatch(a.id, "message.send", { to: b.id, type: "inform", body: "not allowed" }), "CAPABILITY_DENIED");
  assert.equal(coordinator.dispatch(a.id, "discover.agents", { scope: "siblings" }).value.length, 1);
});

test("cancelling an actor resolves its pending requests and is idempotent", () => {
  const coordinator = new Coordinator({ rootId: "fabric" });
  coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true, mayMessagePeers: true } });
  const child = coordinator.dispatch("root", "agent.spawn", { route }).value.agent;
  const request = coordinator.dispatch("root", "message.send", { to: child.id, type: "clarification", body: "answer?", expectsReply: true }).value;
  coordinator.dispatch("root", "agent.cancel", { agentId: child.id });
  assert.equal(coordinator.dispatch("root", "fabric.status", {}).value.pendingRequests.length, 0);
  code(() => coordinator.dispatch(child.id, "message.reply", { requestId: request.request.id, body: "late" }), "REQUEST_ALREADY_RESOLVED");
  assert.deepEqual(coordinator.dispatch("root", "agent.cancel", { agentId: child.id }).value.cancelled, []);
});
