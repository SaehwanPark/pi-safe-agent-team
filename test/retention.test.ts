import test from "node:test";
import assert from "node:assert/strict";
import { Coordinator } from "../src/core/coordinator.ts";
import type { AgentMessage, ModelRoute } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

test("message retention removes only acknowledged messages and emits a replayable prune event", () => {
  const coordinator = new Coordinator({ rootId: "fabric", config: { messageRetention: 2 }, idFactory: (() => { let n = 0; return (prefix: string) => `${prefix}-${++n}`; })() });
  coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: { mayMessagePeers: true } });
  coordinator.dispatch("child", "agent.register", { rootId: "fabric", parentId: "root", route });
  const ids: string[] = [];
  for (const body of ["one", "two", "three"]) ids.push((coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body }).value as { message: AgentMessage }).message.id);
  // Only acknowledged records may be pruned.
  coordinator.dispatch("child", "message.ack", { messageId: ids[0] });
  coordinator.dispatch("child", "message.ack", { messageId: ids[1] });
  const fourth = coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "four" });
  assert.ok(fourth.events.some((event) => event.type === "messages_pruned"));
  const inbox = coordinator.dispatch("child", "message.inbox", {}).value as AgentMessage[];
  assert.deepEqual(inbox.map((message) => message.body), ["three", "four"]);
});

test("pruned dedupe keys can be reused", () => {
  const coordinator = new Coordinator({ rootId: "fabric", config: { messageRetention: 1 } });
  coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: { mayMessagePeers: true } });
  coordinator.dispatch("child", "agent.register", { rootId: "fabric", parentId: "root", route });
  const first = coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "first", clientDedupeKey: "retry" }).value as { message: AgentMessage };
  coordinator.dispatch("child", "message.ack", { messageId: first.message.id });
  coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "other" });
  const retried = coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "retry", clientDedupeKey: "retry" }).value as { message: AgentMessage };
  assert.notEqual(retried.message.id, first.message.id);
});
