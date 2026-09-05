import test from "node:test";
import assert from "node:assert/strict";
import { Coordinator } from "../src/core/coordinator.ts";
import type { ModelRoute } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

test("task completion is explicit before a child can become terminal", () => {
  const coordinator = new Coordinator({ rootId: "fabric", idFactory: (prefix) => `${prefix}-${Math.random()}` });
  coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true, mayMessagePeers: true, mayWriteRepo: true, mayUseShell: true, mayTransferOwnership: true } });
  const child = coordinator.dispatch("root", "agent.spawn", { route, taskDescription: "do work" }).value as { agent: { id: string }; taskId: string };
  coordinator.dispatch("root", "resource.define", { resourceId: "file:work.ts", kind: "file", path: "work.ts" });
  coordinator.dispatch("root", "resource.grant", { resourceId: "file:work.ts", agentId: child.agent.id, permissions: ["read", "write"] });
  coordinator.dispatch(child.agent.id, "resource.borrow", { resourceId: "file:work.ts", mode: "mutable" });
  coordinator.dispatch(child.agent.id, "agent.begin_turn", {});
  const implicit = coordinator.dispatch(child.agent.id, "agent.end_turn", { status: "completed", result: { summary: "model stopped", output: "ignored" } }).value as { agent: { status: string }; task?: { status: string } };
  assert.equal(implicit.agent.status, "ready");
  assert.equal(implicit.task?.status, "active");

  coordinator.dispatch(child.agent.id, "agent.begin_turn", {});
  coordinator.dispatch(child.agent.id, "task.update", { taskId: child.taskId, action: "complete", result: { summary: "done", output: "artifact" } });
  const ended = coordinator.dispatch(child.agent.id, "agent.end_turn", { status: "ready" }).value as { agent: { status: string }; task?: { status: string; owner?: string; result?: { summary: string } } };
  assert.equal(ended.agent.status, "completed");
  assert.equal(ended.task?.status, "completed");
  assert.equal(ended.task?.owner, child.agent.id);
  assert.equal(ended.task?.result?.summary, "done");
  assert.equal((coordinator.dispatch("root", "resource.inspect", { resourceId: "file:work.ts" }).value as { mutableHold?: { agentId: string } }).mutableHold, undefined);
  assert.equal(coordinator.dispatch("root", "task.show", { taskId: child.taskId }).value.status, "completed");
});
