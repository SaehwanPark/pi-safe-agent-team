import test from "node:test";
import assert from "node:assert/strict";
import { Coordinator } from "../src/core/coordinator.ts";
import type { ModelRoute } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

test("completed child turn commits its task before releasing runtime ownership", () => {
  const coordinator = new Coordinator({ rootId: "fabric", idFactory: (prefix) => `${prefix}-${Math.random()}` });
  coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true, mayMessagePeers: true, mayWriteRepo: true, mayUseShell: true, mayTransferOwnership: true } });
  const child = coordinator.dispatch("root", "agent.spawn", { route, taskDescription: "do work" }).value as { agent: { id: string }; taskId: string };
  coordinator.dispatch(child.agent.id, "agent.begin_turn", {});
  const ended = coordinator.dispatch(child.agent.id, "agent.end_turn", { status: "completed", result: { summary: "done", output: "artifact" } }).value as { task?: { status: string; owner?: string; result?: { summary: string } } };
  assert.equal(ended.task?.status, "completed");
  assert.equal(ended.task?.owner, child.agent.id);
  assert.equal(ended.task?.result?.summary, "done");
  assert.equal(coordinator.dispatch("root", "task.show", { taskId: child.taskId }).value.status, "completed");
});
