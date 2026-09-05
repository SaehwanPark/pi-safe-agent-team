import test from "node:test";
import assert from "node:assert/strict";
import { Coordinator } from "../src/core/coordinator.ts";
import { FabricError } from "../src/core/errors.ts";
import type { ModelRoute } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

test("ownership transfer is atomic and capability-bounded", () => {
  const coordinator = new Coordinator({ rootId: "fabric" });
  coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true, mayTransferOwnership: true, mayWriteRepo: true } });
  const child = coordinator.dispatch("root", "agent.spawn", { route, capabilities: { mayTransferOwnership: false } }).value.agent;
  coordinator.dispatch("root", "resource.define", { resourceId: "module:x", kind: "module" });
  const before = coordinator.dispatch("root", "resource.snapshot", { resourceId: "module:x" }).value;
  const transferred = coordinator.dispatch("root", "resource.transfer", { resourceId: "module:x", agentId: child.id }).value;
  assert.equal(transferred.owner, child.id);
  assert.equal(transferred.version, before.version + 1);
  assert.throws(() => coordinator.dispatch(child.id, "resource.transfer", { resourceId: "module:x", agentId: "root" }), (error: unknown) => error instanceof FabricError && error.code === "RESOURCE_NOT_OWNER");
  assert.equal(coordinator.dispatch("root", "resource.inspect", { resourceId: "module:x" }).value.owner, child.id);
});
