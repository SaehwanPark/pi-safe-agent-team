import test from "node:test";
import assert from "node:assert/strict";
import { Coordinator } from "../src/core/coordinator.ts";
import type { ModelRoute, ResourceRecord } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

test("a later mutable borrower cannot bypass an earlier waiter", () => {
  const coordinator = new Coordinator({ rootId: "fabric", config: { maxTotalAgents: 10 }, idFactory: (() => { let i = 0; return (prefix: string) => `${prefix}-${++i}`; })() });
  coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: { mayTransferOwnership: true, mayMessagePeers: true } });
  for (const id of ["holder", "first", "second"]) coordinator.dispatch(id, "agent.register", { rootId: "fabric", parentId: "root", route, capabilities: { mayMessagePeers: true } });
  coordinator.dispatch("root", "resource.define", { resourceId: "file:a", kind: "file" });
  for (const id of ["holder", "first", "second"]) coordinator.dispatch("root", "resource.grant", { resourceId: "file:a", agentId: id, permissions: ["read", "write"] });
  const held = coordinator.dispatch("holder", "resource.borrow", { resourceId: "file:a", mode: "mutable" }).value;
  const first = coordinator.dispatch("first", "resource.borrow", { resourceId: "file:a", mode: "mutable", wait: true }).value;
  const second = coordinator.dispatch("second", "resource.borrow", { resourceId: "file:a", mode: "mutable", wait: true }).value;
  assert.equal(first.status, "waiting");
  assert.equal(second.status, "waiting");
  coordinator.dispatch("holder", "resource.release", { leaseId: held.leaseId });
  const afterFirst = coordinator.dispatch("root", "resource.inspect", { resourceId: "file:a" }).value as ResourceRecord;
  assert.equal(afterFirst.mutableHold?.agentId, "first");
  assert.equal(afterFirst.waiters[0]?.agentId, "second");
});
