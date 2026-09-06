import test from "node:test";
import assert from "node:assert/strict";
import { Coordinator } from "../src/core/coordinator.ts";
import type { AgentRecord, ModelRoute } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

function makeCoordinator(): { coordinator: Coordinator; now: { value: number } } {
  let sequence = 0;
  const now = { value: 1_000 };
  const coordinator = new Coordinator({ rootId: "fabric", clock: () => now.value, idFactory: (prefix) => `${prefix}-${++sequence}` });
  return { coordinator, now };
}

function registerRoot(coordinator: Coordinator): AgentRecord {
  return coordinator.dispatch("root", "agent.register", {
    rootId: "fabric",
    role: "root",
    route,
    capabilities: { maySpawn: true, mayMessagePeers: true, mayEscalate: true, mayTransferOwnership: true, mayWriteRepo: true, mayUseShell: true },
  }).value.agent as AgentRecord;
}

function registerChild(coordinator: Coordinator, id: string, capabilities: Partial<AgentRecord["capabilities"]> = {}): AgentRecord {
  return coordinator.dispatch(id, "agent.register", {
    rootId: "fabric",
    parentId: "root",
    role: "worker",
    route,
    capabilities: { mayMessagePeers: true, ...capabilities },
  }).value.agent as AgentRecord;
}

interface WriteDecision {
  allowed: boolean;
  reason?: string;
  resourceId?: string;
  fenceId?: string;
  expiresAt?: number;
}

function setupWriterPair(coordinator: Coordinator): void {
  registerRoot(coordinator);
  registerChild(coordinator, "writer", { mayWriteRepo: true });
  registerChild(coordinator, "other", { mayWriteRepo: true });
  coordinator.dispatch("root", "resource.define", { resourceId: "file:a.ts", kind: "file", path: "a.ts", permissions: ["write"] });
  for (const agentId of ["writer", "other"]) {
    coordinator.dispatch("root", "resource.grant", { resourceId: "file:a.ts", agentId, permissions: ["read", "write"] });
  }
}

test("a write fence keeps conflicting leases out while the writer's lapsed hold is revalidated", () => {
  const { coordinator, now } = makeCoordinator();
  setupWriterPair(coordinator);
  // The writer takes a short mutable lease and fences the file for its write.
  coordinator.dispatch("writer", "resource.borrow", { resourceId: "file:a.ts", mode: "mutable", leaseMs: 5_000 });
  const fence = coordinator.dispatch("writer", "resource.begin_write", { path: "a.ts", fenceMs: 30_000 }).value as WriteDecision;
  assert.equal(fence.allowed, true);
  assert.equal(fence.resourceId, "file:a.ts");
  assert.equal(typeof fence.fenceId, "string");
  // Time passes; the writer's hold lapses mid-write but the fence outlives it.
  now.value += 10_000;
  coordinator.maintenance();
  assert.throws(
    () => coordinator.dispatch("other", "resource.borrow", { resourceId: "file:a.ts", mode: "mutable" }),
    (error: unknown) => (error as { code?: string }).code === "RESOURCE_CONFLICT",
  );
  // Queuing behind the fence is possible and drains when the fence ends.
  const queued = coordinator.dispatch("other", "resource.borrow", { resourceId: "file:a.ts", mode: "mutable", wait: true }).value as { status: string };
  assert.equal(queued.status, "waiting");
  const ended = coordinator.dispatch("writer", "resource.end_write", { fenceId: fence.fenceId as string }).value as { released: boolean };
  assert.equal(ended.released, true);
  const granted = coordinator.dispatch("other", "resource.borrow", { resourceId: "file:a.ts", mode: "mutable", wait: true }).value as { status: string; leaseId?: string };
  assert.equal(granted.status, "granted");
  assert.equal(typeof granted.leaseId, "string");
});

test("fences expire on their own and ending is idempotent and actor-scoped", () => {
  const { coordinator, now } = makeCoordinator();
  setupWriterPair(coordinator);
  coordinator.dispatch("writer", "resource.borrow", { resourceId: "file:a.ts", mode: "mutable", leaseMs: 5_000 });
  const fence = coordinator.dispatch("writer", "resource.begin_write", { path: "a.ts", fenceMs: 20_000 }).value as WriteDecision;
  const fenceId = fence.fenceId as string;
  // Another actor cannot lift the writer's fence.
  const foreign = coordinator.dispatch("other", "resource.end_write", { fenceId }).value as { released: boolean };
  assert.equal(foreign.released, false);
  // The writer may end it; a second end is an idempotent no-op.
  assert.equal((coordinator.dispatch("writer", "resource.end_write", { fenceId }).value as { released: boolean }).released, true);
  assert.equal((coordinator.dispatch("writer", "resource.end_write", { fenceId }).value as { released: boolean }).released, false);
  assert.equal((coordinator.dispatch("writer", "resource.end_write", { fenceId: "fence-unknown" }).value as { released: boolean }).released, false);
  // A re-taken fence stops expiring: before expiry the borrow is denied, after
  // maintenance reclaims it, the same path is acquirable again.
  // A re-taken fence that is never renewed expires on its own; once
  // maintenance reclaims the writer's short lease and the fence together, the
  // same path is acquirable by the other actor again.
  coordinator.dispatch("writer", "resource.begin_write", { path: "a.ts", fenceMs: 5_000 });
  now.value += 10_000;
  coordinator.maintenance();
  const granted = coordinator.dispatch("other", "resource.borrow", { resourceId: "file:a.ts", mode: "shared", wait: true }).value as { status: string };
  assert.equal(granted.status, "granted");
});

test("begin_write refuses unauthorized writers without leaving a fence behind", () => {
  const { coordinator } = makeCoordinator();
  setupWriterPair(coordinator);
  // Nobody holds file:a.ts, so a guarded child write is unauthorized and must
  // not create a fence that would freeze everyone else out.
  const denied = coordinator.dispatch("writer", "resource.begin_write", { path: "a.ts" }).value as WriteDecision;
  assert.equal(denied.allowed, false);
  assert.equal(denied.fenceId, undefined);
  const granted = coordinator.dispatch("other", "resource.borrow", { resourceId: "file:a.ts", mode: "mutable", wait: true }).value as { status: string };
  assert.equal(granted.status, "granted");
});

test("root host-guard writes fence declared paths but leave undeclared paths unfenced", () => {
  const { coordinator } = makeCoordinator();
  registerRoot(coordinator);
  registerChild(coordinator, "worker", { mayWriteRepo: true });
  coordinator.dispatch("root", "resource.define", { resourceId: "file:m.ts", kind: "file", path: "m.ts", permissions: ["write"] });
  coordinator.dispatch("root", "resource.grant", { resourceId: "file:m.ts", agentId: "worker", permissions: ["read", "write"] });
  // A foreign mutable hold blocks the root through the fence-aware check.
  coordinator.dispatch("worker", "resource.borrow", { resourceId: "file:m.ts", mode: "mutable" });
  const blocked = coordinator.dispatch("root", "resource.begin_write", { path: "m.ts", hostGuard: true }).value as WriteDecision;
  assert.equal(blocked.allowed, false);
  coordinator.dispatch("worker", "resource.release", { resourceId: "file:m.ts" });
  // Declared path, no foreign hold: the root may write and fences the file.
  const declared = coordinator.dispatch("root", "resource.begin_write", { path: "m.ts", hostGuard: true }).value as WriteDecision;
  assert.equal(declared.allowed, true);
  assert.equal(declared.resourceId, "file:m.ts");
  assert.equal(typeof declared.fenceId, "string");
  // The root fence keeps the worker from re-borrowing while the root writes.
  assert.throws(
    () => coordinator.dispatch("worker", "resource.borrow", { resourceId: "file:m.ts", mode: "mutable" }),
    (error: unknown) => (error as { code?: string }).code === "RESOURCE_CONFLICT",
  );
  // Undeclared path: allowed and nothing to fence.
  const undeclared = coordinator.dispatch("root", "resource.begin_write", { path: "notes/free.md", hostGuard: true }).value as WriteDecision;
  assert.equal(undeclared.allowed, true);
  assert.equal(undeclared.fenceId, undefined);
  // Lifting the root fence restores the worker's access.
  coordinator.dispatch("root", "resource.end_write", { fenceId: declared.fenceId as string });
  const granted = coordinator.dispatch("worker", "resource.borrow", { resourceId: "file:m.ts", mode: "mutable", wait: true }).value as { status: string };
  assert.equal(granted.status, "granted");
});
