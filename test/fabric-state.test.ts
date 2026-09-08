import test from "node:test";
import assert from "node:assert/strict";
import { FabricRuntime } from "../src/pi/runtime.ts";
import type { FabricStatus } from "../src/core/types.ts";

function makeMockStatus(overrides: Partial<FabricStatus> = {}): FabricStatus {
  return {
    rootId: "root-1",
    agents: [
      {
        id: "root-1",
        depth: 0,
        role: "root",
        route: { provider: "openai", model: "gpt-5.6-sol", thinking: "high" },
        status: "ready",
        lastActivity: Date.now(),
      },
    ],
    tasks: [],
    resources: [],
    pendingRequests: [],
    recentMessages: [],
    runningChildren: 0,
    config: {
      maxDepth: 3,
      maxChildrenPerAgent: 5,
      maxTotalAgents: 10,
      maxConcurrentAgents: 4,
      maxMailboxMessages: 100,
      maxMessageBody: 65536,
      maxTaskOutput: 65536,
      leaseMs: 30000,
      heartbeatMs: 5000,
      messageRetention: 100,
    },
    activeFences: 0,
    ...overrides,
  };
}

test("fabric-state: newly attached idle root -> active and quiescent", async () => {
  const runtime = new FabricRuntime({ cwd: "/test/repo", startBroker: false });
  (runtime as any).root = {
    agentId: "root-1",
    ctx: { cwd: "/test/repo", sessionId: "sess-1" },
  };
  (runtime as any).status = async () => makeMockStatus();

  const snapshot = await runtime.getFabricStateSnapshot({ cwd: "/test/repo", sessionId: "sess-1" });
  assert.ok(snapshot);
  assert.equal(snapshot.active, true);
  assert.equal(snapshot.quiescent, true);
  assert.equal(snapshot.runningChildren, 0);
  assert.equal(snapshot.unresolvedChildTasks, 0);
  assert.equal(snapshot.mutableHolds, 0);
  assert.equal(snapshot.pendingRootRequests, 0);
  assert.equal(snapshot.pendingRootDeliveries, 0);
});

test("fabric-state: running child -> non-quiescent", async () => {
  const runtime = new FabricRuntime({ cwd: "/test/repo", startBroker: false });
  (runtime as any).root = {
    agentId: "root-1",
    ctx: { cwd: "/test/repo", sessionId: "sess-1" },
  };
  (runtime as any).status = async () =>
    makeMockStatus({
      agents: [
        {
          id: "root-1",
          depth: 0,
          role: "root",
          route: { provider: "openai", model: "gpt-5.6-sol", thinking: "high" },
          status: "ready",
          lastActivity: Date.now(),
        },
        {
          id: "agent-2",
          parentId: "root-1",
          depth: 1,
          role: "implementer",
          route: { provider: "openai", model: "gpt-5.6-sol", thinking: "high" },
          status: "running",
          lastActivity: Date.now(),
        },
      ],
      runningChildren: 1,
    });

  const snapshot = await runtime.getFabricStateSnapshot({ cwd: "/test/repo" });
  assert.ok(snapshot);
  assert.equal(snapshot.active, true);
  assert.equal(snapshot.quiescent, false);
  assert.equal(snapshot.runningChildren, 1);
});

test("fabric-state: child ready but unfinished owned task -> non-quiescent", async () => {
  const runtime = new FabricRuntime({ cwd: "/test/repo", startBroker: false });
  (runtime as any).root = {
    agentId: "root-1",
    ctx: { cwd: "/test/repo", sessionId: "sess-1" },
  };
  (runtime as any).status = async () =>
    makeMockStatus({
      agents: [
        {
          id: "root-1",
          depth: 0,
          role: "root",
          route: { provider: "openai", model: "gpt-5.6-sol", thinking: "high" },
          status: "ready",
          lastActivity: Date.now(),
        },
        {
          id: "agent-2",
          parentId: "root-1",
          depth: 1,
          role: "implementer",
          route: { provider: "openai", model: "gpt-5.6-sol", thinking: "high" },
          status: "ready",
          taskId: "task-1",
          lastActivity: Date.now(),
        },
      ],
      tasks: [
        {
          id: "task-1",
          creator: "root-1",
          description: "Do something",
          status: "active",
          owner: "agent-2",
          dependencies: [],
          createdAt: Date.now(),
          updatedAt: Date.now(),
        },
      ],
    });

  const snapshot = await runtime.getFabricStateSnapshot({ cwd: "/test/repo" });
  assert.ok(snapshot);
  assert.equal(snapshot.quiescent, false);
  assert.equal(snapshot.unresolvedChildTasks, 1);
});

test("fabric-state: blocked child task -> non-quiescent", async () => {
  const runtime = new FabricRuntime({ cwd: "/test/repo", startBroker: false });
  (runtime as any).root = {
    agentId: "root-1",
    ctx: { cwd: "/test/repo", sessionId: "sess-1" },
  };
  (runtime as any).status = async () =>
    makeMockStatus({
      tasks: [
        {
          id: "task-1",
          creator: "root-1",
          description: "Blocked work",
          status: "blocked",
          owner: "agent-2",
          dependencies: [],
          createdAt: Date.now(),
          updatedAt: Date.now(),
        },
      ],
    });

  const snapshot = await runtime.getFabricStateSnapshot({ cwd: "/test/repo" });
  assert.ok(snapshot);
  assert.equal(snapshot.quiescent, false);
  assert.equal(snapshot.unresolvedChildTasks, 1);
});

test("fabric-state: pending clarification to root -> non-quiescent", async () => {
  const runtime = new FabricRuntime({ cwd: "/test/repo", startBroker: false });
  (runtime as any).root = {
    agentId: "root-1",
    ctx: { cwd: "/test/repo", sessionId: "sess-1" },
  };
  (runtime as any).status = async () =>
    makeMockStatus({
      pendingRequests: [
        {
          id: "req-1",
          messageId: "msg-1",
          from: "agent-2",
          to: "root-1",
          status: "pending",
          createdAt: Date.now(),
        },
      ],
    });

  const snapshot = await runtime.getFabricStateSnapshot({ cwd: "/test/repo" });
  assert.ok(snapshot);
  assert.equal(snapshot.quiescent, false);
  assert.equal(snapshot.pendingRootRequests, 1);
});

test("fabric-state: live mutable hold -> non-quiescent", async () => {
  const runtime = new FabricRuntime({ cwd: "/test/repo", startBroker: false });
  (runtime as any).root = {
    agentId: "root-1",
    ctx: { cwd: "/test/repo", sessionId: "sess-1" },
  };
  (runtime as any).status = async () =>
    makeMockStatus({
      resources: [
        {
          id: "res-1",
          kind: "file",
          path: "src/parser.ts",
          version: 1,
          owner: "root-1",
          grants: {},
          sharedHolds: [],
          createdAt: Date.now(),
          updatedAt: Date.now(),
          mutableHold: {
            leaseId: "lease-1",
            agentId: "agent-2",
            mode: "mutable",
            acquiredAt: Date.now(),
            expiresAt: Date.now() + 30000,
            lastHeartbeat: Date.now(),
            leaseMs: 30000,
          },
          waiters: [],
        },
      ],
    });

  const snapshot = await runtime.getFabricStateSnapshot({ cwd: "/test/repo" });
  assert.ok(snapshot);
  assert.equal(snapshot.quiescent, false);
  assert.equal(snapshot.mutableHolds, 1);
  assert.equal(snapshot.mutableResources.length, 1);
  assert.equal(snapshot.mutableResources[0].path, "src/parser.ts");
});

test("fabric-state: in-flight write fence -> non-quiescent", async () => {
  const runtime = new FabricRuntime({ cwd: "/test/repo", startBroker: false });
  (runtime as any).root = {
    agentId: "root-1",
    ctx: { cwd: "/test/repo", sessionId: "sess-1" },
  };
  (runtime as any).status = async () =>
    makeMockStatus({
      activeFences: 1,
    });

  const snapshot = await runtime.getFabricStateSnapshot({ cwd: "/test/repo" });
  assert.ok(snapshot);
  assert.equal(snapshot.quiescent, false);
});

test("fabric-state: all child tasks terminal + no holds/requests -> quiescent", async () => {
  const runtime = new FabricRuntime({ cwd: "/test/repo", startBroker: false });
  (runtime as any).root = {
    agentId: "root-1",
    ctx: { cwd: "/test/repo", sessionId: "sess-1" },
  };
  (runtime as any).status = async () =>
    makeMockStatus({
      tasks: [
        {
          id: "task-1",
          creator: "root-1",
          description: "Completed work",
          status: "completed",
          owner: "agent-2",
          dependencies: [],
          createdAt: Date.now(),
          updatedAt: Date.now(),
        },
        {
          id: "task-2",
          creator: "root-1",
          description: "Cancelled work",
          status: "cancelled",
          owner: "agent-3",
          dependencies: [],
          createdAt: Date.now(),
          updatedAt: Date.now(),
        },
      ],
    });

  const snapshot = await runtime.getFabricStateSnapshot({ cwd: "/test/repo" });
  assert.ok(snapshot);
  assert.equal(snapshot.quiescent, true);
  assert.equal(snapshot.unresolvedChildTasks, 0);
});

test("fabric-state: scope isolation rejects mismatched cwd or sessionId", async () => {
  const runtime = new FabricRuntime({ cwd: "/test/repo", startBroker: false });
  (runtime as any).root = {
    agentId: "root-1",
    ctx: { cwd: "/test/repo", sessionId: "sess-1" },
  };
  (runtime as any).status = async () => makeMockStatus();

  // Mismatched cwd
  const diffCwdSnapshot = await runtime.getFabricStateSnapshot({ cwd: "/other/repo" });
  assert.equal(diffCwdSnapshot, null);

  // Mismatched sessionId
  const diffSessionSnapshot = await runtime.getFabricStateSnapshot({ cwd: "/test/repo", sessionId: "wrong-sess" });
  assert.equal(diffSessionSnapshot, null);
});

test("fabric-state: shutdown or unattached root returns inactive", async () => {
  const runtime = new FabricRuntime({ cwd: "/test/repo", startBroker: false });
  // No root attached
  const snapshot1 = await runtime.getFabricStateSnapshot({ cwd: "/test/repo" });
  assert.ok(snapshot1);
  assert.equal(snapshot1.active, false);
  assert.equal(snapshot1.quiescent, false);

  // Stopped
  (runtime as any).root = { agentId: "root-1", ctx: { cwd: "/test/repo" } };
  (runtime as any).stopped = true;
  const snapshot2 = await runtime.getFabricStateSnapshot({ cwd: "/test/repo" });
  assert.ok(snapshot2);
  assert.equal(snapshot2.active, false);
  assert.equal(snapshot2.quiescent, false);
});
