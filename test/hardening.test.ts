import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";
import { Coordinator } from "../src/core/coordinator.ts";
import { FabricError } from "../src/core/errors.ts";
import { BrokerClient } from "../src/broker/client.ts";
import { BrokerServer } from "../src/broker/server.ts";
import { assertReadOnlyShellCommand, createGuardedChildTools, createGuardedReadOnlyTools, evaluateRootWriteGuard, workspaceRelativePath } from "../src/pi/guards.ts";
import { ManagedChild, taskAwareTurnStatus } from "../src/pi/runtime.ts";
import type { AgentRecord, AgentMessage, ModelRoute, ResourceRecord } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };
const execFileAsync = promisify(execFile);

function makeCoordinator(options: { clock?: () => number; idFactory?: (prefix: string) => string } = {}): Coordinator {
  return new Coordinator({
    rootId: "fabric",
    config: { maxTotalAgents: 32, maxChildrenPerAgent: 16 },
    clock: options.clock ?? (() => 1_000),
    idFactory: options.idFactory,
  });
}

function registerRoot(coordinator: Coordinator, capabilities: Partial<AgentRecord["capabilities"]> = {}): AgentRecord {
  return coordinator.dispatch("root", "agent.register", {
    rootId: "fabric",
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

function registerChildWithToken(coordinator: Coordinator, id: string, capabilities: Partial<AgentRecord["capabilities"]> = {}): { agent: AgentRecord; token: string } {
  return coordinator.dispatch(id, "agent.register", {
    rootId: "fabric",
    parentId: "root",
    route,
    capabilities: { mayMessagePeers: true, ...capabilities },
  }).value as { agent: AgentRecord; token: string };
}

function registerChild(coordinator: Coordinator, id: string, capabilities: Partial<AgentRecord["capabilities"]> = {}): AgentRecord {
  return registerChildWithToken(coordinator, id, capabilities).agent;
}

function expectCode(fn: () => unknown, code: string): void {
  assert.throws(fn, (error: unknown) => error instanceof FabricError && error.code === code);
}

test("shared shell rejects mutation syntax and dangerous read-command options", () => {
  assert.equal(workspaceRelativePath("/tmp/workspace", "src/a.ts"), "src/a.ts");
  assertReadOnlyShellCommand("ls -la");
  assertReadOnlyShellCommand("git status --short");
  assertReadOnlyShellCommand("rg 'foo[0-9].*' .");
  expectCode(() => assertReadOnlyShellCommand("rm -f file"), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("git -c alias.status=!touch status"), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("git branch new-branch"), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("git branch --edit-description"), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("git remote add origin https://example.invalid/repo"), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("git log --exec=touch\\ file"), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("git log --show-signature"), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("rg --pre=touch pattern ."), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("rg --follow pattern ."), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("grep -R secret ."), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("sort -T/tmp input"), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("rg pattern *"), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("find . -exec touch {} \\;"), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("ls; touch file"), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("/tmp/cat secret"), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("cat ../secret"), "CAPABILITY_DENIED");
  expectCode(() => assertReadOnlyShellCommand("grep secret /etc/hosts"), "CAPABILITY_DENIED");
});

test("shared git inspection cannot execute a repository alias", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-git-guard-"));
  try {
    await execFileAsync("git", ["init", "-q"], { cwd: directory });
    await execFileAsync("git", ["config", "alias.status", "!echo pwned > alias-marker"], { cwd: directory });
    const bash = createGuardedChildTools({ client: { request: async <T>() => ({}) as T }, workspacePath: directory, mayWriteRepo: false, mayUseShell: true }).find((tool) => tool.name === "bash");
    assert.ok(bash);
    const sessionContext = { cwd: directory, sessionManager: { getSessionId: () => "test", getSessionFile: () => undefined } };
    await bash.execute("bash-1", { command: "git status" }, undefined, undefined, sessionContext as never);
    await assert.rejects(() => readFile(join(directory, "alias-marker")));
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("read-only built-ins cannot inspect outside the managed workspace", async () => {
  const parent = await mkdtemp(join(tmpdir(), "safe-agents-read-parent-"));
  const workspace = join(parent, "workspace");
  const secret = join(parent, "secret.txt");
  try {
    await mkdir(workspace, { recursive: true });
    await writeFile(secret, "private\n", "utf8");
    const tools = createGuardedReadOnlyTools(workspace);
    const read = tools.find((tool) => tool.name === "read");
    const ls = tools.find((tool) => tool.name === "ls");
    assert.ok(read);
    assert.ok(ls);
    await assert.rejects(() => read.execute("read-1", { path: secret }, undefined, undefined, { cwd: workspace } as never), /escapes the managed workspace/);
    await assert.rejects(() => ls.execute("ls-1", { path: ".." }, undefined, undefined, { cwd: workspace } as never), /escapes the managed workspace/);
  } finally {
    await rm(parent, { recursive: true, force: true });
  }
});

test("owners must acquire a mutable hold, and shared-to-mutable upgrades are denied", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  registerChild(coordinator, "reader");
  coordinator.dispatch("root", "resource.define", { resourceId: "file:src/a.ts", kind: "file", path: "src/a.ts" });
  coordinator.dispatch("root", "resource.grant", { resourceId: "file:src/a.ts", agentId: "reader", permissions: ["read", "write"] });
  const shared = coordinator.dispatch("reader", "resource.borrow", { resourceId: "file:src/a.ts", mode: "shared" }).value;

  assert.deepEqual(coordinator.dispatch("root", "resource.check_write", { resourceId: "file:src/a.ts" }).value, {
    allowed: false,
    resourceId: "file:src/a.ts",
    reason: "Agent root does not hold mutable access to file:src/a.ts",
  });
  expectCode(() => coordinator.dispatch("root", "resource.borrow", { resourceId: "file:src/a.ts", mode: "mutable" }), "RESOURCE_CONFLICT");
  expectCode(() => coordinator.dispatch("reader", "resource.borrow", { resourceId: "file:src/a.ts", mode: "mutable" }), "RESOURCE_CONFLICT");
  coordinator.dispatch("reader", "resource.release", { leaseId: shared.leaseId });
});

test("task facts, not a stopped model turn, determine terminal lifecycle", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const spawned = coordinator.dispatch("root", "agent.spawn", { route, taskDescription: "needs schema" }).value as { agent: AgentRecord; taskId: string };
  coordinator.dispatch(spawned.agent.id, "agent.begin_turn", {});
  coordinator.dispatch(spawned.agent.id, "task.update", { taskId: spawned.taskId, action: "block", reason: "schema is missing" });
  const ended = coordinator.dispatch(spawned.agent.id, "agent.end_turn", { status: "completed" }).value as { agent: AgentRecord; task: { status: string } };
  assert.equal(ended.agent.status, "blocked");
  assert.equal(ended.task.status, "blocked");
  assert.equal(taskAwareTurnStatus({ status: "blocked" }, false), "blocked");
  assert.equal(taskAwareTurnStatus({ status: "active" }, false), "ready");
});

test("all terminal transitions resolve pending requests instead of leaving dead waiters", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const child = registerChild(coordinator, "worker");
  const sent = coordinator.dispatch(child.id, "message.send", { to: "root", type: "clarification", body: "need input", expectsReply: true }).value as { request: { id: string } };
  coordinator.dispatch(child.id, "agent.end_turn", { status: "completed" });
  const status = coordinator.dispatch("root", "fabric.status", {}).value as { pendingRequests: Array<{ id: string; status: string }> };
  assert.equal(status.pendingRequests.some((request) => request.id === sent.request.id), false);
  expectCode(() => coordinator.dispatch("root", "message.reply", { requestId: sent.request.id, body: "too late" }), "REQUEST_ALREADY_RESOLVED");
});

test("failed task owners are released while completed task ownership remains durable", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const failed = coordinator.dispatch("root", "agent.spawn", { route, taskDescription: "will fail" }).value as { agent: AgentRecord; taskId: string };
  coordinator.dispatch(failed.agent.id, "task.update", { taskId: failed.taskId, action: "fail", reason: "test failure" });
  coordinator.dispatch(failed.agent.id, "agent.end_turn", { status: "failed" });
  const failedTask = coordinator.dispatch("root", "task.show", { taskId: failed.taskId }).value as { status: string; owner?: string };
  assert.equal(failedTask.status, "failed");
  assert.equal(failedTask.owner, undefined);

  const completed = coordinator.dispatch("root", "agent.spawn", { route, taskDescription: "will complete" }).value as { agent: AgentRecord; taskId: string };
  coordinator.dispatch(completed.agent.id, "agent.begin_turn", {});
  coordinator.dispatch(completed.agent.id, "task.update", { taskId: completed.taskId, action: "complete", result: { summary: "done" } });
  coordinator.dispatch(completed.agent.id, "agent.end_turn", { status: "ready" });
  const completedTask = coordinator.dispatch("root", "task.show", { taskId: completed.taskId }).value as { status: string; owner?: string };
  assert.equal(completedTask.status, "completed");
  assert.equal(completedTask.owner, completed.agent.id);
});

test("terminal agents cannot reconnect, while the explicit broker-recovery window can", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const completed = registerChildWithToken(coordinator, "completed");
  coordinator.dispatch(completed.agent.id, "agent.begin_turn", {});
  coordinator.dispatch(completed.agent.id, "agent.end_turn", { status: "completed" });
  expectCode(() => coordinator.dispatch(completed.agent.id, "agent.register", { rootId: "fabric", parentId: "root", route, token: completed.token }), "LIFECYCLE_CONFLICT");
  coordinator.dispatch(completed.agent.id, "agent.end_turn", { status: "completed" });

  const failed = registerChildWithToken(coordinator, "failed");
  coordinator.dispatch(failed.agent.id, "agent.begin_turn", {});
  coordinator.dispatch(failed.agent.id, "agent.end_turn", { status: "failed", statusReason: "crashed" });
  expectCode(() => coordinator.dispatch(failed.agent.id, "agent.register", { rootId: "fabric", parentId: "root", route, token: failed.token }), "LIFECYCLE_CONFLICT");

  const cancelled = registerChildWithToken(coordinator, "cancelled");
  coordinator.dispatch("root", "agent.cancel", { agentId: cancelled.agent.id });
  expectCode(() => coordinator.dispatch(cancelled.agent.id, "agent.register", { rootId: "fabric", parentId: "root", route, token: cancelled.token }), "LIFECYCLE_CONFLICT");

  const recoverable = registerChildWithToken(coordinator, "recoverable");
  coordinator.recover();
  assert.equal(coordinator.dispatch(recoverable.agent.id, "agent.register", { rootId: "fabric", parentId: "root", route, token: recoverable.token }).value.agent.status, "ready");
});

test("reconnectable agents reserve capacity so newcomers cannot evict them", () => {
  const coordinator = new Coordinator({
    rootId: "fabric",
    config: { maxTotalAgents: 2, maxChildrenPerAgent: 4 },
    clock: () => 1_000,
  });
  const rootRegistration = coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true } });
  const rootToken = rootRegistration.value.token as string;
  const spawned = coordinator.dispatch("root", "agent.spawn", { role: "worker", route, capabilities: {} }) as { value: { agent: AgentRecord; token: string } };
  coordinator.recover();
  // Both agents are failed+reconnectable; the root reclaims its own slot first.
  coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, token: rootToken });
  // The child's slot stays reserved: a newcomer spawn cannot take it while the
  // reconnect window is open.
  expectCode(() => coordinator.dispatch("root", "agent.spawn", { role: "squatter", route, capabilities: {} }), "AGENT_LIMIT_REACHED");
  const reconnected = coordinator.dispatch(spawned.value.agent.id, "agent.register", { rootId: "fabric", parentId: "root", route, token: spawned.value.token });
  assert.equal(reconnected.value.agent.status, "ready");
  expectCode(() => coordinator.dispatch("root", "agent.spawn", { role: "squatter", route, capabilities: {} }), "AGENT_LIMIT_REACHED");
  // Cancelling the dead child releases its reservation permanently.
  coordinator.dispatch(spawned.value.agent.id, "agent.cancel", { agentId: spawned.value.agent.id });
  const admitted = coordinator.dispatch("root", "agent.spawn", { role: "replacement", route, capabilities: {} }) as { value: { agent: AgentRecord } };
  assert.equal(admitted.value.agent.role, "replacement");
});

test("missing persisted credentials fail closed", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const state = coordinator.exportState();
  state.agents[0]!.authToken = undefined;
  const restored = makeCoordinator();
  restored.restoreState(state);
  assert.equal(restored.authenticate("root"), false);
  expectCode(() => restored.dispatch("root", "agent.register", { rootId: "fabric", route, token: "guessed" }), "IDENTITY_CONFLICT");
});

test("acknowledgement remains idempotent when the clock returns zero", () => {
  const coordinator = makeCoordinator({ clock: () => 0 });
  registerRoot(coordinator);
  registerChild(coordinator, "child");
  const sent = coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "once" }).value as { message: AgentMessage };
  coordinator.dispatch("child", "message.ack", { messageId: sent.message.id });
  assert.deepEqual(coordinator.dispatch("child", "message.inbox", {}).value, []);
  assert.deepEqual(coordinator.dispatch("child", "message.ack", { messageId: sent.message.id }).events, []);
});

test("same-timestamp messages preserve per-sender FIFO", () => {
  let messageIds = 0;
  let otherIds = 0;
  const coordinator = makeCoordinator({
    idFactory: (prefix) => prefix === "message" ? `message-${messageIds++ === 0 ? "z" : "a"}` : `${prefix}-${otherIds++}`,
  });
  registerRoot(coordinator);
  registerChild(coordinator, "child");
  coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "first" });
  coordinator.dispatch("root", "message.send", { to: "child", type: "inform", body: "second" });
  const inbox = coordinator.dispatch("child", "message.inbox", {}).value as AgentMessage[];
  assert.deepEqual(inbox.map((message) => message.body), ["first", "second"]);
  assert.deepEqual(inbox.map((message) => message.senderSequence), [1, 2]);
  assert.ok((inbox[0].brokerSequence ?? 0) < (inbox[1].brokerSequence ?? 0));
});

test("capability flags use strict booleans and fail closed", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  expectCode(() => coordinator.dispatch("root", "agent.spawn", { route, capabilities: { mayWriteRepo: "false" } as never }), "INVALID_ARGUMENT");
});

test("resource capability grants cannot exceed the parent ceiling", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator, { resourceGrants: { "module:src": ["read"] } });
  const child = registerChild(coordinator, "worker", { resourceGrants: { "module:src": ["read", "write"], "module:other": ["read"] } });
  const capabilities = coordinator.dispatch(child.id, "agent.status", {}).value.capabilities;
  assert.deepEqual(capabilities.resourceGrants, { "module:src": ["read"] });
});

test("broker-generated message types cannot be forged by an actor", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const child = registerChild(coordinator, "worker");
  expectCode(() => coordinator.dispatch(child.id, "message.send", { to: "root", type: "agent_failed", body: "fake" }), "CAPABILITY_DENIED");
  expectCode(() => coordinator.dispatch(child.id, "message.send", { to: "root", type: "resource_granted", body: "fake" }), "CAPABILITY_DENIED");
});

test("escalation messages require the escalation capability", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const child = registerChild(coordinator, "worker", { mayEscalate: false });
  expectCode(() => coordinator.dispatch(child.id, "message.send", { to: "root", type: "escalation", body: "blocked" }), "CAPABILITY_DENIED");
});

test("explicit peer exceptions are narrow and cannot be widened by a child", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator, { peerIds: ["peer-a"] });
  registerChild(coordinator, "peer-a", { mayMessagePeers: false });
  const child = registerChild(coordinator, "worker", { mayMessagePeers: false, peerIds: ["peer-a", "peer-b"] });
  registerChild(coordinator, "peer-b", { mayMessagePeers: false });
  const capabilities = coordinator.dispatch("worker", "agent.status", {}).value.capabilities;
  assert.deepEqual(capabilities.peerIds, ["peer-a"]);
  coordinator.dispatch(child.id, "message.send", { to: "peer-a", type: "inform", body: "narrow exception" });
  expectCode(() => coordinator.dispatch(child.id, "message.send", { to: "peer-b", type: "inform", body: "not granted" }), "CAPABILITY_DENIED");
});

test("agent status projections never expose reconnect credentials", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const child = registerChild(coordinator, "worker");
  const status = coordinator.dispatch(child.id, "agent.status", {}).value as Record<string, unknown>;
  assert.equal("authToken" in status, false);
  const updated = coordinator.dispatch(child.id, "agent.update", { status: "waiting" }).value as Record<string, unknown>;
  assert.equal("authToken" in updated, false);
  expectCode(() => coordinator.dispatch(child.id, "agent.update", { taskId: "forged-task" }), "IDENTITY_CONFLICT");
  const ended = coordinator.dispatch(child.id, "agent.end_turn", { status: "ready" }).value as { agent: Record<string, unknown> };
  assert.equal("authToken" in ended.agent, false);
});

test("message history is private to each conversation", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  registerChild(coordinator, "a");
  registerChild(coordinator, "b");
  registerChild(coordinator, "c", { mayMessagePeers: true });
  coordinator.dispatch("a", "message.send", { to: "b", type: "inform", body: "private" });
  assert.deepEqual(coordinator.dispatch("c", "message.list", {}).value, []);
  assert.equal((coordinator.dispatch("root", "message.list", { scope: "all" }).value as AgentMessage[]).length, 1);
});

test("transfer capability does not let a child grant a root-owned resource", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const child = coordinator.dispatch("root", "agent.spawn", { route, capabilities: { mayTransferOwnership: true } }).value.agent;
  coordinator.dispatch("root", "resource.define", { resourceId: "module:root", kind: "module", path: "src" });
  expectCode(() => coordinator.dispatch(child.id, "resource.grant", { resourceId: "module:root", agentId: child.id, permissions: ["read"] }), "CAPABILITY_DENIED");
});

test("declared path holds still conflict when hierarchy links are omitted", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  registerChild(coordinator, "writer", { mayWriteRepo: true });
  registerChild(coordinator, "reader");
  coordinator.dispatch("root", "resource.define", { resourceId: "module:src", kind: "module", path: "src" });
  coordinator.dispatch("root", "resource.define", { resourceId: "file:src/a.ts", kind: "file", path: "src/a.ts" });
  coordinator.dispatch("root", "resource.grant", { resourceId: "module:src", agentId: "writer", permissions: ["read", "write"] });
  coordinator.dispatch("root", "resource.grant", { resourceId: "file:src/a.ts", agentId: "reader", permissions: ["read", "write"] });
  coordinator.dispatch("writer", "resource.borrow", { resourceId: "module:src", mode: "mutable" });
  const waiting = coordinator.dispatch("reader", "resource.borrow", { resourceId: "file:src/a.ts", mode: "shared", wait: true }).value as { status: string };
  assert.equal(waiting.status, "waiting");
  assert.equal((coordinator.dispatch("writer", "resource.check_write", { path: "src/a.ts" }).value as { allowed: boolean }).allowed, true);
});

test("a later compatible reader cannot bypass an earlier blocked writer during waiter draining", () => {
  let now = 1_000;
  const coordinator = makeCoordinator({ clock: () => now });
  registerRoot(coordinator);
  for (const id of ["holder", "writer", "reader"]) registerChild(coordinator, id);
  coordinator.dispatch("root", "resource.define", { resourceId: "module:src", kind: "module", path: "src" });
  coordinator.dispatch("root", "resource.define", { resourceId: "file:src/a.ts", kind: "file", path: "src/a.ts", parentId: "module:src" });
  coordinator.dispatch("root", "resource.define", { resourceId: "file:other.ts", kind: "file", path: "other.ts" });
  for (const id of ["holder", "writer", "reader"]) {
    coordinator.dispatch("root", "resource.grant", { resourceId: "module:src", agentId: id, permissions: ["read", "write"] });
  }
  coordinator.dispatch("root", "resource.grant", { resourceId: "file:other.ts", agentId: "holder", permissions: ["read", "write"] });
  coordinator.dispatch("holder", "resource.borrow", { resourceId: "file:src/a.ts", mode: "shared" });
  const unrelated = coordinator.dispatch("holder", "resource.borrow", { resourceId: "file:other.ts", mode: "shared" }).value;
  coordinator.dispatch("writer", "resource.borrow", { resourceId: "module:src", mode: "mutable", wait: true });
  coordinator.dispatch("reader", "resource.borrow", { resourceId: "file:src/a.ts", mode: "shared", wait: true });
  coordinator.dispatch("holder", "resource.release", { leaseId: unrelated.leaseId });
  const resource = coordinator.dispatch("root", "resource.inspect", { resourceId: "file:src/a.ts" }).value as ResourceRecord;
  assert.equal(resource.mutableHold, undefined);
  assert.equal(resource.sharedHolds.some((hold) => hold.agentId === "reader"), false);
  assert.deepEqual(resource.waiters.map((waiter) => waiter.agentId), ["reader"]);
});

test("older parent-level writer waiters retain priority over later child readers", () => {
  let now = 1_000;
  const coordinator = makeCoordinator({ clock: () => now });
  registerRoot(coordinator);
  for (const id of ["holder", "writer", "reader"]) registerChild(coordinator, id);
  coordinator.dispatch("root", "resource.define", { resourceId: "module:src", kind: "module", path: "src" });
  coordinator.dispatch("root", "resource.define", { resourceId: "file:src/a.ts", kind: "file", path: "src/a.ts", parentId: "module:src" });
  for (const id of ["holder", "writer", "reader"]) coordinator.dispatch("root", "resource.grant", { resourceId: "module:src", agentId: id, permissions: ["read", "write"] });
  const held = coordinator.dispatch("holder", "resource.borrow", { resourceId: "file:src/a.ts", mode: "shared" }).value;
  const writer = coordinator.dispatch("writer", "resource.borrow", { resourceId: "module:src", mode: "mutable", wait: true }).value;
  now += 1;
  const reader = coordinator.dispatch("reader", "resource.borrow", { resourceId: "file:src/a.ts", mode: "shared", wait: true }).value;
  assert.equal(writer.status, "waiting");
  assert.equal(reader.status, "waiting");
  coordinator.dispatch("holder", "resource.release", { leaseId: held.leaseId });
  const resource = coordinator.dispatch("root", "resource.inspect", { resourceId: "module:src" }).value as ResourceRecord;
  assert.equal(resource.mutableHold?.agentId, "writer");
  const childResource = coordinator.dispatch("root", "resource.inspect", { resourceId: "file:src/a.ts" }).value as ResourceRecord;
  assert.equal(childResource.waiters[0]?.agentId, "reader");
});

test("guarded edit performs the coordinator check at the filesystem write boundary", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-guard-"));
  try {
    const target = join(directory, "a.ts");
    await writeFile(target, "one\n", "utf8");
    let allowed = false;
    const calls: Record<string, unknown>[] = [];
    const tools = createGuardedChildTools({
      workspacePath: directory,
      mayWriteRepo: true,
      mayUseShell: false,
      client: {
        async request<T = unknown>(operation: string, args: Record<string, unknown> = {}): Promise<T> {
          calls.push({ operation, ...args });
          // A fenced grant must carry a fenceId, like the real coordinator does.
          return (allowed ? { allowed, resourceId: "file:a.ts", fenceId: "fence-1" } : { allowed, resourceId: "file:a.ts", reason: "mutable hold required" }) as T;
        },
      },
    });
    const edit = tools.find((tool) => tool.name === "edit");
    assert.ok(edit);
    await assert.rejects(() => edit.execute("edit-1", { path: "a.ts", edits: [{ oldText: "one", newText: "blocked" }] }, undefined, undefined, { cwd: directory } as never), /mutable hold required/);
    assert.equal(await readFile(target, "utf8"), "one\n");
    allowed = true;
    await edit.execute("edit-2", { path: "a.ts", edits: [{ oldText: "one", newText: "allowed" }] }, undefined, undefined, { cwd: directory } as never);
    assert.equal(await readFile(target, "utf8"), "allowed\n");
    assert.deepEqual(calls.map((call) => call.operation), ["resource.begin_write", "resource.begin_write", "resource.end_write"]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("guarded edit cannot read outside the managed workspace", async () => {
  const parent = await mkdtemp(join(tmpdir(), "safe-agents-edit-parent-"));
  const workspace = join(parent, "workspace");
  const secret = join(parent, "secret.txt");
  try {
    await mkdir(workspace, { recursive: true });
    await writeFile(secret, "private\n", "utf8");
    let authorizationCalls = 0;
    const edit = createGuardedChildTools({
      workspacePath: workspace,
      mayWriteRepo: true,
      mayUseShell: false,
      client: { request: async <T>() => { authorizationCalls += 1; return { allowed: true } as T; } },
    }).find((tool) => tool.name === "edit");
    assert.ok(edit);
    await assert.rejects(() => edit.execute("edit-outside", { path: secret, edits: [{ oldText: "private", newText: "changed" }] }, undefined, undefined, { cwd: workspace } as never), /CAPABILITY_DENIED/);
    assert.equal(authorizationCalls, 0);
    assert.equal(await readFile(secret, "utf8"), "private\n");
  } finally {
    await rm(parent, { recursive: true, force: true });
  }
});

test("guarded edit accepts a declared path only while the coordinator grants a mutable hold", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-coordinator-guard-"));
  try {
    const target = join(directory, "src", "a.ts");
    const coordinator = makeCoordinator();
    registerRoot(coordinator);
    const child = registerChild(coordinator, "writer", { mayWriteRepo: true });
    coordinator.dispatch("root", "resource.define", { resourceId: "module:src", kind: "module", path: "src" });
    coordinator.dispatch("root", "resource.define", { resourceId: "file:src/a.ts", kind: "file", path: "src/a.ts", parentId: "module:src" });
    coordinator.dispatch("root", "resource.grant", { resourceId: "module:src", agentId: child.id, permissions: ["read", "write"] });
    await mkdir(join(directory, "src"), { recursive: true });
    await writeFile(target, "one\n", "utf8");
    const tools = createGuardedChildTools({
      workspacePath: directory,
      mayWriteRepo: true,
      mayUseShell: false,
      client: { request: async <T>(operation: string, args: Record<string, unknown> = {}) => coordinator.dispatch(child.id, operation, args).value as T },
    });
    const edit = tools.find((tool) => tool.name === "edit");
    assert.ok(edit);
    await assert.rejects(() => edit.execute("edit-1", { path: "src/a.ts", edits: [{ oldText: "one", newText: "blocked" }] }, undefined, undefined, { cwd: directory } as never), /does not hold mutable access/);
    coordinator.dispatch(child.id, "resource.borrow", { resourceId: "file:src/a.ts", mode: "mutable" });
    assert.deepEqual(coordinator.dispatch(child.id, "resource.check_write", { path: "src/a.ts" }).value, { allowed: true, resourceId: "file:src/a.ts" });
    assert.equal((coordinator.dispatch(child.id, "resource.check_write", { path: "src/b.ts" }).value as { allowed: boolean }).allowed, false);
    await edit.execute("edit-2", { path: "src/a.ts", edits: [{ oldText: "one", newText: "allowed" }] }, undefined, undefined, { cwd: directory } as never);
    assert.equal(await readFile(target, "utf8"), "allowed\n");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("root host writes participate in borrowing through the host guard", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const mutableChild = registerChild(coordinator, "mutable-child", { mayWriteRepo: true });
  const sharedChild = registerChild(coordinator, "shared-child", { mayWriteRepo: true });
  coordinator.dispatch("root", "resource.define", { resourceId: "module:src", kind: "module", path: "src" });
  coordinator.dispatch("root", "resource.define", { resourceId: "file:src/a.ts", kind: "file", path: "src/a.ts", parentId: "module:src" });
  coordinator.dispatch("root", "resource.grant", { resourceId: "file:src/a.ts", agentId: mutableChild.id, permissions: ["read", "write"] });
  coordinator.dispatch("root", "resource.grant", { resourceId: "file:src/a.ts", agentId: sharedChild.id, permissions: ["read"] });

  // Undeclared or unheld paths are writable by the root without a declaration.
  assert.deepEqual(coordinator.dispatch("root", "resource.check_write", { path: "docs/untracked.md", hostGuard: true }).value, { allowed: true });
  assert.equal((coordinator.dispatch("root", "resource.check_write", { path: "src/a.ts", hostGuard: true }).value as { allowed: boolean }).allowed, true);

  // A child's shared read hold blocks the root, matching mutable-acquisition rules.
  coordinator.dispatch(sharedChild.id, "resource.borrow", { resourceId: "file:src/a.ts", mode: "shared" });
  const blockedByShared = coordinator.dispatch("root", "resource.check_write", { path: "src/a.ts", hostGuard: true }).value as { allowed: boolean; reason?: string };
  assert.equal(blockedByShared.allowed, false);
  coordinator.dispatch(sharedChild.id, "resource.release", { resourceId: "file:src/a.ts" });

  // A narrow file-level mutable hold blocks only that file, not siblings.
  coordinator.dispatch(mutableChild.id, "resource.borrow", { resourceId: "file:src/a.ts", mode: "mutable" });
  const blockedByMutable = coordinator.dispatch("root", "resource.check_write", { path: "src/a.ts", hostGuard: true }).value as { allowed: boolean; resourceId?: string };
  assert.equal(blockedByMutable.allowed, false);
  assert.equal(blockedByMutable.resourceId, "file:src/a.ts");
  assert.equal((coordinator.dispatch("root", "resource.check_write", { path: "src/nested/deep.ts", hostGuard: true }).value as { allowed: boolean }).allowed, true);
  coordinator.dispatch(mutableChild.id, "resource.release", { resourceId: "file:src/a.ts" });

  // A module-level mutable hold blocks the root across its whole declared subtree.
  coordinator.dispatch("root", "resource.grant", { resourceId: "module:src", agentId: mutableChild.id, permissions: ["read", "write"] });
  coordinator.dispatch(mutableChild.id, "resource.borrow", { resourceId: "module:src", mode: "mutable" });
  const blockedByModule = coordinator.dispatch("root", "resource.check_write", { path: "src/a.ts", hostGuard: true }).value as { allowed: boolean; resourceId?: string };
  assert.equal(blockedByModule.allowed, false);
  assert.equal(blockedByModule.resourceId, "module:src");
  assert.equal((coordinator.dispatch("root", "resource.check_write", { path: "src/nested/deep.ts", hostGuard: true }).value as { allowed: boolean; resourceId?: string }).resourceId, "module:src");

  // Only the fabric root may claim the host-guard exemption.
  expectCode(() => coordinator.dispatch(mutableChild.id, "resource.check_write", { path: "src/a.ts", hostGuard: true }), "CAPABILITY_DENIED");

  // After release the root may write again, and children keep strict guards.
  coordinator.dispatch(mutableChild.id, "resource.release", { resourceId: "module:src" });
  assert.deepEqual(coordinator.dispatch("root", "resource.check_write", { path: "src/a.ts", hostGuard: true }).value, { allowed: true, resourceId: "file:src/a.ts" });
  assert.equal((coordinator.dispatch(mutableChild.id, "resource.check_write", { path: "src/a.ts" }).value as { allowed: boolean }).allowed, false);
});

test("root write guard blocks coordinated paths, skips foreign targets, and fails closed when the broker is down", async () => {
  const workspace = resolve(".");
  const requests: Array<{ operation: string; args: Record<string, unknown> }> = [];
  const guardClient = (decision: { allowed: boolean; reason?: string } | Error) => ({
    request: async <T>(operation: string, args: Record<string, unknown> = {}): Promise<T> => {
      requests.push({ operation, args });
      if (decision instanceof Error) throw decision;
      return decision as T;
    },
  });

  // Non-write tools and foreign/absolute targets never reach the broker.
  assert.equal(await evaluateRootWriteGuard({ client: guardClient({ allowed: true }), workspacePath: workspace }, "read", { path: "src/a.ts" }), undefined);
  assert.equal(await evaluateRootWriteGuard({ client: guardClient({ allowed: true }), workspacePath: workspace }, "edit", { path: join(resolve(".."), "outside.ts") }), undefined);
  assert.equal(requests.length, 0);

  // A workspace-relative edit is checked with the host-guard exemption.
  assert.equal(await evaluateRootWriteGuard({ client: guardClient({ allowed: true }), workspacePath: workspace }, "write", { path: "src/a.ts" }), undefined);
  assert.deepEqual(requests.at(-1), { operation: "resource.begin_write", args: { path: "src/a.ts", hostGuard: true } });

  // A conflicting live hold blocks the tool call before any filesystem write.
  const blocked = await evaluateRootWriteGuard({ client: guardClient({ allowed: false, reason: "A conflicting runtime hold prevents writing src/a.ts" }), workspacePath: workspace }, "edit", { path: "src/a.ts" });
  assert.equal(blocked?.block, true);
  assert.match(blocked?.reason ?? "", /conflicting runtime hold/);

  // While the broker is unavailable, an established fabric fails closed to protect in-flight writes.
  const brokerDown = await evaluateRootWriteGuard({ client: guardClient(new FabricError("BROKER_UNAVAILABLE", "down")), workspacePath: workspace }, "edit", { path: "src/a.ts" });
  assert.equal(brokerDown?.block, true);
  assert.match(brokerDown?.reason ?? "", /could not coordinate the write/);
  // Any other coordination error fails closed as well.
  const failedClosed = await evaluateRootWriteGuard({ client: guardClient(new FabricError("LIFECYCLE_CONFLICT", "root is terminal")), workspacePath: workspace }, "edit", { path: "src/a.ts" });
  assert.equal(failedClosed?.block, true);
  assert.match(failedClosed?.reason ?? "", /root is terminal/);
});

test("duplicate unacknowledged notifications are queued and executed once", async () => {
  let promptCalls = 0;
  let ackCalls = 0;
  let releasePrompt: (() => void) | undefined;
  const promptDone = new Promise<void>((resolvePrompt) => { releasePrompt = resolvePrompt; });
  const child = new ManagedChild({ fabricId: "fabric" } as never, {
    agentId: "child",
    token: "token",
    parentId: "root",
    role: "worker",
    route,
    workspace: { mode: "shared", root: resolve("."), path: resolve(".") },
    cwd: resolve("."),
    stateDirectory: resolve("."),
    agentDir: resolve("."),
    endpoint: "unused",
    model: {} as never,
    capabilities: { maySpawn: false, mayMessagePeers: false, mayEscalate: true, mayTransferOwnership: false, mayWriteRepo: false, mayUseShell: false, peerIds: [], resourceGrants: {} },
  });
  (child.client as any).request = async (operation: string) => {
    if (operation === "message.ack") {
      ackCalls += 1;
      return {};
    }
    if (operation === "agent.begin_turn") return { started: true };
    if (operation === "agent.status") return { id: "child", taskId: undefined };
    if (operation === "agent.end_turn") return { agent: { status: "ready" } };
    return {};
  };
  (child as any).session = {
    isStreaming: false,
    async prompt() {
      promptCalls += 1;
      await promptDone;
    },
  };
  const message: AgentMessage = { id: "message-1", from: "root", to: "child", type: "inform", body: "once", senderSequence: 1, brokerSequence: 1, priority: "normal", createdAt: 1 };
  const first = (child as any).deliverMessage(message) as Promise<void>;
  const second = (child as any).deliverMessage(message) as Promise<void>;
  await Promise.all([first, second]);
  assert.equal(ackCalls, 1);
  releasePrompt?.();
  await new Promise((resolveWait) => setTimeout(resolveWait, 10));
  assert.equal(promptCalls, 1);
});

async function registerBrokerAgent(client: BrokerClient, parentId?: string, token?: string): Promise<{ token: string; agent?: AgentRecord }> {
  return client.request<{ token: string; agent?: AgentRecord }>("agent.register", {
    rootId: "fabric",
    ...(parentId ? { parentId } : {}),
    ...(token ? { token } : {}),
    route,
    capabilities: parentId ? { mayMessagePeers: true } : { maySpawn: true, mayMessagePeers: true },
  });
}

test("pending clarification survives broker restart and can resume after reconnect", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-clarification-restart-"));
  const server1 = new BrokerServer({ directory, rootId: "fabric", maintenanceMs: 60_000 });
  const root = new BrokerClient({ endpoint: server1.endpoint, agentId: "root" });
  let child: BrokerClient | undefined;
  let server2: BrokerServer | undefined;
  try {
    await server1.start();
    await root.connect();
    const rootRegistration = await registerBrokerAgent(root);
    const spawned = await root.request<{ agent: AgentRecord; token: string }>("agent.spawn", { route });
    child = new BrokerClient({ endpoint: server1.endpoint, agentId: spawned.agent.id, token: spawned.token });
    await child.connect();
    await registerBrokerAgent(child, "root", spawned.token);
    const request = await child.request<{ request: { id: string } }>("message.send", { to: "root", type: "clarification", body: "which schema?", expectsReply: true });
    const requestId = request.request.id;

    await server1.stop();
    server2 = new BrokerServer({ directory, rootId: "fabric", maintenanceMs: 60_000 });
    await server2.start();
    root.setIdentity("root", rootRegistration.token);
    await root.reconnect();
    await root.request("agent.register", { rootId: "fabric", route, token: rootRegistration.token, capabilities: { maySpawn: true, mayMessagePeers: true } });
    await child.reconnect();
    await child.request("agent.register", { rootId: "fabric", parentId: "root", route, token: spawned.token, capabilities: { mayMessagePeers: true } });

    const status = await root.request<{ pendingRequests: Array<{ id: string }> }>("fabric.status", {});
    assert.ok(status.pendingRequests.some((pending) => pending.id === requestId));
    await root.request("message.reply", { requestId, body: "schema-v2" });
    const inbox = await child.request<AgentMessage[]>("message.inbox", {});
    assert.equal(inbox.find((message) => message.requestId === requestId)?.body, "schema-v2");
  } finally {
    child?.close();
    root.close();
    await server2?.stop();
    if (server1.isStarted()) await server1.stop();
    await rm(directory, { recursive: true, force: true });
  }
});
