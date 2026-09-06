import test from "node:test";
import assert from "node:assert/strict";
import { appendFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Coordinator } from "../src/core/coordinator.ts";
import { FabricError } from "../src/core/errors.ts";
import { Journal } from "../src/broker/journal.ts";
import { BrokerClient } from "../src/broker/client.ts";
import { BrokerServer } from "../src/broker/server.ts";
import type { AgentRecord, CoordinatorEvent, IdempotencyRecord, ModelRoute, TaskRecord } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

function makeCoordinator(): Coordinator {
  return new Coordinator({
    rootId: "fabric",
    config: { maxTotalAgents: 32, maxChildrenPerAgent: 16 },
    clock: () => 1_000,
  });
}

function registerRoot(coordinator: Coordinator): AgentRecord {
  return coordinator.dispatch("root", "agent.register", {
    rootId: "fabric",
    route,
    capabilities: { maySpawn: true, mayMessagePeers: true, mayEscalate: true, mayTransferOwnership: true, mayWriteRepo: true, mayUseShell: true },
  }).value.agent;
}

function registerChild(coordinator: Coordinator, id: string): AgentRecord {
  return coordinator.dispatch(id, "agent.register", {
    rootId: "fabric",
    parentId: "root",
    route,
    capabilities: { mayMessagePeers: true },
  }).value.agent;
}

function expectCode(fn: () => unknown, code: string): void {
  assert.throws(fn, (error: unknown) => error instanceof FabricError && error.code === code);
}

async function withTempDirectory<T>(run: (directory: string) => Promise<T>): Promise<T> {
  const directory = await mkdtemp(join(tmpdir(), "pi-idempotency-"));
  try {
    return await run(directory);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

test("task.create replays the original response instead of creating a duplicate task", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const first = coordinator.dispatch("root", "task.create", { description: "only once", operationId: "op-task-1" }).value as TaskRecord & { replayed?: boolean };
  const replay = coordinator.dispatch("root", "task.create", { description: "only once", operationId: "op-task-1" }).value as TaskRecord & { replayed?: boolean };
  assert.equal(replay.id, first.id);
  assert.equal(replay.replayed, true);
  assert.notEqual(first.replayed, true);
  const second = coordinator.dispatch("root", "task.create", { description: "only once", operationId: "op-task-2" }).value as TaskRecord;
  assert.notEqual(second.id, first.id);
  const listed = coordinator.dispatch("root", "task.list", {}).value as TaskRecord[];
  assert.equal(listed.length, 2);
});

test("reusing an operationId with different arguments fails closed", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  coordinator.dispatch("root", "task.create", { description: "first intent", operationId: "op-reuse" });
  expectCode(() => coordinator.dispatch("root", "task.create", { description: "different intent", operationId: "op-reuse" }), "IDEMPOTENCY_CONFLICT");
  const listed = coordinator.dispatch("root", "task.list", {}).value as TaskRecord[];
  assert.equal(listed.length, 1);
});

test("agent.spawn replay returns the original child identity and token without a second agent", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const spawnArgs = { role: "worker", route, capabilities: {}, taskDescription: "inspect broker" };
  const first = coordinator.dispatch("root", "agent.spawn", { ...spawnArgs, operationId: "op-spawn-1" }) as { value: { agent: AgentRecord; token: string; replayed?: boolean } };
  const replay = coordinator.dispatch("root", "agent.spawn", { ...spawnArgs, operationId: "op-spawn-1" }) as { value: { agent: AgentRecord; token: string; replayed?: boolean } };
  assert.equal(replay.value.agent.id, first.value.agent.id);
  assert.equal(replay.value.token, first.value.token);
  assert.equal(replay.value.replayed, true);
  const status = coordinator.dispatch("root", "fabric.status", {}).value as { agents: unknown[] };
  assert.equal(status.agents.length, 2);
});

test("operationId scope is per actor; one key addresses one logical request", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  const child = registerChild(coordinator, "worker-1");
  const rootTask = coordinator.dispatch("root", "task.create", { description: "root task", operationId: "same-id" }).value as TaskRecord;
  const childTask = coordinator.dispatch(child.id, "task.create", { description: "child task", operationId: "same-id" }).value as TaskRecord;
  assert.notEqual(childTask.id, rootTask.id);
  assert.notEqual(childTask.creator, rootTask.creator);
  // Within one actor the key addresses one logical request, not one per operation.
  expectCode(() => coordinator.dispatch("root", "agent.spawn", { role: "worker", route, capabilities: {}, operationId: "same-id" }), "IDEMPOTENCY_CONFLICT");
});

test("operationId is rejected for operations without durable deduplication", () => {
  const coordinator = makeCoordinator();
  registerRoot(coordinator);
  expectCode(() => coordinator.dispatch("root", "message.send", { to: "root", type: "notification", body: "x", operationId: "op-msg" }), "INVALID_ARGUMENT");
});

test("journal replay restores idempotency records; a committed task without a dedup record is not silently deduped", async () => {
  await withTempDirectory(async (directory) => {
    const journal = new Journal({ directory });
    const coordinator = makeCoordinator();
    const registration = coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true } });
    await journal.append(registration.events);
    const created = coordinator.dispatch("root", "task.create", { description: "durable dedup", operationId: "op-journal" });
    await journal.append(created.events, created.idempotency as IdempotencyRecord);
    // A pre-hardening journal has the task_changed event but no dedup record.
    const orphan = coordinator.dispatch("root", "task.create", { description: "legacy write", operationId: "op-orphan" });
    await journal.append([...orphan.events]);

    const restarted = makeCoordinator();
    const recovery = await journal.replay(restarted);
    assert.equal(recovery.committedTransactions, 3);
    const replay = restarted.dispatch("root", "task.create", { description: "durable dedup", operationId: "op-journal" }).value as TaskRecord & { replayed?: boolean };
    assert.equal(replay.id, created.value.id);
    assert.equal(replay.replayed, true);
    // The legacy transaction has no dedup record to restore, so a retry under
    // its operationId cannot be matched and creates a distinct task.
    const retry = restarted.dispatch("root", "task.create", { description: "legacy write", operationId: "op-orphan" }).value as TaskRecord;
    assert.notEqual(retry.id, orphan.value.id);
  });
});

test("an uncommitted transaction never restores its idempotency record", async () => {
  await withTempDirectory(async (directory) => {
    const journal = new Journal({ directory });
    await journal.open();
    const coordinator = makeCoordinator();
    const registration = coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true } });
    await journal.append(registration.events);
    const created = coordinator.dispatch("root", "task.create", { description: "partial write", operationId: "op-partial" });
    // begin + events but no commit marker: the transaction must stay invisible.
    await appendFile(journal.filePath, `${JSON.stringify({ kind: "begin", txId: "tx-partial", at: 1 })}\n`, "utf8");
    await appendFile(journal.filePath, `${JSON.stringify({ kind: "events", txId: "tx-partial", events: created.events, idempotency: created.idempotency })}\n`, "utf8");

    const restarted = makeCoordinator();
    const recovery = await journal.replay(restarted);
    assert.equal(recovery.committedTransactions, 1);
    assert.equal(recovery.ignoredTail, true);
    const fresh = restarted.dispatch("root", "task.create", { description: "partial write", operationId: "op-partial" }).value as TaskRecord & { replayed?: boolean };
    assert.notEqual(fresh.replayed, true);
    const listed = restarted.dispatch("root", "task.list", {}).value as TaskRecord[];
    assert.equal(listed.length, 1);
  });
});

test("checkpointed state carries idempotency records across restart", async () => {
  await withTempDirectory(async (directory) => {
    const journal = new Journal({ directory });
    const coordinator = makeCoordinator();
    registerRoot(coordinator);
    const created = coordinator.dispatch("root", "task.create", { description: "checkpoint dedup", operationId: "op-checkpoint" });
    await journal.append(created.events, created.idempotency as IdempotencyRecord);
    await journal.checkpoint(coordinator.exportState());
    const restarted = makeCoordinator();
    await journal.replay(restarted);
    const replay = restarted.dispatch("root", "task.create", { description: "checkpoint dedup", operationId: "op-checkpoint" }).value as TaskRecord & { replayed?: boolean };
    assert.equal(replay.id, created.value.id);
    assert.equal(replay.replayed, true);
  });
});

test("an ambiguous persistence failure is retried once under the same operationId", async () => {
  class FlakyJournal extends Journal {
    failNext = true;

    override async append(events: readonly CoordinatorEvent[], idempotency?: IdempotencyRecord, at?: number): Promise<string> {
      if (this.failNext && events.some((event) => event.type === "task_changed")) {
        this.failNext = false;
        throw new Error("simulated disk failure");
      }
      return super.append(events, idempotency, at);
    }
  }

  await withTempDirectory(async (directory) => {
    const journal = new FlakyJournal({ directory });
    const server = new BrokerServer({ directory, rootId: "fabric", rootAgentId: "root", journal, config: { maxTotalAgents: 8 } });
    await server.start();
    try {
      const root = new BrokerClient({ endpoint: server.endpoint, agentId: "root" });
      await root.request("agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true } });
      const created = await root.requestIdempotent<TaskRecord & { replayed?: boolean }>("task.create", { description: "flaky write" });
      assert.equal(created.replayed, undefined);
      const listed = await root.request<TaskRecord[]>("task.list", {});
      assert.equal(listed.length, 1);
      root.close();
    } finally {
      await server.stop();
    }
  });
});

test("a lost response after a committed write replays the original task on retry", async () => {
  class DelayedJournal extends Journal {
    private readonly gate: Promise<void>;
    private releaseGate!: () => void;
    private notifyEntered!: () => void;
    readonly entered: Promise<void>;

    constructor(directory: string) {
      super({ directory });
      this.gate = new Promise<void>((resolve) => {
        this.releaseGate = resolve;
      });
      this.entered = new Promise<void>((resolve) => {
        this.notifyEntered = resolve;
      });
    }

    release(): void {
      this.releaseGate();
    }

    override async append(events: readonly CoordinatorEvent[], idempotency?: IdempotencyRecord, at?: number): Promise<string> {
      if (events.some((event) => event.type === "task_changed")) {
        this.notifyEntered();
        await this.gate;
      }
      return super.append(events, idempotency, at);
    }
  }

  await withTempDirectory(async (directory) => {
    const journal = new DelayedJournal(directory);
    const server = new BrokerServer({ directory, rootId: "fabric", rootAgentId: "root", journal, config: { maxTotalAgents: 8 } });
    await server.start();
    try {
      const root = new BrokerClient({ endpoint: server.endpoint, agentId: "root", requestTimeoutMs: 100 });
      await root.request("agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true } });
      const slow = root.requestIdempotent<TaskRecord & { replayed?: boolean }>("task.create", { description: "response lost" }, "op-lost");
      // The first attempt blocks inside the journal and times out client-side,
      // so its response is ambiguous; the retry then waits behind it.
      await journal.entered;
      await new Promise((resolve) => setTimeout(resolve, 150));
      journal.release();
      const created = await slow;
      const listed = await root.request<TaskRecord[]>("task.list", {});
      assert.equal(listed.length, 1);
      assert.equal(listed[0].id, created.id);
      root.close();
    } finally {
      journal.release();
      await server.stop();
    }
  });
});
