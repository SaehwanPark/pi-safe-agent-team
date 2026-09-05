import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BrokerClient } from "../src/broker/client.ts";
import { BrokerServer } from "../src/broker/server.ts";
import type { AgentRecord, ModelRoute } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

test("a child reconnects with its credential and recovers an unacknowledged inbox", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-reconnect-"));
  const server1 = new BrokerServer({ directory, rootId: "fabric", maintenanceMs: 60_000 });
  const root = new BrokerClient({ endpoint: server1.endpoint, agentId: "root" });
  let child: BrokerClient | undefined;
  let server2: BrokerServer | undefined;
  try {
    await server1.start();
    await root.connect();
    await root.request("agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true, mayMessagePeers: true } });
    const spawned = await root.request<{ agent: AgentRecord; token: string }>("agent.spawn", { route });
    child = new BrokerClient({ endpoint: server1.endpoint, agentId: spawned.agent.id, token: spawned.token });
    await child.connect();
    await child.request("agent.register", { rootId: "fabric", parentId: "root", route, token: spawned.token });
    await root.request("message.send", { to: spawned.agent.id, type: "inform", body: "survives restart" });
    await server1.stop();
    server2 = new BrokerServer({ directory, rootId: "fabric", maintenanceMs: 60_000 });
    await server2.start();
    await child.reconnect();
    await child.request("agent.register", { rootId: "fabric", parentId: "root", route, token: spawned.token });
    const inbox = await child.request<Array<{ body: string }>>("message.inbox", {});
    assert.equal(inbox.map((message) => message.body).join("\n"), "survives restart");
  } finally {
    child?.close();
    root.close();
    await server2?.stop();
    if (server1.isStarted()) await server1.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("broker recovery reattaches a previously assigned task", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-recovery-task-"));
  const server1 = new BrokerServer({ directory, rootId: "fabric", maintenanceMs: 60_000 });
  const root = new BrokerClient({ endpoint: server1.endpoint, agentId: "root" });
  let child: BrokerClient | undefined;
  let server2: BrokerServer | undefined;
  try {
    await server1.start();
    await root.connect();
    await root.request("agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true, mayMessagePeers: true } });
    const spawned = await root.request<{ agent: AgentRecord; token: string; taskId?: string }>("agent.spawn", { route, taskDescription: "recover this task" });
    assert.ok(spawned.taskId);
    child = new BrokerClient({ endpoint: server1.endpoint, agentId: spawned.agent.id, token: spawned.token });
    await child.connect();
    await child.request("agent.register", { rootId: "fabric", parentId: "root", route, token: spawned.token });
    await server1.stop();
    server2 = new BrokerServer({ directory, rootId: "fabric", maintenanceMs: 60_000 });
    await server2.start();
    await child.reconnect();
    const registered = await child.request<{ agent: AgentRecord }>("agent.register", { rootId: "fabric", parentId: "root", route, token: spawned.token });
    assert.equal(registered.agent.taskId, spawned.taskId);
    const task = await child.request<{ owner?: string; status: string }>("task.show", { taskId: spawned.taskId });
    assert.equal(task.owner, spawned.agent.id);
    assert.equal(task.status, "active");
  } finally {
    child?.close();
    root.close();
    await server2?.stop();
    if (server1.isStarted()) await server1.stop();
    await rm(directory, { recursive: true, force: true });
  }
});
