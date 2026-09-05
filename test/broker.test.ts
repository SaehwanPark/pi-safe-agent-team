import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BrokerClient } from "../src/broker/client.ts";
import { BrokerServer } from "../src/broker/server.ts";
import type { AgentRecord, ModelRoute } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

function waitForEvent(client: BrokerClient, predicate: (event: { event: string; data: any }) => boolean): Promise<{ event: string; data: any }> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      unsubscribe();
      reject(new Error("timed out waiting for broker event"));
    }, 2_000);
    const unsubscribe = client.onEvent((event) => {
      if (!predicate(event)) return;
      clearTimeout(timer);
      unsubscribe();
      resolve(event);
    });
  });
}

test("broker authenticates actors, journals mutations, and notifies durable mailboxes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-broker-"));
  const server = new BrokerServer({ directory, rootId: "fabric", rootAgentId: "root", maintenanceMs: 60_000 });
  const root = new BrokerClient({ endpoint: server.endpoint, agentId: "root" });
  let child: BrokerClient | undefined;
  try {
    await server.start();
    await root.connect();
    const registered = await root.request<{ agent: AgentRecord; token: string }>("agent.register", {
      rootId: "fabric",
      role: "root",
      route,
      capabilities: { maySpawn: true, mayMessagePeers: true },
    });
    assert.ok(registered.token);
    const impersonator = new BrokerClient({ endpoint: server.endpoint, agentId: "root" });
    await assert.rejects(() => impersonator.connect(), /invalid agent reconnect credential/);
    impersonator.close();
    const rogue = new BrokerClient({ endpoint: server.endpoint, agentId: "rogue" });
    await rogue.connect();
    await assert.rejects(() => rogue.request("agent.register", { rootId: "fabric", route }), /Only root agent/);
    rogue.close();
    const spawned = await root.request<{ agent: AgentRecord; token: string }>("agent.spawn", { route, capabilities: { mayMessagePeers: true } });
    child = new BrokerClient({ endpoint: server.endpoint, agentId: spawned.agent.id, token: spawned.token });
    await child.connect();
    await child.request("agent.register", { rootId: "fabric", parentId: "root", route, token: spawned.token });

    const available = waitForEvent(child, (event) => event.event === "message_sent" && event.data.message?.to === spawned.agent.id);
    const sent = await root.request<{ message: { id: string } }>("message.send", { to: spawned.agent.id, type: "inform", body: "durable hello" });
    await available;
    const inbox = await child.request<Array<{ id: string; body: string }>>("message.inbox", {});
    assert.equal(inbox[0].body, "durable hello");
    await child.request("message.ack", { messageId: sent.message.id });
    assert.equal((await child.request<Array<unknown>>("message.inbox", {})).length, 0);

    const unauthorized = new BrokerClient({ endpoint: server.endpoint, agentId: spawned.agent.id, token: "wrong" });
    await assert.rejects(() => unauthorized.connect(), /invalid agent reconnect credential/);
    unauthorized.close();
  } finally {
    child?.close();
    root.close();
    await server.stop();
    await rm(directory, { recursive: true, force: true });
  }
});
