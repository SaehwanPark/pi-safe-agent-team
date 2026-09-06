import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import net from "node:net";
import { join } from "node:path";
import { BrokerClient } from "../src/broker/client.ts";
import { BrokerServer, detectCaseInsensitivePaths, resolveBrokerConfig } from "../src/broker/server.ts";
import { Journal } from "../src/broker/journal.ts";
import type { AgentRecord, CoordinatorEvent, IdempotencyRecord, ModelRoute } from "../src/core/types.ts";

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

class GateJournal extends Journal {
  delayed = false;
  entered?: () => void;
  private readonly gate: Promise<void>;
  private releaseGate!: () => void;

  constructor(directory: string) {
    super({ directory });
    this.gate = new Promise((resolve) => {
      this.releaseGate = resolve;
    });
  }

  release(): void {
    this.releaseGate();
  }

  async waitUntilEntered(): Promise<void> {
    await new Promise<void>((resolve) => {
      this.entered = resolve;
    });
  }

  override async append(events: readonly CoordinatorEvent[], idempotency?: IdempotencyRecord, at?: number): Promise<string> {
    if (this.delayed) {
      this.entered?.();
      await this.gate;
    }
    return super.append(events, idempotency, at);
  }
}

interface RawFrameReader {
  next(): Promise<any>;
  send(frame: unknown): void;
}

function rawFrameReader(socket: net.Socket): RawFrameReader {
  let buffer = "";
  const queued: any[] = [];
  const waiters: Array<(frame: any) => void> = [];
  socket.setEncoding("utf8");
  socket.on("data", (chunk: string) => {
    buffer += chunk;
    let newline = buffer.indexOf("\n");
    while (newline >= 0) {
      const raw = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      if (raw.trim()) {
        const frame = JSON.parse(raw);
        const waiter = waiters.shift();
        if (waiter) waiter(frame);
        else queued.push(frame);
      }
      newline = buffer.indexOf("\n");
    }
  });
  return {
    next: () => queued.length > 0 ? Promise.resolve(queued.shift()) : new Promise((resolve) => waiters.push(resolve)),
    send: (frame) => socket.write(`${JSON.stringify(frame)}\n`),
  };
}

async function nextRawMatching(reader: RawFrameReader, predicate: (frame: any) => boolean): Promise<any> {
  while (true) {
    const frame = await reader.next();
    if (predicate(frame)) return frame;
  }
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

test("broker coalesces duplicate request frames while the mutation is in flight", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-inflight-"));
  const journal = new GateJournal(directory);
  const server = new BrokerServer({ directory, journal, rootId: "fabric", rootAgentId: "root", maintenanceMs: 60_000 });
  const root = new BrokerClient({ endpoint: server.endpoint, agentId: "root" });
  let raw: net.Socket | undefined;
  try {
    await server.start();
    await root.connect();
    const registered = await root.request<{ token: string }>("agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true } });
    raw = net.createConnection(server.endpoint);
    const reader = rawFrameReader(raw);
    await new Promise<void>((resolve, reject) => {
      raw!.once("connect", resolve);
      raw!.once("error", reject);
    });
    reader.send({ kind: "hello", version: 1, agentId: "root", token: registered.token });
    await nextRawMatching(reader, (frame) => frame.kind === "hello" && frame.ok === true);

    journal.delayed = true;
    const duplicate = { id: "same-frame", version: 1, op: "agent.spawn", args: { route, capabilities: { mayMessagePeers: true } } };
    reader.send(duplicate);
    reader.send(duplicate);
    await journal.waitUntilEntered();
    journal.release();

    const responses: any[] = [];
    while (responses.length < 2) {
      responses.push(await nextRawMatching(reader, (frame) => frame.id === duplicate.id));
    }
    assert.equal(responses[0].ok, true);
    assert.equal(responses[1].ok, true);
    assert.equal(responses[0].result.agent.id, responses[1].result.agent.id);
    const status = await root.request<{ agents: AgentRecord[] }>("fabric.status");
    assert.equal(status.agents.length, 2);
  } finally {
    raw?.destroy();
    root.close();
    await server.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("filesystem case-folding detection probes the volume without leaving files", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-fabric-case-"));
  try {
    const detected = detectCaseInsensitivePaths(directory);
    assert.equal(typeof detected, "boolean");
    if (process.platform === "win32") assert.equal(detected, true);
    assert.deepEqual(await readdir(directory), []);
    // An explicit config value always wins over detection.
    assert.equal(resolveBrokerConfig({ directory, rootId: "fabric", config: { caseInsensitivePaths: false } }).caseInsensitivePaths, false);
    assert.equal(typeof resolveBrokerConfig({ directory, rootId: "fabric" }).caseInsensitivePaths, "boolean");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("broker client request timeout can be updated on client instance", () => {
  const client = new BrokerClient({ endpoint: "mock", agentId: "test", requestTimeoutMs: 1_000 });
  assert.equal(client.requestTimeoutMs, 1_000);
  client.requestTimeoutMs = 2_000;
  assert.equal(client.requestTimeoutMs, 2_000);
});

