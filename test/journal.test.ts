import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, appendFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Coordinator } from "../src/core/coordinator.ts";
import { Journal } from "../src/broker/journal.ts";
import type { ModelRoute } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

function make(idFactory?: (prefix: string) => string): Coordinator {
  return new Coordinator({ rootId: "fabric", idFactory, clock: () => 1_000 });
}

test("journal replays committed coordinator events and preserves message sequence", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-journal-"));
  try {
    let id = 0;
    const first = make((prefix) => `${prefix}-${++id}`);
    const journal = new Journal({ directory });
    await journal.open();
    const register = first.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: { maySpawn: true, mayMessagePeers: true } });
    await journal.append(register.events);
    const child = first.dispatch("root", "agent.spawn", { route, capabilities: { mayMessagePeers: true } });
    await journal.append(child.events);
    const message = first.dispatch("root", "message.send", { to: (child.value as { agent: { id: string } }).agent.id, type: "inform", body: "hello" });
    await journal.append(message.events);

    const restored = make((prefix) => `${prefix}-restored-${++id}`);
    const replay = await journal.replay(restored);
    assert.equal(replay.committedTransactions, 3);
    assert.equal(restored.dispatch("root", "agent.status", {}).value.id, "root");
    const childId = (child.value as { agent: { id: string } }).agent.id;
    assert.equal(restored.dispatch("root", "message.list", { scope: "all" }).value[0].from, "root");
    const next = restored.dispatch("root", "message.send", { to: childId, type: "inform", body: "second" }).value.message;
    assert.equal(next.senderSequence, 2);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("incomplete or malformed final journal records are ignored without losing committed state", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-journal-tail-"));
  try {
    const first = make((prefix) => `${prefix}-one`);
    const journal = new Journal({ directory });
    const result = first.dispatch("root", "agent.register", { rootId: "fabric", route });
    await journal.append(result.events);
    await appendFile(journal.filePath, '{"kind":"begin","txId":"unfinished","at":1000}\n{"kind":"events","txId":"unfinished","events":[');
    const restored = make((prefix) => `${prefix}-two`);
    const replay = await journal.replay(restored);
    assert.equal(replay.ignoredTail, true);
    assert.equal(restored.dispatch("root", "agent.status", {}).value.id, "root");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
