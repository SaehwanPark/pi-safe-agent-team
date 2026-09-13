import test from "node:test";
import assert from "node:assert/strict";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import safeAgentsTeam from "../index.ts";
import { FabricRuntime } from "../src/pi/runtime.ts";
import { getInteropProvider, unregisterInteropProvider } from "../src/pi/interop.ts";

type Handler = (event: unknown, context: unknown) => unknown;

function makeExtensionHarness(): Map<string, Handler[]> {
  const handlers = new Map<string, Handler[]>();
  const api = {
    on(event: string, handler: Handler) {
      handlers.set(event, [...(handlers.get(event) ?? []), handler]);
    },
    registerTool() {},
    registerMessageRenderer() {},
    registerCommand() {},
  } as unknown as ExtensionAPI;
  safeAgentsTeam(api);
  return handlers;
}

function flushLifecycle(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

function makeContext(): ExtensionContext {
  return {
    cwd: process.cwd(),
    ui: {
      notify() {},
      setStatus() {},
    },
  } as unknown as ExtensionContext;
}

interface RuntimeMethods {
  ensureRoot: FabricRuntime["ensureRoot"];
  request: FabricRuntime["request"];
  stop: FabricRuntime["stop"];
  abortDescendants: FabricRuntime["abortDescendants"];
}

async function withRuntimeSpies(
  request: (operation: string, args?: Record<string, unknown>) => Promise<unknown>,
  run: () => Promise<void>,
  onStop: () => void = () => {},
  onAbort: () => void = () => {},
): Promise<number> {
  const prototype = FabricRuntime.prototype as unknown as RuntimeMethods;
  const originals = { ensureRoot: prototype.ensureRoot, request: prototype.request, stop: prototype.stop, abortDescendants: prototype.abortDescendants };
  const rootDescriptor = Object.getOwnPropertyDescriptor(FabricRuntime.prototype, "rootAgentId");
  let stopCalls = 0;
  prototype.ensureRoot = async () => {};
  prototype.request = async <T>(_operation: string, args: Record<string, unknown> = {}) => (await request(_operation, args)) as T;
  prototype.stop = async () => {
    stopCalls += 1;
    onStop();
  };
  prototype.abortDescendants = async () => {
    onAbort();
    return [];
  };
  Object.defineProperty(FabricRuntime.prototype, "rootAgentId", { configurable: true, get: () => "root" });
  try {
    await run();
  } finally {
    unregisterInteropProvider("safe-agent-team.fabric-state.v1");
    prototype.ensureRoot = originals.ensureRoot;
    prototype.request = originals.request;
    prototype.stop = originals.stop;
    prototype.abortDescendants = originals.abortDescendants;
    if (rootDescriptor) Object.defineProperty(FabricRuntime.prototype, "rootAgentId", rootDescriptor);
    else delete (FabricRuntime.prototype as unknown as Record<string, unknown>).rootAgentId;
  }
  return stopCalls;
}

test("finalizes the root turn once after Pi settles while observing agent_end", async () => {
  const operations: string[] = [];
  const stopCalls = await withRuntimeSpies(async (operation) => {
    operations.push(operation);
  }, async () => {
    const handlers = makeExtensionHarness();
    const context = makeContext();
    await handlers.get("session_start")?.[0]?.({}, context);
    assert.equal(handlers.has("agent_end"), true);
    handlers.get("agent_settled")?.[0]?.({}, context);
    await flushLifecycle();
    await handlers.get("session_shutdown")?.[0]?.({}, context);
    assert.deepEqual(operations, ["agent.end_turn"]);
  });
  assert.equal(stopCalls, 1);
});

test("automatic retry/compaction agent_start events share one logical root broker turn", async () => {
  const operations: string[] = [];
  await withRuntimeSpies(async (operation) => {
    operations.push(operation);
  }, async () => {
    const handlers = makeExtensionHarness();
    const context = makeContext();
    await handlers.get("session_start")?.[0]?.({}, context);
    handlers.get("agent_start")?.[0]?.({}, context);
    await flushLifecycle();
    handlers.get("agent_end")?.[0]?.({ messages: [{ role: "assistant", stopReason: "error", errorMessage: "temporary provider failure", usage: { input: 1, cacheRead: 0, output: 0 } }] }, context);
    handlers.get("agent_start")?.[0]?.({}, context);
    await flushLifecycle();
    handlers.get("agent_end")?.[0]?.({ messages: [{ role: "assistant", stopReason: "stop", usage: { input: 1, cacheRead: 0, output: 1 } }] }, context);
    handlers.get("agent_settled")?.[0]?.({}, context);
    await flushLifecycle();
    await handlers.get("session_shutdown")?.[0]?.({}, context);
  });
  assert.deepEqual(operations, ["agent.begin_turn", "agent.end_turn"]);
});

test("root agent_start waits for broker admission before Pi can enter the provider loop", async () => {
  let admitted = false;
  let beginCalls = 0;
  await withRuntimeSpies(async (operation) => {
    if (operation === "agent.begin_turn") {
      beginCalls += 1;
      return { started: admitted };
    }
    return {};
  }, async () => {
    const handlers = makeExtensionHarness();
    const context = makeContext();
    await handlers.get("session_start")?.[0]?.({}, context);
    const start = handlers.get("agent_start")?.[0];
    assert.ok(start);
    const pending = start({}, context) as Promise<void>;
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.ok(beginCalls > 0);
    let settled = false;
    void pending.then(() => { settled = true; });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(settled, false);
    admitted = true;
    await pending;
    await handlers.get("session_shutdown")?.[0]?.({}, context);
  });
});

test("an aborted root run drains descendants after settlement", async () => {
  let abortCalls = 0;
  const context = {
    ...makeContext(),
    sessionManager: {
      getEntries: () => [{ type: "message", message: { role: "assistant", stopReason: "aborted" } }],
    },
  } as unknown as ExtensionContext;
  await withRuntimeSpies(async () => {}, async () => {
    const handlers = makeExtensionHarness();
    await handlers.get("session_start")?.[0]?.({}, context);
    handlers.get("agent_start")?.[0]?.({}, context);
    await flushLifecycle();
    handlers.get("agent_end")?.[0]?.({ messages: [{ role: "assistant", stopReason: "aborted", usage: { input: 0, cacheRead: 0, output: 0 } }] }, context);
    handlers.get("agent_settled")?.[0]?.({}, context);
    await flushLifecycle();
    await handlers.get("session_shutdown")?.[0]?.({}, context);
  }, () => {}, () => {
    abortCalls += 1;
  });
  assert.equal(abortCalls, 1);
});

test("shutdown waits for a settled handler before stopping the runtime", async () => {
  let releaseEnd!: () => void;
  let endStarted!: () => void;
  const endEntered = new Promise<void>((resolve) => {
    endStarted = resolve;
  });
  const endRelease = new Promise<void>((resolve) => {
    releaseEnd = resolve;
  });
  let stopped = false;

  await withRuntimeSpies(async (operation) => {
    if (operation === "agent.end_turn") {
      endStarted();
      await endRelease;
    }
  }, async () => {
    const handlers = makeExtensionHarness();
    const context = makeContext();
    await handlers.get("session_start")?.[0]?.({}, context);
    handlers.get("agent_settled")?.[0]?.({}, context);
    await flushLifecycle();
    const shutdown = handlers.get("session_shutdown")?.[0];
    assert.ok(shutdown);
    const shutdownPromise = shutdown({}, context) as Promise<void>;
    await endEntered;
    assert.equal(stopped, false);
    releaseEnd();
    await shutdownPromise;
  }, () => {
    stopped = true;
  });

  assert.equal(stopped, true);
});

test("re-registers fabric provider on session_start after session_shutdown", async () => {
  await withRuntimeSpies(async () => {}, async () => {
    const handlers = makeExtensionHarness();
    const context = makeContext();

    // Initial registration happened when extension was loaded
    assert.ok(getInteropProvider("safe-agent-team.fabric-state.v1"));

    // session_start (Session A)
    await handlers.get("session_start")?.[0]?.({}, context);
    assert.ok(getInteropProvider("safe-agent-team.fabric-state.v1"));

    // session_shutdown (e.g. on resume/fork/reload)
    await handlers.get("session_shutdown")?.[0]?.({}, context);
    assert.equal(getInteropProvider("safe-agent-team.fabric-state.v1"), undefined);

    // session_start (Session B)
    await handlers.get("session_start")?.[0]?.({}, context);
    assert.ok(getInteropProvider("safe-agent-team.fabric-state.v1"));

    // Cleanup
    await handlers.get("session_shutdown")?.[0]?.({}, context);
  });
});
