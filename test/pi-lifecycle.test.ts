import test from "node:test";
import assert from "node:assert/strict";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import safeAgentsTeam from "../index.ts";
import { FabricRuntime } from "../src/pi/runtime.ts";
import { unregisterInteropProvider } from "../src/pi/interop.ts";

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
}

async function withRuntimeSpies(
  request: (operation: string, args?: Record<string, unknown>) => Promise<unknown>,
  run: () => Promise<void>,
  onStop: () => void = () => {},
): Promise<number> {
  const prototype = FabricRuntime.prototype as unknown as RuntimeMethods;
  const originals = { ensureRoot: prototype.ensureRoot, request: prototype.request, stop: prototype.stop };
  const rootDescriptor = Object.getOwnPropertyDescriptor(FabricRuntime.prototype, "rootAgentId");
  let stopCalls = 0;
  prototype.ensureRoot = async () => {};
  prototype.request = async <T>(_operation: string, args: Record<string, unknown> = {}) => (await request(_operation, args)) as T;
  prototype.stop = async () => {
    stopCalls += 1;
    onStop();
  };
  Object.defineProperty(FabricRuntime.prototype, "rootAgentId", { configurable: true, get: () => "root" });
  try {
    await run();
  } finally {
    unregisterInteropProvider("safe-agent-team.fabric-state.v1");
    prototype.ensureRoot = originals.ensureRoot;
    prototype.request = originals.request;
    prototype.stop = originals.stop;
    if (rootDescriptor) Object.defineProperty(FabricRuntime.prototype, "rootAgentId", rootDescriptor);
    else delete (FabricRuntime.prototype as unknown as Record<string, unknown>).rootAgentId;
  }
  return stopCalls;
}

test("finalizes the root turn once after Pi settles instead of on agent_end", async () => {
  const operations: string[] = [];
  const stopCalls = await withRuntimeSpies(async (operation) => {
    operations.push(operation);
  }, async () => {
    const handlers = makeExtensionHarness();
    const context = makeContext();
    await handlers.get("session_start")?.[0]?.({}, context);
    assert.equal(handlers.has("agent_end"), false);
    handlers.get("agent_settled")?.[0]?.({}, context);
    await flushLifecycle();
    await handlers.get("session_shutdown")?.[0]?.({}, context);
    assert.deepEqual(operations, ["agent.end_turn"]);
  });
  assert.equal(stopCalls, 1);
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
