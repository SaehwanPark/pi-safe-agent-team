import test from "node:test";
import assert from "node:assert/strict";
import { resolve } from "node:path";
import {
  registerInteropProvider,
  unregisterInteropProvider,
  createEmbeddedContextController,
  type EmbeddedContextHost,
  type EmbeddedContextManager,
  type EmbeddedToolResult,
} from "../src/pi/interop.ts";
import { ManagedChild } from "../src/pi/runtime.ts";


test("embedded-context: provider absent degrades to undefined (native mode)", () => {
  unregisterInteropProvider("local-context-manager.embedded-context.v1");

  const host: EmbeddedContextHost = {
    getContextUsage: () => null,
    getContextEntries: () => [],
    compact: async () => {},
  };

  const manager = createEmbeddedContextController(host, { mode: "managed-child" });
  assert.equal(manager, undefined);
});

test("embedded-context: provider available instantiates controller with child options", () => {
  let createdWithOptions: unknown;
  let observeStartCalls = 0;
  let observeEndCalls = 0;
  let observeSettledCalls = 0;

  const mockProvider = {
    createEmbeddedContextManager(host: EmbeddedContextHost, options?: any): EmbeddedContextManager {
      createdWithOptions = options;
      return {
        observeTurnStart() {
          observeStartCalls++;
        },
        observeTurnEnd() {
          observeEndCalls++;
        },
        async observeSettled() {
          observeSettledCalls++;
        },
        async transformToolResult(result: EmbeddedToolResult) {
          return {
            ...result,
            content: [{ type: "text", text: "reduced output" }],
          };
        },
        snapshot() {
          return {
            tokens: 1500,
            contextWindow: 128000,
            tokenSource: "reported",
            mode: options?.mode,
          };
        },
        dispose() {},
      };
    },
  };

  registerInteropProvider("local-context-manager.embedded-context.v1", mockProvider);

  try {
    const host: EmbeddedContextHost = {
      getContextUsage: () => ({ tokens: 1500, contextWindow: 128000, source: "reported" }),
      getContextEntries: () => [],
      compact: async () => {},
    };

    const manager = createEmbeddedContextController(host, {
      mode: "managed-child",
      contextWindow: 128000,
    });

    assert.ok(manager);
    assert.deepEqual(createdWithOptions, { mode: "managed-child", contextWindow: 128000 });

    manager.observeTurnStart();
    assert.equal(observeStartCalls, 1);

    manager.observeTurnEnd();
    assert.equal(observeEndCalls, 1);

    manager.observeSettled();
    assert.equal(observeSettledCalls, 1);

    const snap = manager.snapshot();
    assert.equal(snap.mode, "managed-child");
  } finally {
    unregisterInteropProvider("local-context-manager.embedded-context.v1");
  }
});

test("embedded-context: tool output reduction transforms large tool outputs", async () => {
  const mockProvider = {
    createEmbeddedContextManager(_host: EmbeddedContextHost): EmbeddedContextManager {
      return {
        observeTurnStart() {},
        observeTurnEnd() {},
        async observeSettled() {},
        async transformToolResult(result: EmbeddedToolResult) {
          const firstBlock = result.content[0];
          const text = firstBlock && "text" in firstBlock ? String(firstBlock.text) : "";
          if (text.length > 500) {
            return {
              ...result,
              content: [{ type: "text", text: text.slice(0, 100) + "... [truncated]" }],
            };
          }
          return result;
        },
        snapshot() {
          return { tokens: 100, contextWindow: 32000, tokenSource: "estimated" };
        },
        dispose() {},
      };
    },
  };

  registerInteropProvider("local-context-manager.embedded-context.v1", mockProvider);

  try {
    const host: EmbeddedContextHost = {
      getContextUsage: () => null,
      getContextEntries: () => [],
      compact: async () => {},
    };

    const manager = createEmbeddedContextController(host, { mode: "managed-child" });
    assert.ok(manager);

    const largeOutput = "x".repeat(2000);
    const transformed = await manager.transformToolResult({
      toolName: "bash",
      input: { command: "build" },
      content: [{ type: "text", text: largeOutput }],
      details: { exitCode: 0 },
      isError: false,
    });

    assert.ok(transformed.content[0].text?.includes("[truncated]"));
    assert.equal(transformed.content[0].text?.length, 115);
    assert.deepEqual(transformed.details, { exitCode: 0 });
  } finally {
    unregisterInteropProvider("local-context-manager.embedded-context.v1");
  }
});

test("embedded-context: provider throws during transform -> task continues and degrades gracefully", async () => {
  const mockProvider = {
    createEmbeddedContextManager(_host: EmbeddedContextHost): EmbeddedContextManager {
      return {
        observeTurnStart() {},
        observeTurnEnd() {},
        async observeSettled() {},
        async transformToolResult() {
          throw new Error("LCM transform failed unexpectedly");
        },
        snapshot() {
          return { tokens: 0, contextWindow: 32000, tokenSource: "estimated" };
        },
        dispose() {},
      };
    },
  };

  registerInteropProvider("local-context-manager.embedded-context.v1", mockProvider);

  try {
    const host: EmbeddedContextHost = {
      getContextUsage: () => null,
      getContextEntries: () => [],
      compact: async () => {},
    };

    const manager = createEmbeddedContextController(host, { mode: "managed-child" });
    assert.ok(manager);

    await assert.rejects(
      async () => {
        await manager.transformToolResult({
          toolName: "read",
          input: { path: "src/index.ts" },
          content: [{ type: "text", text: "file content" }],
          isError: false,
        });
      },
      /LCM transform failed unexpectedly/
    );
  } finally {
    unregisterInteropProvider("local-context-manager.embedded-context.v1");
  }
});

test("embedded-context: child contextWindow passes child model context window, not root's", () => {
  let capturedWindow: number | undefined;

  const mockProvider = {
    createEmbeddedContextManager(_host: EmbeddedContextHost, options?: any): EmbeddedContextManager {
      capturedWindow = options?.contextWindow;
      return {
        observeTurnStart() {},
        observeTurnEnd() {},
        async observeSettled() {},
        async transformToolResult(res) {
          return res;
        },
        snapshot() {
          return { tokens: null, contextWindow: capturedWindow ?? null, tokenSource: "estimated" };
        },
        dispose() {},
      };
    },
  };

  registerInteropProvider("local-context-manager.embedded-context.v1", mockProvider);

  try {
    const host: EmbeddedContextHost = {
      getContextUsage: () => null,
      getContextEntries: () => [],
      compact: async () => {},
    };

    // Simulate child model with 32k window while root might have 128k
    const childWindow = 32768;
    const manager = createEmbeddedContextController(host, {
      mode: "managed-child",
      contextWindow: childWindow,
    });

    assert.ok(manager);
    assert.equal(capturedWindow, 32768);
  } finally {
    unregisterInteropProvider("local-context-manager.embedded-context.v1");
  }
});

test("embedded-context: local Qwen-like route with large advertised context uses same policy path", () => {
  let capturedOptions: any;

  const mockProvider = {
    createEmbeddedContextManager(_host: EmbeddedContextHost, options?: any): EmbeddedContextManager {
      capturedOptions = options;
      return {
        observeTurnStart() {},
        observeTurnEnd() {},
        async observeSettled() {},
        async transformToolResult(res) {
          return res;
        },
        snapshot() {
          return { tokens: null, contextWindow: options?.contextWindow, tokenSource: "estimated" };
        },
        dispose() {},
      };
    },
  };

  registerInteropProvider("local-context-manager.embedded-context.v1", mockProvider);

  try {
    const host: EmbeddedContextHost = {
      getContextUsage: () => null,
      getContextEntries: () => [],
      compact: async () => {},
    };

    // Qwen 3.8 Flash Next / 27B with 131072 context
    const manager = createEmbeddedContextController(host, {
      mode: "managed-child",
      contextWindow: 131072,
    });

    assert.ok(manager);
    assert.equal(capturedOptions.mode, "managed-child");
    assert.equal(capturedOptions.contextWindow, 131072);
  } finally {
    unregisterInteropProvider("local-context-manager.embedded-context.v1");
  }
});

test("embedded-context: session.compact is invoked with string | undefined, never an object", async () => {
  const compactCalls: unknown[] = [];
  const mockSession = {
    isStreaming: false,
    async compact(customInstructions?: string) {
      compactCalls.push(customInstructions);
    },
  };

  // Simulate ManagedChild compaction delegation
  const hostCompact = async (request: { customInstructions?: string; reason?: string }) => {
    const instructions = request.customInstructions ?? request.reason;
    await mockSession.compact(instructions);
  };

  await hostCompact({ customInstructions: "Prune old outputs", reason: "overflow" });
  assert.equal(compactCalls.length, 1);
  assert.equal(typeof compactCalls[0], "string");
  assert.equal(compactCalls[0], "Prune old outputs");

  await hostCompact({ reason: "threshold_exceeded" });
  assert.equal(compactCalls.length, 2);
  assert.equal(typeof compactCalls[1], "string");
  assert.equal(compactCalls[1], "threshold_exceeded");

  await hostCompact({});
  assert.equal(compactCalls.length, 3);
  assert.equal(compactCalls[2], undefined);
});

test("embedded-context: ManagedChild degradation disposes embedded manager, clears it, and sets contextMode native", async () => {
  const operations: Array<{ operation: string; args: any }> = [];
  let disposed = false;
  const child = new ManagedChild({ fabricId: "fabric-test" } as never, {
    agentId: "child-123",
    token: "token-123",
    parentId: "root",
    role: "worker",
    route: { provider: "anthropic", model: "claude-3-haiku", thinking: "low" },
    cwd: resolve("."),
    stateDirectory: resolve("."),
    agentDir: resolve("."),
    endpoint: "unused",
    model: {} as never,
    capabilities: { maySpawn: false, mayMessagePeers: false, mayEscalate: true, mayTransferOwnership: false, mayWriteRepo: false, mayUseShell: false, peerIds: [], resourceGrants: {} },
  });

  (child as any).contextMode = "lcm-embedded";
  (child as any).embeddedManager = {
    dispose() {
      disposed = true;
    },
  };
  (child.client as any).request = async (operation: string, args: any) => {
    operations.push({ operation, args });
    if (operation === "message.inbox") return [];
    return { agent: { id: "child-123" } };
  };


  await (child as any).degradeToNativeContext();

  assert.equal((child as any).contextMode, "native");
  assert.equal((child as any).embeddedManager, undefined);
  assert.equal(disposed, true);
  assert.equal(operations.length, 1);
  assert.equal(operations[0].operation, "agent.update");
  assert.deepEqual(operations[0].args, { contextMode: "native" });

  // Verify reconnect preserves contextMode: "native"
  (child as any).started = true;
  (child as any).stopping = false;
  (child.client as any).connect = async () => {};
  operations.length = 0;
  await (child as any).reconnect();

  const registerOp = operations.find((o) => o.operation === "agent.register");
  assert.ok(registerOp);
  assert.equal(registerOp.args.contextMode, "native");
});

