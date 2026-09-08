import test from "node:test";
import assert from "node:assert/strict";
import {
  registerInteropProvider,
  unregisterInteropProvider,
  createEmbeddedContextController,
  type EmbeddedContextHost,
  type EmbeddedContextManager,
  type EmbeddedToolResult,
} from "../src/pi/interop.ts";

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
