import test from "node:test";
import assert from "node:assert/strict";
import {
  PI_EXTENSION_INTEROP,
  getInteropRegistry,
  registerInteropProvider,
  unregisterInteropProvider,
  getInteropProvider,
  createEmbeddedContextController,
  LCM_EMBEDDED_CONTEXT_PROVIDER_NAME,
  LEGACY_LCM_EMBEDDED_CONTEXT_PROVIDER_NAME,
  type EmbeddedContextHost,
  type EmbeddedContextManager,
} from "../src/pi/interop.ts";

test("interop registry uses Symbol.for('pi.extension-interop.v1') and handles registration", () => {
  const symbol = Symbol.for("pi.extension-interop.v1");
  assert.equal(PI_EXTENSION_INTEROP, symbol);

  const registry = getInteropRegistry();
  assert.equal(registry.version, 1);
  assert.ok(registry.providers instanceof Map);

  const testProvider = { name: "test-provider", testFn: () => 42 };
  registerInteropProvider("test.provider.v1", testProvider);

  // Idempotent re-registration of exact same instance succeeds
  registerInteropProvider("test.provider.v1", testProvider);

  // Conflicting registration throws
  assert.throws(
    () => registerInteropProvider("test.provider.v1", { name: "different" }),
    /already registered with a different instance/
  );

  const retrieved = getInteropProvider<typeof testProvider>("test.provider.v1");
  assert.equal(retrieved, testProvider);
  assert.equal(retrieved?.testFn(), 42);

  // Unregister with non-matching instance does not remove it
  unregisterInteropProvider("test.provider.v1", { name: "wrong-instance" });
  assert.equal(getInteropProvider("test.provider.v1"), testProvider);

  // Unregister with matching instance removes it
  unregisterInteropProvider("test.provider.v1", testProvider);
  assert.equal(getInteropProvider("test.provider.v1"), undefined);
});

test("canonical LCM provider name wins while the legacy alias remains compatible", () => {
  const manager = (): EmbeddedContextManager => ({
    observeTurnStart() {},
    observeTurnEnd() {},
    async observeSettled() {},
    async transformToolResult(result) { return result; },
    snapshot() { return { tokens: null, contextWindow: null, tokenSource: "estimated" }; },
    dispose() {},
  });
  const host: EmbeddedContextHost = {
    getContextUsage: () => null,
    getContextEntries: () => [],
    compact: async () => {},
  };
  const canonical = () => manager();
  const legacy = () => ({ ...manager(), snapshot: () => ({ tokens: 1, contextWindow: 1, tokenSource: "estimated" }) });
  registerInteropProvider(LEGACY_LCM_EMBEDDED_CONTEXT_PROVIDER_NAME, legacy);
  registerInteropProvider(LCM_EMBEDDED_CONTEXT_PROVIDER_NAME, canonical);
  try {
    const selected = createEmbeddedContextController(host);
    assert.ok(selected);
    assert.equal(selected.snapshot().tokens, null);
  } finally {
    unregisterInteropProvider(LCM_EMBEDDED_CONTEXT_PROVIDER_NAME, canonical);
    unregisterInteropProvider(LEGACY_LCM_EMBEDDED_CONTEXT_PROVIDER_NAME, legacy);
  }
});

test("interop registry preserves an unknown future registry version", () => {
  const globalObj = globalThis as Record<symbol, unknown>;
  const previous = globalObj[PI_EXTENSION_INTEROP];
  const foreign = { version: 99, providers: new Map([["foreign.provider.v99", {}]]) };
  globalObj[PI_EXTENSION_INTEROP] = foreign;
  const provider = { name: "isolated" };
  try {
    const local = getInteropRegistry();
    assert.notEqual(local, foreign);
    registerInteropProvider("safe-agent-test.future", provider);
    assert.equal(foreign.providers.has("safe-agent-test.future"), false);
    assert.equal(getInteropProvider("safe-agent-test.future"), provider);
    unregisterInteropProvider("safe-agent-test.future", provider);
  } finally {
    globalObj[PI_EXTENSION_INTEROP] = previous;
  }
});
