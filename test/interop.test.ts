import test from "node:test";
import assert from "node:assert/strict";
import {
  PI_EXTENSION_INTEROP,
  getInteropRegistry,
  registerInteropProvider,
  unregisterInteropProvider,
  getInteropProvider,
} from "../src/pi/interop.ts";

test("interop registry uses Symbol.for('pi.extension-interop.v1') and handles registration", () => {
  const symbol = Symbol.for("pi.extension-interop.v1");
  assert.equal(PI_EXTENSION_INTEROP, symbol);

  const registry = getInteropRegistry();
  assert.equal(registry.version, 1);
  assert.ok(registry.providers instanceof Map);

  const testProvider = { name: "test-provider", testFn: () => 42 };
  registerInteropProvider("test.provider.v1", testProvider);

  const retrieved = getInteropProvider<typeof testProvider>("test.provider.v1");
  assert.equal(retrieved, testProvider);
  assert.equal(retrieved?.testFn(), 42);

  unregisterInteropProvider("test.provider.v1");
  assert.equal(getInteropProvider("test.provider.v1"), undefined);
});
