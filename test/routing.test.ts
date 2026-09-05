import test from "node:test";
import assert from "node:assert/strict";
import { resolveRoute } from "../src/core/routing.ts";
import { resolveModelRoute } from "../src/pi/model-routing.ts";
import type { Model } from "@earendil-works/pi-ai";

const model = { provider: "local", id: "small", name: "small", api: "test", baseUrl: "http://localhost", reasoning: true, input: ["text"], cost: {}, contextWindow: 8192, maxTokens: 1024 } as unknown as Model<any>;

test("model route precedence is explicit, role, defaults, parent, global", () => {
  const explicit = resolveRoute({
    explicit: { model: "openai/gpt", thinking: "high" },
    role: { model: "role/model", thinking: "low" },
    defaults: { model: "default/model" },
    parent: { provider: "parent", model: "parent-model", thinking: "medium" },
  });
  assert.deepEqual(explicit, { provider: "openai", model: "gpt", thinking: "high", source: "explicit" });
  const inherited = resolveRoute({ explicit: { model: "inherit" }, parent: { provider: "local", model: "small", thinking: "low" } });
  assert.deepEqual(inherited, { provider: "local", model: "small", thinking: "low", source: "parent" });
  assert.throws(() => resolveRoute({ explicit: { provider: "other" }, parent: { provider: "local", model: "small", thinking: "low" } }), /requires an explicit model/);
});

test("Pi adapter resolves an exact registry object and rejects missing models", () => {
  const registry = { find: (provider: string, id: string) => provider === "local" && id === "small" ? model : undefined } as any;
  const resolved = resolveModelRoute({ registry, explicit: { model: "local/small", thinking: "medium" } });
  assert.equal(resolved.model, model);
  assert.equal(resolved.route.provider, "local");
  assert.throws(() => resolveModelRoute({ registry, explicit: { model: "other/missing" } }), /not available/);
});
