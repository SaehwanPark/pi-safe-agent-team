import test from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadFabricConfig } from "../src/pi/config.ts";
import { FabricRuntime } from "../src/pi/runtime.ts";

test("installed runtime loads project safe-agents configuration", async () => {
  const cwd = await mkdtemp(join(tmpdir(), "safe-agents-config-"));
  const agentDir = await mkdtemp(join(tmpdir(), "safe-agents-agent-"));
  try {
    await mkdir(join(cwd, ".pi"), { recursive: true });
    await writeFile(join(cwd, ".pi", "safe-agents.json"), JSON.stringify({
      safeAgents: {
        modelRoutePolicies: { "omlx/qwen3": { maxConcurrent: 1, effectivePrefillBudget: 48_000 } },
      },
    }), "utf8");
    const loaded = loadFabricConfig({ cwd, agentDir, env: {} });
    assert.equal(loaded.source, join(cwd, ".pi", "safe-agents.json"));
    assert.deepEqual(loaded.config.modelRoutePolicies, { "omlx/qwen3": { maxConcurrent: 1, effectivePrefillBudget: 48_000 } });

    const runtime = new FabricRuntime({ cwd, agentDir, startBroker: false });
    assert.deepEqual(runtime.config.modelRoutePolicies, loaded.config.modelRoutePolicies);
  } finally {
    await rm(cwd, { recursive: true, force: true });
    await rm(agentDir, { recursive: true, force: true });
  }
});

test("explicit PI_SAFE_AGENTS_CONFIG takes precedence over project files", async () => {
  const cwd = await mkdtemp(join(tmpdir(), "safe-agents-config-explicit-"));
  const agentDir = await mkdtemp(join(tmpdir(), "safe-agents-agent-explicit-"));
  const explicit = join(cwd, "explicit.json");
  try {
    await writeFile(join(cwd, ".safe-agents.json"), JSON.stringify({ maxConcurrentAgents: 2 }), "utf8");
    await writeFile(explicit, JSON.stringify({ maxConcurrentAgents: 7 }), "utf8");
    const loaded = loadFabricConfig({ cwd, agentDir, env: { PI_SAFE_AGENTS_CONFIG: explicit } });
    assert.equal(loaded.source, explicit);
    assert.equal(loaded.config.maxConcurrentAgents, 7);
  } finally {
    await rm(cwd, { recursive: true, force: true });
    await rm(agentDir, { recursive: true, force: true });
  }
});

test("malformed discovered configuration fails runtime startup instead of falling back", async () => {
  const cwd = await mkdtemp(join(tmpdir(), "safe-agents-config-malformed-"));
  const agentDir = await mkdtemp(join(tmpdir(), "safe-agents-agent-malformed-"));
  try {
    await mkdir(join(cwd, ".pi"), { recursive: true });
    await writeFile(join(cwd, ".pi", "safe-agents.json"), "{\"safeAgents\":", "utf8");
    const loaded = loadFabricConfig({ cwd, agentDir, env: {} });
    assert.equal(loaded.config.maxTotalAgents, undefined);
    assert.equal(loaded.errors.length, 1);
    assert.throws(() => new FabricRuntime({ cwd, agentDir, startBroker: false }), /configuration could not be loaded/);
  } finally {
    await rm(cwd, { recursive: true, force: true });
    await rm(agentDir, { recursive: true, force: true });
  }
});

test("an explicitly missing configuration path is reported", async () => {
  const cwd = await mkdtemp(join(tmpdir(), "safe-agents-config-missing-"));
  const agentDir = await mkdtemp(join(tmpdir(), "safe-agents-agent-missing-"));
  try {
    const explicit = join(cwd, "does-not-exist.json");
    const loaded = loadFabricConfig({ cwd, agentDir, env: { PI_SAFE_AGENTS_CONFIG: explicit } });
    assert.equal(loaded.source, explicit);
    assert.match(loaded.errors[0] ?? "", /explicitly requested but was not found/);
  } finally {
    await rm(cwd, { recursive: true, force: true });
    await rm(agentDir, { recursive: true, force: true });
  }
});
