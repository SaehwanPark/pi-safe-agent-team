import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { FabricRuntime } from "../src/pi/runtime.ts";

test("PI_CODING_AGENT_DIR contains all state and ~/.pi/agent remains untouched", async () => {
  const tmpBase = process.platform === "darwin" ? "/tmp" : tmpdir();
  const testDir = await fs.mkdtemp(join(tmpBase, "pi-ag-"));
  const workspaceDir = join(testDir, "workspace");
  await fs.mkdir(workspaceDir, { recursive: true });

  const realAgentDir = join(homedir(), ".pi", "agent");
  let realDirBefore: string[] = [];
  try {
    realDirBefore = await fs.readdir(realAgentDir);
  } catch {
    realDirBefore = [];
  }

  const previousEnv = process.env.PI_CODING_AGENT_DIR;
  const previousLegacyEnv = process.env.PI_AGENT_DIR;

  try {
    process.env.PI_CODING_AGENT_DIR = testDir;
    delete process.env.PI_AGENT_DIR;

    const runtime = new FabricRuntime({
      cwd: workspaceDir,
      startBroker: true,
    });

    assert.equal(runtime.agentDir, testDir);
    assert.ok(runtime.stateDirectory.startsWith(testDir));

    // Initialize broker state under testDir
    await (runtime as any).ensureBroker();

    // Verify broker files exist under testDir
    const filesUnderStateDir = await fs.readdir(runtime.stateDirectory);
    assert.ok(filesUnderStateDir.includes("broker.lock") || filesUnderStateDir.includes("events.jsonl"));

    await runtime.stop();

    // Verify ~/.pi/agent was not mutated
    let realDirAfter: string[] = [];
    try {
      realDirAfter = await fs.readdir(realAgentDir);
    } catch {
      realDirAfter = [];
    }
    assert.deepEqual(realDirAfter, realDirBefore);
    assert.ok(!runtime.stateDirectory.startsWith(realAgentDir));
  } finally {
    if (previousEnv !== undefined) process.env.PI_CODING_AGENT_DIR = previousEnv;
    else delete process.env.PI_CODING_AGENT_DIR;

    if (previousLegacyEnv !== undefined) process.env.PI_AGENT_DIR = previousLegacyEnv;
    else delete process.env.PI_AGENT_DIR;

    await fs.rm(testDir, { recursive: true, force: true });
  }
});

test("precedence: options.agentDir > PI_CODING_AGENT_DIR (getAgentDir)", async () => {
  const explicitDir = "/explicit/agent/dir";
  const codingEnvDir = "/coding/env/dir";

  const previousEnv = process.env.PI_CODING_AGENT_DIR;

  try {
    process.env.PI_CODING_AGENT_DIR = codingEnvDir;

    // 1. options.agentDir wins when provided
    const runtime1 = new FabricRuntime({ agentDir: explicitDir, startBroker: false });
    assert.equal(runtime1.agentDir, explicitDir);

    // 2. PI_CODING_AGENT_DIR via getAgentDir() is used when options.agentDir is omitted
    const runtime2 = new FabricRuntime({ startBroker: false });
    assert.equal(runtime2.agentDir, codingEnvDir);
  } finally {
    if (previousEnv !== undefined) process.env.PI_CODING_AGENT_DIR = previousEnv;
    else delete process.env.PI_CODING_AGENT_DIR;
  }
});
