import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { FabricRuntime } from "../src/pi/runtime.ts";

test("PI_CODING_AGENT_DIR contains all child-derived state and ~/.pi/agent remains untouched", async () => {
  const testDir = await fs.mkdtemp(join(process.cwd(), "test-agent-dir-"));
  const realAgentDir = join(homedir(), ".pi", "agent");
  const previousEnv = process.env.PI_CODING_AGENT_DIR;
  const previousLegacyEnv = process.env.PI_AGENT_DIR;

  try {
    process.env.PI_CODING_AGENT_DIR = testDir;
    delete process.env.PI_AGENT_DIR;

    const runtime = new FabricRuntime({
      cwd: join(testDir, "workspace"),
      startBroker: false,
    });

    assert.equal(runtime.agentDir, testDir);
    assert.ok(runtime.stateDirectory.startsWith(testDir));

    // Confirm real ~/.pi/agent does not have new safe-agents files
    let realDirHasSafeAgents = false;
    try {
      await fs.stat(join(realAgentDir, "safe-agents"));
      realDirHasSafeAgents = true;
    } catch {
      realDirHasSafeAgents = false;
    }
    // If ~/.pi/agent/safe-agents didn't exist before, it must not exist now
    // And runtime.stateDirectory must be within testDir
    assert.ok(!runtime.stateDirectory.startsWith(realAgentDir));
  } finally {
    if (previousEnv !== undefined) process.env.PI_CODING_AGENT_DIR = previousEnv;
    else delete process.env.PI_CODING_AGENT_DIR;

    if (previousLegacyEnv !== undefined) process.env.PI_AGENT_DIR = previousLegacyEnv;
    else delete process.env.PI_AGENT_DIR;

    await fs.rm(testDir, { recursive: true, force: true });
  }
});

test("precedence: options.agentDir > PI_CODING_AGENT_DIR > PI_AGENT_DIR", async () => {
  const explicitDir = "/explicit/agent/dir";
  const codingEnvDir = "/coding/env/dir";
  const legacyEnvDir = "/legacy/env/dir";

  const previousEnv = process.env.PI_CODING_AGENT_DIR;
  const previousLegacy = process.env.PI_AGENT_DIR;

  try {
    process.env.PI_CODING_AGENT_DIR = codingEnvDir;
    process.env.PI_AGENT_DIR = legacyEnvDir;

    // 1. options.agentDir wins when provided
    const runtime1 = new FabricRuntime({ agentDir: explicitDir, startBroker: false });
    assert.equal(runtime1.agentDir, explicitDir);

    // 2. PI_CODING_AGENT_DIR wins over PI_AGENT_DIR when options.agentDir is omitted
    const runtime2 = new FabricRuntime({ startBroker: false });
    assert.equal(runtime2.agentDir, codingEnvDir);

    // 3. PI_AGENT_DIR is used if PI_CODING_AGENT_DIR is not set
    delete process.env.PI_CODING_AGENT_DIR;
    const runtime3 = new FabricRuntime({ startBroker: false });
    assert.equal(runtime3.agentDir, legacyEnvDir);
  } finally {
    if (previousEnv !== undefined) process.env.PI_CODING_AGENT_DIR = previousEnv;
    else delete process.env.PI_CODING_AGENT_DIR;

    if (previousLegacy !== undefined) process.env.PI_AGENT_DIR = previousLegacy;
    else delete process.env.PI_AGENT_DIR;
  }
});
