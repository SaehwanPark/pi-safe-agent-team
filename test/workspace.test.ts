import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { GitWorkspaceStrategy } from "../src/workspace.ts";

const exec = promisify(execFile);

test("shared workspaces work outside Git and worktrees require a clean committed base", async () => {
  const nonGit = await mkdtemp(join(tmpdir(), "safe-agents-nongit-"));
  const repo = await mkdtemp(join(tmpdir(), "safe-agents-git-"));
  try {
    const strategy = new GitWorkspaceStrategy();
    const shared = await strategy.create({ mode: "shared", cwd: nonGit, stateDirectory: nonGit, agentId: "root" });
    assert.equal(shared.mode, "shared");
    assert.equal(shared.path, nonGit);

    await exec("git", ["init", "-q"], { cwd: repo });
    await exec("git", ["config", "user.email", "test@example.invalid"], { cwd: repo });
    await exec("git", ["config", "user.name", "Test"], { cwd: repo });
    await writeFile(join(repo, "README"), "clean\n");
    await exec("git", ["add", "README"], { cwd: repo });
    await exec("git", ["commit", "-qm", "initial"], { cwd: repo });
    const worktree = await strategy.create({ mode: "worktree", cwd: repo, stateDirectory: repo, agentId: "child" });
    assert.equal(worktree.mode, "worktree");
    assert.ok(worktree.branch);
    assert.notEqual(worktree.path, repo);
    await strategy.cleanup(worktree);

    await writeFile(join(repo, "dirty"), "not committed\n");
    await assert.rejects(() => strategy.create({ mode: "worktree", cwd: repo, stateDirectory: repo, agentId: "dirty" }), /clean base checkout/);
  } finally {
    await rm(nonGit, { recursive: true, force: true });
    await rm(repo, { recursive: true, force: true });
  }
});
