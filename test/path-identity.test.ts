import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Coordinator } from "../src/core/coordinator.ts";
import { createGuardedChildTools, evaluateRootWriteGuard } from "../src/pi/guards.ts";
import type { AgentRecord, ModelRoute } from "../src/core/types.ts";

const route: ModelRoute = { provider: "test", model: "small", thinking: "medium" };

interface RecordedRequest {
  operation: string;
  args: Record<string, unknown>;
}

function recordingClient(decision: { allowed: boolean; reason?: string; resourceId?: string }, requests: RecordedRequest[]) {
  return {
    request: async <T>(operation: string, args: Record<string, unknown> = {}): Promise<T> => {
      requests.push({ operation, args });
      return decision as T;
    },
  };
}

/** Create a directory link. Windows uses junctions (no developer mode needed); other platforms use dir symlinks. */
async function linkDirectory(target: string, linkPath: string): Promise<boolean> {
  try {
    await symlink(target, linkPath, process.platform === "win32" ? "junction" : "dir");
    return true;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "EPERM" || code === "ENOSYS" || code === "EEXIST") return false;
    throw error;
  }
}

async function linkFile(target: string, linkPath: string): Promise<boolean> {
  if (process.platform === "win32") return false; // file symlinks need developer mode; junctions cannot cover file-level cases
  try {
    await symlink(target, linkPath, "file");
    return true;
  } catch {
    return false;
  }
}

test("the root guard coordinates aliased workspace writes under their real path", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-alias-root-"));
  try {
    await mkdir(join(directory, "inner"));
    await writeFile(join(directory, "inner", "target.txt"), "one\n", "utf8");
    if (!(await linkDirectory(join(directory, "inner"), join(directory, "alias")))) return; // links unavailable in this environment
    const requests: RecordedRequest[] = [];
    const client = recordingClient({ allowed: false, reason: "a live child hold", resourceId: "file:inner/target.txt" }, requests);

    const blocked = await evaluateRootWriteGuard({ client, workspacePath: directory }, "edit", { path: join("alias", "target.txt") });
    assert.equal(blocked?.block, true);
    assert.deepEqual(requests[0], { operation: "resource.begin_write", args: { path: "inner/target.txt", hostGuard: true } });

    // A not-yet-existing file under an aliased directory still keys on the real directory.
    requests.length = 0;
    await evaluateRootWriteGuard({ client, workspacePath: directory }, "write", { path: join("alias", "new.txt") });
    assert.equal(requests[0]?.args.path, "inner/new.txt");
  } finally {
    await rm(join(directory, "alias"), { recursive: true, force: true });
    await rm(directory, { recursive: true, force: true });
  }
});

test("root writes whose real identity escapes the workspace stay uncoordinated", async () => {
  const base = await mkdtemp(join(tmpdir(), "safe-agents-alias-escape-"));
  try {
    const workspace = join(base, "workspace");
    const outside = join(base, "outside");
    await mkdir(workspace);
    await mkdir(outside);
    await writeFile(join(outside, "x.txt"), "foreign\n", "utf8");
    if (!(await linkDirectory(outside, join(workspace, "outlink")))) return;
    const requests: RecordedRequest[] = [];
    const client = recordingClient({ allowed: true }, requests);

    assert.equal(await evaluateRootWriteGuard({ client, workspacePath: workspace }, "edit", { path: join("outlink", "x.txt") }), undefined);
    assert.equal(await evaluateRootWriteGuard({ client, workspacePath: workspace }, "write", { path: join("outlink", "missing.txt") }), undefined);
    assert.equal(await evaluateRootWriteGuard({ client, workspacePath: workspace }, "write", { path: "../outside.txt" }), undefined);
    assert.equal(requests.length, 0);
  } finally {
    await rm(join(base, "workspace", "outlink"), { recursive: true, force: true });
    await rm(base, { recursive: true, force: true });
  }
});

test("a guarded child cannot satisfy the declaration rule with an alias spelling", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-alias-child-"));
  try {
    await mkdir(join(directory, "inner"));
    const realFile = join(directory, "inner", "a.ts");
    await writeFile(realFile, "one\n", "utf8");
    if (!(await linkDirectory(join(directory, "inner"), join(directory, "alias")))) return;

    const coordinator = new Coordinator({ rootId: "fabric", config: { maxTotalAgents: 32, maxChildrenPerAgent: 16 }, clock: () => 1_000 });
    coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: { mayWriteRepo: true } });
    const child = coordinator.dispatch("writer", "agent.register", { rootId: "fabric", parentId: "root", route, capabilities: { mayWriteRepo: true } }).value.agent as AgentRecord;
    // The child only ever declared the alias spelling, which no longer matches the enforced identity.
    coordinator.dispatch(child.id, "resource.define", { resourceId: "file:alias/a.ts", kind: "file", path: "alias/a.ts" });
    coordinator.dispatch(child.id, "resource.borrow", { resourceId: "file:alias/a.ts", mode: "mutable" });
    assert.deepEqual(coordinator.dispatch(child.id, "resource.check_write", { path: "alias/a.ts" }).value, { allowed: true, resourceId: "file:alias/a.ts" });

    const tools = createGuardedChildTools({
      workspacePath: directory,
      mayWriteRepo: true,
      mayUseShell: false,
      client: { request: async <T>(operation: string, args: Record<string, unknown> = {}) => coordinator.dispatch(child.id, operation, args).value as T },
    });
    const edit = tools.find((tool) => tool.name === "edit");
    assert.ok(edit);
    await assert.rejects(
      () => edit.execute("edit-1", { path: join("alias", "a.ts"), edits: [{ oldText: "one", newText: "bypassed" }] }, undefined, undefined, { cwd: directory } as never),
      /inner\/a\.ts/,
    );

    // Declaring the real path makes the aliased write legitimate.
    coordinator.dispatch(child.id, "resource.define", { resourceId: "file:inner/a.ts", kind: "file", path: "inner/a.ts" });
    coordinator.dispatch(child.id, "resource.borrow", { resourceId: "file:inner/a.ts", mode: "mutable" });
    await edit.execute("edit-2", { path: join("alias", "a.ts"), edits: [{ oldText: "one", newText: "legit" }] }, undefined, undefined, { cwd: directory } as never);
    assert.equal(await readFile(realFile, "utf8"), "legit\n");
  } finally {
    await rm(join(directory, "alias"), { recursive: true, force: true });
    await rm(directory, { recursive: true, force: true });
  }
});

test("unresolvable real paths fail the root guard closed while loops are refused", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-alias-loop-"));
  try {
    if (!(await linkFile(join(directory, "loop-b"), join(directory, "loop-a")))) return; // loops only provable where file symlinks work
    await linkFile(join(directory, "loop-a"), join(directory, "loop-b"));
    const requests: RecordedRequest[] = [];
    const client = recordingClient({ allowed: true }, requests);
    const blocked = await evaluateRootWriteGuard({ client, workspacePath: directory }, "edit", { path: "loop-a" });
    assert.equal(blocked?.block, true);
    assert.match(blocked?.reason ?? "", /real path/i);
    assert.equal(requests.length, 0);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("NUL paths and workspace-root identities are rejected or left uncoordinated", async () => {
  const directory = await mkdtemp(join(tmpdir(), "safe-agents-identity-root-"));
  try {
    const requests: RecordedRequest[] = [];
    const client = recordingClient({ allowed: true }, requests);
    // The workspace root itself is not a file resource for the root guard.
    assert.equal(await evaluateRootWriteGuard({ client, workspacePath: directory }, "edit", { path: "." }), undefined);
    assert.equal(requests.length, 0);
    // A child-facing NUL path is refused before any broker request.
    const coordinator = new Coordinator({ rootId: "fabric", config: { maxTotalAgents: 32, maxChildrenPerAgent: 16 }, clock: () => 1_000 });
    coordinator.dispatch("root", "agent.register", { rootId: "fabric", route, capabilities: {} });
    const child = coordinator.dispatch("writer", "agent.register", { rootId: "fabric", parentId: "root", route, capabilities: { mayWriteRepo: true } }).value.agent as AgentRecord;
    const tools = createGuardedChildTools({
      workspacePath: directory,
      mayWriteRepo: true,
      mayUseShell: false,
      client: { request: async <T>(operation: string, args: Record<string, unknown> = {}) => coordinator.dispatch(child.id, operation, args).value as T },
    });
    const write = tools.find((tool) => tool.name === "write");
    assert.ok(write);
    await assert.rejects(() => write.execute("write-1", { path: "bad\u0000name.txt", content: "x" }, undefined, undefined, { cwd: directory } as never));
    assert.deepEqual(await readdir(directory), []);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
