import { execFile } from "node:child_process";
import { promises as fs } from "node:fs";
import { promisify } from "node:util";
import { join } from "node:path";
import { FabricError } from "./core/errors.ts";
import type { WorkspaceInfo } from "./core/types.ts";

const execFileAsync = promisify(execFile);

export interface WorkspaceRequest {
  mode?: "shared" | "worktree";
  cwd: string;
  stateDirectory: string;
  agentId: string;
  baseRef?: string;
}

export interface WorkspaceStrategy {
  create(request: WorkspaceRequest): Promise<WorkspaceInfo>;
  cleanup(info: WorkspaceInfo, force?: boolean): Promise<void>;
  /** Optional metadata probe used when useful external artifacts are retained. */
  describe?(info: WorkspaceInfo): Promise<{ headRef?: string }>;
}

function safeSegment(value: string): string {
  return value.replace(/[^a-zA-Z0-9._-]/g, "-").slice(0, 96);
}

async function git(cwd: string, args: string[]): Promise<string> {
  try {
    const result = await execFileAsync("git", args, { cwd, maxBuffer: 1024 * 1024 });
    return result.stdout.trim();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new FabricError("WORKSPACE_FAILURE", `git ${args.join(" ")} failed: ${message}`);
  }
}

export class GitWorkspaceStrategy implements WorkspaceStrategy {
  async create(request: WorkspaceRequest): Promise<WorkspaceInfo> {
    if ((request.mode ?? "shared") === "shared") {
      let root = request.cwd;
      try {
        root = await git(request.cwd, ["rev-parse", "--show-toplevel"]);
      } catch {
        // Shared mode is useful outside Git; worktree mode below remains Git-only.
      }
      return { mode: "shared", root, path: request.cwd };
    }

    const root = await git(request.cwd, ["rev-parse", "--show-toplevel"]);
    const status = await git(root, ["status", "--porcelain"]);
    if (status) throw new FabricError("WORKSPACE_FAILURE", "worktree mode requires a clean base checkout", { root });
    // Resolve the base once. Comparing against a moving branch name during
    // cleanup could make an unchanged child look divergent after the base
    // branch advances.
    const baseRef = await git(root, ["rev-parse", request.baseRef ?? "HEAD"]);
    const worktreeRoot = join(request.stateDirectory, "worktrees");
    await fs.mkdir(worktreeRoot, { recursive: true });
    const branch = `pi-safe/${safeSegment(request.agentId)}-${Date.now().toString(36)}`;
    const path = join(worktreeRoot, `${safeSegment(request.agentId)}-${Date.now().toString(36)}`);
    try {
      await git(root, ["worktree", "add", "-b", branch, path, baseRef]);
    } catch (error) {
      await fs.rm(path, { recursive: true, force: true }).catch(() => undefined);
      throw error;
    }
    return { mode: "worktree", root, path, baseRef, branch };
  }

  async describe(info: WorkspaceInfo): Promise<{ headRef?: string }> {
    if (info.mode !== "worktree") return {};
    try {
      return { headRef: await git(info.path, ["rev-parse", "HEAD"]) };
    } catch {
      return {};
    }
  }

  async cleanup(info: WorkspaceInfo, force = false): Promise<void> {
    if (info.mode !== "worktree") return;
    // A successful cleanup followed by a lost broker response is retried on
    // the next root attachment. Treat the already-removed path as idempotent
    // only when its named branch is gone too; a surviving branch is still a
    // user-visible recovery artifact and must not be certified as cleaned.
    try {
      await fs.access(info.path);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      if (info.branch && await git(info.root, ["branch", "--list", info.branch])) {
        throw new FabricError("WORKSPACE_FAILURE", "refusing to certify a missing worktree with a retained branch", { path: info.path, branch: info.branch });
      }
      return;
    }
    if (!force && !info.baseRef) {
      throw new FabricError("WORKSPACE_FAILURE", "refusing to remove a worktree without a recorded base commit", { path: info.path, branch: info.branch });
    }
    const status = await git(info.path, ["status", "--porcelain"]);
    if (status && !force) throw new FabricError("WORKSPACE_FAILURE", "refusing to remove a dirty worktree without force", { path: info.path });
    if (!force && info.baseRef) {
      const [head, base] = await Promise.all([
        git(info.path, ["rev-parse", "HEAD"]),
        git(info.root, ["rev-parse", info.baseRef]),
      ]);
      if (head !== base) {
        throw new FabricError("WORKSPACE_FAILURE", "refusing to remove a clean worktree with committed child work", {
          path: info.path,
          branch: info.branch,
          head,
          baseRef: base,
        });
      }
    }
    await git(info.root, ["worktree", "remove", ...(force ? ["--force"] : []), info.path]);
    if (info.branch && await git(info.root, ["branch", "--list", info.branch])) {
      await git(info.root, ["branch", "-D", info.branch]);
    }
  }
}

export class SharedWorkspaceStrategy extends GitWorkspaceStrategy {
  async create(request: WorkspaceRequest): Promise<WorkspaceInfo> {
    return super.create({ ...request, mode: "shared" });
  }
}
