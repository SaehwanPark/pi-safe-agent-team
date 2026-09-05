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
    const baseRef = request.baseRef ?? await git(root, ["rev-parse", "HEAD"]);
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

  async cleanup(info: WorkspaceInfo, force = false): Promise<void> {
    if (info.mode !== "worktree") return;
    const status = await git(info.path, ["status", "--porcelain"]);
    if (status && !force) throw new FabricError("WORKSPACE_FAILURE", "refusing to remove a dirty worktree without force", { path: info.path });
    await git(info.root, ["worktree", "remove", ...(force ? ["--force"] : []), info.path]);
    if (info.branch) await git(info.root, ["branch", "-D", info.branch]).catch(() => undefined);
  }
}

export class SharedWorkspaceStrategy extends GitWorkspaceStrategy {
  async create(request: WorkspaceRequest): Promise<WorkspaceInfo> {
    return super.create({ ...request, mode: "shared" });
  }
}
