import { constants as fsConstants } from "node:fs";
import { access as fsAccess, lstat as fsLstat, mkdir as fsMkdir, open as fsOpen, readFile as fsReadFile, realpath as fsRealpath, readdir as fsReaddir, stat as fsStat } from "node:fs/promises";
import { delimiter, dirname, isAbsolute, relative, resolve, sep } from "node:path";
import {
  createBashToolDefinition,
  createEditToolDefinition,
  createFindToolDefinition,
  createGrepToolDefinition,
  createLsToolDefinition,
  createReadToolDefinition,
  createLocalBashOperations,
  createWriteToolDefinition,
  type BashOperations,
  type EditOperations,
  type ToolDefinition,
  type WriteOperations,
} from "@earendil-works/pi-coding-agent";
import { FabricError } from "../core/errors.ts";

export interface WriteAuthorizationClient {
  request<T = unknown>(operation: string, args?: Record<string, unknown>): Promise<T>;
}

export type ChildShellMode = "read-only" | "workspace";

export interface GuardedChildToolOptions {
  client: WriteAuthorizationClient;
  workspacePath: string;
  mayWriteRepo: boolean;
  mayUseShell: boolean;
  /** Worktree shells are explicitly trusted to mutate their isolated workspace. */
  shellMode?: ChildShellMode;
}

interface WriteDecision {
  allowed: boolean;
  reason?: string;
  resourceId?: string;
}

/**
 * Build the Pi built-ins for a managed child. File mutation is checked in the
 * operation that performs the final filesystem write, rather than only in a
 * prompt hook, so a tool cannot bypass the coordinator by changing its input
 * after authorization.
 */
type AnyToolDefinition = ToolDefinition<any, any, any>;

export function createGuardedReadOnlyTools(workspacePath: string): AnyToolDefinition[] {
  const read = createReadToolDefinition(workspacePath, {
    operations: {
      access: async (absolutePath) => {
        await assertFilesystemTargetWithinWorkspace(workspacePath, absolutePath);
        await fsAccess(absolutePath);
      },
      readFile: async (absolutePath) => {
        await assertFilesystemTargetWithinWorkspace(workspacePath, absolutePath);
        return fsReadFile(absolutePath);
      },
    },
  });
  const grep = createGrepToolDefinition(workspacePath, {
    operations: {
      isDirectory: async (absolutePath) => {
        await assertFilesystemTargetWithinWorkspace(workspacePath, absolutePath);
        return (await fsStat(absolutePath)).isDirectory();
      },
      readFile: async (absolutePath) => {
        await assertFilesystemTargetWithinWorkspace(workspacePath, absolutePath);
        return fsReadFile(absolutePath, "utf8");
      },
    },
  });
  const find = scopeReadOnlyTool(createFindToolDefinition(workspacePath), workspacePath);
  const ls = createLsToolDefinition(workspacePath, {
    operations: {
      exists: async (absolutePath) => {
        await assertFilesystemTargetWithinWorkspace(workspacePath, absolutePath);
        try {
          await fsStat(absolutePath);
          return true;
        } catch {
          return false;
        }
      },
      stat: async (absolutePath) => {
        await assertFilesystemTargetWithinWorkspace(workspacePath, absolutePath);
        return fsStat(absolutePath);
      },
      readdir: async (absolutePath) => {
        await assertFilesystemTargetWithinWorkspace(workspacePath, absolutePath);
        return fsReaddir(absolutePath);
      },
    },
  });
  return [scopeReadOnlyTool(read, workspacePath), scopeReadOnlyTool(grep, workspacePath), find, scopeReadOnlyTool(ls, workspacePath)];
}

export function createGuardedChildTools(options: GuardedChildToolOptions): AnyToolDefinition[] {
  const tools: AnyToolDefinition[] = [];
  if (options.mayUseShell === true) {
    const shellOperations = options.shellMode === "workspace"
      ? createLocalBashOperations()
      : createReadOnlyShellOperations(options.workspacePath);
    tools.push(createBashToolDefinition(options.workspacePath, { operations: shellOperations }));
  }
  if (options.mayWriteRepo === true) {
    const authorize = (absolutePath: string): Promise<void> => authorizeWrite(options.client, options.workspacePath, absolutePath);
    const editOperations: EditOperations = {
      access: async (absolutePath) => {
        await assertFilesystemTargetWithinWorkspace(options.workspacePath, absolutePath);
        return fsAccess(absolutePath);
      },
      readFile: async (absolutePath) => {
        await assertFilesystemTargetWithinWorkspace(options.workspacePath, absolutePath);
        return fsReadFile(absolutePath);
      },
      writeFile: async (absolutePath, content) => {
        await authorize(absolutePath);
        await writeFileWithoutFollowingSymlink(absolutePath, content);
      },
    };
    const writeOperations: WriteOperations = {
      // The write tool asks for mkdir before writeFile. Defer directory
      // creation until after the target file has passed the resource check.
      mkdir: async () => undefined,
      writeFile: async (absolutePath, content) => {
        await authorize(absolutePath);
        await ensureDirectoryWithoutFollowingSymlinks(options.workspacePath, dirname(absolutePath));
        await writeFileWithoutFollowingSymlink(absolutePath, content);
      },
    };
    tools.push(createEditToolDefinition(options.workspacePath, { operations: editOperations }));
    tools.push(createWriteToolDefinition(options.workspacePath, { operations: writeOperations }));
  }
  return tools;
}

function scopeReadOnlyTool(tool: AnyToolDefinition, workspacePath: string): AnyToolDefinition {
  const execute = tool.execute;
  tool.execute = async (toolCallId, params, signal, onUpdate, ctx) => {
    const requestedPath = (params as { path?: string }).path ?? ".";
    await assertFilesystemTargetWithinWorkspace(workspacePath, resolve(ctx?.cwd || workspacePath, requestedPath));
    return execute(toolCallId, params, signal, onUpdate, ctx);
  };
  return tool;
}

async function writeFileWithoutFollowingSymlink(absolutePath: string, content: string): Promise<void> {
  try {
    if ((await fsLstat(absolutePath)).isSymbolicLink()) {
      throw new FabricError("CAPABILITY_DENIED", `Refusing to write through symlink ${absolutePath}`);
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const noFollow = (fsConstants as { O_NOFOLLOW?: number }).O_NOFOLLOW ?? 0;
  const flags = fsConstants.O_WRONLY | fsConstants.O_CREAT | fsConstants.O_TRUNC | noFollow;
  const handle = await fsOpen(absolutePath, flags, 0o666);
  try {
    await handle.writeFile(content, "utf8");
  } finally {
    await handle.close();
  }
}

async function ensureDirectoryWithoutFollowingSymlinks(workspacePath: string, directory: string): Promise<void> {
  await assertFilesystemTargetWithinWorkspace(workspacePath, directory);
  const root = resolve(workspacePath);
  const target = resolve(root, directory);
  const candidates: string[] = [];
  let current = target;
  while (true) {
    candidates.push(current);
    const parent = dirname(current);
    if (parent === current || current === root) break;
    current = parent;
  }
  candidates.reverse();
  for (const candidate of candidates) {
    try {
      const stat = await fsLstat(candidate);
      if (stat.isSymbolicLink() && candidate !== root) throw new FabricError("CAPABILITY_DENIED", `Refusing to create through symlink ${candidate}`);
      if (!stat.isDirectory()) throw new FabricError("CAPABILITY_DENIED", `Write parent ${candidate} is not a directory`);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      try {
        await fsMkdir(candidate);
      } catch (mkdirError) {
        if ((mkdirError as NodeJS.ErrnoException).code !== "EEXIST") throw mkdirError;
      }
      const stat = await fsLstat(candidate);
      if ((stat.isSymbolicLink() && candidate !== root) || !stat.isDirectory()) throw new FabricError("CAPABILITY_DENIED", `Write parent ${candidate} is not a safe directory`);
    }
    await assertFilesystemTargetWithinWorkspace(workspacePath, candidate);
  }
}

async function authorizeWrite(client: WriteAuthorizationClient, workspacePath: string, absolutePath: string): Promise<void> {
  const path = workspaceRelativePath(workspacePath, absolutePath);
  await assertFilesystemTargetWithinWorkspace(workspacePath, absolutePath);
  const decision = await client.request<WriteDecision>("resource.check_write", { path });
  if (decision?.allowed !== true) {
    throw new FabricError("CAPABILITY_DENIED", typeof decision?.reason === "string" ? decision.reason : `No mutable resource hold authorizes ${path}`, {
      path,
      resourceId: typeof decision?.resourceId === "string" ? decision.resourceId : undefined,
    });
  }
}

async function assertFilesystemTargetWithinWorkspace(workspacePath: string, targetPath: string): Promise<void> {
  if (targetPath.includes("\u0000")) throw new FabricError("CAPABILITY_DENIED", "Filesystem paths must not contain NUL characters");
  const root = resolve(workspacePath);
  const target = resolve(root, targetPath);
  const realRoot = await fsRealpath(root).catch((error: unknown) => {
    throw new FabricError("CAPABILITY_DENIED", `Managed workspace is not accessible: ${error instanceof Error ? error.message : String(error)}`, { workspacePath });
  });
  let probe = target;
  while (true) {
    try {
      const realProbe = await fsRealpath(probe);
      const relativeProbe = relative(realRoot, realProbe);
      if (relativeProbe === ".." || relativeProbe.startsWith(`..${sep}`) || relativeProbe.startsWith(sep)) {
        throw new FabricError("CAPABILITY_DENIED", `Target ${targetPath} escapes the managed workspace`, { workspacePath, targetPath });
      }
      return;
    } catch (error) {
      if (error instanceof FabricError) throw error;
      const parent = dirname(probe);
      if (parent === probe) {
        throw new FabricError("CAPABILITY_DENIED", `Target ${targetPath} has no accessible workspace ancestor`, { workspacePath, targetPath });
      }
      probe = parent;
    }
  }
}

export function workspaceRelativePath(workspacePath: string, targetPath: string): string {
  if (targetPath.includes("\u0000")) throw new FabricError("CAPABILITY_DENIED", "Filesystem paths must not contain NUL characters");
  const root = resolve(workspacePath);
  const target = resolve(root, targetPath);
  const relativePath = relative(root, target);
  if (!relativePath || relativePath === ".." || relativePath.startsWith(`..${sep}`) || relativePath.startsWith(sep) || isAbsolute(relativePath)) {
    throw new FabricError("CAPABILITY_DENIED", `Target ${targetPath} is outside the managed workspace`, { workspacePath, targetPath });
  }
  return relativePath.split(sep).join("/");
}

function createReadOnlyShellOperations(workspacePath: string): BashOperations {
  const local = createLocalBashOperations();
  return {
    exec: async (command, cwd, options) => {
      await assertFilesystemTargetWithinWorkspace(workspacePath, cwd);
      assertReadOnlyShellCommand(command);
      await assertShellArgumentsWithinWorkspace(workspacePath, cwd, command);
      return local.exec(hardenGitCommand(command), cwd, {
        ...options,
        env: safeShellEnvironment(options.env, workspacePath),
      });
    },
  };
}

/**
 * Shell syntax is intentionally conservative. Shared-workspace children get a
 * small read-only inspection surface; worktree children may opt into the
 * explicitly trusted workspace shell above. This is not a shell sandbox.
 */
export function assertReadOnlyShellCommand(command: string): void {
  const trimmed = command.trim();
  if (!trimmed) throw new FabricError("CAPABILITY_DENIED", "Empty shell commands are not allowed");
  if (/[\u0000\r\n;&|<>`$%!]/.test(trimmed)) {
    throw new FabricError("CAPABILITY_DENIED", "Shared-workspace shell accepts one read-only command without shell operators");
  }
  if (hasUnquotedShellExpansion(trimmed)) {
    throw new FabricError("CAPABILITY_DENIED", "Shared-workspace shell does not allow shell glob or brace expansion");
  }
  const tokens = trimmed.match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g) ?? [];
  if (tokens.length === 0) throw new FabricError("CAPABILITY_DENIED", "Invalid shell command");
  const executableToken = stripQuotes(tokens[0]!);
  if (/[\\/]/.test(executableToken) || /^[A-Za-z]:/.test(executableToken)) {
    throw new FabricError("CAPABILITY_DENIED", "Shared-workspace shell requires an allowlisted executable name");
  }
  const executable = executableToken.toLowerCase().replace(/\.exe$/, "");
  const readOnlyExecutables = new Set([
    "cat", "cut", "diff", "du", "file", "find", "grep", "head", "ls", "pwd", "rg", "sort", "stat", "tail", "type", "uniq", "wc", "where", "which",
  ]);
  if (tokens.slice(1).some((token) => shellTokenEscapesWorkspace(stripQuotes(token)))) {
    throw new FabricError("CAPABILITY_DENIED", "Shared-workspace shell arguments must remain workspace-relative");
  }
  if (executable === "git") {
    const subcommandIndex = tokens.findIndex((token, index) => index > 0 && !token.startsWith("-"));
    const subcommand = subcommandIndex >= 0 ? stripQuotes(tokens[subcommandIndex]!) : undefined;
    if (!subcommand || !new Set(["branch", "describe", "diff", "log", "ls-files", "remote", "rev-parse", "show", "status"]).has(subcommand)) {
      throw new FabricError("CAPABILITY_DENIED", "Shared-workspace shell permits only read-only git subcommands");
    }
    const gitArgs = tokens.slice(subcommandIndex + 1).map((token) => stripQuotes(token));
    if (subcommand === "branch" && gitArgs.some((argument) => !argument.startsWith("-"))) {
      throw new FabricError("CAPABILITY_DENIED", "Shared-workspace git branch only permits listing options");
    }
    if (subcommand === "remote" && gitArgs.some((argument) => !["-v", "--verbose"].includes(argument))) {
      throw new FabricError("CAPABILITY_DENIED", "Shared-workspace git remote only permits listing remotes");
    }
    if (tokens.some((token) => ["-C", "-c", "--config-env", "--config-system", "--config-global", "--exec-path", "--git-dir", "--work-tree", "-D", "-d", "-M", "-m", "--delete", "--move", "--output", "--output=", "--edit-description", "--set-upstream-to", "--unset-upstream", "--track", "--create-reflog", "--exec", "--ext-diff", "--textconv", "--show-signature", "--upload-pack", "--receive-pack", "--help", "-h", "remove", "add", "set-url"].some((flag) => stripQuotes(token) === flag || stripQuotes(token).startsWith(flag)))) {
      throw new FabricError("CAPABILITY_DENIED", "This git option can mutate the workspace or execute configured commands");
    }
    return;
  }
  if ((executable === "find" || executable === "grep" || executable === "ls" || executable === "stat" || executable === "du" || executable === "file") && tokens.some((token) => {
    const option = stripQuotes(token);
    const followsSymlink = executable === "grep"
      ? /^-[^-]*R/.test(option)
      : executable === "find"
        ? /^-[^-]*L/.test(option) || option === "-H"
        : executable === "ls"
          ? /^-[^-]*L/.test(option) || option === "-H"
          : executable === "stat" || executable === "file"
            ? /^-[^-]*L/.test(option)
            : option === "-D";
    return followsSymlink || option === "--dereference" || option === "--dereference-args" || option === "--dereference-recursive" || option === "--dereference-command-line" || option === "--dereference-command-line-symlink-to-dir";
  })) {
    throw new FabricError("CAPABILITY_DENIED", "Shared-workspace inspection cannot follow symbolic links");
  }
  if (executable === "find" && tokens.some((token) => {
    const option = stripQuotes(token);
    return ["-delete", "-exec", "-execdir", "-ok", "-okdir"].includes(option) || option.startsWith("-fdelete") || option.startsWith("-fprint") || option.startsWith("-fls");
  })) {
    throw new FabricError("CAPABILITY_DENIED", "Mutating find actions are not allowed in a shared workspace");
  }
  if (executable === "sort" && tokens.some((token) => {
    const option = stripQuotes(token);
    return option === "-T" || option.startsWith("-T") || option === "--temporary-directory" || option.startsWith("--temporary-directory=");
  })) {
    throw new FabricError("CAPABILITY_DENIED", "sort temporary directories are not allowed in a shared workspace");
  }
  if (executable === "sort" && tokens.some((token) => {
    const option = stripQuotes(token);
    return ["-o", "--output", "--compress-program"].includes(option) || option.startsWith("--output=") || option.startsWith("--compress-program=");
  })) {
    throw new FabricError("CAPABILITY_DENIED", "sort output files are not allowed in a shared workspace");
  }
  if (executable === "rg" && tokens.some((token) => {
    const option = stripQuotes(token);
    return option === "-L" || /^-[^-]*L/.test(option) || option === "--follow" || option === "--pre" || option.startsWith("--pre=") || option === "--hostname-bin" || option.startsWith("--hostname-bin=");
  })) {
    throw new FabricError("CAPABILITY_DENIED", "ripgrep preprocessors and command hooks are not allowed in a shared workspace");
  }
  if ((executable === "diff" || executable === "git") && tokens.some((token) => {
    const option = stripQuotes(token);
    return option === "-o" || option === "--output" || option.startsWith("--output=");
  })) {
    throw new FabricError("CAPABILITY_DENIED", "Commands that write an output file are not allowed in a shared workspace");
  }
  if (!readOnlyExecutables.has(executable)) {
    throw new FabricError("CAPABILITY_DENIED", `Shell command ${executable} is not in the read-only allowlist`);
  }
}

function hasUnquotedShellExpansion(command: string): boolean {
  let quote: "'" | '"' | undefined;
  let escaped = false;
  for (const character of command) {
    if (escaped) {
      escaped = false;
      continue;
    }
    if (character === "\\" && quote !== "'") {
      escaped = true;
      continue;
    }
    if (quote) {
      if (character === quote) quote = undefined;
      continue;
    }
    if (character === "'" || character === '"') {
      quote = character;
      continue;
    }
    if ("*?[]{}".includes(character)) return true;
  }
  return false;
}

async function assertShellArgumentsWithinWorkspace(workspacePath: string, cwd: string, command: string): Promise<void> {
  const tokens = command.trim().match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g) ?? [];
  let afterDoubleDash = false;
  for (const token of tokens.slice(1)) {
    const value = stripQuotes(token);
    if (value === "--") {
      afterDoubleDash = true;
      continue;
    }
    const candidate = value.includes("=") ? value.slice(value.indexOf("=") + 1) : value;
    if (!candidate || (!afterDoubleDash && candidate.startsWith("-"))) continue;
    await assertFilesystemTargetWithinWorkspace(workspacePath, resolve(cwd, candidate));
  }
}

// Repository-local Git config may define aliases, pagers, fsmonitor hooks, or diff helpers;
// force the read-only invocation to use the built-in command without those hooks.
function hardenGitCommand(command: string): string {
  const trimmed = command.trim();
  const tokens = trimmed.match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g) ?? [];
  if (tokens.length < 2 || stripQuotes(tokens[0]!).toLowerCase() !== "git") return command;
  const subcommandIndex = tokens.findIndex((token, index) => index > 0 && !token.startsWith("-"));
  if (subcommandIndex < 0) return command;
  const rawSubcommand = tokens[subcommandIndex]!;
  const subcommand = stripQuotes(rawSubcommand);
  const firstSpace = trimmed.search(/\s/);
  if (firstSpace < 0) return command;
  const rest = trimmed.slice(firstSpace + 1).trimStart();
  const subcommandPosition = rest.indexOf(rawSubcommand);
  if (subcommandPosition < 0) return command;
  const beforeSubcommand = rest.slice(0, subcommandPosition + rawSubcommand.length);
  const afterSubcommand = rest.slice(subcommandPosition + rawSubcommand.length).trimStart();
  const safeOptions = [`-c alias.${subcommand}=`, "-c core.fsmonitor=false"];
  const commandOptions = ["diff", "log", "show"].includes(subcommand) ? " --no-ext-diff --no-textconv" : "";
  return `git ${safeOptions.join(" ")} ${beforeSubcommand}${commandOptions}${afterSubcommand ? ` ${afterSubcommand}` : ""}`;
}

// Non-interactive Bash honors BASH_ENV and imports exported functions. Remove those
// ambient hooks, along with Git redirection variables, before starting the inspection shell.
function safeShellEnvironment(environment: NodeJS.ProcessEnv | undefined, workspacePath: string): NodeJS.ProcessEnv {
  const env = { ...(environment ?? {}) };
  // A workspace-local or relative PATH entry could shadow an allowlisted name
  // with a model-created executable; retain only absolute external entries.
  const pathKey = Object.keys(env).find((key) => key.toUpperCase() === "PATH");
  if (pathKey) {
    const root = resolve(workspacePath);
    env[pathKey] = (env[pathKey] ?? "").split(delimiter).filter((entry) => {
      if (!entry || !isAbsolute(entry)) return false;
      const resolvedEntry = resolve(entry);
      const relativeEntry = relative(root, resolvedEntry);
      const insideWorkspace = relativeEntry === "" || (relativeEntry !== ".." && !relativeEntry.startsWith(`..${sep}`) && !isAbsolute(relativeEntry));
      return !insideWorkspace;
    }).join(delimiter);
  }
  for (const key of Object.keys(env)) {
    const upper = key.toUpperCase();
    if (upper.startsWith("BASH_FUNC_") || upper === "BASH_ENV" || upper === "ENV" || upper.startsWith("GIT_")) delete env[key];
  }
  env.BASH_ENV = "/dev/null";
  env.ENV = "/dev/null";
  env.CDPATH = "";
  env.GIT_CONFIG_NOSYSTEM = "1";
  env.GIT_CONFIG_GLOBAL = "/dev/null";
  env.GIT_OPTIONAL_LOCKS = "0";
  env.GIT_EXTERNAL_DIFF = "";
  env.GIT_PAGER = "cat";
  env.GIT_TERMINAL_PROMPT = "0";
  env.PAGER = "cat";
  return env;
}

function shellTokenEscapesWorkspace(token: string): boolean {
  const value = token.trim();
  const argumentValue = value.includes("=") ? value.slice(value.indexOf("=") + 1) : value;
  const attachedOptionValue = value.match(/^-+[A-Za-z]+(.+)$/)?.[1];
  return [argumentValue, attachedOptionValue].filter((candidate): candidate is string => Boolean(candidate)).some((candidate) => /^[A-Za-z]:/.test(candidate)
    || candidate.startsWith("/")
    || candidate.startsWith("\\\\")
    || candidate.startsWith("~")
    || /(?:^|[\\/])\.\.(?:[\\/]|$)/.test(candidate));
}

function stripQuotes(value: string): string {
  return value.replace(/^['"]|['"]$/g, "");
}
