export type RootShellRisk =
  | { kind: "read-only" }
  | { kind: "known-mutator"; scope: "broad" | "path"; paths?: string[]; reason: string }
  | { kind: "unknown" };

/**
 * Split a shell command into sub-commands chained by ;, &&, ||, or |
 * while respecting quotes and simple escapes.
 */
function splitChainedCommands(command: string): string[] {
  const parts: string[] = [];
  let current = "";
  let inSingleQuote = false;
  let inDoubleQuote = false;

  for (let i = 0; i < command.length; i++) {
    const char = command[i];
    if (char === "'" && !inDoubleQuote) {
      inSingleQuote = !inSingleQuote;
      current += char;
    } else if (char === '"' && !inSingleQuote) {
      inDoubleQuote = !inDoubleQuote;
      current += char;
    } else if (!inSingleQuote && !inDoubleQuote) {
      if (char === ";" || char === "\n") {
        if (current.trim()) parts.push(current.trim());
        current = "";
      } else if (char === "&" && command[i + 1] === "&") {
        if (current.trim()) parts.push(current.trim());
        current = "";
        i++;
      } else if (char === "|" && command[i + 1] === "|") {
        if (current.trim()) parts.push(current.trim());
        current = "";
        i++;
      } else if (char === "|") {
        if (current.trim()) parts.push(current.trim());
        current = "";
      } else {
        current += char;
      }
    } else {
      current += char;
    }
  }
  if (current.trim()) parts.push(current.trim());
  return parts;
}

/**
 * Check for mutating file redirection (> or >>) not directed to /dev/null or file descriptors.
 */
function extractRedirectionPaths(segment: string): { hasRedirection: boolean; paths: string[] } {
  // Regex to match redirection operators > or >> (including no-space like echo x>a.ts)
  // Excludes here-docs << and file descriptors &1, &2, and /dev/null
  const redirRegex = /(?<!<)(?:[0-9]*>>?)\s*("[^"]*"|'[^']*'|[^\s;&|<>]+)/g;
  let match: RegExpExecArray | null;
  const paths: string[] = [];
  let hasRedirection = false;

  while ((match = redirRegex.exec(segment)) !== null) {
    const target = match[1]?.trim();
    if (!target) continue;
    // Redirection to file descriptor like &1 or &2
    if (target.startsWith("&")) continue;
    // Redirection to /dev/null
    if (target === "/dev/null" || target.endsWith("/dev/null")) continue;

    hasRedirection = true;
    const cleanTarget = target.replace(/^['"]|['"]$/g, "");
    paths.push(cleanTarget);
  }

  return { hasRedirection, paths };
}

/**
 * Parse tokens of a single command (without chain operators).
 */
function tokenize(command: string): string[] {
  const tokens: string[] = [];
  let current = "";
  let inSingleQuote = false;
  let inDoubleQuote = false;

  for (let i = 0; i < command.length; i++) {
    const char = command[i];
    if (char === "'" && !inDoubleQuote) {
      inSingleQuote = !inSingleQuote;
    } else if (char === '"' && !inSingleQuote) {
      inDoubleQuote = !inDoubleQuote;
    } else if (/\s/.test(char) && !inSingleQuote && !inDoubleQuote) {
      if (current) {
        tokens.push(current);
        current = "";
      }
    } else {
      current += char;
    }
  }
  if (current) tokens.push(current);
  return tokens;
}

/**
 * Classify a single command invocation (without redirection).
 */
function classifySingleCommand(tokens: string[]): RootShellRisk {
  // Strip leading env var assignments: VAR=VAL
  let idx = 0;
  while (idx < tokens.length && /^[A-Za-z_][A-Za-z0-9_]*=/.test(tokens[idx])) {
    idx++;
  }
  if (idx >= tokens.length) return { kind: "read-only" };

  const rawBin = tokens[idx];
  const bin = rawBin.includes("/") ? rawBin.split("/").pop()! : rawBin;
  const args = tokens.slice(idx + 1);

  // Check known broad mutators
  if (bin === "cargo") {
    if (args[0] === "fmt") {
      return { kind: "known-mutator", scope: "broad", reason: "cargo fmt formats files in place" };
    }
    if (["read-manifest", "tree", "verify-project"].includes(args[0])) {
      return { kind: "read-only" };
    }
    // cargo test/check/build/bench execute arbitrary project build scripts/hooks -> unknown
    return { kind: "unknown" };
  }

  if (bin === "ruff") {
    if (args[0] === "format") {
      return { kind: "known-mutator", scope: "broad", reason: "ruff format formats files in place" };
    }
    if (args[0] === "check" && args.includes("--fix")) {
      return { kind: "known-mutator", scope: "broad", reason: "ruff check --fix modifies files in place" };
    }
    if (args[0] === "check") {
      return { kind: "read-only" };
    }
  }

  if (bin === "black") {
    if (args.includes("--check") || args.includes("--diff")) {
      return { kind: "read-only" };
    }
    return { kind: "known-mutator", scope: "broad", reason: "black formats files in place" };
  }

  if (bin === "prettier") {
    if (args.includes("--write") || args.includes("-w")) {
      const paths = args.filter((a) => !a.startsWith("-"));
      return {
        kind: "known-mutator",
        scope: paths.length > 0 ? "path" : "broad",
        paths: paths.length > 0 ? paths : undefined,
        reason: "prettier --write formats files in place",
      };
    }
    if (args.includes("--check") || args.includes("-c") || args.includes("--list-different") || args.includes("-l")) {
      return { kind: "read-only" };
    }
  }

  if (bin === "eslint") {
    if (args.includes("--fix")) {
      return { kind: "known-mutator", scope: "broad", reason: "eslint --fix modifies files in place" };
    }
    return { kind: "read-only" };
  }

  if (bin === "go") {
    if (args[0] === "fmt") {
      return { kind: "known-mutator", scope: "broad", reason: "go fmt formats files in place" };
    }
    if (["version", "env", "list"].includes(args[0])) {
      return { kind: "read-only" };
    }
    // go test/vet execute project code -> unknown
    return { kind: "unknown" };
  }

  if (bin === "gofmt") {
    if (args.includes("-w")) {
      return { kind: "known-mutator", scope: "broad", reason: "gofmt -w formats files in place" };
    }
    return { kind: "read-only" };
  }

  if (bin === "dotnet") {
    if (args[0] === "format") {
      return { kind: "known-mutator", scope: "broad", reason: "dotnet format formats files in place" };
    }
    if (["--version", "--info"].includes(args[0])) {
      return { kind: "read-only" };
    }
    // dotnet test/build/run execute project code -> unknown
    return { kind: "unknown" };
  }

  if (bin === "mix") {
    if (args[0] === "format") {
      return { kind: "known-mutator", scope: "broad", reason: "mix format formats files in place" };
    }
    if (args.includes("--version")) {
      return { kind: "read-only" };
    }
    return { kind: "unknown" };
  }

  if (bin === "git") {
    const sub = args[0];
    if (["checkout", "restore", "reset", "clean", "switch", "merge", "rebase", "pull", "cherry-pick", "stash"].includes(sub)) {
      return { kind: "known-mutator", scope: "broad", reason: `git ${sub} mutates working tree files` };
    }
    if (["diff", "status", "log", "show", "branch", "tag", "rev-parse", "ls-files", "grep", "cat-file"].includes(sub)) {
      return { kind: "read-only" };
    }
  }

  if (bin === "sed") {
    if (args.includes("-i") || args.some((a) => a.startsWith("-i"))) {
      const paths = args.filter((a) => !a.startsWith("-") && !a.startsWith("'") && !a.startsWith('"') && !a.includes("/"));
      return { kind: "known-mutator", scope: "broad", paths: paths.length ? paths : undefined, reason: "sed -i edits files in place" };
    }
    return { kind: "read-only" };
  }

  if (bin === "perl") {
    if (args.includes("-pi") || args.includes("-i") || args.some((a) => a.startsWith("-pi") || a.startsWith("-i"))) {
      return { kind: "known-mutator", scope: "broad", reason: "perl in-place edit mutates files" };
    }
  }

  if (bin === "tee") {
    const paths = args.filter((a) => !a.startsWith("-"));
    return {
      kind: "known-mutator",
      scope: paths.length > 0 ? "path" : "broad",
      paths: paths.length > 0 ? paths : undefined,
      reason: "tee writes to files",
    };
  }

  if (["rm", "unlink", "mv", "cp", "mkdir", "rmdir", "touch", "chmod", "chown"].includes(bin)) {
    const paths = args.filter((a) => !a.startsWith("-"));
    return {
      kind: "known-mutator",
      scope: paths.length > 0 ? "path" : "broad",
      paths: paths.length > 0 ? paths : undefined,
      reason: `${bin} mutates filesystem entries`,
    };
  }

  if (bin === "npm" || bin === "pnpm" || bin === "yarn") {
    if (args.includes("format") || args.includes("lint:fix") || args.includes("format:fix")) {
      return { kind: "known-mutator", scope: "broad", reason: `${bin} format script mutates files` };
    }
    // npm test/run/etc execute arbitrary scripts -> unknown
    return { kind: "unknown" };
  }

  // Intrinsically observational commands (no code execution)
  if ([
    "rg",
    "grep",
    "egrep",
    "fgrep",
    "ls",
    "dir",
    "cat",
    "head",
    "tail",
    "less",
    "more",
    "pwd",
    "whoami",
    "which",
    "where",
    "type",
    "echo",
    "printf",
    "stat",
    "file",
    "wc",
    "diff",
    "cmp",
    "true",
    "false",
    "test",
    "uname",
  ].includes(bin)) {
    return { kind: "read-only" };
  }

  if (bin === "find") {
    if (args.includes("-delete") || args.includes("-exec") || args.includes("-execdir")) {
      return { kind: "known-mutator", scope: "broad", reason: "find with mutating flags modifies filesystem" };
    }
    return { kind: "read-only" };
  }

  return { kind: "unknown" };
}

/**
 * Classify a full root shell command string.
 */
export function classifyRootShellCommand(command: string): RootShellRisk {
  const trimmed = command.trim();
  if (!trimmed) return { kind: "read-only" };

  const segments = splitChainedCommands(trimmed);
  let hasUnknown = false;
  const allMutators: Array<{ scope: "broad" | "path"; paths?: string[]; reason: string }> = [];

  for (const segment of segments) {
    const { hasRedirection, paths: redirPaths } = extractRedirectionPaths(segment);
    if (hasRedirection) {
      allMutators.push({
        scope: redirPaths.length > 0 ? "path" : "broad",
        paths: redirPaths.length > 0 ? redirPaths : undefined,
        reason: "shell output redirection mutates target file",
      });
    }

    // Strip redirection tokens from segment for command classification
    const cleanedSegment = segment.replace(/(?<!<)(?:[0-9]*>>?)\s*(?:"[^"]*"|'[^']*'|[^\s;&|<>]+)/g, " ").trim();
    if (!cleanedSegment) continue;

    const tokens = tokenize(cleanedSegment);
    if (tokens.length === 0) continue;

    const risk = classifySingleCommand(tokens);
    if (risk.kind === "known-mutator") {
      allMutators.push({
        scope: risk.scope,
        paths: risk.paths,
        reason: risk.reason,
      });
    } else if (risk.kind === "unknown") {
      hasUnknown = true;
    }
  }

  if (allMutators.length > 0) {
    // If the chained command changes directories (e.g. cd or pushd), relative path targets cannot be reliably scoped
    const changesDir = segments.some((s) => /\b(?:cd|pushd)\b/.test(s));
    if (changesDir) {
      for (const m of allMutators) {
        m.scope = "broad";
      }
    }
    // If any mutator has broad scope, the whole command is broad
    const isBroad = allMutators.some((m) => m.scope === "broad");
    const allPaths = allMutators.flatMap((m) => m.paths ?? []);
    return {
      kind: "known-mutator",
      scope: isBroad ? "broad" : "path",
      paths: allPaths.length > 0 ? allPaths : undefined,
      reason: allMutators.map((m) => m.reason).join("; "),
    };
  }

  if (hasUnknown) {
    return { kind: "unknown" };
  }

  return { kind: "read-only" };
}
