import test from "node:test";
import assert from "node:assert/strict";
import { classifyRootShellCommand } from "../src/pi/shell-classifier.ts";

test("shell-classifier: Section 16 required test matrix", () => {
  const matrix: Array<[string, "read-only" | "known-mutator" | "unknown", ("broad" | "path")?]> = [
    ["cargo test", "unknown"],
    ["git diff", "read-only"],
    ["git status", "read-only"],
    ["rg foo src", "read-only"],
    ["cargo fmt", "known-mutator", "broad"],
    ["ruff format .", "known-mutator", "broad"],
    ["prettier --write src", "known-mutator"],
    ["git restore .", "known-mutator", "broad"],
    ["git reset --hard", "known-mutator", "broad"],
    ["sed -i 's/a/b/g' file.txt", "known-mutator"],
    ["echo x > src/a.ts", "known-mutator", "path"],
    ["unknown-tool --foo", "unknown"],
  ];

  for (const [cmd, expectedKind, expectedScope] of matrix) {
    const risk = classifyRootShellCommand(cmd);
    assert.equal(risk.kind, expectedKind, `Failed kind check for '${cmd}'`);
    if (expectedScope && risk.kind === "known-mutator") {
      assert.equal(risk.scope, expectedScope, `Failed scope check for '${cmd}'`);
    }
  }
});

test("shell-classifier: additional known mutators and read-only tools", () => {
  assert.equal(classifyRootShellCommand("black .").kind, "known-mutator");
  assert.equal(classifyRootShellCommand("eslint --fix src").kind, "known-mutator");
  assert.equal(classifyRootShellCommand("go fmt ./...").kind, "known-mutator");
  assert.equal(classifyRootShellCommand("gofmt -w main.go").kind, "known-mutator");
  assert.equal(classifyRootShellCommand("dotnet format").kind, "known-mutator");
  assert.equal(classifyRootShellCommand("mix format").kind, "known-mutator");
  assert.equal(classifyRootShellCommand("git clean -fd").kind, "known-mutator");
  assert.equal(classifyRootShellCommand("git checkout main").kind, "known-mutator");
  assert.equal(classifyRootShellCommand("rm -rf dist").kind, "known-mutator");

  // Pure observational tools remain read-only
  assert.equal(classifyRootShellCommand("git log -n 5").kind, "read-only");
  assert.equal(classifyRootShellCommand("git show HEAD").kind, "read-only");
  assert.equal(classifyRootShellCommand("cat src/index.ts").kind, "read-only");
  assert.equal(classifyRootShellCommand("head -n 20 README.md").kind, "read-only");
  assert.equal(classifyRootShellCommand("rg 'export' src").kind, "read-only");

  // Arbitrary project code execution commands are unknown (trusted-root escape hatch)
  assert.equal(classifyRootShellCommand("cargo check").kind, "unknown");
  assert.equal(classifyRootShellCommand("cargo test").kind, "unknown");
  assert.equal(classifyRootShellCommand("npm test").kind, "unknown");
  assert.equal(classifyRootShellCommand("pytest").kind, "unknown");
  assert.equal(classifyRootShellCommand("node script-that-writes.js").kind, "unknown");
  assert.equal(classifyRootShellCommand("dotnet run").kind, "unknown");
});

test("shell-classifier: chained commands and pipelines", () => {
  // Read-only pipeline
  const pipeRead = classifyRootShellCommand("git status | grep modified | wc -l");
  assert.equal(pipeRead.kind, "read-only");

  // Pure read-only chain
  const chainRead = classifyRootShellCommand("git status && git diff");
  assert.equal(chainRead.kind, "read-only");

  // Chain containing unknown code execution becomes unknown
  const chainUnknown = classifyRootShellCommand("git status && cargo test");
  assert.equal(chainUnknown.kind, "unknown");

  // Chain containing a mutator becomes a mutator
  const chainMutator = classifyRootShellCommand("cargo test && cargo fmt");
  assert.equal(chainMutator.kind, "known-mutator");
  assert.equal((chainMutator as any).scope, "broad");

  // Redirection to /dev/null does not mutate
  const devNull = classifyRootShellCommand("git diff > /dev/null 2>&1");
  assert.equal(devNull.kind, "read-only");

  // Redirection with space to a file mutates that path
  const fileRedir = classifyRootShellCommand("echo 'new content' >> src/types.ts");
  assert.equal(fileRedir.kind, "known-mutator");
  assert.equal((fileRedir as any).scope, "path");
  assert.deepEqual((fileRedir as any).paths, ["src/types.ts"]);

  // No-space redirection (echo x>src/a.ts) mutates that path
  const noSpaceRedir = classifyRootShellCommand("echo x>src/a.ts");
  assert.equal(noSpaceRedir.kind, "known-mutator");
  assert.equal((noSpaceRedir as any).scope, "path");
  assert.deepEqual((noSpaceRedir as any).paths, ["src/a.ts"]);

  // No-space append redirection (printf x>>src/a.ts) mutates that path
  const noSpaceAppend = classifyRootShellCommand("printf x>>src/a.ts");
  assert.equal(noSpaceAppend.kind, "known-mutator");
  assert.equal((noSpaceAppend as any).scope, "path");
  assert.deepEqual((noSpaceAppend as any).paths, ["src/a.ts"]);

  // tee pipeline writes to target file
  const teePipe = classifyRootShellCommand("cat input | tee src/a.ts");
  assert.equal(teePipe.kind, "known-mutator");
  assert.equal((teePipe as any).scope, "path");
  assert.deepEqual((teePipe as any).paths, ["src/a.ts"]);

  // Chained command with directory change (cd/pushd) broadens scoped mutators
  const cdMutator = classifyRootShellCommand("cd subdir && prettier --write file.ts");
  assert.equal(cdMutator.kind, "known-mutator");
  assert.equal((cdMutator as any).scope, "broad");
});

test("shell-classifier: execution wrappers (npx, pnpm exec, uv run, python -m, etc.) unwrapping", () => {
  // Scoped mutators under wrappers
  const npxPrettier = classifyRootShellCommand("npx prettier --write src");
  assert.equal(npxPrettier.kind, "known-mutator");
  assert.equal((npxPrettier as any).scope, "path");
  assert.deepEqual((npxPrettier as any).paths, ["src"]);

  const pnpmPrettier = classifyRootShellCommand("pnpm exec prettier --write src");
  assert.equal(pnpmPrettier.kind, "known-mutator");
  assert.equal((pnpmPrettier as any).scope, "path");
  assert.deepEqual((pnpmPrettier as any).paths, ["src"]);

  const yarnPrettier = classifyRootShellCommand("yarn exec prettier --write src");
  assert.equal(yarnPrettier.kind, "known-mutator");
  assert.equal((yarnPrettier as any).scope, "path");
  assert.deepEqual((yarnPrettier as any).paths, ["src"]);

  const yarnEslint = classifyRootShellCommand("yarn exec eslint --fix src");
  assert.equal(yarnEslint.kind, "known-mutator");
  assert.equal((yarnEslint as any).scope, "broad");

  const bunxPrettier = classifyRootShellCommand("bunx prettier --write src");
  assert.equal(bunxPrettier.kind, "known-mutator");
  assert.equal((bunxPrettier as any).scope, "path");
  assert.deepEqual((bunxPrettier as any).paths, ["src"]);

  // Broad mutators under wrappers
  const uvRuff = classifyRootShellCommand("uv run ruff format .");
  assert.equal(uvRuff.kind, "known-mutator");
  assert.equal((uvRuff as any).scope, "broad");

  const poetryBlack = classifyRootShellCommand("poetry run black .");
  assert.equal(poetryBlack.kind, "known-mutator");
  assert.equal((poetryBlack as any).scope, "broad");

  const pipenvBlack = classifyRootShellCommand("pipenv run black .");
  assert.equal(pipenvBlack.kind, "known-mutator");
  assert.equal((pipenvBlack as any).scope, "broad");

  const pythonBlack = classifyRootShellCommand("python -m black .");
  assert.equal(pythonBlack.kind, "known-mutator");
  assert.equal((pythonBlack as any).scope, "broad");

  const python3Black = classifyRootShellCommand("python3 -m black .");
  assert.equal(python3Black.kind, "known-mutator");
  assert.equal((python3Black as any).scope, "broad");

  // Read-only under wrappers
  const npxGit = classifyRootShellCommand("npx git status");
  assert.equal(npxGit.kind, "read-only");

  // Arbitrary execution under wrapper remains unknown
  const uvPytest = classifyRootShellCommand("uv run pytest");
  assert.equal(uvPytest.kind, "unknown");
});

