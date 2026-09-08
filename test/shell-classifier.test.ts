import test from "node:test";
import assert from "node:assert/strict";
import { classifyRootShellCommand } from "../src/pi/shell-classifier.ts";

test("shell-classifier: Section 16 required test matrix", () => {
  const matrix: Array<[string, "read-only" | "known-mutator" | "unknown", ("broad" | "path")?]> = [
    ["cargo test", "read-only"],
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

  assert.equal(classifyRootShellCommand("git log -n 5").kind, "read-only");
  assert.equal(classifyRootShellCommand("git show HEAD").kind, "read-only");
  assert.equal(classifyRootShellCommand("cargo check").kind, "read-only");
  assert.equal(classifyRootShellCommand("npm test").kind, "read-only");
  assert.equal(classifyRootShellCommand("cat src/index.ts").kind, "read-only");
  assert.equal(classifyRootShellCommand("head -n 20 README.md").kind, "read-only");
});

test("shell-classifier: chained commands and pipelines", () => {
  // Read-only pipeline
  const pipeRead = classifyRootShellCommand("git status | grep modified | wc -l");
  assert.equal(pipeRead.kind, "read-only");

  // Read-only chain
  const chainRead = classifyRootShellCommand("git status && cargo test");
  assert.equal(chainRead.kind, "read-only");

  // Chain containing a mutator becomes a mutator
  const chainMutator = classifyRootShellCommand("cargo test && cargo fmt");
  assert.equal(chainMutator.kind, "known-mutator");
  assert.equal((chainMutator as any).scope, "broad");

  // Redirection to /dev/null does not mutate
  const devNull = classifyRootShellCommand("git diff > /dev/null 2>&1");
  assert.equal(devNull.kind, "read-only");

  // Redirection to a file mutates that path
  const fileRedir = classifyRootShellCommand("echo 'new content' >> src/types.ts");
  assert.equal(fileRedir.kind, "known-mutator");
  assert.equal((fileRedir as any).scope, "path");
  assert.deepEqual((fileRedir as any).paths, ["src/types.ts"]);
});
