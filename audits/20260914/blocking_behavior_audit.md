Yes. I think this incident exposes a genuine design mismatch in the current extension.

The extension's primary job is **coordination correctness**—preventing agents from racing, clobbering shared work, corrupting ownership state, or leaving unreclaimable processes. The current shared-workspace shell instead behaves much closer to a **security sandbox**: it allows only a small executable allowlist and requires shell path arguments to remain workspace-relative.  That is why a harmless observational application such as `codexbar --provider codex --json-only` gets rejected despite having essentially no relationship to repository concurrency.

I would change this before `v0.3.1`.

## The conceptual mistake

There are really three different concerns:

1. **Coordination safety** — can this operation race another agent's filesystem work?
2. **Process lifecycle safety** — can the operation escape cancellation/reclamation, e.g. background daemons?
3. **General security/sandboxing** — can the child run an arbitrary program, inspect `$HOME`, access the network, credentials, etc.?

`pi-safe-agent-team` needs to be very strong on 1 and 2. It does **not** need to silently impose a strict sandbox for 3 unless the user explicitly asks for one.

Today those are conflated. Shared-workspace children with `mayUseShell=true` still go through `assertReadOnlyShellCommand()`, which permits only a hard-coded set such as `cat`, `grep`, `rg`, `ls`, `git status`, etc. Unknown executable names are rejected outright.

That makes `mayUseShell=true` surprisingly weak.

By contrast, the project already has a significantly richer shell classifier used on the root side. It distinguishes `read-only`, `known-mutator`, and `unknown`, recognizes things like `git reset`, `sed -i`, `prettier --write`, `rm`, `tee`, redirection, etc., and unwraps `npx`, `pnpm exec`, Python modules and similar wrappers.  That is much closer to the right primitive for children too.

## I would make blocklist/risk-based behavior the default

I suggest three shell policies, orthogonal to `mayUseShell`:

| Policy             | Intended use                        | Behavior                                                                                    |
| ------------------ | ----------------------------------- | ------------------------------------------------------------------------------------------- |
| **`coordination`** | **Default** shared-workspace shell  | Block known dangerous/mutating patterns; permit unfamiliar foreground programs              |
| **`strict`**       | Explicit high-security environments | Current allowlist + workspace-only path rules                                               |
| **`trusted`**      | Isolated worktrees                  | Essentially current worktree shell; block only lifecycle hazards such as detached processes |

`mayUseShell=false` would still mean **no shell at all**.

So granting `mayUseShell=true` becomes meaningful: "this child may execute foreground commands, subject to coordination safeguards." Users who want a true narrow sandbox explicitly select `strict`.

For your example:

```text
codexbar --provider codex --json-only
```

would be classified as an unfamiliar foreground command and **allowed** under `coordination`.

`strict` would continue rejecting it until explicitly allowlisted.

That is much less surprising.

## But I would not simply change “unknown → allow”

There is one important reason.

An unknown binary can secretly mutate the repository:

```text
mystery-tool
```

could internally rewrite `src/foo.ts`.

If we simply allow it in a shared workspace, we lose the extension's strongest claim: shell access becomes an unguarded way around resource borrowing.

The better blocklist design is therefore:

> **Unknown does not mean prohibited; unknown means opaque.**

An opaque command should be allowed, but treated conservatively for coordination.

I would introduce an **opaque shell execution fence** around unknown shared-workspace commands:

```text
child wants:
codexbar --provider codex --json-only

broker:
acquire opaque shared-workspace shell barrier
        ↓
run codexbar
        ↓
release barrier
```

While that barrier exists:

* other child shared-workspace mutations cannot begin;
* root guarded `edit`/`write` cannot race it;
* conflicting mutating shell operations cannot begin;
* ordinary reads remain unrestricted.

Thus even if an unfamiliar executable unexpectedly touches the workspace, **it cannot race another writer**.

This is an important distinction:

> We don't have to prove an unfamiliar application is read-only.
> We only have to make its uncertainty safe with respect to concurrency.

For `codexbar`, the fence would be acquired briefly, the command would run, and the task would continue rather than escalating to the root.

### Known commands can be less conservative

The existing classifier already gives us useful information.

A sensible policy would be:

```text
known observational
    → allow concurrently

unknown / opaque foreground command
    → allow under broad workspace shell fence

known path mutator
    → initially block in shared workspace
       (later: permit if corresponding mutable resources can be fenced)

known broad mutator
    → block in shared workspace

detached/background process
    → always block for managed children
```

Examples:

```text
codexbar --provider codex --json-only
→ opaque, allowed under fence

gh issue view 123
→ opaque, allowed under fence

pytest
→ opaque/project-executing, allowed under fence

git status
→ observational, no fence required

git reset --hard
→ known broad mutator, blocked

prettier --write src/
→ known mutator, blocked

rm -rf src/
→ known mutator, blocked

nohup foo &
→ process-lifecycle violation, blocked
```

That is a **blocklist-first architecture without pretending arbitrary commands are provably harmless**.

## There is an even better future extension

Known path-scoped mutators could eventually participate directly in borrowing.

Suppose the child runs:

```text
prettier --write src/foo.ts
```

The classifier already knows this is a path mutator and can often identify `src/foo.ts`.

Instead of permanently blocking it, a later version could:

```text
classify target src/foo.ts
    ↓
verify child has mutable borrow
    ↓
resource.begin_write(src/foo.ts)
    ↓
execute prettier
    ↓
resource.end_write(...)
```

Then shell mutations become first-class coordinated writes too.

I would **not** make that necessary for v0.3.1. The opaque barrier is much simpler and fixes the usability failure without weakening concurrency behavior.

## Outside-workspace access should be a separate policy axis

I would be a little more conservative here than "blocklist everything."

The current code also requires shell arguments to stay workspace-relative.  That's partly why the child complained about `/Users/saehwan`.

But these are different cases:

```text
codexbar --provider codex --json-only
```

and

```text
cat ~/.ssh/id_rsa
```

Both may involve data outside the repo, but they are obviously not equivalent from a privacy/security perspective.

Importantly, **running CodexBar does not require giving the LLM arbitrary direct filesystem-reading tools outside the workspace**. CodexBar itself can read whatever application state it legitimately needs under the user's OS account.

So I would keep external filesystem inspection as a separate configurable boundary:

```text
shellPolicy: coordination     # default
externalPathAccess: deny      # reasonable default

# opt-in alternatives
externalPathAccess: read
externalPathAccess: any
```

This gives us the behavior you wanted:

```text
codexbar --provider codex --json-only
→ allowed
```

without automatically turning:

```text
cat /Users/saehwan/.ssh/id_rsa
```

into an accepted child operation.

That separation is valuable especially because some children may be using cloud models.

## This also fits the existing capability model better

There's another reason I favor this change: `mayUseShell` already exists as an explicit capability.

The model currently has to pass two gates:

```text
mayUseShell=true
AND
command belongs to tiny fixed allowlist
```

The second gate makes the first one much less useful than its name suggests.

I think the semantics should instead be:

```text
mayUseShell=false
→ no command execution

mayUseShell=true + coordination policy
→ foreground shell is a trusted capability,
  mechanically guarded against coordination races

mayUseShell=true + strict policy
→ restrictive allowlist sandbox

worktree + trusted
→ isolated ordinary shell
```

That is easier for users and agents to reason about.

## One thing I would change in the messaging too

This child's behavior was actually reasonable given the restrictions:

> "please run the documented command from root and share JSON/output"

But this creates exactly the orchestration friction we don't want:

```text
delegate task
→ harmless command blocked
→ child blocks
→ root wakes
→ root runs command
→ sends result
→ child wakes again
```

That consumes two additional coordination/model turns for something the child could have completed locally in milliseconds.

In local-model workflows in particular, that is wasted context, latency, and model work.

A core UX principle for this project should probably become:

> **Coordination should block conflicting work, not merely unfamiliar work.**

That is a much better north star than "children may execute only things we know."

## Suggested v0.3.1 change

I would actually fold this into the RC rather than defer it, because you've now encountered it in a real workload.

My preferred minimal implementation is:

1. Rename/reframe the current shared shell implementation as **strict mode**.
2. Add a new default **coordination mode** using the existing shell classifier.
3. In coordination mode:

   * allow known observational commands;
   * allow unknown foreground executables;
   * block known mutators initially;
   * continue blocking detached/background execution;
   * keep explicit external-path access separately controlled.
4. Ideally wrap unknown commands with a **broad opaque-shell coordination fence** for their execution lifetime.
5. Keep worktree shell behavior as the trusted/isolated mode.
6. Add examples/regressions for:

   * `codexbar --provider codex --json-only` → allowed;
   * `gh`, `docker inspect`, `nvidia-smi`, `ollama list`, custom internal CLI → allowed;
   * `git reset --hard`, `rm`, `sed -i`, `prettier --write` → blocked in shared mode;
   * detached/background shell → blocked;
   * strict mode retains today's allowlist behavior.

Without the broad fence, I would still prefer the blocklist default on usability grounds, but we'd need to explicitly document shared child shell as a **trusted/best-effort escape hatch**, analogous to the root shell.

With the broad fence, I think we can have both properties:

**far less delegation friction** and **no meaningful regression in race-condition safety**.

That is the direction I would choose for `v0.3.1`.
