# Prior-art notes

This project was designed after inspecting Pi's current SDK/extension implementation and representative agent-team packages. It is intentionally a new coordinator rather than a fork.

## Pi official subagent example

Source inspected: `@earendil-works/pi-coding-agent/examples/extensions/subagent/` in Pi 0.85.0.

Strong ideas retained:

- use `createAgentSession()` for in-process foreground child sessions;
- use the Pi model/session APIs instead of reimplementing provider calls;
- keep agent definitions in Markdown frontmatter plus a system prompt;
- use read-only tools for scouts/reviewers and explicit tool allowlists;
- stream child lifecycle events and expose usage/status in a compact renderer;
- use `SessionManager` for persistent child sessions and `ExtensionAPI` for tools/commands.

Architectural limits for this project:

- the example is a delegation tool, not an authoritative shared runtime;
- child-to-parent clarification and peer mailboxes are outside its scope;
- parallel process/session control does not itself provide task/resource ownership;
- model inheritance must be resolved from the parent's in-memory `ctx.model`, not by allowing a child with no explicit model to consult shared Pi settings.

Relevant Pi primitives used here are documented in `docs/extensions.md`, `docs/sdk.md`, `docs/rpc.md`, and `docs/session-format.md` from the installed Pi package.

## nicobailon/pi-subagents

Sources inspected:

- `README.md`, `docs/agents.md`, `docs/models.md`, `docs/observability.md`, `docs/tool-reference.md`, `docs/workflows.md`, `docs/configuration.md`;
- issue #335 (foreground clarification deadlock), #266 (cross-session model contamination), #581 (async supervisor request visibility), and #1377 (explicit model exclusion falling through to the parent model).

Strong ideas retained:

- parent-controlled bounded delegation;
- explicit fresh/fork context choices;
- model/provider/thinking precedence and per-role overrides;
- child-safety limits, output/artifact boundaries, and structured status;
- supervisor requests as a separate control channel;
- worktree isolation as a mechanical complement to semantic coordination.

Limitations addressed here:

- a blocking clarification must transition to `waiting` and release execution capacity rather than synchronously blocking a foreground parent;
- exact route resolution is done before child creation and unresolved/excluded explicit models fail closed;
- this project provides a durable broker mailbox rather than relying on parent-idle injection or a separate intercom extension;
- resource/task ownership is a coordinator invariant, not a prompt convention.

## tmustier/pi-agent-teams

Source inspected: README and issue #35 (branch-context workers inherited an unfinished leader turn and stayed idle) plus issue #9 (worktrees/session artifacts accumulated without robust automatic lifecycle cleanup).

Strong ideas retained:

- shared task board with dependencies;
- direct messages plus optional urgent steering;
- role-based least privilege;
- optional Git worktrees;
- explicit lifecycle and status visibility.

Limitations addressed here:

- managed child sessions start from a clean bootstrap prompt; they do not clone an in-flight tool-call leaf;
- mailbox delivery is durable and acknowledged rather than inferred from a file read or terminal injection;
- worktree paths and lifecycle state are recorded independently of model context; v1 leaves dirty artifacts for explicit inspection instead of silently deleting them.

## LinYS77/pi-agentteam

Source inspected: README and its documented architecture.

Strong ideas retained:

- leader-gated task facts and structured `report_done`/`report_blocked` outcomes;
- compact task/message references rather than copying full message bodies into every task record;
- explicit receive boundaries and bounded attention signals;
- role-level model configuration.

Limitations addressed here:

- the v1 fabric allows recursive child orchestration when the coordinator grants it;
- parent/peer routing is an actor-level capability and visibility rule, not only a leader/team workflow;
- the core coordinator is transport-independent and testable without tmux.

## ross-jill-ws/pi-teammate

Source inspected: README and documented design/communication layout.

Strong ideas retained:

- equal peer communication can be useful for collaboration;
- a shared message bus and a mechanical noise-guard preserve model context;
- roster/discovery should be compact and explicit.

Limitations addressed here:

- a peer network without authoritative ownership is insufficient for correctness;
- the broker grants scoped visibility and capabilities rather than making every agent a member of a global chatroom;
- the core transport uses one local broker process, not a shared mutable JSON file.

## Source links

- [Pi / earendil-works](https://github.com/earendil-works/pi)
- [nicobailon/pi-subagents](https://github.com/nicobailon/pi-subagents)
- [tmustier/pi-agent-teams](https://github.com/tmustier/pi-agent-teams)
- [LinYS77/pi-agentteam](https://github.com/LinYS77/pi-agentteam)
- [ross-jill-ws/pi-teammate](https://github.com/ross-jill-ws/pi-teammate)

## Design conclusion

The unified boundary is an actor-style local broker:

- Pi sessions and LLMs decide *what* to ask, delegate, share, or escalate;
- the broker decides *who* may do it, *whether* it conflicts, *when* it is delivered, and *what* survives a crash;
- Pi SDK sessions remain the model/runtime primitive;
- worktrees remain an optional mechanical isolation layer, not the semantic conflict detector.
