# Model routing

Every managed child is created with an explicit Pi `Model` object and thinking level. The broker stores the resulting provider/model/thinking route for inspection; it does not perform provider calls.

## Precedence

The route resolver applies this order:

```text
spawn override
  > role configuration
  > fabric defaults
  > caller's selected in-memory model (inherit)
  > Pi/global default only when no caller model exists
```

`inherit` means the exact parent provider/model, not “look up whatever the global default is now.” A bare model reference is resolved uniquely in the current `ModelRegistry`; an explicit `provider/model` route must resolve to that exact pair.

Thinking level is resolved independently using the same explicit/role/default/parent/global order. Unsupported explicit thinking levels fail closed rather than silently changing the requested model or provider.

## Why this is explicit

Long-lived sibling sessions may be created while the root changes models. Reading shared settings at child creation can contaminate a child with a later or unrelated selection. Passing an exact model object to `createAgentSession({ model, thinkingLevel })` keeps each session stable and testable.

No implicit provider fallback is performed. If a role names a missing provider/model, spawn returns `MODEL_NOT_FOUND`/`MODEL_ROUTE_INVALID` and does not create a worker. The coordinator-created identity is cancelled/released if the Pi session cannot start.

## Roles

Role files are intentionally small in v1. A role may specify `model`, `provider`, `thinking`, and capability ceilings. The caller may narrow a role's capabilities, never widen its parent's ceiling. Role prompt text belongs in the child bootstrap/session resource loader; it is not an authority source.

Example:

```json
{
  "scout": {
    "model": "local/qwen2.5-coder",
    "thinking": "low",
    "capabilities": {
      "maySpawn": false,
      "mayWriteRepo": false,
      "mayUseShell": false,
      "mayMessagePeers": true
    }
  }
}
```

## Provider independence

The extension uses Pi's `ModelRegistry` and `createAgentSession`; it does not import OpenAI, Anthropic, or a local-server SDK. Any provider available to Pi can be selected if it supplies a registry model and configured authentication.
