/**
 * Coordinator wire-format internals: argument parsing, capability ceilings,
 * policy-key canonicalization, idempotency keys/fingerprints, clone helpers,
 * and dispatch-result detection. Pure module-scope functions extracted from
 * the Coordinator class boundary so state transitions and their argument
 * contracts can be read separately. Not part of the public package surface.
 */
import { createHash } from "node:crypto";
import { FabricError, assertCondition } from "./errors.ts";
import { cloneAgent, type AgentCapabilities, type AgentRecord, type AgentStatus, type DispatchResult, type FabricConfig, type MessageType, type ResourcePermission, type TaskRecord } from "./types.ts";

export const TERMINAL_STATUSES = new Set<AgentStatus>(["completed", "failed", "cancelled"]);
export const ACTIVE_STATUSES = new Set<AgentStatus>(["starting", "ready", "running", "waiting", "blocked"]);
export const ALL_MESSAGE_TYPES = new Set<MessageType>([
  "inform",
  "clarification",
  "decision_request",
  "escalation",
  "blocked",
  "progress",
  "result",
  "task_result",
  "handoff",
  "resource_request",
  "resource_granted",
  "request",
  "response",
  "cancel",
  "steer",
  "agent_failed",
]);
export const INTERNAL_MESSAGE_TYPES = new Set<MessageType>(["agent_failed", "resource_granted"]);
export const ALL_RESOURCE_PERMISSIONS = new Set<ResourcePermission>(["read", "comment", "write", "test"]);
export const ALL_THINKING_LEVELS = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);

/** @internal Coordinator internals; not part of the public surface. */
export function isTerminal(status: AgentStatus): boolean {
  return TERMINAL_STATUSES.has(status);
}

/** @internal Coordinator internals; not part of the public surface. */
export function isTaskTerminal(status: TaskRecord["status"]): boolean {
  return status === "completed" || status === "failed" || status === "cancelled";
}

export function defaultCapabilities(): AgentCapabilities {
  return {
    maySpawn: false,
    mayMessagePeers: false,
    mayEscalate: true,
    mayTransferOwnership: false,
    mayWriteRepo: false,
    mayUseShell: false,
    peerIds: [],
    resourceGrants: {},
  };
}

export function mergeCapabilities(requested: Partial<AgentCapabilities> | undefined, parent?: AgentCapabilities): AgentCapabilities {
  const base = defaultCapabilities();
  if (requested !== undefined) assertCondition(Boolean(requested) && typeof requested === "object" && !Array.isArray(requested), "INVALID_ARGUMENT", "capabilities must be an object");
  for (const key of ["maySpawn", "mayMessagePeers", "mayEscalate", "mayTransferOwnership", "mayWriteRepo", "mayUseShell"] as const) {
    if (requested?.[key] !== undefined) assertCondition(typeof requested[key] === "boolean", "INVALID_ARGUMENT", `${key} must be a boolean`);
  }
  if (requested?.peerIds !== undefined) {
    assertCondition(Array.isArray(requested.peerIds), "INVALID_ARGUMENT", "peerIds must be an array");
    for (const peerId of requested.peerIds) parseAgentId(peerId, "peerIds[]");
  }
  if (requested?.resourceGrants !== undefined) {
    assertCondition(Boolean(requested.resourceGrants) && typeof requested.resourceGrants === "object" && !Array.isArray(requested.resourceGrants), "INVALID_ARGUMENT", "resourceGrants must be an object");
  }
  const ceiling = parent ?? {
    maySpawn: true,
    mayMessagePeers: true,
    mayEscalate: true,
    mayTransferOwnership: true,
    mayWriteRepo: true,
    mayUseShell: true,
    peerIds: [],
    resourceGrants: {},
  };
  const result: AgentCapabilities = {
    maySpawn: Boolean(requested?.maySpawn && ceiling.maySpawn),
    mayMessagePeers: Boolean(requested?.mayMessagePeers && ceiling.mayMessagePeers),
    mayEscalate: requested?.mayEscalate === undefined ? ceiling.mayEscalate : Boolean(requested.mayEscalate && ceiling.mayEscalate),
    mayTransferOwnership: Boolean(requested?.mayTransferOwnership && ceiling.mayTransferOwnership),
    mayWriteRepo: Boolean(requested?.mayWriteRepo && ceiling.mayWriteRepo),
    mayUseShell: Boolean(requested?.mayUseShell && ceiling.mayUseShell),
    // An empty explicit list means no explicit exceptions. Only a root may
    // establish the initial list; descendants can receive a strict subset.
    peerIds: parent
      ? [...new Set((requested?.peerIds ?? []).filter((id) => parent.peerIds.includes(id)))]
      : [...new Set(requested?.peerIds ?? [])],
    resourceGrants: {},
  };
  for (const [resourceId, rawPermissions] of Object.entries(requested?.resourceGrants ?? {})) {
    assertCondition(Array.isArray(rawPermissions), "INVALID_ARGUMENT", `resourceGrants.${resourceId} must be an array`);
    const permissions = rawPermissions.map((permission) => {
      assertCondition(typeof permission === "string" && ALL_RESOURCE_PERMISSIONS.has(permission as ResourcePermission), "INVALID_ARGUMENT", `Unknown resource permission ${String(permission)}`);
      return permission as ResourcePermission;
    });
    const ceilingPermissions = parent && hasOwn(parent.resourceGrants, resourceId) ? parent.resourceGrants[resourceId] : undefined;
    const boundedPermissions = [...new Set(parent
      ? permissions.filter((permission) => ceilingPermissions?.includes(permission))
      : permissions)];
    if (boundedPermissions.length > 0) Object.defineProperty(result.resourceGrants, resourceId, { value: boundedPermissions, enumerable: true, configurable: true, writable: true });
  }
  return { ...base, ...result };
}

export function normalizeResourcePath(value: unknown, name = "path", caseInsensitive: boolean = process.platform === "win32"): string {
  const raw = parseString(value, name, 4096).replaceAll("\\", "/");
  assertCondition(!raw.startsWith("/") && !/^[A-Za-z]:/.test(raw), "INVALID_ARGUMENT", `${name} must be workspace-relative`);
  const parts = raw.split("/").filter((part) => part.length > 0 && part !== ".");
  assertCondition(parts.length > 0 && !parts.includes(".."), "INVALID_ARGUMENT", `${name} must not escape the workspace`);
  const normalized = parts.join("/");
  // Case-insensitive workspace volumes treat differently-cased spellings as
  // the same file. Canonicalize the policy key so casing cannot split a
  // resource's identity and bypass a matching file resource.
  return caseInsensitive ? normalized.toLowerCase() : normalized;
}

export function parseString(value: unknown, name: string, maxLength = 512): string {
  assertCondition(typeof value === "string" && value.length > 0 && value.length <= maxLength, "INVALID_ARGUMENT", `${name} must be a non-empty string of at most ${maxLength} characters`);
  assertCondition(!value.includes("\u0000"), "INVALID_ARGUMENT", `${name} must not contain a NUL character`);
  return value;
}

export function parseAgentId(value: unknown, name = "agentId"): string {
  const id = parseString(value, name);
  assertCondition(!["__proto__", "constructor", "prototype"].includes(id), "INVALID_ARGUMENT", `${name} uses a reserved identity key`);
  return id;
}

export function hasOwn(value: object, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(value, key);
}

export function parseOptionalString(value: unknown, name: string, maxLength = 512): string | undefined {
  if (value === undefined || value === null) return undefined;
  return parseString(value, name, maxLength);
}

export function parseNumber(value: unknown, name: string, fallback: number): number {
  if (value === undefined) return fallback;
  assertCondition(typeof value === "number" && Number.isFinite(value), "INVALID_ARGUMENT", `${name} must be a finite number`);
  return value;
}

export function parseMetadata(value: unknown): Record<string, unknown> | undefined {
  if (value === undefined) return undefined;
  assertCondition(Boolean(value) && typeof value === "object" && !Array.isArray(value), "INVALID_ARGUMENT", "metadata must be an object");
  try {
    const encoded = JSON.stringify(value);
    assertCondition(encoded.length <= 16 * 1024, "INVALID_ARGUMENT", "metadata is too large");
    return JSON.parse(encoded) as Record<string, unknown>;
  } catch (error) {
    if (error instanceof FabricError) throw error;
    throw new FabricError("INVALID_ARGUMENT", `metadata must be JSON-serializable: ${error instanceof Error ? error.message : String(error)}`);
  }
}

export function parseWorkspace(value: unknown): AgentRecord["workspace"] | undefined {
  if (value === undefined) return undefined;
  assertCondition(Boolean(value) && typeof value === "object" && !Array.isArray(value), "INVALID_ARGUMENT", "workspace must be an object");
  const workspace = value as Record<string, unknown>;
  const mode = workspace.mode;
  assertCondition(mode === "shared" || mode === "worktree", "INVALID_ARGUMENT", "workspace.mode must be shared or worktree");
  return {
    mode,
    root: parseString(workspace.root, "workspace.root", 4096),
    path: parseString(workspace.path, "workspace.path", 4096),
    baseRef: workspace.baseRef === undefined ? undefined : parseString(workspace.baseRef, "workspace.baseRef", 512),
    branch: workspace.branch === undefined ? undefined : parseString(workspace.branch, "workspace.branch", 512),
  };
}

export function cloneConfig(config: FabricConfig): FabricConfig {
  return { ...config };
}

export function idempotencyKey(actorId: string, operationId: string): string {
  return `${actorId}\u0000${operationId}`;
}

export function stableStringify(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record).sort().map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`).join(",")}}`;
}

/** Hash of everything the request would apply; operationId itself is excluded. */
export function requestFingerprint(operation: string, args: Record<string, unknown>): string {
  const { operationId: _operationId, ...rest } = args;
  return createHash("sha256").update(stableStringify({ operation, args: rest })).digest("hex");
}

export function normalizeClone<T>(value: T): T {
  return value === undefined ? value : (JSON.parse(JSON.stringify(value)) as T);
}

export function publicAgent(agent: AgentRecord): Omit<AgentRecord, "authToken"> {
  const { authToken: _authToken, ...result } = cloneAgent(agent);
  return result;
}

export function isDispatchResult<T>(value: T | DispatchResult<T>): value is DispatchResult<T> {
  return Boolean(value && typeof value === "object" && "value" in (value as object) && "events" in (value as object));
}
