import { randomUUID } from "node:crypto";
import { FabricError, assertCondition } from "./errors.ts";
import {
  DEFAULT_FABRIC_CONFIG,
  cloneAgent,
  cloneMessage,
  cloneRequest,
  cloneResource,
  cloneTask,
  type AgentCapabilities,
  type AgentId,
  type AgentMessage,
  type AgentRecord,
  type AgentStatus,
  type AgentSummary,
  type BorrowMode,
  type CoordinatorEvent,
  type DispatchResult,
  type FabricConfig,
  type FabricStatus,
  type MessagePriority,
  type MessageType,
  type ModelRoute,
  type PersistedCoordinatorState,
  type RequestId,
  type RequestRecord,
  type RequestStatus,
  type ResourceHold,
  type ResourceId,
  type ResourcePermission,
  type ResourceRecord,
  type ResourceWaiter,
  type TaskId,
  type TaskRecord,
  type TaskResult,
} from "./types.ts";

export interface CoordinatorOptions {
  rootId: string;
  rootAgentId?: string;
  config?: Partial<FabricConfig>;
  clock?: () => number;
  idFactory?: (prefix: string) => string;
}

export interface RegisterAgentArgs {
  rootId?: string;
  parentId?: AgentId;
  depth?: number;
  role?: string;
  route: ModelRoute;
  capabilities?: Partial<AgentCapabilities>;
  sessionId?: string;
  workspace?: AgentRecord["workspace"];
  taskId?: TaskId;
  token?: string;
  initialStatus?: "starting" | "ready";
}

export interface SpawnAgentArgs {
  role?: string;
  route: ModelRoute;
  capabilities?: Partial<AgentCapabilities>;
  sessionId?: string;
  taskId?: TaskId;
  taskDescription?: string;
  workspace?: AgentRecord["workspace"];
}

export interface MessageSendArgs {
  to: AgentId;
  type: MessageType;
  body: string;
  priority?: MessagePriority;
  expectsReply?: boolean;
  clientDedupeKey?: string;
  metadata?: Record<string, unknown>;
}

export interface ResourceBorrowArgs {
  resourceId: ResourceId;
  mode: BorrowMode;
  leaseMs?: number;
  wait?: boolean;
}

export interface CoordinatorSnapshot {
  state: PersistedCoordinatorState;
}

const TERMINAL_STATUSES = new Set<AgentStatus>(["completed", "failed", "cancelled"]);
const ACTIVE_STATUSES = new Set<AgentStatus>(["starting", "ready", "running", "waiting", "blocked"]);
const ALL_MESSAGE_TYPES = new Set<MessageType>([
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
const INTERNAL_MESSAGE_TYPES = new Set<MessageType>(["agent_failed", "resource_granted"]);
const ALL_RESOURCE_PERMISSIONS = new Set<ResourcePermission>(["read", "comment", "write", "test"]);
const ALL_THINKING_LEVELS = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);

function isTerminal(status: AgentStatus): boolean {
  return TERMINAL_STATUSES.has(status);
}

function isTaskTerminal(status: TaskRecord["status"]): boolean {
  return status === "completed" || status === "failed" || status === "cancelled";
}

function defaultCapabilities(): AgentCapabilities {
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

function mergeCapabilities(requested: Partial<AgentCapabilities> | undefined, parent?: AgentCapabilities): AgentCapabilities {
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

function normalizeResourcePath(value: unknown, name = "path"): string {
  const raw = parseString(value, name, 4096).replaceAll("\\", "/");
  assertCondition(!raw.startsWith("/") && !/^[A-Za-z]:/.test(raw), "INVALID_ARGUMENT", `${name} must be workspace-relative`);
  const parts = raw.split("/").filter((part) => part.length > 0 && part !== ".");
  assertCondition(parts.length > 0 && !parts.includes(".."), "INVALID_ARGUMENT", `${name} must not escape the workspace`);
  const normalized = parts.join("/");
  // Windows workspace paths are case-insensitive even when the declared
  // resource spelling is not. Canonicalize the policy key so casing cannot
  // bypass a matching file resource.
  return process.platform === "win32" ? normalized.toLowerCase() : normalized;
}

function parseString(value: unknown, name: string, maxLength = 512): string {
  assertCondition(typeof value === "string" && value.length > 0 && value.length <= maxLength, "INVALID_ARGUMENT", `${name} must be a non-empty string of at most ${maxLength} characters`);
  assertCondition(!value.includes("\u0000"), "INVALID_ARGUMENT", `${name} must not contain a NUL character`);
  return value;
}

function parseAgentId(value: unknown, name = "agentId"): string {
  const id = parseString(value, name);
  assertCondition(!["__proto__", "constructor", "prototype"].includes(id), "INVALID_ARGUMENT", `${name} uses a reserved identity key`);
  return id;
}

function hasOwn(value: object, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function parseOptionalString(value: unknown, name: string, maxLength = 512): string | undefined {
  if (value === undefined || value === null) return undefined;
  return parseString(value, name, maxLength);
}

function parseNumber(value: unknown, name: string, fallback: number): number {
  if (value === undefined) return fallback;
  assertCondition(typeof value === "number" && Number.isFinite(value), "INVALID_ARGUMENT", `${name} must be a finite number`);
  return value;
}

function parseMetadata(value: unknown): Record<string, unknown> | undefined {
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

function parseWorkspace(value: unknown): AgentRecord["workspace"] | undefined {
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

function cloneConfig(config: FabricConfig): FabricConfig {
  return { ...config };
}

export class Coordinator {
  readonly rootId: string;
  readonly rootAgentId?: string;
  readonly config: FabricConfig;

  private readonly clock: () => number;
  private readonly idFactory: (prefix: string) => string;
  private agents = new Map<AgentId, AgentRecord>();
  private tasks = new Map<TaskId, TaskRecord>();
  private resources = new Map<ResourceId, ResourceRecord>();
  private messages = new Map<string, AgentMessage>();
  private requests = new Map<RequestId, RequestRecord>();
  private dedupe = new Map<string, string>();
  private nextMessageSequence = new Map<AgentId, number>();
  private nextBrokerSequence = 0;
  private nextResourceWaiterSequence = 0;

  constructor(options: CoordinatorOptions) {
    this.rootId = parseString(options.rootId, "rootId");
    this.rootAgentId = options.rootAgentId === undefined ? undefined : parseString(options.rootAgentId, "rootAgentId");
    this.clock = options.clock ?? (() => Date.now());
    this.idFactory = options.idFactory ?? ((prefix) => `${prefix}-${randomUUID()}`);
    this.config = {
      ...DEFAULT_FABRIC_CONFIG,
      ...(options.config ?? {}),
    };
    assertCondition(this.config.maxDepth >= 0, "INVALID_ARGUMENT", "maxDepth must be non-negative");
    assertCondition(this.config.maxChildrenPerAgent >= 0, "INVALID_ARGUMENT", "maxChildrenPerAgent must be non-negative");
    assertCondition(this.config.maxTotalAgents > 0, "INVALID_ARGUMENT", "maxTotalAgents must be positive");
    assertCondition(this.config.maxConcurrentAgents > 0, "INVALID_ARGUMENT", "maxConcurrentAgents must be positive");
    assertCondition(this.config.maxMailboxMessages > 0, "INVALID_ARGUMENT", "maxMailboxMessages must be positive");
    assertCondition(this.config.maxMessageBody > 0, "INVALID_ARGUMENT", "maxMessageBody must be positive");
    assertCondition(this.config.maxTaskOutput > 0, "INVALID_ARGUMENT", "maxTaskOutput must be positive");
    assertCondition(this.config.messageRetention > 0, "INVALID_ARGUMENT", "messageRetention must be positive");
    assertCondition(this.config.leaseMs > 0, "INVALID_ARGUMENT", "leaseMs must be positive");
    assertCondition(this.config.heartbeatMs > 0, "INVALID_ARGUMENT", "heartbeatMs must be positive");
  }

  /** Apply a protocol operation as one synchronous, atomic state transition. */
  dispatch(actorId: AgentId | undefined, operation: string, args: Record<string, unknown> = {}): DispatchResult<any> {
    const before = this.exportState();
    const events: CoordinatorEvent[] = [];
    assertCondition(Boolean(args) && typeof args === "object" && !Array.isArray(args), "INVALID_ARGUMENT", "operation args must be an object");
    this.reclaimExpired(this.clock(), events);

    try {
      switch (operation) {
      case "agent.register":
        return this.withEvents(events, this.registerAgent(actorId, args as unknown as RegisterAgentArgs, events));
      case "agent.spawn":
        return this.withEvents(events, this.spawnAgent(this.requireActor(actorId).id, args as unknown as SpawnAgentArgs, events));
      case "agent.update":
        return this.withEvents(events, this.updateAgent(this.requireActor(actorId).id, args, events));
      case "agent.configure_child":
        return this.withEvents(events, this.configureChild(this.requireActor(actorId).id, args, events));
      case "agent.begin_turn":
        return this.withEvents(events, this.beginTurn(this.requireActor(actorId).id, events));
      case "agent.end_turn":
        return this.withEvents(events, this.endTurn(this.requireBoundAgent(actorId).id, args, events));
      case "agent.heartbeat":
        return this.withEvents(events, this.heartbeat(this.requireActor(actorId).id, events));
      case "agent.cancel":
        return this.withEvents(events, this.cancelAgent(this.requireActor(actorId).id, parseString(args.agentId ?? actorId, "agentId"), events));
      case "agent.status":
        return this.withEvents(events, this.getAgentStatus(this.requireActor(actorId).id, parseOptionalString(args.agentId, "agentId"), args.scope), events);
      case "discover.agents":
        return this.withEvents(events, this.discoverAgents(this.requireActor(actorId).id, args.scope), events);
      case "message.send":
        return this.withEvents(events, this.sendMessage(this.requireActor(actorId).id, args as unknown as MessageSendArgs, events));
      case "message.reply":
        return this.withEvents(events, this.replyToRequest(parseString(actorId, "agentId"), args, events));
      case "message.ack":
        return this.withEvents(events, this.ackMessage(this.requireActor(actorId).id, parseString(args.messageId, "messageId"), events));
      case "message.inbox":
        return this.withEvents(events, this.inbox(this.requireActor(actorId).id, args.limit), events);
      case "message.list":
        return this.withEvents(events, this.listMessages(this.requireActor(actorId).id, args), events);
      case "task.create":
        return this.withEvents(events, this.createTask(this.requireActor(actorId).id, args, events));
      case "task.claim":
        return this.withEvents(events, this.claimTask(this.requireActor(actorId).id, parseString(args.taskId, "taskId"), events));
      case "task.update":
        return this.withEvents(events, this.updateTask(this.requireActor(actorId).id, args, events));
      case "task.list":
        return this.withEvents(events, this.listTasks(this.requireActor(actorId).id, args), events);
      case "task.show":
        return this.withEvents(events, this.showTask(this.requireActor(actorId).id, parseString(args.taskId, "taskId")), events);
      case "resource.define":
        return this.withEvents(events, this.defineResource(this.requireActor(actorId).id, args, events));
      case "resource.inspect":
        return this.withEvents(events, this.inspectResource(this.requireActor(actorId).id, parseString(args.resourceId, "resourceId"), args.version), events);
      case "resource.snapshot":
        return this.withEvents(events, this.resourceSnapshot(this.requireActor(actorId).id, parseString(args.resourceId, "resourceId")), events);
      case "resource.list":
        return this.withEvents(events, this.listResources(this.requireActor(actorId).id), events);
      case "resource.grant":
        return this.withEvents(events, this.grantResource(this.requireActor(actorId).id, args, events));
      case "resource.claim":
      case "resource.own":
        return this.withEvents(events, this.claimResource(this.requireActor(actorId).id, args, events));
      case "resource.borrow":
        return this.withEvents(events, this.borrowResource(this.requireActor(actorId).id, args as unknown as ResourceBorrowArgs, events));
      case "resource.transfer":
        return this.withEvents(events, this.transferResource(this.requireActor(actorId).id, args, events));
      case "resource.release":
        return this.withEvents(events, this.releaseResource(this.requireActor(actorId).id, args, events));
      case "resource.check_write":
        return this.withEvents(events, this.checkWrite(this.requireActor(actorId).id, args), events);
      case "fabric.status":
        return this.withEvents(events, this.status(this.requireActor(actorId).id, args), events);
        default:
          throw new FabricError("INVALID_ARGUMENT", `Unknown coordinator operation: ${operation}`);
      }
    } catch (error) {
      this.restoreState(before);
      throw error;
    }
  }

  registerAgent(actorId: AgentId | undefined, input: RegisterAgentArgs, events: CoordinatorEvent[] = []): { agent: Omit<AgentRecord, "authToken">; token: string } {
    const id = parseAgentId(actorId);
    assertCondition(id !== "broker", "IDENTITY_CONFLICT", "The broker identity is reserved");
    const rootId = parseString(input.rootId, "rootId");
    const parentId = input.parentId === undefined ? undefined : parseAgentId(input.parentId, "parentId");
    const taskId = input.taskId === undefined ? undefined : parseString(input.taskId, "taskId");
    const sessionId = parseOptionalString(input.sessionId, "sessionId");
    const token = input.token === undefined ? undefined : parseString(input.token, "token");
    const role = parseOptionalString(input.role, "role", 128);
    const now = this.clock();
    const route = this.validateRoute(input.route);
    const workspace = parseWorkspace(input.workspace);
    const existing = this.agents.get(id);

    if (existing) {
      assertCondition(Boolean(existing.authToken) && token === existing.authToken, "IDENTITY_CONFLICT", `Agent ${id} requires its reconnect credential`);
      if (parentId && parentId !== existing.parentId) {
        throw new FabricError("IDENTITY_CONFLICT", `Agent ${id} cannot change parent identity`);
      }
      if (rootId !== this.rootId) {
        throw new FabricError("IDENTITY_CONFLICT", `Agent ${id} belongs to another fabric`);
      }
      assertCondition(!isTerminal(existing.status) || existing.reconnectable === true, "LIFECYCLE_CONFLICT", `Agent ${id} is terminal and cannot reconnect`);
      const next = cloneAgent(existing);
      if (!next.authToken) next.authToken = token ?? this.idFactory("token");
      if (next.reconnectable) {
        assertCondition(next.status === "failed", "LIFECYCLE_CONFLICT", `Agent ${id} has an invalid recovery state`);
        next.status = "ready";
        next.reconnectable = false;
      } else if (next.status === "starting") {
        next.status = "ready";
      }
      next.statusReason = undefined;
      next.lastActivity = now;
      next.sessionId = sessionId ?? next.sessionId;
      next.workspace = workspace ?? next.workspace;
      assertCondition(taskId === undefined || taskId === next.taskId, "IDENTITY_CONFLICT", `Agent ${id} cannot change its assigned task`);
      if (next.taskId) {
        const task = this.tasks.get(next.taskId);
        if (!task || task.owner && task.owner !== id || task && isTaskTerminal(task.status)) {
          next.taskId = undefined;
        } else if (!task.owner) {
          task.owner = id;
          task.status = this.taskDependenciesCompleted(task) ? "active" : "pending";
          task.blockedReason = undefined;
          task.updatedAt = now;
          events.push({ type: "task_changed", task: cloneTask(task) });
        }
      }
      if (input.route && (input.route.provider !== existing.route.provider || input.route.model !== existing.route.model || input.route.thinking !== existing.route.thinking)) {
        next.route = route;
      }
      this.agents.set(id, next);
      events.push({ type: "agent_updated", agent: cloneAgent(next) });
      return { agent: publicAgent(next), token: next.authToken as string };
    }

    assertCondition(rootId === this.rootId, "IDENTITY_CONFLICT", `Agent ${id} must register with fabric ${this.rootId}`);
    assertCondition(taskId === undefined, "IDENTITY_CONFLICT", "A new agent cannot self-assign a task; use agent.spawn or task.claim");
    if (!parentId) {
      assertCondition(!this.rootAgentId || id === this.rootAgentId, "IDENTITY_CONFLICT", `Only root agent ${this.rootAgentId ?? "the configured root"} may register without a parent`);
      assertCondition(![...this.agents.values()].some((agent) => !agent.parentId), "IDENTITY_CONFLICT", "The fabric already has a root agent");
    }
    const parent = parentId ? this.requireAgent(parentId) : undefined;
    const depth = parent ? parent.depth + 1 : parseNumber(input.depth, "depth", 0);
    assertCondition(Number.isInteger(depth), "INVALID_ARGUMENT", "depth must be an integer");
    assertCondition(parent || depth === 0, "IDENTITY_CONFLICT", "A root agent must have depth 0");
    assertCondition(depth >= 0 && depth <= this.config.maxDepth, "AGENT_LIMIT_REACHED", `Agent depth ${depth} exceeds maxDepth ${this.config.maxDepth}`);
    assertCondition(this.activeAgentCount() < this.config.maxTotalAgents, "AGENT_LIMIT_REACHED", "The fabric has reached maxTotalAgents");
    if (parent) {
      assertCondition(parent.rootId === this.rootId, "IDENTITY_CONFLICT", "Parent belongs to another fabric");
      assertCondition(parent.childrenCreated < this.config.maxChildrenPerAgent, "AGENT_LIMIT_REACHED", `Agent ${parent.id} reached maxChildrenPerAgent`);
      assertCondition(depth === parent.depth + 1, "IDENTITY_CONFLICT", "Child depth must be parent depth plus one");
    }

    const initialStatus = input.initialStatus ?? "ready";
    assertCondition(initialStatus === "starting" || initialStatus === "ready", "INVALID_ARGUMENT", "initialStatus must be starting or ready");
    const record: AgentRecord = {
      id,
      rootId: this.rootId,
      parentId: parent?.id,
      depth,
      role: role ?? "agent",
      route,
      capabilities: mergeCapabilities(input.capabilities, parent?.capabilities),
      status: initialStatus,
      sessionId,
      workspace,
      createdAt: now,
      lastActivity: now,
      childrenCreated: 0,
      authToken: token ?? this.idFactory("token"),
      reconnectable: false,
    };
    this.agents.set(id, record);
    this.nextMessageSequence.set(id, 0);
    if (parent) {
      parent.childrenCreated += 1;
      parent.lastActivity = now;
      events.push({ type: "agent_updated", agent: cloneAgent(parent) });
    }
    events.push({ type: "agent_registered", agent: cloneAgent(record) });
    return { agent: publicAgent(record), token: record.authToken as string };
  }

  private spawnAgent(actorId: AgentId, input: SpawnAgentArgs, events: CoordinatorEvent[]): { agent: AgentRecord; token: string; taskId?: TaskId } {
    const parent = this.requireActor(actorId);
    assertCondition(parent.capabilities.maySpawn, "CAPABILITY_DENIED", `Agent ${actorId} cannot spawn children`);
    assertCondition(!isTerminal(parent.status), "LIFECYCLE_CONFLICT", `Agent ${actorId} is terminal`);
    assertCondition(parent.depth < this.config.maxDepth, "AGENT_LIMIT_REACHED", "Maximum recursion depth reached");
    assertCondition(parent.childrenCreated < this.config.maxChildrenPerAgent, "AGENT_LIMIT_REACHED", "Maximum child count reached");
    assertCondition(this.activeAgentCount() < this.config.maxTotalAgents, "AGENT_LIMIT_REACHED", "The fabric has reached maxTotalAgents");
    const requestedCapabilities = input.capabilities ?? {};
    const childId = this.idFactory("agent");
    const result = this.registerAgent(childId, {
      rootId: this.rootId,
      parentId: actorId,
      depth: parent.depth + 1,
      role: input.role,
      route: this.validateRoute(input.route),
      capabilities: requestedCapabilities,
      sessionId: input.sessionId,
      workspace: input.workspace,
      initialStatus: "starting",
    }, events);

    let taskId = input.taskId;
    if (input.taskDescription !== undefined) {
      taskId = this.createTask(actorId, {
        description: input.taskDescription,
        owner: result.agent.id,
        parentTaskId: undefined,
        dependencies: [],
      }, events).value.id as TaskId;
    } else if (taskId) {
      const task = this.requireTask(taskId);
      const currentOwner = task.owner ? this.requireAgent(task.owner) : undefined;
      assertCondition(!isTaskTerminal(task.status), "TASK_BUSY", `Task ${taskId} is already ${task.status}`);
      assertCondition(!currentOwner || isTerminal(currentOwner.status) || currentOwner.id === result.agent.id, "TASK_BUSY", `Task ${taskId} already has an owner`);
      assertCondition(this.taskDependenciesCompleted(task), "TASK_BLOCKED", `Task ${taskId} dependencies are not complete`);
      task.owner = result.agent.id;
      task.status = "active";
      task.updatedAt = this.clock();
      events.push({ type: "task_changed", task: cloneTask(task) });
    }

    const child = this.requireAgent(result.agent.id);
    if (taskId) {
      child.taskId = taskId;
      events.push({ type: "agent_updated", agent: cloneAgent(child) });
    }
    return { agent: publicAgent(child), token: result.token, taskId };
  }

  private configureChild(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): AgentRecord {
    const actor = this.requireActor(actorId);
    const target = this.requireAgent(parseString(args.agentId, "agentId"));
    assertCondition(actor.capabilities.maySpawn, "CAPABILITY_DENIED", `Agent ${actorId} cannot configure children`);
    assertCondition(this.canControl(actor, target) && target.parentId === actorId, "CAPABILITY_DENIED", `Agent ${actorId} cannot configure child ${target.id}`);
    const next = cloneAgent(target);
    if (args.sessionId !== undefined) next.sessionId = parseString(args.sessionId, "sessionId", 512);
    if (args.workspace !== undefined) next.workspace = parseWorkspace(args.workspace);
    next.lastActivity = this.clock();
    this.agents.set(target.id, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    return publicAgent(next);
  }

  private updateAgent(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): AgentRecord {
    const agent = this.requireAgent(actorId);
    const next = cloneAgent(agent);
    const requestedStatus = args.status as AgentStatus | undefined;
    if (requestedStatus === "running" && agent.status !== "running") {
      assertCondition(this.runningAgentCount() < this.config.maxConcurrentAgents, "AGENT_LIMIT_REACHED", "maxConcurrentAgents reached");
    }
    if (requestedStatus && isTerminal(requestedStatus)) {
      throw new FabricError("LIFECYCLE_CONFLICT", "Use agent.end_turn for terminal transitions so runtime claims are released");
    }
    if (requestedStatus) this.transitionStatus(next, requestedStatus, parseOptionalString(args.statusReason, "statusReason", 2048));
    assertCondition(args.taskId === undefined, "IDENTITY_CONFLICT", "Use task.claim or task.update to change task ownership");
    if (args.route !== undefined) next.route = this.validateRoute(args.route as ModelRoute);
    if (args.workspace !== undefined) next.workspace = parseWorkspace(args.workspace);
    next.lastActivity = this.clock();
    this.agents.set(actorId, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    return publicAgent(next);
  }

  private beginTurn(actorId: AgentId, events: CoordinatorEvent[]): { started: boolean; reason?: string } {
    const agent = this.requireAgent(actorId);
    if (agent.status === "running") throw new FabricError("LIFECYCLE_CONFLICT", `Agent ${actorId} is already running`);
    if (isTerminal(agent.status)) throw new FabricError("LIFECYCLE_CONFLICT", `Agent ${actorId} is terminal`);
    if (this.runningAgentCount() >= this.config.maxConcurrentAgents) {
      return { started: false, reason: "maxConcurrentAgents reached" };
    }
    const next = cloneAgent(agent);
    if (next.status === "starting") next.status = "ready";
    this.transitionStatus(next, "running");
    next.lastActivity = this.clock();
    this.agents.set(actorId, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    return { started: true };
  }

  private endTurn(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): { agent: AgentRecord; task?: TaskRecord } {
    const agent = this.requireAgent(actorId);
    const requested = (args.status as AgentStatus | undefined) ?? "ready";
    assertCondition(!isTerminal(agent.status) || requested === agent.status, "LIFECYCLE_CONFLICT", `Agent ${actorId} is already ${agent.status}`);
    if (isTerminal(agent.status)) return { agent: publicAgent(agent), task: agent.taskId ? cloneTask(this.requireTask(agent.taskId)) : undefined };

    // A model turn ending is not a task fact. When a worker has an assigned
    // task, deterministic task state controls whether its lifecycle may become
    // terminal; an uncompleted task can only leave the worker ready.
    const effectiveStatus = this.statusAfterTask(agent, requested);
    const next = cloneAgent(agent);
    this.transitionStatus(next, effectiveStatus, parseOptionalString(args.statusReason, "statusReason", 2048));
    if (isTerminal(effectiveStatus)) next.reconnectable = false;
    next.lastActivity = this.clock();
    this.agents.set(actorId, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });

    let task = agent.taskId ? cloneTask(this.requireTask(agent.taskId)) : undefined;
    if (isTerminal(effectiveStatus)) {
      this.cancelRequestsFor(actorId, effectiveStatus === "cancelled" ? "cancelled" : "failed", `Agent ${actorId} became ${effectiveStatus}`, events);
      // Every terminal state releases runtime claims. Successful task facts
      // remain durable, but a completed worker must not keep a lease alive.
      this.releaseAgentRuntime(actorId, effectiveStatus === "cancelled" ? "cancelled" : "released", events);
      if (effectiveStatus === "failed" && next.parentId) {
        this.sendInternalMessage(actorId, next.parentId, "agent_failed", next.statusReason ?? `Agent ${actorId} failed`, { failedAgentId: actorId }, events);
      }
      if (effectiveStatus !== "completed" && agent.taskId) {
        task = cloneTask(this.requireTask(agent.taskId));
      }
    }
    return { agent: publicAgent(this.requireAgent(actorId)), task };
  }

  private statusAfterTask(agent: AgentRecord, requested: AgentStatus): AgentStatus {
    if (!agent.taskId) return requested;
    const task = this.requireTask(agent.taskId);
    switch (task.status) {
      case "completed":
        return "completed";
      case "failed":
        return "failed";
      case "cancelled":
        return "cancelled";
      case "blocked":
        return "blocked";
      default:
        return requested === "completed" ? "ready" : requested;
    }
  }

  private heartbeat(actorId: AgentId, events: CoordinatorEvent[]): { agent: AgentRecord; leases: number } {
    const agent = this.requireAgent(actorId);
    const now = this.clock();
    const next = cloneAgent(agent);
    next.lastActivity = now;
    this.agents.set(actorId, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    let leases = 0;
    for (const resource of this.resources.values()) {
      let changed = false;
      for (const hold of resource.sharedHolds) {
        if (hold.agentId === actorId) {
          hold.lastHeartbeat = now;
          hold.expiresAt = now + hold.leaseMs;
          leases += 1;
          changed = true;
        }
      }
      if (resource.mutableHold?.agentId === actorId) {
        resource.mutableHold.lastHeartbeat = now;
        resource.mutableHold.expiresAt = now + resource.mutableHold.leaseMs;
        leases += 1;
        changed = true;
      }
      if (changed) {
        resource.updatedAt = now;
        events.push({ type: "resource_changed", resource: cloneResource(resource) });
      }
    }
    return { agent: publicAgent(next), leases };
  }

  private cancelAgent(actorId: AgentId, targetId: AgentId, events: CoordinatorEvent[]): { cancelled: AgentId[] } {
    const actor = this.requireActor(actorId);
    const target = this.requireAgent(targetId);
    assertCondition(actor.id === target.id || actor.capabilities.maySpawn, "CAPABILITY_DENIED", `Agent ${actorId} cannot cancel descendants`);
    assertCondition(this.canControl(actor, target), "CAPABILITY_DENIED", `Agent ${actorId} cannot cancel ${targetId}`);
    const cancelled: AgentId[] = [];
    const visit = (agent: AgentRecord): void => {
      for (const child of this.agents.values()) if (child.parentId === agent.id) visit(child);
      if (isTerminal(agent.status) && !agent.reconnectable) return;
      const next = cloneAgent(agent);
      next.status = "cancelled";
      next.reconnectable = false;
      next.statusReason = `Cancelled by ${actorId}`;
      next.lastActivity = this.clock();
      this.agents.set(next.id, next);
      events.push({ type: "agent_updated", agent: cloneAgent(next) });
      this.cancelRequestsFor(next.id, "cancelled", "Agent was cancelled", events);
      this.releaseAgentRuntime(next.id, "cancelled", events);
      cancelled.push(next.id);
    };
    visit(target);
    return { cancelled };
  }

  private getAgentStatus(actorId: AgentId, requestedId: string | undefined, scope: unknown): unknown {
    const target = requestedId ? this.requireAgent(requestedId) : this.requireActor(actorId);
    const actor = this.requireActor(actorId);
    assertCondition(actor.id === target.id || this.canControl(actor, target) || this.isVisiblePeer(actor, target), "MESSAGE_NOT_VISIBLE", `Agent ${actorId} cannot inspect ${target.id}`);
    if (scope === "tree" || scope === "children") return this.discoverAgents(actorId, scope);
    return publicAgent(target);
  }

  private discoverAgents(actorId: AgentId, scope: unknown): AgentSummary[] {
    const actor = this.requireActor(actorId);
    const selected = [...this.agents.values()].filter((candidate) => {
      if (candidate.id === actor.id) return true;
      switch (scope) {
        case "children":
          return candidate.parentId === actor.id;
        case "parent":
          return candidate.id === actor.parentId;
        case "siblings":
          return actor.capabilities.mayMessagePeers && Boolean(actor.parentId && candidate.parentId === actor.parentId);
        case "task":
          return actor.capabilities.mayMessagePeers && Boolean(actor.taskId && candidate.taskId === actor.taskId);
        case "all":
          return actor.depth === 0 || actor.capabilities.mayMessagePeers;
        default:
          return this.canControl(actor, candidate) || this.isVisiblePeer(actor, candidate);
      }
    });
    return selected.map((candidate) => this.toSummary(candidate));
  }

  private sendMessage(actorId: AgentId, input: MessageSendArgs, events: CoordinatorEvent[]): { message: AgentMessage; request?: RequestRecord } {
    const to = parseString(input.to, "to");
    const type = input.type;
    assertCondition(ALL_MESSAGE_TYPES.has(type), "INVALID_ARGUMENT", `Unknown message type: ${String(type)}`);
    assertCondition(!INTERNAL_MESSAGE_TYPES.has(type), "CAPABILITY_DENIED", `Message type ${type} is broker-generated`);
    const body = parseString(input.body, "body", this.config.maxMessageBody);
    const recipient = this.requireAgent(to);
    const sender = this.requireActor(actorId);
    if (type === "escalation") assertCondition(sender.capabilities.mayEscalate, "CAPABILITY_DENIED", `Agent ${actorId} cannot escalate`);
    assertCondition(!isTerminal(sender.status), "LIFECYCLE_CONFLICT", `Agent ${actorId} is terminal`);
    assertCondition(!isTerminal(recipient.status), "AGENT_NOT_FOUND", `Recipient ${to} is not active`);
    assertCondition(this.canMessage(sender, recipient), "CAPABILITY_DENIED", `Agent ${actorId} cannot message ${to}`);
    return this.recordMessage(sender, recipient, type, body, {
      priority: input.priority,
      expectsReply: input.expectsReply,
      clientDedupeKey: input.clientDedupeKey === undefined ? undefined : parseString(input.clientDedupeKey, "clientDedupeKey", 512),
      metadata: input.metadata,
    }, events);
  }

  private replyToRequest(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): { message: AgentMessage; request: RequestRecord } {
    const requestId = parseString(args.requestId, "requestId");
    const request = this.requests.get(requestId);
    assertCondition(request, "REQUEST_NOT_FOUND", `Request ${requestId} was not found`);
    assertCondition(request.status === "pending", "REQUEST_ALREADY_RESOLVED", `Request ${requestId} is already ${request.status}`);
    assertCondition(request.to === actorId, "MESSAGE_NOT_VISIBLE", `Agent ${actorId} cannot answer request ${requestId}`);
    const body = parseString(args.body, "body", this.config.maxMessageBody);
    const recipient = this.requireAgent(request.from);
    const result = this.recordMessage(this.requireActor(actorId), recipient, "response", body, {
      priority: "urgent",
      requestId,
      replyTo: request.messageId,
      metadata: args.metadata as Record<string, unknown> | undefined,
      expectsReply: false,
    }, events);
    const resolved: RequestRecord = { ...request, status: "resolved", resolvedAt: this.clock(), responseMessageId: result.message.id };
    this.requests.set(requestId, resolved);
    events.push({ type: "request_changed", request: cloneRequest(resolved) });
    return { message: result.message, request: cloneRequest(resolved) };
  }

  private ackMessage(actorId: AgentId, messageId: string, events: CoordinatorEvent[]): AgentMessage {
    const message = this.messages.get(messageId);
    assertCondition(message, "MESSAGE_NOT_FOUND", `Message ${messageId} was not found`);
    assertCondition(message.to === actorId, "MESSAGE_NOT_VISIBLE", `Agent ${actorId} cannot acknowledge ${messageId}`);
    if (message.acknowledgedAt !== undefined) return cloneMessage(message);
    message.acknowledgedAt = this.clock();
    if (message.deliveredAt === undefined) message.deliveredAt = message.acknowledgedAt;
    events.push({ type: "message_acknowledged", message: cloneMessage(message) });
    this.pruneMessages(events);
    return cloneMessage(message);
  }

  private inbox(actorId: AgentId, limit: unknown): AgentMessage[] {
    const max = Math.max(1, Math.min(100, Math.floor(parseNumber(limit, "limit", 50))));
    return [...this.messages.values()]
      .filter((message) => message.to === actorId && message.acknowledgedAt === undefined)
      .sort((left, right) => left.senderSequence - right.senderSequence || (left.brokerSequence ?? 0) - (right.brokerSequence ?? 0) || left.id.localeCompare(right.id))
      .slice(0, max)
      .map(cloneMessage);
  }

  private listMessages(actorId: AgentId, args: Record<string, unknown>): AgentMessage[] {
    const actor = this.requireActor(actorId);
    const all = actor.depth === 0 && args.scope === "all";
    return [...this.messages.values()]
      // Workers may inspect only their own conversations. A root may request
      // the separate, explicitly privileged audit projection.
      .filter((message) => all || message.from === actorId || message.to === actorId)
      .sort((left, right) => (right.brokerSequence ?? 0) - (left.brokerSequence ?? 0) || right.createdAt - left.createdAt || right.id.localeCompare(left.id))
      .slice(0, 100)
      .map(cloneMessage);
  }

  private createTask(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): DispatchResult<TaskRecord> {
    const actor = this.requireActor(actorId);
    const description = parseString(args.description, "description", 16 * 1024);
    if (args.dependencies !== undefined) assertCondition(Array.isArray(args.dependencies), "INVALID_ARGUMENT", "dependencies must be an array");
    const dependencies = (args.dependencies as unknown[] | undefined)?.map((id) => parseString(id, "dependency")) ?? [];
    for (const dependency of dependencies) {
      assertCondition(this.tasks.has(dependency), "TASK_NOT_FOUND", `Dependency ${dependency} was not found`);
    }
    const parentTaskId = parseOptionalString(args.parentTaskId, "parentTaskId");
    if (parentTaskId) assertCondition(this.tasks.has(parentTaskId), "TASK_NOT_FOUND", `Parent task ${parentTaskId} was not found`);
    const owner = parseOptionalString(args.owner, "owner");
    if (owner) {
      const ownerAgent = this.requireAgent(owner);
      assertCondition(!isTerminal(ownerAgent.status), "AGENT_NOT_FOUND", `Agent ${owner} is not active`);
      assertCondition(this.canControl(actor, ownerAgent) || owner === actorId, "CAPABILITY_DENIED", `Agent ${actorId} cannot assign a task to ${owner}`);
      assertCondition(!ownerAgent.taskId || ownerAgent.taskId === undefined, "TASK_BUSY", `Agent ${owner} already has a primary task`);
    }
    const id = this.idFactory("task");
    const now = this.clock();
    const ready = dependencies.every((dependency) => this.tasks.get(dependency)?.status === "completed");
    const task: TaskRecord = {
      id,
      description,
      owner,
      creator: actorId,
      parentTaskId,
      dependencies,
      status: owner ? (ready ? "active" : "pending") : (ready ? "ready" : "pending"),
      createdAt: now,
      updatedAt: now,
    };
    this.tasks.set(id, task);
    if (owner) {
      const target = this.requireAgent(owner);
      target.taskId = id;
      target.lastActivity = now;
      events.push({ type: "agent_updated", agent: cloneAgent(target) });
    }
    events.push({ type: "task_changed", task: cloneTask(task) });
    return { value: cloneTask(task), events };
  }

  private claimTask(actorId: AgentId, taskId: TaskId, events: CoordinatorEvent[]): TaskRecord {
    const actor = this.requireActor(actorId);
    const task = this.requireTask(taskId);
    if (task.owner && isTerminal(this.requireAgent(task.owner).status)) {
      task.owner = undefined;
      task.status = "ready";
      task.updatedAt = this.clock();
    }
    assertCondition(!task.owner || task.owner === actorId, "TASK_BUSY", `Task ${taskId} is owned by ${task.owner}`);
    assertCondition(!actor.taskId || actor.taskId === taskId, "TASK_BUSY", `Agent ${actorId} already has primary task ${actor.taskId}`);
    assertCondition(!isTaskTerminal(task.status), "TASK_BUSY", `Task ${taskId} is already ${task.status}`);
    assertCondition(this.taskDependenciesCompleted(task), "TASK_BLOCKED", `Task ${taskId} dependencies are not complete`);
    task.owner = actorId;
    task.status = "active";
    task.updatedAt = this.clock();
    if (!actor.taskId) actor.taskId = taskId;
    events.push({ type: "agent_updated", agent: cloneAgent(actor) });
    events.push({ type: "task_changed", task: cloneTask(task) });
    return cloneTask(task);
  }

  private updateTask(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): TaskRecord {
    const task = this.requireTask(parseString(args.taskId, "taskId"));
    const actor = this.requireActor(actorId);
    assertCondition(this.canControlTask(actor, task), "TASK_NOT_OWNER", `Agent ${actorId} cannot update task ${task.id}`);
    const action = parseString(args.action, "action", 64);
    switch (action) {
      case "complete":
        return this.completeTask(actorId, task, args.result, events);
      case "block":
        task.status = "blocked";
        task.blockedReason = parseString(args.reason, "reason", 4096);
        task.updatedAt = this.clock();
        events.push({ type: "task_changed", task: cloneTask(task) });
        return cloneTask(task);
      case "ready":
      case "reopen":
        assertCondition(!isTaskTerminal(task.status) || action === "reopen", "LIFECYCLE_CONFLICT", `Task ${task.id} cannot be reopened from ${task.status}`);
        assertCondition(this.taskDependenciesCompleted(task), "TASK_BLOCKED", `Task ${task.id} dependencies are not complete`);
        task.status = task.owner ? "active" : "ready";
        task.blockedReason = undefined;
        task.updatedAt = this.clock();
        events.push({ type: "task_changed", task: cloneTask(task) });
        return cloneTask(task);
      case "cancel":
        if (task.owner) {
          const owner = this.agents.get(task.owner);
          if (owner?.taskId === task.id) owner.taskId = undefined;
          if (owner) events.push({ type: "agent_updated", agent: cloneAgent(owner) });
        }
        task.owner = undefined;
        task.status = "cancelled";
        task.updatedAt = this.clock();
        events.push({ type: "task_changed", task: cloneTask(task) });
        return cloneTask(task);
      case "fail":
        task.status = "failed";
        task.blockedReason = parseOptionalString(args.reason, "reason", 4096);
        task.updatedAt = this.clock();
        events.push({ type: "task_changed", task: cloneTask(task) });
        return cloneTask(task);
      default:
        throw new FabricError("INVALID_ARGUMENT", `Unknown task action ${action}`);
    }
  }

  private completeTask(actorId: AgentId, task: TaskRecord, rawResult: unknown, events: CoordinatorEvent[]): TaskRecord {
    if (task.status === "completed") return cloneTask(task);
    assertCondition(!isTaskTerminal(task.status), "LIFECYCLE_CONFLICT", `Task ${task.id} is already ${task.status}`);
    const resultObject = rawResult && typeof rawResult === "object" ? rawResult as Record<string, unknown> : {};
    const summary = parseString(resultObject.summary ?? "completed", "result.summary", this.config.maxTaskOutput);
    const output = resultObject.output === undefined ? undefined : parseString(resultObject.output, "result.output", this.config.maxTaskOutput);
    task.status = "completed";
    task.result = { summary, output, completedAt: this.clock(), by: actorId };
    task.blockedReason = undefined;
    task.updatedAt = this.clock();
    events.push({ type: "task_changed", task: cloneTask(task) });
    this.refreshTaskReadiness(events);
    const ownerAgent = task.owner ? this.agents.get(task.owner) : undefined;
    const notify = ownerAgent?.parentId ?? (task.creator !== actorId ? task.creator : undefined);
    if (notify && this.agents.has(notify)) {
      this.sendInternalMessage(actorId, notify, "task_result", summary, { taskId: task.id, output }, events);
    }
    return cloneTask(task);
  }

  private refreshTaskReadiness(events: CoordinatorEvent[]): void {
    for (const task of this.tasks.values()) {
      if (task.status === "pending" && this.taskDependenciesCompleted(task)) {
        task.status = task.owner ? "active" : "ready";
        task.updatedAt = this.clock();
        events.push({ type: "task_changed", task: cloneTask(task) });
      }
    }
  }

  private listTasks(actorId: AgentId, args: Record<string, unknown>): TaskRecord[] {
    const actor = this.requireActor(actorId);
    const includeAll = actor.depth === 0 && args.scope === "all";
    return [...this.tasks.values()]
      .filter((task) => includeAll || task.owner === actorId || task.creator === actorId || (task.parentTaskId && this.taskVisibleTo(actorId, task)))
      .sort((left, right) => left.createdAt - right.createdAt)
      .map(cloneTask);
  }

  private showTask(actorId: AgentId, taskId: TaskId): TaskRecord {
    const task = this.requireTask(taskId);
    assertCondition(this.listTasks(actorId, { scope: "all" }).some((candidate) => candidate.id === taskId), "MESSAGE_NOT_VISIBLE", `Task ${taskId} is not visible to ${actorId}`);
    return cloneTask(task);
  }

  private defineResource(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): ResourceRecord {
    const actor = this.requireActor(actorId);
    assertCondition(actor.capabilities.mayWriteRepo || actor.capabilities.mayTransferOwnership, "CAPABILITY_DENIED", `Agent ${actorId} cannot define resources`);
    const id = parseString(args.resourceId, "resourceId", 1024);
    const kind = parseString(args.kind ?? "resource", "kind", 128);
    const parentId = parseOptionalString(args.parentId, "parentId", 1024);
    const resourcePath = args.path === undefined ? undefined : normalizeResourcePath(args.path);
    const existing = this.resources.get(id);
    if (existing) {
      assertCondition(existing.kind === kind && existing.parentId === parentId && (args.path === undefined || existing.path === resourcePath), "IDENTITY_CONFLICT", `Resource ${id} already has a different definition`);
      return cloneResource(existing);
    }
    if (parentId) {
      assertCondition(parentId !== id && this.resources.has(parentId), "RESOURCE_NOT_FOUND", `Parent resource ${parentId} was not found`);
      assertCondition(!this.wouldCreateResourceCycle(id, parentId), "INVALID_ARGUMENT", `Resource ${id} would create a hierarchy cycle`);
    }
    const permissions = this.parsePermissions(args.permissions);
    const now = this.clock();
    const resource: ResourceRecord = {
      id,
      kind,
      parentId,
      path: resourcePath,
      owner: actorId,
      version: 1,
      grants: { [actorId]: permissions.length ? permissions : ["read", "comment", "write", "test"] },
      sharedHolds: [],
      waiters: [],
      createdAt: now,
      updatedAt: now,
    };
    this.resources.set(id, resource);
    events.push({ type: "resource_changed", resource: cloneResource(resource) });
    return cloneResource(resource);
  }

  private inspectResource(actorId: AgentId, resourceId: ResourceId, version: unknown): ResourceRecord & { stale?: boolean } {
    const resource = this.requireResource(resourceId);
    assertCondition(this.canInspectResource(actorId, resource), "CAPABILITY_DENIED", `Agent ${actorId} cannot inspect ${resourceId}`);
    const requestedVersion = version === undefined ? undefined : parseNumber(version, "version", resource.version);
    return { ...cloneResource(resource), stale: requestedVersion !== undefined && requestedVersion !== resource.version };
  }

  private resourceSnapshot(actorId: AgentId, resourceId: ResourceId): { resourceId: ResourceId; version: number; token: string } {
    const resource = this.requireResource(resourceId);
    assertCondition(this.canInspectResource(actorId, resource), "CAPABILITY_DENIED", `Agent ${actorId} cannot inspect ${resourceId}`);
    return { resourceId, version: resource.version, token: `${resourceId}@${resource.version}` };
  }

  private listResources(actorId: AgentId): ResourceRecord[] {
    return [...this.resources.values()].filter((resource) => this.canInspectResource(actorId, resource)).map(cloneResource);
  }

  private grantResource(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): ResourceRecord {
    const resource = this.requireResource(parseString(args.resourceId, "resourceId"));
    const actor = this.requireActor(actorId);
    assertCondition(this.canManageResource(actor, resource), "CAPABILITY_DENIED", `Agent ${actorId} cannot grant ${resource.id}`);
    const targetId = parseString(args.agentId, "agentId");
    const target = this.requireAgent(targetId);
    assertCondition(!isTerminal(target.status), "AGENT_NOT_FOUND", `Agent ${targetId} is not active`);
    const permissions = this.parsePermissions(args.permissions);
    assertCondition(permissions.length > 0, "INVALID_ARGUMENT", "permissions must contain at least one known permission");
    resource.grants[targetId] = [...new Set(permissions)];
    resource.updatedAt = this.clock();
    events.push({ type: "resource_changed", resource: cloneResource(resource) });
    return cloneResource(resource);
  }

  private claimResource(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): ResourceRecord {
    const resource = this.requireResource(parseString(args.resourceId, "resourceId"));
    const actor = this.requireActor(actorId);
    assertCondition(this.hasPermission(resource, actorId, "write") || actor.capabilities.mayTransferOwnership, "CAPABILITY_DENIED", `Agent ${actorId} cannot claim ${resource.id}`);
    if (resource.owner && resource.owner !== actorId) {
      const owner = this.requireAgent(resource.owner);
      assertCondition(this.canControl(actor, owner) && actor.capabilities.mayTransferOwnership, "RESOURCE_NOT_OWNER", `Resource ${resource.id} is owned by ${resource.owner}`);
    }
    for (const overlap of this.overlappingResources(resource.id)) {
      if (overlap.owner && overlap.owner !== actorId) {
        const owner = this.requireAgent(overlap.owner);
        assertCondition(this.canControl(actor, owner) && actor.capabilities.mayTransferOwnership, "RESOURCE_NOT_OWNER", `Overlapping resource ${overlap.id} is owned by ${overlap.owner}`);
      }
    }
    resource.owner = actorId;
    resource.grants[actorId] = [...new Set([...(resource.grants[actorId] ?? []), "read", "comment", "write", "test"] as ResourcePermission[])];
    resource.version += 1;
    resource.updatedAt = this.clock();
    events.push({ type: "resource_changed", resource: cloneResource(resource) });
    return cloneResource(resource);
  }

  private borrowResource(actorId: AgentId, input: ResourceBorrowArgs, events: CoordinatorEvent[]): { status: "granted" | "waiting"; leaseId?: string; requestId?: RequestId; resource: ResourceRecord } {
    const resource = this.requireResource(parseString(input.resourceId, "resourceId"));
    const actor = this.requireActor(actorId);
    const mode = input.mode;
    assertCondition(mode === "shared" || mode === "mutable", "INVALID_ARGUMENT", "mode must be shared or mutable");
    assertCondition(this.hasPermission(resource, actorId, mode === "mutable" ? "write" : "read"), "CAPABILITY_DENIED", `Agent ${actorId} has no ${mode} permission for ${resource.id}`);
    const leaseMs = Math.max(1000, Math.min(24 * 60 * 60 * 1000, Math.floor(parseNumber(input.leaseMs, "leaseMs", this.config.leaseMs))));
    const existing = this.findHold(actorId, resource.id, mode);
    if (existing) return { status: "granted", leaseId: existing.leaseId, resource: cloneResource(resource) };
    const queuedAhead = this.hasQueuedWaiterAhead(resource, actorId);
    if (queuedAhead || !this.canAcquire(resource, actorId, mode)) {
      if (!input.wait) throw new FabricError("RESOURCE_CONFLICT", `Resource ${resource.id} is busy or has an earlier waiter`, { resourceId: resource.id, mode });
      const currentWaiter = resource.waiters.find((waiter) => waiter.agentId === actorId && waiter.mode === mode);
      if (currentWaiter) return { status: "waiting", requestId: currentWaiter.requestId, resource: cloneResource(resource) };
      const waiter: ResourceWaiter = {
        requestId: this.idFactory("resource-request"),
        enqueuedSequence: ++this.nextResourceWaiterSequence,
        agentId: actorId,
        mode,
        enqueuedAt: this.clock(),
        leaseMs,
      };
      resource.waiters.push(waiter);
      resource.updatedAt = this.clock();
      events.push({ type: "resource_changed", resource: cloneResource(resource) });
      return { status: "waiting", requestId: waiter.requestId, resource: cloneResource(resource) };
    }
    const hold = this.addHold(resource, actorId, mode, leaseMs);
    events.push({ type: "resource_changed", resource: cloneResource(resource) });
    return { status: "granted", leaseId: hold.leaseId, resource: cloneResource(resource) };
  }

  private transferResource(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): ResourceRecord {
    const resource = this.requireResource(parseString(args.resourceId, "resourceId"));
    const actor = this.requireActor(actorId);
    const targetId = parseString(args.agentId, "agentId");
    const target = this.requireAgent(targetId);
    assertCondition(!isTerminal(target.status), "AGENT_NOT_FOUND", `Agent ${targetId} is not active`);
    const owner = resource.owner ? this.requireAgent(resource.owner) : undefined;
    assertCondition(Boolean(owner && actor.capabilities.mayTransferOwnership && (owner.id === actorId || this.canControl(actor, owner))), "RESOURCE_NOT_OWNER", `Agent ${actorId} cannot transfer ${resource.id}`);
    for (const overlap of this.overlappingResources(resource.id)) {
      const holds = [...overlap.sharedHolds, ...(overlap.mutableHold ? [overlap.mutableHold] : [])];
      assertCondition(holds.length === 0, "RESOURCE_CONFLICT", `Cannot transfer ${resource.id} while ${overlap.id} is held`);
    }
    resource.owner = targetId;
    resource.grants[targetId] = [...new Set([...(resource.grants[targetId] ?? []), "read", "comment", "write", "test"] as ResourcePermission[])];
    resource.version += 1;
    resource.updatedAt = this.clock();
    events.push({ type: "resource_changed", resource: cloneResource(resource) });
    return cloneResource(resource);
  }

  private releaseResource(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): { released: boolean; resourceId?: string; leaseId?: string } {
    const actor = this.requireActor(actorId);
    const resourceId = parseOptionalString(args.resourceId, "resourceId");
    const leaseId = parseOptionalString(args.leaseId, "leaseId");
    let released = false;
    let releasedResourceId: string | undefined;
    let releasedLeaseId: string | undefined;
    for (const resource of this.resources.values()) {
      if (resourceId && resource.id !== resourceId) continue;
      const shared = resource.sharedHolds.filter((hold) => hold.agentId === actor.id && (!leaseId || hold.leaseId === leaseId));
      const removeMutable = Boolean(resource.mutableHold && resource.mutableHold.agentId === actor.id && (!leaseId || resource.mutableHold.leaseId === leaseId));
      if (shared.length === 0 && !removeMutable) continue;
      resource.sharedHolds = resource.sharedHolds.filter((hold) => !shared.some((candidate) => candidate.leaseId === hold.leaseId));
      if (removeMutable && resource.mutableHold) {
        releasedLeaseId ??= resource.mutableHold.leaseId;
        resource.mutableHold = undefined;
        resource.version += 1;
      }
      released = true;
      releasedResourceId ??= resource.id;
      releasedLeaseId ??= shared[0]?.leaseId;
      resource.updatedAt = this.clock();
      events.push({ type: "resource_changed", resource: cloneResource(resource) });
    }
    if (released) this.drainWaiters(events);
    return { released, resourceId: releasedResourceId, leaseId: releasedLeaseId };
  }

  private checkWrite(actorId: AgentId, args: Record<string, unknown>): { allowed: boolean; reason?: string; resourceId?: string } {
    const actor = this.requireActor(actorId);
    if (!actor.capabilities.mayWriteRepo) {
      return { allowed: false, reason: `Agent ${actorId} is not allowed to write repository files` };
    }
    const resourceId = parseOptionalString(args.resourceId, "resourceId");
    const requestedPath = args.path === undefined ? undefined : normalizeResourcePath(args.path);
    assertCondition(resourceId || requestedPath, "INVALID_ARGUMENT", "resource.check_write requires resourceId or path");

    const selectedResource = resourceId ? this.requireResource(resourceId) : undefined;
    const candidates = requestedPath
      ? this.resourcesForPath(requestedPath)
      : selectedResource ? [selectedResource] : [];
    if (selectedResource && requestedPath && !candidates.some((candidate) => candidate.id === selectedResource.id)) {
      return { allowed: false, resourceId: selectedResource.id, reason: `Resource ${selectedResource.id} does not declare ${requestedPath}` };
    }
    if (candidates.length === 0) {
      return { allowed: false, reason: `No declared resource matches ${requestedPath}` };
    }
    const conflicting = requestedPath === undefined ? undefined : candidates.find((resource) => resource.sharedHolds.length > 0 || resource.mutableHold && resource.mutableHold.agentId !== actorId);
    if (conflicting) {
      return { allowed: false, resourceId: conflicting.id, reason: `A conflicting runtime hold prevents writing ${requestedPath}` };
    }
    const allowed = candidates.find((resource) => this.hasMutableHold(resource, actorId));
    if (allowed) return { allowed: true, resourceId: allowed.id };
    return {
      allowed: false,
      resourceId: candidates[0].id,
      reason: `Agent ${actorId} does not hold mutable access to ${candidates[0].id}`,
    };
  }

  private status(actorId: AgentId, args: Record<string, unknown>): FabricStatus {
    const actor = this.requireActor(actorId);
    assertCondition(actor.depth === 0, "CAPABILITY_DENIED", "Only a fabric root may request full status");
    const allMessages = [...this.messages.values()].sort((left, right) => (right.brokerSequence ?? 0) - (left.brokerSequence ?? 0) || right.createdAt - left.createdAt).slice(0, 100).map(cloneMessage);
    return {
      rootId: this.rootId,
      agents: [...this.agents.values()].map((candidate) => this.toSummary(candidate)),
      tasks: [...this.tasks.values()].map(cloneTask),
      resources: [...this.resources.values()].map(cloneResource),
      pendingRequests: [...this.requests.values()].filter((request) => request.status === "pending").map(cloneRequest),
      recentMessages: allMessages,
      runningChildren: this.runningAgentCount(),
      config: cloneConfig(this.config),
    };
  }

  /** Run the time-based maintenance transition without requiring an actor request. */
  maintenance(): DispatchResult<null> {
    const events: CoordinatorEvent[] = [];
    this.reclaimExpired(this.clock(), events);
    return { value: null, events };
  }

  /** Mark non-terminal runtime actors stale after a broker restart and release their leases. */
  recover(): DispatchResult<{ recovered: AgentId[] }> {
    const events: CoordinatorEvent[] = [];
    const recovered: AgentId[] = [];
    for (const agent of [...this.agents.values()]) {
      if (!ACTIVE_STATUSES.has(agent.status)) continue;
      const next = cloneAgent(agent);
      next.status = "failed";
      next.statusReason = "Broker restarted before the agent reconnected";
      next.reconnectable = true;
      next.lastActivity = this.clock();
      this.agents.set(next.id, next);
      events.push({ type: "agent_updated", agent: cloneAgent(next) });
      // A restart only proves that transport liveness was interrupted. Keep
      // semantic requests pending until an actor explicitly resolves or fails
      // them; a reconnecting host may still be waiting on the reply.
      this.releaseAgentRuntime(next.id, "broker-recovery", events);
      recovered.push(next.id);
    }
    return { value: { recovered }, events };
  }

  getAgent(agentId: AgentId): Omit<AgentRecord, "authToken"> | undefined {
    const agent = this.agents.get(agentId);
    if (!agent) return undefined;
    const { authToken: _authToken, ...publicAgent } = cloneAgent(agent);
    return publicAgent;
  }

  authenticate(agentId: AgentId, token?: string): boolean {
    const agent = this.agents.get(agentId);
    if (!agent) return true;
    return Boolean(agent.authToken && token !== undefined && token === agent.authToken);
  }

  exportState(): PersistedCoordinatorState {
    return {
      version: 1,
      nextMessageSequence: Object.fromEntries(this.nextMessageSequence),
      agents: [...this.agents.values()].map(cloneAgent),
      tasks: [...this.tasks.values()].map(cloneTask),
      resources: [...this.resources.values()].map(cloneResource),
      messages: [...this.messages.values()].map(cloneMessage),
      requests: [...this.requests.values()].map(cloneRequest),
      dedupe: [...this.dedupe.entries()],
      nextBrokerSequence: this.nextBrokerSequence,
      nextResourceWaiterSequence: this.nextResourceWaiterSequence,
    };
  }

  restoreState(state: PersistedCoordinatorState): void {
    assertCondition(state.version === 1, "PROTOCOL_VERSION_UNSUPPORTED", `Unsupported coordinator state version ${state.version}`);
    this.agents.clear();
    this.tasks.clear();
    this.resources.clear();
    this.messages.clear();
    this.requests.clear();
    this.dedupe.clear();
    this.nextMessageSequence.clear();
    this.nextBrokerSequence = state.nextBrokerSequence ?? 0;
    this.nextResourceWaiterSequence = state.nextResourceWaiterSequence ?? 0;
    for (const agent of state.agents) {
      assertCondition(agent.rootId === this.rootId, "IDENTITY_CONFLICT", `Persisted agent ${agent.id} belongs to another fabric`);
      this.agents.set(agent.id, cloneAgent(agent));
    }
    for (const task of state.tasks) this.tasks.set(task.id, cloneTask(task));
    for (const resource of state.resources) {
      const next = cloneResource(resource);
      for (const waiter of next.waiters) this.nextResourceWaiterSequence = Math.max(this.nextResourceWaiterSequence, waiter.enqueuedSequence ?? 0);
      this.resources.set(next.id, next);
    }
    for (const message of state.messages) {
      const next = cloneMessage(message);
      if (next.brokerSequence !== undefined) this.nextBrokerSequence = Math.max(this.nextBrokerSequence, next.brokerSequence);
      this.messages.set(next.id, next);
    }
    for (const request of state.requests) this.requests.set(request.id, cloneRequest(request));
    for (const [key, value] of state.dedupe) this.dedupe.set(key, value);
    for (const [agentId, sequence] of Object.entries(state.nextMessageSequence)) this.nextMessageSequence.set(agentId, sequence);
  }

  applyEvents(events: readonly CoordinatorEvent[]): void {
    for (const event of events) {
      switch (event.type) {
        case "agent_registered":
        case "agent_updated":
        case "agent_terminal":
          this.agents.set(event.agent.id, cloneAgent(event.agent));
          if (!this.nextMessageSequence.has(event.agent.id)) this.nextMessageSequence.set(event.agent.id, 0);
          break;
        case "task_changed":
          this.tasks.set(event.task.id, cloneTask(event.task));
          break;
        case "resource_changed": {
          const resource = cloneResource(event.resource);
          for (const waiter of resource.waiters) this.nextResourceWaiterSequence = Math.max(this.nextResourceWaiterSequence, waiter.enqueuedSequence ?? 0);
          this.resources.set(resource.id, resource);
          break;
        }
        case "message_sent": {
          const message = cloneMessage(event.message);
          // Journals from before brokerSequence was introduced are upgraded in
          // replay order, preserving their committed event order.
          message.brokerSequence ??= ++this.nextBrokerSequence;
          this.nextBrokerSequence = Math.max(this.nextBrokerSequence, message.brokerSequence);
          this.messages.set(message.id, message);
          this.nextMessageSequence.set(message.from, Math.max(this.nextMessageSequence.get(message.from) ?? 0, message.senderSequence));
          if (message.clientDedupeKey) this.dedupe.set(`${message.from}\u0000${message.clientDedupeKey}`, message.id);
          if (event.request) this.requests.set(event.request.id, cloneRequest(event.request));
          break;
        }
        case "message_acknowledged":
          this.messages.set(event.message.id, cloneMessage(event.message));
          break;
        case "messages_pruned":
          for (const id of event.ids) {
            const message = this.messages.get(id);
            this.messages.delete(id);
            if (message?.clientDedupeKey) this.dedupe.delete(`${message.from}\u0000${message.clientDedupeKey}`);
          }
          break;
        case "request_changed":
          this.requests.set(event.request.id, cloneRequest(event.request));
          break;
        case "slot_available":
        case "diagnostic":
          break;
      }
    }
  }

  private withEvents<T>(initial: CoordinatorEvent[], result: T | DispatchResult<T>, _events: CoordinatorEvent[] = initial): DispatchResult<T> {
    if (isDispatchResult(result)) {
      return { value: result.value, events: result.events };
    }
    return { value: result as T, events: initial };
  }

  private requireBoundAgent(actorId: AgentId | undefined): AgentRecord {
    assertCondition(actorId, "IDENTITY_CONFLICT", "A bound actor identity is required");
    return this.requireAgent(actorId);
  }

  private requireActor(actorId: AgentId | undefined): AgentRecord {
    const actor = this.requireBoundAgent(actorId);
    assertCondition(!isTerminal(actor.status), "LIFECYCLE_CONFLICT", `Agent ${actorId} is terminal`);
    return actor;
  }

  private requireAgent(agentId: AgentId): AgentRecord {
    const id = parseAgentId(agentId);
    const agent = this.agents.get(id);
    assertCondition(agent, "AGENT_NOT_FOUND", `Agent ${id} was not found`);
    return agent;
  }

  private requireTask(taskId: TaskId): TaskRecord {
    const task = this.tasks.get(taskId);
    assertCondition(task, "TASK_NOT_FOUND", `Task ${taskId} was not found`);
    return task;
  }

  private requireResource(resourceId: ResourceId): ResourceRecord {
    const resource = this.resources.get(resourceId);
    assertCondition(resource, "RESOURCE_NOT_FOUND", `Resource ${resourceId} was not found`);
    return resource;
  }

  private validateRoute(route: ModelRoute): ModelRoute {
    assertCondition(route && typeof route === "object", "MODEL_ROUTE_INVALID", "A model route is required");
    const provider = parseString(route.provider, "route.provider", 256);
    const model = parseString(route.model, "route.model", 512);
    const thinking = parseString(route.thinking, "route.thinking", 32) as ModelRoute["thinking"];
    assertCondition(ALL_THINKING_LEVELS.has(thinking), "MODEL_ROUTE_INVALID", `Unknown thinking level ${thinking}`);
    return { provider, model, thinking };
  }

  private transitionStatus(agent: AgentRecord, next: AgentStatus, reason?: string): void {
    if (agent.status === next) {
      agent.statusReason = reason;
      return;
    }
    if (isTerminal(agent.status)) throw new FabricError("LIFECYCLE_CONFLICT", `Agent ${agent.id} is already ${agent.status}`);
    const valid: Record<AgentStatus, AgentStatus[]> = {
      starting: ["ready", "failed", "cancelled"],
      ready: ["running", "waiting", "blocked", "completed", "failed", "cancelled"],
      running: ["ready", "waiting", "blocked", "completed", "failed", "cancelled"],
      waiting: ["ready", "running", "blocked", "completed", "failed", "cancelled"],
      blocked: ["ready", "running", "waiting", "completed", "failed", "cancelled"],
      completed: [],
      failed: [],
      cancelled: [],
    };
    assertCondition(valid[agent.status].includes(next), "LIFECYCLE_CONFLICT", `Invalid agent transition ${agent.status} -> ${next}`);
    agent.status = next;
    agent.statusReason = reason;
  }

  private recordMessage(
    sender: AgentRecord,
    recipient: AgentRecord,
    type: MessageType,
    body: string,
    options: {
      priority?: MessagePriority;
      expectsReply?: boolean;
      requestId?: RequestId;
      replyTo?: string;
      clientDedupeKey?: string;
      metadata?: Record<string, unknown>;
    },
    events: CoordinatorEvent[],
  ): { message: AgentMessage; request?: RequestRecord } {
    const dedupeKey = options.clientDedupeKey ? `${sender.id}\u0000${options.clientDedupeKey}` : undefined;
    if (dedupeKey) {
      const existingId = this.dedupe.get(dedupeKey);
      if (existingId) {
        const existing = this.messages.get(existingId);
        if (existing) return { message: cloneMessage(existing), request: existing.requestId ? this.requests.get(existing.requestId) && cloneRequest(this.requests.get(existing.requestId) as RequestRecord) : undefined };
      }
    }
    const pendingCount = [...this.messages.values()].filter((message) => message.to === recipient.id && message.acknowledgedAt === undefined).length;
    assertCondition(pendingCount < this.config.maxMailboxMessages, "MAILBOX_FULL", `Mailbox for ${recipient.id} is full`, { recipient: recipient.id });
    const now = this.clock();
    const priority = options.priority ?? "normal";
    assertCondition(priority === "normal" || priority === "urgent", "INVALID_ARGUMENT", "priority must be normal or urgent");
    assertCondition(options.expectsReply === undefined || typeof options.expectsReply === "boolean", "INVALID_ARGUMENT", "expectsReply must be boolean");
    const sequence = (this.nextMessageSequence.get(sender.id) ?? 0) + 1;
    this.nextMessageSequence.set(sender.id, sequence);
    const brokerSequence = ++this.nextBrokerSequence;
    const message: AgentMessage = {
      id: this.idFactory("message"),
      from: sender.id,
      to: recipient.id,
      type,
      body,
      senderSequence: sequence,
      brokerSequence,
      requestId: options.requestId,
      replyTo: options.replyTo,
      priority,
      createdAt: now,
      metadata: parseMetadata(options.metadata),
      clientDedupeKey: options.clientDedupeKey,
    };
    this.messages.set(message.id, message);
    if (dedupeKey) this.dedupe.set(dedupeKey, message.id);
    let request: RequestRecord | undefined;
    if (options.expectsReply === true) {
      request = {
        id: options.requestId ?? this.idFactory("request"),
        messageId: message.id,
        from: sender.id,
        to: recipient.id,
        status: "pending",
        createdAt: now,
      };
      message.requestId = request.id;
      this.requests.set(request.id, request);
    }
    events.push({ type: "message_sent", message: cloneMessage(message), request: request && cloneRequest(request) });
    this.pruneMessages(events);
    return { message: cloneMessage(message), request: request && cloneRequest(request) };
  }

  private pruneMessages(events: CoordinatorEvent[]): void {
    if (this.messages.size <= this.config.messageRetention) return;
    const candidates = [...this.messages.values()]
      .filter((message) => message.acknowledgedAt !== undefined)
      .sort((left, right) => (left.brokerSequence ?? 0) - (right.brokerSequence ?? 0) || left.createdAt - right.createdAt || left.id.localeCompare(right.id));
    const ids: string[] = [];
    let remaining = this.messages.size;
    for (const message of candidates) {
      if (remaining <= this.config.messageRetention) break;
      this.messages.delete(message.id);
      if (message.clientDedupeKey) this.dedupe.delete(`${message.from}\u0000${message.clientDedupeKey}`);
      ids.push(message.id);
      remaining -= 1;
    }
    if (ids.length > 0) events.push({ type: "messages_pruned", ids });
  }

  private sendInternalMessage(fromId: AgentId, toId: AgentId, type: MessageType, body: string, metadata: Record<string, unknown>, events: CoordinatorEvent[]): void {
    const to = this.requireAgent(toId);
    if (isTerminal(to.status)) return;
    const from = fromId === "broker" ? this.brokerActor() : this.requireAgent(fromId);
    try {
      this.recordMessage(from, to, type, body.slice(0, this.config.maxMessageBody), { priority: "urgent", metadata }, events);
    } catch (error) {
      events.push({ type: "diagnostic", code: "MAILBOX_FULL", message: error instanceof Error ? error.message : String(error), details: { to: toId, type } });
    }
  }

  private brokerActor(): AgentRecord {
    return {
      id: "broker",
      rootId: this.rootId,
      depth: -1,
      role: "broker",
      route: { provider: "broker", model: "coordinator", thinking: "off" },
      capabilities: {
        maySpawn: false,
        mayMessagePeers: true,
        mayEscalate: true,
        mayTransferOwnership: true,
        mayWriteRepo: false,
        mayUseShell: false,
        peerIds: [],
        resourceGrants: {},
      },
      status: "ready",
      createdAt: 0,
      lastActivity: this.clock(),
      childrenCreated: 0,
    };
  }

  private cancelRequestsFor(agentId: AgentId, status: RequestStatus, reason: string, events: CoordinatorEvent[]): void {
    for (const request of this.requests.values()) {
      if (request.status !== "pending" || (request.from !== agentId && request.to !== agentId)) continue;
      const next: RequestRecord = { ...request, status, resolvedAt: this.clock(), failureReason: reason };
      this.requests.set(request.id, next);
      events.push({ type: "request_changed", request: cloneRequest(next) });
    }
  }

  private releaseAgentRuntime(agentId: AgentId, reason: string, events: CoordinatorEvent[]): void {
    for (const resource of this.resources.values()) {
      let changed = false;
      const beforeShared = resource.sharedHolds.length;
      resource.sharedHolds = resource.sharedHolds.filter((hold) => hold.agentId !== agentId);
      if (resource.sharedHolds.length !== beforeShared) changed = true;
      if (resource.mutableHold?.agentId === agentId) {
        resource.mutableHold = undefined;
        resource.version += 1;
        changed = true;
      }
      const beforeWaiters = resource.waiters.length;
      resource.waiters = resource.waiters.filter((waiter) => waiter.agentId !== agentId);
      if (resource.waiters.length !== beforeWaiters) changed = true;
      if (changed) {
        resource.updatedAt = this.clock();
        events.push({ type: "resource_changed", resource: cloneResource(resource) });
      }
    }
    let ownerTaskCleared = false;
    const owner = this.agents.get(agentId);
    for (const task of this.tasks.values()) {
      if (task.owner !== agentId || task.status === "completed") continue;
      const wasTerminal = isTaskTerminal(task.status);
      task.owner = undefined;
      if (!wasTerminal) {
        task.status = reason === "cancelled" ? "cancelled" : "ready";
        task.blockedReason = reason === "cancelled" ? "Agent cancelled" : `Owner ${agentId} released (${reason})`;
      }
      task.updatedAt = this.clock();
      if (owner?.taskId === task.id && (reason !== "broker-recovery" || wasTerminal)) {
        owner.taskId = undefined;
        ownerTaskCleared = true;
      }
      events.push({ type: "task_changed", task: cloneTask(task) });
    }
    if (ownerTaskCleared && owner) events.push({ type: "agent_updated", agent: cloneAgent(owner) });
    this.drainWaiters(events);
  }

  private drainWaiters(events: CoordinatorEvent[]): void {
    let changed = true;
    while (changed) {
      changed = false;
      const candidates: Array<{ resource: ResourceRecord; waiter: ResourceWaiter }> = [];
      for (const resource of this.resources.values()) {
        for (const waiter of resource.waiters) candidates.push({ resource, waiter });
      }
      candidates.sort((left, right) => this.waiterPrecedes(left.waiter, right.waiter) ? -1 : this.waiterPrecedes(right.waiter, left.waiter) ? 1 : 0);
      for (const candidate of candidates) {
        if (!this.agents.has(candidate.waiter.agentId) || isTerminal(this.requireAgent(candidate.waiter.agentId).status)) {
          candidate.resource.waiters = candidate.resource.waiters.filter((waiter) => waiter.requestId !== candidate.waiter.requestId);
          events.push({ type: "resource_changed", resource: cloneResource(candidate.resource) });
          changed = true;
          break;
        }
        if (candidate.resource.waiters[0]?.requestId !== candidate.waiter.requestId) continue;
        if (this.hasEarlierOverlappingWaiter(candidate.resource.id, candidate.waiter)) continue;
        if (!this.canAcquire(candidate.resource, candidate.waiter.agentId, candidate.waiter.mode)) continue;
        candidate.resource.waiters = candidate.resource.waiters.filter((waiter) => waiter.requestId !== candidate.waiter.requestId);
        const hold = this.addHold(candidate.resource, candidate.waiter.agentId, candidate.waiter.mode, candidate.waiter.leaseMs);
        events.push({ type: "resource_changed", resource: cloneResource(candidate.resource) });
        this.sendInternalMessage("broker", candidate.waiter.agentId, "resource_granted", `Resource ${candidate.resource.id} is available`, { resourceId: candidate.resource.id, leaseId: hold.leaseId, requestId: candidate.waiter.requestId }, events);
        changed = true;
        break;
      }
    }
  }

  private addHold(resource: ResourceRecord, agentId: AgentId, mode: BorrowMode, leaseMs: number): ResourceHold {
    const now = this.clock();
    const hold: ResourceHold = {
      leaseId: this.idFactory("lease"),
      agentId,
      mode,
      acquiredAt: now,
      lastHeartbeat: now,
      expiresAt: now + leaseMs,
      leaseMs,
    };
    if (mode === "mutable") resource.mutableHold = hold;
    else resource.sharedHolds.push(hold);
    resource.updatedAt = now;
    return hold;
  }

  private reclaimExpired(now: number, events: CoordinatorEvent[]): void {
    for (const resource of this.resources.values()) {
      let changed = false;
      const before = resource.sharedHolds.length;
      resource.sharedHolds = resource.sharedHolds.filter((hold) => {
        if (hold.expiresAt <= now) {
          changed = true;
          return false;
        }
        return true;
      });
      if (resource.sharedHolds.length !== before) changed = true;
      if (resource.mutableHold && resource.mutableHold.expiresAt <= now) {
        resource.mutableHold = undefined;
        resource.version += 1;
        changed = true;
      }
      if (changed) {
        resource.updatedAt = now;
        events.push({ type: "resource_changed", resource: cloneResource(resource) });
      }
    }
    if (events.some((event) => event.type === "resource_changed")) this.drainWaiters(events);
  }

  private findHold(agentId: AgentId, resourceId: ResourceId, mode: BorrowMode): ResourceHold | undefined {
    const resource = this.requireResource(resourceId);
    if (mode === "mutable" && resource.mutableHold?.agentId === agentId) return resource.mutableHold;
    return mode === "shared" ? resource.sharedHolds.find((hold) => hold.agentId === agentId) : undefined;
  }

  private hasQueuedWaiterAhead(resource: ResourceRecord, agentId: AgentId): boolean {
    return this.overlappingResources(resource.id).some((candidate) => candidate.waiters.some((waiter) => waiter.agentId !== agentId));
  }

  private hasEarlierOverlappingWaiter(resourceId: ResourceId, waiter: ResourceWaiter): boolean {
    return this.overlappingResources(resourceId).some((candidate) => candidate.waiters.some((other) => other.requestId !== waiter.requestId && this.waiterPrecedes(other, waiter)));
  }

  private waiterPrecedes(left: ResourceWaiter, right: ResourceWaiter): boolean {
    if (left.enqueuedSequence !== undefined && right.enqueuedSequence !== undefined && left.enqueuedSequence !== right.enqueuedSequence) return left.enqueuedSequence < right.enqueuedSequence;
    return left.enqueuedAt < right.enqueuedAt || (left.enqueuedAt === right.enqueuedAt && left.requestId.localeCompare(right.requestId) < 0);
  }

  private canAcquire(resource: ResourceRecord, agentId: AgentId, mode: BorrowMode): boolean {
    for (const overlap of this.overlappingResources(resource.id)) {
      // A holder cannot downgrade/upgrade itself behind the coordinator's
      // back: shared and mutable holds for one actor are still conflicting.
      if (overlap.mutableHold) return false;
      if (mode === "mutable" && overlap.sharedHolds.length > 0) return false;
    }
    return true;
  }

  private overlappingResources(resourceId: ResourceId): ResourceRecord[] {
    return [...this.resources.values()].filter((candidate) => this.overlaps(resourceId, candidate.id));
  }

  private overlaps(left: ResourceId, right: ResourceId): boolean {
    if (this.isAncestor(left, right) || this.isAncestor(right, left)) return true;
    const leftResource = this.resources.get(left);
    const rightResource = this.resources.get(right);
    if (!leftResource || !rightResource) return false;
    const leftPath = this.declaredResourcePath(leftResource);
    const rightPath = this.declaredResourcePath(rightResource);
    if (!leftPath || !rightPath) return false;
    if (leftResource.kind === "file" && rightResource.kind === "file") return leftPath === rightPath;
    if (leftResource.kind === "file") return this.pathContains(rightPath, leftPath);
    if (rightResource.kind === "file") return this.pathContains(leftPath, rightPath);
    return this.pathContains(leftPath, rightPath) || this.pathContains(rightPath, leftPath);
  }

  private pathContains(directoryPath: string, candidatePath: string): boolean {
    return directoryPath === candidatePath || candidatePath.startsWith(`${directoryPath}/`);
  }

  private isAncestor(ancestorId: ResourceId, descendantId: ResourceId): boolean {
    if (ancestorId === descendantId) return true;
    let current = this.resources.get(descendantId);
    const seen = new Set<string>();
    while (current?.parentId && !seen.has(current.id)) {
      seen.add(current.id);
      if (current.parentId === ancestorId) return true;
      current = this.resources.get(current.parentId);
    }
    return false;
  }

  private wouldCreateResourceCycle(id: ResourceId, parentId: ResourceId): boolean {
    let current = this.resources.get(parentId);
    const seen = new Set<string>();
    while (current && !seen.has(current.id)) {
      if (current.id === id) return true;
      seen.add(current.id);
      current = current.parentId ? this.resources.get(current.parentId) : undefined;
    }
    return false;
  }

  private parsePermissions(value: unknown): ResourcePermission[] {
    if (value === undefined) return [];
    assertCondition(Array.isArray(value), "INVALID_ARGUMENT", "permissions must be an array");
    return value.map((permission) => {
      assertCondition(typeof permission === "string" && ALL_RESOURCE_PERMISSIONS.has(permission as ResourcePermission), "INVALID_ARGUMENT", `Unknown resource permission ${String(permission)}`);
      return permission as ResourcePermission;
    });
  }

  private hasPermission(resource: ResourceRecord, agentId: AgentId, permission: ResourcePermission): boolean {
    const agent = this.agents.get(agentId);
    for (const candidate of this.resourceAncestors(resource)) {
      if (candidate.owner === agentId) return true;
      if (hasOwn(candidate.grants, agentId) && candidate.grants[agentId].includes(permission)) return true;
      if (agent && hasOwn(agent.capabilities.resourceGrants, candidate.id) && agent.capabilities.resourceGrants[candidate.id]!.includes(permission)) return true;
    }
    return false;
  }

  private canManageResource(actor: AgentRecord, resource: ResourceRecord): boolean {
    if (resource.owner === actor.id) return true;
    if (!actor.capabilities.mayTransferOwnership) return false;
    if (!resource.owner) return actor.depth === 0;
    return this.canControl(actor, this.requireAgent(resource.owner));
  }

  private hasMutableHold(resource: ResourceRecord, agentId: AgentId): boolean {
    // A hold on an ancestor or declared directory path authorizes descendant
    // paths, but a narrow descendant hold must not accidentally authorize its
    // parent or sibling.
    return [...this.resources.values()].some((candidate) => candidate.mutableHold?.agentId === agentId && this.resourceContains(candidate, resource));
  }

  private resourceContains(container: ResourceRecord, candidate: ResourceRecord): boolean {
    if (this.isAncestor(container.id, candidate.id)) return true;
    const containerPath = this.declaredResourcePath(container);
    const candidatePath = this.declaredResourcePath(candidate);
    if (!containerPath || !candidatePath) return false;
    if (container.kind === "file") return candidate.kind === "file" && containerPath === candidatePath;
    return this.pathContains(containerPath, candidatePath);
  }

  private resourcesForPath(path: string): ResourceRecord[] {
    return [...this.resources.values()]
      .filter((resource) => {
        const resourcePath = this.declaredResourcePath(resource);
        if (!resourcePath) return false;
        if (resource.kind === "file") return path === resourcePath;
        return path === resourcePath || path.startsWith(`${resourcePath}/`);
      })
      .sort((left, right) => this.resourcePathSpecificity(right) - this.resourcePathSpecificity(left) || left.id.localeCompare(right.id));
  }

  private declaredResourcePath(resource: ResourceRecord): string | undefined {
    if (resource.path) return resource.path;
    if (resource.kind === "file" && resource.id.startsWith("file:")) {
      try {
        return normalizeResourcePath(resource.id.slice("file:".length));
      } catch {
        return undefined;
      }
    }
    return undefined;
  }

  private resourcePathSpecificity(resource: ResourceRecord): number {
    return this.declaredResourcePath(resource)?.length ?? 0;
  }

  private canInspectResource(agentId: AgentId, resource: ResourceRecord): boolean {
    if (this.hasPermission(resource, agentId, "read")) return true;
    return [...resource.sharedHolds, ...(resource.mutableHold ? [resource.mutableHold] : [])].some((hold) => hold.agentId === agentId);
  }

  private resourceAncestors(resource: ResourceRecord): ResourceRecord[] {
    const result = [resource];
    let current = resource;
    const seen = new Set<string>([resource.id]);
    while (current.parentId && !seen.has(current.parentId)) {
      const parent = this.resources.get(current.parentId);
      if (!parent) break;
      result.push(parent);
      seen.add(parent.id);
      current = parent;
    }
    return result;
  }

  private canMessage(sender: AgentRecord, recipient: AgentRecord | undefined): boolean {
    if (!recipient) return false;
    if (sender.id === recipient.id) return true;
    if (sender.parentId === recipient.id || recipient.parentId === sender.id) return true;
    if (sender.capabilities.peerIds.includes(recipient.id)) return true;
    if (!sender.capabilities.mayMessagePeers) return false;
    if (sender.parentId && sender.parentId === recipient.parentId) return true;
    if (sender.taskId && sender.taskId === recipient.taskId) return true;
    return sender.depth === 0;
  }

  private isVisiblePeer(actor: AgentRecord, candidate: AgentRecord): boolean {
    return this.canMessage(actor, candidate);
  }

  private canControl(actor: AgentRecord, target: AgentRecord): boolean {
    if (actor.id === target.id) return true;
    return actor.depth === 0 || this.isAncestorAgent(actor.id, target.id);
  }

  private isAncestorAgent(ancestorId: AgentId, descendantId: AgentId): boolean {
    let current = this.agents.get(descendantId);
    const seen = new Set<string>();
    while (current?.parentId && !seen.has(current.id)) {
      seen.add(current.id);
      if (current.parentId === ancestorId) return true;
      current = this.agents.get(current.parentId);
    }
    return false;
  }

  private canControlTask(actor: AgentRecord, task: TaskRecord): boolean {
    if (task.owner === actor.id || task.creator === actor.id) return true;
    if (task.owner) return this.canControl(actor, this.requireAgent(task.owner));
    return actor.depth === 0 || task.creator === actor.id;
  }

  private taskVisibleTo(actorId: AgentId, task: TaskRecord): boolean {
    const actor = this.requireAgent(actorId);
    if (task.owner && this.canControl(actor, this.requireAgent(task.owner))) return true;
    return task.creator === actorId || (task.parentTaskId ? this.taskVisibleTo(actorId, this.requireTask(task.parentTaskId)) : actor.depth === 0);
  }

  private taskDependenciesCompleted(task: TaskRecord): boolean {
    return task.dependencies.every((dependency) => this.tasks.get(dependency)?.status === "completed");
  }

  private activeAgentCount(): number {
    return [...this.agents.values()].filter((agent) => ACTIVE_STATUSES.has(agent.status)).length;
  }

  private runningAgentCount(): number {
    return [...this.agents.values()].filter((agent) => agent.status === "running").length;
  }

  private toSummary(agent: AgentRecord): AgentSummary {
    return {
      id: agent.id,
      parentId: agent.parentId,
      depth: agent.depth,
      role: agent.role,
      taskId: agent.taskId,
      route: { ...agent.route },
      status: agent.status,
      workspace: agent.workspace ? { ...agent.workspace } : undefined,
      lastActivity: agent.lastActivity,
    };
  }
}

function publicAgent(agent: AgentRecord): Omit<AgentRecord, "authToken"> {
  const { authToken: _authToken, ...result } = cloneAgent(agent);
  return result;
}

function isDispatchResult<T>(value: T | DispatchResult<T>): value is DispatchResult<T> {
  return Boolean(value && typeof value === "object" && "value" in (value as object) && "events" in (value as object));
}
