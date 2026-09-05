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
  if (requested?.peerIds !== undefined) assertCondition(Array.isArray(requested.peerIds), "INVALID_ARGUMENT", "peerIds must be an array");
  if (requested?.resourceGrants !== undefined) assertCondition(Boolean(requested.resourceGrants) && typeof requested.resourceGrants === "object" && !Array.isArray(requested.resourceGrants), "INVALID_ARGUMENT", "resourceGrants must be an object");
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
    peerIds: (requested?.peerIds ?? []).filter((id) => ceiling.peerIds.length === 0 || ceiling.peerIds.includes(id)),
    resourceGrants: {},
  };
  for (const [resourceId, permissions] of Object.entries(requested?.resourceGrants ?? {})) {
    result.resourceGrants[resourceId] = permissions.filter((permission) => ALL_RESOURCE_PERMISSIONS.has(permission));
  }
  return { ...base, ...result };
}

function parseString(value: unknown, name: string, maxLength = 512): string {
  assertCondition(typeof value === "string" && value.length > 0 && value.length <= maxLength, "INVALID_ARGUMENT", `${name} must be a non-empty string of at most ${maxLength} characters`);
  return value;
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
    assertCondition(this.config.messageRetention > 0, "INVALID_ARGUMENT", "messageRetention must be positive");
    assertCondition(this.config.leaseMs > 0, "INVALID_ARGUMENT", "leaseMs must be positive");
  }

  /** Apply a protocol operation as one synchronous, atomic state transition. */
  dispatch(actorId: AgentId | undefined, operation: string, args: Record<string, unknown> = {}): DispatchResult<any> {
    const before = this.exportState();
    const events: CoordinatorEvent[] = [];
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
        return this.withEvents(events, this.endTurn(this.requireActor(actorId).id, args, events));
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
    const id = parseString(actorId, "agentId");
    const now = this.clock();
    const route = this.validateRoute(input.route);
    const existing = this.agents.get(id);

    if (existing) {
      if (existing.authToken && input.token !== existing.authToken) {
        throw new FabricError("IDENTITY_CONFLICT", `Agent ${id} has a different reconnect credential`);
      }
      if (input.parentId && input.parentId !== existing.parentId) {
        throw new FabricError("IDENTITY_CONFLICT", `Agent ${id} cannot change parent identity`);
      }
      if (input.rootId && input.rootId !== this.rootId) {
        throw new FabricError("IDENTITY_CONFLICT", `Agent ${id} belongs to another fabric`);
      }
      const next = cloneAgent(existing);
      if (isTerminal(next.status)) next.status = "ready";
      if (next.status === "starting") next.status = "ready";
      next.statusReason = undefined;
      next.lastActivity = now;
      next.sessionId = input.sessionId ?? next.sessionId;
      next.workspace = input.workspace ?? next.workspace;
      assertCondition(input.taskId === undefined || input.taskId === next.taskId, "IDENTITY_CONFLICT", `Agent ${id} cannot change its assigned task`);
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

    assertCondition(input.rootId === this.rootId, "IDENTITY_CONFLICT", `Agent ${id} must register with fabric ${this.rootId}`);
    if (!input.parentId) {
      assertCondition(!this.rootAgentId || id === this.rootAgentId, "IDENTITY_CONFLICT", `Only root agent ${this.rootAgentId ?? "the configured root"} may register without a parent`);
      assertCondition(![...this.agents.values()].some((agent) => !agent.parentId), "IDENTITY_CONFLICT", "The fabric already has a root agent");
    }
    const parent = input.parentId ? this.requireAgent(input.parentId) : undefined;
    const depth = parent ? parent.depth + 1 : input.depth ?? 0;
    assertCondition(depth >= 0 && depth <= this.config.maxDepth, "AGENT_LIMIT_REACHED", `Agent depth ${depth} exceeds maxDepth ${this.config.maxDepth}`);
    assertCondition(this.activeAgentCount() < this.config.maxTotalAgents, "AGENT_LIMIT_REACHED", "The fabric has reached maxTotalAgents");
    if (parent) {
      assertCondition(parent.rootId === this.rootId, "IDENTITY_CONFLICT", "Parent belongs to another fabric");
      assertCondition(parent.childrenCreated < this.config.maxChildrenPerAgent, "AGENT_LIMIT_REACHED", `Agent ${parent.id} reached maxChildrenPerAgent`);
      assertCondition(depth === parent.depth + 1, "IDENTITY_CONFLICT", "Child depth must be parent depth plus one");
    }

    const record: AgentRecord = {
      id,
      rootId: this.rootId,
      parentId: parent?.id,
      depth,
      role: parseOptionalString(input.role, "role", 128) ?? "agent",
      route,
      capabilities: mergeCapabilities(input.capabilities, parent?.capabilities),
      status: input.initialStatus ?? "ready",
      sessionId: input.sessionId,
      workspace: input.workspace,
      createdAt: now,
      lastActivity: now,
      childrenCreated: 0,
      authToken: input.token ?? this.idFactory("token"),
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
      assertCondition(!task.owner || task.owner === result.agent.id, "TASK_BUSY", `Task ${taskId} already has an owner`);
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
    if (args.workspace !== undefined) next.workspace = args.workspace as AgentRecord["workspace"];
    if (args.sessionId !== undefined) next.sessionId = parseString(args.sessionId, "sessionId", 512);
    next.lastActivity = this.clock();
    this.agents.set(target.id, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    return cloneAgent(next);
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
    if (args.taskId !== undefined) next.taskId = parseOptionalString(args.taskId, "taskId");
    if (args.route !== undefined) next.route = this.validateRoute(args.route as ModelRoute);
    if (args.workspace !== undefined) next.workspace = args.workspace as AgentRecord["workspace"];
    next.lastActivity = this.clock();
    this.agents.set(actorId, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    return cloneAgent(next);
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
    const next = cloneAgent(agent);
    this.transitionStatus(next, requested, parseOptionalString(args.statusReason, "statusReason", 2048));
    next.lastActivity = this.clock();
    this.agents.set(actorId, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });

    let task: TaskRecord | undefined;
    if (isTerminal(requested)) {
      // Complete before runtime cleanup: cleanup intentionally clears a live
      // owner, while a successful terminal turn must retain the task result.
      if (requested === "completed" && agent.taskId) {
        const current = this.requireTask(agent.taskId);
        if (current.owner === actorId && !isTaskTerminal(current.status)) task = this.completeTask(actorId, current, args.result, events);
      }
      if (requested === "failed" || requested === "cancelled") this.cancelRequestsFor(actorId, requested, `Agent ${actorId} became ${requested}`, events);
      this.releaseAgentRuntime(actorId, requested === "cancelled" ? "cancelled" : "released", events);
      if (requested !== "completed" && agent.taskId) {
        const current = this.requireTask(agent.taskId);
        if (!isTaskTerminal(current.status)) task = cloneTask(current);
      }
      if (requested === "failed" && next.parentId) {
        this.sendInternalMessage(actorId, next.parentId, "agent_failed", next.statusReason ?? `Agent ${actorId} failed`, { failedAgentId: actorId }, events);
      }
    }
    return { agent: cloneAgent(this.requireAgent(actorId)), task };
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
    return { agent: cloneAgent(next), leases };
  }

  private cancelAgent(actorId: AgentId, targetId: AgentId, events: CoordinatorEvent[]): { cancelled: AgentId[] } {
    const actor = this.requireActor(actorId);
    const target = this.requireAgent(targetId);
    assertCondition(actor.id === target.id || actor.capabilities.maySpawn, "CAPABILITY_DENIED", `Agent ${actorId} cannot cancel descendants`);
    assertCondition(this.canControl(actor, target), "CAPABILITY_DENIED", `Agent ${actorId} cannot cancel ${targetId}`);
    const cancelled: AgentId[] = [];
    const visit = (agent: AgentRecord): void => {
      for (const child of this.agents.values()) if (child.parentId === agent.id) visit(child);
      if (isTerminal(agent.status)) return;
      const next = cloneAgent(agent);
      next.status = "cancelled";
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
    return cloneAgent(target);
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
    const body = parseString(input.body, "body", this.config.maxMessageBody);
    const recipient = this.requireAgent(to);
    const sender = this.requireActor(actorId);
    assertCondition(!isTerminal(sender.status), "LIFECYCLE_CONFLICT", `Agent ${actorId} is terminal`);
    assertCondition(!isTerminal(recipient.status), "AGENT_NOT_FOUND", `Recipient ${to} is not active`);
    assertCondition(this.canMessage(sender, recipient), "CAPABILITY_DENIED", `Agent ${actorId} cannot message ${to}`);
    return this.recordMessage(sender, recipient, type, body, {
      priority: input.priority,
      expectsReply: input.expectsReply,
      clientDedupeKey: input.clientDedupeKey,
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
    if (message.acknowledgedAt) return cloneMessage(message);
    message.acknowledgedAt = this.clock();
    if (!message.deliveredAt) message.deliveredAt = message.acknowledgedAt;
    events.push({ type: "message_acknowledged", message: cloneMessage(message) });
    this.pruneMessages(events);
    return cloneMessage(message);
  }

  private inbox(actorId: AgentId, limit: unknown): AgentMessage[] {
    const max = Math.max(1, Math.min(100, Math.floor(parseNumber(limit, "limit", 50))));
    return [...this.messages.values()]
      .filter((message) => message.to === actorId && !message.acknowledgedAt)
      .sort((left, right) => left.createdAt - right.createdAt || left.id.localeCompare(right.id))
      .slice(0, max)
      .map(cloneMessage);
  }

  private listMessages(actorId: AgentId, args: Record<string, unknown>): AgentMessage[] {
    const actor = this.requireActor(actorId);
    const all = actor.depth === 0 && args.scope === "all";
    return [...this.messages.values()]
      .filter((message) => all || message.from === actorId || message.to === actorId || this.canMessage(actor, this.agents.get(message.from) as AgentRecord) || this.canMessage(actor, this.agents.get(message.to) as AgentRecord))
      .sort((left, right) => right.createdAt - left.createdAt || right.id.localeCompare(left.id))
      .slice(0, 100)
      .map(cloneMessage);
  }

  private createTask(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): DispatchResult<TaskRecord> {
    const actor = this.requireActor(actorId);
    const description = parseString(args.description, "description", 16 * 1024);
    const dependencies = Array.isArray(args.dependencies) ? args.dependencies.map((id) => parseString(id, "dependency")) : [];
    for (const dependency of dependencies) {
      assertCondition(this.tasks.has(dependency), "TASK_NOT_FOUND", `Dependency ${dependency} was not found`);
    }
    const parentTaskId = parseOptionalString(args.parentTaskId, "parentTaskId");
    if (parentTaskId) assertCondition(this.tasks.has(parentTaskId), "TASK_NOT_FOUND", `Parent task ${parentTaskId} was not found`);
    const owner = parseOptionalString(args.owner, "owner");
    if (owner) {
      const ownerAgent = this.requireAgent(owner);
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
    const existing = this.resources.get(id);
    if (existing) {
      assertCondition(existing.kind === kind && existing.parentId === parseOptionalString(args.parentId, "parentId", 1024), "IDENTITY_CONFLICT", `Resource ${id} already has a different definition`);
      return cloneResource(existing);
    }
    const parentId = parseOptionalString(args.parentId, "parentId", 1024);
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
    assertCondition(resource.owner === actorId || actor.capabilities.mayTransferOwnership, "CAPABILITY_DENIED", `Agent ${actorId} cannot grant ${resource.id}`);
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
    const resourceId = parseOptionalString(args.resourceId, "resourceId");
    if (!resourceId) {
      const actor = this.requireActor(actorId);
      return { allowed: actor.capabilities.mayWriteRepo };
    }
    const resource = this.requireResource(resourceId);
    if (resource.owner === actorId) return { allowed: true, resourceId };
    if (resource.mutableHold?.agentId === actorId) return { allowed: true, resourceId };
    return { allowed: false, resourceId, reason: `Agent ${actorId} does not hold mutable access to ${resourceId}` };
  }

  private status(actorId: AgentId, args: Record<string, unknown>): FabricStatus {
    const actor = this.requireActor(actorId);
    assertCondition(actor.depth === 0, "CAPABILITY_DENIED", "Only a fabric root may request full status");
    const allMessages = [...this.messages.values()].sort((left, right) => right.createdAt - left.createdAt).slice(0, 100).map(cloneMessage);
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
      next.lastActivity = this.clock();
      this.agents.set(next.id, next);
      events.push({ type: "agent_updated", agent: cloneAgent(next) });
      this.cancelRequestsFor(next.id, "failed", "Broker restarted before the agent reconnected", events);
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
    return Boolean(agent.authToken && token === agent.authToken);
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
    for (const agent of state.agents) {
      assertCondition(agent.rootId === this.rootId, "IDENTITY_CONFLICT", `Persisted agent ${agent.id} belongs to another fabric`);
      this.agents.set(agent.id, cloneAgent(agent));
    }
    for (const task of state.tasks) this.tasks.set(task.id, cloneTask(task));
    for (const resource of state.resources) this.resources.set(resource.id, cloneResource(resource));
    for (const message of state.messages) this.messages.set(message.id, cloneMessage(message));
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
        case "resource_changed":
          this.resources.set(event.resource.id, cloneResource(event.resource));
          break;
        case "message_sent":
          this.messages.set(event.message.id, cloneMessage(event.message));
          this.nextMessageSequence.set(event.message.from, Math.max(this.nextMessageSequence.get(event.message.from) ?? 0, event.message.senderSequence));
          if (event.message.clientDedupeKey) this.dedupe.set(`${event.message.from}\u0000${event.message.clientDedupeKey}`, event.message.id);
          if (event.request) this.requests.set(event.request.id, cloneRequest(event.request));
          break;
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

  private requireActor(actorId: AgentId | undefined): AgentRecord {
    assertCondition(actorId, "IDENTITY_CONFLICT", "A bound actor identity is required");
    const actor = this.requireAgent(actorId);
    assertCondition(!isTerminal(actor.status), "LIFECYCLE_CONFLICT", `Agent ${actorId} is terminal`);
    return actor;
  }

  private requireAgent(agentId: AgentId): AgentRecord {
    const id = parseString(agentId, "agentId");
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
    const pendingCount = [...this.messages.values()].filter((message) => message.to === recipient.id && !message.acknowledgedAt).length;
    assertCondition(pendingCount < this.config.maxMailboxMessages, "MAILBOX_FULL", `Mailbox for ${recipient.id} is full`, { recipient: recipient.id });
    const now = this.clock();
    const priority = options.priority ?? "normal";
    assertCondition(priority === "normal" || priority === "urgent", "INVALID_ARGUMENT", "priority must be normal or urgent");
    assertCondition(options.expectsReply === undefined || typeof options.expectsReply === "boolean", "INVALID_ARGUMENT", "expectsReply must be boolean");
    const sequence = (this.nextMessageSequence.get(sender.id) ?? 0) + 1;
    this.nextMessageSequence.set(sender.id, sequence);
    const message: AgentMessage = {
      id: this.idFactory("message"),
      from: sender.id,
      to: recipient.id,
      type,
      body,
      senderSequence: sequence,
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
      .filter((message) => Boolean(message.acknowledgedAt))
      .sort((left, right) => left.createdAt - right.createdAt || left.id.localeCompare(right.id));
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
      if (task.owner !== agentId || isTaskTerminal(task.status)) continue;
      task.owner = undefined;
      task.status = reason === "cancelled" ? "cancelled" : "ready";
      task.blockedReason = reason === "cancelled" ? "Agent cancelled" : `Owner ${agentId} released (${reason})`;
      task.updatedAt = this.clock();
      if (owner?.taskId === task.id && reason !== "broker-recovery") {
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
      candidates.sort((left, right) => left.waiter.enqueuedAt - right.waiter.enqueuedAt || left.waiter.requestId.localeCompare(right.waiter.requestId));
      for (const candidate of candidates) {
        if (!this.agents.has(candidate.waiter.agentId) || isTerminal(this.requireAgent(candidate.waiter.agentId).status)) {
          candidate.resource.waiters = candidate.resource.waiters.filter((waiter) => waiter.requestId !== candidate.waiter.requestId);
          events.push({ type: "resource_changed", resource: cloneResource(candidate.resource) });
          changed = true;
          break;
        }
        if (candidate.resource.waiters[0]?.requestId !== candidate.waiter.requestId) continue;
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

  private canAcquire(resource: ResourceRecord, agentId: AgentId, mode: BorrowMode): boolean {
    for (const overlap of this.overlappingResources(resource.id)) {
      if (overlap.mutableHold && overlap.mutableHold.agentId !== agentId) return false;
      if (mode === "mutable" && overlap.sharedHolds.some((hold) => hold.agentId !== agentId)) return false;
    }
    return true;
  }

  private overlappingResources(resourceId: ResourceId): ResourceRecord[] {
    return [...this.resources.values()].filter((candidate) => this.overlaps(resourceId, candidate.id));
  }

  private overlaps(left: ResourceId, right: ResourceId): boolean {
    return this.isAncestor(left, right) || this.isAncestor(right, left);
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
    for (const candidate of this.resourceAncestors(resource)) {
      if (candidate.owner === agentId) return true;
      if (candidate.grants[agentId]?.includes(permission)) return true;
    }
    return false;
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
