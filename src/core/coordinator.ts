import { createHash, randomUUID } from "node:crypto";
import { FabricError, assertCondition } from "./errors.ts";
import {
  DEFAULT_FABRIC_CONFIG,
  cloneAckTombstone,
  cloneAgent,
  cloneCapabilities,
  cloneMessage,
  cloneModelTurnRecoveryReservation,
  cloneModelTurnWaiter,
  cloneRequest,
  cloneResource,
  cloneTask,
  resourceFromTombstone,
  type ActiveFenceSummary,
  type AgentCapabilities,
  type AgentId,
  type AgentTombstone,
  type AgentMessage,
  type AgentRecord,
  type AgentStatus,
  type AgentSummary,
  type BorrowMode,
  type CoordinatorEvent,
  type DispatchResult,
  type FabricConfig,
  type FabricStatus,
  type IdempotencyRecord,
  type IdempotencyStateEntry,
  type MessageAckProofLookup,
  type MessageAckTombstone,
  type MessageId,
  type MessagePriority,
  type ModelTurnRecoveryReservation,
  type ModelTurnRecoveryState,
  type ModelTurnWaiter,
  type MessageType,
  type ModelRoute,
  type PersistedCoordinatorState,
  type PersistedCoordinatorStateV2,
  type RequestId,
  type RequestRecord,
  type RequestStatus,
  type RequestTombstone,
  type ResourceHold,
  type RetainedArtifactRecord,
  type ResourceId,
  type ResourcePermission,
  type ResourceRecord,
  type ResourceTombstone,
  type ResourceWaiter,
  type TaskId,
  type TaskRecord,
  type TaskResult,
  type TaskStatus,
  type TaskTombstone,
  type FabricSnapshot,
  type ModelTurnPurpose,
  type WriteFenceRecord,
} from "./types.ts";
import {
  ACTIVE_STATUSES,
  ALL_MESSAGE_TYPES,
  ALL_RESOURCE_PERMISSIONS,
  ALL_THINKING_LEVELS,
  INTERNAL_MESSAGE_TYPES,
  TERMINAL_STATUSES,
  defaultCapabilities,
  decodePaginationCursor,
  encodePaginationCursor,
  hasOwn,
  idempotencyKey,
  isDispatchResult,
  isTaskTerminal,
  isTerminal,
  mergeCapabilities,
  modelRouteCapacity,
  modelRouteKey,
  modelRouteCapacityKey,
  normalizeClone,
  normalizeResourcePath,
  parseAgentId,
  parseMetadata,
  parseNumber,
  parseOptionalString,
  parseString,
  parseWorkspace,
  publicAgent,
  requestFingerprint,
  stableStringify,
} from "./coordinator-wire.ts";

export interface CoordinatorOptions {
  rootId: string;
  rootAgentId?: string;
  config?: Partial<FabricConfig>;
  clock?: () => number;
  idFactory?: (prefix: string) => string;
  /** Optional cold ACK proof index used by a broker; standalone coordinators keep a fallback cache. */
  ackProofs?: MessageAckProofLookup;
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
  contextMode?: string;
  contextDiagnostic?: string;
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


export class Coordinator {
  /** Bounded durable deduplication window; the oldest entries are evicted first. */
  static readonly maxIdempotencyEntries = 256;
  /** Operations whose responses and effects are safe to replay after a lost response. */
  static readonly idempotentOperations = new Set([
    "agent.spawn",
    "task.create",
    "agent.begin_turn",
    "agent.end_turn",
    "agent.finish_turn",
    "message.send",
    "message.ack",
  ]);
  /** Operations that only read hot state and therefore do not need rollback snapshots. */
  static readonly readOnlyOperations = new Set([
    "agent.status",
    "discover.agents",
    "message.inbox",
    "message.list",
    "task.list",
    "task.show",
    "resource.inspect",
    "resource.snapshot",
    "resource.list",
    "agent.artifacts",
    "resource.check_write",
    "fabric.status",
    "fabric.snapshot",
  ]);

  static isReadOnlyOperation(operation: string): boolean {
    return Coordinator.readOnlyOperations.has(operation);
  }

  readonly rootId: string;
  readonly rootAgentId?: string;
  readonly config: FabricConfig;
  /** Whether workspace-relative policy keys are case-folded (see FabricConfig.caseInsensitivePaths). */
  caseFoldPaths: boolean;

  private readonly clock: () => number;
  private readonly idFactory: (prefix: string) => string;
  private ackProofs?: MessageAckProofLookup;
  private agents = new Map<AgentId, AgentRecord>();
  private tasks = new Map<TaskId, TaskRecord>();
  private resources = new Map<ResourceId, ResourceRecord>();
  private messages = new Map<string, AgentMessage>();
  private requests = new Map<RequestId, RequestRecord>();
  private dedupe = new Map<string, string>();
  private nextMessageSequence = new Map<AgentId, number>();
  private nextBrokerSequence = 0;
  private nextResourceWaiterSequence = 0;
  private idempotency = new Map<string, IdempotencyStateEntry>();
  /** Durable FIFO admission tickets for model turns waiting on capacity. */
  private modelWaiters = new Map<string, ModelTurnWaiter>();
  private nextModelTurnWaiterSequence = 0;
  /** Terminal records leave compact tombstones so dependencies and identities remain safe. */
  private archivedAgents = new Map<AgentId, AgentTombstone>();
  private archivedTasks = new Map<TaskId, TaskTombstone>();
  private archivedRequests = new Map<RequestId, RequestTombstone>();
  private archivedResources = new Map<ResourceId, ResourceTombstone>();
  /** Retained Git/session artifacts are cold metadata, not hot agent pins. */
  private retainedArtifacts = new Map<string, RetainedArtifactRecord>();
  /** Compact ACK proofs survive message and ordinary idempotency retention. */
  private acknowledgedMessages = new Map<string, MessageAckTombstone>();
  /** Reservations preserve physical provider capacity across broker recovery. */
  private recoveryTurnReservations = new Map<AgentId, ModelTurnRecoveryReservation>();
  /** Ephemeral write fences (begin_write/end_write); never persisted or replayed. */
  private fences = new Map<string, WriteFenceRecord>();

  constructor(options: CoordinatorOptions) {
    this.rootId = parseString(options.rootId, "rootId");
    this.rootAgentId = options.rootAgentId === undefined ? undefined : parseString(options.rootAgentId, "rootAgentId");
    this.clock = options.clock ?? (() => Date.now());
    this.idFactory = options.idFactory ?? ((prefix) => `${prefix}-${randomUUID()}`);
    this.ackProofs = options.ackProofs;
    this.config = {
      ...DEFAULT_FABRIC_CONFIG,
      ...(options.config ?? {}),
    };
    // Policy-key case folding follows the resolved config; undefined keeps the
    // historical platform rule (Windows folds, others do not) unless the
    // broker probed the actual volume at startup and injected the result.
    this.caseFoldPaths = this.config.caseInsensitivePaths ?? process.platform === "win32";
    assertCondition(this.config.maxDepth >= 0, "INVALID_ARGUMENT", "maxDepth must be non-negative");
    assertCondition(this.config.maxChildrenPerAgent >= 0, "INVALID_ARGUMENT", "maxChildrenPerAgent must be non-negative");
    assertCondition(this.config.maxChildrenCreatedPerAgent === undefined || this.config.maxChildrenCreatedPerAgent >= 0, "INVALID_ARGUMENT", "maxChildrenCreatedPerAgent must be non-negative");
    assertCondition(this.config.maxTotalAgents > 0, "INVALID_ARGUMENT", "maxTotalAgents must be positive");
    assertCondition(this.config.maxConcurrentAgents > 0, "INVALID_ARGUMENT", "maxConcurrentAgents must be positive");
    assertCondition(this.config.maxMailboxMessages > 0, "INVALID_ARGUMENT", "maxMailboxMessages must be positive");
    assertCondition(this.config.maxMessageBody > 0, "INVALID_ARGUMENT", "maxMessageBody must be positive");
    assertCondition(this.config.maxTaskOutput > 0, "INVALID_ARGUMENT", "maxTaskOutput must be positive");
    assertCondition(this.config.messageRetention > 0, "INVALID_ARGUMENT", "messageRetention must be positive");
    assertCondition((this.config.historyRetentionMs ?? 0) > 0, "INVALID_ARGUMENT", "historyRetentionMs must be positive");
    assertCondition((this.config.maxArchivedRecords ?? 0) > 0, "INVALID_ARGUMENT", "maxArchivedRecords must be positive");
    assertCondition((this.config.maxRetainedArtifacts ?? 0) > 0, "INVALID_ARGUMENT", "maxRetainedArtifacts must be positive");
    assertCondition(this.config.leaseMs > 0, "INVALID_ARGUMENT", "leaseMs must be positive");
    assertCondition(this.config.heartbeatMs > 0, "INVALID_ARGUMENT", "heartbeatMs must be positive");
    assertCondition((this.config.agentHeartbeatTimeoutMs ?? 0) > 0, "INVALID_ARGUMENT", "agentHeartbeatTimeoutMs must be positive");
    assertCondition((this.config.reconnectGraceMs ?? 0) > 0, "INVALID_ARGUMENT", "reconnectGraceMs must be positive");
    assertCondition(Number.isInteger(this.config.modelTurnGrantTtlMs) && (this.config.modelTurnGrantTtlMs ?? 0) > 0, "INVALID_ARGUMENT", "modelTurnGrantTtlMs must be a positive integer");
    assertCondition(Number.isInteger(this.config.historyGcBatchSize) && (this.config.historyGcBatchSize ?? 0) > 0, "INVALID_ARGUMENT", "historyGcBatchSize must be a positive integer");
    this.validateRoutePolicies();
  }

  private validateRoutePolicies(): void {
    const maps = [this.config.modelRouteCapacity, this.config.modelRouteCapacities] as const;
    for (const map of maps) {
      if (!map) continue;
      for (const [key, value] of Object.entries(map)) {
        assertCondition(Boolean(key) && key.length <= 1024, "INVALID_ARGUMENT", "model route policy keys must be bounded");
        assertCondition(typeof value === "number" && Number.isInteger(value) && value > 0, "INVALID_ARGUMENT", `model route capacity for ${key} must be a positive integer`);
      }
    }
    if (this.config.effectivePrefillBudgets) {
      for (const [key, value] of Object.entries(this.config.effectivePrefillBudgets)) {
        assertCondition(Boolean(key) && key.length <= 1024, "INVALID_ARGUMENT", "effective prefill budget keys must be bounded");
        assertCondition(typeof value === "number" && Number.isFinite(value) && value > 0, "INVALID_ARGUMENT", `effective prefill budget for ${key} must be a positive finite number`);
      }
    }
    if (this.config.modelRoutePolicies) {
      for (const [key, policy] of Object.entries(this.config.modelRoutePolicies)) {
        assertCondition(Boolean(key) && key.length <= 1024, "INVALID_ARGUMENT", "model route policy keys must be bounded");
        assertCondition(Boolean(policy) && typeof policy === "object" && !Array.isArray(policy), "INVALID_ARGUMENT", `model route policy for ${key} must be an object`);
        if (policy.maxConcurrent !== undefined) assertCondition(typeof policy.maxConcurrent === "number" && Number.isInteger(policy.maxConcurrent) && policy.maxConcurrent > 0, "INVALID_ARGUMENT", `model route maxConcurrent for ${key} must be a positive integer`);
        if (policy.effectivePrefillBudget !== undefined) assertCondition(typeof policy.effectivePrefillBudget === "number" && Number.isFinite(policy.effectivePrefillBudget) && policy.effectivePrefillBudget > 0, "INVALID_ARGUMENT", `effective prefill budget for ${key} must be a positive finite number`);
        if (policy.capacityGroup !== undefined) assertCondition(typeof policy.capacityGroup === "string" && policy.capacityGroup.length > 0 && policy.capacityGroup.length <= 256, "INVALID_ARGUMENT", `capacityGroup for ${key} must be a bounded non-empty string`);
      }
    }
  }

  setCaseInsensitivePaths(caseInsensitive: boolean): void {
    this.caseFoldPaths = caseInsensitive;
    this.config.caseInsensitivePaths = caseInsensitive;
  }

  /** Attach the broker-owned cold ACK proof index after construction. */
  setAckProofs(ackProofs: MessageAckProofLookup | undefined): void {
    this.ackProofs = ackProofs;
    if (ackProofs) this.acknowledgedMessages.clear();
  }

  /** Apply a protocol operation as one synchronous, atomic state transition. */
  dispatch(
    actorId: AgentId | undefined,
    operation: string,
    args: Record<string, unknown> = {},
    rollbackState?: PersistedCoordinatorState,
  ): DispatchResult<any> {
    const replay = this.idempotentReplay(actorId, operation, args);
    if (replay) return replay;
    const readOnly = Coordinator.isReadOnlyOperation(operation);
    // Read projections do not mutate coordinator state and must not pay the
    // full-world clone cost. The broker supplies its one rollback snapshot for
    // mutations so coordinator and transport do not clone the same state twice.
    const before = readOnly ? undefined : rollbackState ?? this.exportState();
    const events: CoordinatorEvent[] = [];
    assertCondition(Boolean(args) && typeof args === "object" && !Array.isArray(args), "INVALID_ARGUMENT", "operation args must be an object");
    if (!readOnly) this.reclaimExpired(this.clock(), events);

    try {
      const run = (): DispatchResult<any> => {
      switch (operation) {
      case "agent.register":
        return this.withEvents(events, this.registerAgent(actorId, args as unknown as RegisterAgentArgs, events));
      case "agent.spawn":
        return this.withEvents(events, this.spawnAgent(this.requireActor(actorId).id, args as unknown as SpawnAgentArgs, events));
      case "agent.update":
        return this.withEvents(events, this.updateAgent(this.requireActor(actorId).id, args, events));
      case "agent.configure_child":
        return this.withEvents(events, this.configureChild(this.requireActor(actorId).id, args, events));
      case "agent.mark_artifacts_cleaned":
        return this.withEvents(events, this.markArtifactsCleaned(this.requireActor(actorId).id, parseString(args.agentId, "agentId"), events));
      case "agent.mark_artifacts_retained":
        return this.withEvents(events, this.markArtifactsRetained(this.requireActor(actorId).id, parseString(args.agentId, "agentId"), args.artifact, events));
      case "agent.resolve_artifact":
        return this.withEvents(events, this.resolveArtifact(this.requireActor(actorId).id, parseString(args.artifactId, "artifactId"), args.resolution, events));
      case "agent.artifacts":
        return this.withEvents(events, this.listRetainedArtifacts(this.requireActor(actorId).id, args), events);
      case "agent.begin_turn":
        return this.withEvents(events, this.beginTurn(this.requireActor(actorId).id, args, events));
      case "agent.reconcile_turn":
        return this.withEvents(events, this.reconcileTurn(this.requireActor(actorId).id, args, events));
      case "agent.drain":
        return this.withEvents(events, this.drainAgent(this.requireActor(actorId).id, parseString(args.agentId ?? actorId, "agentId"), parseOptionalString(args.reason, "reason", 2048), events));
      case "agent.end_turn":
        return this.withEvents(events, this.endTurn(this.requireBoundAgent(actorId).id, args, events));
      case "agent.finish_turn":
        return this.withEvents(events, this.finishTurn(this.requireBoundAgent(actorId).id, args, events));
      case "agent.heartbeat":
        return this.withEvents(events, this.heartbeat(this.requireActor(actorId).id, events));
      case "agent.cancel":
        return this.withEvents(events, this.cancelAgent(this.requireActor(actorId).id, parseString(args.agentId ?? actorId, "agentId"), events));
      case "agent.status":
        return this.withEvents(events, this.getAgentStatus(this.requireActor(actorId).id, parseOptionalString(args.agentId, "agentId"), args.scope), events);
      case "discover.agents":
        return this.withEvents(events, this.discoverAgents(this.requireActor(actorId).id, args), events);
      case "message.send":
        return this.withEvents(events, this.sendMessage(this.requireActor(actorId).id, args as unknown as MessageSendArgs, events));
      case "message.reply":
        return this.withEvents(events, this.replyToRequest(parseString(actorId, "agentId"), args, events));
      case "message.ack":
        return this.withEvents(events, this.ackMessage(this.requireActor(actorId).id, parseString(args.messageId, "messageId"), args.revision, events));
      case "message.inbox":
        return this.withEvents(events, this.inbox(this.requireActor(actorId).id, args.limit, args.afterBrokerSequence), events);
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
        return this.withEvents(events, this.listResources(this.requireActor(actorId).id, args), events);
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
      case "resource.begin_write":
        return this.withEvents(events, this.beginWrite(this.requireActor(actorId).id, args, events), events);
      case "resource.end_write":
        return this.withEvents(events, this.endWrite(this.requireActor(actorId).id, args, events));
      case "resource.retire":
        return this.withEvents(events, this.retireResource(this.requireActor(actorId).id, parseString(args.resourceId, "resourceId"), events));
      case "fabric.status":
        return this.withEvents(events, this.status(this.requireActor(actorId).id, args), events);
      case "fabric.snapshot":
        return this.withEvents(events, this.snapshot(this.requireActor(actorId).id), events);
        default:
          throw new FabricError("INVALID_ARGUMENT", `Unknown coordinator operation: ${operation}`);
      }
      };
      const result = run();
      this.rememberIdempotency(actorId, operation, args, result);
      return result;
    } catch (error) {
      if (before) this.restoreState(before);
      throw error;
    }
  }

  /**
   * Resolve a duplicate write before it can mutate state. An operationId is
   * scoped to its actor; replaying the same request returns the original
   * response with `replayed: true`, while reusing the operationId for
   * different arguments is a client bug and fails closed.
   */
  private idempotentReplay(actorId: AgentId | undefined, operation: string, args: Record<string, unknown>): DispatchResult<any> | undefined {
    if (!args || typeof args !== "object" || Array.isArray(args)) return undefined;
    const operationId = args.operationId;
    if (operationId === undefined) return undefined;
    // Reconciliation uses operationId as the physical provider-operation
    // identity, not as a replay-cache key. Once its reservation is resolved,
    // an identical retry is a harmless no-op observation.
    if (operation === "agent.reconcile_turn") return undefined;
    assertCondition(Coordinator.idempotentOperations.has(operation), "INVALID_ARGUMENT", `operationId is not supported for ${operation}`);
    assertCondition(typeof operationId === "string" && operationId.length > 0 && operationId.length <= 128 && !operationId.includes("\u0000"), "INVALID_ARGUMENT", "operationId must be a bounded non-empty string without NUL");
    assertCondition(actorId !== undefined, "IDENTITY_CONFLICT", "operationId requires an authenticated actor");
    const prior = this.idempotency.get(idempotencyKey(String(actorId), operationId));
    if (!prior) return undefined;
    const requestHash = requestFingerprint(operation, args);
    assertCondition(prior.operation === operation && prior.requestHash === requestHash, "IDEMPOTENCY_CONFLICT", `operationId '${operationId}' was already used for a different request`, { operationId });
    return { value: { ...(structuredClone(prior.response) as object), replayed: true }, events: [] };
  }

  private rememberIdempotency(actorId: AgentId | undefined, operation: string, args: Record<string, unknown>, result: DispatchResult<any>): void {
    const operationId = args.operationId;
    if (typeof operationId !== "string" || operationId.length === 0) return;
    if (!Coordinator.idempotentOperations.has(operation)) return;
    // A capacity admission that did not start is only a probe. Do not pin the
    // operationId to a transient false result; a retry with the same logical
    // turn may succeed once another actor releases its slot.
    if (operation === "agent.begin_turn" && result.value && typeof result.value === "object" && (result.value as { started?: unknown }).started === false) return;
    const key = idempotencyKey(String(actorId), operationId);
    const entry: IdempotencyStateEntry = { key, operation, requestHash: requestFingerprint(operation, args), response: normalizeClone(result.value) };
    this.rememberIdempotencyEntry(entry);
    result.idempotency = { actorId: String(actorId), operationId, operation, requestHash: entry.requestHash, response: entry.response };
  }

  private rememberIdempotencyEntry(entry: IdempotencyStateEntry): void {
    if (this.idempotency.size >= Coordinator.maxIdempotencyEntries && !this.idempotency.has(entry.key)) {
      const oldest = this.idempotency.keys().next();
      if (!oldest.done) this.idempotency.delete(oldest.value);
    }
    this.idempotency.set(entry.key, entry);
  }

  /** Rehydrate one durable idempotency record during journal replay. */
  restoreIdempotency(record: IdempotencyRecord): void {
    this.rememberIdempotencyEntry({
      key: idempotencyKey(record.actorId, record.operationId),
      operation: record.operation,
      requestHash: record.requestHash,
      response: record.response,
    });
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
      const reconnecting = existing.reconnectable === true;
      assertCondition(!isTerminal(existing.status) || reconnecting, "LIFECYCLE_CONFLICT", `Agent ${id} is terminal and cannot reconnect`);
      if (reconnecting) {
        assertCondition(existing.status === "failed", "LIFECYCLE_CONFLICT", `Agent ${id} has an invalid recovery state`);
        const reconnectParent = existing.parentId ? this.requireAgent(existing.parentId) : undefined;
        if (reconnectParent) {
          assertCondition(this.isReservedLive(reconnectParent) && reconnectParent.reconnectable !== true && reconnectParent.status !== "draining", "LIFECYCLE_CONFLICT", `Agent ${id} cannot reconnect beneath inactive parent ${reconnectParent.id}`);
          this.assertChildCapacity(reconnectParent, id, false);
          assertCondition(existing.depth === reconnectParent.depth + 1, "IDENTITY_CONFLICT", "Recovered child depth no longer matches its parent");
        } else {
          assertCondition(existing.depth === 0, "IDENTITY_CONFLICT", "A recovered non-root agent must retain its parent");
        }
        assertCondition(existing.depth >= 0 && existing.depth <= this.config.maxDepth, "AGENT_LIMIT_REACHED", `Agent depth ${existing.depth} exceeds maxDepth ${this.config.maxDepth}`);
      }
      const next = cloneAgent(existing);
      if (!next.authToken) next.authToken = token ?? this.idFactory("token");
      if (next.reconnectable) {
        assertCondition(next.status === "failed", "LIFECYCLE_CONFLICT", `Agent ${id} has an invalid recovery state`);
        next.status = "ready";
        next.reconnectable = false;
        next.recoveryExpiredAt = undefined;
        next.terminalAt = undefined;
      } else if (next.status === "starting") {
        next.status = "ready";
      }
      next.statusReason = undefined;
      if (input.contextDiagnostic !== undefined) next.contextDiagnostic = parseOptionalString(input.contextDiagnostic, "contextDiagnostic", 2048);
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
      const routeChanged = input.route && (input.route.provider !== existing.route.provider || input.route.model !== existing.route.model || input.route.thinking !== existing.route.thinking || input.route.capacityGroup !== existing.route.capacityGroup);
      assertCondition(!routeChanged || this.findModelTurnWaiter(id) === undefined, "LIFECYCLE_CONFLICT", `Agent ${id} cannot change route while a model turn is queued`);
      assertCondition(!routeChanged || !this.recoveryTurnReservations.has(id), "LIFECYCLE_CONFLICT", `Agent ${id} cannot change route while its recovered model turn is unresolved`);
      if (routeChanged) next.route = route;
      this.agents.set(id, next);
      events.push({ type: "agent_updated", agent: cloneAgent(next) });
      return { agent: publicAgent(next), token: next.authToken as string };
    }

    assertCondition(!this.archivedAgents.has(id), "IDENTITY_CONFLICT", `Agent ${id} is an archived terminal identity and cannot be reused`);
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
    assertCondition(this.reservedAgentCount() < this.config.maxTotalAgents, "AGENT_LIMIT_REACHED", "The fabric has reached maxTotalAgents (slots awaiting reconnecting agents stay reserved)");
    if (parent) {
      assertCondition(parent.rootId === this.rootId, "IDENTITY_CONFLICT", "Parent belongs to another fabric");
      assertCondition(ACTIVE_STATUSES.has(parent.status) && parent.status !== "draining", "LIFECYCLE_CONFLICT", `Parent ${parent.id} is not active for a new child`);
      this.assertChildCapacity(parent);
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
      contextMode: parseOptionalString(input.contextMode, "contextMode", 128),
      contextDiagnostic: parseOptionalString(input.contextDiagnostic, "contextDiagnostic", 2048),
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
    assertCondition(parent.status !== "draining", "LIFECYCLE_CONFLICT", `Agent ${actorId} is draining and cannot spawn children`);
    assertCondition(parent.depth < this.config.maxDepth, "AGENT_LIMIT_REACHED", "Maximum recursion depth reached");
    this.assertChildCapacity(parent);
    assertCondition(this.reservedAgentCount() < this.config.maxTotalAgents, "AGENT_LIMIT_REACHED", "The fabric has reached maxTotalAgents (slots awaiting reconnecting agents stay reserved)");
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
      assertCondition(!currentOwner || (isTerminal(currentOwner.status) && currentOwner.reconnectable !== true) || currentOwner.id === result.agent.id, "TASK_BUSY", `Task ${taskId} already has an owner`);
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
    assertCondition(actor.status !== "draining", "LIFECYCLE_CONFLICT", `Agent ${actorId} is draining and cannot configure children`);
    const target = this.requireAgent(parseString(args.agentId, "agentId"));
    assertCondition(actor.capabilities.maySpawn, "CAPABILITY_DENIED", `Agent ${actorId} cannot configure children`);
    assertCondition(this.canControl(actor, target) && target.parentId === actorId, "CAPABILITY_DENIED", `Agent ${actorId} cannot configure child ${target.id}`);
    const next = cloneAgent(target);
    if (args.sessionId !== undefined) next.sessionId = parseString(args.sessionId, "sessionId", 512);
    if (args.workspace !== undefined) next.workspace = parseWorkspace(args.workspace);
    if (args.contextMode !== undefined) next.contextMode = parseOptionalString(args.contextMode, "contextMode", 128);
    if (args.contextDiagnostic !== undefined) next.contextDiagnostic = parseOptionalString(args.contextDiagnostic, "contextDiagnostic", 2048);
    next.lastActivity = this.clock();
    this.agents.set(target.id, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    return publicAgent(next);
  }

  private markArtifactsCleaned(actorId: AgentId, targetId: AgentId, events: CoordinatorEvent[]): AgentRecord {
    const actor = this.requireActor(actorId);
    const target = this.requireAgent(targetId);
    assertCondition(target.depth > 0, "INVALID_ARGUMENT", "Only child agent artifacts may be marked cleaned");
    assertCondition(this.canControl(actor, target), "CAPABILITY_DENIED", `Agent ${actorId} cannot mark artifacts for ${targetId}`);
    assertCondition(isTerminal(target.status) && target.reconnectable !== true, "LIFECYCLE_CONFLICT", `Agent ${targetId} is not permanently stopped`);
    if (target.artifactsCleanedAt !== undefined) return publicAgent(target);
    assertCondition(target.artifactDisposition !== "retained", "LIFECYCLE_CONFLICT", `Agent ${targetId} has retained artifacts that must not be certified as cleaned`);
    const next = cloneAgent(target);
    next.artifactsCleanedAt = this.clock();
    next.artifactDisposition = "cleaned";
    this.agents.set(target.id, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    return publicAgent(next);
  }

  private markArtifactsRetained(actorId: AgentId, targetId: AgentId, rawArtifact: unknown, events: CoordinatorEvent[]): AgentRecord {
    const actor = this.requireActor(actorId);
    const target = this.requireAgent(targetId);
    assertCondition(target.depth > 0, "INVALID_ARGUMENT", "Only child agent artifacts may be retained");
    assertCondition(this.canControl(actor, target), "CAPABILITY_DENIED", `Agent ${actorId} cannot retain artifacts for ${targetId}`);
    assertCondition(isTerminal(target.status) && target.reconnectable !== true, "LIFECYCLE_CONFLICT", `Agent ${targetId} is not permanently stopped`);
    assertCondition(target.artifactDisposition !== "cleaned" && target.artifactsCleanedAt === undefined, "LIFECYCLE_CONFLICT", `Agent ${targetId} has already been certified as cleaned`);
    assertCondition(rawArtifact === undefined || (Boolean(rawArtifact) && typeof rawArtifact === "object" && !Array.isArray(rawArtifact)), "INVALID_ARGUMENT", "artifact must be an object");
    const input = (rawArtifact ?? {}) as Record<string, unknown>;
    const existing = target.retainedArtifactId ? this.retainedArtifacts.get(target.retainedArtifactId) : undefined;
    assertCondition(input.workspaceRetained === undefined || typeof input.workspaceRetained === "boolean", "INVALID_ARGUMENT", "artifact.workspaceRetained must be boolean");
    assertCondition(input.sessionRetained === undefined || typeof input.sessionRetained === "boolean", "INVALID_ARGUMENT", "artifact.sessionRetained must be boolean");
    assertCondition(!existing || existing.status !== "resolved", "LIFECYCLE_CONFLICT", `Artifact ${existing?.id ?? target.retainedArtifactId} has already been resolved`);
    if (!existing) {
      this.trimRetainedArtifacts(events, true);
      assertCondition(this.retainedArtifacts.size < (this.config.maxRetainedArtifacts ?? DEFAULT_FABRIC_CONFIG.maxRetainedArtifacts!), "AGENT_LIMIT_REACHED", "retained artifact limit reached; resolve an existing artifact before retaining another");
    }
    const keepWorkspace = input.workspaceRetained === undefined ? true : input.workspaceRetained;
    const keepSession = input.sessionRetained === undefined ? true : input.sessionRetained;
    const artifact: RetainedArtifactRecord = {
      id: existing?.id ?? this.idFactory("retained-artifact"),
      agentId: target.id,
      workspace: input.workspace !== undefined ? parseWorkspace(input.workspace) : keepWorkspace ? existing?.workspace ? { ...existing.workspace } : target.workspace ? { ...target.workspace } : undefined : undefined,
      sessionPath: !keepSession ? undefined : input.sessionPath === undefined ? existing?.sessionPath : parseOptionalString(input.sessionPath, "artifact.sessionPath", 4096),
      baseRef: input.baseRef === undefined ? existing?.baseRef : parseOptionalString(input.baseRef, "artifact.baseRef", 512),
      headRef: input.headRef === undefined ? existing?.headRef : parseOptionalString(input.headRef, "artifact.headRef", 512),
      reason: input.reason === undefined ? existing?.reason : parseOptionalString(input.reason, "artifact.reason", 2048),
      retainedAt: existing?.retainedAt ?? this.clock(),
      status: "retained",
    };
    this.retainedArtifacts.set(artifact.id, { ...artifact, workspace: artifact.workspace ? { ...artifact.workspace } : undefined });
    const next = cloneAgent(target);
    next.artifactDisposition = "retained";
    next.retainedArtifactId = artifact.id;
    next.artifactsCleanedAt = undefined;
    next.lastActivity = this.clock();
    this.agents.set(target.id, next);
    events.push({ type: "agent_artifacts_retained", agentId: target.id, artifact: { ...artifact, workspace: artifact.workspace ? { ...artifact.workspace } : undefined } });
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    return publicAgent(next);
  }

  private trimRetainedArtifacts(events: CoordinatorEvent[], reserveSlot = false): void {
    const limit = this.config.maxRetainedArtifacts ?? DEFAULT_FABRIC_CONFIG.maxRetainedArtifacts!;
    // Admission happens before insertion, so a full collection must make one
    // slot available when the caller is about to retain a new artifact.
    const target = reserveSlot ? limit - 1 : limit;
    if (this.retainedArtifacts.size <= target) return;
    const resolved = [...this.retainedArtifacts.values()]
      .filter((artifact) => artifact.status === "resolved")
      .sort((left, right) => (left.resolvedAt ?? 0) - (right.resolvedAt ?? 0) || left.id.localeCompare(right.id));
    for (const artifact of resolved) {
      if (this.retainedArtifacts.size <= target) break;
      this.retainedArtifacts.delete(artifact.id);
      events.push({ type: "agent_artifact_pruned", artifactId: artifact.id });
    }
  }

  private listRetainedArtifacts(actorId: AgentId, args: Record<string, unknown>): { artifacts: RetainedArtifactRecord[]; total: number; nextAfter?: string } {
    const actor = this.requireActor(actorId);
    assertCondition(actor.depth === 0, "CAPABILITY_DENIED", "Only a fabric root may list retained artifacts");
    const limit = this.listLimit(args.limit);
    const after = args.after === undefined ? undefined : parseString(args.after, "after", 16 * 1024);
    const records = [...this.retainedArtifacts.values()].sort((left, right) => left.retainedAt - right.retainedAt || left.id.localeCompare(right.id));
    let start = 0;
    if (after !== undefined) {
      start = records.findIndex((artifact) => artifact.id === after) + 1;
      assertCondition(start > 0, "CURSOR_STALE", `Retained artifact cursor ${after} is stale`);
    }
    const page = records.slice(start, start + limit).map((artifact) => ({ ...artifact, workspace: artifact.workspace ? { ...artifact.workspace } : undefined }));
    return { artifacts: page, total: records.length, ...(start + page.length < records.length && page.length > 0 ? { nextAfter: page[page.length - 1].id } : {}) };
  }

  private resolveArtifact(actorId: AgentId, artifactId: string, rawResolution: unknown, events: CoordinatorEvent[]): RetainedArtifactRecord {
    const actor = this.requireActor(actorId);
    assertCondition(actor.depth === 0, "CAPABILITY_DENIED", "Only a fabric root may resolve retained artifacts");
    const artifact = this.retainedArtifacts.get(artifactId);
    assertCondition(artifact, "INVALID_ARGUMENT", `Retained artifact ${artifactId} was not found`);
    if (artifact.status === "resolved") return { ...artifact, workspace: artifact.workspace ? { ...artifact.workspace } : undefined };
    const resolution = rawResolution === undefined ? undefined : parseString(rawResolution, "resolution", 2048);
    const resolvedAt = this.clock();
    artifact.status = "resolved";
    artifact.resolvedAt = resolvedAt;
    artifact.resolution = resolution;
    events.push({ type: "agent_artifacts_resolved", agentId: artifact.agentId, artifactId, resolvedAt, resolution });
    return { ...artifact, workspace: artifact.workspace ? { ...artifact.workspace } : undefined };
  }

  private updateAgent(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): AgentRecord {
    const agent = this.requireAgent(actorId);
    const next = cloneAgent(agent);
    const requestedStatus = args.status as AgentStatus | undefined;
    if (args.route !== undefined) next.route = this.validateRoute(args.route as ModelRoute);
    const routeChanged = next.route.provider !== agent.route.provider || next.route.model !== agent.route.model || next.route.thinking !== agent.route.thinking || next.route.capacityGroup !== agent.route.capacityGroup;
    assertCondition(!routeChanged || this.findModelTurnWaiter(agent.id) === undefined, "LIFECYCLE_CONFLICT", `Agent ${agent.id} cannot change route while a model turn is queued`);
    if ((requestedStatus === "running" && agent.status !== "running") || (agent.status === "running" && routeChanged)) {
      const runningExcludingSelf = this.runningAgentCount() - (agent.status === "running" ? 1 : 0) + this.grantedModelTurnCount() + this.recoveryTurnReservationCount();
      assertCondition(runningExcludingSelf < this.config.maxConcurrentAgents, "AGENT_LIMIT_REACHED", "maxConcurrentAgents reached");
      this.assertRouteCapacity(next.route, agent.id);
    }
    if (requestedStatus && isTerminal(requestedStatus)) {
      throw new FabricError("LIFECYCLE_CONFLICT", "Use agent.end_turn for terminal transitions so runtime claims are released");
    }
    if (requestedStatus) this.transitionStatus(next, requestedStatus, parseOptionalString(args.statusReason, "statusReason", 2048));
    if (requestedStatus === "running" && next.status === "running" && next.activeTurnOperationId === undefined) {
      assertCondition(!this.recoveryTurnReservations.has(agent.id), "LIFECYCLE_CONFLICT", `Agent ${agent.id} must reconcile its recovered model turn first`);
      next.activeTurnOperationId = this.idFactory("turn");
      next.activeTurnPurpose = "turn";
    } else if (requestedStatus !== undefined && requestedStatus !== "running") {
      next.activeTurnOperationId = undefined;
      next.activeTurnPurpose = undefined;
    }
    assertCondition(args.taskId === undefined, "IDENTITY_CONFLICT", "Use task.claim or task.update to change task ownership");
    if (args.workspace !== undefined) next.workspace = parseWorkspace(args.workspace);
    if (args.contextMode !== undefined) next.contextMode = parseOptionalString(args.contextMode, "contextMode", 128);
    if (args.contextDiagnostic !== undefined) next.contextDiagnostic = parseOptionalString(args.contextDiagnostic, "contextDiagnostic", 2048);
    next.lastActivity = this.clock();
    this.agents.set(actorId, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    if (requestedStatus === "blocked") {
      this.removeModelTurnWaiter(actorId, events);
      this.removeRecoveryTurnReservation(actorId, events);
      this.releaseAgentResourceClaims(actorId, events);
      this.drainModelTurnWaiters(events);
    }
    return publicAgent(next);
  }

  private beginTurn(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): { started: boolean; reason?: string; queued?: boolean } {
    const agent = this.requireAgent(actorId);
    const purpose = this.parseTurnPurpose(args.purpose);
    const operationId = args.operationId === undefined ? undefined : parseString(args.operationId, "operationId", 128);
    // A compaction request made while a normal root turn is running is nested
    // and must not release the outer reservation. Other retries may only claim
    // the active turn when their logical operation identity matches it.
    if (agent.status === "running") {
      if (purpose === "compaction") throw new FabricError("LIFECYCLE_CONFLICT", `Agent ${actorId} is already running`);
      if (operationId !== undefined && agent.activeTurnOperationId !== undefined && operationId !== agent.activeTurnOperationId) {
        throw new FabricError("LIFECYCLE_CONFLICT", `Agent ${actorId} is already running another model turn`);
      }
      if (operationId !== undefined && agent.activeTurnOperationId === undefined) {
        throw new FabricError("LIFECYCLE_CONFLICT", `Agent ${actorId} has no matching active model turn`);
      }
      return { started: true };
    }
    if (isTerminal(agent.status)) throw new FabricError("LIFECYCLE_CONFLICT", `Agent ${actorId} is terminal`);
    assertCondition(agent.status !== "draining", "LIFECYCLE_CONFLICT", `Agent ${actorId} is draining and cannot start another turn`);

    const waiter = this.findModelTurnWaiter(actorId);
    if (waiter) {
      if (waiter.operationId !== undefined && waiter.operationId !== operationId || waiter.purpose !== undefined && waiter.purpose !== purpose) {
        throw new FabricError("LIFECYCLE_CONFLICT", `Agent ${actorId} already has a queued model turn`);
      }
      this.drainModelTurnWaiters(events);
      const current = this.findModelTurnWaiter(actorId);
      if (current?.state === "granted" && (current.operationId === undefined || current.operationId === operationId)) {
        this.modelWaiters.delete(current.id);
        events.push({ type: "model_turn_claimed", waiterId: current.id, agentId: actorId, operationId: current.operationId, purpose: current.purpose });
        this.startTurn(agent, events, current.operationId ?? operationId, current.purpose ?? purpose);
        return { started: true };
      }
      return { started: false, reason: this.turnCapacityReason(agent), queued: true };
    }

    const reason = this.turnCapacityReason(agent);
    if (reason) {
      const waiter: ModelTurnWaiter = {
        id: this.idFactory("model-turn"),
        enqueuedSequence: ++this.nextModelTurnWaiterSequence,
        agentId: actorId,
        route: { ...agent.route },
        capacityKey: modelRouteCapacityKey(this.config, agent.route),
        enqueuedAt: this.clock(),
        operationId,
        purpose,
        state: "queued",
      };
      this.modelWaiters.set(waiter.id, waiter);
      events.push({ type: "model_turn_waiting", waiter: cloneModelTurnWaiter(waiter) });
      // `queued` is only needed by current hosts. Omitting it for direct
      // coordinator callers preserves the legacy false-result shape while the
      // brokered runtime switches from polling to the durable wake event.
      return args.operationId === undefined
        ? { started: false, reason }
        : { started: false, reason, queued: true };
    }

    this.startTurn(agent, events, operationId, purpose);
    return { started: true };
  }

  private parseTurnPurpose(value: unknown): ModelTurnPurpose {
    if (value === undefined) return "turn";
    assertCondition(value === "turn" || value === "compaction", "INVALID_ARGUMENT", "purpose must be turn or compaction");
    return value;
  }

  private startTurn(agent: AgentRecord, events: CoordinatorEvent[], operationId?: string, purpose: ModelTurnPurpose = "turn"): void {
    const next = cloneAgent(agent);
    if (next.status === "starting") next.status = "ready";
    this.transitionStatus(next, "running");
    next.activeTurnOperationId = operationId ?? this.idFactory("turn");
    next.activeTurnPurpose = purpose;
    next.lastActivity = this.clock();
    this.agents.set(next.id, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
  }

  /**
   * Resolve the one provider call that may have survived an independent broker
   * restart. A reconnecting host must explicitly say whether its local
   * session is still streaming before the reservation can be released or
   * converted back into a normal running turn.
   */
  private reconcileTurn(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): { agent: AgentRecord; reconciled: boolean } {
    const agent = this.requireActor(actorId);
    const state = args.state;
    assertCondition(state === "running" || state === "stopped", "INVALID_ARGUMENT", "reconcile state must be running or stopped");
    const operationId = args.operationId === undefined ? undefined : parseString(args.operationId, "operationId", 128);
    const reservation = this.recoveryTurnReservations.get(actorId);
    if (!reservation) return { agent: publicAgent(agent), reconciled: false };
    assertCondition(operationId === undefined || operationId === reservation.operationId, "IDENTITY_CONFLICT", `Agent ${actorId} supplied the wrong recovered model operation`);
    const next = cloneAgent(agent);
    const now = this.clock();
    this.recoveryTurnReservations.delete(actorId);
    events.push({ type: "model_turn_recovery_resolved", agentId: actorId, operationId: reservation.operationId, state });
    if (state === "running") {
      next.activeTurnOperationId = reservation.operationId ?? this.idFactory("recovered-turn");
      next.activeTurnPurpose = reservation.purpose ?? "turn";
      this.transitionStatus(next, "running", undefined);
    } else {
      next.activeTurnOperationId = undefined;
      next.activeTurnPurpose = undefined;
      if (next.status !== "ready") this.transitionStatus(next, "ready");
    }
    next.lastActivity = now;
    this.agents.set(actorId, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    this.drainModelTurnWaiters(events);
    return { agent: publicAgent(next), reconciled: true };
  }

  private turnCapacityReason(agent: AgentRecord): string | undefined {
    if (this.recoveryTurnReservationCount() > 0 && this.recoveryTurnReservations.has(agent.id)) return "Agent must reconcile its recovered model turn before starting new work";
    if (this.runningAgentCount() + this.grantedModelTurnCount() + this.recoveryTurnReservationCount() >= this.config.maxConcurrentAgents) return "maxConcurrentAgents reached";
    const routeLimit = this.routeCapacityLimit(agent.route);
    if (routeLimit !== undefined && this.runningRouteCount(agent.route, agent.id) + this.grantedModelTurnCount(modelRouteCapacityKey(this.config, agent.route)) + this.recoveryTurnReservationCount(modelRouteCapacityKey(this.config, agent.route)) >= routeLimit) {
      return `model route capacity reached for ${modelRouteKey(agent.route)}`;
    }
    return undefined;
  }

  private grantedModelTurnCount(capacityKey?: string): number {
    return [...this.modelWaiters.values()].filter((waiter) => waiter.state === "granted" && (capacityKey === undefined || waiter.capacityKey === capacityKey)).length;
  }

  private recoveryTurnReservationCount(capacityKey?: string): number {
    return [...this.recoveryTurnReservations.values()].filter((reservation) => capacityKey === undefined || reservation.capacityKey === capacityKey).length;
  }

  private findModelTurnWaiter(agentId: AgentId): ModelTurnWaiter | undefined {
    return [...this.modelWaiters.values()].find((waiter) => waiter.agentId === agentId);
  }

  private drainModelTurnWaiters(events: CoordinatorEvent[]): void {
    let changed = true;
    while (changed) {
      changed = false;
      const waiters = [...this.modelWaiters.values()].sort((left, right) => left.enqueuedSequence - right.enqueuedSequence || left.id.localeCompare(right.id));
      for (const waiter of waiters) {
        const agent = this.agents.get(waiter.agentId);
        if (!agent || isTerminal(agent.status) || agent.reconnectable === true || agent.status === "draining") {
          this.modelWaiters.delete(waiter.id);
          events.push({ type: "model_turn_cancelled", waiterId: waiter.id, agentId: waiter.agentId });
          changed = true;
          break;
        }
        if (waiter.state === "granted") continue;
        // Preserve FIFO per physical backend while allowing an unrelated route
        // to use an otherwise free global slot. A grant reserves capacity until
        // this exact host claims it or the short lease expires.
        if ([...this.modelWaiters.values()].some((earlier) => earlier.id !== waiter.id && earlier.state !== "granted" && earlier.capacityKey === waiter.capacityKey && earlier.enqueuedSequence < waiter.enqueuedSequence)) continue;
        if (this.turnCapacityReason(agent)) continue;
        const now = this.clock();
        waiter.state = "granted";
        waiter.grantedAt = now;
        waiter.grantExpiresAt = now + (this.config.modelTurnGrantTtlMs ?? DEFAULT_FABRIC_CONFIG.modelTurnGrantTtlMs!);
        events.push({ type: "model_turn_granted", waiterId: waiter.id, agentId: waiter.agentId, operationId: waiter.operationId, purpose: waiter.purpose, grantedAt: waiter.grantedAt, grantExpiresAt: waiter.grantExpiresAt });
        events.push({ type: "slot_available", agentId: waiter.agentId });
        changed = true;
        break;
      }
    }
  }

  private expireModelTurnGrants(now: number, events: CoordinatorEvent[]): boolean {
    let expired = false;
    for (const waiter of this.modelWaiters.values()) {
      if (waiter.state !== "granted" || (waiter.grantExpiresAt ?? Number.POSITIVE_INFINITY) > now) continue;
      // A lost grant notification must not lose the caller's logical turn.
      // Permit one notification-loss retry, then forfeit priority by moving
      // the ticket to the tail so one dead/nonclaiming waiter cannot
      // repeatedly reacquire a single-slot route ahead of healthy peers.
      waiter.state = "queued";
      waiter.grantExpiryCount = (waiter.grantExpiryCount ?? 0) + 1;
      if (waiter.grantExpiryCount > 1) {
        waiter.enqueuedSequence = ++this.nextModelTurnWaiterSequence;
        waiter.enqueuedAt = now;
      }
      waiter.grantedAt = undefined;
      waiter.grantExpiresAt = undefined;
      events.push({ type: "model_turn_waiting", waiter: cloneModelTurnWaiter(waiter) });
      expired = true;
    }
    return expired;
  }

  private removeModelTurnWaiter(agentId: AgentId, events: CoordinatorEvent[]): void {
    for (const waiter of [...this.modelWaiters.values()]) {
      if (waiter.agentId !== agentId) continue;
      this.modelWaiters.delete(waiter.id);
      events.push({ type: "model_turn_cancelled", waiterId: waiter.id, agentId });
    }
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
    if (effectiveStatus === "completed") this.assertNoLiveDescendants(agent.id);

    // Tear down a failed or cancelled subtree before the parent terminal
    // update becomes observable. This preserves the no-orphan invariant for
    // readers that process the emitted events in order.
    if (effectiveStatus === "failed" || effectiveStatus === "cancelled") {
      this.cancelDescendants(agent.id, effectiveStatus === "cancelled" ? "Parent was cancelled" : "Parent failed", events);
    }

    const next = cloneAgent(agent);
    this.transitionStatus(next, effectiveStatus, parseOptionalString(args.statusReason, "statusReason", 2048));
    next.activeTurnOperationId = undefined;
    next.activeTurnPurpose = undefined;
    if (isTerminal(effectiveStatus)) {
      next.reconnectable = false;
      next.terminalAt = this.clock();
    }
    next.lastActivity = this.clock();
    this.removeModelTurnWaiter(actorId, events);
    // An explicit end_turn is the host's definitive stopped signal for a
    // recovered provider operation when it did not need to resume running.
    this.removeRecoveryTurnReservation(actorId, events);
    this.agents.set(actorId, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });

    // A blocked context/provider turn is deliberately recoverable, but it is
    // not making progress. Release mutable claims immediately so another actor
    // can proceed while the task remains assigned for explicit recovery.
    if (effectiveStatus === "blocked") this.releaseAgentResourceClaims(actorId, events);

    let task = agent.taskId ? cloneTask(this.requireTask(agent.taskId)) : undefined;
    if (isTerminal(effectiveStatus)) {
      this.cancelRequestsFor(actorId, effectiveStatus === "cancelled" ? "cancelled" : "failed", `Agent ${actorId} became ${effectiveStatus}`, events);
      this.markMessagesUndeliverable(actorId, events);
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
    this.drainModelTurnWaiters(events);
    return { agent: publicAgent(this.requireAgent(actorId)), task };
  }

  /**
   * Commit a model-turn outcome as one coordinator transition. Runtime hosts
   * use this instead of separate task.update/agent.end_turn/message.send calls
   * so a broker disconnect cannot expose a half-finished recovery state.
   */
  private finishTurn(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): { agent: AgentRecord; task?: TaskRecord } {
    const taskId = parseOptionalString(args.taskId, "taskId");
    const taskBefore = taskId ? cloneTask(this.requireTask(taskId)) : undefined;
    const taskAction = parseOptionalString(args.taskAction, "taskAction", 32);
    if (taskAction !== undefined) {
      assertCondition(taskId !== undefined, "INVALID_ARGUMENT", "taskAction requires taskId");
      assertCondition(taskAction === "block" || taskAction === "fail", "INVALID_ARGUMENT", "taskAction must be block or fail");
      this.updateTask(actorId, {
        taskId,
        action: taskAction,
        reason: parseString(args.reason, "reason", 4096),
      }, events);
    }

    const requested = (args.status as AgentStatus | undefined) ?? "ready";
    assertCondition(requested === "ready" || requested === "blocked" || requested === "failed", "INVALID_ARGUMENT", "finish status must be ready, blocked, or failed");
    const reason = parseOptionalString(args.statusReason ?? args.reason, "statusReason", 2048);
    const result = this.endTurn(actorId, { status: requested, statusReason: reason }, events);
    const task = result.agent.taskId ? cloneTask(this.requireTask(result.agent.taskId)) : taskId ? cloneTask(this.requireTask(taskId)) : undefined;

    // A late context failure may still be useful to the parent, but only when
    // the task was not already terminal. Completed/failed/cancelled facts are
    // durable semantic outcomes and must not acquire a contradictory notice.
    if (
      requested === "blocked" &&
      result.agent.parentId &&
      !isTaskTerminal(taskBefore?.status ?? "pending") &&
      result.agent.status === "blocked"
    ) {
      const metadata = parseMetadata(args.metadata) ?? {};
      this.sendInternalMessage(actorId, result.agent.parentId, "blocked", reason ?? "Agent is blocked pending recovery", metadata, events);
    }
    return { agent: result.agent, task };
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
    // Heartbeats without leases still refresh the in-memory liveness marker,
    // but do not force a synchronous journal transaction every minute. Lease
    // holders retain the durable agent update alongside their lease renewals.
    if (leases > 0) events.push({ type: "agent_updated", agent: cloneAgent(next) });
    return { agent: publicAgent(next), leases };
  }

  private cancelAgent(actorId: AgentId, targetId: AgentId, events: CoordinatorEvent[]): { cancelled: AgentId[] } {
    const actor = this.requireActor(actorId);
    const target = this.requireAgent(targetId);
    assertCondition(actor.id === target.id || actor.capabilities.maySpawn, "CAPABILITY_DENIED", `Agent ${actorId} cannot cancel descendants`);
    assertCondition(this.canControl(actor, target), "CAPABILITY_DENIED", `Agent ${actorId} cannot cancel ${targetId}`);
    const cancelled: AgentId[] = [];
    this.cancelSubtree(target, `Cancelled by ${actorId}`, events, cancelled);
    this.drainModelTurnWaiters(events);
    return { cancelled };
  }

  /** Enter a bounded, non-terminal shutdown phase for a subtree. */
  private drainAgent(actorId: AgentId, targetId: AgentId, reason: string | undefined, events: CoordinatorEvent[]): { draining: AgentId[] } {
    const actor = this.requireActor(actorId);
    const target = this.requireAgent(targetId);
    assertCondition(actor.id === target.id || actor.capabilities.maySpawn, "CAPABILITY_DENIED", `Agent ${actorId} cannot drain descendants`);
    assertCondition(this.canControl(actor, target), "CAPABILITY_DENIED", `Agent ${actorId} cannot drain ${targetId}`);
    const draining: AgentId[] = [];
    const visit = (agent: AgentRecord): void => {
      for (const child of this.agents.values()) if (child.parentId === agent.id) visit(child);
      if (isTerminal(agent.status) || agent.status === "draining") return;
      const next = cloneAgent(agent);
      this.removeModelTurnWaiter(agent.id, events);
      this.transitionStatus(next, "draining", reason ?? `Draining by ${actorId}`);
      next.lastActivity = this.clock();
      this.agents.set(next.id, next);
      events.push({ type: "agent_updated", agent: cloneAgent(next) });
      draining.push(next.id);
    };
    visit(target);
    this.drainModelTurnWaiters(events);
    return { draining };
  }

  private assertNoLiveDescendants(agentId: AgentId): void {
    const live = [...this.agents.values()].filter((candidate) => this.isReservedLive(candidate) && this.isAncestorAgent(agentId, candidate.id));
    assertCondition(live.length === 0, "LIFECYCLE_CONFLICT", `Agent ${agentId} cannot complete with ${live.length} live descendant${live.length === 1 ? "" : "s"}; drain or cancel them first`);
  }

  private cancelDescendants(parentId: AgentId, reason: string, events: CoordinatorEvent[]): AgentId[] {
    const cancelled: AgentId[] = [];
    for (const child of this.agents.values()) {
      if (child.parentId === parentId) this.cancelSubtree(child, reason, events, cancelled);
    }
    return cancelled;
  }

  private cancelSubtree(agent: AgentRecord, reason: string, events: CoordinatorEvent[], cancelled: AgentId[]): void {
    for (const child of this.agents.values()) if (child.parentId === agent.id) this.cancelSubtree(child, reason, events, cancelled);
    if (isTerminal(agent.status) && !agent.reconnectable) return;
    const next = cloneAgent(agent);
    this.removeRecoveryTurnReservation(next.id, events);
    next.status = "cancelled";
    next.reconnectable = false;
    next.terminalAt = this.clock();
    next.statusReason = reason;
    next.activeTurnOperationId = undefined;
    next.activeTurnPurpose = undefined;
    next.lastActivity = this.clock();
    this.removeModelTurnWaiter(next.id, events);
    this.agents.set(next.id, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    this.cancelRequestsFor(next.id, "cancelled", "Agent was cancelled", events);
    this.markMessagesUndeliverable(next.id, events);
    this.releaseAgentRuntime(next.id, "cancelled", events);
    cancelled.push(next.id);
  }

  private getAgentStatus(actorId: AgentId, requestedId: string | undefined, scope: unknown): unknown {
    const target = requestedId ? this.requireAgent(requestedId) : this.requireActor(actorId);
    const actor = this.requireActor(actorId);
    assertCondition(actor.id === target.id || this.canControl(actor, target) || this.isVisiblePeer(actor, target), "MESSAGE_NOT_VISIBLE", `Agent ${actorId} cannot inspect ${target.id}`);
    if (scope === "tree" || scope === "children") return this.discoverAgents(actorId, { scope });
    return publicAgent(target);
  }

  private discoverAgents(actorId: AgentId, args: Record<string, unknown>): AgentSummary[] {
    const actor = this.requireActor(actorId);
    const scope = args.scope;
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
    const status = args.status === undefined ? undefined : parseString(args.status, "status", 32);
    const filtered = status ? selected.filter((candidate) => candidate.status === status) : selected;
    const sorted = filtered.sort((left, right) => left.createdAt - right.createdAt || left.id.localeCompare(right.id));
    const after = args.after === undefined ? undefined : parseString(args.after, "after", 16 * 1024);
    const anchor = after === undefined ? undefined : this.agentPaginationAnchor(after);
    const start = anchor === undefined ? 0 : sorted.findIndex((candidate) => candidate.createdAt > anchor.createdAt || candidate.createdAt === anchor.createdAt && candidate.id.localeCompare(anchor.id) > 0);
    return sorted.slice(start < 0 ? sorted.length : start, (start < 0 ? sorted.length : start) + this.listLimit(args.limit)).map((candidate) => ({
      ...this.toSummary(candidate),
      cursor: encodePaginationCursor(candidate.createdAt, candidate.id),
    }));
  }

  private agentPaginationAnchor(after: string): { createdAt: number; id: string } {
    const decoded = decodePaginationCursor(after);
    if (decoded) {
      const hot = this.agents.get(decoded.id);
      const archived = this.archivedAgents.get(decoded.id);
      const createdAt = hot?.createdAt ?? archived?.createdAt ?? archived?.terminalAt;
      assertCondition(createdAt !== undefined && createdAt === decoded.createdAt, "CURSOR_STALE", `Agent pagination cursor ${after} is stale`);
      return decoded;
    }
    assertCondition(!after.startsWith("v1:") && !after.startsWith("v2:"), "INVALID_ARGUMENT", "after contains an invalid pagination cursor");
    const hot = this.agents.get(after);
    const archived = this.archivedAgents.get(after);
    assertCondition(hot || archived, "CURSOR_STALE", `Agent pagination cursor ${after} is stale`);
    return { createdAt: hot?.createdAt ?? archived?.createdAt ?? archived?.terminalAt ?? 0, id: after };
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
    if (!request) {
      const archived = this.archivedRequests.get(requestId);
      if (archived) throw new FabricError("REQUEST_ALREADY_RESOLVED", `Request ${requestId} is already ${archived.status}`);
      throw new FabricError("REQUEST_NOT_FOUND", `Request ${requestId} was not found`);
    }
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

  private ackTombstoneKey(to: AgentId, messageId: MessageId, revision: number): string {
    return `${to}\u0000${messageId}\u0000${revision}`;
  }

  private rememberAckTombstone(message: Pick<AgentMessage, "id" | "to" | "revision" | "acknowledgedAt">): MessageAckTombstone | undefined {
    if (message.acknowledgedAt === undefined) return undefined;
    const revision = message.revision ?? 1;
    const tombstone: MessageAckTombstone = {
      id: message.id,
      to: message.to,
      revision,
      acknowledgedAt: message.acknowledgedAt,
      acknowledged: true,
    };
    // Brokered coordinators consult the external cold store instead of keeping
    // an unbounded proof map in every rollback image. The local map remains a
    // compatibility fallback for direct/unit coordinators.
    if (!this.ackProofs) this.acknowledgedMessages.set(this.ackTombstoneKey(tombstone.to, tombstone.id, tombstone.revision), tombstone);
    return tombstone;
  }

  private ackMessage(actorId: AgentId, messageId: string, rawRevision: unknown, events: CoordinatorEvent[]): AgentMessage | MessageAckTombstone {
    const requestedRevision = rawRevision === undefined ? 1 : parseNumber(rawRevision, "revision", 1);
    assertCondition(Number.isInteger(requestedRevision) && requestedRevision > 0, "INVALID_ARGUMENT", "revision must be a positive integer");
    const message = this.messages.get(messageId);
    if (!message) {
      const exact = this.ackProofs?.findExact(actorId, messageId, requestedRevision)
        ?? this.acknowledgedMessages.get(this.ackTombstoneKey(actorId, messageId, requestedRevision));
      if (exact) return { ...cloneAckTombstone(exact), alreadyAcknowledged: true };
      const otherRevision = this.ackProofs?.findMessage(actorId, messageId)
        ?? [...this.acknowledgedMessages.values()].find((candidate) => candidate.id === messageId && candidate.to === actorId);
      if (otherRevision) {
        throw new FabricError("MESSAGE_REVISION_CONFLICT", `Message ${messageId} was acknowledged at revision ${otherRevision.revision}; revision ${requestedRevision} is stale`, {
          messageId,
          expectedRevision: otherRevision.revision,
          receivedRevision: requestedRevision,
        });
      }
      throw new FabricError("MESSAGE_NOT_FOUND", `Message ${messageId} was not found`);
    }
    assertCondition(message.to === actorId, "MESSAGE_NOT_VISIBLE", `Agent ${actorId} cannot acknowledge ${messageId}`);
    const currentRevision = message.revision ?? 1;
    assertCondition(requestedRevision === currentRevision, "MESSAGE_REVISION_CONFLICT", `Message ${messageId} is at revision ${currentRevision}; revision ${requestedRevision} is stale`, {
      messageId,
      expectedRevision: currentRevision,
      receivedRevision: requestedRevision,
    });
    if (message.acknowledgedAt !== undefined) {
      const key = this.ackTombstoneKey(message.to, message.id, currentRevision);
      const hadTombstone = this.acknowledgedMessages.has(key);
      this.rememberAckTombstone(message);
      if (!hadTombstone) events.push({ type: "message_acknowledged", message: cloneMessage(message) });
      return cloneMessage(message);
    }
    message.acknowledgedAt = this.clock();
    if (message.deliveredAt === undefined) message.deliveredAt = message.acknowledgedAt;
    this.rememberAckTombstone(message);
    events.push({ type: "message_acknowledged", message: cloneMessage(message) });
    this.pruneMessages(events);
    return cloneMessage(message);
  }

  private inbox(actorId: AgentId, limit: unknown, afterBrokerSequence?: unknown): AgentMessage[] {
    const max = Math.max(1, Math.min(100, Math.floor(parseNumber(limit, "limit", 50))));
    const after = afterBrokerSequence === undefined ? undefined : parseNumber(afterBrokerSequence, "afterBrokerSequence", 0);
    const pending = [...this.messages.values()]
      .filter((message) => message.to === actorId && message.acknowledgedAt === undefined && message.abandonedAt === undefined)
      .filter((message) => after === undefined || (message.brokerSequence ?? 0) > after);
    if (after !== undefined) {
      return pending
        .sort((left, right) => (left.brokerSequence ?? 0) - (right.brokerSequence ?? 0) || left.id.localeCompare(right.id))
        .slice(0, max)
        .map(cloneMessage);
    }
    return pending
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
    assertCondition(actor.status !== "draining", "LIFECYCLE_CONFLICT", `Agent ${actorId} is draining and cannot create tasks`);
    const description = parseString(args.description, "description", 16 * 1024);
    if (args.dependencies !== undefined) assertCondition(Array.isArray(args.dependencies), "INVALID_ARGUMENT", "dependencies must be an array");
    const dependencies = (args.dependencies as unknown[] | undefined)?.map((id) => parseString(id, "dependency")) ?? [];
    for (const dependency of dependencies) {
      assertCondition(this.hasTask(dependency), "TASK_NOT_FOUND", `Dependency ${dependency} was not found`);
    }
    const parentTaskId = parseOptionalString(args.parentTaskId, "parentTaskId");
    if (parentTaskId) assertCondition(this.hasTask(parentTaskId), "TASK_NOT_FOUND", `Parent task ${parentTaskId} was not found`);
    const owner = parseOptionalString(args.owner, "owner");
    if (owner) {
      const ownerAgent = this.requireAgent(owner);
      assertCondition(!isTerminal(ownerAgent.status), "AGENT_NOT_FOUND", `Agent ${owner} is not active`);
      assertCondition(this.canControl(actor, ownerAgent) || owner === actorId, "CAPABILITY_DENIED", `Agent ${actorId} cannot assign a task to ${owner}`);
      assertCondition(!ownerAgent.taskId || ownerAgent.taskId === undefined, "TASK_BUSY", `Agent ${owner} already has a primary task`);
    }
    const id = this.idFactory("task");
    const now = this.clock();
    const ready = dependencies.every((dependency) => this.taskStatus(dependency) === "completed");
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
    assertCondition(actor.status !== "draining", "LIFECYCLE_CONFLICT", `Agent ${actorId} is draining and cannot claim tasks`);
    const task = this.requireTask(taskId);
    if (task.owner) {
      const currentOwner = this.agents.get(task.owner);
      if (currentOwner && isTerminal(currentOwner.status) && currentOwner.reconnectable !== true) {
        task.owner = undefined;
        task.status = "ready";
        task.updatedAt = this.clock();
      }
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
        if (isTaskTerminal(task.status)) return cloneTask(task);
        task.status = "blocked";
        task.blockedReason = parseString(args.reason, "reason", 4096);
        task.updatedAt = this.clock();
        events.push({ type: "task_changed", task: cloneTask(task) });
        if (task.owner) this.releaseAgentResourceClaims(task.owner, events);
        return cloneTask(task);
      case "ready":
      case "reopen":
        assertCondition(!isTaskTerminal(task.status) || action === "reopen", "LIFECYCLE_CONFLICT", `Task ${task.id} cannot be reopened from ${task.status}`);
        if (action === "reopen" && task.status === "completed") {
          const dependents = this.taskDependents(task.id);
          assertCondition(
            dependents.length === 0,
            "LIFECYCLE_CONFLICT",
            `Task ${task.id} cannot be reopened while ${dependents.length} dependent task${dependents.length === 1 ? " is" : "s are"} still active`,
            { taskId: task.id, dependentTaskIds: dependents.map((candidate) => candidate.id) },
          );
        }
        assertCondition(this.taskDependenciesCompleted(task), "TASK_BLOCKED", `Task ${task.id} dependencies are not complete`);
        task.status = task.owner ? "active" : "ready";
        task.blockedReason = undefined;
        task.updatedAt = this.clock();
        events.push({ type: "task_changed", task: cloneTask(task) });
        return cloneTask(task);
      case "cancel":
        if (isTaskTerminal(task.status)) return cloneTask(task);
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
        if (isTaskTerminal(task.status)) return cloneTask(task);
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
    if (task.owner) this.assertNoLiveDescendants(task.owner);
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
    const status = args.status === undefined ? undefined : parseString(args.status, "status", 32);
    const owner = args.owner === undefined ? undefined : parseString(args.owner, "owner", 512);
    const sorted = [...this.tasks.values()]
      .filter((task) => includeAll || task.owner === actorId || task.creator === actorId || Boolean(task.parentTaskId && this.taskVisibleTo(actorId, task)))
      .filter((task) => status === undefined || task.status === status)
      .filter((task) => owner === undefined || task.owner === owner)
      .sort((left, right) => left.createdAt - right.createdAt || left.id.localeCompare(right.id));
    const after = args.after === undefined ? undefined : parseString(args.after, "after", 16 * 1024);
    const anchor = after === undefined ? undefined : this.taskPaginationAnchor(after);
    const start = anchor === undefined ? 0 : sorted.findIndex((task) => task.createdAt > anchor.createdAt || task.createdAt === anchor.createdAt && task.id.localeCompare(anchor.id) > 0);
    const offset = start < 0 ? sorted.length : start;
    return sorted.slice(offset, offset + this.listLimit(args.limit)).map((task) => ({
      ...cloneTask(task),
      cursor: encodePaginationCursor(task.createdAt, task.id),
    }));
  }

  private taskPaginationAnchor(after: string): { createdAt: number; id: string } {
    const decoded = decodePaginationCursor(after);
    if (decoded) {
      const hot = this.tasks.get(decoded.id);
      const archived = this.archivedTasks.get(decoded.id);
      const createdAt = hot?.createdAt ?? archived?.createdAt ?? archived?.updatedAt;
      assertCondition(createdAt !== undefined && createdAt === decoded.createdAt, "CURSOR_STALE", `Task pagination cursor ${after} is stale`);
      return decoded;
    }
    assertCondition(!after.startsWith("v1:") && !after.startsWith("v2:"), "INVALID_ARGUMENT", "after contains an invalid pagination cursor");
    const hot = this.tasks.get(after);
    const archived = this.archivedTasks.get(after);
    assertCondition(hot || archived, "CURSOR_STALE", `Task pagination cursor ${after} is stale`);
    return { createdAt: hot?.createdAt ?? archived?.createdAt ?? archived?.updatedAt ?? 0, id: after };
  }

  private showTask(actorId: AgentId, taskId: TaskId): TaskRecord {
    const task = this.requireTask(taskId);
    const actor = this.requireActor(actorId);
    assertCondition(actor.depth === 0 || task.owner === actorId || task.creator === actorId || Boolean(task.parentTaskId && this.taskVisibleTo(actorId, task)), "MESSAGE_NOT_VISIBLE", `Task ${taskId} is not visible to ${actorId}`);
    return cloneTask(task);
  }

  private listLimit(value: unknown): number {
    return Math.max(1, Math.min(100, Math.floor(parseNumber(value, "limit", 100))));
  }

  private defineResource(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): ResourceRecord {
    const actor = this.requireActor(actorId);
    assertCondition(actor.status !== "draining", "LIFECYCLE_CONFLICT", `Agent ${actorId} is draining and cannot define resources`);
    assertCondition(actor.capabilities.mayWriteRepo || actor.capabilities.mayTransferOwnership, "CAPABILITY_DENIED", `Agent ${actorId} cannot define resources`);
    const id = parseString(args.resourceId, "resourceId", 1024);
    const kind = parseString(args.kind ?? "resource", "kind", 128);
    const parentId = parseOptionalString(args.parentId, "parentId", 1024);
    const resourcePath = args.path === undefined ? undefined : normalizeResourcePath(args.path, "path", this.caseFoldPaths);
    const existing = this.resources.get(id);
    if (existing) {
      assertCondition(existing.kind === kind && existing.parentId === parentId && (args.path === undefined || existing.path === resourcePath), "IDENTITY_CONFLICT", `Resource ${id} already has a different definition`);
      return cloneResource(existing);
    }
    assertCondition(!this.archivedResources.has(id), "IDENTITY_CONFLICT", `Resource ${id} is a retired identity and cannot be reused`);
    if (parentId) {
      assertCondition(parentId !== id && this.resources.has(parentId), "RESOURCE_NOT_FOUND", `Parent resource ${parentId} was not found`);
      assertCondition(this.isResourceActive(this.requireResource(parentId)), "LIFECYCLE_CONFLICT", `Parent resource ${parentId} is retired`);
      assertCondition(!this.wouldCreateResourceCycle(id, parentId), "INVALID_ARGUMENT", `Resource ${id} would create a hierarchy cycle`);
    }
    const permissions = this.parsePermissions(args.permissions);
    const now = this.clock();
    const resource: ResourceRecord = {
      id,
      incarnation: this.idFactory("resource-incarnation"),
      kind,
      parentId,
      path: resourcePath,
      owner: actorId,
      status: "active",
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

  private resourceSnapshot(actorId: AgentId, resourceId: ResourceId): { resourceId: ResourceId; incarnation: string; version: number; token: string } {
    const resource = this.requireResource(resourceId);
    assertCondition(this.canInspectResource(actorId, resource), "CAPABILITY_DENIED", `Agent ${actorId} cannot inspect ${resourceId}`);
    const incarnation = this.resourceIncarnation(resource);
    return {
      resourceId,
      incarnation,
      version: resource.version,
      token: `${resourceId}@${incarnation}@${resource.version}`,
    };
  }

  private listResources(actorId: AgentId, args: Record<string, unknown>): Array<ResourceRecord & { cursor?: string }> {
    const allResources = [
      ...this.resources.values(),
      ...[...this.archivedResources.values()].map(resourceFromTombstone),
    ];
    const sorted = allResources
      .filter((resource) => this.canInspectResource(actorId, resource))
      .sort((left, right) => left.createdAt - right.createdAt || left.id.localeCompare(right.id));
    const after = args.after === undefined ? undefined : parseString(args.after, "after", 16 * 1024);
    const anchor = after === undefined ? undefined : this.resourcePaginationAnchor(after);
    const start = anchor === undefined ? 0 : sorted.findIndex((resource) => resource.createdAt > anchor.createdAt || resource.createdAt === anchor.createdAt && resource.id.localeCompare(anchor.id) > 0);
    const offset = start < 0 ? sorted.length : start;
    return sorted.slice(offset, offset + this.listLimit(args.limit)).map((resource) => ({
      ...cloneResource(resource),
      cursor: encodePaginationCursor(resource.createdAt, resource.id),
    }));
  }

  private resourcePaginationAnchor(after: string): { createdAt: number; id: string } {
    const decoded = decodePaginationCursor(after);
    if (decoded) {
      const resource = this.resources.get(decoded.id) ?? (this.archivedResources.has(decoded.id) ? resourceFromTombstone(this.archivedResources.get(decoded.id) as ResourceTombstone) : undefined);
      assertCondition(resource !== undefined && resource.createdAt === decoded.createdAt, "CURSOR_STALE", `Resource pagination cursor ${after} is stale`);
      return decoded;
    }
    assertCondition(!after.startsWith("v1:") && !after.startsWith("v2:"), "INVALID_ARGUMENT", "after contains an invalid pagination cursor");
    const resource = this.resources.get(after) ?? (this.archivedResources.has(after) ? resourceFromTombstone(this.archivedResources.get(after) as ResourceTombstone) : undefined);
    assertCondition(resource !== undefined, "CURSOR_STALE", `Resource pagination cursor ${after} is stale`);
    return { createdAt: resource.createdAt, id: resource.id };
  }

  private retireResource(actorId: AgentId, resourceId: ResourceId, events: CoordinatorEvent[]): ResourceRecord {
    const actor = this.requireActor(actorId);
    assertCondition(actor.depth === 0, "CAPABILITY_DENIED", "Only a fabric root may retire resources");
    const resource = this.requireResource(resourceId);
    if (!this.isResourceActive(resource)) return cloneResource(resource);
    assertCondition(resource.sharedHolds.length === 0 && resource.mutableHold === undefined, "RESOURCE_CONFLICT", `Cannot retire ${resource.id} while it is held`);
    assertCondition(resource.waiters.length === 0, "RESOURCE_CONFLICT", `Cannot retire ${resource.id} while borrow requests are waiting`);
    assertCondition((resource.writeQuarantineUntil ?? 0) <= this.clock(), "RESOURCE_CONFLICT", `Cannot retire ${resource.id} while a write quarantine is active`);
    assertCondition(![...this.fences.values()].some((fence) => fence.expiresAt > this.clock() && this.overlaps(fence.resourceId, resource.id)), "RESOURCE_CONFLICT", `Cannot retire ${resource.id} while a write fence is active`);
    assertCondition(![...this.resources.values()].some((candidate) => candidate.id !== resource.id && this.isResourceActive(candidate) && (candidate.parentId === resource.id || this.isAncestor(resource.id, candidate.id))), "RESOURCE_CONFLICT", `Cannot retire ${resource.id} while active child resources remain`);
    resource.status = "retired";
    resource.writeQuarantineUntil = undefined;
    resource.writeQuarantineActorId = undefined;
    resource.writeQuarantineFenceId = undefined;
    resource.retiredAt = this.clock();
    resource.version += 1;
    resource.updatedAt = resource.retiredAt;
    events.push({ type: "resource_changed", resource: cloneResource(resource) });
    return cloneResource(resource);
  }

  private grantResource(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): ResourceRecord {
    const resource = this.requireResource(parseString(args.resourceId, "resourceId"));
    assertCondition(this.isResourceActive(resource), "LIFECYCLE_CONFLICT", `Resource ${resource.id} is retired`);
    const actor = this.requireActor(actorId);
    assertCondition(actor.status !== "draining", "LIFECYCLE_CONFLICT", `Agent ${actorId} is draining and cannot grant resources`);
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
    assertCondition(this.isResourceActive(resource), "LIFECYCLE_CONFLICT", `Resource ${resource.id} is retired`);
    const actor = this.requireActor(actorId);
    assertCondition(actor.status !== "draining", "LIFECYCLE_CONFLICT", `Agent ${actorId} is draining and cannot claim resources`);
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
    if (resource.owner === actorId) {
      // A same-owner claim is an idempotent reaffirmation. Owner permissions are
      // implicit, so bumping the version here would only emit false stale-
      // dependency signals for consumers that snapshot resourceId@version.
      return cloneResource(resource);
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
    assertCondition(this.isResourceActive(resource), "LIFECYCLE_CONFLICT", `Resource ${resource.id} is retired`);
    const actor = this.requireActor(actorId);
    assertCondition(actor.status !== "draining", "LIFECYCLE_CONFLICT", `Agent ${actorId} is draining and cannot borrow resources`);
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
    assertCondition(this.isResourceActive(resource), "LIFECYCLE_CONFLICT", `Resource ${resource.id} is retired`);
    const actor = this.requireActor(actorId);
    assertCondition(actor.status !== "draining", "LIFECYCLE_CONFLICT", `Agent ${actorId} is draining and cannot transfer resources`);
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
    assertCondition(args.all === undefined || typeof args.all === "boolean", "INVALID_ARGUMENT", "all must be a boolean");
    const all = args.all === true;
    // Releasing every hold of an actor is deliberate, never the accidental
    // result of forgetting a selector: exactly one of resourceId, leaseId, or
    // all=true is required.
    const selectors = (resourceId !== undefined ? 1 : 0) + (leaseId !== undefined ? 1 : 0) + (all ? 1 : 0);
    assertCondition(selectors === 1, "INVALID_ARGUMENT", "resource.release requires exactly one of resourceId, leaseId, or all=true");
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
    // The fabric root may participate in borrowing through the host guard: it
    // authorizes an external write purely by the absence of conflicting holds
    // instead of requiring its own declared resource and mutable hold. Only
    // the depth-0 root may claim this exemption; children keep the strict rule.
    const hostGuard = args.hostGuard === true;
    assertCondition(!hostGuard || actor.depth === 0, "CAPABILITY_DENIED", `Agent ${actorId} cannot use the root host write guard`);
    if (!actor.capabilities.mayWriteRepo) {
      return { allowed: false, reason: `Agent ${actorId} is not allowed to write repository files` };
    }
    const resourceId = parseOptionalString(args.resourceId, "resourceId");
    const requestedPath = args.path === undefined ? undefined : normalizeResourcePath(args.path, "path", this.caseFoldPaths);
    assertCondition(resourceId || requestedPath, "INVALID_ARGUMENT", "resource.check_write requires resourceId or path");

    const selectedResource = resourceId ? this.requireResource(resourceId) : undefined;
    if (selectedResource && !this.isResourceActive(selectedResource)) {
      return { allowed: false, resourceId: selectedResource.id, reason: `Resource ${selectedResource.id} is retired` };
    }
    const candidates = requestedPath
      ? this.resourcesForPath(requestedPath)
      : selectedResource ? [selectedResource] : [];
    if (selectedResource && requestedPath && !candidates.some((candidate) => candidate.id === selectedResource.id)) {
      return { allowed: false, resourceId: selectedResource.id, reason: `Resource ${selectedResource.id} does not declare ${requestedPath}` };
    }
    if (candidates.length === 0) {
      if (hostGuard) return { allowed: true };
      return { allowed: false, reason: `No declared resource matches ${requestedPath}` };
    }
    const fenced = candidates.find((resource) => this.activeForeignFence(resource, actorId));
    if (fenced) {
      return {
        allowed: false,
        resourceId: fenced.id,
        reason: `An in-flight write prevents writing ${requestedPath ?? fenced.id}`,
      };
    }
    const quarantined = candidates.find((resource) => this.activeForeignWriteQuarantine(resource, actorId));
    if (quarantined) {
      return {
        allowed: false,
        resourceId: quarantined.id,
        reason: `A broker-restart write quarantine prevents writing ${requestedPath ?? quarantined.id}`,
      };
    }
    const conflicting = requestedPath === undefined ? undefined : candidates.find((resource) => resource.sharedHolds.length > 0 || resource.mutableHold && resource.mutableHold.agentId !== actorId);
    if (conflicting) {
      return { allowed: false, resourceId: conflicting.id, reason: `A conflicting runtime hold prevents writing ${requestedPath}` };
    }
    if (hostGuard) return { allowed: true, resourceId: candidates[0].id };
    const allowed = candidates.find((resource) => this.hasMutableHold(resource, actorId));
    if (allowed) return { allowed: true, resourceId: allowed.id };
    return {
      allowed: false,
      resourceId: candidates[0].id,
      reason: `Agent ${actorId} does not hold mutable access to ${candidates[0].id}`,
    };
  }

  /**
   * Authorize a write exactly like `resource.check_write` and, when allowed,
   * place a short-lived fence on the matched resource. While the fence is
   * active no conflicting lease may be granted to another actor, so a hold
   * that lapses mid-write cannot immediately hand the same file to a
   * competing writer. Undeclared paths (root host-guard freedom) stay allowed
   * with no fence because there is no resource to protect.
   */
  private beginWrite(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): { allowed: boolean; reason?: string; resourceId?: string; fenceId?: string; expiresAt?: number } {
    const decision = this.checkWrite(actorId, args);
    if (!decision.allowed || decision.resourceId === undefined) return decision;
    const fenceMs = Math.max(1_000, Math.min(120_000, Math.floor(parseNumber(args.fenceMs, "fenceMs", 30_000))));
    const now = this.clock();
    const fence: WriteFenceRecord = {
      id: this.idFactory("fence"),
      resourceId: decision.resourceId,
      actorId,
      createdAt: now,
      expiresAt: now + fenceMs,
    };
    this.fences.set(fence.id, fence);
    const resource = this.resources.get(decision.resourceId);
    if (resource) {
      resource.writeQuarantineUntil = fence.expiresAt;
      resource.writeQuarantineActorId = actorId;
      resource.writeQuarantineFenceId = fence.id;
      resource.updatedAt = now;
      events.push({ type: "resource_changed", resource: cloneResource(resource) });
    }
    return { ...decision, fenceId: fence.id, expiresAt: fence.expiresAt };
  }

  /**
   * Lift a fence early. Only the fencing actor may release it, ending is
   * idempotent, and waiters excluded by the fence are drained on release.
   */
  private endWrite(actorId: AgentId, args: Record<string, unknown>, events: CoordinatorEvent[]): { released: boolean } {
    const fenceId = parseString(args.fenceId, "fenceId", 512);
    const fence = this.fences.get(fenceId);
    const released = fence !== undefined && fence.actorId === actorId && this.fences.delete(fenceId);
    // A fence object is intentionally not restored across broker restart, but
    // the original authenticated actor may still complete its old write after
    // reconnecting. Let that end_write clear the durable quarantine even when
    // the in-memory fence record is already gone.
    const now = this.clock();
    const quarantined = !released
      ? [...this.resources.values()].find((resource) => resource.writeQuarantineFenceId === fenceId && resource.writeQuarantineActorId === actorId && (resource.writeQuarantineUntil ?? 0) > now)
      : undefined;
    const resource = released ? this.resources.get(fence.resourceId) : quarantined;
    if ((released || quarantined) && resource?.writeQuarantineFenceId === fenceId) {
      resource.writeQuarantineUntil = undefined;
      resource.writeQuarantineActorId = undefined;
      resource.writeQuarantineFenceId = undefined;
      resource.updatedAt = now;
      events.push({ type: "resource_changed", resource: cloneResource(resource) });
    }
    if (released || quarantined) this.drainWaiters(events);
    return { released: Boolean(released || quarantined) };
  }

  private activeForeignFence(resource: ResourceRecord, actorId: AgentId): boolean {
    const now = this.clock();
    for (const fence of this.fences.values()) {
      if (fence.expiresAt <= now || fence.actorId === actorId) continue;
      if (this.overlaps(fence.resourceId, resource.id)) return true;
    }
    return false;
  }

  private activeForeignWriteQuarantine(resource: ResourceRecord, actorId: AgentId): boolean {
    const now = this.clock();
    for (const candidate of this.resources.values()) {
      if ((candidate.writeQuarantineUntil ?? 0) <= now || candidate.writeQuarantineActorId === actorId) continue;
      if (this.overlaps(candidate.id, resource.id)) return true;
    }
    return false;
  }

  private status(actorId: AgentId, _args: Record<string, unknown>): FabricStatus {
    const actor = this.requireActor(actorId);
    assertCondition(actor.depth === 0, "CAPABILITY_DENIED", "Only a fabric root may request full status");
    const now = this.clock();
    const agents = [...this.agents.values()].sort((left, right) => left.createdAt - right.createdAt || left.id.localeCompare(right.id));
    const tasks = [...this.tasks.values()].sort((left, right) => left.createdAt - right.createdAt || left.id.localeCompare(right.id));
    const resources = [
      ...this.resources.values(),
      ...[...this.archivedResources.values()].map(resourceFromTombstone),
    ].sort((left, right) => {
      const leftContention = (left.mutableHold ? 4 : 0) + (left.sharedHolds.length > 0 ? 2 : 0) + (left.waiters.length > 0 ? 1 : 0) + ((left.writeQuarantineUntil ?? 0) > now ? 2 : 0);
      const rightContention = (right.mutableHold ? 4 : 0) + (right.sharedHolds.length > 0 ? 2 : 0) + (right.waiters.length > 0 ? 1 : 0) + ((right.writeQuarantineUntil ?? 0) > now ? 2 : 0);
      return rightContention - leftContention || left.createdAt - right.createdAt || left.id.localeCompare(right.id);
    });
    const pendingRequests = [...this.requests.values()]
      .filter((request) => request.status === "pending")
      .sort((left, right) => left.createdAt - right.createdAt || left.id.localeCompare(right.id));
    const recentMessages = [...this.messages.values()]
      .sort((left, right) => (right.brokerSequence ?? 0) - (left.brokerSequence ?? 0) || right.createdAt - left.createdAt || right.id.localeCompare(left.id));
    const fences = [...this.fences.values()].filter((candidate) => candidate.expiresAt > now);
    const boundedLimit = 50;
    return {
      rootId: this.rootId,
      rootAgentId: (this.rootAgentId ? this.agents.get(this.rootAgentId) : agents.find((candidate) => candidate.depth === 0))?.id,
      agents: agents.slice(0, boundedLimit).map((candidate) => this.toBoundedSummary(candidate)),
      tasks: tasks.slice(0, boundedLimit).map((task) => this.toBoundedTask(task)),
      resources: resources.slice(0, boundedLimit).map((resource) => this.toBoundedResource(resource)),
      pendingRequests: pendingRequests.slice(0, boundedLimit).map((request) => this.toBoundedRequest(request)),
      recentMessages: recentMessages.slice(0, 20).map((message) => this.toBoundedMessage(message)),
      runningChildren: agents.filter((candidate) => candidate.depth > 0 && candidate.status === "running").length,
      totalAgents: agents.length,
      totalTasks: tasks.length,
      totalResources: resources.length,
      totalPendingRequests: pendingRequests.length,
      totalRecentMessages: recentMessages.length,
      config: this.boundedConfig(),
      activeFences: fences.length,
      activeMutableHolds: resources.filter((resource) => this.isResourceActive(resource) && resource.mutableHold !== undefined).length,
      fences: fences.slice(0, boundedLimit).map((fence) => {
        const res = this.resources.get(fence.resourceId);
        return {
          id: fence.id,
          resourceId: fence.resourceId,
          path: res?.path,
          actorId: fence.actorId,
        };
      }),
      activeWriteQuarantines: [...this.resources.values()].filter((resource) => this.isResourceActive(resource) && (resource.writeQuarantineUntil ?? 0) > now).length,
      pendingModelTurns: [...this.modelWaiters.values()].sort((left, right) => left.enqueuedSequence - right.enqueuedSequence).slice(0, boundedLimit).map(cloneModelTurnWaiter),
      recoveryTurnReservations: [...this.recoveryTurnReservations.values()].slice(0, boundedLimit).map(cloneModelTurnRecoveryReservation),
      archivedCounts: {
        agents: this.archivedAgents.size,
        tasks: this.archivedTasks.size,
        requests: this.archivedRequests.size,
        resources: this.archivedResources.size,
      },
      truncated: {
        agents: agents.length > boundedLimit,
        tasks: tasks.length > boundedLimit,
        resources: resources.length > boundedLimit,
        pendingRequests: pendingRequests.length > boundedLimit,
        recentMessages: recentMessages.length > 20,
      },
    };
  }

  private snapshot(actorId: AgentId): FabricSnapshot {
    const actor = this.requireActor(actorId);
    assertCondition(actor.depth === 0, "CAPABILITY_DENIED", "Only a fabric root may request a fabric snapshot");
    const now = this.clock();
    const root = this.rootAgentId ? this.agents.get(this.rootAgentId) : [...this.agents.values()].find((candidate) => candidate.depth === 0);
    const unresolvedStatuses = new Set<TaskStatus>(["pending", "ready", "active", "waiting", "blocked"]);
    const unresolvedTasks = [...this.tasks.values()].filter((task) => unresolvedStatuses.has(task.status));
    const pendingRequests = [...this.requests.values()].filter((request) => request.status === "pending");
    const activeWriteFences = [...this.fences.values()].filter((fence) => fence.expiresAt > now);
    const mutableResources = [...this.resources.values()]
      .filter((resource) => this.isResourceActive(resource) && resource.mutableHold !== undefined)
      .sort((left, right) => left.updatedAt - right.updatedAt || left.id.localeCompare(right.id));
    return {
      rootId: this.rootId,
      rootAgentId: root?.id,
      rootStatus: root?.status,
      caseInsensitivePaths: this.caseFoldPaths,
      runningChildren: [...this.agents.values()].filter((candidate) => candidate.depth > 0 && ["starting", "running", "draining"].includes(candidate.status)).length,
      recoveringAgents: [...this.agents.values()].filter((candidate) => candidate.reconnectable === true).length,
      unresolvedChildTasks: unresolvedTasks.length,
      unownedUnresolvedTasks: unresolvedTasks.filter((task) => task.owner === undefined).length,
      mutableHolds: mutableResources.length,
      activeWriteFences: activeWriteFences.length,
      activeWriteQuarantines: [...this.resources.values()].filter((resource) => this.isResourceActive(resource) && (resource.writeQuarantineUntil ?? 0) > now).length,
      pendingRootRequests: root ? pendingRequests.filter((request) => request.to === root.id || request.from === root.id).length : 0,
      pendingModelTurns: this.modelWaiters.size,
      fences: activeWriteFences.slice(0, 50).map((fence) => {
        const resource = this.resources.get(fence.resourceId);
        return { id: fence.id, resourceId: fence.resourceId, path: resource?.path, actorId: fence.actorId };
      }),
      activeTasks: unresolvedTasks.slice(0, 50).map((task) => ({ id: task.id, status: task.status, owner: task.owner, description: task.description.slice(0, 1024) })),
      mutableResources: mutableResources.slice(0, 50).map((resource) => ({ id: resource.id, path: resource.path, holder: resource.mutableHold?.agentId })),
    };
  }

  private boundedConfig(): FabricConfig {
    // Route-policy maps are configuration, not quiescence state. Omitting
    // them from the diagnostic projection prevents a large user config from
    // defeating the broker's frame bound; the authoritative config remains in
    // the broker process and checkpoints.
    return {
      caseInsensitivePaths: this.config.caseInsensitivePaths,
      maxDepth: this.config.maxDepth,
      maxChildrenPerAgent: this.config.maxChildrenPerAgent,
      maxChildrenCreatedPerAgent: this.config.maxChildrenCreatedPerAgent,
      maxTotalAgents: this.config.maxTotalAgents,
      maxConcurrentAgents: this.config.maxConcurrentAgents,
      maxMailboxMessages: this.config.maxMailboxMessages,
      maxMessageBody: this.config.maxMessageBody,
      maxTaskOutput: this.config.maxTaskOutput,
      leaseMs: this.config.leaseMs,
      heartbeatMs: this.config.heartbeatMs,
      agentHeartbeatTimeoutMs: this.config.agentHeartbeatTimeoutMs,
      reconnectGraceMs: this.config.reconnectGraceMs,
      modelTurnGrantTtlMs: this.config.modelTurnGrantTtlMs,
      messageRetention: this.config.messageRetention,
      historyRetentionMs: this.config.historyRetentionMs,
      maxArchivedRecords: this.config.maxArchivedRecords,
      maxRetainedArtifacts: this.config.maxRetainedArtifacts,
      historyGcBatchSize: this.config.historyGcBatchSize,
    };
  }

  private toBoundedSummary(agent: AgentRecord): AgentSummary {
    const summary = this.toSummary(agent, false);
    if (summary.contextDiagnostic) summary.contextDiagnostic = summary.contextDiagnostic.slice(0, 1024);
    summary.capabilities = {
      maySpawn: agent.capabilities.maySpawn,
      mayMessagePeers: agent.capabilities.mayMessagePeers,
      mayEscalate: agent.capabilities.mayEscalate,
      mayTransferOwnership: agent.capabilities.mayTransferOwnership,
      mayWriteRepo: agent.capabilities.mayWriteRepo,
      mayUseShell: agent.capabilities.mayUseShell,
      peerIds: agent.capabilities.peerIds.slice(0, 16).map((id) => id.slice(0, 128)),
      resourceGrants: {},
    };
    return summary;
  }

  private toBoundedTask(task: TaskRecord): TaskRecord {
    return {
      id: task.id,
      description: task.description.slice(0, 1024),
      owner: task.owner,
      creator: task.creator,
      parentTaskId: task.parentTaskId,
      dependencies: (task.dependencies ?? []).slice(0, 50),
      status: task.status,
      result: task.result ? {
        summary: task.result.summary.slice(0, 1024),
        output: task.result.output?.slice(0, 1024),
        completedAt: task.result.completedAt,
        by: task.result.by,
      } : undefined,
      blockedReason: task.blockedReason?.slice(0, 1024),
      createdAt: task.createdAt,
      updatedAt: task.updatedAt,
    };
  }

  private toBoundedResource(resource: ResourceRecord): ResourceRecord {
    const boundedHold = (hold: ResourceHold): ResourceHold => ({
      leaseId: hold.leaseId.slice(0, 128),
      agentId: hold.agentId.slice(0, 128),
      mode: hold.mode,
      acquiredAt: hold.acquiredAt,
      lastHeartbeat: hold.lastHeartbeat,
      expiresAt: hold.expiresAt,
      leaseMs: hold.leaseMs,
    });
    const boundedWaiter = (waiter: ResourceWaiter): ResourceWaiter => ({
      requestId: waiter.requestId.slice(0, 128),
      enqueuedSequence: waiter.enqueuedSequence,
      agentId: waiter.agentId.slice(0, 128),
      mode: waiter.mode,
      enqueuedAt: waiter.enqueuedAt,
      leaseMs: waiter.leaseMs,
    });
    return {
      id: resource.id,
      incarnation: this.resourceIncarnation(resource),
      kind: resource.kind,
      parentId: resource.parentId,
      path: resource.path?.slice(0, 1024),
      owner: resource.owner,
      version: resource.version,
      status: resource.status ?? "active",
      retiredAt: resource.retiredAt,
      grants: {},
      sharedHolds: resource.sharedHolds.slice(0, 8).map(boundedHold),
      mutableHold: resource.mutableHold ? boundedHold(resource.mutableHold) : undefined,
      waiters: resource.waiters.slice(0, 8).map(boundedWaiter),
      writeQuarantineUntil: resource.writeQuarantineUntil,
      writeQuarantineActorId: resource.writeQuarantineActorId?.slice(0, 128),
      writeQuarantineFenceId: resource.writeQuarantineFenceId?.slice(0, 128),
      createdAt: resource.createdAt,
      updatedAt: resource.updatedAt,
    };
  }

  private toBoundedRequest(request: RequestRecord): RequestRecord {
    const bounded = cloneRequest(request);
    if (bounded.failureReason) bounded.failureReason = bounded.failureReason.slice(0, 1024);
    return bounded;
  }

  private toBoundedMessage(message: AgentMessage): AgentMessage {
    return {
      id: message.id,
      from: message.from,
      to: message.to,
      type: message.type,
      body: message.body.slice(0, 2048),
      revision: message.revision,
      senderSequence: message.senderSequence,
      brokerSequence: message.brokerSequence,
      requestId: message.requestId,
      replyTo: message.replyTo,
      priority: message.priority,
      createdAt: message.createdAt,
      clientDedupeKey: message.clientDedupeKey?.slice(0, 128),
      deliveredAt: message.deliveredAt,
      acknowledgedAt: message.acknowledgedAt,
      abandonedAt: message.abandonedAt,
    };
  }

  /** Run the time-based maintenance transition without requiring an actor request. */
  maintenance(options: { skipStaleAgents?: boolean; skipHistoricalArchival?: boolean } = {}): DispatchResult<null> {
    const events: CoordinatorEvent[] = [];
    const now = this.clock();
    this.reclaimExpired(now, events);
    this.reclaimStaleAgents(now, events, options.skipStaleAgents !== true);
    // A long event-loop suspension can make a large history eligible at once.
    // Give live hosts one tick to reattach before doing bounded archival work.
    if (options.skipHistoricalArchival !== true) this.archiveHistoricalRecords(now, events);
    return { value: null, events };
  }

  /**
   * Bound broker-only liveness failures. A stale actor first becomes
   * reconnectable and releases runtime claims while retaining its task link
   * for a bounded reattach window. If it never reconnects, retire its subtree
   * and return unfinished tasks to the ready pool so capacity cannot remain
   * reserved forever.
   */
  private reclaimStaleAgents(now: number, events: CoordinatorEvent[], allowNewStale = true): void {
    const heartbeatTimeout = this.config.agentHeartbeatTimeoutMs ?? this.config.heartbeatMs * 3;
    const reconnectGrace = this.config.reconnectGraceMs ?? heartbeatTimeout * 2;
    if (allowNewStale) {
      const visited = new Set<AgentId>();
      for (const agent of [...this.agents.values()]) {
        if (!ACTIVE_STATUSES.has(agent.status) || agent.reconnectable === true) continue;
        if (now - agent.lastActivity < heartbeatTimeout) continue;
        this.markReconnectableSubtree(agent.id, "Agent heartbeat expired before the host reconnected", now, events, visited);
      }
    }

    for (const agent of [...this.agents.values()]) {
      if (agent.depth === 0 || agent.status !== "failed" || agent.reconnectable !== true) continue;
      if (now - agent.lastActivity < reconnectGrace) continue;
      this.retireReconnectableSubtree(agent, events);
    }
    this.drainModelTurnWaiters(events);
  }

  private retireReconnectableSubtree(agent: AgentRecord, events: CoordinatorEvent[]): void {
    for (const child of [...this.agents.values()]) {
      if (child.parentId === agent.id && (this.isReservedLive(child) || child.reconnectable === true)) this.retireReconnectableSubtree(child, events);
    }
    const current = this.agents.get(agent.id);
    if (!current || (!this.isReservedLive(current) && current.reconnectable !== true)) return;
    const next = cloneAgent(current);
    this.removeRecoveryTurnReservation(next.id, events);
    next.status = "cancelled";
    next.reconnectable = false;
    next.recoveryExpiredAt = this.clock();
    next.terminalAt = next.recoveryExpiredAt;
    next.activeTurnOperationId = undefined;
    next.activeTurnPurpose = undefined;
    next.statusReason = "Reconnect grace expired; unfinished work returned to the task pool";
    next.lastActivity = next.recoveryExpiredAt;
    this.removeModelTurnWaiter(next.id, events);
    this.agents.set(next.id, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    this.cancelRequestsFor(next.id, "cancelled", "Agent reconnect grace expired", events);
    this.markMessagesUndeliverable(next.id, events);
    this.releaseAgentRuntime(next.id, "reconnect-expired", events);
    if (next.parentId && this.agents.has(next.parentId)) {
      // A grace expiry is a semantic retirement, unlike a temporary broker
      // recovery transition, so the parent receives the ordinary failure wake.
      this.sendInternalMessage(next.id, next.parentId, "agent_failed", next.statusReason, { failedAgentId: next.id }, events);
    }
  }

  private reserveRecoveryTurn(agent: AgentRecord, now: number, events: CoordinatorEvent[]): void {
    if (agent.status !== "running" || this.recoveryTurnReservations.has(agent.id)) return;
    const reservation: ModelTurnRecoveryReservation = {
      agentId: agent.id,
      operationId: agent.activeTurnOperationId,
      purpose: agent.activeTurnPurpose ?? "turn",
      route: { ...agent.route },
      capacityKey: modelRouteCapacityKey(this.config, agent.route),
      reservedAt: now,
      expiresAt: now + (this.config.reconnectGraceMs ?? DEFAULT_FABRIC_CONFIG.reconnectGraceMs!),
    };
    this.recoveryTurnReservations.set(agent.id, reservation);
    events.push({ type: "model_turn_recovery_reserved", reservation: cloneModelTurnRecoveryReservation(reservation) });
  }

  private expireRecoveryTurnReservations(now: number, events: CoordinatorEvent[]): boolean {
    let expired = false;
    for (const [agentId, reservation] of [...this.recoveryTurnReservations.entries()]) {
      if (reservation.expiresAt > now) continue;
      this.recoveryTurnReservations.delete(agentId);
      events.push({ type: "model_turn_recovery_resolved", agentId, operationId: reservation.operationId, state: "expired" });
      expired = true;
    }
    return expired;
  }

  private removeRecoveryTurnReservation(agentId: AgentId, events: CoordinatorEvent[], state: ModelTurnRecoveryState | "expired" = "stopped"): void {
    const reservation = this.recoveryTurnReservations.get(agentId);
    if (!reservation) return;
    this.recoveryTurnReservations.delete(agentId);
    events.push({ type: "model_turn_recovery_resolved", agentId, operationId: reservation.operationId, state });
  }

  private archiveHistoricalRecords(now: number, events: CoordinatorEvent[]): void {
    const retention = this.config.historyRetentionMs ?? DEFAULT_FABRIC_CONFIG.historyRetentionMs as number;
    const cutoff = now - retention;
    let budget = Math.max(1, Math.floor(this.config.historyGcBatchSize ?? DEFAULT_FABRIC_CONFIG.historyGcBatchSize!));

    // Build each relationship index once per bounded pass. The previous
    // implementation rescanned every whole collection for every candidate,
    // making a permanently pinned terminal set quadratic under the default
    // 24-hour retention window.
    const childrenByParent = new Map<AgentId, Set<AgentId>>();
    for (const agent of this.agents.values()) {
      if (!agent.parentId) continue;
      const children = childrenByParent.get(agent.parentId) ?? new Set<AgentId>();
      children.add(agent.id);
      childrenByParent.set(agent.parentId, children);
    }
    const nonTerminalTasksByOwner = new Set<AgentId>();
    for (const task of this.tasks.values()) if (task.owner && !isTaskTerminal(task.status)) nonTerminalTasksByOwner.add(task.owner);
    const resourceOwners = new Set<AgentId>();
    const grantedResourcesByAgent = new Map<AgentId, ResourceRecord[]>();
    for (const resource of this.resources.values()) {
      if (resource.owner && this.isResourceActive(resource)) resourceOwners.add(resource.owner);
      for (const agentId of Object.keys(resource.grants)) {
        const granted = grantedResourcesByAgent.get(agentId) ?? [];
        granted.push(resource);
        grantedResourcesByAgent.set(agentId, granted);
      }
    }

    // Archive leaves first. Removing a child updates the small parent index so
    // a terminal parent can become eligible without another full scan.
    const terminalAgents = [...this.agents.values()]
      .filter((agent) => agent.depth > 0 && isTerminal(agent.status) && agent.reconnectable !== true)
      .sort((left, right) => right.depth - left.depth || (left.terminalAt ?? left.lastActivity) - (right.terminalAt ?? right.lastActivity));
    for (const agent of terminalAgents) {
      if (budget <= 0) break;
      const current = this.agents.get(agent.id);
      if (!current || !isTerminal(current.status) || current.reconnectable === true) continue;
      const terminalAt = current.terminalAt ?? current.lastActivity;
      if (terminalAt > cutoff) continue;
      // A useful committed/dirty worktree is retained deliberately. Its cold
      // artifact record is sufficient; the hot agent identity need not remain.
      if (current.workspace && current.artifactDisposition !== "cleaned" && current.artifactDisposition !== "retained" && current.artifactsCleanedAt === undefined) continue;
      if ((childrenByParent.get(current.id)?.size ?? 0) > 0) continue;
      if (nonTerminalTasksByOwner.has(current.id) || resourceOwners.has(current.id)) continue;

      for (const resource of grantedResourcesByAgent.get(current.id) ?? []) {
        if (!hasOwn(resource.grants, current.id)) continue;
        delete resource.grants[current.id];
        resource.updatedAt = now;
        events.push({ type: "resource_changed", resource: cloneResource(resource) });
      }
      let retainedArtifactId = current.retainedArtifactId;
      if (current.artifactDisposition === "retained" && !retainedArtifactId) {
        this.trimRetainedArtifacts(events, true);
        if (this.retainedArtifacts.size >= (this.config.maxRetainedArtifacts ?? DEFAULT_FABRIC_CONFIG.maxRetainedArtifacts!)) continue;
        retainedArtifactId = this.idFactory("retained-artifact");
        const artifact: RetainedArtifactRecord = {
          id: retainedArtifactId,
          agentId: current.id,
          workspace: current.workspace ? { ...current.workspace } : undefined,
          retainedAt: now,
          reason: "Terminal agent artifacts were retained",
          status: "retained",
        };
        this.retainedArtifacts.set(retainedArtifactId, artifact);
        // The artifact is externalized by the broker after the journal append;
        // make the creation replayable instead of relying on a checkpoint copy.
        events.push({ type: "agent_artifacts_retained", agentId: current.id, artifact: { ...artifact, workspace: artifact.workspace ? { ...artifact.workspace } : undefined } });
      }
      const tombstone: AgentTombstone = {
        id: current.id,
        rootId: current.rootId,
        parentId: current.parentId,
        depth: current.depth,
        role: current.role,
        status: current.status as AgentTombstone["status"],
        createdAt: current.createdAt,
        terminalAt,
        taskId: current.taskId,
        recoveryExpiredAt: current.recoveryExpiredAt,
        artifactsCleanedAt: current.artifactsCleanedAt,
        artifactDisposition: current.artifactDisposition ?? (current.artifactsCleanedAt !== undefined ? "cleaned" : undefined),
        retainedArtifactId,
      };
      this.agents.delete(current.id);
      this.archivedAgents.set(current.id, tombstone);
      this.nextMessageSequence.delete(current.id);
      if (current.parentId) childrenByParent.get(current.parentId)?.delete(current.id);
      events.push({ type: "agent_archived", agent: { ...tombstone } });
      budget -= 1;
    }

    const nonTerminalDependents = new Set<TaskId>();
    for (const candidate of this.tasks.values()) {
      if (isTaskTerminal(candidate.status)) continue;
      for (const dependency of candidate.dependencies ?? []) nonTerminalDependents.add(dependency);
    }
    const referencedByAgent = new Set<TaskId>();
    for (const agent of this.agents.values()) {
      if (!agent.taskId || isTerminal(agent.status)) continue;
      if (this.tasks.has(agent.taskId)) referencedByAgent.add(agent.taskId);
    }
    for (const task of [...this.tasks.values()].sort((left, right) => left.updatedAt - right.updatedAt || left.id.localeCompare(right.id))) {
      if (budget <= 0) break;
      if (!isTaskTerminal(task.status) || task.updatedAt > cutoff) continue;
      if (referencedByAgent.has(task.id) || nonTerminalDependents.has(task.id)) continue;
      const tombstone: TaskTombstone = {
        id: task.id,
        status: task.status as TaskTombstone["status"],
        dependencies: [...(task.dependencies ?? [])],
        parentTaskId: task.parentTaskId,
        createdAt: task.createdAt,
        updatedAt: task.updatedAt,
      };
      this.tasks.delete(task.id);
      this.archivedTasks.set(task.id, tombstone);
      events.push({ type: "task_archived", task: { ...tombstone } });
      budget -= 1;
    }

    if (budget > 0) {
      for (const request of [...this.requests.values()].sort((left, right) => (left.resolvedAt ?? 0) - (right.resolvedAt ?? 0) || left.id.localeCompare(right.id))) {
        if (budget <= 0) break;
        if (request.status === "pending" || request.resolvedAt === undefined || request.resolvedAt > cutoff) continue;
        const tombstone: RequestTombstone = { id: request.id, status: request.status, resolvedAt: request.resolvedAt };
        this.requests.delete(request.id);
        this.archivedRequests.set(request.id, tombstone);
        events.push({ type: "request_archived", request: { ...tombstone } });
        budget -= 1;
      }
    }

    // Retired resources no longer participate in overlap, ownership, or
    // waiter checks. Replace their bulky grant/hold maps with an inspectable
    // identity tombstone once the normal history horizon has elapsed.
    if (budget > 0) {
      for (const resource of [...this.resources.values()].sort((left, right) => (left.retiredAt ?? left.updatedAt) - (right.retiredAt ?? right.updatedAt) || left.id.localeCompare(right.id))) {
        if (budget <= 0 || resource.status !== "retired") continue;
        const retiredAt = resource.retiredAt ?? resource.updatedAt;
        if (retiredAt > cutoff) continue;
        const tombstone: ResourceTombstone = {
          id: resource.id,
          incarnation: resource.incarnation,
          kind: resource.kind,
          parentId: resource.parentId,
          path: resource.path,
          owner: resource.owner,
          version: resource.version,
          status: "retired",
          createdAt: resource.createdAt,
          retiredAt,
          updatedAt: resource.updatedAt,
        };
        this.resources.delete(resource.id);
        this.archivedResources.set(resource.id, tombstone);
        events.push({ type: "resource_archived", resource: { ...tombstone } });
        budget -= 1;
      }
    }
    this.trimArchivedHistory(events);
    this.trimRetainedArtifacts(events);
  }

  private trimArchivedHistory(events: CoordinatorEvent[]): void {
    const limit = this.config.maxArchivedRecords ?? DEFAULT_FABRIC_CONFIG.maxArchivedRecords as number;
    const referencedAgents = new Set<AgentId>();
    for (const task of this.tasks.values()) {
      if (task.owner) referencedAgents.add(task.owner);
      referencedAgents.add(task.creator);
    }
    for (const resource of this.resources.values()) {
      if (!this.isResourceActive(resource)) continue;
      if (resource.owner) referencedAgents.add(resource.owner);
      for (const id of Object.keys(resource.grants)) referencedAgents.add(id);
      for (const hold of [...resource.sharedHolds, ...(resource.mutableHold ? [resource.mutableHold] : [])]) referencedAgents.add(hold.agentId);
      for (const waiter of resource.waiters) referencedAgents.add(waiter.agentId);
    }
    for (const message of this.messages.values()) {
      referencedAgents.add(message.from);
      referencedAgents.add(message.to);
    }
    for (const waiter of this.modelWaiters.values()) referencedAgents.add(waiter.agentId);

    const referencedTasks = new Set<TaskId>();
    // Only hot records can still become live again or participate in a live
    // dependency check. A tombstone's terminal edges are retained for
    // diagnostics but must not pin the entire historical dependency graph.
    for (const task of this.tasks.values()) {
      if (task.parentTaskId) referencedTasks.add(task.parentTaskId);
      for (const dependency of task.dependencies ?? []) referencedTasks.add(dependency);
    }
    const referencedRequests = new Set<RequestId>();
    for (const message of this.messages.values()) if (message.requestId) referencedRequests.add(message.requestId);

    this.trimArchiveMap(this.archivedAgents, limit, (entry) => entry.terminalAt, (entry) => referencedAgents.has(entry.id), (id) => events.push({ type: "agent_archive_pruned", agentId: id }));
    this.trimArchiveMap(this.archivedTasks, limit, (entry) => entry.updatedAt, (entry) => referencedTasks.has(entry.id), (id) => events.push({ type: "task_archive_pruned", taskId: id }));
    this.trimArchiveMap(this.archivedRequests, limit, (entry) => entry.resolvedAt, (entry) => referencedRequests.has(entry.id), (id) => events.push({ type: "request_archive_pruned", requestId: id }));
    this.trimArchiveMap(this.archivedResources, limit, (entry) => entry.retiredAt, () => false, (id) => events.push({ type: "resource_archive_pruned", resourceId: id }));
  }

  private trimArchiveMap<T extends { id: string }>(
    archive: Map<string, T>,
    limit: number,
    timestamp: (entry: T) => number,
    referenced: (entry: T) => boolean,
    onPrune: (id: string) => void,
  ): void {
    if (archive.size <= limit) return;
    const candidates = [...archive.values()].filter((entry) => !referenced(entry)).sort((left, right) => timestamp(left) - timestamp(right) || left.id.localeCompare(right.id));
    for (const entry of candidates) {
      if (archive.size <= limit) break;
      archive.delete(entry.id);
      onPrune(entry.id);
    }
  }

  /** Mark non-terminal runtime actors stale after a broker restart and release their leases. */
  recover(): DispatchResult<{ recovered: AgentId[] }> {
    const events: CoordinatorEvent[] = [];
    const recovered: AgentId[] = [];
    const now = this.clock();
    // A provider call may outlive the broker process. Reserve every route slot
    // represented by a running actor before recovery changes that actor to a
    // reconnectable liveness state.
    for (const agent of this.agents.values()) this.reserveRecoveryTurn(agent, now, events);
    const visited = new Set<AgentId>();
    for (const agent of [...this.agents.values()]) {
      if (!ACTIVE_STATUSES.has(agent.status) || agent.reconnectable === true) continue;
      this.markReconnectableSubtree(agent.id, "Broker restarted before the agent reconnected", now, events, visited, recovered);
    }
    // A persisted grant may have expired while the broker was down. Reapply
    // the normal expiry/requeue path before exposing the recovered state.
    this.reclaimExpired(now, events);
    return { value: { recovered }, events };
  }

  /**
   * Fence a stale actor's complete live subtree as one recovery unit. A parent
   * may not become recoverable while a descendant remains active because the
   * reconnect window can later make that descendant live again.
   */
  private markReconnectableSubtree(
    agentId: AgentId,
    reason: string,
    now: number,
    events: CoordinatorEvent[],
    visited: Set<AgentId>,
    recovered?: AgentId[],
  ): void {
    if (visited.has(agentId)) return;
    visited.add(agentId);
    for (const child of [...this.agents.values()]) {
      if (child.parentId === agentId && (this.isReservedLive(child) || child.reconnectable === true)) {
        this.markReconnectableSubtree(child.id, reason, now, events, visited, recovered);
      }
    }
    const current = this.agents.get(agentId);
    if (!current || !this.isReservedLive(current) || current.reconnectable === true) return;
    const next = cloneAgent(current);
    next.status = "failed";
    next.statusReason = reason;
    next.reconnectable = true;
    next.activeTurnOperationId = undefined;
    next.activeTurnPurpose = undefined;
    next.terminalAt = undefined;
    next.lastActivity = now;
    this.agents.set(next.id, next);
    events.push({ type: "agent_updated", agent: cloneAgent(next) });
    // Recovery releases only runtime claims. A queued model turn is a runtime
    // claim too, so cancel its ticket; the reconnecting host will enqueue a
    // fresh turn after it has reconciled its durable task state. The task owner
    // remains reserved until this actor reconnects or grace expires.
    this.removeModelTurnWaiter(next.id, events);
    this.releaseAgentResourceClaims(next.id, events);
    recovered?.push(next.id);
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

  exportState(options: { includeRetainedArtifacts?: boolean } = {}): PersistedCoordinatorStateV2 {
    const includeRetainedArtifacts = options.includeRetainedArtifacts !== false;
    return {
      version: 2,
      nextMessageSequence: Object.fromEntries(this.nextMessageSequence),
      agents: [...this.agents.values()].map(cloneAgent),
      tasks: [...this.tasks.values()].map(cloneTask),
      resources: [...this.resources.values()].map(cloneResource),
      messages: [...this.messages.values()].map(cloneMessage),
      requests: [...this.requests.values()].map(cloneRequest),
      dedupe: [...this.dedupe.entries()],
      nextBrokerSequence: this.nextBrokerSequence,
      nextResourceWaiterSequence: this.nextResourceWaiterSequence,
      idempotency: [...this.idempotency.values()].map((entry) => ({ ...entry })),
      nextModelTurnWaiterSequence: this.nextModelTurnWaiterSequence,
      modelWaiters: [...this.modelWaiters.values()].map(cloneModelTurnWaiter),
      archivedAgents: [...this.archivedAgents.values()].map((agent) => ({ ...agent })),
      archivedTasks: [...this.archivedTasks.values()].map((task) => ({ ...task, dependencies: [...(task.dependencies ?? [])] })),
      archivedRequests: [...this.archivedRequests.values()].map((request) => ({ ...request })),
      archivedResources: [...this.archivedResources.values()].map((resource) => ({ ...resource, incarnation: this.resourceIncarnation(resource) })),
      ...(includeRetainedArtifacts
        ? { retainedArtifacts: [...this.retainedArtifacts.values()].map((artifact) => ({ ...artifact, workspace: artifact.workspace ? { ...artifact.workspace } : undefined })) }
        : { retainedArtifactIds: [...this.retainedArtifacts.keys()] }),
      ...(!this.ackProofs ? { acknowledgedMessages: [...this.acknowledgedMessages.values()].map(cloneAckTombstone) } : {}),
      recoveryTurnReservations: [...this.recoveryTurnReservations.values()].map(cloneModelTurnRecoveryReservation),
    };
  }

  exportAckProofs(): MessageAckTombstone[] {
    return [...this.acknowledgedMessages.values()].map(cloneAckTombstone);
  }

  exportRetainedArtifacts(): RetainedArtifactRecord[] {
    return [...this.retainedArtifacts.values()].map((artifact) => ({ ...artifact, workspace: artifact.workspace ? { ...artifact.workspace } : undefined }));
  }

  restoreRetainedArtifacts(records: readonly RetainedArtifactRecord[]): void {
    assertCondition(records.length <= (this.config.maxRetainedArtifacts ?? DEFAULT_FABRIC_CONFIG.maxRetainedArtifacts!), "AGENT_LIMIT_REACHED", "retained artifact manifest exceeds configured capacity");
    this.retainedArtifacts.clear();
    for (const artifact of records) this.retainedArtifacts.set(artifact.id, { ...artifact, workspace: artifact.workspace ? { ...artifact.workspace } : undefined });
  }

  restoreState(state: PersistedCoordinatorState, options: { preserveExternalRetainedArtifacts?: boolean } = {}): void {
    const migrated = this.migratePersistedState(state);
    this.agents.clear();
    this.tasks.clear();
    this.resources.clear();
    this.messages.clear();
    this.requests.clear();
    this.dedupe.clear();
    this.modelWaiters.clear();
    this.archivedAgents.clear();
    this.archivedTasks.clear();
    this.archivedRequests.clear();
    this.archivedResources.clear();
    if (!options.preserveExternalRetainedArtifacts && migrated.retainedArtifacts !== undefined) this.retainedArtifacts.clear();
    this.acknowledgedMessages.clear();
    this.recoveryTurnReservations.clear();
    this.nextMessageSequence.clear();
    this.nextBrokerSequence = migrated.nextBrokerSequence ?? 0;
    this.nextResourceWaiterSequence = migrated.nextResourceWaiterSequence ?? 0;
    this.nextModelTurnWaiterSequence = migrated.nextModelTurnWaiterSequence ?? 0;
    for (const agent of migrated.agents) {
      assertCondition(agent.rootId === this.rootId, "IDENTITY_CONFLICT", `Persisted agent ${agent.id} belongs to another fabric`);
      this.agents.set(agent.id, cloneAgent(agent));
    }
    for (const task of migrated.tasks) this.tasks.set(task.id, cloneTask(task));
    for (const resource of migrated.resources) {
      const next = cloneResource(resource);
      for (const waiter of next.waiters) this.nextResourceWaiterSequence = Math.max(this.nextResourceWaiterSequence, waiter.enqueuedSequence ?? 0);
      this.resources.set(next.id, next);
    }
    for (const message of migrated.messages) {
      const next = cloneMessage(message);
      if (next.brokerSequence !== undefined) this.nextBrokerSequence = Math.max(this.nextBrokerSequence, next.brokerSequence);
      this.messages.set(next.id, next);
    }
    for (const request of migrated.requests) this.requests.set(request.id, cloneRequest(request));
    for (const [key, value] of migrated.dedupe) this.dedupe.set(key, value);
    for (const [agentId, sequence] of Object.entries(migrated.nextMessageSequence)) this.nextMessageSequence.set(agentId, sequence);
    for (const waiter of migrated.modelWaiters ?? []) {
      const next = cloneModelTurnWaiter(waiter);
      this.nextModelTurnWaiterSequence = Math.max(this.nextModelTurnWaiterSequence, next.enqueuedSequence);
      this.modelWaiters.set(next.id, next);
    }
    for (const archived of migrated.archivedAgents ?? []) this.archivedAgents.set(archived.id, { ...archived });
    for (const archived of migrated.archivedTasks ?? []) this.archivedTasks.set(archived.id, { ...archived, dependencies: [...(archived.dependencies ?? [])] });
    for (const archived of migrated.archivedRequests ?? []) this.archivedRequests.set(archived.id, { ...archived });
    for (const archived of migrated.archivedResources ?? []) this.archivedResources.set(archived.id, { ...archived, incarnation: this.resourceIncarnation(archived) });
    if (migrated.retainedArtifacts !== undefined && (!options.preserveExternalRetainedArtifacts || this.retainedArtifacts.size === 0)) {
      this.retainedArtifacts.clear();
      for (const artifact of migrated.retainedArtifacts) this.retainedArtifacts.set(artifact.id, { ...artifact, workspace: artifact.workspace ? { ...artifact.workspace } : undefined });
    }
    if (!this.ackProofs) {
      for (const acknowledged of migrated.acknowledgedMessages ?? []) this.acknowledgedMessages.set(this.ackTombstoneKey(acknowledged.to, acknowledged.id, acknowledged.revision), { ...acknowledged, acknowledged: true });
    }
    for (const reservation of migrated.recoveryTurnReservations ?? []) this.recoveryTurnReservations.set(reservation.agentId, cloneModelTurnRecoveryReservation(reservation));
    this.idempotency.clear();
    for (const entry of migrated.idempotency ?? []) this.rememberIdempotencyEntry({ ...entry });
  }

  private migratePersistedState(state: PersistedCoordinatorState): PersistedCoordinatorStateV2 {
    const version = (state as { version?: unknown }).version;
    assertCondition(version === 1 || version === 2, "PROTOCOL_VERSION_UNSUPPORTED", `Unsupported coordinator state version ${String(version)}`);
    const source = state as unknown as PersistedCoordinatorState & { resources: ResourceRecord[] };
    return {
      ...source,
      version: 2,
      resources: source.resources.map((resource) => ({
        ...resource,
        incarnation: resource.incarnation ?? this.legacyResourceIncarnation(resource),
      })) as ResourceRecord[],
      archivedResources: source.archivedResources?.map((resource) => ({
        ...resource,
        incarnation: resource.incarnation ?? this.legacyResourceIncarnation(resource),
      })) as ResourceTombstone[] | undefined,
    };
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
          const resource = cloneResource({ ...event.resource, incarnation: this.resourceIncarnation(event.resource) });
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
        case "message_updated": {
          const message = cloneMessage(event.message);
          this.messages.set(message.id, message);
          if (message.clientDedupeKey) this.dedupe.set(`${message.from}\u0000${message.clientDedupeKey}`, message.id);
          break;
        }
        case "message_acknowledged": {
          const message = cloneMessage(event.message);
          this.messages.set(message.id, message);
          this.rememberAckTombstone(message);
          break;
        }
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
        case "model_turn_waiting": {
          const waiter = cloneModelTurnWaiter(event.waiter);
          this.nextModelTurnWaiterSequence = Math.max(this.nextModelTurnWaiterSequence, waiter.enqueuedSequence);
          this.modelWaiters.set(waiter.id, waiter);
          break;
        }
        case "model_turn_granted": {
          const waiter = this.modelWaiters.get(event.waiterId);
          // Pre-R8 journals deleted a waiter when it was granted and emitted an
          // agent_updated running record. New grants remain durable until the
          // exact host claims them.
          if (!waiter || event.grantExpiresAt === undefined) {
            this.modelWaiters.delete(event.waiterId);
            break;
          }
          waiter.state = "granted";
          waiter.operationId = event.operationId ?? waiter.operationId;
          waiter.purpose = event.purpose ?? waiter.purpose;
          waiter.grantExpiresAt = event.grantExpiresAt;
          waiter.grantedAt = event.grantedAt ?? waiter.grantedAt;
          break;
        }
        case "model_turn_claimed":
        case "model_turn_cancelled":
          this.modelWaiters.delete(event.waiterId);
          break;
        case "model_turn_recovery_reserved":
          this.recoveryTurnReservations.set(event.reservation.agentId, cloneModelTurnRecoveryReservation(event.reservation));
          break;
        case "model_turn_recovery_resolved":
          this.recoveryTurnReservations.delete(event.agentId);
          break;
        case "agent_artifacts_retained":
          this.retainedArtifacts.set(event.artifact.id, { ...event.artifact, workspace: event.artifact.workspace ? { ...event.artifact.workspace } : undefined });
          break;
        case "agent_artifacts_resolved": {
          const artifact = this.retainedArtifacts.get(event.artifactId);
          if (artifact) {
            artifact.status = "resolved";
            artifact.resolvedAt = event.resolvedAt;
            artifact.resolution = event.resolution;
          }
          break;
        }
        case "agent_artifact_pruned":
          this.retainedArtifacts.delete(event.artifactId);
          break;
        case "resource_archived":
          this.resources.delete(event.resource.id);
          this.archivedResources.set(event.resource.id, { ...event.resource, incarnation: this.resourceIncarnation(event.resource) });
          break;
        case "resource_archive_pruned":
          this.archivedResources.delete(event.resourceId);
          break;
        case "agent_archived":
          this.agents.delete(event.agent.id);
          this.archivedAgents.set(event.agent.id, { ...event.agent });
          this.nextMessageSequence.delete(event.agent.id);
          break;
        case "task_archived":
          this.tasks.delete(event.task.id);
          this.archivedTasks.set(event.task.id, { ...event.task, dependencies: [...(event.task.dependencies ?? [])] });
          break;
        case "request_archived":
          this.requests.delete(event.request.id);
          this.archivedRequests.set(event.request.id, { ...event.request });
          break;
        case "agent_archive_pruned":
          this.archivedAgents.delete(event.agentId);
          break;
        case "task_archive_pruned":
          this.archivedTasks.delete(event.taskId);
          break;
        case "request_archive_pruned":
          this.archivedRequests.delete(event.requestId);
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
    if (resource) return resource;
    const archived = this.archivedResources.get(resourceId);
    if (archived) return resourceFromTombstone(archived);
    throw new FabricError("RESOURCE_NOT_FOUND", `Resource ${resourceId} was not found`);
  }

  private isResourceActive(resource: ResourceRecord): boolean {
    return resource.status !== "retired";
  }

  /** Return the durable resource identity, deriving one only for legacy input. */
  private resourceIncarnation(resource: { id: string; kind: string; parentId?: string; path?: string; createdAt: number; retiredAt?: number; incarnation?: string }): string {
    return resource.incarnation ?? this.legacyResourceIncarnation(resource);
  }

  private legacyResourceIncarnation(resource: { id: string; kind: string; parentId?: string; path?: string; createdAt: number; retiredAt?: number }): string {
    const seed = [this.rootId, resource.id, resource.kind, resource.parentId ?? "", resource.path ?? "", resource.createdAt, resource.retiredAt ?? ""].join("\u0000");
    return `legacy-${createHash("sha256").update(seed).digest("hex").slice(0, 32)}`;
  }

  private validateRoute(route: ModelRoute): ModelRoute {
    assertCondition(route && typeof route === "object", "MODEL_ROUTE_INVALID", "A model route is required");
    const provider = parseString(route.provider, "route.provider", 256);
    const model = parseString(route.model, "route.model", 512);
    const thinking = parseString(route.thinking, "route.thinking", 32) as ModelRoute["thinking"];
    assertCondition(ALL_THINKING_LEVELS.has(thinking), "MODEL_ROUTE_INVALID", `Unknown thinking level ${thinking}`);
    const capacityGroup = route.capacityGroup === undefined ? undefined : parseString(route.capacityGroup, "route.capacityGroup", 256);
    return { provider, model, thinking, ...(capacityGroup ? { capacityGroup } : {}) };
  }

  private transitionStatus(agent: AgentRecord, next: AgentStatus, reason?: string): void {
    if (agent.status === next) {
      agent.statusReason = reason;
      return;
    }
    if (isTerminal(agent.status)) throw new FabricError("LIFECYCLE_CONFLICT", `Agent ${agent.id} is already ${agent.status}`);
    const valid: Record<AgentStatus, AgentStatus[]> = {
      starting: ["ready", "draining", "failed", "cancelled"],
      ready: ["running", "waiting", "blocked", "draining", "completed", "failed", "cancelled"],
      running: ["ready", "waiting", "blocked", "draining", "completed", "failed", "cancelled"],
      waiting: ["ready", "running", "blocked", "draining", "completed", "failed", "cancelled"],
      blocked: ["ready", "running", "waiting", "draining", "completed", "failed", "cancelled"],
      draining: ["completed", "failed", "cancelled"],
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
        if (existing && existing.acknowledgedAt === undefined) {
          // Broker-generated control notices are state notifications, not an
          // append-only log. Replace the unacknowledged payload so a delayed
          // wake carries the newest lease/task/actor state rather than stale
          // metadata from the first notification in the coalescing window.
          if (options.clientDedupeKey?.startsWith("control:")) {
            const updated: AgentMessage = {
              ...cloneMessage(existing),
              body,
              revision: (existing.revision ?? 1) + 1,
              priority: options.priority ?? existing.priority,
              metadata: parseMetadata(options.metadata),
              abandonedAt: undefined,
            };
            this.messages.set(existing.id, updated);
            events.push({ type: "message_updated", message: cloneMessage(updated) });
            return { message: cloneMessage(updated), request: existing.requestId ? this.requests.get(existing.requestId) && cloneRequest(this.requests.get(existing.requestId) as RequestRecord) : undefined };
          }
          return { message: cloneMessage(existing), request: existing.requestId ? this.requests.get(existing.requestId) && cloneRequest(this.requests.get(existing.requestId) as RequestRecord) : undefined };
        }
        this.dedupe.delete(dedupeKey);
      }
    }
    const now = this.clock();
    const priority = options.priority ?? "normal";
    assertCondition(priority === "normal" || priority === "urgent", "INVALID_ARGUMENT", "priority must be normal or urgent");
    const pending = [...this.messages.values()].filter((message) => message.to === recipient.id && message.acknowledgedAt === undefined);
    const controlReserve = Math.min(32, Math.max(1, Math.floor(this.config.maxMailboxMessages / 8)));
    const normalLimit = Math.max(1, this.config.maxMailboxMessages - controlReserve);
    const pendingNormal = pending.filter((message) => message.priority === "normal").length;
    const limit = priority === "urgent" ? this.config.maxMailboxMessages + controlReserve : Math.min(this.config.maxMailboxMessages, normalLimit);
    assertCondition(priority === "urgent" ? pending.length < limit : pendingNormal < limit && pending.length < this.config.maxMailboxMessages, "MAILBOX_FULL", `Mailbox for ${recipient.id} is full`, { recipient: recipient.id, priority });
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
      revision: 1,
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
      .filter((message) => message.acknowledgedAt !== undefined || message.abandonedAt !== undefined)
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

  private markMessagesUndeliverable(recipientId: AgentId, events: CoordinatorEvent[]): void {
    const abandonedAt = this.clock();
    for (const message of this.messages.values()) {
      if (message.to !== recipientId || message.acknowledgedAt !== undefined || message.abandonedAt !== undefined) continue;
      message.abandonedAt = abandonedAt;
      events.push({ type: "message_updated", message: cloneMessage(message) });
    }
    this.pruneMessages(events);
  }

  private sendInternalMessage(fromId: AgentId, toId: AgentId, type: MessageType, body: string, metadata: Record<string, unknown>, events: CoordinatorEvent[]): void {
    const to = this.requireAgent(toId);
    if (isTerminal(to.status) && to.reconnectable !== true) return;
    const from = fromId === "broker" ? this.brokerActor() : this.requireAgent(fromId);
    try {
      const entity = metadata.requestId ?? metadata.taskId ?? metadata.failedAgentId ?? metadata.resourceId ?? stableStringify(metadata);
      this.recordMessage(from, to, type, body.slice(0, this.config.maxMessageBody), {
        priority: "urgent",
        metadata,
        // Coalesce repeated control-plane notices for the same entity while a
        // recipient is offline. Durable task/resource state remains the source
        // of truth, so retaining one latest wake is sufficient and bounded.
        clientDedupeKey: `control:${type}:${String(entity)}`,
      }, events);
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

  /** Release leases, waiters, and in-flight write fences without changing task ownership. */
  private releaseAgentResourceClaims(agentId: AgentId, events: CoordinatorEvent[]): void {
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
      for (const [id, fence] of this.fences) {
        if (fence.actorId === agentId) this.fences.delete(id);
      }
      if (changed) {
        resource.updatedAt = this.clock();
        events.push({ type: "resource_changed", resource: cloneResource(resource) });
      }
    }
    this.drainWaiters(events);
  }

  private releaseAgentRuntime(agentId: AgentId, reason: string, events: CoordinatorEvent[]): void {
    this.removeRecoveryTurnReservation(agentId, events);
    this.releaseAgentResourceClaims(agentId, events);
    this.releaseAgentTaskOwnership(agentId, reason, events);
  }

  /** Release semantic task ownership only after the actor is truly terminal. */
  private releaseAgentTaskOwnership(agentId: AgentId, reason: string, events: CoordinatorEvent[]): void {
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
      if (owner?.taskId === task.id) {
        owner.taskId = undefined;
        ownerTaskCleared = true;
      }
      events.push({ type: "task_changed", task: cloneTask(task) });
    }
    if (ownerTaskCleared && owner) events.push({ type: "agent_updated", agent: cloneAgent(owner) });
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
    const expiredModelTurnGrants = this.expireModelTurnGrants(now, events);
    const expiredRecoveryTurnReservations = this.expireRecoveryTurnReservations(now, events);
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
      if ((resource.writeQuarantineUntil ?? 0) <= now && resource.writeQuarantineUntil !== undefined) {
        resource.writeQuarantineUntil = undefined;
        resource.writeQuarantineActorId = undefined;
        resource.writeQuarantineFenceId = undefined;
        changed = true;
      }
      if (changed) {
        resource.updatedAt = now;
        events.push({ type: "resource_changed", resource: cloneResource(resource) });
      }
    }
    let fencesPruned = false;
    for (const [id, fence] of this.fences) {
      if (fence.expiresAt <= now) {
        this.fences.delete(id);
        fencesPruned = true;
      }
    }
    if (fencesPruned || events.some((event) => event.type === "resource_changed")) this.drainWaiters(events);
    if (expiredModelTurnGrants || expiredRecoveryTurnReservations) this.drainModelTurnWaiters(events);
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
    // A write fence is a promise that no one else touches the file while a
    // guarded write is in flight, so it excludes conflicting grants even in
    // the gap where the writer's own lease has just lapsed.
    if (this.activeForeignFence(resource, agentId)) return false;
    if (this.activeForeignWriteQuarantine(resource, agentId)) return false;
    for (const overlap of this.overlappingResources(resource.id)) {
      // A holder cannot downgrade/upgrade itself behind the coordinator's
      // back: shared and mutable holds for one actor are still conflicting.
      if (overlap.mutableHold) return false;
      if (mode === "mutable" && overlap.sharedHolds.length > 0) return false;
    }
    return true;
  }

  private overlappingResources(resourceId: ResourceId): ResourceRecord[] {
    return [...this.resources.values()].filter((candidate) => this.isResourceActive(candidate) && this.overlaps(resourceId, candidate.id));
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
      .filter((resource) => this.isResourceActive(resource))
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
        return normalizeResourcePath(resource.id.slice("file:".length), "path", this.caseFoldPaths);
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
    // Compacted retired resources intentionally keep only identity metadata;
    // the root and the final owner retain inspectability without resurrecting
    // the old grant/hold maps.
    const agent = this.agents.get(agentId);
    if (resource.status === "retired") return agent?.depth === 0 || resource.owner === agentId;
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
    if (actor.depth === 0 || task.owner === actor.id || task.creator === actor.id) return true;
    if (task.owner) {
      const owner = this.agents.get(task.owner);
      if (owner && this.canControl(actor, owner)) return true;
    }
    return task.creator === actor.id;
  }

  private taskVisibleTo(actorId: AgentId, task: TaskRecord): boolean {
    const actor = this.requireAgent(actorId);
    if (actor.depth === 0) return true;
    if (task.owner) {
      const owner = this.agents.get(task.owner);
      if (owner && this.canControl(actor, owner)) return true;
    }
    if (task.creator === actorId) return true;
    if (!task.parentTaskId) return false;
    const parent = this.tasks.get(task.parentTaskId);
    // An archived parent remains valid for dependency semantics, but its
    // tombstone does not grant a child visibility into another actor's task.
    return parent ? this.taskVisibleTo(actorId, parent) : false;
  }

  private hasTask(taskId: TaskId): boolean {
    return this.tasks.has(taskId) || this.archivedTasks.has(taskId);
  }

  private taskStatus(taskId: TaskId): TaskStatus | undefined {
    return this.tasks.get(taskId)?.status ?? this.archivedTasks.get(taskId)?.status;
  }

  private taskDependenciesCompleted(task: TaskRecord): boolean {
    return (task.dependencies ?? []).every((dependency) => this.taskStatus(dependency) === "completed");
  }

  /** Return every non-terminal downstream task; terminal history is semantically inert for reopen. */
  private taskDependents(taskId: TaskId): Array<{ id: TaskId; status: TaskStatus }> {
    const dependents: Array<{ id: TaskId; status: TaskStatus }> = [];
    const seen = new Set<TaskId>([taskId]);
    const queue: TaskId[] = [taskId];
    const candidates = [
      ...this.tasks.values(),
      ...this.archivedTasks.values(),
    ];
    while (queue.length > 0) {
      const prerequisite = queue.shift() as TaskId;
      for (const candidate of candidates) {
        if (seen.has(candidate.id) || isTaskTerminal(candidate.status) || !(candidate.dependencies ?? []).includes(prerequisite)) continue;
        seen.add(candidate.id);
        queue.push(candidate.id);
        dependents.push({ id: candidate.id, status: candidate.status });
      }
    }
    return dependents;
  }

  /**
   * Capacity accounting for admitting new agents. A broker-recovery failure
   * keeps an agent's maxTotalAgents slot reserved while its one-shot reconnect
   * window stays open, so a newcomer can never evict an expected reconnection
   * by filling the fabric. The reservation ends when the agent reconnects,
   * resolves its turn, or is cancelled.
   */
  private reservedAgentCount(): number {
    return [...this.agents.values()].filter((agent) => this.isReservedLive(agent)).length;
  }

  private isReservedLive(agent: AgentRecord): boolean {
    return ACTIVE_STATUSES.has(agent.status) || agent.reconnectable === true;
  }

  private liveChildrenCount(parentId: AgentId, excludeAgentId?: AgentId): number {
    return [...this.agents.values()].filter((agent) => agent.id !== excludeAgentId && agent.parentId === parentId && this.isReservedLive(agent)).length;
  }

  private assertChildCapacity(parent: AgentRecord, excludeAgentId?: AgentId, checkCreationLimit = true): void {
    assertCondition(this.liveChildrenCount(parent.id, excludeAgentId) < this.config.maxChildrenPerAgent, "AGENT_LIMIT_REACHED", `Agent ${parent.id} reached maxChildrenPerAgent live-child limit`);
    if (checkCreationLimit && this.config.maxChildrenCreatedPerAgent !== undefined) {
      assertCondition(parent.childrenCreated < this.config.maxChildrenCreatedPerAgent, "AGENT_LIMIT_REACHED", `Agent ${parent.id} reached maxChildrenCreatedPerAgent`);
    }
  }

  private runningAgentCount(): number {
    return [...this.agents.values()].filter((agent) => agent.status === "running").length;
  }

  private runningRouteCount(route: ModelRoute, excludeAgentId?: string): number {
    const capacityKey = modelRouteCapacityKey(this.config, route);
    return [...this.agents.values()].filter((agent) => agent.status === "running" && agent.id !== excludeAgentId && modelRouteCapacityKey(this.config, agent.route) === capacityKey).length;
  }

  private routeCapacityLimit(route: ModelRoute): number | undefined {
    const capacityKey = modelRouteCapacityKey(this.config, route);
    const limits: number[] = [];
    const requested = modelRouteCapacity(this.config, route);
    if (requested !== undefined) limits.push(requested);
    for (const agent of this.agents.values()) {
      if (modelRouteCapacityKey(this.config, agent.route) !== capacityKey) continue;
      const limit = modelRouteCapacity(this.config, agent.route);
      if (limit !== undefined) limits.push(limit);
    }
    for (const policy of Object.values(this.config.modelRoutePolicies ?? {})) {
      if (policy.capacityGroup !== capacityKey || policy.maxConcurrent === undefined) continue;
      limits.push(policy.maxConcurrent);
    }
    return limits.length > 0 ? Math.min(...limits) : undefined;
  }

  private assertRouteCapacity(route: ModelRoute, excludeAgentId?: string): void {
    const limit = this.routeCapacityLimit(route);
    if (limit !== undefined) {
      assertCondition(this.runningRouteCount(route, excludeAgentId) + this.grantedModelTurnCount(modelRouteCapacityKey(this.config, route)) + this.recoveryTurnReservationCount(modelRouteCapacityKey(this.config, route)) < limit, "AGENT_LIMIT_REACHED", `model route capacity reached for ${modelRouteKey(route)}`);
    }
  }

  private toSummary(agent: AgentRecord, includeCapabilities = true): AgentSummary {
    return {
      id: agent.id,
      parentId: agent.parentId,
      depth: agent.depth,
      role: agent.role,
      taskId: agent.taskId,
      route: { ...agent.route },
      status: agent.status,
      reconnectable: agent.reconnectable,
      recoveryExpiredAt: agent.recoveryExpiredAt,
      artifactsCleanedAt: agent.artifactsCleanedAt,
      artifactDisposition: agent.artifactDisposition,
      retainedArtifactId: agent.retainedArtifactId,
      workspace: agent.workspace ? { ...agent.workspace } : undefined,
      lastActivity: agent.lastActivity,
      contextMode: agent.contextMode,
      contextDiagnostic: agent.contextDiagnostic,
      capabilities: includeCapabilities ? cloneCapabilities(agent.capabilities) : undefined,
    };
  }
}
