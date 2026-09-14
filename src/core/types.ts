import type { ThinkingLevel } from "@earendil-works/pi-agent-core";

export type AgentId = string;
export type RootId = string;
export type TaskId = string;
export type ResourceId = string;
export type MessageId = string;
export type RequestId = string;
export type LeaseId = string;

export type AgentStatus =
  | "starting"
  | "ready"
  | "running"
  | "waiting"
  | "blocked"
  /** Cooperative shutdown phase; no new work may be admitted. */
  | "draining"
  | "completed"
  | "failed"
  | "cancelled";

export type TaskStatus = "pending" | "ready" | "active" | "waiting" | "blocked" | "completed" | "failed" | "cancelled";
export type BorrowMode = "shared" | "mutable";
export type MessagePriority = "normal" | "urgent";
export type ModelTurnPurpose = "turn" | "compaction";
export type ModelTurnState = "queued" | "granted";
export type ModelTurnRecoveryState = "running" | "stopped";
export type ArtifactDisposition = "cleaned" | "retained";
export type RetainedArtifactStatus = "retained" | "resolved";
export type ResourceStatus = "active" | "retired";
/** Policy used for managed child shell execution. */
export type ShellPolicy = "coordination" | "strict" | "trusted";
/** Boundary for explicit filesystem paths passed to a managed shell. */
export type ExternalPathAccess = "deny" | "read" | "any";

export const MESSAGE_TYPES = [
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
] as const;
export type MessageType = (typeof MESSAGE_TYPES)[number];

export const REQUEST_MESSAGE_TYPES = [
  "clarification",
  "decision_request",
  "escalation",
  "resource_request",
  "request",
] as const satisfies readonly MessageType[];

export interface ModelRoute {
  provider: string;
  model: string;
  thinking: ThinkingLevel;
  /** Optional physical backend identity shared by semantic route aliases. */
  capacityGroup?: string;
}

/** Optional policy for a concrete provider/model runtime. */
export interface ModelRoutePolicy {
  /** Maximum number of logical model turns using this route at once. */
  maxConcurrent?: number;
  /** Conservative operational prefill budget, in tokens. */
  effectivePrefillBudget?: number;
  /** Explicit physical backend identity for aliases sharing one runtime. */
  capacityGroup?: string;
}

export interface AgentCapabilities {
  maySpawn: boolean;
  mayMessagePeers: boolean;
  mayEscalate: boolean;
  mayTransferOwnership: boolean;
  mayWriteRepo: boolean;
  mayUseShell: boolean;
  /** Explicit peer IDs in addition to the normal parent/sibling/task visibility. */
  peerIds: AgentId[];
  /** Resource permissions keyed by exact resource ID. */
  resourceGrants: Record<string, ResourcePermission[]>;
}

export type ResourcePermission = "read" | "comment" | "write" | "test";

export interface WorkspaceInfo {
  mode: "shared" | "worktree";
  root: string;
  path: string;
  baseRef?: string;
  branch?: string;
}

export interface AgentRecord {
  id: AgentId;
  rootId: RootId;
  parentId?: AgentId;
  depth: number;
  role: string;
  taskId?: TaskId;
  route: ModelRoute;
  capabilities: AgentCapabilities;
  status: AgentStatus;
  statusReason?: string;
  sessionId?: string;
  workspace?: WorkspaceInfo;
  createdAt: number;
  lastActivity: number;
  childrenCreated: number;
  /** Broker-only reconnect credential; never include this in public projections. */
  authToken?: string;
  /** True only for a liveness recovery window after a broker restart. */
  reconnectable?: boolean;
  /** Set when broker recovery grace expires and the actor becomes permanently retired. */
  recoveryExpiredAt?: number;
  /** Timestamp of the terminal transition, used for bounded hot-state retention. */
  terminalAt?: number;
  /** Durable marker set after the matching workspace/session artifacts are handled. */
  artifactsCleanedAt?: number;
  /** Terminal artifact handling may retain useful Git work instead of deleting it. */
  artifactDisposition?: ArtifactDisposition;
  /** Stable reference into the cold retained-artifact collection. */
  retainedArtifactId?: string;
  /** Logical model operation currently holding the broker turn slot. */
  activeTurnOperationId?: string;
  activeTurnPurpose?: ModelTurnPurpose;
  /** Context management mode (e.g. lcm-embedded or native). */
  contextMode?: string;
  /** Bounded last context/provider diagnostic, for recovery visibility. */
  contextDiagnostic?: string;
}

export interface TaskResult {
  summary: string;
  output?: string;
  completedAt: number;
  by: AgentId;
}

export interface TaskRecord {
  id: TaskId;
  /** Stable pagination cursor present only on task.list projections. */
  cursor?: string;
  /** True only on a response replayed from a prior request with the same operationId. */
  replayed?: boolean;
  description: string;
  owner?: AgentId;
  creator: AgentId;
  parentTaskId?: TaskId;
  dependencies: TaskId[];
  status: TaskStatus;
  result?: TaskResult;
  blockedReason?: string;
  createdAt: number;
  updatedAt: number;
}

/** Compact identity/status records retained after hot records are archived. */
export interface AgentTombstone {
  id: AgentId;
  rootId: RootId;
  parentId?: AgentId;
  depth: number;
  role: string;
  status: Extract<AgentStatus, "completed" | "failed" | "cancelled">;
  createdAt?: number;
  terminalAt: number;
  taskId?: TaskId;
  recoveryExpiredAt?: number;
  artifactsCleanedAt?: number;
  artifactDisposition?: ArtifactDisposition;
  retainedArtifactId?: string;
}

export interface TaskTombstone {
  id: TaskId;
  status: Extract<TaskStatus, "completed" | "failed" | "cancelled">;
  /** Keep dependency edges so archive timing cannot change reopen semantics. */
  dependencies: TaskId[];
  parentTaskId?: TaskId;
  createdAt?: number;
  updatedAt: number;
}

/** Cold metadata for a terminal agent whose useful external artifacts were retained. */
export interface RetainedArtifactRecord {
  id: string;
  agentId: AgentId;
  workspace?: WorkspaceInfo;
  sessionPath?: string;
  baseRef?: string;
  headRef?: string;
  reason?: string;
  retainedAt: number;
  /** Retained artifacts remain recoverable until an operator resolves them. */
  status?: RetainedArtifactStatus;
  resolvedAt?: number;
  resolution?: string;
}

/** Compact acknowledgement proof retained after the full mailbox record is pruned. */
export interface MessageAckTombstone {
  id: MessageId;
  to: AgentId;
  revision: number;
  acknowledgedAt: number;
  acknowledged: true;
  /** Present only on a replay after the full mailbox record was pruned. */
  alreadyAcknowledged?: true;
}

/** Synchronous lookup boundary for ACK proofs kept outside coordinator hot state. */
export interface MessageAckProofLookup {
  findExact(to: AgentId, id: MessageId, revision: number): MessageAckTombstone | undefined;
  findMessage(to: AgentId, id: MessageId): MessageAckTombstone | undefined;
}

/** Physical route capacity reserved while a running host reconnects after broker recovery. */
export interface ModelTurnRecoveryReservation {
  agentId: AgentId;
  operationId?: string;
  purpose?: ModelTurnPurpose;
  route: ModelRoute;
  capacityKey: string;
  reservedAt: number;
  expiresAt: number;
}

/** Compact retired-resource identity retained after its runtime record is compacted. */
export interface ResourceTombstone {
  id: ResourceId;
  /** Durable identity of this resource incarnation. */
  incarnation?: string;
  kind: string;
  parentId?: ResourceId;
  path?: string;
  owner?: AgentId;
  version: number;
  status: "retired";
  createdAt: number;
  retiredAt: number;
  updatedAt: number;
}

export interface RequestTombstone {
  id: RequestId;
  status: Exclude<RequestStatus, "pending">;
  resolvedAt: number;
}

/** Durable FIFO ticket for a turn waiting on global or route capacity. */
export interface ModelTurnWaiter {
  id: string;
  enqueuedSequence: number;
  agentId: AgentId;
  route: ModelRoute;
  capacityKey: string;
  enqueuedAt: number;
  /** Identifies the exact logical begin_turn request that may claim a grant. */
  operationId?: string;
  purpose?: ModelTurnPurpose;
  state?: ModelTurnState;
  grantedAt?: number;
  grantExpiresAt?: number;
  /** Number of expired unclaimed leases; after one retry the ticket demotes. */
  grantExpiryCount?: number;
}

/**
 * A short-lived write fence covering one in-flight guarded write. While a
 * fence is active the coordinator grants no conflicting lease to another
 * actor, which shrinks the formal check→write window: a hold that lapses
 * mid-write cannot be handed to a competing writer until the fence ends or
 * expires. Fences are deliberately ephemeral in-memory records. The matched
 * resource carries a separate journaled restart quarantine through the same
 * expiry, so recovery retains write exclusion even though the live fence
 * object is not restored across an independent broker restart.
 */
export interface WriteFenceRecord {
  id: string;
  resourceId: ResourceId;
  actorId: AgentId;
  createdAt: number;
  expiresAt: number;
}

/**
 * An opaque shared-workspace shell operation. Unlike a resource write fence,
 * this barrier covers the whole managed workspace because the executable's
 * write set is not knowable before it runs.
 */
export interface OpaqueShellBarrierRecord {
  id: string;
  actorId: AgentId;
  createdAt: number;
  /** Number of concurrent opaque commands from the owning host. */
  references?: number;
}

export interface ActiveShellBarrierSummary {
  id: string;
  actorId: AgentId;
}

export interface ResourceHold {
  leaseId: LeaseId;
  agentId: AgentId;
  mode: BorrowMode;
  acquiredAt: number;
  lastHeartbeat: number;
  expiresAt: number;
  leaseMs: number;
}

export interface ResourceWaiter {
  requestId: RequestId;
  /** Coordinator-assigned FIFO position; optional for pre-hardening journals. */
  enqueuedSequence?: number;
  agentId: AgentId;
  mode: BorrowMode;
  enqueuedAt: number;
  leaseMs: number;
}

export interface ResourceRecord {
  id: ResourceId;
  /** Durable incarnation identity; unlike `version`, it survives retirement history pruning. */
  incarnation?: string;
  kind: string;
  parentId?: ResourceId;
  /** Workspace-relative path for mechanically guarded file/module writes. */
  path?: string;
  owner?: AgentId;
  version: number;
  grants: Record<AgentId, ResourcePermission[]>;
  sharedHolds: ResourceHold[];
  mutableHold?: ResourceHold;
  waiters: ResourceWaiter[];
  /** Explicit lifecycle; retired resources remain inspectable but cannot be borrowed. */
  status?: ResourceStatus;
  retiredAt?: number;
  /** Durable restart quarantine for an in-flight guarded write. */
  writeQuarantineUntil?: number;
  /** Actor that started the quarantined write, when known. */
  writeQuarantineActorId?: AgentId;
  /** Fence identity used to clear the quarantine on a normal end_write. */
  writeQuarantineFenceId?: string;
  createdAt: number;
  updatedAt: number;
}

export interface AgentMessage {
  id: MessageId;
  from: AgentId;
  to: AgentId;
  type: MessageType;
  body: string;
  /** Monotonic durable payload revision; coalesced messages retain their ID. */
  revision?: number;
  /** Monotonic sequence assigned by the sender; inboxes sort by this value. */
  senderSequence: number;
  /** Monotonic broker sequence used for durable cross-sender replay ordering. */
  brokerSequence?: number;
  requestId?: RequestId;
  replyTo?: MessageId;
  priority: MessagePriority;
  createdAt: number;
  metadata?: Record<string, unknown>;
  clientDedupeKey?: string;
  deliveredAt?: number;
  acknowledgedAt?: number;
  /** Message was retained as undeliverable after its recipient became terminal. */
  abandonedAt?: number;
}

export type RequestStatus = "pending" | "resolved" | "failed" | "cancelled";

export interface RequestRecord {
  id: RequestId;
  messageId: MessageId;
  from: AgentId;
  to: AgentId;
  status: RequestStatus;
  createdAt: number;
  resolvedAt?: number;
  responseMessageId?: MessageId;
  failureReason?: string;
}

export interface FabricConfig {
  /**
   * Fold workspace-relative policy keys case-insensitively. Undefined means
   * auto: true on Windows, false elsewhere, unless the broker probes the
   * filesystem at startup. Set explicitly to keep policy identity aligned
   * with a case-insensitive workspace volume (for example on macOS).
   */
  caseInsensitivePaths?: boolean;
  maxDepth: number;
  /** Maximum number of non-terminal direct children at once. */
  maxChildrenPerAgent: number;
  /** Optional cumulative creation ceiling; omitted means no lifetime ceiling. */
  maxChildrenCreatedPerAgent?: number;
  maxTotalAgents: number;
  maxConcurrentAgents: number;
  maxMailboxMessages: number;
  maxMessageBody: number;
  maxTaskOutput: number;
  leaseMs: number;
  heartbeatMs: number;
  /** Mark an actor stale when no heartbeat is observed for this interval. */
  agentHeartbeatTimeoutMs?: number;
  /** Retain a stale reconnectable actor's slot for this long before retiring it. */
  reconnectGraceMs?: number;
  /** Lease duration for a granted model-turn ticket before it returns to the queue. */
  modelTurnGrantTtlMs?: number;
  messageRetention: number;
  /** Exact provider/model route capacities, keyed as `provider/model`. */
  modelRouteCapacity?: Record<string, number>;
  /** Pluggable route policy form; `maxConcurrent` and budget are optional. */
  modelRoutePolicies?: Record<string, ModelRoutePolicy>;
  /** Compatibility alias for callers that prefer the plural spelling. */
  modelRouteCapacities?: Record<string, number>;
  /** Explicit effective prefill budgets keyed as `provider/model`. */
  effectivePrefillBudgets?: Record<string, number>;
  /** Age after which terminal hot records may be moved to compact tombstones. */
  historyRetentionMs?: number;
  /** Bound each compact tombstone collection so cold history cannot grow forever. */
  maxArchivedRecords?: number;
  /** Bound retained-artifact metadata; resolved entries are pruned first. */
  maxRetainedArtifacts?: number;
  /** Maximum records archived by one maintenance pass. */
  historyGcBatchSize?: number;
  /** Managed child shell policy; shared children default to coordination. */
  shellPolicy?: ShellPolicy;
  /** Explicit path boundary for managed child shell arguments. */
  externalPathAccess?: ExternalPathAccess;
}

export const DEFAULT_FABRIC_CONFIG: FabricConfig = {
  maxDepth: 4,
  maxChildrenPerAgent: 8,
  maxTotalAgents: 32,
  maxConcurrentAgents: 8,
  maxMailboxMessages: 512,
  maxMessageBody: 64 * 1024,
  maxTaskOutput: 32 * 1024,
  leaseMs: 30 * 60 * 1000,
  heartbeatMs: 60 * 1000,
  agentHeartbeatTimeoutMs: 3 * 60 * 1000,
  reconnectGraceMs: 10 * 60 * 1000,
  modelTurnGrantTtlMs: 30 * 1000,
  messageRetention: 2048,
  historyRetentionMs: 24 * 60 * 60 * 1000,
  maxArchivedRecords: 4_096,
  maxRetainedArtifacts: 4_096,
  historyGcBatchSize: 256,
  shellPolicy: "coordination",
  externalPathAccess: "deny",
};

type PersistedResourceRecord = Omit<ResourceRecord, "incarnation"> & { incarnation?: string };

interface PersistedCoordinatorStateFields {
  nextMessageSequence: Record<AgentId, number>;
  agents: AgentRecord[];
  tasks: TaskRecord[];
  messages: AgentMessage[];
  requests: RequestRecord[];
  dedupe: Array<[string, MessageId]>;
  /** Optional for replay compatibility with pre-hardening journals. */
  nextBrokerSequence?: number;
  /** Optional for replay compatibility with pre-hardening journals. */
  nextResourceWaiterSequence?: number;
  /** Optional for replay compatibility with pre-idempotency journals. */
  idempotency?: IdempotencyStateEntry[];
  /** Optional for replay compatibility with pre-R7 journals. */
  nextModelTurnWaiterSequence?: number;
  /** Optional for replay compatibility with pre-R7 journals. */
  modelWaiters?: ModelTurnWaiter[];
  /** Compact terminal history retained for dependencies and identity fencing. */
  archivedAgents?: AgentTombstone[];
  archivedTasks?: TaskTombstone[];
  archivedRequests?: RequestTombstone[];
  /** Legacy/full form; production broker checkpoints keep this collection external. */
  retainedArtifacts?: RetainedArtifactRecord[];
  /** Compact references emitted when retained artifact metadata is externalized. */
  retainedArtifactIds?: string[];
  /** Optional in standalone coordinator snapshots; brokered ACK proofs are cold. */
  acknowledgedMessages?: MessageAckTombstone[];
  /** Capacity reservations for provider calls that may outlive a broker restart. */
  recoveryTurnReservations?: ModelTurnRecoveryReservation[];
  /** Opaque shared-workspace shell barriers that may span a broker restart. */
  shellBarriers?: OpaqueShellBarrierRecord[];
}

/** Persisted state written by v0.2.x. It is accepted only as migration input. */
export interface PersistedCoordinatorStateV1 extends PersistedCoordinatorStateFields {
  version: 1;
  resources: PersistedResourceRecord[];
  /** Compact retired-resource history introduced after v0.2.3. */
  archivedResources?: Array<Omit<ResourceTombstone, "incarnation"> & { incarnation?: string }>;
}

/** Persisted state written by v0.3.x and later. */
export interface PersistedCoordinatorStateV2 extends PersistedCoordinatorStateFields {
  version: 2;
  resources: ResourceRecord[];
  /** Compact retired-resource history; active resources stay in `resources`. */
  archivedResources?: ResourceTombstone[];
}

export type PersistedCoordinatorState = PersistedCoordinatorStateV1 | PersistedCoordinatorStateV2;

export interface AgentSummary {
  id: AgentId;
  /** Stable pagination cursor present only on discover.agents projections. */
  cursor?: string;
  parentId?: AgentId;
  depth: number;
  role: string;
  taskId?: TaskId;
  route: ModelRoute;
  status: AgentStatus;
  /** A failed actor may still reconnect during its bounded recovery window. */
  reconnectable?: boolean;
  /** Broker timestamp at which a recovery-retired actor became terminal. */
  recoveryExpiredAt?: number;
  /** Durable marker set after the matching workspace/session artifacts are handled. */
  artifactsCleanedAt?: number;
  artifactDisposition?: ArtifactDisposition;
  retainedArtifactId?: string;
  workspace?: WorkspaceInfo;
  lastActivity: number;
  contextMode?: string;
  contextDiagnostic?: string;
  capabilities?: AgentCapabilities;
}

export interface ActiveFenceSummary {
  id: string;
  resourceId: ResourceId;
  path?: string;
  actorId: AgentId;
}

export interface FabricStatus {
  rootId: RootId;
  rootAgentId?: AgentId;
  /** Bounded diagnostic pages; use the list/show operations for complete records. */
  agents: AgentSummary[];
  tasks: TaskRecord[];
  resources: ResourceRecord[];
  pendingRequests: RequestRecord[];
  recentMessages: AgentMessage[];
  runningChildren: number;
  totalAgents?: number;
  totalTasks?: number;
  totalResources?: number;
  totalPendingRequests?: number;
  totalRecentMessages?: number;
  config: FabricConfig;
  activeFences?: number;
  activeMutableHolds?: number;
  fences?: ActiveFenceSummary[];
  activeWriteQuarantines?: number;
  /** Opaque shared-workspace shell operations currently holding the global barrier. */
  activeShellBarriers?: number;
  shellBarriers?: ActiveShellBarrierSummary[];
  /** Durable turn requests waiting for global or route capacity. */
  pendingModelTurns?: ModelTurnWaiter[];
  /** Physical route reservations awaiting host recovery confirmation. */
  recoveryTurnReservations?: ModelTurnRecoveryReservation[];
  /** Counts of terminal records moved out of hot coordinator state. */
  archivedCounts?: { agents: number; tasks: number; requests: number; resources?: number };
  truncated?: { agents: boolean; tasks: boolean; resources: boolean; pendingRequests: boolean; recentMessages: boolean };
}

export interface FabricTaskSummary {
  id: TaskId;
  status: TaskStatus;
  owner?: AgentId;
  description?: string;
}

export interface FabricResourceSummary {
  id: ResourceId;
  path?: string;
  holder?: AgentId;
}

/** Bounded server-side projection used for quiescence and replacement decisions. */
export interface FabricSnapshot {
  rootId: RootId;
  rootAgentId?: AgentId;
  rootStatus?: AgentStatus;
  caseInsensitivePaths?: boolean;
  runningChildren: number;
  recoveringAgents: number;
  unresolvedChildTasks: number;
  unownedUnresolvedTasks: number;
  mutableHolds: number;
  activeWriteFences: number;
  activeWriteQuarantines: number;
  /** Opaque shared-workspace shell operations currently holding the global barrier. */
  activeShellBarriers: number;
  pendingRootRequests: number;
  pendingModelTurns: number;
  /** Bounded fence details; the count remains authoritative when truncated. */
  fences?: ActiveFenceSummary[];
  activeTasks: FabricTaskSummary[];
  mutableResources: FabricResourceSummary[];
}

export type CoordinatorEvent =
  | { type: "agent_registered"; agent: AgentRecord }
  | { type: "agent_updated"; agent: AgentRecord }
  | { type: "agent_terminal"; agent: AgentRecord }
  | { type: "task_changed"; task: TaskRecord }
  | { type: "resource_changed"; resource: ResourceRecord }
  | { type: "message_sent"; message: AgentMessage; request?: RequestRecord }
  | { type: "message_updated"; message: AgentMessage }
  | { type: "message_acknowledged"; message: AgentMessage }
  | { type: "messages_pruned"; ids: MessageId[] }
  | { type: "request_changed"; request: RequestRecord }
  | { type: "slot_available"; agentId: AgentId }
  | { type: "model_turn_waiting"; waiter: ModelTurnWaiter }
  | { type: "model_turn_granted"; waiterId: string; agentId: AgentId; operationId?: string; purpose?: ModelTurnPurpose; grantedAt?: number; grantExpiresAt?: number }
  | { type: "model_turn_claimed"; waiterId: string; agentId: AgentId; operationId?: string; purpose?: ModelTurnPurpose }
  | { type: "model_turn_cancelled"; waiterId: string; agentId: AgentId }
  | { type: "model_turn_recovery_reserved"; reservation: ModelTurnRecoveryReservation }
  | { type: "model_turn_recovery_resolved"; agentId: AgentId; operationId?: string; state: ModelTurnRecoveryState | "expired" }
  | { type: "shell_barrier_started"; barrier: OpaqueShellBarrierRecord }
  | { type: "shell_barrier_released"; barrierId: string; actorId: AgentId; remainingReferences?: number }
  | { type: "agent_artifacts_retained"; agentId: AgentId; artifact: RetainedArtifactRecord }
  | { type: "agent_artifacts_resolved"; agentId: AgentId; artifactId: string; resolvedAt: number; resolution?: string }
  | { type: "agent_artifact_pruned"; artifactId: string }
  | { type: "resource_archived"; resource: ResourceTombstone }
  | { type: "resource_archive_pruned"; resourceId: ResourceId }
  | { type: "agent_archived"; agent: AgentTombstone }
  | { type: "task_archived"; task: TaskTombstone }
  | { type: "request_archived"; request: RequestTombstone }
  | { type: "agent_archive_pruned"; agentId: AgentId }
  | { type: "task_archive_pruned"; taskId: TaskId }
  | { type: "request_archive_pruned"; requestId: RequestId }
  | { type: "diagnostic"; code: string; message: string; details?: Record<string, unknown> };

export interface DispatchResult<T = unknown> {
  value: T;
  events: CoordinatorEvent[];
  /** Present when the request carried an operationId; journal it with the events. */
  idempotency?: IdempotencyRecord;
}

/**
 * Durable deduplication record for an idempotent write.
 * A replay of the same actor + operationId + arguments returns the original
 * response instead of creating a second child or task.
 */
export interface IdempotencyRecord {
  readonly actorId: AgentId;
  readonly operationId: string;
  readonly operation: string;
  readonly requestHash: string;
  readonly response: unknown;
}

export interface IdempotencyStateEntry {
  readonly key: string;
  readonly operation: string;
  readonly requestHash: string;
  readonly response: unknown;
}

export function cloneCapabilities(capabilities: AgentCapabilities): AgentCapabilities {
  const legacy = capabilities as AgentCapabilities & { peerIds?: AgentId[]; resourceGrants?: Record<string, ResourcePermission[]> };
  return {
    ...capabilities,
    peerIds: [...(legacy.peerIds ?? [])],
    resourceGrants: Object.fromEntries(
      Object.entries(legacy.resourceGrants ?? {}).map(([resourceId, permissions]) => [resourceId, [...permissions]]),
    ),
  };
}

export function cloneAgent(agent: AgentRecord): AgentRecord {
  return {
    ...agent,
    capabilities: cloneCapabilities(agent.capabilities),
    workspace: agent.workspace ? { ...agent.workspace } : undefined,
    contextMode: agent.contextMode,
  };
}

export function cloneModelTurnWaiter(waiter: ModelTurnWaiter): ModelTurnWaiter {
  return { ...waiter, route: { ...waiter.route } };
}

export function cloneModelTurnRecoveryReservation(reservation: ModelTurnRecoveryReservation): ModelTurnRecoveryReservation {
  return { ...reservation, route: { ...reservation.route } };
}

export function cloneTask(task: TaskRecord): TaskRecord {
  return { ...task, dependencies: [...(task.dependencies ?? [])], result: task.result ? { ...task.result } : undefined };
}

export function cloneResource(resource: ResourceRecord): ResourceRecord {
  return {
    ...resource,
    status: resource.status ?? "active",
    grants: Object.fromEntries(Object.entries(resource.grants).map(([id, permissions]) => [id, [...permissions]])),
    sharedHolds: resource.sharedHolds.map((hold) => ({ ...hold })),
    mutableHold: resource.mutableHold ? { ...resource.mutableHold } : undefined,
    waiters: resource.waiters.map((waiter) => ({ ...waiter })),
  };
}

export function resourceFromTombstone(tombstone: ResourceTombstone): ResourceRecord {
  return {
    id: tombstone.id,
    incarnation: tombstone.incarnation,
    kind: tombstone.kind,
    parentId: tombstone.parentId,
    path: tombstone.path,
    owner: tombstone.owner,
    version: tombstone.version,
    status: "retired",
    grants: {},
    sharedHolds: [],
    waiters: [],
    retiredAt: tombstone.retiredAt,
    createdAt: tombstone.createdAt,
    updatedAt: tombstone.updatedAt,
  };
}

export function cloneMessage(message: AgentMessage): AgentMessage {
  return {
    ...message,
    revision: Number.isInteger(message.revision) && (message.revision as number) > 0 ? message.revision : 1,
    metadata: message.metadata ? JSON.parse(JSON.stringify(message.metadata)) as Record<string, unknown> : undefined,
  };
}

export function cloneRequest(request: RequestRecord): RequestRecord {
  return { ...request };
}

export function cloneAckTombstone(tombstone: MessageAckTombstone): MessageAckTombstone {
  return { ...tombstone };
}
