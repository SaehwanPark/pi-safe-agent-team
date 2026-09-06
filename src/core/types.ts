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
  | "completed"
  | "failed"
  | "cancelled";

export type TaskStatus = "pending" | "ready" | "active" | "waiting" | "blocked" | "completed" | "failed" | "cancelled";
export type BorrowMode = "shared" | "mutable";
export type MessagePriority = "normal" | "urgent";

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
}

export interface TaskResult {
  summary: string;
  output?: string;
  completedAt: number;
  by: AgentId;
}

export interface TaskRecord {
  id: TaskId;
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
  createdAt: number;
  updatedAt: number;
}

export interface AgentMessage {
  id: MessageId;
  from: AgentId;
  to: AgentId;
  type: MessageType;
  body: string;
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
  maxDepth: number;
  maxChildrenPerAgent: number;
  maxTotalAgents: number;
  maxConcurrentAgents: number;
  maxMailboxMessages: number;
  maxMessageBody: number;
  maxTaskOutput: number;
  leaseMs: number;
  heartbeatMs: number;
  messageRetention: number;
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
  messageRetention: 2048,
};

export interface PersistedCoordinatorState {
  version: 1;
  nextMessageSequence: Record<AgentId, number>;
  agents: AgentRecord[];
  tasks: TaskRecord[];
  resources: ResourceRecord[];
  messages: AgentMessage[];
  requests: RequestRecord[];
  dedupe: Array<[string, MessageId]>;
  /** Optional for replay compatibility with pre-hardening journals. */
  nextBrokerSequence?: number;
  /** Optional for replay compatibility with pre-hardening journals. */
  nextResourceWaiterSequence?: number;
  /** Optional for replay compatibility with pre-idempotency journals. */
  idempotency?: IdempotencyStateEntry[];
}

export interface AgentSummary {
  id: AgentId;
  parentId?: AgentId;
  depth: number;
  role: string;
  taskId?: TaskId;
  route: ModelRoute;
  status: AgentStatus;
  workspace?: WorkspaceInfo;
  lastActivity: number;
}

export interface FabricStatus {
  rootId: RootId;
  agents: AgentSummary[];
  tasks: TaskRecord[];
  resources: ResourceRecord[];
  pendingRequests: RequestRecord[];
  recentMessages: AgentMessage[];
  runningChildren: number;
  config: FabricConfig;
}

export type CoordinatorEvent =
  | { type: "agent_registered"; agent: AgentRecord }
  | { type: "agent_updated"; agent: AgentRecord }
  | { type: "agent_terminal"; agent: AgentRecord }
  | { type: "task_changed"; task: TaskRecord }
  | { type: "resource_changed"; resource: ResourceRecord }
  | { type: "message_sent"; message: AgentMessage; request?: RequestRecord }
  | { type: "message_acknowledged"; message: AgentMessage }
  | { type: "messages_pruned"; ids: MessageId[] }
  | { type: "request_changed"; request: RequestRecord }
  | { type: "slot_available"; agentId: AgentId }
  | { type: "diagnostic"; code: string; message: string; details?: Record<string, unknown> };

export interface DispatchResult<T = unknown> {
  value: T;
  events: CoordinatorEvent[];
  /** Present when the request carried an operationId; journal it with the events. */
  idempotency?: IdempotencyRecord;
}

/**
 * Durable deduplication record for an idempotent write (agent.spawn, task.create).
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
  return { ...agent, capabilities: cloneCapabilities(agent.capabilities), workspace: agent.workspace ? { ...agent.workspace } : undefined };
}

export function cloneTask(task: TaskRecord): TaskRecord {
  return { ...task, dependencies: [...task.dependencies], result: task.result ? { ...task.result } : undefined };
}

export function cloneResource(resource: ResourceRecord): ResourceRecord {
  return {
    ...resource,
    grants: Object.fromEntries(Object.entries(resource.grants).map(([id, permissions]) => [id, [...permissions]])),
    sharedHolds: resource.sharedHolds.map((hold) => ({ ...hold })),
    mutableHold: resource.mutableHold ? { ...resource.mutableHold } : undefined,
    waiters: resource.waiters.map((waiter) => ({ ...waiter })),
  };
}

export function cloneMessage(message: AgentMessage): AgentMessage {
  return {
    ...message,
    metadata: message.metadata ? JSON.parse(JSON.stringify(message.metadata)) as Record<string, unknown> : undefined,
  };
}

export function cloneRequest(request: RequestRecord): RequestRecord {
  return { ...request };
}
