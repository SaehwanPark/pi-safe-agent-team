import { createHash } from "node:crypto";
import { realpathSync } from "node:fs";
import { promises as fs } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { createAgentSession, DefaultResourceLoader, getAgentDir, ModelRuntime, SessionManager, SettingsManager, type AgentSession, type ExtensionAPI, type ExtensionContext, type ToolDefinition } from "@earendil-works/pi-coding-agent";
import type { Model } from "@earendil-works/pi-ai";
import type { ThinkingLevel } from "@earendil-works/pi-agent-core";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { FabricError, asFabricError } from "../core/errors.ts";
import { Coordinator } from "../core/coordinator.ts";
import { DEFAULT_FABRIC_CONFIG, type AgentStatus, type FabricConfig, type AgentMessage, type AgentRecord, type FabricStatus, type ModelRoute, type TaskRecord } from "../core/types.ts";
import { BrokerClient } from "../broker/client.ts";
import { BrokerServer, defaultEndpoint, detectCaseInsensitivePaths } from "../broker/server.ts";
import { GitWorkspaceStrategy, type WorkspaceStrategy } from "../workspace.ts";
import { resolveChildModel, routeFromModel } from "./model-routing.ts";
import { createCoordinationTools, type SpawnToolInput } from "./tools.ts";
import { createGuardedChildTools, createGuardedReadOnlyTools, evaluateRootShellGuard, evaluateRootWriteGuard, releaseRootWriteFence, type RootWriteGuardOutcome } from "./guards.ts";
import { createEmbeddedContextController, type EmbeddedCompactionRequest, type EmbeddedContextHost, type EmbeddedContextManager, type FabricSnapshotRequest, type FabricStateSnapshotV1 } from "./interop.ts";
import { effectivePrefillBudget } from "../core/coordinator-wire.ts";
import { ModelRouteCapacityArbiter } from "./model-capacity.ts";
import { classifyAssistantMessage, classifyCompactionFailure, describeTurnOutcome, findFinalAssistantMessage, isBlockingOutcome, type ModelTurnOutcome } from "./turn-outcome.ts";

export interface RoleConfig {
  model?: string;
  provider?: string;
  thinking?: ThinkingLevel;
  capabilities?: Partial<AgentRecord["capabilities"]>;
}

export interface FabricRuntimeOptions {
  cwd?: string;
  /** Optional session identity for programmatic hosts and deterministic tests. */
  sessionId?: string;
  fabricId?: string;
  stateDirectory?: string;
  agentDir?: string;
  endpoint?: string;
  config?: Partial<FabricConfig>;
  defaults?: RoleConfig;
  roles?: Record<string, RoleConfig>;
  workspaceStrategy?: WorkspaceStrategy;
  startBroker?: boolean;
}

export interface SpawnedAgentSummary {
  agent: Omit<AgentRecord, "authToken">;
  taskId?: string;
  workspace?: AgentRecord["workspace"];
  routeSource?: string;
  contextMode?: string;
  summaryText?: string;
}

export type DescendantShutdownMode = "graceful" | "budget" | "now";

/** Deterministic, model-free recovery state captured before descendant abort. */
export interface HandoffSnapshot {
  agentId: string;
  parentId?: string;
  role: string;
  status: AgentStatus;
  task?: Pick<TaskRecord, "id" | "description" | "status" | "owner" | "result">;
  workspace?: AgentRecord["workspace"];
  route: ModelRoute;
  lastActivity: number;
  pendingRequests: string[];
  mutableResources: string[];
  activeWriteFences: string[];
  reason: string;
}

/** Derive a host lifecycle result from durable task facts, never model text. */
export function taskAwareTurnStatus(task: Pick<TaskRecord, "status"> | undefined, hasPendingReply: boolean): AgentStatus {
  switch (task?.status) {
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "cancelled":
      return "cancelled";
    case "blocked":
      return "blocked";
    default:
      return hasPendingReply ? "waiting" : "ready";
  }
}

interface RootBinding {
  api: ExtensionAPI;
  ctx: ExtensionContext;
  agentId: string;
  client: BrokerClient;
}

function normalizeSessionId(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function hashIdentity(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 24);
}

function canonicalWorkspacePath(value: string): string {
  const resolved = resolve(value);
  try {
    return realpathSync.native(resolved);
  } catch {
    return resolved;
  }
}

function resolvePiSessionId(ctx: ExtensionContext): string {
  const sessionManager = ctx.sessionManager as unknown as {
    getSessionId?: () => unknown;
    sessionId?: unknown;
  };
  let value: unknown;
  if (typeof sessionManager?.getSessionId === "function") {
    value = sessionManager.getSessionId();
  } else {
    value = sessionManager?.sessionId;
  }
  const sessionId = normalizeSessionId(value);
  if (!sessionId) {
    throw new FabricError("INVALID_ARGUMENT", "Pi session identity is required before attaching a fabric root");
  }
  return sessionId;
}

export class FabricRuntime {
  readonly cwd: string;
  private _fabricId: string;
  private _stateDirectory: string;
  readonly agentDir: string;
  private _endpoint: string;
  readonly config?: Partial<FabricConfig>;
  readonly workspaceStrategy: WorkspaceStrategy;

  private readonly options: FabricRuntimeOptions;
  private server?: BrokerServer;
  private root?: RootBinding;
  private rootToken?: string;
  private modelRegistry?: ModelRegistry;
  private childModelRuntime?: ModelRuntime;
  private roles: Record<string, RoleConfig>;
  private children = new Map<string, ManagedChild>();
  private rootEventUnsubscribe?: () => void;
  private rootCloseUnsubscribe?: () => void;
  private rootHeartbeatTimer?: NodeJS.Timeout;
  private rootReconnectPromise?: Promise<void>;
  private stopped = false;
  private rootDelivery?: (message: AgentMessage) => void;
  private caseInsensitivePaths?: boolean;
  private sessionId?: string;
  private draining = false;
  /** Invalidates in-flight spawns when a shutdown begins, even after draining ends. */
  private lifecycleEpoch = 0;
  private lastHandoffSnapshots: HandoffSnapshot[] = [];
  private readonly modelCapacity: ModelRouteCapacityArbiter;
  private rootCompactionInFlight = 0;
  private rootCompactionReleases: Array<() => void> = [];
  private rootCompactionBrokerReservations: boolean[] = [];
  private rootContextHealth: "healthy" | "degraded" = "healthy";
  private rootContextDiagnostic?: string;
  private drainPromise?: Promise<readonly HandoffSnapshot[]>;
  private drainDeadlineAt = 0;
  private drainWake?: () => void;
  private drainChildren: ManagedChild[] = [];

  static readonly shutdownRpcTimeoutMs = 1_500;
  static readonly shutdownAbortTimeoutMs = 1_500;
  static readonly shutdownDeadlineMs = 3_000;

  get fabricId(): string {
    return this._fabricId;
  }

  get stateDirectory(): string {
    return this._stateDirectory;
  }

  get endpoint(): string {
    return this._endpoint;
  }

  get isDraining(): boolean {
    return this.draining;
  }

  get isRootCompactionInFlight(): boolean {
    return this.rootCompactionInFlight > 0;
  }

  get rootHealth(): "healthy" | "degraded" {
    return this.rootContextHealth;
  }

  get rootHealthDiagnostic(): string | undefined {
    return this.rootContextDiagnostic;
  }

  get handoffSnapshots(): readonly HandoffSnapshot[] {
    return this.lastHandoffSnapshots.map((snapshot) => ({
      ...snapshot,
      task: snapshot.task ? { ...snapshot.task, result: snapshot.task.result ? { ...snapshot.task.result } : undefined } : undefined,
      workspace: snapshot.workspace ? { ...snapshot.workspace } : undefined,
      route: { ...snapshot.route },
      pendingRequests: [...snapshot.pendingRequests],
      mutableResources: [...snapshot.mutableResources],
      activeWriteFences: [...snapshot.activeWriteFences],
    }));
  }

  constructor(options: FabricRuntimeOptions = {}) {
    this.options = options;
    this.cwd = canonicalWorkspacePath(options.cwd ?? process.cwd());
    this.agentDir = options.agentDir ?? getAgentDir();
    this.config = options.config;
    this.caseInsensitivePaths = options.config?.caseInsensitivePaths;
    this.workspaceStrategy = options.workspaceStrategy ?? new GitWorkspaceStrategy();
    this.roles = options.roles ?? {};
    this.sessionId = normalizeSessionId(options.sessionId);
    const identityKey = this.identityKey(this.sessionId ?? "unattached");
    this._fabricId = options.fabricId ?? `fabric-${hashIdentity(identityKey)}`;
    this._stateDirectory = options.stateDirectory ?? join(this.agentDir, "safe-agents", hashIdentity(identityKey));
    this._endpoint = options.endpoint ?? defaultEndpoint(this._stateDirectory);
    this.modelCapacity = new ModelRouteCapacityArbiter({ ...DEFAULT_FABRIC_CONFIG, ...(options.config ?? {}) });
  }

  /** Begin observing a root manual/automatic compaction before Pi mutates context. */
  async beginRootCompaction(): Promise<void> {
    this.rootCompactionInFlight += 1;
    this.rootCompactionBrokerReservations.push(false);
    const route = this.root?.ctx.model ? routeFromModel(this.root.ctx.model, this.root.ctx.thinkingLevel ?? "medium") : undefined;
    if (!route) return;
    try {
      this.rootCompactionReleases.push(await this.modelCapacity.acquire(route));
    } catch {
      // The quiescence flag remains conservative when a local arbiter is unavailable.
    }
    if (this.root) {
      try {
        const result = await this.root.client.request<{ started?: boolean }>("agent.begin_turn", {});
        if (result?.started === true) this.rootCompactionBrokerReservations[this.rootCompactionBrokerReservations.length - 1] = true;
      } catch {
        // An already-running root (automatic compaction) or unavailable broker
        // is still covered by the local compaction flag.
      }
    }
  }

  async endRootCompaction(): Promise<void> {
    if (this.rootCompactionInFlight > 0) this.rootCompactionInFlight -= 1;
    this.rootCompactionReleases.pop()?.();
    const brokerReservation = this.rootCompactionBrokerReservations.pop() ?? false;
    if (brokerReservation && this.root) {
      await this.root.client.request("agent.end_turn", { status: "ready" }, FabricRuntime.shutdownRpcTimeoutMs).catch(() => undefined);
    }
  }

  resetRootCompactionState(): void {
    this.rootCompactionInFlight = 0;
    this.rootCompactionBrokerReservations = [];
    for (const release of this.rootCompactionReleases.splice(0)) release();
  }

  markRootContextDegraded(outcome: ModelTurnOutcome): void {
    if (outcome.kind === "success" || outcome.kind === "aborted") return;
    this.rootContextHealth = "degraded";
    this.rootContextDiagnostic = describeTurnOutcome(outcome);
  }

  markRootContextHealthy(): void {
    this.rootContextHealth = "healthy";
    this.rootContextDiagnostic = undefined;
  }

  resetRootContextHealth(): void {
    this.markRootContextHealthy();
  }

  async withModelRouteCapacity<T>(route: ModelRoute, operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    return this.modelCapacity.withCapacity(route, operation, signal);
  }

  getEffectivePrefillBudget(route: ModelRoute, logicalContextWindow?: number): number | undefined {
    return effectivePrefillBudget({ ...DEFAULT_FABRIC_CONFIG, ...(this.config ?? {}) }, route, logicalContextWindow);
  }

  private identityKey(sessionId: string): string {
    return this.options.fabricId
      ? `fabric\u0000${this.options.fabricId}`
      : `${this.cwd}\u0000${sessionId}`;
  }

  private configureSessionIdentity(sessionId: string): void {
    const normalized = normalizeSessionId(sessionId);
    if (!normalized) {
      throw new FabricError("INVALID_ARGUMENT", "Pi session identity is required before attaching a fabric root");
    }
    if (this.options.fabricId) {
      this.sessionId = normalized;
      return;
    }
    if (this.sessionId === normalized) {
      return;
    }
    if (this.root || this.server) {
      throw new FabricError("IDENTITY_CONFLICT", "Cannot change the default fabric identity while it is attached");
    }

    this.sessionId = normalized;
    const identityKey = this.identityKey(normalized);
    this._fabricId = `fabric-${hashIdentity(identityKey)}`;
    if (!this.options.stateDirectory) {
      this._stateDirectory = join(this.agentDir, "safe-agents", hashIdentity(identityKey));
    }
    if (!this.options.endpoint) {
      this._endpoint = defaultEndpoint(this._stateDirectory);
    }
    // A root token belongs to the previous default fabric identity.
    this.rootToken = undefined;
  }


  async attachRoot(api: ExtensionAPI, ctx: ExtensionContext, rootDelivery?: (message: AgentMessage) => void): Promise<RootBinding> {
    const sessionId = resolvePiSessionId(ctx);
    this.configureSessionIdentity(sessionId);
    // A Pi process may start another session after session_shutdown. The
    // durable root is reusable, so a fresh attachment re-enables reconnects.
    this.stopped = false;
    if (this.root) {
      this.root.api = api;
      this.root.ctx = ctx;
      this.rootDelivery = rootDelivery;
      this.modelRegistry = ctx.modelRegistry;
      if (ctx.model) {
        const refreshed = await this.root.client.request<{ agent: AgentRecord; token: string }>("agent.register", {
          rootId: this.fabricId,
          role: "root",
          route: routeFromModel(ctx.model, ctx.thinkingLevel ?? "medium"),
          capabilities: { maySpawn: true, mayMessagePeers: true, mayEscalate: true, mayTransferOwnership: true, mayWriteRepo: true, mayUseShell: true },
          sessionId,
          token: this.rootToken,
          workspace: { mode: "shared", root: ctx.cwd, path: ctx.cwd },
        });
        this.rootToken = refreshed.token;
        this.root.client.setIdentity(this.root.agentId, refreshed.token);
      }
      return this.root;
    }
    if (!ctx.model) throw new FabricError("MODEL_ROUTE_INVALID", "Pi has no selected model; select a model before starting the agent fabric");
    this.rootDelivery = rootDelivery;
    this.modelRegistry = ctx.modelRegistry;
    await this.ensureBroker();
    await this.loadRootToken();
    const agentId = `root-${createHash("sha256").update(this.fabricId).digest("hex").slice(0, 24)}`;
    const client = new BrokerClient({ endpoint: this.endpoint, agentId, token: this.rootToken });
    await client.connect();
    const route = routeFromModel(ctx.model, ctx.thinkingLevel ?? "medium");
    const result = await client.request<{ agent: AgentRecord; token: string }>("agent.register", {
      rootId: this.fabricId,
      role: "root",
      route,
      capabilities: {
        maySpawn: true,
        mayMessagePeers: true,
        mayEscalate: true,
        mayTransferOwnership: true,
        mayWriteRepo: true,
        mayUseShell: true,
      },
      sessionId,
      workspace: { mode: "shared", root: ctx.cwd, path: ctx.cwd },
      token: this.rootToken,
    });
    this.rootToken = result.token;
    await this.saveRootToken(result.token);
    client.setIdentity(agentId, result.token);
    this.root = { api, ctx, agentId, client };
    this.rootHeartbeatTimer = setInterval(() => {
      void client.request("agent.heartbeat", {}).catch(() => undefined);
    }, Math.max(1000, this.config?.heartbeatMs ?? 60_000));
    this.rootHeartbeatTimer.unref();
    this.rootEventUnsubscribe = client.onEvent((event) => this.handleRootEvent(event));
    this.rootCloseUnsubscribe = client.onClose(() => {
      void this.reconnectRoot();
    });
    return this.root;
  }

  async ensureRoot(api: ExtensionAPI, ctx: ExtensionContext, rootDelivery?: (message: AgentMessage) => void): Promise<void> {
    await this.attachRoot(api, ctx, rootDelivery);
  }

  get rootAgentId(): string | undefined {
    return this.root?.agentId;
  }

  get client(): BrokerClient | undefined {
    return this.root?.client;
  }

  async request<T = unknown>(
    operation: string,
    args: Record<string, unknown> = {},
    timeoutMs?: number,
    signal?: AbortSignal,
  ): Promise<T> {
    if (!this.root) throw new FabricError("BROKER_UNAVAILABLE", "Fabric root is not attached");
    return this.root.client.request<T>(operation, args, timeoutMs, signal);
  }

  async requestIdempotent<T = unknown>(
    operation: string,
    args: Record<string, unknown> = {},
    operationId?: string,
    timeoutMs?: number,
  ): Promise<T> {
    if (!this.root) throw new FabricError("BROKER_UNAVAILABLE", "Fabric root is not attached");
    return this.root.client.requestIdempotent<T>(operation, args, operationId, timeoutMs);
  }

  /** Coordinate a root-session file mutation against live borrowing state. */
  async guardRootMutation(toolName: string, input: unknown, workspacePath: string): Promise<RootWriteGuardOutcome | undefined> {
    if (!this.root) return undefined;
    if (this.draining) return { block: true, reason: "safe-agents blocked root writes while the fabric is draining descendants" };
    return evaluateRootWriteGuard({ client: this.root.client, workspacePath }, toolName, input);
  }

  /** Best-effort lift of a root write fence taken by guardRootMutation. */
  async releaseRootFence(fenceId: string, timeoutMs?: number): Promise<void> {
    if (!this.root) return;
    await releaseRootWriteFence(this.root.client, fenceId, timeoutMs);
  }

  /** Coordinate a root-session shell command against live child holds. */
  async guardRootShell(input: unknown, workspacePath: string): Promise<RootWriteGuardOutcome | undefined> {
    if (!this.root) return undefined;
    if (this.draining) return { block: true, reason: "safe-agents blocked root shell commands while the fabric is draining descendants" };
    return evaluateRootShellGuard({ client: this.root.client, workspacePath }, input);
  }

  private pendingRootDeliveriesCount = 0;
  private pendingRootFencesCount = 0;

  setPendingRootDeliveriesCount(count: number): void {
    this.pendingRootDeliveriesCount = count;
  }

  setPendingRootFencesCount(count: number): void {
    this.pendingRootFencesCount = count;
  }

  /** Provide a deterministic fabric state snapshot for context managers. */
  async getFabricStateSnapshot(request: FabricSnapshotRequest): Promise<FabricStateSnapshotV1 | null> {
    if (this.caseInsensitivePaths === undefined) {
      this.caseInsensitivePaths = this.config?.caseInsensitivePaths ?? detectCaseInsensitivePaths(this.cwd);
    }
    const caseInsensitive = this.caseInsensitivePaths;
    const normalizeScopePath = (p: string): string => {
      const resolved = canonicalWorkspacePath(p);
      return caseInsensitive ? resolved.toLowerCase() : resolved;
    };
    if (normalizeScopePath(request.cwd) !== normalizeScopePath(this.cwd)) {
      return null;
    }
    const rootSessionId = this.sessionId ?? (this.root?.ctx as any)?.sessionManager?.getSessionId?.() ?? (this.root?.ctx as any)?.sessionId;
    if (request.sessionId && rootSessionId && request.sessionId !== rootSessionId) {
      return null;
    }

    const now = Date.now();
    if (this.stopped || !this.root) {
      return {
        version: 1,
        active: false,
        quiescent: false,
        state: "known",
        sessionReplacementSafe: false,
        capturedAt: now,
        rootSessionId,
        cwd: this.cwd,
        runningChildren: 0,
        unresolvedChildTasks: 0,
        mutableHolds: 0,
        activeWriteFences: 0,
        pendingRootRequests: 0,
        pendingRootDeliveries: 0,
        rootCompactionInFlight: this.isRootCompactionInFlight,
        activeTasks: [],
        mutableResources: [],
        quiescenceReasons: ["fabric_runtime_unattached_or_stopped"],
      };
    }

    try {
      const status = (await this.status(request.signal)) as FabricStatus;
      if (status.config?.caseInsensitivePaths !== undefined) {
        this.caseInsensitivePaths = status.config.caseInsensitivePaths;
      }
      const rootAgent = status.agents.find((a) => a.id === status.rootId);
      const rootBusy = !rootAgent || rootAgent.status === "starting" || rootAgent.status === "running";

      const runningChildren = status.agents.filter(
        (a) => a.depth > 0 && (a.status === "starting" || a.status === "running" || a.status === "draining"),
      ).length;
      const unresolvedChildTasks = status.tasks.filter(
        (t) => t.owner && t.owner !== status.rootId && ["pending", "ready", "active", "waiting", "blocked"].includes(t.status),
      ).length;
      const mutableHolds = status.resources.filter((r) => r.mutableHold !== undefined).length;
      const pendingRootRequests = status.pendingRequests.filter(
        (r) => r.to === status.rootId && r.status === "pending",
      ).length;
      const pendingRootDeliveries = this.pendingRootDeliveriesCount;
      const activeWriteFences = status.activeFences ?? 0;

      const quiescenceReasons: string[] = [];
      if (this.draining) quiescenceReasons.push("fabric_draining");
      if (this.isRootCompactionInFlight) quiescenceReasons.push("root_compaction_in_flight");
      if (rootBusy) quiescenceReasons.push("root_agent_active_or_running");
      if (runningChildren > 0) quiescenceReasons.push("running_children_active");
      if (unresolvedChildTasks > 0) quiescenceReasons.push("unresolved_child_tasks");
      if (mutableHolds > 0) quiescenceReasons.push("active_mutable_holds");
      if (activeWriteFences > 0) quiescenceReasons.push("active_write_fences");
      if (pendingRootRequests > 0) quiescenceReasons.push("pending_root_requests");
      if (pendingRootDeliveries > 0) quiescenceReasons.push("pending_root_deliveries");

      const quiescent = quiescenceReasons.length === 0;
      const sessionReplacementSafe = quiescent;

      const activeTasks = status.tasks
        .filter((t) => ["pending", "ready", "active", "waiting", "blocked"].includes(t.status))
        .slice(0, 50)
        .map((t) => ({
          id: t.id,
          status: t.status,
          owner: t.owner,
          description: t.description,
        }));

      const mutableResources = status.resources
        .filter((r) => r.mutableHold !== undefined)
        .slice(0, 50)
        .map((r) => ({
          id: r.id,
          path: r.path,
          holder: r.mutableHold?.agentId,
        }));

      return {
        version: 1,
        active: true,
        quiescent,
        state: "known",
        sessionReplacementSafe,
        capturedAt: now,
        rootSessionId,
        cwd: this.cwd,
        runningChildren,
        unresolvedChildTasks,
        mutableHolds,
        activeWriteFences,
        pendingRootRequests,
        pendingRootDeliveries,
        rootCompactionInFlight: this.isRootCompactionInFlight,
        activeTasks,
        mutableResources,
        quiescenceReasons,
      };
    } catch {
      return {
        version: 1,
        active: true,
        quiescent: false,
        state: "uncertain",
        sessionReplacementSafe: false,
        capturedAt: now,
        rootSessionId,
        cwd: this.cwd,
        runningChildren: 0,
        unresolvedChildTasks: 0,
        mutableHolds: 0,
        activeWriteFences: 0,
        pendingRootRequests: 0,
        pendingRootDeliveries: this.pendingRootDeliveriesCount,
        rootCompactionInFlight: this.isRootCompactionInFlight,
        activeTasks: [],
        mutableResources: [],
        quiescenceReasons: ["broker_status_query_failed"],
      };
    }
  }

  async status(signal?: AbortSignal, timeoutMs?: number): Promise<unknown> {
    return this.request("fabric.status", {}, timeoutMs, signal);
  }

  async modelRuntimeForChildren(): Promise<ModelRuntime> {
    if (!this.childModelRuntime) {
      this.childModelRuntime = await ModelRuntime.create({
        authPath: join(this.agentDir, "auth.json"),
        modelsPath: join(this.agentDir, "models.json"),
        allowModelNetwork: false,
        refreshOnCreate: false,
      });
    }
    return this.childModelRuntime;
  }

  async spawnFromRoot(input: SpawnToolInput, parentModel?: Model<any>, parentThinking?: string): Promise<SpawnedAgentSummary> {
    if (!this.root || !this.modelRegistry) throw new FabricError("BROKER_UNAVAILABLE", "Fabric root is not attached");
    if (this.draining) throw new FabricError("LIFECYCLE_CONFLICT", "The fabric is draining; new children are not admitted");
    return this.spawnChild(this.root.agentId, this.root.client, input, parentModel ?? this.root.ctx.model, parentThinking ?? this.root.ctx.thinkingLevel ?? "medium", this.root.ctx.cwd, this.lifecycleEpoch);
  }

  async spawnChildFrom(parent: ManagedChild, input: SpawnToolInput): Promise<SpawnedAgentSummary> {
    if (this.draining) throw new FabricError("LIFECYCLE_CONFLICT", "The fabric is draining; new children are not admitted");
    return this.spawnChild(parent.agentId, parent.client, input, parent.session?.model, parent.session?.thinkingLevel ?? "medium", parent.workspacePath, this.lifecycleEpoch);
  }

  /** Remove only the stopped instance; a replacement with the same id wins. */
  onChildStopped(child: ManagedChild): void {
    if (this.children.get(child.agentId) === child) this.children.delete(child.agentId);
  }

  /**
   * Capture deterministic recovery metadata and stop every managed descendant.
   * This is deliberately separate from stop(): the root remains attached and
   * usable after an aborted run, while a session shutdown tears down the host.
   */
  async abortDescendants(options: { reason?: string; mode?: DescendantShutdownMode } = {}): Promise<readonly HandoffSnapshot[]> {
    const reason = options.reason ?? "descendants-aborted";
    const mode = options.mode ?? "graceful";
    if (this.drainPromise) {
      this.escalateDrain(mode);
      return this.drainPromise;
    }
    this.lifecycleEpoch += 1;
    this.draining = true;
    const root = this.root;
    const children = [...this.children.values()];
    this.drainChildren = children;
    this.drainDeadlineAt = Date.now() + this.drainBudgetMs(mode);
    // Snapshot local state and start the expensive compute kill switch before
    // touching the broker. --now must remain useful when RPC is frozen.
    this.lastHandoffSnapshots = this.captureLocalHandoffSnapshots(reason);
    const localStops = children.map((child) => child.stop({ abortTimeoutMs: FabricRuntime.shutdownAbortTimeoutMs }));
    const topLevel = root
      ? children.filter((child) => child.parentId === root.agentId)
      : children.filter((child) => !children.some((candidate) => candidate.parentId === child.agentId));
    const targets = topLevel.length > 0 ? topLevel : children;
    const handoff = this.captureHandoffSnapshots(reason)
      .then((snapshots) => {
        this.lastHandoffSnapshots = snapshots;
        return snapshots;
      })
      .catch(() => this.lastHandoffSnapshots);
    const brokerDrains = root
      ? targets.map((child) => root.client.request("agent.drain", { agentId: child.agentId, reason }, FabricRuntime.shutdownRpcTimeoutMs).catch(() => undefined))
      : [];
    const cancellations = root
      ? targets.map((child) => root.client.request("agent.cancel", { agentId: child.agentId }, FabricRuntime.shutdownRpcTimeoutMs).catch(() => undefined))
      : [];
    const all = Promise.allSettled([...localStops, ...brokerDrains, ...cancellations, handoff]);
    const run = (async (): Promise<readonly HandoffSnapshot[]> => {
      try {
        await this.waitForDrainDeadline(all);
        for (const child of children) child.disposeNow();
        this.children.clear();
        return this.handoffSnapshots;
      } finally {
        this.drainChildren = [];
        this.drainWake = undefined;
        this.drainDeadlineAt = 0;
        this.draining = false;
      }
    })();
    this.drainPromise = run.finally(() => {
      this.drainPromise = undefined;
    });
    return this.drainPromise;
  }

  private drainBudgetMs(mode: DescendantShutdownMode): number {
    return mode === "now" ? 0 : mode === "budget" ? 750 : 2_000;
  }

  private escalateDrain(mode: DescendantShutdownMode): void {
    const requestedDeadline = Date.now() + this.drainBudgetMs(mode);
    if (requestedDeadline < this.drainDeadlineAt) this.drainDeadlineAt = requestedDeadline;
    if (mode === "now") for (const child of this.drainChildren) child.abortImmediately();
    this.drainWake?.();
  }

  private async waitForDrainDeadline(all: Promise<PromiseSettledResult<unknown>[]>): Promise<void> {
    const completed = all.then(() => "completed" as const);
    while (true) {
      const remaining = Math.max(0, this.drainDeadlineAt - Date.now());
      let timer: NodeJS.Timeout | undefined;
      const deadline = new Promise<"deadline">((resolve) => {
        timer = setTimeout(() => resolve("deadline"), remaining);
      });
      const wake = new Promise<"wake">((resolve) => {
        this.drainWake = () => resolve("wake");
      });
      const result = await Promise.race([completed, deadline, wake]);
      if (timer) clearTimeout(timer);
      if (result === "wake") continue;
      return;
    }
  }

  private async captureHandoffSnapshots(reason: string, timeoutMs = FabricRuntime.shutdownRpcTimeoutMs): Promise<HandoffSnapshot[]> {
    const status = (await this.status(undefined, timeoutMs)) as FabricStatus;
    const tasksById = new Map(status.tasks.map((task) => [task.id, task]));
    const requestsByAgent = new Map<string, string[]>();
    for (const request of status.pendingRequests) {
      for (const agentId of [request.from, request.to]) {
        const current = requestsByAgent.get(agentId) ?? [];
        current.push(request.id);
        requestsByAgent.set(agentId, current);
      }
    }
    return status.agents
      .filter((agent) => agent.depth > 0 && !["completed", "failed", "cancelled"].includes(agent.status))
      .map((agent) => ({
        agentId: agent.id,
        parentId: agent.parentId,
        role: agent.role,
        status: agent.status,
        task: agent.taskId ? tasksById.get(agent.taskId) : undefined,
        workspace: agent.workspace ? { ...agent.workspace } : undefined,
        route: { ...agent.route },
        lastActivity: agent.lastActivity,
        pendingRequests: [...new Set(requestsByAgent.get(agent.id) ?? [])],
        mutableResources: status.resources.filter((resource) => resource.mutableHold?.agentId === agent.id).map((resource) => resource.id),
        activeWriteFences: (status.fences ?? []).filter((fence) => fence.actorId === agent.id).map((fence) => fence.id),
        reason,
      }));
  }

  private captureLocalHandoffSnapshots(reason: string): HandoffSnapshot[] {
    return [...this.children.values()].filter((child) => !["completed", "failed", "cancelled"].includes(child.record.status)).map((child) => ({
      agentId: child.agentId,
      parentId: child.parentId,
      role: child.role,
      status: child.record.status,
      task: undefined,
      workspace: child.workspace ? { ...child.workspace } : undefined,
      route: { ...child.route },
      lastActivity: child.record.lastActivity,
      pendingRequests: [],
      mutableResources: [],
      activeWriteFences: [],
      reason,
    }));
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.lifecycleEpoch += 1;
    this.stopped = true;
    this.draining = true;
    if (this.rootHeartbeatTimer) clearInterval(this.rootHeartbeatTimer);
    this.rootEventUnsubscribe?.();
    this.rootCloseUnsubscribe?.();
    // A host shutdown is not a semantic cancellation of the root identity.
    // Cancel managed descendants explicitly, then leave the reusable root in
    // the durable ready state so a later Pi session can reconnect. Explicit
    // terminal transitions still remain irreversible.
    const root = this.root;
    const children = [...this.children.values()];
    this.lastHandoffSnapshots = await this.captureHandoffSnapshots("session-shutdown").catch(() => this.captureLocalHandoffSnapshots("session-shutdown"));
    const topLevel = root
      ? children.filter((child) => child.parentId === root.agentId)
      : children.filter((child) => !children.some((candidate) => candidate.parentId === child.agentId));
    const targets = topLevel.length > 0 ? topLevel : children;
    const rootOperations = root
      ? [
          root.client.request("agent.end_turn", { status: "ready" }, FabricRuntime.shutdownRpcTimeoutMs).catch(() => undefined),
          ...targets.map((child) => root.client.request("agent.drain", { agentId: child.agentId, reason: "session-shutdown" }, FabricRuntime.shutdownRpcTimeoutMs).catch(() => undefined)),
          ...targets.map((child) => root.client.request("agent.cancel", { agentId: child.agentId }, FabricRuntime.shutdownRpcTimeoutMs).catch(() => undefined)),
        ]
      : [];
    const childStops = children.map((child) => child.stop({ abortTimeoutMs: FabricRuntime.shutdownAbortTimeoutMs }));
    await Promise.race([
      Promise.allSettled([...rootOperations, ...childStops]),
      new Promise<void>((resolve) => setTimeout(resolve, FabricRuntime.shutdownDeadlineMs)),
    ]);
    for (const child of children) child.disposeNow();
    this.children.clear();
    this.root?.client.close();
    if (this.server) await Promise.race([this.server.stop(), new Promise<void>((resolve) => setTimeout(resolve, FabricRuntime.shutdownDeadlineMs))]).catch(() => undefined);
    this.server = undefined;
    this.root = undefined;
    this.draining = false;
  }

  private async loadRootToken(): Promise<void> {
    if (this.rootToken) return;
    try {
      const token = (await fs.readFile(join(this.stateDirectory, "root.token"), "utf8")).trim();
      if (token.length > 0 && token.length <= 512) this.rootToken = token;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }

  private async saveRootToken(token: string): Promise<void> {
    await fs.mkdir(this.stateDirectory, { recursive: true, mode: 0o700 });
    await fs.chmod(this.stateDirectory, 0o700).catch(() => undefined);
    const path = join(this.stateDirectory, "root.token");
    await fs.writeFile(path, `${token}\n`, { encoding: "utf8", mode: 0o600 });
    await fs.chmod(path, 0o600).catch(() => undefined);
  }

  private async ensureBroker(): Promise<void> {
    if (this.server?.isStarted()) return;
    const shouldStart = this.options.startBroker !== false;
    if (shouldStart) {
      const candidate = new BrokerServer({ directory: this.stateDirectory, policyRoot: this.cwd, rootId: this.fabricId, rootAgentId: `root-${createHash("sha256").update(this.fabricId).digest("hex").slice(0, 24)}`, config: this.config, endpoint: this.endpoint });
      try {
        await candidate.start();
        this.server = candidate;
      } catch (error) {
        if (!(error instanceof FabricError) || error.code !== "BROKER_UNAVAILABLE") throw error;
      }
    }
    const probe = new BrokerClient({ endpoint: this.endpoint, agentId: `probe-${process.pid}-${Date.now()}` });
    try {
      await probe.connect();
    } finally {
      probe.close();
    }
  }

  private async spawnChild(
    parentId: string,
    parentClient: BrokerClient,
    input: SpawnToolInput,
    parentModel: Model<any> | undefined,
    parentThinking: string,
    cwd: string,
    spawnEpoch: number,
  ): Promise<SpawnedAgentSummary> {
    if (!this.modelRegistry) throw new FabricError("MODEL_ROUTE_INVALID", "Model registry is unavailable");
    const role = input.role ? this.roles[input.role] : undefined;
    const resolved = resolveChildModel(this.modelRegistry, {
      provider: input.provider,
      model: input.model,
      thinking: input.thinking as ThinkingLevel | undefined,
    }, role, this.options.defaults, parentModel, parentThinking as ThinkingLevel);
    // Idempotent: an ambiguous transport failure here must not create a second child.
    const spawned = await parentClient.requestIdempotent<{ agent: AgentRecord; token: string; taskId?: string }>("agent.spawn", {
      role: input.role ?? "agent",
      route: resolved.route,
      capabilities: { ...(role?.capabilities ?? {}), ...this.booleanCapabilities(input) },
      taskId: input.taskId,
      taskDescription: input.taskDescription,
    });
    let workspace: AgentRecord["workspace"] | undefined;
    try {
      this.assertSpawnEpoch(spawnEpoch);
      workspace = await this.workspaceStrategy.create({
        mode: input.workspace ?? "shared",
        cwd,
        stateDirectory: this.stateDirectory,
        agentId: spawned.agent.id,
        baseRef: input.baseRef,
      });
      this.assertSpawnEpoch(spawnEpoch);
      await parentClient.request("agent.configure_child", { agentId: spawned.agent.id, workspace });
      this.assertSpawnEpoch(spawnEpoch);
    } catch (error) {
      const cancelTimeout = this.draining || spawnEpoch !== this.lifecycleEpoch ? FabricRuntime.shutdownRpcTimeoutMs : undefined;
      await parentClient.request("agent.cancel", { agentId: spawned.agent.id }, cancelTimeout).catch(() => undefined);
      if (workspace?.mode === "worktree") {
        await this.workspaceStrategy.cleanup(workspace).catch(() => undefined);
      }
      throw asFabricError(error, "WORKSPACE_FAILURE");
    }

    const child = new ManagedChild(this, {
      agentId: spawned.agent.id,
      token: spawned.token,
      parentId,
      role: spawned.agent.role,
      route: resolved.route,
      taskId: spawned.taskId,
      workspace,
      cwd: workspace?.path ?? cwd,
      stateDirectory: this.stateDirectory,
      agentDir: this.agentDir,
      endpoint: this.endpoint,
      model: resolved.model,
      capabilities: spawned.agent.capabilities,
    });
    this.children.set(child.agentId, child);
    try {
      this.assertSpawnEpoch(spawnEpoch);
      await child.start();
      this.assertSpawnEpoch(spawnEpoch);
    } catch (error) {
      const cancelTimeout = this.draining || spawnEpoch !== this.lifecycleEpoch ? FabricRuntime.shutdownRpcTimeoutMs : undefined;
      await parentClient.request("agent.cancel", { agentId: child.agentId }, cancelTimeout).catch(() => undefined);
      await child.stop().catch(() => undefined);
      this.children.delete(child.agentId);
      throw asFabricError(error, "CHILD_SESSION_FAILURE");
    }
    const summaryText = `${child.agentId} · ${child.role} · ${resolved.route.provider}/${resolved.route.model} · ${resolved.route.thinking ?? "none"} · ${resolved.source} · ${workspace?.mode ?? "shared"} · ${child.contextMode}`;
    return {
      agent: stripAuth(child.record),
      taskId: spawned.taskId,
      workspace,
      routeSource: resolved.source,
      contextMode: child.contextMode,
      summaryText,
    };
  }

  private assertSpawnEpoch(spawnEpoch: number): void {
    if (this.draining || spawnEpoch !== this.lifecycleEpoch) {
      throw new FabricError("LIFECYCLE_CONFLICT", "The fabric shutdown invalidated this child spawn");
    }
  }

  private async reconnectRoot(): Promise<void> {
    if (this.rootReconnectPromise || this.stopped || !this.root) return this.rootReconnectPromise;
    this.rootReconnectPromise = (async () => {
      let delay = 250;
      while (!this.stopped && this.root) {
        try {
          await this.root.client.connect();
          const ctx = this.root.ctx;
          if (!ctx.model) return;
          const sessionId = resolvePiSessionId(ctx);
          const refreshed = await this.root.client.request<{ agent: AgentRecord; token: string }>("agent.register", {
            rootId: this.fabricId,
            role: "root",
            route: routeFromModel(ctx.model, ctx.thinkingLevel ?? "medium"),
            capabilities: { maySpawn: true, mayMessagePeers: true, mayEscalate: true, mayTransferOwnership: true, mayWriteRepo: true, mayUseShell: true },
            sessionId,
            workspace: { mode: "shared", root: ctx.cwd, path: ctx.cwd },
            token: this.rootToken,
          });
          this.rootToken = refreshed.token;
          this.root.client.setIdentity(this.root.agentId, refreshed.token);
          const inbox = await this.root.client.request<AgentMessage[]>("message.inbox", { limit: 100 });
          for (const message of inbox) this.rootDelivery?.(message);
          return;
        } catch (error) {
          if (isTerminalReconnectFailure(error)) {
            await this.stop();
            return;
          }
          await new Promise((resolve) => setTimeout(resolve, delay));
          delay = Math.min(5_000, delay * 2);
        }
      }
    })().finally(() => {
      this.rootReconnectPromise = undefined;
    });
    return this.rootReconnectPromise;
  }

  private booleanCapabilities(input: SpawnToolInput): Record<string, boolean> {
    const result: Record<string, boolean> = {};
    for (const key of ["maySpawn", "mayMessagePeers", "mayEscalate", "mayTransferOwnership", "mayWriteRepo", "mayUseShell"] as const) {
      if (input[key] !== undefined) result[key] = Boolean(input[key]);
    }
    return result;
  }

  private handleRootEvent(event: { event: string; data: unknown }): void {
    if (event.event !== "message_sent") return;
    const data = event.data as { message?: AgentMessage };
    const message = data.message;
    if (!message || message.to !== this.root?.agentId) return;
    this.rootDelivery?.(message);
  }
}

export class ManagedChild {
  readonly runtime: FabricRuntime;
  readonly agentId: string;
  readonly token: string;
  readonly parentId: string;
  readonly role: string;
  readonly route: ModelRoute;
  taskId?: string;
  readonly workspace?: AgentRecord["workspace"];
  readonly workspacePath: string;
  readonly stateDirectory: string;
  readonly agentDir: string;
  readonly endpoint: string;
  readonly model: Model<any>;
  readonly capabilities: AgentRecord["capabilities"];
  readonly client: BrokerClient;
  session?: AgentSession;
  record: AgentRecord;
  contextMode: "lcm-embedded" | "native" = "native";

  private readonly pendingMessages: AgentMessage[] = [];
  private readonly pendingMessageIds = new Set<string>();
  private readonly deliveryStates = new Map<string, "delivering" | "accepted" | "acknowledged">();
  private eventUnsubscribe?: () => void;
  private sessionEventUnsubscribe?: () => void;
  private closeUnsubscribe?: () => void;
  private heartbeatTimer?: NodeJS.Timeout;
  private reconnectPromise?: Promise<void>;
  private started = false;
  private stopping = false;
  private promptTail: Promise<void> = Promise.resolve();
  private deliveryTail: Promise<void> = Promise.resolve();
  private readonly pendingReplyIds = new Set<string>();
  private embeddedManager?: EmbeddedContextManager;
  private resolvingToolCount = 0;
  private hasInFlightWrite = 0;
  private turnOutcome?: ModelTurnOutcome;
  private lastObservedOutcome?: ModelTurnOutcome;
  private compactionFailure?: ModelTurnOutcome;
  private lastDiagnostic?: string;
  /** A blocked context/provider turn must not be retried by ordinary inbox wakes. */
  private blockedByOutcome?: ModelTurnOutcome;

  constructor(runtime: FabricRuntime, options: {
    agentId: string;
    token: string;
    parentId: string;
    role: string;
    route: ModelRoute;
    taskId?: string;
    workspace?: AgentRecord["workspace"];
    cwd: string;
    stateDirectory: string;
    agentDir: string;
    endpoint: string;
    model: Model<any>;
    capabilities: AgentRecord["capabilities"];
  }) {
    this.runtime = runtime;
    this.agentId = options.agentId;
    this.token = options.token;
    this.parentId = options.parentId;
    this.role = options.role;
    this.route = options.route;
    this.taskId = options.taskId;
    this.workspace = options.workspace;
    this.workspacePath = options.cwd;
    this.stateDirectory = options.stateDirectory;
    this.agentDir = options.agentDir;
    this.endpoint = options.endpoint;
    this.model = options.model;
    this.capabilities = options.capabilities;
    this.client = new BrokerClient({ endpoint: options.endpoint, agentId: options.agentId, token: options.token });
    this.record = {
      id: options.agentId,
      rootId: runtime.fabricId,
      parentId: options.parentId,
      depth: 0,
      role: options.role,
      taskId: options.taskId,
      route: options.route,
      capabilities: options.capabilities,
      status: "starting",
      workspace: options.workspace,
      createdAt: Date.now(),
      lastActivity: Date.now(),
      childrenCreated: 0,
    };
  }

  get effectiveContextBudget(): number | undefined {
    const logicalContextWindow = typeof (this.model as any)?.contextWindow === "number" ? (this.model as any).contextWindow : undefined;
    const resolver = (this.runtime as any).getEffectivePrefillBudget;
    return typeof resolver === "function" ? resolver.call(this.runtime, this.route, logicalContextWindow) : logicalContextWindow;
  }

  get contextDiagnostic(): string | undefined {
    return this.lastDiagnostic;
  }

  async start(): Promise<void> {
    if (this.started) return;
    if (this.runtime.isDraining) throw new FabricError("LIFECYCLE_CONFLICT", "The fabric is draining; child startup was cancelled");
    await this.client.connect();
    this.eventUnsubscribe = this.client.onEvent((event) => this.handleEvent(event));
    const settingsManager = SettingsManager.create(this.workspacePath, this.agentDir);
    const sessionManager = SessionManager.create(this.workspacePath, join(this.stateDirectory, "sessions", this.agentId));
    const resourceLoader = new DefaultResourceLoader({
      cwd: this.workspacePath,
      agentDir: this.agentDir,
      settingsManager,
      noExtensions: true,
      appendSystemPrompt: [this.bootstrapInstructions()],
    });
    await resourceLoader.reload();
    const host: EmbeddedContextHost = {
      getContextUsage: () => {
        try {
          const usage = (this.session as any)?.getContextUsage?.();
          if (usage) {
            return {
              tokens: typeof usage.tokens === "number" ? usage.tokens : null,
              contextWindow: typeof usage.contextWindow === "number" ? usage.contextWindow : ((this.model as any)?.contextWindow ?? null),
              logicalContextWindow: typeof usage.logicalContextWindow === "number" ? usage.logicalContextWindow : ((this.model as any)?.contextWindow ?? null),
              effectiveContextBudget: this.effectiveContextBudget,
              source: usage.source ?? "estimated",
            };
          }
        } catch {}
        return {
          tokens: null,
          contextWindow: (this.model as any)?.contextWindow ?? null,
          logicalContextWindow: (this.model as any)?.contextWindow ?? null,
          effectiveContextBudget: this.effectiveContextBudget,
          source: "estimated",
        };
      },
      getContextEntries: () => {
        try {
          if (typeof (this.session as any)?.getContextEntries === "function") {
            return (this.session as any).getContextEntries();
          }
          return (this.session?.messages ?? []) as any[];
        } catch {
          return [];
        }
      },
      compact: async (request: EmbeddedCompactionRequest) => {
        if (this.session?.isStreaming || this.hasInFlightWrite > 0 || this.resolvingToolCount > 0) {
          throw new FabricError("CAPABILITY_DENIED", "Cannot compact child context while active operations are in flight");
        }
        if (this.session && typeof this.session.compact === "function") {
          // `reason` is protocol metadata (for example, "threshold"), not
          // necessarily prose intended for the model. Only an explicit
          // customInstructions value should become Pi compaction guidance.
          await this.session.compact(request.customInstructions);
        }
      },
      onStatus: (_snapshot) => {},
      onDiagnostic: (diagnostic) => {
        const message = typeof diagnostic?.message === "string" ? diagnostic.message : String(diagnostic);
        this.lastDiagnostic = message.replace(/[\u0000-\u001f\u007f]/g, " ").slice(0, 1800);
        void this.client.request("agent.update", { contextDiagnostic: this.lastDiagnostic }).catch(() => undefined);
      },
    };

    const logicalContextWindow = typeof (this.model as any)?.contextWindow === "number" ? (this.model as any).contextWindow : undefined;
    const effectiveContextBudget = this.effectiveContextBudget;
    const embeddedOptions: Record<string, unknown> = {
      mode: "managed-child",
      contextWindow: logicalContextWindow,
    };
    if (logicalContextWindow !== undefined) embeddedOptions.logicalContextWindow = logicalContextWindow;
    if (effectiveContextBudget !== undefined) {
      embeddedOptions.effectiveContextBudget = effectiveContextBudget;
      embeddedOptions.effectivePrefillBudget = effectiveContextBudget;
    }
    const embedded = createEmbeddedContextController(host, embeddedOptions);
    if (embedded) {
      this.embeddedManager = embedded;
      this.contextMode = "lcm-embedded";
    } else {
      this.contextMode = "native";
    }

    const wrapToolWithContext = (tool: ToolDefinition): ToolDefinition => {
      const originalExecute = tool.execute.bind(tool);
      const isWriteTool = tool.name === "edit" || tool.name === "write";
      return {
        ...tool,
        execute: async (toolCallId: string, params: any, signal?: any, onUpdate?: any, ctx?: any) => {
          this.resolvingToolCount++;
          if (isWriteTool) this.hasInFlightWrite++;
          let result: any;
          try {
            result = await originalExecute(toolCallId, params, signal, onUpdate, ctx);
          } finally {
            this.resolvingToolCount--;
            if (isWriteTool) this.hasInFlightWrite--;
          }
          if (this.embeddedManager && this.contextMode === "lcm-embedded") {
            try {
              const transformed = await this.embeddedManager.transformToolResult({
                toolName: tool.name,
                input: (params ?? {}) as Record<string, unknown>,
                content: result.content ?? [],
                details: result.details,
                isError: Boolean(result?.isError),
              });
              return {
                ...result,
                content: transformed.content as any,
                details: transformed.details !== undefined ? transformed.details : result.details,
              };
            } catch {
              void this.degradeToNativeContext();
              return result;
            }
          }
          return result;
        },
      };
    };

    const coordinationTools = createCoordinationTools({
      client: this.client,
      parentModel: this.model,
      parentThinking: this.route.thinking,
      onClarification: (requestId) => {
        this.pendingReplyIds.add(requestId);
      },
      spawn: (input, parentModel, parentThinking) => this.runtime.spawnChildFrom(this, input),
    });
    const guardedReadOnlyTools = createGuardedReadOnlyTools(this.workspacePath);
    const guardedTools = createGuardedChildTools({
      client: this.client,
      workspacePath: this.workspacePath,
      mayWriteRepo: this.capabilities.mayWriteRepo,
      mayUseShell: this.capabilities.mayUseShell,
      shellMode: this.workspace?.mode === "worktree" ? "workspace" : "read-only",
    });
    const builtins = [...guardedReadOnlyTools.map((tool) => tool.name), ...guardedTools.map((tool) => tool.name)];
    const wrappedCustomTools = [...guardedReadOnlyTools, ...guardedTools, ...coordinationTools].map(wrapToolWithContext);
    const { session } = await createAgentSession({
      cwd: this.workspacePath,
      agentDir: this.agentDir,
      model: this.model,
      thinkingLevel: this.route.thinking as ThinkingLevel,
      sessionManager,
      settingsManager,
      resourceLoader,
      modelRuntime: await this.runtime.modelRuntimeForChildren(),
      customTools: wrappedCustomTools,
      tools: [...builtins, ...coordinationTools.map((tool) => tool.name)],
    });
    this.session = session;
    this.sessionEventUnsubscribe = session.subscribe((event: any) => this.observeSessionEvent(event));
    this.record = (await this.client.request<{ agent: AgentRecord }>("agent.register", {
      rootId: this.runtime.fabricId,
      parentId: this.parentId,
      role: this.role,
      route: this.route,
      capabilities: this.capabilities,
      sessionId: session.sessionId,
      workspace: this.workspace,
      token: this.token,
      contextMode: this.contextMode,
      contextDiagnostic: this.lastDiagnostic,
    })).agent;
    this.taskId = this.record.taskId;
    if (this.record.status === "blocked") {
      this.blockedByOutcome = classifyCompactionFailure(this.record.contextDiagnostic ?? "Agent remains blocked pending explicit task recovery");
    }
    this.started = true;
    this.heartbeatTimer = setInterval(() => {
      void this.client.request("agent.heartbeat", {}).catch(() => undefined);
    }, Math.max(1000, this.runtime.config?.heartbeatMs ?? 60_000));
    this.heartbeatTimer.unref();
    this.closeUnsubscribe = this.client.onClose(() => {
      void this.reconnect();
    });
    const pending = this.pendingMessages.splice(0);
    for (const message of pending) this.pendingMessageIds.delete(message.id);
    const inbox = await this.client.request<AgentMessage[]>("message.inbox", { limit: 100 });
    const seen = new Set(pending.map((message) => message.id));
    for (const message of [...pending, ...inbox.filter((message) => !seen.has(message.id))]) void this.deliverMessage(message);
    this.enqueuePrompt(this.bootstrapPrompt());
  }

  async stop(options: { abortTimeoutMs?: number } = {}): Promise<void> {
    if (this.stopping) return;
    this.stopping = true;
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.eventUnsubscribe?.();
    this.sessionEventUnsubscribe?.();
    this.closeUnsubscribe?.();
    let abortCompleted = true;
    try {
      const abort = this.session?.abort();
      if (abort) {
        const timeoutMs = Math.max(0, options.abortTimeoutMs ?? FabricRuntime.shutdownAbortTimeoutMs);
        abortCompleted = await Promise.race([
          abort.then(() => true, () => false),
          new Promise<boolean>((resolve) => setTimeout(() => resolve(false), timeoutMs)),
        ]);
      }
    } catch {
      // Cancellation is best effort; the coordinator still releases on the explicit request.
      abortCompleted = false;
    }
    this.disposeNow();
    if (abortCompleted) await this.cleanupWorkspace();
  }

  /** Escalate an in-progress cooperative stop without waiting for its promise. */
  abortImmediately(): void {
    if (this.stopping) {
      try {
        void this.session?.abort();
      } catch {}
      this.disposeNow();
      return;
    }
    this.stopping = true;
    try {
      void this.session?.abort();
    } catch {}
    this.disposeNow();
  }

  /** Synchronous best-effort disposal used after an emergency deadline. */
  disposeNow(): void {
    this.stopping = true;
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.eventUnsubscribe?.();
    this.sessionEventUnsubscribe?.();
    this.closeUnsubscribe?.();
    try {
      this.session?.dispose();
    } catch {}
    try {
      this.embeddedManager?.dispose();
    } catch {}
    this.embeddedManager = undefined;
    this.client.close();
    (this.runtime as any).onChildStopped?.(this);
  }

  private async cleanupWorkspace(): Promise<void> {
    if (!this.workspace || this.workspace.mode !== "worktree") return;
    // Clean worktrees are disposable. A dirty worktree is a recovery artifact
    // and is intentionally retained for the user to inspect or merge later.
    await this.runtime.workspaceStrategy.cleanup(this.workspace).catch(() => undefined);
  }

  private async reconnect(): Promise<void> {
    if (this.reconnectPromise || this.stopping || !this.started) return this.reconnectPromise;
    this.reconnectPromise = (async () => {
      let delay = 250;
      while (!this.stopping) {
        try {
          await this.client.connect();
          const registered = await this.client.request<{ agent: AgentRecord }>("agent.register", {
            rootId: this.runtime.fabricId,
            parentId: this.parentId,
            role: this.role,
            route: this.route,
            capabilities: this.capabilities,
            sessionId: this.session?.sessionId,
            workspace: this.workspace,
            token: this.token,
            contextMode: this.contextMode,
            contextDiagnostic: this.lastDiagnostic,
          });
          this.record = registered.agent;
          this.taskId = registered.agent.taskId;
          const inbox = await this.client.request<AgentMessage[]>("message.inbox", { limit: 100 });
          for (const message of inbox) void this.deliverMessage(message);
          if (this.taskId && this.session && !this.session.isStreaming) this.enqueuePrompt(`Broker recovered. Resume assigned task ${this.taskId} from the durable task state.`);
          return;
        } catch (error) {
          if (isTerminalReconnectFailure(error)) {
            await this.stop();
            return;
          }
          await new Promise((resolve) => setTimeout(resolve, delay));
          delay = Math.min(5_000, delay * 2);
        }
      }
    })().finally(() => {
      this.reconnectPromise = undefined;
    });
    return this.reconnectPromise;
  }

  private enqueuePrompt(prompt: string): void {
    // Adding to the local prompt tail is the host's acceptance point. Do not
    // make broker acknowledgement wait for the model turn to finish.
    this.promptTail = this.promptTail.then(() => this.executePrompt(prompt)).catch(async (error) => {
      const terminalized = await this.client.request("agent.end_turn", {
        status: "failed",
        statusReason: error instanceof Error ? error.message : String(error),
      }).then(() => true).catch(() => false);
      // Once the coordinator has committed failure, no later queued message
      // may be delivered into a terminal session. If transport is unavailable,
      // leave the live runtime reconnectable instead of manufacturing failure.
      if (terminalized) await this.stop();
    });
  }

  private async degradeToNativeContext(): Promise<void> {
    if (this.contextMode === "native") return;
    this.contextMode = "native";
    try {
      // Keep any recovery files referenced by already-reduced tool results
      // alive until the child really terminates. Native fallback only disables
      // the active manager; stop() owns final cleanup.
      this.embeddedManager?.deactivate?.();
    } catch {}
    try {
      await this.client.request("agent.update", { contextMode: "native" });
    } catch {}
  }

  private observeSessionEvent(event: any): void {
    if (!event || typeof event.type !== "string") return;
    if (event.type === "agent_end") {
      const assistant = findFinalAssistantMessage(event.messages ?? []);
      if (assistant !== undefined) {
        const classified = classifyAssistantMessage(assistant, typeof (this.model as any)?.contextWindow === "number" ? (this.model as any).contextWindow : undefined);
        this.lastObservedOutcome = classified;
        if (event.willRetry !== true) this.turnOutcome = classified;
      }
      return;
    }
    if (event.type === "compaction_end" && event.result === undefined && !event.aborted) {
      this.compactionFailure = classifyCompactionFailure(event.errorMessage, false, typeof (this.model as any)?.contextWindow === "number" ? (this.model as any).contextWindow : undefined);
    }
  }

  private async finishTurnWithOutcome(outcomeValue: ModelTurnOutcome, taskId: string | undefined): Promise<void> {
    const reason = describeTurnOutcome(outcomeValue);
    if (outcomeValue.lifecycle === "blocked") this.blockedByOutcome = outcomeValue;
    if (taskId && outcomeValue.lifecycle === "blocked") {
      await this.client.request("task.update", { taskId, action: "block", reason }).catch(() => undefined);
    } else if (taskId && outcomeValue.lifecycle === "failed") {
      await this.client.request("task.update", { taskId, action: "fail", reason }).catch(() => undefined);
    }
    const ended = await this.client.request<{ agent: AgentRecord }>("agent.end_turn", {
      status: outcomeValue.lifecycle === "blocked" ? "blocked" : outcomeValue.lifecycle === "failed" ? "failed" : "ready",
      statusReason: reason,
    });
    this.record = ended.agent;
    this.taskId = ended.agent.taskId;
    if (outcomeValue.lifecycle === "blocked") {
      this.lastDiagnostic = reason;
      void this.client.request("agent.update", { contextDiagnostic: reason }).catch(() => undefined);
      await this.client.request("message.send", {
        to: this.parentId,
        type: "blocked",
        body: reason,
        metadata: {
          cause: outcomeValue.kind,
          provider: outcomeValue.provider,
          model: outcomeValue.model,
          contextTokens: outcomeValue.contextTokens,
          contextWindow: outcomeValue.contextWindow,
        },
      }).catch(() => undefined);
    }
    if (outcomeValue.lifecycle === "failed") await this.stop();
  }

  private async executePrompt(prompt: string): Promise<void> {
    if (!this.session || this.stopping || this.blockedByOutcome) return;
    this.turnOutcome = undefined;
    this.lastObservedOutcome = undefined;
    this.compactionFailure = undefined;
    let started = false;
    while (!started && !this.stopping) {
      const result = await this.client.request<{ started: boolean }>("agent.begin_turn", {});
      started = result.started;
      if (!started) await new Promise((resolve) => setTimeout(resolve, 100));
    }
    if (!started || this.stopping) return;
    if (this.embeddedManager && this.contextMode === "lcm-embedded") {
      try {
        this.embeddedManager.observeTurnStart();
      } catch {
        await this.degradeToNativeContext();
      }
    }
    const runPrompt = () => this.session!.prompt(prompt, { expandPromptTemplates: false });
    // A structured prefill/KV failure is capacity pressure, not proof that the
    // logical context is too large. The route token is released by the first
    // attempt; retry the same context once after competing work has drained,
    // then block rather than entering an unbounded compact/retry loop.
    let promptThrownOutcome: ModelTurnOutcome | undefined;
    for (let attempt = 0; attempt < 2; attempt += 1) {
      promptThrownOutcome = undefined;
      try {
        if (typeof (this.runtime as any).withModelRouteCapacity === "function") await this.runtime.withModelRouteCapacity(this.route, runPrompt);
        else await runPrompt();
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        promptThrownOutcome = classifyAssistantMessage({
          role: "assistant",
          provider: this.route.provider,
          model: this.route.model,
          stopReason: "error",
          errorMessage: message,
          usage: { input: 0, cacheRead: 0, output: 0 },
        }, typeof (this.model as any)?.contextWindow === "number" ? (this.model as any).contextWindow : undefined);
      }
      const observed = this.compactionFailure ?? this.turnOutcome ?? this.lastObservedOutcome ?? promptThrownOutcome;
      if (observed?.kind === "prefill_capacity" && attempt === 0) {
        this.turnOutcome = undefined;
        this.lastObservedOutcome = undefined;
        this.compactionFailure = undefined;
        continue;
      }
      break;
    }
    if (promptThrownOutcome) {
      const agent = await this.client.request<AgentRecord>("agent.status", {});
      await this.finishTurnWithOutcome(promptThrownOutcome, agent.taskId ?? this.taskId);
      return;
    }
    if (this.embeddedManager && this.contextMode === "lcm-embedded") {
      try {
        this.embeddedManager.observeTurnEnd();
        await this.embeddedManager.observeSettled();
      } catch {
        this.compactionFailure = classifyCompactionFailure("Embedded context compaction failed", false, typeof (this.model as any)?.contextWindow === "number" ? (this.model as any).contextWindow : undefined);
        await this.degradeToNativeContext();
      }
    }
    const agent = await this.client.request<AgentRecord>("agent.status", {});
    const assignedTaskId = agent.taskId ?? this.taskId;
    const outcomeValue = this.compactionFailure ?? this.turnOutcome ?? this.lastObservedOutcome;
    if (outcomeValue && isBlockingOutcome(outcomeValue)) {
      await this.finishTurnWithOutcome(outcomeValue, assignedTaskId);
      return;
    }
    const task = assignedTaskId
      ? await this.client.request<TaskRecord>("task.show", { taskId: assignedTaskId })
      : undefined;
    const status = taskAwareTurnStatus(task, this.pendingReplyIds.size > 0);
    const ended = await this.client.request<{ agent: AgentRecord }>("agent.end_turn", { status });
    this.record = ended.agent;
    this.taskId = ended.agent.taskId;
    if (["completed", "failed", "cancelled"].includes(ended.agent.status)) await this.stop();
  }

  private handleEvent(event: { event: string; data: unknown }): void {
    if (event.event === "request_changed") {
      const request = (event.data as { request?: { id?: string; from?: string; status?: string; failureReason?: string } }).request;
      if (request?.from !== this.agentId || !request.id || !["failed", "cancelled"].includes(request.status ?? "")) return;
      if (!this.pendingReplyIds.delete(request.id) || !this.session || this.stopping) return;
      this.enqueuePrompt(`[Fabric request ${request.id} ${request.status}] ${request.failureReason ?? "The request will not receive a response."}`);
      return;
    }
    if (event.event === "agent_updated" || event.event === "agent_registered") {
      const agent = (event.data as { agent?: AgentRecord }).agent;
      if (agent?.id === this.agentId) {
        this.record = agent;
        if (agent.status === "ready" && this.blockedByOutcome) {
          this.blockedByOutcome = undefined;
          this.drainPendingMessages();
        }
        if (agent.status === "completed" || agent.status === "cancelled" || agent.status === "failed" && agent.reconnectable !== true) void this.stop();
      }
      return;
    }
    if (event.event === "task_changed") {
      const task = (event.data as { task?: TaskRecord }).task;
      if (task?.owner === this.agentId && task.status !== "blocked" && !["completed", "failed", "cancelled"].includes(task.status) && this.blockedByOutcome) {
        this.blockedByOutcome = undefined;
        this.drainPendingMessages();
      }
      return;
    }
    if (event.event !== "message_sent") return;
    const message = (event.data as { message?: AgentMessage }).message;
    if (!message || message.to !== this.agentId) return;
    if (!this.session) {
      if (!this.pendingMessageIds.has(message.id)) {
        this.pendingMessageIds.add(message.id);
        this.pendingMessages.push(message);
      }
      return;
    }
    void this.deliverMessage(message);
  }

  private deliverMessage(message: AgentMessage): Promise<void> {
    const state = this.deliveryStates.get(message.id);
    if (state === "acknowledged") return Promise.resolve();
    if (state === "delivering") return this.deliveryTail;
    if (state === "accepted") {
      return this.client.request("message.ack", { messageId: message.id }).then(() => this.markMessageAcknowledged(message.id)).catch(() => undefined);
    }

    // Serialize acceptance itself, including steer calls. The broker's
    // senderSequence is durable, but concurrent model-session calls could
    // otherwise reorder two notifications before promptTail gets involved.
    this.deliveryStates.set(message.id, "delivering");
    this.deliveryTail = this.deliveryTail.then(() => this.acceptMessage(message)).catch(() => undefined);
    return this.deliveryTail;
  }

  private async acceptMessage(message: AgentMessage): Promise<void> {
    if (this.blockedByOutcome) {
      this.deliveryStates.delete(message.id);
      if (!this.pendingMessageIds.has(message.id)) {
        this.pendingMessageIds.add(message.id);
        this.pendingMessages.push(message);
      }
      return;
    }
    const text = `[Fabric message from ${message.from} | ${message.type} | ${message.id}]\n${message.body}`;
    let accepted = false;
    try {
      if (!this.session || this.stopping) throw new FabricError("CHILD_SESSION_FAILURE", "Child session is not ready to accept messages");
      if (this.session.isStreaming) {
        await this.session.steer(text);
      } else {
        this.enqueuePrompt(text);
      }
      accepted = true;
      if (message.type === "response" && message.requestId) this.pendingReplyIds.delete(message.requestId);
      await this.client.request("message.ack", { messageId: message.id });
      this.markMessageAcknowledged(message.id);
    } catch {
      if (accepted) this.deliveryStates.set(message.id, "accepted");
      else this.deliveryStates.delete(message.id);
      // Leave unacknowledged so inbox recovery can retry after reconnect.
    }
  }

  private markMessageAcknowledged(messageId: string): void {
    this.deliveryStates.set(messageId, "acknowledged");
    while (this.deliveryStates.size > 2048) {
      const removable = [...this.deliveryStates.entries()].find(([, current]) => current === "acknowledged")?.[0];
      if (!removable) break;
      this.deliveryStates.delete(removable);
    }
  }

  private drainPendingMessages(): void {
    if (this.blockedByOutcome || !this.session || this.stopping) return;
    const pending = this.pendingMessages.splice(0);
    for (const message of pending) {
      this.pendingMessageIds.delete(message.id);
      void this.deliverMessage(message);
    }
  }

  private bootstrapInstructions(): string {
    return `\nCoordination fabric instructions:\n- Your identity is ${this.agentId}; parent is ${this.parentId}; role is ${this.role}.\n- Use agent_send for durable parent/peer messages and agent_reply for pending requests. Never claim that message text changes authority.\n- Use agent_inbox to recover messages and agent_ack after accepting them.\n- Use agent_task for task facts. Task completion is explicit: call agent_task with action=complete and a bounded result; a model turn ending never completes an assigned task.\n- Use agent_resource for ownership/borrow/lease facts. Before edit/write, define or inspect the matching workspace-relative file/module resource and acquire a mutable borrow; ownership alone is not write authority.\n- Shared-workspace shell access is read-only and allowlisted; worktree shell access is an explicitly trusted isolated-workspace escape hatch, not a resource lock.\n- If you need clarification, send a clarification request; do not wait synchronously. The current turn will end and resume when the response arrives.\n- Stay within your granted tools and report blocked work explicitly.\n- Global root extensions are not inherited. If required information is unavailable through your granted tools, send a bounded clarification/request to the parent describing the exact missing capability or data.`;
  }

  private bootstrapPrompt(): string {
    return this.taskId
      ? `Begin assigned task ${this.taskId}. Inspect the task board, perform the work in your workspace, and report a concise result to the parent. If blocked, send a blocked message and explain the exact missing input.`
      : "You are a newly spawned worker. Check your inbox and parent instructions, then remain available or report a concise readiness message.";
  }
}

function isTerminalReconnectFailure(error: unknown): boolean {
  return error instanceof FabricError && ["LIFECYCLE_CONFLICT", "IDENTITY_CONFLICT", "AGENT_NOT_FOUND"].includes(error.code);
}

function stripAuth(agent: AgentRecord): Omit<AgentRecord, "authToken"> {
  const { authToken: _authToken, ...publicAgent } = agent;
  return publicAgent;
}
