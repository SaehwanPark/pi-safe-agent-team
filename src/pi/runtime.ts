import { createHash, randomUUID } from "node:crypto";
import { realpathSync } from "node:fs";
import { promises as fs } from "node:fs";
import { homedir } from "node:os";
import { join, resolve, sep } from "node:path";
import { createAgentSession, DefaultResourceLoader, getAgentDir, ModelRuntime, SessionManager, SettingsManager, type AgentSession, type ExtensionAPI, type ExtensionContext, type ToolDefinition } from "@earendil-works/pi-coding-agent";
import type { Model } from "@earendil-works/pi-ai";
import type { ThinkingLevel } from "@earendil-works/pi-agent-core";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { FabricError, asFabricError } from "../core/errors.ts";
import { Coordinator } from "../core/coordinator.ts";
import { DEFAULT_FABRIC_CONFIG, type AgentStatus, type AgentSummary, type FabricConfig, type AgentMessage, type AgentRecord, type FabricSnapshot, type FabricStatus, type ModelRoute, type TaskRecord } from "../core/types.ts";
import { BrokerClient } from "../broker/client.ts";
import { BrokerServer, defaultEndpoint, detectCaseInsensitivePaths } from "../broker/server.ts";
import { GitWorkspaceStrategy, type WorkspaceStrategy } from "../workspace.ts";
import { resolveChildModel, routeFromModel } from "./model-routing.ts";
import { createCoordinationTools, type SpawnToolInput } from "./tools.ts";
import { createGuardedChildTools, createGuardedReadOnlyTools, evaluateRootShellGuard, evaluateRootWriteGuard, releaseRootWriteFence, type RootWriteGuardOutcome } from "./guards.ts";
import { createEmbeddedContextController, type EmbeddedCompactionRequest, type EmbeddedContextHost, type EmbeddedContextManager, type FabricSnapshotRequest, type FabricStateSnapshotV1 } from "./interop.ts";
import { effectivePrefillBudget } from "../core/coordinator-wire.ts";
import { ModelRouteCapacityArbiter } from "./model-capacity.ts";
import { classifyAssistantMessage, classifyCompactionFailure, describeTurnOutcome, findFinalAssistantMessage, isAbortLikeMessage, isBlockingOutcome, type ModelTurnOutcome } from "./turn-outcome.ts";
import { loadFabricConfig } from "./config.ts";

export interface RoleConfig {
  model?: string;
  provider?: string;
  thinking?: ThinkingLevel;
  capacityGroup?: string;
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

/**
 * A durable child-message receipt is a dedicated, machine-readable line. It
 * intentionally does not use substring matching: quoted IDs in ordinary
 * model text cannot satisfy this exact receipt grammar.
 */
function messageRevision(message: Pick<AgentMessage, "revision">): number {
  return Number.isInteger(message.revision) && (message.revision as number) > 0 ? message.revision as number : 1;
}

interface ChildMessageReceipt {
  id: string;
  revision: number;
}

function childMessageReceipt(message: Pick<AgentMessage, "id" | "revision">): string {
  return `<safe-agents-message id="${message.id}" revision="${messageRevision(message)}"/>`;
}

function childMessagePrompt(message: AgentMessage): string {
  return `${childMessageReceipt(message)}\n[Fabric message from ${message.from} | ${message.type} | ${message.id} revision ${messageRevision(message)}]\n${message.body}`;
}

function extractChildMessageReceipt(content: unknown): ChildMessageReceipt | undefined {
  const text = typeof content === "string"
    ? content
    : Array.isArray(content)
      ? content.filter((part: any) => part?.type === "text").map((part: any) => part.text).join("\n")
      : "";
  if (typeof text !== "string") return undefined;
  const firstLine = text.split(/\r?\n/, 1)[0]?.trim();
  const structured = /^<safe-agents-message id="([^"]+)" revision="([0-9]+)"\/>$/.exec(firstLine ?? "");
  if (structured) return { id: structured[1], revision: Number(structured[2]) };
  const legacyStructured = /^<safe-agents-message id="([^"]+)"\/>$/.exec(firstLine ?? "");
  if (legacyStructured) return { id: legacyStructured[1], revision: 1 };
  // Read transcripts created before the structured receipt was introduced,
  // but still require the complete canonical header rather than includes().
  const legacy = /^\[Fabric message from [^|\]]+ \| [^|\]]+ \| ([^\]]+)\]$/.exec(firstLine ?? "");
  if (legacy) return { id: legacy[1], revision: 1 };
  // Very early test/host adapters only retained the compact receipt line.
  const compact = /^\[Fabric message ([^\]]+)\]$/.exec(firstLine ?? "");
  return compact ? { id: compact[1], revision: 1 } : undefined;
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
  readonly config: Partial<FabricConfig>;
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
  private cleanupPromise?: Promise<void>;
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
  private rootCompactionReleases: Array<(() => void) | undefined> = [];
  private rootCompactionBrokerReservations: boolean[] = [];
  private rootCompactionOperationIds: Array<string | undefined> = [];
  private rootCompactionSequence = 0;
  private readonly operationNonce = randomUUID();
  private rootCompactionEpoch = 0;
  private rootModelCapacityRelease?: () => void;
  private rootModelCapacityController?: AbortController;
  private rootContextHealth: "healthy" | "degraded" = "healthy";
  private rootContextDiagnostic?: string;
  /** Broker wake for a root turn admitted from the durable capacity queue. */
  private rootTurnAdmissionWake?: () => void;
  private rootTurnAdmissionNotified = false;
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
    const loadedConfig = loadFabricConfig({ cwd: this.cwd, agentDir: this.agentDir });
    if (loadedConfig.errors.length > 0) {
      throw new FabricError("INVALID_ARGUMENT", `Safe-agents configuration could not be loaded: ${loadedConfig.errors.join("; ")}`);
    }
    this.config = { ...loadedConfig.config, ...(options.config ?? {}) };
    this.caseInsensitivePaths = options.config?.caseInsensitivePaths;
    this.workspaceStrategy = options.workspaceStrategy ?? new GitWorkspaceStrategy();
    this.roles = options.roles ?? {};
    this.sessionId = normalizeSessionId(options.sessionId);
    const identityKey = this.identityKey(this.sessionId ?? "unattached");
    this._fabricId = options.fabricId ?? `fabric-${hashIdentity(identityKey)}`;
    this._stateDirectory = options.stateDirectory ?? join(this.agentDir, "safe-agents", hashIdentity(identityKey));
    this._endpoint = options.endpoint ?? defaultEndpoint(this._stateDirectory);
    this.modelCapacity = new ModelRouteCapacityArbiter({ ...DEFAULT_FABRIC_CONFIG, ...this.config });
  }

  /** Begin observing a root manual/automatic compaction before Pi mutates context. */
  async beginRootCompaction(signal?: AbortSignal): Promise<void> {
    const epoch = this.rootCompactionEpoch;
    const reservationIndex = this.rootCompactionBrokerReservations.length;
    this.rootCompactionInFlight += 1;
    this.rootCompactionBrokerReservations.push(false);
    const operationBase = `root-compaction-${this.operationNonce}-${++this.rootCompactionSequence}`;
    this.rootCompactionOperationIds.push(undefined);
    const releaseIndex = this.rootCompactionReleases.length;
    this.rootCompactionReleases.push(undefined);
    const route = this.root?.ctx.model ? routeFromModel(this.root.ctx.model, this.root.ctx.thinkingLevel ?? "medium") : undefined;
    if (!route) {
      this.rootCompactionReleases.pop();
      this.rootCompactionBrokerReservations.pop();
      this.rootCompactionOperationIds.pop();
      this.rootCompactionInFlight = Math.max(0, this.rootCompactionInFlight - 1);
      return;
    }
    let brokerReservation = false;
    try {
      // The broker is authoritative across processes. Acquire it before the
      // process-local gate so every model operation uses the same lock order
      // (broker -> local) and a local waiter cannot deadlock a remote holder.
      if (this.root) {
        try {
          while (true) {
            if (this.stopped) throw new FabricError("LIFECYCLE_CONFLICT", "Root compaction admission stopped with the fabric");
            if (signal?.aborted) throw new FabricError("BROKER_UNAVAILABLE", "Root compaction admission was aborted");
            const result = await this.requestLifecycleOnClient<{ started?: boolean; queued?: boolean }>(this.root.client, "agent.begin_turn", { purpose: "compaction" }, `${operationBase}:begin`, FabricRuntime.shutdownRpcTimeoutMs, signal);
            if (result?.started === true) {
              brokerReservation = true;
              break;
            }
            if (result?.queued === true) await this.waitForRootTurnAdmission(signal);
            else await new Promise((resolve) => setTimeout(resolve, 100));
          }
        } catch (error) {
          // Automatic compaction runs inside an already-admitted root turn, so
          // the coordinator correctly reports a lifecycle conflict. Any other
          // error means admission is unknown and compaction must fail closed.
          if (!(error instanceof FabricError) || error.code !== "LIFECYCLE_CONFLICT" || !/already running/i.test(error.message)) throw error;
        }
      }

      // Automatic compaction can run inside an already-admitted root model
      // turn. The root permit is intentionally re-entrant for that nested
      // lifecycle path; acquiring a second permit on a single-capacity local
      // backend would make the root wait on itself until compaction aborts.
      const release = this.rootModelCapacityRelease
        ? undefined
        : await this.modelCapacity.acquire(route, signal);
      if (epoch !== this.rootCompactionEpoch || this.stopped) {
        release?.();
        if (brokerReservation && this.root) await this.requestLifecycleOnClient(this.root.client, "agent.end_turn", { status: "ready" }, `${operationBase}:end`, FabricRuntime.shutdownRpcTimeoutMs).catch(() => undefined);
        return;
      }
      this.rootCompactionReleases[releaseIndex] = release;
      this.rootCompactionBrokerReservations[reservationIndex] = brokerReservation;
      if (brokerReservation) this.rootCompactionOperationIds[reservationIndex] = operationBase;
    } catch (error) {
      if (brokerReservation && this.root) await this.requestLifecycleOnClient(this.root.client, "agent.end_turn", { status: "ready" }, `${operationBase}:end`, FabricRuntime.shutdownRpcTimeoutMs).catch(() => undefined);
      // Keep direct callers from leaking a compaction slot when admission or
      // the local gate is aborted before Pi can emit its failure hook.
      if (releaseIndex === this.rootCompactionReleases.length - 1) this.rootCompactionReleases.pop();
      if (reservationIndex === this.rootCompactionBrokerReservations.length - 1) this.rootCompactionBrokerReservations.pop();
      if (reservationIndex === this.rootCompactionOperationIds.length - 1) this.rootCompactionOperationIds.pop();
      if (this.rootCompactionInFlight > 0) this.rootCompactionInFlight -= 1;
      throw error;
    }
  }

  async endRootCompaction(): Promise<void> {
    if (this.rootCompactionInFlight > 0) this.rootCompactionInFlight -= 1;
    this.rootCompactionReleases.pop()?.();
    const brokerReservation = this.rootCompactionBrokerReservations.pop() ?? false;
    const operationBase = this.rootCompactionOperationIds.pop();
    if (brokerReservation && this.root) {
      await this.requestLifecycleOnClient(this.root.client, "agent.end_turn", { status: "ready" }, `${operationBase ?? `root-compaction-${this.operationNonce}-${this.rootCompactionSequence}`}:end`, FabricRuntime.shutdownRpcTimeoutMs).catch(() => undefined);
    }
  }

  resetRootCompactionState(): void {
    this.rootCompactionEpoch += 1;
    this.rootCompactionInFlight = 0;
    const reservations = this.rootCompactionBrokerReservations.splice(0);
    const operationIds = this.rootCompactionOperationIds.splice(0);
    for (const release of this.rootCompactionReleases.splice(0)) release?.();
    if (this.root) {
      for (let index = 0; index < reservations.length; index += 1) {
        if (reservations[index]) {
          const operationBase = operationIds[index] ?? `root-compaction-${this.operationNonce}-${this.rootCompactionSequence}`;
          void this.requestLifecycleOnClient(this.root.client, "agent.end_turn", { status: "ready" }, `${operationBase}:end`, FabricRuntime.shutdownRpcTimeoutMs).catch(() => undefined);
        }
      }
    }
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

  /** Hold the process-local route permit for one normal root provider run. */
  async beginRootModelTurnCapacity(signal?: AbortSignal): Promise<void> {
    if (!this.root?.ctx.model || this.rootModelCapacityRelease) return;
    const route = routeFromModel(this.root.ctx.model, this.root.ctx.thinkingLevel ?? "medium");
    const controller = new AbortController();
    this.rootModelCapacityController = controller;
    const onAbort = () => controller.abort();
    signal?.addEventListener("abort", onAbort, { once: true });
    try {
      this.rootModelCapacityRelease = await this.modelCapacity.acquire(route, controller.signal);
    } catch (error) {
      this.rootModelCapacityController = undefined;
      throw error;
    } finally {
      signal?.removeEventListener("abort", onAbort);
    }
  }

  async endRootModelTurnCapacity(): Promise<void> {
    this.rootModelCapacityController?.abort();
    this.rootModelCapacityController = undefined;
    this.rootModelCapacityRelease?.();
    this.rootModelCapacityRelease = undefined;
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
        await this.reconcileRecoveredTurn(this.root.client, refreshed.agent, false);
      }
      await this.cleanupAbandonedAgentArtifacts().catch(() => undefined);
      if (this.rootDelivery) {
        const inbox = await this.drainRootInbox();
        for (const message of inbox) this.rootDelivery(message);
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
    await this.reconcileRecoveredTurn(client, result.agent, false);
    this.root = { api, ctx, agentId, client };
    await this.cleanupAbandonedAgentArtifacts().catch(() => undefined);
    this.rootHeartbeatTimer = setInterval(() => {
      void client.request("agent.heartbeat", {}).catch(() => {
        // A live socket can survive broker-side liveness reclamation or an
        // event-loop pause. Re-register on lifecycle errors so a managed root
        // does not remain silently failed until the recovery grace expires.
        void this.reconnectRoot();
      });
    }, Math.max(1000, this.config?.heartbeatMs ?? 60_000));
    this.rootHeartbeatTimer.unref();
    this.rootEventUnsubscribe = client.onEvent((event) => this.handleRootEvent(event));
    this.rootCloseUnsubscribe = client.onClose(() => {
      void this.reconnectRoot();
    });
    if (this.rootDelivery) {
      const inbox = await this.drainRootInbox();
      for (const message of inbox) this.rootDelivery(message);
    }
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

  private requestLifecycleOnClient<T = unknown>(
    client: BrokerClient,
    operation: string,
    args: Record<string, unknown>,
    operationId: string,
    timeoutMs?: number,
    signal?: AbortSignal,
  ): Promise<T> {
    return client.requestIdempotent<T>(operation, args, operationId, timeoutMs, signal).catch((error) => {
      // A host may briefly talk to a pre-R5 broker that understands the
      // lifecycle operation but not durable operation IDs. Retry only that
      // explicit capability error without an operationId; other INVALID_ARGUMENT
      // failures remain deterministic and must not be replayed blindly.
      if (error instanceof FabricError && error.code === "INVALID_ARGUMENT" && /operationId.*supported/i.test(error.message)) {
        return client.request<T>(operation, args, timeoutMs, signal);
      }
      throw error;
    });
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
        recoveringAgents: 0,
        unresolvedChildTasks: 0,
        unownedUnresolvedTasks: 0,
        mutableHolds: 0,
        activeWriteFences: 0,
        activeWriteQuarantines: 0,
        activeShellBarriers: 0,
        pendingRootRequests: 0,
        pendingRootDeliveries: 0,
        pendingModelTurns: 0,
        rootCompactionInFlight: this.isRootCompactionInFlight,
        rootContextHealth: this.rootHealth,
        rootContextDiagnostic: this.rootHealthDiagnostic,
        activeTasks: [],
        mutableResources: [],
        quiescenceReasons: ["fabric_runtime_unattached_or_stopped"],
      };
    }

    try {
      // Quiescence only needs bounded aggregates. Keeping this query separate
      // from the diagnostic status projection prevents historical task output
      // from becoming an LCM transport failure.
      const raw = (await this.status(request.signal, undefined, "fabric.snapshot")) as FabricSnapshot | FabricStatus;
      const snapshot = "activeTasks" in raw ? raw : this.snapshotFromLegacyStatus(raw);
      if (snapshot.caseInsensitivePaths !== undefined) this.caseInsensitivePaths = snapshot.caseInsensitivePaths;
      const rootBusy = snapshot.rootStatus === undefined || snapshot.rootStatus === "starting" || snapshot.rootStatus === "running" || snapshot.rootStatus === "draining";
      const runningChildren = snapshot.runningChildren;
      const recoveringAgents = snapshot.recoveringAgents;
      const unresolvedChildTasks = snapshot.unresolvedChildTasks;
      const unownedUnresolvedTasks = snapshot.unownedUnresolvedTasks;
      const mutableHolds = snapshot.mutableHolds;
      const pendingRootRequests = snapshot.pendingRootRequests;
      const pendingRootDeliveries = this.pendingRootDeliveriesCount;
      const pendingModelTurns = snapshot.pendingModelTurns;
      const activeWriteFences = snapshot.activeWriteFences;
      const activeWriteQuarantines = snapshot.activeWriteQuarantines;
      const activeShellBarriers = snapshot.activeShellBarriers;

      const quiescenceReasons: string[] = [];
      if (this.draining) quiescenceReasons.push("fabric_draining");
      if (this.isRootCompactionInFlight) quiescenceReasons.push("root_compaction_in_flight");
      if (rootBusy) quiescenceReasons.push("root_agent_active_or_running");
      if (runningChildren > 0) quiescenceReasons.push("running_children_active");
      if (recoveringAgents > 0) quiescenceReasons.push("recovering_agents");
      if (unresolvedChildTasks > 0) quiescenceReasons.push("unresolved_child_tasks");
      if (unownedUnresolvedTasks > 0) quiescenceReasons.push("unowned_unresolved_tasks");
      if (mutableHolds > 0) quiescenceReasons.push("active_mutable_holds");
      if (activeWriteFences > 0) quiescenceReasons.push("active_write_fences");
      if (activeWriteQuarantines > 0) quiescenceReasons.push("active_write_quarantines");
      if (activeShellBarriers > 0) quiescenceReasons.push("active_shell_barriers");
      if (pendingRootRequests > 0) quiescenceReasons.push("pending_root_requests");
      if (pendingRootDeliveries > 0) quiescenceReasons.push("pending_root_deliveries");
      if (pendingModelTurns > 0) quiescenceReasons.push("model_turns_waiting_for_capacity");

      const quiescent = quiescenceReasons.length === 0;
      const sessionReplacementSafe = quiescent;

      const activeTasks = snapshot.activeTasks.slice(0, 50).map((task) => ({
        id: task.id,
        status: task.status,
        owner: task.owner,
        description: task.description,
      }));

      const mutableResources = snapshot.mutableResources.slice(0, 50).map((resource) => ({
        id: resource.id,
        path: resource.path,
        holder: resource.holder,
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
        recoveringAgents,
        unresolvedChildTasks,
        unownedUnresolvedTasks,
        mutableHolds,
        activeWriteFences,
        activeWriteQuarantines,
        activeShellBarriers,
        pendingRootRequests,
        pendingRootDeliveries,
        pendingModelTurns,
        rootCompactionInFlight: this.isRootCompactionInFlight,
        rootContextHealth: this.rootHealth,
        rootContextDiagnostic: this.rootHealthDiagnostic,
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
        recoveringAgents: 0,
        unresolvedChildTasks: 0,
        unownedUnresolvedTasks: 0,
        mutableHolds: 0,
        activeWriteFences: 0,
        activeWriteQuarantines: 0,
        activeShellBarriers: 0,
        pendingRootRequests: 0,
        pendingRootDeliveries: this.pendingRootDeliveriesCount,
        pendingModelTurns: 0,
        rootCompactionInFlight: this.isRootCompactionInFlight,
        rootContextHealth: this.rootHealth,
        rootContextDiagnostic: this.rootHealthDiagnostic,
        activeTasks: [],
        mutableResources: [],
        quiescenceReasons: ["broker_status_query_failed"],
      };
    }
  }

  private snapshotFromLegacyStatus(status: FabricStatus): FabricSnapshot {
    const rootAgent = status.agents.find((agent) => agent.id === this.root?.agentId) ?? status.agents.find((agent) => agent.depth === 0);
    const unresolvedStatuses = new Set(["pending", "ready", "active", "waiting", "blocked"]);
    const unresolved = status.tasks.filter((task) => unresolvedStatuses.has(task.status));
    return {
      rootId: status.rootId,
      rootAgentId: rootAgent?.id,
      rootStatus: rootAgent?.status,
      caseInsensitivePaths: status.config?.caseInsensitivePaths,
      runningChildren: status.agents.filter((agent) => agent.depth > 0 && ["starting", "running", "draining"].includes(agent.status)).length,
      recoveringAgents: status.agents.filter((agent) => agent.reconnectable === true).length,
      unresolvedChildTasks: unresolved.length,
      unownedUnresolvedTasks: unresolved.filter((task) => task.owner === undefined).length,
      mutableHolds: status.resources.filter((resource) => resource.mutableHold !== undefined).length,
      activeWriteFences: status.activeFences ?? 0,
      activeWriteQuarantines: status.activeWriteQuarantines ?? status.resources.filter((resource) => (resource.writeQuarantineUntil ?? 0) > Date.now()).length,
      activeShellBarriers: status.activeShellBarriers ?? status.shellBarriers?.length ?? 0,
      pendingRootRequests: rootAgent ? status.pendingRequests.filter((request) => request.status === "pending" && (request.to === rootAgent.id || request.from === rootAgent.id)).length : 0,
      pendingModelTurns: status.pendingModelTurns?.length ?? 0,
      activeTasks: unresolved.slice(0, 50).map((task) => ({ id: task.id, status: task.status, owner: task.owner, description: task.description.slice(0, 1024) })),
      mutableResources: status.resources.filter((resource) => resource.mutableHold !== undefined).slice(0, 50).map((resource) => ({ id: resource.id, path: resource.path, holder: resource.mutableHold?.agentId })),
    };
  }

  async status(signal?: AbortSignal, timeoutMs?: number, operation = "fabric.status"): Promise<unknown> {
    try {
      return await this.request(operation, {}, timeoutMs, signal);
    } catch (error) {
      // Keep hosts compatible with a pre-R8 broker while new brokers use the
      // bounded server-side snapshot operation. Do not mask frame or transport
      // failures as a legacy status query.
      if (operation === "fabric.snapshot" && error instanceof FabricError && error.code === "INVALID_ARGUMENT" && /unknown coordinator operation/i.test(error.message)) {
        return this.request("fabric.status", {}, timeoutMs, signal);
      }
      throw error;
    }
  }

  /** Wait for the broker's FIFO admission wake instead of polling capacity. */
  async waitForRootTurnAdmission(signal?: AbortSignal): Promise<void> {
    if (this.rootTurnAdmissionNotified) {
      this.rootTurnAdmissionNotified = false;
      return;
    }
    if (signal?.aborted) throw new FabricError("BROKER_UNAVAILABLE", "Root turn admission was aborted");
    await new Promise<void>((resolve, reject) => {
      let onAbort: (() => void) | undefined;
      const finish = (error?: unknown): void => {
        this.rootTurnAdmissionNotified = false;
        if (signal && onAbort) signal.removeEventListener("abort", onAbort);
        if (this.rootTurnAdmissionWake === wake) this.rootTurnAdmissionWake = undefined;
        if (error) reject(error);
        else resolve();
      };
      const wake = () => finish();
      this.rootTurnAdmissionWake = wake;
      if (signal) {
        onAbort = () => finish(new FabricError("BROKER_UNAVAILABLE", "Root turn admission was aborted"));
        signal.addEventListener("abort", onAbort, { once: true });
      }
      if (this.rootTurnAdmissionNotified) {
        this.rootTurnAdmissionNotified = false;
        finish();
      }
    });
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

  /** Persist successful handling of a terminal child's external artifacts. */
  async markArtifactsCleaned(agentId: string): Promise<void> {
    if (!this.root) return;
    await this.root.client.request("agent.mark_artifacts_cleaned", { agentId });
  }

  /** Persist that useful external artifacts were retained instead of deleted. */
  async markArtifactsRetained(agentId: string, artifact: Record<string, unknown> = {}): Promise<void> {
    if (!this.root) return;
    await this.root.client.request("agent.mark_artifacts_retained", { agentId, artifact });
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
        // Broker status may observe the subtree after drain/cancel has already
        // committed. Preserve the synchronous local snapshot in that case,
        // while enriching each entry whenever durable metadata is available.
        const enriched = new Map(snapshots.map((snapshot) => [snapshot.agentId, snapshot]));
        const merged = this.lastHandoffSnapshots.map((snapshot) => enriched.get(snapshot.agentId) ?? snapshot);
        const seen = new Set(merged.map((snapshot) => snapshot.agentId));
        for (const snapshot of snapshots) if (!seen.has(snapshot.agentId)) merged.push(snapshot);
        this.lastHandoffSnapshots = merged;
        return merged;
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
      .filter((agent) => agent.depth > 0 && (!["completed", "failed", "cancelled"].includes(agent.status) || agent.reconnectable === true))
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
    return [...this.children.values()].filter((child) => (!["completed", "failed", "cancelled"].includes(child.record.status) || child.record.reconnectable === true)).map((child) => ({
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
    this.resetRootCompactionState();
    this.rootTurnAdmissionNotified = true;
    this.rootTurnAdmissionWake?.();
    await this.endRootModelTurnCapacity();
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
          this.requestLifecycleOnClient(root.client, "agent.end_turn", { status: "ready" }, `root-stop-${this.operationNonce}-${this.lifecycleEpoch}:end`, FabricRuntime.shutdownRpcTimeoutMs).catch(() => undefined),
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
      capacityGroup: input.capacityGroup,
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

  async reconcileRecoveredTurn(client: BrokerClient, registered: AgentRecord, providerStillRunning: boolean, operationId?: string): Promise<AgentRecord> {
    try {
      const result = await client.request<{ agent: AgentRecord }>("agent.reconcile_turn", {
        state: providerStillRunning ? "running" : "stopped",
        ...(operationId ? { operationId } : {}),
      });
      return result.agent;
    } catch (error) {
      // Older brokers do not have the recovery reservation operation. They
      // already released runtime capacity during recovery, so retain legacy
      // reconnect behaviour only for this explicit unknown-operation error.
      if (error instanceof FabricError && error.code === "INVALID_ARGUMENT" && /unknown coordinator operation/i.test(error.message)) return registered;
      throw error;
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
          await this.reconcileRecoveredTurn(this.root.client, refreshed.agent, this.rootModelCapacityRelease !== undefined);
          this.rootToken = refreshed.token;
          this.root.client.setIdentity(this.root.agentId, refreshed.token);
          this.rootTurnAdmissionNotified = true;
          this.rootTurnAdmissionWake?.();
          await this.cleanupAbandonedAgentArtifacts().catch(() => undefined);
          const inbox = await this.drainRootInbox();
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

  private async drainRootInbox(): Promise<AgentMessage[]> {
    if (!this.root) return [];
    const messages: AgentMessage[] = [];
    let afterBrokerSequence = 0;
    const limit = 100;
    while (!this.stopped && this.root) {
      const batch = await this.root.client.request<AgentMessage[]>("message.inbox", { limit, afterBrokerSequence });
      if (!Array.isArray(batch) || batch.length === 0) break;
      messages.push(...batch);
      const sequences = batch
        .map((message) => message.brokerSequence)
        .filter((sequence): sequence is number => typeof sequence === "number" && sequence > afterBrokerSequence);
      if (sequences.length === 0 || batch.length < limit) break;
      afterBrokerSequence = Math.max(...sequences);
      await Promise.resolve();
    }
    return messages;
  }

  /**
   * Reclaim artifacts left by actors whose broker recovery window expired.
   * A failed cleanup is an intentional retained-artifact outcome, not an
   * unhandled state: the useful branch/session location moves to cold metadata
   * so the terminal agent and completed task can still leave hot state.
   */
  private cleanupAbandonedAgentArtifacts(status?: FabricStatus): Promise<void> {
    if (this.cleanupPromise) return this.cleanupPromise;
    const operation = (async () => {
      if (!this.root || this.stopped) return;
      const durable = status ?? (await this.status() as FabricStatus);
      const agents = await this.discoverAllAgentsForCleanup(durable);
      const now = Date.now();
      const grace = this.config.reconnectGraceMs ?? DEFAULT_FABRIC_CONFIG.reconnectGraceMs ?? 0;
      const caseFold = process.platform === "win32" || this.caseInsensitivePaths === true;
      const pathKey = (path: string): string => {
        const canonical = canonicalWorkspacePath(path);
        return caseFold ? canonical.toLowerCase() : canonical;
      };
      const protectedPaths = new Set(
        agents
          .filter((agent) => (!["completed", "failed", "cancelled"].includes(agent.status) || agent.reconnectable === true) && agent.workspace?.mode === "worktree")
          .map((agent) => agent.workspace?.path)
          .filter((path): path is string => Boolean(path))
          .map(pathKey),
      );
      const candidates = agents.filter((agent) => {
        if (agent.depth === 0 || !agent.workspace || agent.artifactDisposition !== undefined || agent.artifactsCleanedAt !== undefined || !["completed", "failed", "cancelled"].includes(agent.status) || agent.reconnectable === true) return false;
        if (agent.recoveryExpiredAt !== undefined) return agent.recoveryExpiredAt <= now;
        return now - agent.lastActivity >= grace;
      });
      for (const agent of candidates) {
        const workspace = agent.workspace;
        if (!workspace || protectedPaths.has(pathKey(workspace.path))) continue;
        const sessionsRoot = resolve(this.stateDirectory, "sessions");
        const sessionPath = resolve(sessionsRoot, agent.id);
        if (sessionPath === sessionsRoot || !sessionPath.startsWith(`${sessionsRoot}${sep}`)) continue;
        let workspaceError: unknown;
        try {
          await this.workspaceStrategy.cleanup(workspace);
        } catch (error) {
          // A dirty or divergent worktree is intentionally retained, but it
          // must not prevent the independent session cleanup below.
          workspaceError = error;
        }
        let sessionError: unknown;
        try {
          await fs.rm(sessionPath, { recursive: true, force: true });
        } catch (error) {
          sessionError = error;
        }
        if (workspaceError || sessionError) {
          const reasons = [workspaceError, sessionError]
            .filter((error): error is unknown => error !== undefined)
            .map((error) => error instanceof Error ? error.message : String(error))
            .join("; ");
          await this.markArtifactsRetained(agent.id, await this.retainedArtifactMetadata(
            workspaceError ? workspace : undefined,
            sessionError ? sessionPath : undefined,
            new Error(reasons || "artifact cleanup failed"),
          )).catch(() => undefined);
          continue;
        }
        // The marker is written only after both external artifacts have been
        // handled. It makes a successful GC pass durable across every later
        // root attachment instead of repeatedly revisiting the same actor.
        await this.markArtifactsCleaned(agent.id).catch(() => undefined);
      }
    })();
    const cleanupPromise = operation.finally(() => {
      if (this.cleanupPromise === cleanupPromise) this.cleanupPromise = undefined;
    });
    this.cleanupPromise = cleanupPromise;
    return cleanupPromise;
  }

  private async discoverAllAgentsForCleanup(status: FabricStatus): Promise<AgentSummary[]> {
    const byId = new Map(status.agents.map((agent) => [agent.id, agent]));
    if (status.truncated?.agents !== true || !this.root) return [...byId.values()];
    // `fabric.status` summaries deliberately omit pagination cursors. Restart
    // the complete discovery from its own first page rather than attempting to
    // page from the last bounded diagnostic row.
    let after: string | undefined;
    for (let page = 0; page < 10_000; page++) {
      const batch = await this.root.client.request<AgentSummary[]>("discover.agents", {
        scope: "all",
        limit: 100,
        ...(after ? { after } : {}),
      });
      if (!Array.isArray(batch) || batch.length === 0) break;
      for (const agent of batch) byId.set(agent.id, agent);
      const next = batch.at(-1)?.cursor;
      if (!next || next === after || batch.length < 100) break;
      after = next;
    }
    return [...byId.values()];
  }

  private async retainedArtifactMetadata(workspace: AgentRecord["workspace"] | undefined, sessionPath: string | undefined, error: unknown): Promise<Record<string, unknown>> {
    const metadata: Record<string, unknown> = {
      workspace,
      workspaceRetained: workspace !== undefined,
      sessionPath,
      sessionRetained: sessionPath !== undefined,
      baseRef: workspace?.baseRef,
      reason: (error instanceof Error ? error.message : String(error)).slice(0, 1800),
    };
    if (workspace && this.workspaceStrategy.describe) {
      const described: { headRef?: string } = await this.workspaceStrategy.describe(workspace).catch((): { headRef?: string } => ({}));
      if (described.headRef) metadata.headRef = described.headRef;
    }
    return metadata;
  }

  private booleanCapabilities(input: SpawnToolInput): Record<string, boolean> {
    const result: Record<string, boolean> = {};
    for (const key of ["maySpawn", "mayMessagePeers", "mayEscalate", "mayTransferOwnership", "mayWriteRepo", "mayUseShell"] as const) {
      if (input[key] !== undefined) result[key] = Boolean(input[key]);
    }
    return result;
  }

  private handleRootEvent(event: { event: string; data: unknown }): void {
    if (event.event === "slot_available" || event.event === "model_turn_granted" || event.event === "model_turn_cancelled") {
      const data = event.data as { agentId?: string };
      if (data.agentId === this.root?.agentId) {
        this.rootTurnAdmissionNotified = true;
        this.rootTurnAdmissionWake?.();
      }
      return;
    }
    if (event.event !== "message_sent" && event.event !== "message_updated") return;
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
  private readonly deliveryStates = new Map<string, { state: "delivering" | "accepted" | "acknowledged"; message: AgentMessage }>();
  /** Messages accepted by Pi but waiting for a durable session transcript boundary. */
  private readonly pendingChildAcks = new Map<string, AgentMessage>();
  /** Prevent duplicate ACK RPCs when prompt completion and agent_settled race. */
  private readonly childAckInFlight = new Map<string, Promise<void>>();
  /** Set only after a prompt/settlement boundary has completed for this ID/revision. */
  private readonly persistedChildMessageIds = new Map<string, number>();
  /** Steered messages use the surrounding agent_settled event as their boundary. */
  private readonly steeredChildMessageIds = new Map<string, number>();
  private eventUnsubscribe?: () => void;
  private sessionEventUnsubscribe?: () => void;
  private closeUnsubscribe?: () => void;
  private heartbeatTimer?: NodeJS.Timeout;
  private reconnectPromise?: Promise<void>;
  private started = false;
  private stopping = false;
  private promptTail: Promise<boolean> = Promise.resolve(true);
  private deliveryTail: Promise<void> = Promise.resolve();
  private readonly pendingReplyIds = new Set<string>();
  /** Cancels a route-capacity wait as soon as local child shutdown begins. */
  private readonly stopController = new AbortController();
  /** Resolves when the broker grants this child's durable turn ticket. */
  private turnAdmissionWake?: () => void;
  private turnAdmissionNotified = false;
  private embeddedManager?: EmbeddedContextManager;
  /** Number of model operations currently holding the child route token. */
  private modelCapacityDepth = 0;
  private resolvingToolCount = 0;
  private hasInFlightWrite = 0;
  private turnOutcome?: ModelTurnOutcome;
  private lastObservedOutcome?: ModelTurnOutcome;
  private compactionFailure?: ModelTurnOutcome;
  /** Operation identity of the provider call currently running locally. */
  private activeTurnOperationId?: string;
  private lastDiagnostic?: string;
  /** A blocked context/provider turn must not be retried by ordinary inbox wakes. */
  private blockedByOutcome?: ModelTurnOutcome;
  /** Guards the one-shot wake scheduled for an explicit blocked-task reopen. */
  private recoveryWakePending = false;
  private recoveryWakeEpoch = 0;
  /** Monotonic logical-turn counter used to derive stable lifecycle operation IDs. */
  private turnSequence = 0;
  /** Prevent operationId reuse after a child host process is restarted. */
  private readonly operationNonce = randomUUID();

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
        const compact = async (): Promise<void> => {
          if (this.session && typeof this.session.compact === "function") {
            // `reason` is protocol metadata (for example, "threshold"), not
            // necessarily prose intended for the model. Only an explicit
            // customInstructions value should become Pi compaction guidance.
            await this.session.compact(request.customInstructions);
          }
        };
        // LCM normally requests compaction after the generation token has been
        // released. If a future provider requests it reentrantly during a
        // generation, avoid self-deadlocking on the same non-reentrant token;
        // Pi's streaming/in-flight guards above still reject unsafe overlap.
        if (this.modelCapacityDepth > 0 || typeof (this.runtime as any).withModelRouteCapacity !== "function") {
          await compact();
        } else {
          await this.runtime.withModelRouteCapacity(this.route, compact, this.stopController.signal);
        }
      },
      onStatus: (_snapshot) => {},
      onDiagnostic: (diagnostic) => {
        const message = typeof diagnostic?.message === "string" ? diagnostic.message : String(diagnostic);
        this.lastDiagnostic = message.replace(/[\u0000-\u001f\u007f]/g, " ").slice(0, 1800);
        // The companion LCM intentionally catches host.compact() failures so
        // it can keep its own gate consistent. Promote only its explicit
        // compaction-failure diagnostic to a lifecycle outcome; storage and
        // output-reduction warnings remain non-blocking.
        if (/\b(?:embedded\s+)?compaction failed\b/i.test(message)) {
          this.compactionFailure = classifyCompactionFailure(
            this.lastDiagnostic,
            false,
            typeof (this.model as any)?.contextWindow === "number" ? (this.model as any).contextWindow : undefined,
          );
        }
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
      shellPolicy: this.runtime.config.shellPolicy ?? (this.workspace?.mode === "worktree" ? "trusted" : "coordination"),
      externalPathAccess: this.runtime.config.externalPathAccess ?? "deny",
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
    const registered = (await this.client.request<{ agent: AgentRecord }>("agent.register", {
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
    this.record = await this.reconcileRecoveredTurn(registered, false);
    this.taskId = this.record.taskId;
    await this.reconcileDurableAgentState(this.record);
    this.started = true;
    this.heartbeatTimer = setInterval(() => {
      void this.client.request("agent.heartbeat", {}).catch(() => {
        // Heartbeat failures can mean that maintenance reclaimed this actor
        // while the host was suspended. Re-register through the normal
        // credentialed recovery path instead of swallowing the lifecycle
        // error and letting the recovery window expire.
        void this.reconnect();
      });
    }, Math.max(1000, this.runtime.config?.heartbeatMs ?? 60_000));
    this.heartbeatTimer.unref();
    this.closeUnsubscribe = this.client.onClose(() => {
      void this.reconnect();
    });
    const pending = this.pendingMessages.splice(0);
    for (const message of pending) this.pendingMessageIds.delete(message.id);
    const inbox = await this.drainInbox();
    // A coalesced notification may have advanced between the event that was
    // buffered before session startup and the inbox scan. Merge by logical ID
    // and keep the highest revision so startup cannot re-deliver an old body
    // and then get stuck on a stale-revision ACK without seeing the update.
    const recovered = new Map<string, AgentMessage>();
    for (const message of [...pending, ...inbox]) {
      const existing = recovered.get(message.id);
      if (!existing || messageRevision(existing) < messageRevision(message)) recovered.set(message.id, message);
    }
    for (const message of [...recovered.values()].sort((left, right) => (left.brokerSequence ?? 0) - (right.brokerSequence ?? 0))) void this.deliverMessage(message);
    this.enqueuePrompt(this.bootstrapPrompt());
  }

  async stop(options: { abortTimeoutMs?: number } = {}): Promise<void> {
    if (this.stopping) return;
    this.stopping = true;
    this.stopController.abort();
    this.turnAdmissionWake?.();
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
      this.stopController.abort();
      try {
        void this.session?.abort();
      } catch {}
      this.disposeNow();
      return;
    }
    this.stopping = true;
    this.stopController.abort();
    this.turnAdmissionWake?.();
    try {
      void this.session?.abort();
    } catch {}
    this.disposeNow();
  }

  /** Synchronous best-effort disposal used after an emergency deadline. */
  disposeNow(): void {
    this.stopping = true;
    this.stopController.abort();
    this.turnAdmissionWake?.();
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
    // Only a clean worktree still at its recorded base is disposable. A clean
    // branch with commits is a user-owned recovery artifact and must remain
    // available for merge, cherry-pick, or inspection. Workspace and session
    // cleanup are independent: retaining a useful branch must not retain its
    // much larger Pi transcript as a side effect.
    let workspaceError: unknown;
    if (this.workspace?.mode === "worktree") {
      try {
        await this.runtime.workspaceStrategy.cleanup(this.workspace);
      } catch (error) {
        workspaceError = error;
      }
    }
    let sessionError: unknown;
    const sessionPath = this.sessionArtifactPath();
    try {
      await this.cleanupSessionArtifacts();
    } catch (error) {
      sessionError = error;
    }
    if (workspaceError || sessionError) {
      const reasons = [workspaceError, sessionError]
        .filter((error): error is unknown => error !== undefined)
        .map((error) => error instanceof Error ? error.message : String(error))
        .join("; ");
      await (this.runtime.markArtifactsRetained?.(this.agentId, await this.retainedArtifactMetadata(
        workspaceError ? this.workspace : undefined,
        sessionError ? sessionPath : undefined,
        new Error(reasons || "artifact cleanup failed"),
      )) ?? Promise.resolve()).catch(() => undefined);
      return;
    }
    // If the broker acknowledgement is lost after both external artifacts are
    // handled, leave the terminal record eligible for a later cleanup retry;
    // do not mislabel already-deleted artifacts as retained.
    await (this.runtime.markArtifactsCleaned?.(this.agentId) ?? Promise.resolve()).catch(() => undefined);
  }

  private sessionArtifactPath(): string {
    return resolve(this.stateDirectory, "sessions", this.agentId);
  }

  private async retainedArtifactMetadata(
    workspace: AgentRecord["workspace"] | undefined,
    sessionPath: string | undefined,
    error: unknown,
  ): Promise<Record<string, unknown>> {
    const metadata: Record<string, unknown> = {
      workspace,
      workspaceRetained: workspace !== undefined,
      sessionPath,
      sessionRetained: sessionPath !== undefined,
      baseRef: workspace?.baseRef,
      reason: (error instanceof Error ? error.message : String(error)).slice(0, 1800),
    };
    if (workspace && this.runtime.workspaceStrategy.describe) {
      const described: { headRef?: string } = await this.runtime.workspaceStrategy.describe(workspace).catch((): { headRef?: string } => ({}));
      if (described.headRef) metadata.headRef = described.headRef;
    }
    return metadata;
  }

  private async cleanupSessionArtifacts(): Promise<void> {
    const sessionsRoot = resolve(this.stateDirectory, "sessions");
    const sessionPath = resolve(sessionsRoot, this.agentId);
    if (sessionPath === sessionsRoot || !sessionPath.startsWith(`${sessionsRoot}${sep}`)) {
      throw new FabricError("WORKSPACE_FAILURE", "refusing to remove a child session outside the sessions directory");
    }
    await fs.rm(sessionPath, { recursive: true, force: true });
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
          const reconciled = await this.reconcileRecoveredTurn(registered.agent, this.session?.isStreaming === true, this.activeTurnOperationId);
          const durable = await this.reconcileDurableAgentState(reconciled);
          // A slot grant may have happened while the socket was down. Wake the
          // admission loop; its idempotent begin_turn retry observes the
          // durable running state even if the notification was missed.
          this.turnAdmissionNotified = true;
          this.turnAdmissionWake?.();
          if (["completed", "cancelled"].includes(durable.status) || durable.status === "failed" && durable.reconnectable !== true) {
            await this.stop();
            return;
          }
          const inbox = await this.drainInbox();
          for (const message of inbox) void this.deliverMessage(message);
          if (!this.blockedByOutcome && !this.recoveryWakePending && this.taskId && this.session && !this.session.isStreaming) {
            this.enqueuePrompt(`Broker recovered. Resume assigned task ${this.taskId} from the durable task state.`);
          }
          return;
        } catch (error) {
          if (isTerminalReconnectFailure(error) && !isParentRecoveryConflict(error)) {
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

  /**
   * Rebuild the local recovery gate from durable broker state after startup or
   * reconnect. Task updates and agent updates are separate event streams, so a
   * transport gap can leave either one newer than the other; querying both
   * makes a blocked fact authoritative even when its companion event was
   * missed. Clearing first also lets an explicit task reopen release a stale
   * in-memory gate before inbox delivery resumes.
   */
  private async reconcileRecoveredTurn(registered: AgentRecord, providerStillRunning: boolean, operationId?: string): Promise<AgentRecord> {
    const reconcile = (this.runtime as unknown as { reconcileRecoveredTurn?: (client: BrokerClient, agent: AgentRecord, running: boolean, operationId?: string) => Promise<AgentRecord> }).reconcileRecoveredTurn;
    return typeof reconcile === "function" ? reconcile.call(this.runtime, this.client, registered, providerStillRunning, operationId) : registered;
  }

  private async reconcileDurableAgentState(agent?: AgentRecord): Promise<AgentRecord> {
    const durable = agent ?? await this.client.request<AgentRecord>("agent.status", {});
    this.record = durable;
    this.taskId = durable.taskId;
    await this.reconcileRecoveryGate(durable);
    return durable;
  }

  private async reconcileRecoveryGate(agent: AgentRecord): Promise<void> {
    const previousGate = this.blockedByOutcome;
    let task: TaskRecord | undefined;
    if (agent.taskId) {
      try {
        task = await this.client.request<TaskRecord>("task.show", { taskId: agent.taskId });
      } catch {
        // A successful re-register followed by an uncertain task read must
        // not resume provider work with an unknown semantic state. Keep the
        // child gated until a later task event/reconnect can reconcile it.
        this.blockedByOutcome = previousGate ?? classifyCompactionFailure(
          agent.contextDiagnostic ?? "Unable to reconcile assigned task state after broker reconnect",
        );
        return;
      }
    }
    // A durable terminal task is stronger than a stale agent status. Do not
    // re-arm a recovery gate (or resume work) from an older blocked event.
    if (task && ["completed", "failed", "cancelled"].includes(task.status)) {
      this.blockedByOutcome = undefined;
      return;
    }
    if (agent.status === "blocked" || task?.status === "blocked") {
      this.blockedByOutcome = classifyCompactionFailure(
        task?.blockedReason ?? agent.contextDiagnostic ?? "Agent remains blocked pending explicit task recovery",
      );
      return;
    }
    this.blockedByOutcome = undefined;
    if (previousGate && task) this.scheduleRecoveryWake(task.id);
  }

  private scheduleRecoveryWake(taskId: string | undefined): void {
    if (!taskId || this.recoveryWakePending || !this.session || this.stopping) return;
    this.recoveryWakePending = true;
    const epoch = ++this.recoveryWakeEpoch;
    const operation = this.enqueuePrompt(
      `Task ${taskId} was explicitly reopened. Resume from durable task state. Reacquire any required mutable resources before writing.`,
    );
    void operation.finally(() => {
      if (epoch === this.recoveryWakeEpoch) this.recoveryWakePending = false;
    });
  }

  private waitForTurnAdmission(): Promise<void> {
    if (this.turnAdmissionNotified) {
      this.turnAdmissionNotified = false;
      return Promise.resolve();
    }
    if (this.stopping) return Promise.resolve();
    return new Promise<void>((resolve) => {
      this.turnAdmissionWake = () => {
        this.turnAdmissionWake = undefined;
        this.turnAdmissionNotified = false;
        resolve();
      };
      if (this.turnAdmissionNotified || this.stopping) {
        this.turnAdmissionWake = undefined;
        this.turnAdmissionNotified = false;
        resolve();
      }
    });
  }

  private requestLifecycle<T = unknown>(operation: string, args: Record<string, unknown>, operationId: string): Promise<T> {
    const retry = (this.client as unknown as { requestIdempotent?: (...input: any[]) => Promise<unknown> }).requestIdempotent;
    if (typeof retry === "function") {
      return (retry.call(this.client, operation, args, operationId) as Promise<T>).catch((error) => {
        if (error instanceof FabricError && error.code === "INVALID_ARGUMENT" && /operationId.*supported/i.test(error.message)) {
          return this.client.request<T>(operation, args);
        }
        throw error;
      });
    }
    // Lightweight lifecycle fakes used by hosts/tests may expose only request;
    // retain compatibility while production BrokerClient gets durable replay.
    return this.client.request<T>(operation, args);
  }

  private enqueuePrompt(prompt: string): Promise<boolean> {
    // The returned promise resolves only after Pi has finished the queued
    // prompt. Message callers additionally verify that the corresponding user
    // entry is present in the durable SessionManager transcript before ACKing.
    const operation = this.promptTail.then(() => this.executePrompt(prompt)).catch(async (error) => {
      // An exception can mean the prior lifecycle response was lost after a
      // commit. Reconcile durable status first; never blindly apply a second
      // failure transition to an operation that may already have ended ready,
      // blocked, or terminal.
      const current = await this.reconcileDurableAgentState().catch(() => undefined);
      let terminalized = false;
      if (current && ["starting", "running", "waiting"].includes(current.status)) {
        const ended = await this.requestLifecycle<{ agent?: AgentRecord }>("agent.end_turn", {
          status: "failed",
          statusReason: error instanceof Error ? error.message : String(error),
        }, `recovery-${this.operationNonce}-${this.turnSequence++}-end`).catch(() => undefined);
        terminalized = Boolean(ended?.agent && ["completed", "failed", "cancelled"].includes(ended.agent.status));
      }
      if (current && (current.status === "completed" || current.status === "cancelled" || current.status === "failed" && current.reconnectable !== true)) {
        terminalized = true;
      }
      // Once the coordinator has committed failure, no later queued message
      // may be delivered into a terminal session. If transport is unavailable,
      // leave the live runtime reconnectable instead of manufacturing failure.
      if (terminalized) await this.stop();
      return false;
    });
    this.promptTail = operation;
    return operation;
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
    if (event.type === "message_end" && event.message?.role === "user") {
      const content = event.message.content;
      const messageText = typeof content === "string"
        ? content
        : Array.isArray(content)
          ? content.filter((part: any) => part?.type === "text").map((part: any) => part.text).join("\n")
          : "";
      if (typeof messageText === "string") {
        for (const message of this.pendingChildAcks.values()) {
          const receipt = extractChildMessageReceipt(messageText);
          if (receipt?.id === message.id && receipt.revision === messageRevision(message)) this.persistedChildMessageIds.set(message.id, messageRevision(message));
        }
      }
      return;
    }
    if (event.type === "agent_end") {
      const assistant = findFinalAssistantMessage(event.messages ?? []);
      if (assistant !== undefined) {
        const classified = classifyAssistantMessage(assistant, typeof (this.model as any)?.contextWindow === "number" ? (this.model as any).contextWindow : undefined);
        this.lastObservedOutcome = classified;
        if (event.willRetry !== true) this.turnOutcome = classified;
      }
      return;
    }
    if (event.type === "agent_settled") {
      void this.acknowledgePersistedChildMessages();
      return;
    }
    if (event.type === "compaction_end" && event.result === undefined && !event.aborted) {
      this.compactionFailure = classifyCompactionFailure(event.errorMessage, false, typeof (this.model as any)?.contextWindow === "number" ? (this.model as any).contextWindow : undefined);
    }
  }

  private async finishTurnWithOutcome(outcomeValue: ModelTurnOutcome, taskId: string | undefined, turnId: string): Promise<void> {
    const reason = describeTurnOutcome(outcomeValue);
    const task = taskId
      ? await this.client.request<TaskRecord>("task.show", { taskId }).catch(() => undefined)
      : undefined;
    const taskIsTerminal = Boolean(task && ["completed", "failed", "cancelled"].includes(task.status));
    // A semantic task completion wins over a late provider/compaction outcome.
    // Keep the diagnostic for observability, but never arm the blocked gate or
    // emit a contradictory parent notification for an already-terminal task.
    if (outcomeValue.lifecycle === "blocked" && !taskIsTerminal) this.blockedByOutcome = outcomeValue;

    const finishArgs: Record<string, unknown> = {
      status: taskIsTerminal ? "ready" : outcomeValue.lifecycle === "blocked" ? "blocked" : outcomeValue.lifecycle === "failed" ? "failed" : "ready",
      statusReason: reason,
      ...(taskId && !taskIsTerminal && outcomeValue.lifecycle === "blocked" ? { taskId, taskAction: "block", reason } : {}),
      ...(taskId && !taskIsTerminal && outcomeValue.lifecycle === "failed" ? { taskId, taskAction: "fail", reason } : {}),
      metadata: {
        cause: outcomeValue.kind,
        provider: outcomeValue.provider,
        model: outcomeValue.model,
        contextTokens: outcomeValue.contextTokens,
        contextWindow: outcomeValue.contextWindow,
      },
    };
    let ended: { agent: AgentRecord; task?: TaskRecord };
    try {
      ended = await this.requestLifecycle<{ agent: AgentRecord; task?: TaskRecord }>("agent.finish_turn", finishArgs, `${turnId}:finish`);
      if (!ended?.agent) throw new FabricError("INVALID_ARGUMENT", "broker returned no finished agent");
    } catch (error) {
      // A broker process from an older safe-agent build may not know the
      // compound operation. Preserve compatibility, while current brokers use
      // the atomic path above to avoid task/agent/notice split-brain states.
      if (!(error instanceof FabricError) || error.code !== "INVALID_ARGUMENT") throw error;
      if (taskId && !taskIsTerminal && outcomeValue.lifecycle === "blocked") await this.client.request("task.update", { taskId, action: "block", reason }).catch(() => undefined);
      if (taskId && !taskIsTerminal && outcomeValue.lifecycle === "failed") await this.client.request("task.update", { taskId, action: "fail", reason }).catch(() => undefined);
      const legacy = await this.client.request<{ agent: AgentRecord }>("agent.end_turn", {
        status: finishArgs.status,
        statusReason: reason,
      });
      if (outcomeValue.lifecycle === "blocked" && !taskIsTerminal) {
        await this.client.request("message.send", {
          to: this.parentId,
          type: "blocked",
          body: reason,
          metadata: finishArgs.metadata,
        }).catch(() => undefined);
      }
      ended = legacy;
    }
    this.record = ended.agent;
    this.taskId = ended.agent.taskId;
    if (outcomeValue.lifecycle === "blocked") {
      this.lastDiagnostic = reason;
      void this.client.request("agent.update", { contextDiagnostic: reason }).catch(() => undefined);
    }
    if (outcomeValue.lifecycle === "failed" || ["completed", "failed", "cancelled"].includes(ended.agent.status)) await this.stop();
  }

  private async executePrompt(prompt: string): Promise<boolean> {
    if (!this.session || this.stopping || this.blockedByOutcome) return false;
    const turnId = `turn-${this.operationNonce}-${++this.turnSequence}`;
    this.activeTurnOperationId = `${turnId}:begin`;
    this.turnOutcome = undefined;
    this.lastObservedOutcome = undefined;
    this.compactionFailure = undefined;
    let started = false;
    this.turnAdmissionNotified = false;
    while (!started && !this.stopping) {
      const result = await this.requestLifecycle<{ started: boolean; queued?: boolean }>("agent.begin_turn", {}, `${turnId}:begin`);
      started = result.started;
      if (!started) {
        if (result.queued === true) await this.waitForTurnAdmission();
        else await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }
    if (!started || this.stopping) return false;
    if (this.embeddedManager && this.contextMode === "lcm-embedded") {
      try {
        this.embeddedManager.observeTurnStart();
      } catch {
        await this.degradeToNativeContext();
      }
    }
    const runPrompt = () => this.session!.prompt(prompt, { expandPromptTemplates: false });
    // Public AgentSession.prompt() creates a new user message on every call.
    // Never emulate a same-context retry by calling it twice: Pi's native retry
    // machinery is the only safe generation-level retry seam. If a structured
    // prefill/KV failure is not natively retryable, finish this turn as blocked
    // after the single prompt so the conversation is not silently duplicated.
    let promptThrownOutcome: ModelTurnOutcome | undefined;
    try {
      const runPromptWithDepth = async (): Promise<void> => {
        this.modelCapacityDepth += 1;
        try {
          await runPrompt();
        } finally {
          this.modelCapacityDepth -= 1;
        }
      };
      if (typeof (this.runtime as any).withModelRouteCapacity === "function") await this.runtime.withModelRouteCapacity(this.route, runPromptWithDepth, this.stopController.signal);
      else await runPromptWithDepth();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (this.stopping) return false;
      promptThrownOutcome = classifyAssistantMessage({
        role: "assistant",
        provider: this.route.provider,
        model: this.route.model,
        stopReason: isAbortLikeMessage(undefined, message) ? "aborted" : "error",
        errorMessage: message,
        usage: { input: 0, cacheRead: 0, output: 0 },
      }, typeof (this.model as any)?.contextWindow === "number" ? (this.model as any).contextWindow : undefined);
    }
    if (promptThrownOutcome) {
      const agent = await this.client.request<AgentRecord>("agent.status", {});
      await this.finishTurnWithOutcome(promptThrownOutcome, agent.taskId ?? this.taskId, turnId);
      this.activeTurnOperationId = undefined;
      return true;
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
      await this.finishTurnWithOutcome(outcomeValue, assignedTaskId, turnId);
      return true;
    }
    const task = assignedTaskId
      ? await this.client.request<TaskRecord>("task.show", { taskId: assignedTaskId })
      : undefined;
    const status = taskAwareTurnStatus(task, this.pendingReplyIds.size > 0);
    const ended = await this.requestLifecycle<{ agent: AgentRecord }>("agent.end_turn", { status }, `${turnId}:end`);
    this.record = ended.agent;
    this.taskId = ended.agent.taskId;
    if (["completed", "failed", "cancelled"].includes(ended.agent.status)) await this.stop();
    this.activeTurnOperationId = undefined;
    return true;
  }

  private handleEvent(event: { event: string; data: unknown }): void {
    if (event.event === "slot_available" || event.event === "model_turn_granted" || event.event === "model_turn_cancelled") {
      const data = event.data as { agentId?: string };
      if (data.agentId === this.agentId) {
        this.turnAdmissionNotified = true;
        this.turnAdmissionWake?.();
      }
      return;
    }
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
        this.taskId = agent.taskId;
        void this.reconcileDurableAgentState(agent).then((durable) => {
          if (durable.status === "completed" || durable.status === "cancelled" || durable.status === "failed" && durable.reconnectable !== true) {
            void this.stop();
          } else if (durable.status !== "failed" && !this.blockedByOutcome) {
            this.drainPendingMessages();
          }
        }).catch(() => undefined);
      }
      return;
    }
    if (event.event === "task_changed") {
      const task = (event.data as { task?: TaskRecord }).task;
      if (task?.owner === this.agentId && task.status === "blocked") {
        this.blockedByOutcome = classifyCompactionFailure(task.blockedReason ?? "Assigned task remains blocked pending explicit recovery");
        return;
      }
      if (task?.owner === this.agentId && task.status !== "blocked" && !["completed", "failed", "cancelled"].includes(task.status) && this.blockedByOutcome) {
        this.blockedByOutcome = undefined;
        this.scheduleRecoveryWake(task.id);
        this.drainPendingMessages();
      }
      return;
    }
    if (event.event !== "message_sent" && event.event !== "message_updated") return;
    const message = (event.data as { message?: AgentMessage }).message;
    if (!message || message.to !== this.agentId) return;
    if (!this.session) {
      this.rememberPendingMessage(message);
      return;
    }
    void this.deliverMessage(message);
  }

  private rememberPendingMessage(message: AgentMessage): void {
    this.pendingMessageIds.add(message.id);
    const existing = this.pendingMessages.findIndex((candidate) => candidate.id === message.id);
    if (existing >= 0) {
      if (messageRevision(this.pendingMessages[existing]) < messageRevision(message)) this.pendingMessages[existing] = message;
      return;
    }
    this.pendingMessages.push(message);
  }

  private deliverMessage(message: AgentMessage): Promise<void> {
    const current = this.deliveryStates.get(message.id);
    if (current && messageRevision(message) <= messageRevision(current.message)) {
      if (current.state === "accepted") return this.acknowledgePersistedChildMessage(current.message);
      return Promise.resolve();
    }
    if (!this.session || this.stopping) {
      this.rememberPendingMessage(message);
      return Promise.resolve();
    }

    if (current?.state === "delivering") {
      current.message = message;
      this.pendingChildAcks.set(message.id, message);
      return this.deliveryTail;
    }

    // A coalesced control message keeps its logical ID but receives a new
    // revision. Re-enter delivery for the new revision even if the older
    // payload was already accepted by Pi; its ACK must never acknowledge the
    // newer broker state.
    this.deliveryStates.set(message.id, { state: "delivering", message });
    this.pendingChildAcks.set(message.id, message);

    // Serialize acceptance itself, including steer calls. The broker's
    // senderSequence is durable, but concurrent model-session calls could
    // otherwise reorder two notifications before promptTail gets involved.
    return this.enqueueMessageDelivery(message);
  }

  private enqueueMessageDelivery(message: AgentMessage): Promise<void> {
    this.deliveryTail = this.deliveryTail.then(() => this.acceptMessage(message)).catch(() => undefined);
    return this.deliveryTail;
  }

  private queueNewerMessage(message: AgentMessage): void {
    const current = this.deliveryStates.get(message.id);
    if (!current || messageRevision(current.message) < messageRevision(message)) {
      this.deliveryStates.set(message.id, { state: "delivering", message });
      this.pendingChildAcks.set(message.id, message);
    } else {
      current.state = "delivering";
      current.message = message;
      this.pendingChildAcks.set(message.id, message);
    }
    void this.enqueueMessageDelivery(message);
  }

  private async acceptMessage(message: AgentMessage): Promise<void> {
    const current = this.deliveryStates.get(message.id);
    if (current && messageRevision(current.message) > messageRevision(message)) message = current.message;
    if (current?.state === "acknowledged") return;
    if (this.blockedByOutcome) {
      this.deliveryStates.delete(message.id);
      this.rememberPendingMessage(message);
      return;
    }
    const text = childMessagePrompt(message);
    let accepted = false;
    try {
      if (!this.session || this.stopping) throw new FabricError("CHILD_SESSION_FAILURE", "Child session is not ready to accept messages");
      // A reconnect can observe an unacknowledged broker message whose Pi user
      // entry was already flushed before the process died. Acknowledge that
      // durable copy without injecting a duplicate prompt.
      if (this.hasPersistedChildMessage(message)) {
        accepted = true;
        this.deliveryStates.set(message.id, { state: "accepted", message });
        this.pendingChildAcks.set(message.id, message);
        this.persistedChildMessageIds.set(message.id, messageRevision(message));
        await this.acknowledgePersistedChildMessage(message, true);
        return;
      }
      if (this.session.isStreaming) {
        await this.session.steer(text);
        const latest = this.deliveryStates.get(message.id)?.message;
        if (latest && messageRevision(latest) > messageRevision(message)) {
          this.queueNewerMessage(latest);
          return;
        }
        this.steeredChildMessageIds.set(message.id, messageRevision(message));
      } else {
        const promptCompletion = this.enqueuePrompt(text);
        accepted = true;
        if (message.type === "response" && message.requestId) this.pendingReplyIds.delete(message.requestId);
        this.deliveryStates.set(message.id, { state: "accepted", message });
        this.pendingChildAcks.set(message.id, message);
        void promptCompletion.then((completed) => {
          if (!completed) return;
          const manager = (this.session as any)?.sessionManager as { getEntries?: () => readonly any[] } | undefined;
          const latest = this.deliveryStates.get(message.id)?.message;
          if (latest && messageRevision(latest) > messageRevision(message)) return;
          if (this.persistedChildMessageIds.get(message.id) !== messageRevision(message)) {
            if (typeof manager?.getEntries === "function" && !this.hasPersistedChildMessage(message)) return;
            this.persistedChildMessageIds.set(message.id, messageRevision(message));
          }
          return this.acknowledgePersistedChildMessage(message, true);
        });
        return;
      }
      accepted = true;
      if (message.type === "response" && message.requestId) this.pendingReplyIds.delete(message.requestId);
      this.deliveryStates.set(message.id, { state: "accepted", message });
      this.pendingChildAcks.set(message.id, message);
      // Steered messages are persisted with the surrounding turn. The
      // agent_settled listener supplies the durable boundary for their ACK.
    } catch {
      const latest = this.deliveryStates.get(message.id)?.message;
      if (latest && messageRevision(latest) > messageRevision(message)) {
        this.queueNewerMessage(latest);
      } else if (accepted) this.deliveryStates.set(message.id, { state: "accepted", message });
      else this.deliveryStates.delete(message.id);
      // Leave unacknowledged so inbox recovery can retry after reconnect.
    }
  }

  private markMessageAcknowledged(message: AgentMessage): void {
    const messageId = message.id;
    const current = this.deliveryStates.get(messageId);
    if (current && messageRevision(current.message) > messageRevision(message)) {
      current.state = "delivering";
      void this.deliverMessage(current.message);
      return;
    }
    this.pendingChildAcks.delete(messageId);
    this.persistedChildMessageIds.delete(messageId);
    this.steeredChildMessageIds.delete(messageId);
    this.deliveryStates.set(messageId, { state: "acknowledged", message });
    while (this.deliveryStates.size > 2048) {
      const removable = [...this.deliveryStates.entries()].find(([, current]) => current.state === "acknowledged")?.[0];
      if (!removable) break;
      this.deliveryStates.delete(removable);
    }
    while (this.persistedChildMessageIds.size > 2048) {
      const removable = this.persistedChildMessageIds.keys().next().value as string | undefined;
      if (!removable) break;
      this.persistedChildMessageIds.delete(removable);
    }
  }

  private hasPersistedChildMessage(message: Pick<AgentMessage, "id" | "revision">): boolean {
    try {
      const manager = (this.session as any)?.sessionManager as { getEntries?: () => readonly any[] } | undefined;
      const entries = typeof manager?.getEntries === "function" ? manager.getEntries() : [];
      return entries.some((entry: any) => {
        if (entry?.type !== "message" || entry.message?.role !== "user") return false;
        const content = entry.message.content;
        const text = typeof content === "string"
          ? content
          : Array.isArray(content)
            ? content.filter((part: any) => part?.type === "text").map((part: any) => part.text).join("\n")
            : "";
        const receipt = extractChildMessageReceipt(text);
        return receipt?.id === message.id && receipt.revision === messageRevision(message);
      });
    } catch {
      return false;
    }
  }

  private acknowledgePersistedChildMessage(message: AgentMessage, boundary = false): Promise<void> {
    // `boundary` is supplied only after a completed prompt/settled turn or a
    // reconnect scan of an existing transcript entry. Once that boundary is
    // observed, a later compaction may legitimately remove the original user
    // entry, so do not require a second transcript scan before ACKing.
    const latest = this.deliveryStates.get(message.id)?.message;
    if (latest && messageRevision(latest) > messageRevision(message)) return this.deliverMessage(latest);
    const revision = messageRevision(message);
    const ackKey = `${message.id}\u0000${revision}`;
    const inFlight = this.childAckInFlight.get(ackKey);
    if (inFlight) return inFlight;
    const manager = (this.session as any)?.sessionManager as { getEntries?: () => readonly any[] } | undefined;
    let durableBoundary = boundary || this.persistedChildMessageIds.get(message.id) === revision;
    if (!durableBoundary && typeof manager?.getEntries === "function" && this.hasPersistedChildMessage(message)) {
      this.persistedChildMessageIds.set(message.id, revision);
      durableBoundary = true;
    }
    if (!durableBoundary) return Promise.resolve();
    // Once a prior completion/settlement boundary recorded the ID, a later
    // compaction may legitimately remove that transcript entry. Do not make a
    // broker ACK retry depend on the entry still being present.
    const ackArgs = { messageId: message.id, revision };
    const ackOperationId = `ack:${this.agentId}:${message.id}:${revision}`;
    const retry = (this.client as unknown as { requestIdempotent?: (...input: any[]) => Promise<unknown> }).requestIdempotent;
    const operation = (typeof retry === "function"
      ? (retry.call(this.client, "message.ack", ackArgs, ackOperationId) as Promise<unknown>).catch((error) => {
        if (error instanceof FabricError && error.code === "INVALID_ARGUMENT" && /operationId.*supported/i.test(error.message)) {
          return this.client.request("message.ack", ackArgs);
        }
        throw error;
      })
      : this.client.request("message.ack", ackArgs))
      .then(() => this.markMessageAcknowledged(message))
      .catch((error) => {
        // The broker may have coalesced a newer payload before this ACK
        // arrived. Keep the logical message pending and schedule the current
        // revision for delivery.
        if (error instanceof FabricError && error.code === "MESSAGE_REVISION_CONFLICT") {
          const currentMessage = this.deliveryStates.get(message.id)?.message;
          if (currentMessage && messageRevision(currentMessage) > revision) void this.deliverMessage(currentMessage);
        }
        // Keep the accepted marker and broker copy for a later settlement or
        // reconnect retry. A lost ACK must never turn into message loss.
      })
      .finally(() => {
        if (this.childAckInFlight.get(ackKey) === operation) this.childAckInFlight.delete(ackKey);
      });
    this.childAckInFlight.set(ackKey, operation);
    return operation;
  }

  private async acknowledgePersistedChildMessages(): Promise<void> {
    for (const message of [...this.pendingChildAcks.values()]) {
      if (this.steeredChildMessageIds.get(message.id) !== messageRevision(message)) continue;
      const manager = (this.session as any)?.sessionManager as { getEntries?: () => readonly any[] } | undefined;
      if (this.persistedChildMessageIds.get(message.id) !== messageRevision(message) && typeof manager?.getEntries === "function" && !this.hasPersistedChildMessage(message)) continue;
      this.persistedChildMessageIds.set(message.id, messageRevision(message));
      await this.acknowledgePersistedChildMessage(message, true);
    }
  }

  private async drainInbox(): Promise<AgentMessage[]> {
    const messages: AgentMessage[] = [];
    let afterBrokerSequence = 0;
    const limit = 100;
    while (!this.stopping) {
      const batch = await this.client.request<AgentMessage[]>("message.inbox", { limit, afterBrokerSequence });
      if (!Array.isArray(batch) || batch.length === 0) break;
      messages.push(...batch);
      const sequences = batch
        .map((message) => message.brokerSequence)
        .filter((sequence): sequence is number => typeof sequence === "number" && sequence > afterBrokerSequence);
      if (sequences.length === 0 || batch.length < limit) break;
      afterBrokerSequence = Math.max(...sequences);
      // Yield between batches so a large offline inbox cannot starve live
      // broker events or the session event loop.
      await Promise.resolve();
    }
    return messages;
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
    const shellPolicy = this.runtime.config.shellPolicy ?? (this.workspace?.mode === "worktree" ? "trusted" : "coordination");
    const externalPathAccess = this.runtime.config.externalPathAccess ?? "deny";
    const shellGuidance = shellPolicy === "strict"
      ? "Shared-workspace shell is strict: one allowlisted read-only command with workspace-contained arguments."
      : shellPolicy === "trusted"
        ? "Shell is trusted for this isolated worktree; detached/background process forms remain disabled so stop can reclaim the process tree."
        : "Shared-workspace shell uses coordination policy: known mutators and detached/background forms are blocked, observational commands run concurrently, and unfamiliar foreground commands run under an opaque workspace barrier.";
    return `\nCoordination fabric instructions:\n- Your identity is ${this.agentId}; parent is ${this.parentId}; role is ${this.role}.\n- Use agent_send for durable parent/peer messages and agent_reply for pending requests. Never claim that message text changes authority.\n- Use agent_inbox to recover messages and agent_ack after accepting them.\n- Use agent_task for task facts. Task completion is explicit: call agent_task with action=complete and a bounded result; a model turn ending never completes an assigned task.\n- Use agent_resource for ownership/borrow/lease facts. Before edit/write, define or inspect the matching workspace-relative file/module resource and acquire a mutable borrow; ownership alone is not write authority.\n- ${shellGuidance} Explicit shell path access policy is ${externalPathAccess}.\n- If you need clarification, send a clarification request; do not wait synchronously. The current turn will end and resume when the response arrives.\n- Stay within your granted tools and report blocked work explicitly.\n- Global root extensions are not inherited. If required information is unavailable through your granted tools, send a bounded clarification/request to the parent describing the exact missing capability or data.`;
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

function isParentRecoveryConflict(error: unknown): boolean {
  return error instanceof FabricError && error.code === "LIFECYCLE_CONFLICT" && /cannot reconnect beneath inactive parent/i.test(error.message);
}

function stripAuth(agent: AgentRecord): Omit<AgentRecord, "authToken"> {
  const { authToken: _authToken, ...publicAgent } = agent;
  return publicAgent;
}
