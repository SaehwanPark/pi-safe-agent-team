import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager, type AgentSession, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { Model } from "@earendil-works/pi-ai";
import type { ThinkingLevel } from "@earendil-works/pi-agent-core";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { FabricError, asFabricError } from "../core/errors.ts";
import { Coordinator } from "../core/coordinator.ts";
import type { AgentStatus, FabricConfig, AgentMessage, AgentRecord, ModelRoute, TaskRecord } from "../core/types.ts";
import { BrokerClient } from "../broker/client.ts";
import { BrokerServer, defaultEndpoint } from "../broker/server.ts";
import { GitWorkspaceStrategy, type WorkspaceStrategy } from "../workspace.ts";
import { resolveChildModel, routeFromModel } from "./model-routing.ts";
import { createCoordinationTools, type SpawnToolInput } from "./tools.ts";
import { createGuardedChildTools, createGuardedReadOnlyTools } from "./guards.ts";

export interface RoleConfig {
  model?: string;
  provider?: string;
  thinking?: ThinkingLevel;
  capabilities?: Partial<AgentRecord["capabilities"]>;
}

export interface FabricRuntimeOptions {
  cwd?: string;
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

export class FabricRuntime {
  readonly cwd: string;
  readonly fabricId: string;
  readonly stateDirectory: string;
  readonly agentDir: string;
  readonly endpoint: string;
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

  constructor(options: FabricRuntimeOptions = {}) {
    this.options = options;
    this.cwd = options.cwd ?? process.cwd();
    this.fabricId = options.fabricId ?? `fabric-${createHash("sha256").update(this.cwd).digest("hex").slice(0, 24)}`;
    this.agentDir = options.agentDir ?? process.env.PI_AGENT_DIR ?? join(homedir(), ".pi", "agent");
    this.stateDirectory = options.stateDirectory ?? join(this.agentDir, "safe-agents", createHash("sha256").update(this.cwd).digest("hex").slice(0, 24));
    this.endpoint = options.endpoint ?? defaultEndpoint(this.stateDirectory);
    this.config = options.config;
    this.workspaceStrategy = options.workspaceStrategy ?? new GitWorkspaceStrategy();
    this.roles = options.roles ?? {};
  }

  async attachRoot(api: ExtensionAPI, ctx: ExtensionContext, rootDelivery?: (message: AgentMessage) => void): Promise<RootBinding> {
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
          sessionId: ctx.sessionManager.getSessionId(),
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
    const sessionId = ctx.sessionManager.getSessionId();
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

  async request<T = unknown>(operation: string, args: Record<string, unknown> = {}): Promise<T> {
    if (!this.root) throw new FabricError("BROKER_UNAVAILABLE", "Fabric root is not attached");
    return this.root.client.request<T>(operation, args);
  }

  async status(): Promise<unknown> {
    return this.request("fabric.status", {});
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
    return this.spawnChild(this.root.agentId, this.root.client, input, parentModel ?? this.root.ctx.model, parentThinking ?? this.root.ctx.thinkingLevel ?? "medium", this.root.ctx.cwd);
  }

  async spawnChildFrom(parent: ManagedChild, input: SpawnToolInput): Promise<SpawnedAgentSummary> {
    return this.spawnChild(parent.agentId, parent.client, input, parent.session?.model, parent.session?.thinkingLevel ?? "medium", parent.workspacePath);
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    if (this.rootHeartbeatTimer) clearInterval(this.rootHeartbeatTimer);
    this.rootEventUnsubscribe?.();
    this.rootCloseUnsubscribe?.();
    // A host shutdown is not a semantic cancellation of the root identity.
    // Cancel managed descendants explicitly, then leave the reusable root in
    // the durable ready state so a later Pi session can reconnect. Explicit
    // terminal transitions still remain irreversible.
    if (this.root) {
      await this.root.client.request("agent.end_turn", { status: "ready" }).catch(() => undefined);
      for (const child of this.children.values()) {
        await this.root.client.request("agent.cancel", { agentId: child.agentId }).catch(() => undefined);
        await child.stop();
      }
    } else {
      for (const child of this.children.values()) await child.stop();
    }
    this.children.clear();
    this.root?.client.close();
    if (this.server) await this.server.stop();
    this.server = undefined;
    this.root = undefined;
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
      const candidate = new BrokerServer({ directory: this.stateDirectory, rootId: this.fabricId, rootAgentId: `root-${createHash("sha256").update(this.fabricId).digest("hex").slice(0, 24)}`, config: this.config, endpoint: this.endpoint });
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
  ): Promise<SpawnedAgentSummary> {
    if (!this.modelRegistry) throw new FabricError("MODEL_ROUTE_INVALID", "Model registry is unavailable");
    const role = input.role ? this.roles[input.role] : undefined;
    const resolved = resolveChildModel(this.modelRegistry, {
      provider: input.provider,
      model: input.model,
      thinking: input.thinking as ThinkingLevel | undefined,
    }, role, this.options.defaults, parentModel, parentThinking as ThinkingLevel);
    const spawned = await parentClient.request<{ agent: AgentRecord; token: string; taskId?: string }>("agent.spawn", {
      role: input.role ?? "agent",
      route: resolved.route,
      capabilities: { ...(role?.capabilities ?? {}), ...this.booleanCapabilities(input) },
      taskId: input.taskId,
      taskDescription: input.taskDescription,
    });
    let workspace: AgentRecord["workspace"] | undefined;
    try {
      workspace = await this.workspaceStrategy.create({
        mode: input.workspace ?? "shared",
        cwd,
        stateDirectory: this.stateDirectory,
        agentId: spawned.agent.id,
        baseRef: input.baseRef,
      });
      await parentClient.request("agent.configure_child", { agentId: spawned.agent.id, workspace });
    } catch (error) {
      await parentClient.request("agent.cancel", { agentId: spawned.agent.id }).catch(() => undefined);
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
      await child.start();
    } catch (error) {
      await parentClient.request("agent.cancel", { agentId: child.agentId }).catch(() => undefined);
      await child.stop().catch(() => undefined);
      this.children.delete(child.agentId);
      throw asFabricError(error, "CHILD_SESSION_FAILURE");
    }
    return {
      agent: stripAuth(child.record),
      taskId: spawned.taskId,
      workspace,
      routeSource: resolved.source,
    };
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
          const refreshed = await this.root.client.request<{ agent: AgentRecord; token: string }>("agent.register", {
            rootId: this.fabricId,
            role: "root",
            route: routeFromModel(ctx.model, ctx.thinkingLevel ?? "medium"),
            capabilities: { maySpawn: true, mayMessagePeers: true, mayEscalate: true, mayTransferOwnership: true, mayWriteRepo: true, mayUseShell: true },
            sessionId: ctx.sessionManager.getSessionId(),
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

  private readonly pendingMessages: AgentMessage[] = [];
  private readonly pendingMessageIds = new Set<string>();
  private readonly deliveryStates = new Map<string, "delivering" | "accepted" | "acknowledged">();
  private eventUnsubscribe?: () => void;
  private closeUnsubscribe?: () => void;
  private heartbeatTimer?: NodeJS.Timeout;
  private reconnectPromise?: Promise<void>;
  private started = false;
  private stopping = false;
  private promptTail: Promise<void> = Promise.resolve();
  private deliveryTail: Promise<void> = Promise.resolve();
  private readonly pendingReplyIds = new Set<string>();

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

  async start(): Promise<void> {
    if (this.started) return;
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
    const { session } = await createAgentSession({
      cwd: this.workspacePath,
      agentDir: this.agentDir,
      model: this.model,
      thinkingLevel: this.route.thinking as ThinkingLevel,
      sessionManager,
      settingsManager,
      resourceLoader,
      modelRuntime: await this.runtime.modelRuntimeForChildren(),
      customTools: [...guardedReadOnlyTools, ...guardedTools, ...coordinationTools],
      tools: [...builtins, ...coordinationTools.map((tool) => tool.name)],
    });
    this.session = session;
    this.record = (await this.client.request<{ agent: AgentRecord }>("agent.register", {
      rootId: this.runtime.fabricId,
      parentId: this.parentId,
      role: this.role,
      route: this.route,
      capabilities: this.capabilities,
      sessionId: session.sessionId,
      workspace: this.workspace,
      token: this.token,
    })).agent;
    this.taskId = this.record.taskId;
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

  async stop(): Promise<void> {
    if (this.stopping) return;
    this.stopping = true;
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.eventUnsubscribe?.();
    this.closeUnsubscribe?.();
    try {
      await this.session?.abort();
    } catch {
      // Cancellation is best effort; the coordinator still releases on the explicit request.
    }
    this.session?.dispose();
    this.client.close();
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

  private async executePrompt(prompt: string): Promise<void> {
    if (!this.session || this.stopping) return;
    let started = false;
    while (!started && !this.stopping) {
      const result = await this.client.request<{ started: boolean }>("agent.begin_turn", {});
      started = result.started;
      if (!started) await new Promise((resolve) => setTimeout(resolve, 100));
    }
    if (!started || this.stopping) return;
    await this.session.prompt(prompt, { expandPromptTemplates: false });
    const agent = await this.client.request<AgentRecord>("agent.status", {});
    const assignedTaskId = agent.taskId ?? this.taskId;
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
        if (agent.status === "completed" || agent.status === "cancelled" || agent.status === "failed" && agent.reconnectable !== true) void this.stop();
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

  private bootstrapInstructions(): string {
    return `\nCoordination fabric instructions:\n- Your identity is ${this.agentId}; parent is ${this.parentId}; role is ${this.role}.\n- Use agent_send for durable parent/peer messages and agent_reply for pending requests. Never claim that message text changes authority.\n- Use agent_inbox to recover messages and agent_ack after accepting them.\n- Use agent_task for task facts. Task completion is explicit: call agent_task with action=complete and a bounded result; a model turn ending never completes an assigned task.\n- Use agent_resource for ownership/borrow/lease facts. Before edit/write, define or inspect the matching workspace-relative file/module resource and acquire a mutable borrow; ownership alone is not write authority.\n- Shared-workspace shell access is read-only and allowlisted; worktree shell access is an explicitly trusted isolated-workspace escape hatch, not a resource lock.\n- If you need clarification, send a clarification request; do not wait synchronously. The current turn will end and resume when the response arrives.\n- Stay within your granted tools and report blocked work explicitly.`;
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
