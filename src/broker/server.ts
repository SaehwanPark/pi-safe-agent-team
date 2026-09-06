import net from "node:net";
import { createHash } from "node:crypto";
import { existsSync, unlinkSync, writeFileSync, promises as fs } from "node:fs";
import { dirname, join } from "node:path";
import { platform } from "node:os";
import { FabricError, asFabricError } from "../core/errors.ts";
import { Coordinator } from "../core/coordinator.ts";
import type { CoordinatorEvent, FabricConfig, PersistedCoordinatorState } from "../core/types.ts";
import { Journal } from "./journal.ts";

export const PROTOCOL_VERSION = 1;

export interface BrokerServerOptions {
  directory: string;
  rootId: string;
  rootAgentId?: string;
  config?: Partial<FabricConfig>;
  endpoint?: string;
  coordinator?: Coordinator;
  journal?: Journal;
  maintenanceMs?: number;
}

export interface HelloFrame {
  kind: "hello";
  version: number;
  agentId: string;
  token?: string;
}

export interface RequestFrame {
  id: string;
  version: number;
  op: string;
  args?: Record<string, unknown>;
}

export interface ResponseFrame {
  id: string;
  version: number;
  ok: boolean;
  result?: unknown;
  error?: ReturnType<FabricError["toJSON"]>;
}

export interface EventFrame {
  kind: "event";
  version: number;
  event: CoordinatorEvent["type"];
  data: unknown;
}

export function defaultEndpoint(directory: string): string {
  if (platform() !== "win32") return join(directory, "broker.sock");
  const suffix = createHash("sha256").update(directory).digest("hex").slice(0, 40);
  return `\\\\.\\pipe\\pi-safe-agents-${suffix}`;
}

class BrokerConnection {
  readonly socket: net.Socket;
  actorId?: string;
  ready = false;
  private buffer = "";
  private readonly onFrame: (connection: BrokerConnection, frame: unknown) => void;
  private readonly onClose: (connection: BrokerConnection) => void;

  constructor(socket: net.Socket, onFrame: (connection: BrokerConnection, frame: unknown) => void, onClose: (connection: BrokerConnection) => void) {
    this.socket = socket;
    this.onFrame = onFrame;
    this.onClose = onClose;
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => this.receive(chunk));
    socket.on("error", () => undefined);
    socket.on("close", () => this.onClose(this));
  }

  send(frame: unknown): void {
    if (this.socket.destroyed) return;
    try {
      this.socket.write(`${JSON.stringify(frame)}\n`);
    } catch {
      // The durable mailbox/journal is authoritative when a connection closes.
    }
  }

  close(): void {
    this.socket.end();
  }

  private receive(chunk: string): void {
    this.buffer += chunk;
    if (this.buffer.length > 2 * 1024 * 1024) {
      this.send({ kind: "error", version: PROTOCOL_VERSION, error: { code: "INVALID_ARGUMENT", message: "frame is too large" } });
      this.socket.destroy();
      return;
    }
    let newline = this.buffer.indexOf("\n");
    while (newline >= 0) {
      const raw = this.buffer.slice(0, newline);
      this.buffer = this.buffer.slice(newline + 1);
      if (raw.trim()) {
        try {
          this.onFrame(this, JSON.parse(raw));
        } catch (error) {
          this.send({ kind: "error", version: PROTOCOL_VERSION, error: asFabricError(error).toJSON() });
        }
      }
      newline = this.buffer.indexOf("\n");
    }
  }
}

/** One local authoritative broker for a fabric. */
/**
 * Resolve the broker's FabricConfig, auto-detecting policy-key case folding
 * when unset. The probe runs against the broker state directory, which lives
 * on the same volume policy decisions protect; an explicit config value
 * always wins.
 */
export function resolveBrokerConfig(options: BrokerServerOptions): Partial<FabricConfig> {
  const config: Partial<FabricConfig> = { ...(options.config ?? {}) };
  if (config.caseInsensitivePaths === undefined) config.caseInsensitivePaths = detectCaseInsensitivePaths(options.directory);
  return config;
}

/**
 * Write a mixed-case probe file and check whether a differently-cased name
 * resolves to it. Returns false when the probe cannot run (fail open to the
 * non-folding, purely lexical policy keys).
 */
export function detectCaseInsensitivePaths(directory: string): boolean {
  if (process.platform === "win32") return true;
  const base = join(directory, `.pi-case-${process.pid}-${Math.random().toString(36).slice(2, 8)}`);
  const written = `${base}.MiXeD`;
  const probed = `${base}.UPPER`;
  try {
    writeFileSync(written, "");
    return existsSync(probed);
  } catch {
    return false;
  } finally {
    try {
      unlinkSync(written);
    } catch {
      // The probe name is unique; a failed unlink must not mask the result.
    }
  }
}

export class BrokerServer {
  readonly directory: string;
  readonly endpoint: string;
  readonly coordinator: Coordinator;
  readonly journal: Journal;

  private readonly server = net.createServer((socket) => this.accept(socket));
  private readonly connections = new Set<BrokerConnection>();
  private readonly requestCache = new Map<string, ResponseFrame>();
  private readonly inFlightRequests = new Map<string, Promise<ResponseFrame>>();
  private readonly requestOrder: string[] = [];
  private operationTail: Promise<void> = Promise.resolve();
  private readonly maintenanceMs: number;
  private maintenanceTimer?: NodeJS.Timeout;
  private lockHandle?: fs.FileHandle;
  private started = false;
  private stopping = false;

  constructor(options: BrokerServerOptions) {
    this.directory = options.directory;
    this.endpoint = options.endpoint ?? defaultEndpoint(options.directory);
    this.coordinator = options.coordinator ?? new Coordinator({ rootId: options.rootId, rootAgentId: options.rootAgentId, config: resolveBrokerConfig(options) });
    this.journal = options.journal ?? new Journal({ directory: options.directory });
    this.maintenanceMs = Math.max(1000, options.maintenanceMs ?? this.coordinator.config.heartbeatMs);
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.requestCache.clear();
    this.inFlightRequests.clear();
    this.requestOrder.length = 0;
    await this.acquireLock();
    try {
      await this.journal.replay(this.coordinator);
      const recovery = this.coordinator.recover();
      if (recovery.events.length > 0) await this.journal.append(recovery.events);
      if (platform() !== "win32") {
        try {
          await fs.unlink(this.endpoint);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        }
      }
      await new Promise<void>((resolve, reject) => {
        const onError = (error: Error): void => {
          this.server.off("listening", onListening);
          reject(error);
        };
        const onListening = (): void => {
          this.server.off("error", onError);
          resolve();
        };
        this.server.once("error", onError);
        this.server.once("listening", onListening);
        this.server.listen(this.endpoint);
      });
      if (platform() !== "win32") await fs.chmod(this.endpoint, 0o600).catch(() => undefined);
      this.stopping = false;
      this.started = true;
      this.maintenanceTimer = setInterval(() => {
        this.enqueueMaintenance();
      }, this.maintenanceMs);
      this.maintenanceTimer.unref();
    } catch (error) {
      await this.releaseLock();
      throw error;
    }
  }

  async stop(): Promise<void> {
    if (!this.started) {
      await this.releaseLock();
      return;
    }
    this.stopping = true;
    this.started = false;
    if (this.maintenanceTimer) clearInterval(this.maintenanceTimer);
    await this.operationTail.catch(() => undefined);
    for (const connection of this.connections) connection.close();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
    if (platform() !== "win32") {
      try {
        await fs.unlink(this.endpoint);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
    }
    await this.releaseLock();
  }

  isStarted(): boolean {
    return this.started;
  }

  private accept(socket: net.Socket): void {
    const connection = new BrokerConnection(socket, (current, frame) => this.handleFrame(current, frame), (current) => this.connections.delete(current));
    this.connections.add(connection);
  }

  private handleFrame(connection: BrokerConnection, frame: unknown): void {
    if (!connection.ready) {
      this.handleHello(connection, frame);
      return;
    }
    if (this.stopping) {
      connection.send({ kind: "error", version: PROTOCOL_VERSION, error: new FabricError("BROKER_UNAVAILABLE", "Broker is stopping").toJSON() });
      return;
    }
    const request = frame && typeof frame === "object" && !Array.isArray(frame) ? frame as Partial<RequestFrame> : {};
    if (typeof request.id !== "string" || request.id.length === 0 || request.id.length > 512 || request.id.includes("\u0000") || typeof request.op !== "string" || request.op.length === 0 || request.op.length > 128 || request.op.includes("\u0000") || request.version !== PROTOCOL_VERSION) {
      connection.send({ kind: "error", version: PROTOCOL_VERSION, error: new FabricError("PROTOCOL_VERSION_UNSUPPORTED", "request must include protocol version 1, bounded id/op, and op").toJSON() });
      return;
    }
    const key = `${connection.actorId}\u0000${request.id}`;
    const cached = this.requestCache.get(key);
    if (cached) {
      connection.send(cached);
      return;
    }
    const inFlight = this.inFlightRequests.get(key);
    if (inFlight) {
      void inFlight.then((response) => connection.send(response), () => undefined);
      return;
    }
    const normalized: RequestFrame = { id: request.id, version: request.version, op: request.op, args: request.args ?? {} };
    const flight = this.operationTail.then(() => this.handleRequest(connection, normalized, key));
    this.inFlightRequests.set(key, flight);
    this.operationTail = flight.then(() => undefined, () => undefined);
    void flight.then((response) => connection.send(response), () => undefined).finally(() => {
      if (this.inFlightRequests.get(key) === flight) this.inFlightRequests.delete(key);
    });
  }

  private handleHello(connection: BrokerConnection, frame: unknown): void {
    const hello = frame && typeof frame === "object" && !Array.isArray(frame) ? frame as Partial<HelloFrame> : {};
    if (hello.kind !== "hello" || hello.version !== PROTOCOL_VERSION || typeof hello.agentId !== "string" || hello.agentId.length === 0 || hello.agentId.length > 512 || hello.agentId.includes("\u0000") || (hello.token !== undefined && (typeof hello.token !== "string" || hello.token.length > 512 || hello.token.includes("\u0000")))) {
      connection.send({ kind: "hello", version: PROTOCOL_VERSION, ok: false, error: new FabricError("PROTOCOL_VERSION_UNSUPPORTED", "expected a version 1 hello frame").toJSON() });
      connection.close();
      return;
    }
    if (!this.coordinator.authenticate(hello.agentId, hello.token)) {
      connection.send({ kind: "hello", version: PROTOCOL_VERSION, ok: false, error: new FabricError("IDENTITY_CONFLICT", "invalid agent reconnect credential").toJSON() });
      connection.close();
      return;
    }
    connection.actorId = hello.agentId;
    connection.ready = true;
    connection.send({ kind: "hello", version: PROTOCOL_VERSION, ok: true });
  }

  private async handleRequest(connection: BrokerConnection, request: RequestFrame, cacheKey: string): Promise<ResponseFrame> {
    const actorId = connection.actorId;
    if (!actorId) {
      return { id: request.id, version: PROTOCOL_VERSION, ok: false, error: new FabricError("IDENTITY_CONFLICT", "connection is not authenticated").toJSON() };
    }
    const before = this.coordinator.exportState();
    let response: ResponseFrame;
    try {
      if (request.args !== undefined && (!request.args || typeof request.args !== "object" || Array.isArray(request.args))) {
        throw new FabricError("INVALID_ARGUMENT", "request args must be an object");
      }
      if (request.op === "agent.register" && request.args?.parentId && !this.coordinator.getAgent(actorId)) {
        throw new FabricError("IDENTITY_CONFLICT", "child registration requires a coordinator-issued identity and reconnect credential");
      }
      const result = this.coordinator.dispatch(actorId, request.op, request.args ?? {});
      if (result.events.length > 0 || result.idempotency !== undefined) {
        await this.journal.append(result.events, result.idempotency);
      }
      response = { id: request.id, version: PROTOCOL_VERSION, ok: true, result: result.value };
      this.broadcast(result.events);
    } catch (error) {
      this.coordinator.restoreState(before);
      response = { id: request.id, version: PROTOCOL_VERSION, ok: false, error: asFabricError(error, "PERSISTENCE_FAILURE").toJSON() };
    }
    this.cacheResponse(cacheKey, response);
    return response;
  }

  private enqueueMaintenance(): void {
    this.operationTail = this.operationTail.then(() => this.runMaintenance()).catch(() => undefined);
  }

  private async runMaintenance(): Promise<void> {
    if (!this.started) return;
    const before = this.coordinator.exportState();
    try {
      const result = this.coordinator.maintenance();
      if (result.events.length > 0) {
        await this.journal.append(result.events);
        this.broadcast(result.events);
      }
    } catch {
      this.coordinator.restoreState(before);
    }
  }

  private broadcast(events: readonly CoordinatorEvent[]): void {
    for (const event of events) {
      for (const connection of this.connections) {
        if (!connection.ready || !connection.actorId || !this.eventVisibleTo(connection.actorId, event)) continue;
        connection.send({ kind: "event", version: PROTOCOL_VERSION, event: event.type, data: this.publicEvent(event) } satisfies EventFrame);
      }
    }
  }

  private eventVisibleTo(actorId: string, event: CoordinatorEvent): boolean {
    const observer = this.coordinator.getAgent(actorId);
    if (!observer) return false;
    switch (event.type) {
      case "message_sent":
        return event.message.to === actorId || event.message.from === actorId || observer.depth === 0;
      case "message_acknowledged":
        return event.message.to === actorId || event.message.from === actorId || observer.depth === 0;
      case "messages_pruned":
        return observer.depth === 0;
      case "request_changed":
        return event.request.from === actorId || event.request.to === actorId || observer.depth === 0;
      case "resource_changed":
        return observer.depth === 0 || event.resource.owner === actorId || Object.prototype.hasOwnProperty.call(event.resource.grants, actorId) || event.resource.sharedHolds.some((hold) => hold.agentId === actorId) || event.resource.mutableHold?.agentId === actorId || event.resource.waiters.some((waiter) => waiter.agentId === actorId);
      case "task_changed":
        return observer.depth === 0 || event.task.creator === actorId || event.task.owner === actorId || event.task.owner === observer.parentId;
      case "agent_registered":
      case "agent_updated":
      case "agent_terminal":
        return observer.depth === 0 || event.agent.id === actorId || event.agent.parentId === actorId || observer.parentId === event.agent.id;
      case "slot_available":
        return event.agentId === actorId;
      case "diagnostic":
        return observer.depth === 0;
    }
  }

  private publicEvent(event: CoordinatorEvent): unknown {
    const cloned = JSON.parse(JSON.stringify(event)) as Record<string, unknown>;
    if (cloned.agent && typeof cloned.agent === "object") delete (cloned.agent as Record<string, unknown>).authToken;
    return cloned;
  }

  private cacheResponse(key: string, response: ResponseFrame): void {
    this.requestCache.set(key, response);
    this.requestOrder.push(key);
    while (this.requestOrder.length > 512) {
      const oldest = this.requestOrder.shift();
      if (oldest) this.requestCache.delete(oldest);
    }
  }

  private async acquireLock(): Promise<void> {
    await fs.mkdir(dirname(join(this.directory, "broker.lock")), { recursive: true });
    const lockPath = join(this.directory, "broker.lock");
    try {
      this.lockHandle = await fs.open(lockPath, "wx");
      await this.lockHandle.writeFile(JSON.stringify({ pid: process.pid, endpoint: this.endpoint, version: PROTOCOL_VERSION }));
      await this.lockHandle.sync();
      await fs.chmod(lockPath, 0o600).catch(() => undefined);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      let stale = false;
      try {
        const lock = JSON.parse(await fs.readFile(lockPath, "utf8")) as { pid?: number };
        if (!lock.pid) stale = true;
        else {
          try {
            process.kill(lock.pid, 0);
          } catch (error) {
            const code = (error as NodeJS.ErrnoException).code;
            stale = code === "ESRCH" || code === "ENOENT";
          }
        }
      } catch {
        stale = true;
      }
      if (!stale) throw new FabricError("BROKER_UNAVAILABLE", `A broker already owns ${this.directory}`);
      await fs.unlink(lockPath).catch(() => undefined);
      this.lockHandle = await fs.open(lockPath, "wx");
      await this.lockHandle.writeFile(JSON.stringify({ pid: process.pid, endpoint: this.endpoint, version: PROTOCOL_VERSION }));
      await this.lockHandle.sync();
      await fs.chmod(lockPath, 0o600).catch(() => undefined);
    }
  }

  private async releaseLock(): Promise<void> {
    const lockPath = join(this.directory, "broker.lock");
    if (!this.lockHandle) return;
    await this.lockHandle.close().catch(() => undefined);
    this.lockHandle = undefined;
    await fs.unlink(lockPath).catch(() => undefined);
  }
}

export async function startBroker(options: BrokerServerOptions): Promise<BrokerServer> {
  const server = new BrokerServer(options);
  await server.start();
  return server;
}
