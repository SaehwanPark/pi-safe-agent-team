import net from "node:net";
import { randomUUID } from "node:crypto";
import { FabricError } from "../core/errors.ts";
import { PROTOCOL_VERSION, type EventFrame, type HelloFrame, type RequestFrame, type ResponseFrame } from "./server.ts";

export interface BrokerClientOptions {
  endpoint: string;
  agentId: string;
  token?: string;
  requestTimeoutMs?: number;
}

type EventListener = (event: EventFrame) => void;
type CloseListener = () => void;

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
  timer: NodeJS.Timeout;
}

export class BrokerClient {
  readonly endpoint: string;
  readonly requestTimeoutMs: number;
  private agentId: string;
  private token?: string;
  private socket?: net.Socket;
  private buffer = "";
  private connecting?: Promise<void>;
  private pending = new Map<string, PendingRequest>();
  private listeners = new Set<EventListener>();
  private closeListeners = new Set<CloseListener>();
  private closedExplicitly = false;

  constructor(options: BrokerClientOptions) {
    this.endpoint = options.endpoint;
    this.agentId = options.agentId;
    this.token = options.token;
    this.requestTimeoutMs = options.requestTimeoutMs ?? 60_000;
  }

  get identity(): { agentId: string; token?: string } {
    return { agentId: this.agentId, token: this.token };
  }

  setIdentity(agentId: string, token?: string): void {
    this.agentId = agentId;
    this.token = token;
  }

  async connect(): Promise<void> {
    if (this.socket && !this.socket.destroyed) return;
    if (this.connecting) return this.connecting;
    this.closedExplicitly = false;
    this.connecting = this.openConnection();
    try {
      await this.connecting;
    } finally {
      this.connecting = undefined;
    }
  }

  async request<T = unknown>(op: string, args: Record<string, unknown> = {}): Promise<T> {
    await this.connect();
    const socket = this.socket;
    if (!socket || socket.destroyed) throw new FabricError("BROKER_UNAVAILABLE", "Broker connection is not available");
    const id = `request-${randomUUID()}`;
    const frame: RequestFrame = { id, version: PROTOCOL_VERSION, op, args };
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new FabricError("BROKER_UNAVAILABLE", `Broker request ${op} timed out`));
      }, this.requestTimeoutMs);
      this.pending.set(id, { resolve: resolve as (value: unknown) => void, reject, timer });
      try {
        socket.write(`${JSON.stringify(frame)}\n`);
      } catch (error) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(new FabricError("BROKER_UNAVAILABLE", error instanceof Error ? error.message : String(error)));
      }
    });
  }

  onEvent(listener: EventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  onClose(listener: CloseListener): () => void {
    this.closeListeners.add(listener);
    return () => this.closeListeners.delete(listener);
  }

  async reconnect(): Promise<void> {
    this.close();
    await this.connect();
  }

  close(): void {
    this.closedExplicitly = true;
    const socket = this.socket;
    this.socket = undefined;
    if (socket && !socket.destroyed) socket.end();
    for (const [id, pending] of this.pending) {
      clearTimeout(pending.timer);
      pending.reject(new FabricError("BROKER_UNAVAILABLE", "Broker connection closed"));
      this.pending.delete(id);
    }
  }

  private async openConnection(): Promise<void> {
    this.buffer = "";
    const socket = net.createConnection(this.endpoint);
    socket.setEncoding("utf8");
    socket.setNoDelay(true);
    this.socket = socket;
    const hello = new Promise<void>((resolve, reject) => {
      let settled = false;
      const finishError = (error: unknown): void => {
        if (settled) return;
        settled = true;
        reject(error instanceof FabricError ? error : new FabricError("BROKER_UNAVAILABLE", error instanceof Error ? error.message : String(error)));
      };
      const finishOk = (): void => {
        if (settled) return;
        settled = true;
        resolve();
      };
      socket.once("connect", () => {
        const frame: HelloFrame = { kind: "hello", version: PROTOCOL_VERSION, agentId: this.agentId, token: this.token };
        socket.write(`${JSON.stringify(frame)}\n`);
      });
      socket.once("error", finishError);
      socket.on("close", () => {
        if (!settled) finishError(new FabricError("BROKER_UNAVAILABLE", "Broker closed during handshake"));
        this.handleClose(socket);
      });
      socket.on("data", (chunk: string) => {
        this.buffer += chunk;
        if (this.buffer.length > 2 * 1024 * 1024) {
          finishError(new FabricError("BROKER_UNAVAILABLE", "Broker frame is too large"));
          socket.destroy();
          return;
        }
        let newline = this.buffer.indexOf("\n");
        while (newline >= 0) {
          const raw = this.buffer.slice(0, newline);
          this.buffer = this.buffer.slice(newline + 1);
          if (raw.trim()) {
            let frame: unknown;
            try {
              frame = JSON.parse(raw);
            } catch (error) {
              finishError(new FabricError("BROKER_UNAVAILABLE", `Malformed broker response: ${error instanceof Error ? error.message : String(error)}`));
              newline = this.buffer.indexOf("\n");
              continue;
            }
            if ((frame as { kind?: string }).kind === "hello") {
              const helloResponse = frame as { ok?: boolean; error?: { code: string; message: string; details?: Record<string, unknown> } };
              if (helloResponse.ok) finishOk();
              else finishError(new FabricError((helloResponse.error?.code as never) ?? "IDENTITY_CONFLICT", helloResponse.error?.message ?? "Broker rejected hello", helloResponse.error?.details));
            } else {
              this.handleFrame(frame as ResponseFrame | EventFrame);
            }
          }
          newline = this.buffer.indexOf("\n");
        }
      });
    });
    try {
      await hello;
    } catch (error) {
      if (this.socket === socket) this.socket = undefined;
      socket.destroy();
      throw error;
    }
  }

  private handleFrame(frame: ResponseFrame | EventFrame): void {
    if ((frame as EventFrame).kind === "event") {
      for (const listener of this.listeners) {
        try {
          listener(frame as EventFrame);
        } catch {
          // A notification consumer cannot break the transport parser.
        }
      }
      return;
    }
    const response = frame as ResponseFrame;
    if (!response.id) return;
    const pending = this.pending.get(response.id);
    if (!pending) return;
    this.pending.delete(response.id);
    clearTimeout(pending.timer);
    if (response.ok) pending.resolve(response.result);
    else {
      const error = response.error;
      pending.reject(new FabricError((error?.code as never) ?? "BROKER_UNAVAILABLE", error?.message ?? "Broker request failed", error?.details));
    }
  }

  private handleClose(socket: net.Socket): void {
    if (this.socket === socket) this.socket = undefined;
    if (this.closedExplicitly) return;
    for (const [id, pending] of this.pending) {
      clearTimeout(pending.timer);
      pending.reject(new FabricError("BROKER_UNAVAILABLE", "Broker connection closed"));
      this.pending.delete(id);
    }
    for (const listener of this.closeListeners) {
      try {
        listener();
      } catch {
        // Reconnect observers cannot break client cleanup.
      }
    }
  }
}
