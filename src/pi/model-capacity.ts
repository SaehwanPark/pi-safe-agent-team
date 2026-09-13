import type { ModelRoute, FabricConfig } from "../core/types.ts";
import { modelRouteCapacity, modelRouteCapacityKey } from "../core/coordinator-wire.ts";

interface Waiter {
  resolve: (release: () => void) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}
/**
 * Process-local arbiter for expensive provider operations (including
 * compaction). The coordinator remains authoritative across processes; this
 * arbiter prevents two operations in one host from racing before their broker
 * turn state has changed.
 */
export class ModelRouteCapacityArbiter {
  private readonly config: FabricConfig;
  private readonly active = new Map<string, number>();
  private readonly waiters = new Map<string, Waiter[]>();
  private readonly limits = new Map<string, number | undefined>();
  private closed = false;

  constructor(config: FabricConfig) {
    this.config = config;
  }

  capacity(route: ModelRoute): number | undefined {
    return modelRouteCapacity(this.config, route);
  }

  activeCount(route: ModelRoute): number {
    return this.active.get(modelRouteCapacityKey(this.config, route)) ?? 0;
  }

  async acquire(route: ModelRoute, signal?: AbortSignal): Promise<() => void> {
    const key = modelRouteCapacityKey(this.config, route);
    const requestedLimit = this.capacity(route);
    // A physical backend may be referenced by several semantic aliases. Keep
    // the strictest configured limit observed for the group so an alias with
    // a larger value cannot widen a permit already constrained by another
    // route. An unlimited alias inherits a previously known group limit.
    const previousLimit = this.limits.get(key);
    const limit = previousLimit === undefined
      ? requestedLimit
      : requestedLimit === undefined
        ? previousLimit
        : Math.min(previousLimit, requestedLimit);
    this.limits.set(key, limit);
    if (limit === undefined) return () => undefined;
    if (this.closed) throw new Error("model route capacity arbiter is closed");
    if ((this.active.get(key) ?? 0) < limit) return this.grant(key);
    if (signal?.aborted) throw new Error("model route capacity wait was aborted");

    return new Promise<() => void>((resolve, reject) => {
      const waiter: Waiter = { resolve, reject, signal };
      waiter.onAbort = () => {
        const queue = this.waiters.get(key);
        if (queue) {
          const index = queue.indexOf(waiter);
          if (index >= 0) queue.splice(index, 1);
          if (queue.length === 0) this.waiters.delete(key);
        }
        reject(new Error("model route capacity wait was aborted"));
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      const queue = this.waiters.get(key) ?? [];
      queue.push(waiter);
      this.waiters.set(key, queue);
      this.pump(key);
    });
  }

  async withCapacity<T>(route: ModelRoute, operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    const release = await this.acquire(route, signal);
    try {
      return await operation();
    } finally {
      release();
    }
  }

  close(reason = "model route capacity arbiter closed"): void {
    this.closed = true;
    for (const queue of this.waiters.values()) {
      for (const waiter of queue) {
        if (waiter.signal && waiter.onAbort) waiter.signal.removeEventListener("abort", waiter.onAbort);
        waiter.reject(new Error(reason));
      }
    }
    this.waiters.clear();
  }

  private grant(key: string): () => void {
    this.active.set(key, (this.active.get(key) ?? 0) + 1);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = (this.active.get(key) ?? 1) - 1;
      if (next > 0) this.active.set(key, next);
      else this.active.delete(key);
      this.pump(key);
    };
  }

  private pump(key: string): void {
    if (this.closed) return;
    const queue = this.waiters.get(key);
    if (!queue || queue.length === 0) return;
    const limit = this.capacityFromKey(key);
    if (limit === undefined) {
      this.waiters.delete(key);
      for (const waiter of queue.splice(0)) {
        if (waiter.signal && waiter.onAbort) waiter.signal.removeEventListener("abort", waiter.onAbort);
        waiter.resolve(() => undefined);
      }
      return;
    }
    while (queue.length > 0 && (this.active.get(key) ?? 0) < limit) {
      const waiter = queue.shift() as Waiter;
      if (waiter.signal?.aborted) {
        waiter.reject(new Error("model route capacity wait was aborted"));
        continue;
      }
      if (waiter.signal && waiter.onAbort) waiter.signal.removeEventListener("abort", waiter.onAbort);
      waiter.resolve(this.grant(key));
    }
    if (queue.length === 0) this.waiters.delete(key);
  }

  private capacityFromKey(key: string): number | undefined {
    return this.limits.get(key);
  }
}
