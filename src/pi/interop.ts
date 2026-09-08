export const PI_EXTENSION_INTEROP = Symbol.for("pi.extension-interop.v1");

export interface PiExtensionInteropRegistryV1 {
  version: 1;
  providers: Map<string, unknown>;
}

export function getInteropRegistry(): PiExtensionInteropRegistryV1 {
  const globalObj = globalThis as Record<symbol, unknown>;
  let registry = globalObj[PI_EXTENSION_INTEROP] as PiExtensionInteropRegistryV1 | undefined;
  if (!registry || registry.version !== 1 || !(registry.providers instanceof Map)) {
    registry = { version: 1, providers: new Map() };
    globalObj[PI_EXTENSION_INTEROP] = registry;
  }
  return registry;
}

export function registerInteropProvider(name: string, provider: unknown): void {
  const registry = getInteropRegistry();
  registry.providers.set(name, provider);
}

export function unregisterInteropProvider(name: string): void {
  const registry = getInteropRegistry();
  registry.providers.delete(name);
}

export function getInteropProvider<T = unknown>(name: string): T | undefined {
  const registry = getInteropRegistry();
  return registry.providers.get(name) as T | undefined;
}

// safe-agent-team.fabric-state.v1
export interface FabricSnapshotRequest {
  cwd: string;
  sessionId?: string;
}

export interface FabricStateSnapshotV1 {
  active: boolean;
  quiescent: boolean;
  capturedAt: number;

  runningChildren: number;
  unresolvedChildTasks: number;
  mutableHolds: number;
  pendingRootRequests: number;
  pendingRootDeliveries: number;

  activeTasks: Array<{
    id: string;
    status: string;
    owner?: string;
    description?: string;
  }>;

  mutableResources: Array<{
    id: string;
    path?: string;
    holder?: string;
  }>;
}

export interface FabricStateProviderV1 {
  getSnapshot(request: FabricSnapshotRequest): Promise<FabricStateSnapshotV1 | null>;
}

// local-context-manager.embedded-context.v1
export interface EmbeddedContextUsage {
  tokens: number | null;
  contextWindow: number | null;
  source: "reported" | "estimated";
}

export interface EmbeddedCompactionRequest {
  targetTokens?: number;
  reason?: string;
}

export interface EmbeddedContextSnapshot {
  tokens: number | null;
  contextWindow: number | null;
  tokenSource: "reported" | "estimated";
  thresholdRatio?: number;
  mode?: string;
  reductionRatio?: number;
}

export interface EmbeddedContextDiagnostic {
  message: string;
  level: "info" | "warning" | "error";
}

export interface EmbeddedContextHost {
  getContextUsage(): EmbeddedContextUsage | null;
  getContextEntries(): unknown[];
  compact(request: EmbeddedCompactionRequest): Promise<void>;
  onStatus?(snapshot: EmbeddedContextSnapshot): void;
  onDiagnostic?(diagnostic: EmbeddedContextDiagnostic): void;
}

export interface EmbeddedContextManagerOptions {
  config?: Record<string, unknown>;
  mode?: "root" | "managed-child";
  contextWindow?: number;
}

export interface ToolContentBlock {
  type: string;
  text?: string;
  [key: string]: unknown;
}

export interface EmbeddedToolResult {
  toolName: string;
  input: Record<string, unknown>;
  content: ReadonlyArray<ToolContentBlock>;
  details?: unknown;
  isError: boolean;
}

export interface EmbeddedContextManager {
  observeTurnStart(): void;
  observeTurnEnd(): void;
  observeSettled(): Promise<void>;
  transformToolResult(result: EmbeddedToolResult): Promise<EmbeddedToolResult>;
  snapshot(): EmbeddedContextSnapshot;
  dispose(): void;
}

export type EmbeddedContextProviderV1 =
  | ((host: EmbeddedContextHost, options?: EmbeddedContextManagerOptions) => EmbeddedContextManager)
  | {
      createEmbeddedContextManager?(host: EmbeddedContextHost, options?: EmbeddedContextManagerOptions): EmbeddedContextManager;
      createManager?(host: EmbeddedContextHost, options?: EmbeddedContextManagerOptions): EmbeddedContextManager;
    };

export function createEmbeddedContextController(
  host: EmbeddedContextHost,
  options?: EmbeddedContextManagerOptions,
): EmbeddedContextManager | undefined {
  const provider = getInteropProvider<EmbeddedContextProviderV1>("local-context-manager.embedded-context.v1");
  if (!provider) return undefined;
  try {
    if (typeof provider === "function") {
      return provider(host, options);
    }
    if (typeof provider.createEmbeddedContextManager === "function") {
      return provider.createEmbeddedContextManager(host, options);
    }
    if (typeof provider.createManager === "function") {
      return provider.createManager(host, options);
    }
  } catch {
    return undefined;
  }
  return undefined;
}
