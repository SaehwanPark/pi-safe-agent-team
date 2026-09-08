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
  if (!name || typeof name !== "string") {
    throw new Error("Invalid provider name for interop registration");
  }
  const registry = getInteropRegistry();
  const existing = registry.providers.get(name);
  if (existing !== undefined) {
    if (existing === provider) return;
    throw new Error(`Interop provider '${name}' is already registered with a different instance`);
  }
  registry.providers.set(name, provider);
}

export function unregisterInteropProvider(name: string, provider?: unknown): void {
  if (!name) return;
  const registry = getInteropRegistry();
  if (provider !== undefined) {
    const existing = registry.providers.get(name);
    if (existing !== provider) return;
  }
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

export interface FabricTaskSnapshot {
  id: string;
  status: string;
  owner?: string;
  description?: string;
}

export interface FabricResourceSnapshot {
  id: string;
  path?: string;
  holder?: string;
}

export interface FabricStateSnapshotV1 {
  version: 1;
  active: boolean;
  quiescent: boolean;
  state: "known" | "uncertain";
  sessionReplacementSafe: boolean;
  capturedAt: number;
  rootSessionId?: string;
  cwd?: string;

  runningChildren: number;
  unresolvedChildTasks: number;
  mutableHolds: number;
  activeWriteFences: number;
  pendingRootRequests: number;
  pendingRootDeliveries: number;

  activeTasks: FabricTaskSnapshot[];
  mutableResources: FabricResourceSnapshot[];
  quiescenceReasons: string[];
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
  reason?: string;
  customInstructions?: string;
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
  /** Stop context behavior while retaining manager-owned recovery artifacts. */
  deactivate?(): void;
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
