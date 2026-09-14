export const PI_EXTENSION_INTEROP = Symbol.for("pi.extension-interop.v1");
export const LCM_EMBEDDED_CONTEXT_PROVIDER_NAME = "pi-local-context-manager.embedded-context.v1";
export const LEGACY_LCM_EMBEDDED_CONTEXT_PROVIDER_NAME = "local-context-manager.embedded-context.v1";

let privateRegistry: PiExtensionInteropRegistryV1 | undefined;

function createInteropRegistry(): PiExtensionInteropRegistryV1 {
  return { version: 1, providers: new Map() };
}

/**
 * Preserve a well-formed future registry published by another extension. A
 * private v1 table keeps safe-agent registration isolated without replacing
 * state that this build cannot interpret.
 */
function interopRegistry(): PiExtensionInteropRegistryV1 {
  const globalObj = globalThis as Record<symbol, unknown>;
  const published = globalObj[PI_EXTENSION_INTEROP] as PiExtensionInteropRegistryV1 | undefined;
  if (published === undefined || published === null) {
    const registry = createInteropRegistry();
    globalObj[PI_EXTENSION_INTEROP] = registry;
    return registry;
  }
  const version = typeof (published as any)?.version === "number" && Number.isFinite((published as any).version)
    ? (published as any).version as number
    : undefined;
  if (version !== undefined && version !== 1) {
    privateRegistry ??= createInteropRegistry();
    return privateRegistry;
  }
  if (version === 1 && (published as any).providers instanceof Map) return published;
  // Unversioned/malformed data has no compatible peer state worth preserving.
  const registry = createInteropRegistry();
  globalObj[PI_EXTENSION_INTEROP] = registry;
  return registry;
}

export interface PiExtensionInteropRegistryV1 {
  version: 1;
  providers: Map<string, unknown>;
}

export function getInteropRegistry(): PiExtensionInteropRegistryV1 {
  return interopRegistry();
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
  signal?: AbortSignal;
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
  /** Actors in the bounded broker-recovery window. */
  recoveringAgents: number;
  unresolvedChildTasks: number;
  /** Unfinished tasks whose owner has been released or was never assigned. */
  unownedUnresolvedTasks: number;
  mutableHolds: number;
  activeWriteFences: number;
  /** Durable restart quarantine for a write that may have crossed broker loss. */
  activeWriteQuarantines: number;
  pendingRootRequests: number;
  pendingRootDeliveries: number;
  /** True while Pi is performing manual/automatic root compaction. */
  rootCompactionInFlight?: boolean;
  /** Root provider health gate after an exhausted model/context outcome. */
  rootContextHealth?: "healthy" | "degraded";
  rootContextDiagnostic?: string;

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
  /** Logical model window, when an operational prefill budget is applied. */
  logicalContextWindow?: number | null;
  /** Hardware-safe operational prefill budget, if known. */
  effectiveContextBudget?: number | null;
  /** Compatibility alias used by newer local-context-manager hosts. */
  effectivePrefillBudget?: number | null;
  /** Provider integrations may use their own bounded provenance labels. */
  source: string;
}

export interface EmbeddedCompactionRequest {
  reason?: string;
  customInstructions?: string;
}

export interface EmbeddedContextSnapshot {
  tokens: number | null;
  contextWindow: number | null;
  logicalContextWindow?: number | null;
  effectiveContextBudget?: number | null;
  tokenSource: string;
  thresholdRatio?: number;
  mode?: string;
  reductionRatio?: number;
}

export interface EmbeddedContextDiagnostic {
  message: string;
  level: "info" | "warning" | "error";
  code?: string;
  details?: Record<string, unknown>;
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
  /** Advertised/logical model context window. */
  contextWindow?: number;
  logicalContextWindow?: number;
  /** Operational budget used by proactive context management. */
  effectiveContextBudget?: number;
  /** Alias accepted by newer LCM builds. */
  effectivePrefillBudget?: number;
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
  // Prefer LCM's canonical package-qualified provider name. Keep the legacy
  // alias for older companion versions so an upgrade remains non-breaking.
  const provider = getInteropProvider<EmbeddedContextProviderV1>(LCM_EMBEDDED_CONTEXT_PROVIDER_NAME)
    ?? getInteropProvider<EmbeddedContextProviderV1>(LEGACY_LCM_EMBEDDED_CONTEXT_PROVIDER_NAME);
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
