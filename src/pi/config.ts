import { existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import type { FabricConfig } from "../core/types.ts";

/** Runtime configuration files consulted by the installed extension. */
export interface FabricConfigLoadOptions {
  cwd: string;
  agentDir: string;
  env?: NodeJS.ProcessEnv;
}

export interface FabricConfigLoadResult {
  config: Partial<FabricConfig>;
  source?: string;
  errors: string[];
}

const CONFIG_KEYS = new Set<keyof FabricConfig>([
  "caseInsensitivePaths",
  "maxDepth",
  "maxChildrenPerAgent",
  "maxChildrenCreatedPerAgent",
  "maxTotalAgents",
  "maxConcurrentAgents",
  "maxMailboxMessages",
  "maxMessageBody",
  "maxTaskOutput",
  "leaseMs",
  "heartbeatMs",
  "messageRetention",
  "modelRouteCapacity",
  "modelRoutePolicies",
  "modelRouteCapacities",
  "effectivePrefillBudgets",
]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function extractConfig(value: unknown): Partial<FabricConfig> | undefined {
  if (!isRecord(value)) return undefined;
  const nested = value.safeAgents ?? value.piSafeAgents ?? value.fabric;
  const source = isRecord(nested) ? nested : value;
  const config: Record<string, unknown> = {};
  for (const [key, candidate] of Object.entries(source)) {
    if (CONFIG_KEYS.has(key as keyof FabricConfig)) config[key] = candidate;
  }
  return config as Partial<FabricConfig>;
}

function readCandidate(path: string, errors: string[]): Partial<FabricConfig> | undefined {
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8")) as unknown;
    const config = extractConfig(parsed);
    if (!config) {
      errors.push(`Ignoring malformed safe-agents configuration in ${path}: expected a JSON object`);
      return undefined;
    }
    return config;
  } catch (error) {
    errors.push(`Ignoring safe-agents configuration in ${path}: ${error instanceof Error ? error.message : String(error)}`);
    return undefined;
  }
}

/**
 * Load the normal installed-extension configuration without requiring a
 * programmatic FabricRuntime constructor. Explicit options still override the
 * resulting map at runtime.
 *
 * Precedence is an explicit PI_SAFE_AGENTS_CONFIG path, then the project files
 * `.pi/safe-agents.json` and `.safe-agents.json`, then the user agent files
 * `<agentDir>/safe-agents/config.json` and `<agentDir>/safe-agents.json`.
 */
export function loadFabricConfig(options: FabricConfigLoadOptions): FabricConfigLoadResult {
  const env = options.env ?? process.env;
  const errors: string[] = [];
  const explicit = typeof env.PI_SAFE_AGENTS_CONFIG === "string" && env.PI_SAFE_AGENTS_CONFIG.trim()
    ? resolve(env.PI_SAFE_AGENTS_CONFIG.trim())
    : undefined;
  const candidates = explicit
    ? [explicit]
    : [
        join(options.cwd, ".pi", "safe-agents.json"),
        join(options.cwd, ".safe-agents.json"),
        join(options.agentDir, "safe-agents", "config.json"),
        join(options.agentDir, "safe-agents.json"),
      ];

  for (const path of candidates) {
    if (!existsSync(path)) continue;
    const config = readCandidate(path, errors);
    if (config) return { config, source: path, errors };
  }
  return { config: {}, errors };
}
