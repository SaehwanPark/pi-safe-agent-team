import type { Model } from "@earendil-works/pi-ai";
import type { ThinkingLevel } from "@earendil-works/pi-agent-core";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { FabricError } from "../core/errors.ts";
import { resolveRoute, type RouteResolutionInput } from "../core/routing.ts";
import type { ModelRoute } from "../core/types.ts";

export interface ResolvedModelRoute {
  route: ModelRoute;
  model: Model<any>;
  source: ReturnType<typeof resolveRoute>["source"];
}

export function routeFromModel(model: Model<any>, thinking: ThinkingLevel): ModelRoute {
  return { provider: model.provider, model: model.id, thinking };
}

/** Resolve a route against a specific registry, never against mutable global defaults. */
export function resolveModelRoute(input: RouteResolutionInput & { registry: ModelRegistry }): ResolvedModelRoute {
  let resolved: ReturnType<typeof resolveRoute>;
  try {
    resolved = resolveRoute(input);
  } catch (error) {
    throw new FabricError("MODEL_ROUTE_INVALID", error instanceof Error ? error.message : String(error));
  }
  const provider = resolved.provider;
  if (!provider) throw new FabricError("MODEL_ROUTE_INVALID", `Model route ${resolved.model} has no provider; pass provider/model or inherit a selected parent model`);
  const model = input.registry.find(provider, resolved.model);
  if (!model) throw new FabricError("MODEL_NOT_FOUND", `Model ${provider}/${resolved.model} is not available in this Pi registry`, { provider, model: resolved.model });
  if (model.provider !== provider || model.id !== resolved.model) {
    throw new FabricError("MODEL_ROUTE_INVALID", `Registry returned a different model for ${provider}/${resolved.model}`);
  }
  if (resolved.thinking !== "off" && !model.reasoning) {
    throw new FabricError("MODEL_ROUTE_INVALID", `Model ${provider}/${resolved.model} does not support thinking level ${resolved.thinking}`);
  }
  if (resolved.thinking !== "off" && model.thinkingLevelMap?.[resolved.thinking] === null) {
    throw new FabricError("MODEL_ROUTE_INVALID", `Model ${provider}/${resolved.model} does not support thinking level ${resolved.thinking}`);
  }
  return { route: { provider, model: resolved.model, thinking: resolved.thinking }, model, source: resolved.source };
}

export function resolveChildModel(
  registry: ModelRegistry,
  explicit: { provider?: string; model?: string; thinking?: ThinkingLevel } | undefined,
  role: { provider?: string; model?: string; thinking?: ThinkingLevel } | undefined,
  defaults: { provider?: string; model?: string; thinking?: ThinkingLevel } | undefined,
  parentModel: Model<any> | undefined,
  parentThinking: ThinkingLevel,
): ResolvedModelRoute {
  const parent = parentModel ? routeFromModel(parentModel, parentThinking) : undefined;
  return resolveModelRoute({ registry, explicit, role, defaults, parent });
}
