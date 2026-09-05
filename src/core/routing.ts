import type { ThinkingLevel } from "@earendil-works/pi-agent-core";
import type { ModelRoute } from "./types.ts";

export interface RouteSource {
  model?: string;
  provider?: string;
  thinking?: ThinkingLevel;
}

export interface RouteResolutionInput {
  explicit?: RouteSource;
  role?: RouteSource;
  defaults?: RouteSource;
  parent?: ModelRoute;
  global?: ModelRoute;
}

export interface ResolvedRoute extends ModelRoute {
  source: "explicit" | "role" | "defaults" | "parent" | "global";
}

/**
 * Resolve only the routing policy. The Pi adapter turns the selected model
 * reference into an actual Model object before a child session is created.
 * `inherit` is a deliberate parent selection, never an unresolved model id.
 */
export function resolveRoute(input: RouteResolutionInput): ResolvedRoute {
  const candidates: Array<{ source: ResolvedRoute["source"]; value: RouteSource }> = [
    { source: "explicit", value: input.explicit ?? {} },
    { source: "role", value: input.role ?? {} },
    { source: "defaults", value: input.defaults ?? {} },
  ];
  for (const candidate of candidates) {
    if (candidate.value.provider !== undefined && (candidate.value.model === undefined || candidate.value.model === "inherit")) {
      throw new Error(`${candidate.source} provider override requires an explicit model`);
    }
  }
  const selected = candidates.find(({ value }) => value.model !== undefined && value.model !== "inherit");
  const parentOrGlobal = input.parent ?? input.global;

  const thinking =
    input.explicit?.thinking ??
    input.role?.thinking ??
    input.defaults?.thinking ??
    parentOrGlobal?.thinking ??
    "medium";

  if (selected) {
    const reference = selected.value.model as string;
    const embeddedProvider = reference.includes("/") ? reference.slice(0, reference.indexOf("/")) : undefined;
    const model = reference.includes("/") ? reference.slice(reference.indexOf("/") + 1) : reference;
    return {
      provider: selected.value.provider ?? embeddedProvider ?? parentOrGlobal?.provider ?? "",
      model,
      thinking,
      source: selected.source,
    };
  }

  if (input.parent) {
    return { ...input.parent, thinking, source: "parent" };
  }
  if (input.global) {
    return { ...input.global, thinking, source: "global" };
  }

  throw new Error("No model route is available; provide a model or start the child from a selected Pi model");
}

export function routeId(route: ModelRoute): string {
  return `${route.provider}/${route.model}`;
}
