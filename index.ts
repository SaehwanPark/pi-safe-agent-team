import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Box, Text } from "@earendil-works/pi-tui";
import { FabricError, asFabricError } from "./src/core/errors.ts";
import type { AgentMessage, FabricStatus } from "./src/core/types.ts";
import { FabricRuntime } from "./src/pi/runtime.ts";
import { LifecycleQueue } from "./src/pi/lifecycle.ts";
import { createCoordinationTools } from "./src/pi/tools.ts";

export { Coordinator } from "./src/core/coordinator.ts";
export { FabricError } from "./src/core/errors.ts";
export { resolveRoute, routeId } from "./src/core/routing.ts";
export { BrokerClient } from "./src/broker/client.ts";
export { BrokerServer, startBroker } from "./src/broker/server.ts";
export { Journal } from "./src/broker/journal.ts";
export { FabricRuntime, ManagedChild, taskAwareTurnStatus } from "./src/pi/runtime.ts";
export { assertReadOnlyShellCommand, createGuardedChildTools, createGuardedReadOnlyTools, evaluateRootWriteGuard, workspaceRelativePath } from "./src/pi/guards.ts";
export { GitWorkspaceStrategy, SharedWorkspaceStrategy } from "./src/workspace.ts";
export * from "./src/core/types.ts";

export default function safeAgentsTeam(pi: ExtensionAPI): void {
  const runtime = new FabricRuntime();
  const lazyClient = {
    request: <T = unknown>(operation: string, args: Record<string, unknown> = {}) => runtime.request<T>(operation, args),
    requestIdempotent: <T = unknown>(operation: string, args: Record<string, unknown> = {}, operationId?: string, timeoutMs?: number) =>
      runtime.requestIdempotent<T>(operation, args, operationId, timeoutMs),
  };

  for (const tool of createCoordinationTools({
    client: lazyClient,
    spawn: (input, parentModel, parentThinking) => runtime.spawnFromRoot(input, parentModel, parentThinking),
  })) {
    pi.registerTool(tool);
  }

  pi.registerMessageRenderer("safe-agents.message", (message, { expanded, outputPad }, theme) => {
    const details = message.details as AgentMessage | undefined;
    const type = details?.type ?? "message";
    const prefix = theme.fg(type === "agent_failed" || type === "escalation" ? "error" : type === "clarification" || type === "decision_request" ? "warning" : "accent", `[${type}]`);
    const sender = details?.from ? theme.fg("dim", ` from ${details.from}`) : "";
    let text = `${prefix}${sender} ${message.content}`;
    if (expanded && details?.id) text += `\n${theme.fg("dim", `  id: ${details.id}`)}`;
    const box = new Box(outputPad, 1, (value) => theme.bg("customMessageBg", value));
    box.addChild(new Text(text, 0, 0));
    return box;
  });

  pi.registerMessageRenderer("safe-agents.status", (message, { outputPad }, theme) => {
    const box = new Box(outputPad, 1, (value) => theme.bg("customMessageBg", value));
    box.addChild(new Text(theme.fg("accent", typeof message.content === "string" ? message.content : JSON.stringify(message.content)), 0, 0));
    return box;
  });

  const rootDeliveryStates = new Map<string, "delivering" | "accepted" | "acknowledged">();
  let rootDeliveryTail: Promise<void> = Promise.resolve();
  const lifecycleQueue = new LifecycleQueue();

  const enqueueLifecycle = (generation: number, operation: () => Promise<void>): Promise<void> =>
    lifecycleQueue.enqueue(generation, operation);

  const notifyLifecycleFailure = (
    ctx: ExtensionContext,
    error: unknown,
    severity: "error" | "warning",
    generation?: number,
  ): void => {
    // A late host callback from an older generation is expected during shutdown
    // or reload; it must not surface as a failure in the next session.
    if (generation !== undefined && generation !== lifecycleQueue.currentGeneration) return;
    const fabricError = asFabricError(error);
    ctx.ui.notify(`safe-agents: ${fabricError.message}`, severity);
  };
  const rootDelivery = (api: ExtensionAPI) => (message: AgentMessage): void => {
    const state = rootDeliveryStates.get(message.id);
    if (state === "acknowledged" || state === "delivering") return;
    if (state === "accepted") {
      void runtime.request("message.ack", { messageId: message.id }).then(() => rememberRootMessage(message.id, "acknowledged")).catch(() => undefined);
      return;
    }
    rootDeliveryStates.set(message.id, "delivering");
    // Preserve broker order at the host boundary too. In particular, two
    // urgent messages must not race through api.sendMessage and reach the
    // root model in the reverse order.
    rootDeliveryTail = rootDeliveryTail.then(async () => {
      try {
        const content = `[${message.type} from ${message.from}]\n${message.body}`;
        await api.sendMessage({ customType: "safe-agents.message", content, display: true, details: message }, {
          triggerTurn: true,
          deliverAs: message.priority === "urgent" ? "steer" : "followUp",
        });
        rememberRootMessage(message.id, "accepted");
        await runtime.request("message.ack", { messageId: message.id });
        rememberRootMessage(message.id, "acknowledged");
      } catch {
        rootDeliveryStates.delete(message.id);
      }
    }).catch(() => undefined);
  };

  function rememberRootMessage(messageId: string, state: "accepted" | "acknowledged"): void {
    rootDeliveryStates.set(messageId, state);
    while (rootDeliveryStates.size > 2048) {
      const removable = [...rootDeliveryStates.entries()].find(([, current]) => current === "acknowledged")?.[0];
      if (!removable) break;
      rootDeliveryStates.delete(removable);
    }
  }

  // The root participates in borrowing too: ordinary Pi edit/write calls are
  // vetoed before the filesystem mutation when a live hold overlaps the path,
  // and allowed writes take a short-lived fence so a hold that lapses
  // mid-write cannot be handed to a competing writer. Root shell stays
  // unguarded and is documented as trusted, not sandboxed.
  const pendingRootFences = new Map<string, string>();
  pi.on("tool_call", async (event, ctx) => {
    if (event.toolName !== "edit" && event.toolName !== "write") return undefined;
    const outcome = await runtime.guardRootMutation(event.toolName, event.input, ctx.cwd);
    if (outcome?.block) return outcome;
    if (outcome?.fenceId) pendingRootFences.set(event.toolCallId, outcome.fenceId);
    return undefined;
  });

  pi.on("tool_result", (event) => {
    const fenceId = pendingRootFences.get(event.toolCallId);
    if (fenceId === undefined) return undefined;
    pendingRootFences.delete(event.toolCallId);
    void runtime.releaseRootFence(fenceId);
    return undefined;
  });

  pi.on("session_start", async (_event, ctx) => {
    const generation = lifecycleQueue.beginSession();
    try {
      await enqueueLifecycle(generation, async () => {
        await runtime.ensureRoot(pi, ctx, rootDelivery(pi));
        if (generation !== lifecycleQueue.currentGeneration) return;
        ctx.ui.setStatus("safe-agents", `fabric ${runtime.rootAgentId ?? "starting"}`);
      });
    } catch (error) {
      notifyLifecycleFailure(ctx, error, "error", generation);
    }
  });

  pi.on("agent_start", (_event, ctx) => {
    const generation = lifecycleQueue.currentGeneration;
    void enqueueLifecycle(generation, async () => {
      await runtime.ensureRoot(pi, ctx, rootDelivery(pi));
      if (generation !== lifecycleQueue.currentGeneration) return;
      await runtime.request("agent.begin_turn", {});
    }).catch((error) => notifyLifecycleFailure(ctx, error, "warning", generation));
  });

  // agent_end can be followed by Pi's automatic retry, compaction, or queued
  // continuation. End the broker turn only after Pi confirms the run is
  // settled, so the root never advertises readiness during another run.
  pi.on("agent_settled", (_event, ctx) => {
    const generation = lifecycleQueue.currentGeneration;
    void enqueueLifecycle(generation, async () => {
      if (!runtime.rootAgentId) return;
      await runtime.request("agent.end_turn", { status: "ready" });
    }).catch((error) => notifyLifecycleFailure(ctx, error, "warning", generation));
  });

  pi.on("session_shutdown", async (_event, ctx) => {
    const pendingLifecycle = lifecycleQueue.shutdown();
    await pendingLifecycle.catch(() => undefined);
    await runtime.stop().catch((error) => notifyLifecycleFailure(ctx, error, "warning"));
  });

  pi.registerCommand("agents", {
    description: "Inspect the safe-agents fabric (status, tree, tasks, resources, messages, inbox)",
    handler: async (args, ctx) => {
      try {
        await runtime.ensureRoot(pi, ctx, rootDelivery(pi));
        const mode = args.trim() || "status";
        if (mode === "inbox") {
          const messages = await runtime.request<AgentMessage[]>("message.inbox", { limit: 50 });
          ctx.ui.notify(messages.length ? messages.map(formatMessage).join("\n\n") : "safe-agents inbox is empty", "info");
          return;
        }
        const status = await runtime.status() as FabricStatus;
        ctx.ui.notify(formatStatus(status, mode), "info");
      } catch (error) {
        ctx.ui.notify(`safe-agents: ${asFabricError(error).message}`, "error");
      }
    },
  });

  pi.registerCommand("agent-stop", {
    description: "Cancel a safe-agents child by ID",
    handler: async (args, ctx) => {
      try {
        await runtime.ensureRoot(pi, ctx, rootDelivery(pi));
        const agentId = args.trim();
        if (!agentId) throw new FabricError("INVALID_ARGUMENT", "usage: /agent-stop <agent-id>");
        await runtime.request("agent.cancel", { agentId });
        ctx.ui.notify(`safe-agents: cancellation requested for ${agentId}`, "info");
      } catch (error) {
        ctx.ui.notify(`safe-agents: ${asFabricError(error).message}`, "error");
      }
    },
  });
}

function formatMessage(message: AgentMessage): string {
  return `[${message.type}] ${message.from} -> ${message.to}: ${message.body}`;
}

function formatStatus(status: FabricStatus, mode: string): string {
  if (mode === "tree" || mode === "agents") {
    return status.agents.map((agent) => `${"  ".repeat(agent.depth)}${agent.id} [${agent.status}] ${agent.role} ${agent.route.provider}/${agent.route.model}${agent.taskId ? ` task=${agent.taskId}` : ""}`).join("\n") || "safe-agents: no agents";
  }
  if (mode === "tasks") return status.tasks.map((task) => `${task.id} [${task.status}] ${task.owner ?? "unclaimed"}: ${task.description}`).join("\n") || "safe-agents: no tasks";
  if (mode === "resources") return status.resources.map((resource) => `${resource.id}@${resource.version} owner=${resource.owner ?? "none"} shared=${resource.sharedHolds.length} mutable=${resource.mutableHold?.agentId ?? "none"} waiters=${resource.waiters.length}`).join("\n") || "safe-agents: no resources";
  if (mode === "messages") return status.recentMessages.slice(0, 30).map(formatMessage).join("\n\n") || "safe-agents: no recent messages";
  return `fabric ${status.rootId}\nagents: ${status.agents.length} (running ${status.runningChildren})\ntasks: ${status.tasks.length}\nresources: ${status.resources.length}\npending requests: ${status.pendingRequests.length}\n\n${status.agents.map((agent) => `${agent.id} [${agent.status}] ${agent.role}`).join("\n")}`;
}
