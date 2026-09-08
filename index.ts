import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Box, Text } from "@earendil-works/pi-tui";
import { FabricError, asFabricError } from "./src/core/errors.ts";
import type { AgentMessage, FabricStatus } from "./src/core/types.ts";
import { FabricRuntime } from "./src/pi/runtime.ts";
import { LifecycleQueue } from "./src/pi/lifecycle.ts";
import { createCoordinationTools } from "./src/pi/tools.ts";
import { classifyRootDelivery } from "./src/pi/delivery.ts";
import { registerInteropProvider, unregisterInteropProvider, type FabricSnapshotRequest, type FabricStateProviderV1, type FabricStateSnapshotV1 } from "./src/pi/interop.ts";

export { Coordinator } from "./src/core/coordinator.ts";
export { FabricError } from "./src/core/errors.ts";
export { resolveRoute, routeId } from "./src/core/routing.ts";
export { BrokerClient } from "./src/broker/client.ts";
export { BrokerServer, startBroker } from "./src/broker/server.ts";
export { Journal } from "./src/broker/journal.ts";
export { FabricRuntime, ManagedChild, taskAwareTurnStatus } from "./src/pi/runtime.ts";
export { assertReadOnlyShellCommand, createGuardedChildTools, createGuardedReadOnlyTools, evaluateRootShellGuard, evaluateRootWriteGuard, workspaceRelativePath } from "./src/pi/guards.ts";
export { classifyRootShellCommand, type RootShellRisk } from "./src/pi/shell-classifier.ts";
export { classifyRootDelivery, type RootDeliveryDecision, type RootDeliveryContext } from "./src/pi/delivery.ts";
export { getInteropRegistry, getInteropProvider, registerInteropProvider, unregisterInteropProvider, PI_EXTENSION_INTEROP } from "./src/pi/interop.ts";
export type { FabricSnapshotRequest, FabricStateSnapshotV1, FabricStateProviderV1, EmbeddedContextHost, EmbeddedContextManager, EmbeddedToolResult } from "./src/pi/interop.ts";
export { GitWorkspaceStrategy, SharedWorkspaceStrategy } from "./src/workspace.ts";
export * from "./src/core/types.ts";

export default function safeAgentsTeam(pi: ExtensionAPI): void {
  const runtime = new FabricRuntime();
  const lazyClient = {
    request: <T = unknown>(operation: string, args: Record<string, unknown> = {}) => runtime.request<T>(operation, args),
    requestIdempotent: <T = unknown>(operation: string, args: Record<string, unknown> = {}, operationId?: string, timeoutMs?: number) =>
      runtime.requestIdempotent<T>(operation, args, operationId, timeoutMs),
  };

  const interopProvider: FabricStateProviderV1 = {
    getSnapshot: (request: FabricSnapshotRequest) => runtime.getFabricStateSnapshot(request),
  };
  registerInteropProvider("safe-agent-team.fabric-state.v1", interopProvider);

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

  const updatePendingDeliveries = (): void => {
    const count = [...rootDeliveryStates.values()].filter((s) => s === "delivering").length;
    runtime.setPendingRootDeliveriesCount(count);
  };

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
    updatePendingDeliveries();

    // Preserve broker order at the host boundary too. In particular, two
    // urgent messages must not race through api.sendMessage and reach the
    // root model in the reverse order.
    rootDeliveryTail = rootDeliveryTail.then(async () => {
      try {
        const content = `[${message.type} from ${message.from}]\n${message.body}`;
        const hasPendingRootRequest = message.type === "resource_granted"
          ? await runtime.hasPendingRootRequest(message)
          : false;
        const decision = classifyRootDelivery(message, { hasPendingRootRequest });
        await api.sendMessage({ customType: "safe-agents.message", content, display: decision.display, details: message }, {
          triggerTurn: decision.triggerTurn,
          deliverAs: decision.deliverAs,
        });
        rememberRootMessage(message.id, "accepted");
        await runtime.request("message.ack", { messageId: message.id });
        rememberRootMessage(message.id, "acknowledged");
      } catch {
        rootDeliveryStates.delete(message.id);
      } finally {
        updatePendingDeliveries();
      }
    }).catch(() => undefined);
  };

  function rememberRootMessage(messageId: string, state: "accepted" | "acknowledged"): void {
    rootDeliveryStates.set(messageId, state);
    updatePendingDeliveries();
    while (rootDeliveryStates.size > 2048) {
      const removable = [...rootDeliveryStates.entries()].find(([, current]) => current === "acknowledged")?.[0];
      if (!removable) break;
      rootDeliveryStates.delete(removable);
    }
  }

  // The root participates in borrowing too: ordinary Pi edit/write calls are
  // vetoed before the filesystem mutation when a live hold overlaps the path,
  // and allowed writes take a short-lived fence so a hold that lapses
  // mid-write cannot be handed to a competing writer. Common broad root shell
  // mutations are preflight guarded while child coordination is live.
  const pendingRootFences = new Map<string, string>();
  pi.on("tool_call", async (event, ctx) => {
    if (event.toolName === "bash") {
      const outcome = await runtime.guardRootShell(event.input, ctx.cwd);
      if (outcome?.block) return outcome;
      return undefined;
    }
    if (event.toolName !== "edit" && event.toolName !== "write") return undefined;
    const outcome = await runtime.guardRootMutation(event.toolName, event.input, ctx.cwd);
    if (outcome?.block) return outcome;
    if (outcome?.fenceId) {
      pendingRootFences.set(event.toolCallId, outcome.fenceId);
      runtime.setPendingRootFencesCount(pendingRootFences.size);
    }
    return undefined;
  });

  pi.on("tool_result", (event) => {
    const fenceId = pendingRootFences.get(event.toolCallId);
    if (fenceId === undefined) return undefined;
    pendingRootFences.delete(event.toolCallId);
    runtime.setPendingRootFencesCount(pendingRootFences.size);
    void runtime.releaseRootFence(fenceId);
    return undefined;
  });

  pi.on("before_agent_start", (event) => {
    const delegationGuideline = `Delegation policy:
- Use safe-agent children for repository analysis, implementation, tests, and coordinated parallel work.
- Managed children do not automatically inherit root extensions.
- If a delegated task requires web, Chrome, computer-use, or MCP and the child does not have an explicitly brokered equivalent, perform that external I/O at the root and send the result to the child.
- Do not infer child capabilities from root tool availability.`;
    if (event.systemPrompt && !event.systemPrompt.includes("Delegation policy:")) {
      return { systemPrompt: `${event.systemPrompt}\n\n${delegationGuideline}` };
    }
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
    unregisterInteropProvider("safe-agent-team.fabric-state.v1", interopProvider);
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
        const status = (await runtime.status()) as FabricStatus;
        const snapshot = await runtime.getFabricStateSnapshot({
          cwd: ctx.cwd,
          sessionId: (ctx as any).sessionManager?.getSessionId?.(),
        });
        ctx.ui.notify(formatStatus(status, mode, snapshot), "info");
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

function formatStatus(status: FabricStatus, mode: string, snapshot?: FabricStateSnapshotV1 | null): string {
  if (mode === "tree" || mode === "agents") {
    return status.agents.map((agent) => {
      const indent = "  ".repeat(agent.depth);
      if (agent.depth === 0) {
        return `${agent.id} [${agent.status}] ${agent.role} ${agent.route.provider}/${agent.route.model}`;
      }
      const lines = [
        `${indent}${agent.id} [${agent.status}] role=${agent.role}`,
        `${indent}  model=${agent.route.provider}/${agent.route.model} thinking=${agent.route.thinking ?? "none"}`,
        agent.taskId ? `${indent}  task=${agent.taskId}` : undefined,
        agent.workspace ? `${indent}  workspace=${agent.workspace.mode}:${agent.workspace.path}` : undefined,
        `${indent}  repo-write=${agent.capabilities?.mayWriteRepo ? "yes" : "no"} shell=${agent.workspace?.mode === "worktree" ? "workspace" : "read-only"}`,
        `${indent}  spawn=${agent.capabilities?.maySpawn ? "yes" : "no"} peers=${agent.capabilities?.mayMessagePeers ? "yes" : "no"} escalate=${agent.capabilities?.mayEscalate ? "yes" : "no"}`,
        `${indent}  external-root-extensions=not-inherited`,
        `${indent}  context=${agent.contextMode ?? "native"}`,
      ].filter(Boolean);
      return lines.join("\n");
    }).join("\n") || "safe-agents: no agents";
  }
  if (mode === "tasks") return status.tasks.map((task) => `${task.id} [${task.status}] ${task.owner ?? "unclaimed"}: ${task.description}`).join("\n") || "safe-agents: no tasks";
  if (mode === "resources") return status.resources.map((resource) => `${resource.id}@${resource.version} owner=${resource.owner ?? "none"} shared=${resource.sharedHolds.length} mutable=${resource.mutableHold?.agentId ?? "none"} waiters=${resource.waiters.length}`).join("\n") || "safe-agents: no resources";
  if (mode === "messages") return status.recentMessages.slice(0, 30).map(formatMessage).join("\n\n") || "safe-agents: no recent messages";

  const quiescentStr = snapshot ? (snapshot.quiescent ? "yes" : "no") : "unknown";
  const unresolvedTasks = snapshot ? snapshot.unresolvedChildTasks : status.tasks.filter((t) => t.owner && t.owner !== status.rootId && !["completed", "failed", "cancelled"].includes(t.status)).length;
  const mutableHolds = snapshot ? snapshot.mutableHolds : status.resources.filter((r) => r.mutableHold !== undefined).length;
  const pendingRequests = snapshot ? snapshot.pendingRootRequests : status.pendingRequests.length;
  const pendingDeliveries = snapshot ? snapshot.pendingRootDeliveries : 0;

  const summary = [
    `fabric ${status.rootId}`,
    `quiescent: ${quiescentStr}`,
    `running children: ${status.runningChildren}`,
    `unresolved child tasks: ${unresolvedTasks}`,
    `mutable holds: ${mutableHolds}`,
    `pending root requests: ${pendingRequests}`,
    `pending root deliveries: ${pendingDeliveries}`,
    `agents: ${status.agents.length} (running ${status.runningChildren})`,
    `tasks: ${status.tasks.length}`,
    `resources: ${status.resources.length}`,
    "",
    ...status.agents.map((agent) => `${agent.id} [${agent.status}] ${agent.role} context=${agent.contextMode ?? "native"}`),
  ].join("\n");

  return summary;
}
