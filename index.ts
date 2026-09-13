import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Box, Text } from "@earendil-works/pi-tui";
import { FabricError, asFabricError } from "./src/core/errors.ts";
import type { AgentMessage, FabricStatus } from "./src/core/types.ts";
import { FabricRuntime, type DescendantShutdownMode } from "./src/pi/runtime.ts";
import { LifecycleQueue } from "./src/pi/lifecycle.ts";
import { createCoordinationTools } from "./src/pi/tools.ts";
import { classifyRootDelivery } from "./src/pi/delivery.ts";
import { registerInteropProvider, unregisterInteropProvider, type FabricSnapshotRequest, type FabricStateProviderV1, type FabricStateSnapshotV1 } from "./src/pi/interop.ts";
import { classifyAssistantMessage, classifyCompactionFailure, findFinalAssistantMessage, type ModelTurnOutcome } from "./src/pi/turn-outcome.ts";

export { Coordinator } from "./src/core/coordinator.ts";
export { FabricError } from "./src/core/errors.ts";
export { resolveRoute, routeId } from "./src/core/routing.ts";
export { effectivePrefillBudget, modelRouteCapacity, modelRouteCapacityKey, modelRouteKey, modelRoutePolicy } from "./src/core/coordinator-wire.ts";
export { BrokerClient } from "./src/broker/client.ts";
export { BrokerServer, startBroker } from "./src/broker/server.ts";
export { Journal } from "./src/broker/journal.ts";
export { FabricRuntime, ManagedChild, taskAwareTurnStatus } from "./src/pi/runtime.ts";
export type { DescendantShutdownMode, HandoffSnapshot } from "./src/pi/runtime.ts";
export { assertNoDetachedShellCommand, assertReadOnlyShellCommand, createGuardedChildTools, createGuardedReadOnlyTools, evaluateRootShellGuard, evaluateRootWriteGuard, workspaceRelativePath } from "./src/pi/guards.ts";
export { classifyRootShellCommand, type RootShellRisk } from "./src/pi/shell-classifier.ts";
export { classifyRootDelivery, type RootDeliveryDecision, type RootDeliveryContext } from "./src/pi/delivery.ts";
export { ModelRouteCapacityArbiter } from "./src/pi/model-capacity.ts";
export { loadFabricConfig, type FabricConfigLoadOptions, type FabricConfigLoadResult } from "./src/pi/config.ts";
export { classifyAssistantMessage, classifyCompactionFailure, describeTurnOutcome, findFinalAssistantMessage, isAbortLikeMessage, isBlockingOutcome, type ModelTurnOutcome, type ModelTurnOutcomeKind } from "./src/pi/turn-outcome.ts";
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
  let rootDeliveryEpoch = 0;
  const deferredRootMessages = new Map<string, AgentMessage>();
  const deferredRootPersisted = new Set<string>();
  const deferredRootRunMessages = new Set<string>();
  /** IDs accepted by Pi but not yet proven present in session history. */
  const persistedRootMessageIds = new Set<string>();
  const rootAckInFlight = new Map<string, Promise<void>>();
  let deferredRootWakePending = false;
  let rootContext: ExtensionContext | undefined;
  const lifecycleQueue = new LifecycleQueue();
  let rootLogicalRunActive = false;
  let rootLogicalTurnId = "root-turn-0";
  let rootTurnSequence = 0;
  let lastFinalRootOutcome: ModelTurnOutcome | undefined;

  const requestRootLifecycle = <T = unknown>(operation: string, args: Record<string, unknown>, operationId: string): Promise<T> => {
    // Runtime test hosts may provide only the ordinary request surface. The
    // real BrokerClient path uses the durable coordinator replay record.
    if (runtime.client && typeof runtime.client.requestIdempotent === "function") {
      return runtime.requestIdempotent<T>(operation, args, operationId, FabricRuntime.shutdownRpcTimeoutMs);
    }
    return runtime.request<T>(operation, args, FabricRuntime.shutdownRpcTimeoutMs);
  };

  const enqueueLifecycle = (generation: number, operation: () => Promise<void>): Promise<void> =>
    lifecycleQueue.enqueue(generation, operation);

  const updatePendingDeliveries = (): void => {
    // Accepted/deferred messages remain broker-unacknowledged until Pi has
    // crossed the session boundary, so they must keep root quiescence closed.
    const count = [...rootDeliveryStates.values()].filter((s) => s !== "acknowledged").length;
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

  const rootDelivery = (api: ExtensionAPI, deliveryEpoch = rootDeliveryEpoch) => (message: AgentMessage): void => {
    const epoch = deliveryEpoch;
    const state = rootDeliveryStates.get(message.id);
    if (state === "acknowledged" || state === "delivering") return;
    if (state === "accepted") {
      if (epoch !== rootDeliveryEpoch) return;
      void acknowledgePersistedRootMessage(message.id);
      return;
    }
    if (hasPersistedRootMessage(message.id)) {
      rootDeliveryStates.set(message.id, "accepted");
      persistedRootMessageIds.add(message.id);
      void acknowledgePersistedRootMessage(message.id);
      return;
    }
    rootDeliveryStates.set(message.id, "delivering");
    updatePendingDeliveries();

    // Preserve broker order at the host boundary too. In particular, two
    // urgent messages must not race through api.sendMessage and reach the
    // root model in the reverse order.
    rootDeliveryTail = rootDeliveryTail.then(async () => {
      try {
        if (epoch !== rootDeliveryEpoch) return;
        const content = `[${message.type} from ${message.from}]\n${message.body}`;
        const decision = classifyRootDelivery(message, {
          rootCompactionInFlight: runtime.isRootCompactionInFlight,
          rootContextHealth: runtime.rootHealth,
        });
        await api.sendMessage({
          customType: "safe-agents.message",
          content,
          display: decision.display,
          // Keep a structured exact receipt alongside the legacy AgentMessage
          // details object. Durable ACKs must never rely on text substring
          // matching or an unrelated quoted message ID.
          details: { ...message, fabricMessageId: message.id },
        }, {
          triggerTurn: decision.triggerTurn,
          deliverAs: decision.deliverAs,
        });
        if (epoch !== rootDeliveryEpoch) return;
        rememberRootMessage(message.id, "accepted");
        if (decision.deliverAs === "nextTurn") {
          // Pi only persists nextTurn messages when a real prompt starts. Keep
          // the broker copy unacknowledged until the resulting agent_end has
          // crossed that session-history boundary.
          deferredRootMessages.set(message.id, message);
          return;
        }
        await acknowledgePersistedRootMessage(message.id);
      } catch {
        rootDeliveryStates.delete(message.id);
      } finally {
        if (epoch !== rootDeliveryEpoch) rootDeliveryStates.delete(message.id);
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

  function hasPersistedRootMessage(messageId: string): boolean {
    try {
      const manager = rootContext?.sessionManager as unknown as { getEntries?: () => readonly unknown[] } | undefined;
      const entries = typeof manager?.getEntries === "function" ? manager.getEntries() : [];
      return entries.some((entry: any) => entry?.type === "custom_message" && entry?.customType === "safe-agents.message" && (
        entry?.details?.fabricMessageId === messageId || entry?.details?.id === messageId
      ));
    } catch {
      return false;
    }
  }

  async function acknowledgePersistedDeferredRootMessages(): Promise<void> {
    await acknowledgePersistedRootMessages();
  }

  async function acknowledgePersistedRootMessages(): Promise<void> {
    for (const [messageId, state] of rootDeliveryStates) {
      if (state !== "accepted") continue;
      await acknowledgePersistedRootMessage(messageId);
    }
  }

  async function acknowledgePersistedRootMessage(messageId: string): Promise<void> {
    const message = deferredRootMessages.get(messageId);
    if (message && !deferredRootRunMessages.has(messageId) && !deferredRootPersisted.has(messageId)) return;
    if (!persistedRootMessageIds.has(messageId)) {
      if (!hasPersistedRootMessage(messageId)) return;
      persistedRootMessageIds.add(messageId);
      if (message) deferredRootPersisted.add(messageId);
    }
    const inFlight = rootAckInFlight.get(messageId);
    if (inFlight) return inFlight;
    const operation = runtime.request("message.ack", { messageId }, FabricRuntime.shutdownRpcTimeoutMs)
      .then(() => {
        if (message) {
          deferredRootMessages.delete(messageId);
          deferredRootPersisted.delete(messageId);
          deferredRootRunMessages.delete(messageId);
        }
        rememberRootMessage(messageId, "acknowledged");
      })
      .catch(() => undefined)
      .finally(() => {
        if (rootAckInFlight.get(messageId) === operation) rootAckInFlight.delete(messageId);
      });
    rootAckInFlight.set(messageId, operation);
    return operation;
  }

  function wakeDeferredRoot(api: ExtensionAPI): void {
    if (deferredRootMessages.size === 0 || runtime.isRootCompactionInFlight || runtime.rootHealth === "degraded") return;
    // A retry/continuation is already running in Pi. The messages were
    // delivered as next-turn context, so wait until that logical run settles
    // before enqueueing a synthetic wake. `nextTurn` messages are injected by
    // Pi only when a real prompt starts, so a custom-message wake would leave
    // them stranded in the pending-next-turn queue.
    if (rootLogicalRunActive) {
      deferredRootWakePending = true;
      return;
    }
    const pendingDelivery = [...deferredRootMessages.keys()].some((messageId) => !deferredRootPersisted.has(messageId));
    if (!pendingDelivery) return;
    deferredRootWakePending = false;
    const sendUserMessage = (api as ExtensionAPI & { sendUserMessage?: (content: string, options?: { expandPromptTemplates?: boolean }) => void }).sendUserMessage;
    if (typeof sendUserMessage === "function") {
      // This starts a normal prompt, which flushes Pi's pending `nextTurn`
      // messages into the model context before generation begins.
      try {
        void Promise.resolve(sendUserMessage("Review the deferred safe-agents messages that were waiting for root context recovery.", {
          expandPromptTemplates: false,
        })).catch(() => undefined);
      } catch {
        // Some lightweight hosts expose a synchronous sendUserMessage shim;
        // a throw there must not break the lifecycle callback or lose the
        // still-unacknowledged broker message.
      }
      return;
    }
    // Lightweight hosts predating sendUserMessage still get a visible wake;
    // their custom-message implementation may not expose Pi's pending-next-turn
    // queue, so retain the compatibility fallback.
    void api.sendMessage({
      customType: "safe-agents.status",
      content: "Deferred fabric messages are available in the session context; review them before continuing.",
      display: true,
      details: { reason: "root-compaction-settled" },
    }, { triggerTurn: true, deliverAs: "followUp" });
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
    void runtime.releaseRootFence(fenceId, FabricRuntime.shutdownRpcTimeoutMs);
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
    rootDeliveryEpoch += 1;
    rootDeliveryTail = Promise.resolve();
    rootDeliveryStates.clear();
    runtime.setPendingRootDeliveriesCount(0);
    deferredRootMessages.clear();
    deferredRootPersisted.clear();
    deferredRootRunMessages.clear();
    persistedRootMessageIds.clear();
    rootAckInFlight.clear();
    deferredRootWakePending = false;
    rootContext = ctx;
    rootLogicalRunActive = false;
    lastFinalRootOutcome = undefined;
    runtime.resetRootCompactionState();
    runtime.resetRootContextHealth();
    registerInteropProvider("safe-agent-team.fabric-state.v1", interopProvider);
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

  pi.on("agent_start", async (_event, ctx) => {
    const generation = lifecycleQueue.currentGeneration;
    try {
      await enqueueLifecycle(generation, async () => {
        if (rootLogicalRunActive) return;
        await runtime.ensureRoot(pi, ctx, rootDelivery(pi));
        if (generation !== lifecycleQueue.currentGeneration) return;
        rootLogicalRunActive = true;
        rootLogicalTurnId = `root-turn-${++rootTurnSequence}`;
        lastFinalRootOutcome = undefined;
        runtime.resetRootContextHealth();
        let started = false;
        while (!started && generation === lifecycleQueue.currentGeneration) {
          const result = await requestRootLifecycle<{ started?: boolean }>("agent.begin_turn", {}, `${rootLogicalTurnId}:begin`);
          started = result?.started !== false;
          if (!started) await new Promise((resolve) => setTimeout(resolve, 100));
        }
        if (started) {
          // Keep the broker admission and process-local capacity permit in the
          // same order used by managed children and root compaction.
          await runtime.beginRootModelTurnCapacity();
          // The nextTurn queue is now part of this Pi prompt's immutable input;
          // ACK only after a later agent_end confirms its message_end entries.
          for (const messageId of deferredRootMessages.keys()) deferredRootRunMessages.add(messageId);
        }
      });
    } catch (error) {
      rootLogicalRunActive = false;
      await runtime.endRootModelTurnCapacity().catch(() => undefined);
      await requestRootLifecycle("agent.end_turn", { status: "ready" }, `${rootLogicalTurnId}:end`).catch(() => undefined);
      notifyLifecycleFailure(ctx, error, "warning", generation);
      throw error;
    }
  });

  // Observe the actual low-level terminal response. Pi may emit multiple
  // agent_end events while retrying/compacting; the latest one before
  // agent_settled is the logical run outcome, without transcript inference.
  pi.on("agent_end", async (event, ctx) => {
    const assistant = findFinalAssistantMessage(event.messages ?? []);
    if (assistant !== undefined) {
      lastFinalRootOutcome = classifyAssistantMessage(assistant, ctx.model?.contextWindow);
    }
    await acknowledgePersistedDeferredRootMessages();
  });

  // Manual, threshold, and overflow compaction all mutate root context outside
  // a normal broker turn. Keep the fabric conservative through the full hook
  // interval and let durable deliveries queue as nextTurn/context-only.
  pi.on("session_before_compact", async (event) => {
    await runtime.beginRootCompaction(event.signal);
  });
  pi.on("session_compact", async (_event, ctx) => {
    await runtime.endRootCompaction();
    runtime.markRootContextHealthy();
    await acknowledgePersistedRootMessages();
    wakeDeferredRoot(pi);
  });
  pi.on("session_compact_failed", async (event, ctx) => {
    await runtime.endRootCompaction();
    if (!event.aborted) {
      const outcome = classifyCompactionFailure(event.errorMessage, false, ctx.model?.contextWindow);
      lastFinalRootOutcome = outcome;
      runtime.markRootContextDegraded(outcome);
    }
  });

  // agent_end can be followed by Pi's automatic retry, compaction, or queued
  // continuation. End the broker turn only after Pi confirms the run is
  // settled, so the root never advertises readiness during another run.
  pi.on("agent_settled", (_event, ctx) => {
    const generation = lifecycleQueue.currentGeneration;
    void enqueueLifecycle(generation, async () => {
      if (!runtime.rootAgentId) return;
      const outcome = lastFinalRootOutcome;
      if (outcome?.kind === "aborted") {
        await runtime.abortDescendants({ reason: "root-aborted", mode: "budget" });
      }
      if (outcome && outcome.kind !== "success" && outcome.kind !== "aborted") runtime.markRootContextDegraded(outcome);
      else if (outcome?.kind === "success") runtime.markRootContextHealthy();
      try {
        await requestRootLifecycle("agent.end_turn", { status: "ready" }, `${rootLogicalTurnId}:end`);
      } finally {
        await runtime.endRootModelTurnCapacity();
        await acknowledgePersistedRootMessages();
        rootLogicalRunActive = false;
        lastFinalRootOutcome = undefined;
      }
      if (deferredRootWakePending || deferredRootMessages.size > 0) wakeDeferredRoot(pi);
    }).catch((error) => notifyLifecycleFailure(ctx, error, "warning", generation));
  });

  pi.on("session_shutdown", async (_event, ctx) => {
    rootDeliveryEpoch += 1;
    rootDeliveryTail = Promise.resolve();
    deferredRootMessages.clear();
    deferredRootPersisted.clear();
    deferredRootRunMessages.clear();
    persistedRootMessageIds.clear();
    rootAckInFlight.clear();
    deferredRootWakePending = false;
    rootLogicalRunActive = false;
    lastFinalRootOutcome = undefined;
    runtime.resetRootCompactionState();
    rootDeliveryStates.clear();
    runtime.setPendingRootDeliveriesCount(0);
    unregisterInteropProvider("safe-agent-team.fabric-state.v1", interopProvider);
    const pendingLifecycle = lifecycleQueue.shutdown();
    await pendingLifecycle.catch(() => undefined);
    const pendingFences = [...pendingRootFences.values()];
    pendingRootFences.clear();
    runtime.setPendingRootFencesCount(0);
    await Promise.allSettled(pendingFences.map((fenceId) => runtime.releaseRootFence(fenceId, FabricRuntime.shutdownRpcTimeoutMs)));
    await runtime.stop().catch((error) => notifyLifecycleFailure(ctx, error, "warning"));
    rootContext = undefined;
  });

  pi.registerCommand("agents", {
    description: "Inspect or stop the safe-agents fabric (status, tree, tasks, resources, messages, inbox, stop)",
    handler: async (args, ctx) => {
      try {
        const mode = args.trim() || "status";
        if (mode === "stop" || mode.startsWith("stop ")) {
          const requestedMode = mode.slice("stop".length).trim();
          if (requestedMode !== "" && requestedMode !== "--now" && requestedMode !== "--budget") {
            throw new FabricError("INVALID_ARGUMENT", "usage: /agents stop [--budget|--now]");
          }
          const shutdownMode: DescendantShutdownMode = requestedMode === "--now" ? "now" : requestedMode === "--budget" ? "budget" : "graceful";
          // Start the local emergency kill switch before any broker-backed
          // root attach/status work. A frozen broker must not delay --now.
          if (shutdownMode === "now") {
            const snapshots = await runtime.abortDescendants({ reason: `agents-stop:${shutdownMode}`, mode: shutdownMode });
            ctx.ui.notify(`safe-agents: stopped ${snapshots.length} descendant${snapshots.length === 1 ? "" : "s"} (${shutdownMode}); deterministic handoff captured`, "info");
            return;
          }
          await runtime.ensureRoot(pi, ctx, rootDelivery(pi));
          const snapshots = await runtime.abortDescendants({ reason: `agents-stop:${shutdownMode}`, mode: shutdownMode });
          ctx.ui.notify(`safe-agents: stopped ${snapshots.length} descendant${snapshots.length === 1 ? "" : "s"} (${shutdownMode}); deterministic handoff captured`, "info");
          return;
        }
        await runtime.ensureRoot(pi, ctx, rootDelivery(pi));
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
        agent.contextDiagnostic ? `${indent}  context-diagnostic=${agent.contextDiagnostic}` : undefined,
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
    `root context: ${snapshot?.rootCompactionInFlight ? "compacting" : snapshot?.rootContextHealth === "degraded" ? `degraded${snapshot.rootContextDiagnostic ? ` (${snapshot.rootContextDiagnostic})` : ""}` : "healthy"}`,
    `agents: ${status.agents.length} (running ${status.runningChildren})`,
    `tasks: ${status.tasks.length}`,
    `resources: ${status.resources.length}`,
    "",
    ...status.agents.map((agent) => `${agent.id} [${agent.status}] ${agent.role} context=${agent.contextMode ?? "native"}`),
  ].filter((line): line is string => Boolean(line)).join("\n");

  return summary;
}
