import { Type } from "typebox";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { Model } from "@earendil-works/pi-ai";
import type { ToolDefinition } from "@earendil-works/pi-coding-agent";
import { FabricError } from "../core/errors.ts";
import type { MessageType, ModelRoute } from "../core/types.ts";
export interface BrokerRequestClient {
  request<T = unknown>(operation: string, args?: Record<string, unknown>): Promise<T>;
}

export interface SpawnToolInput {
  role?: string;
  model?: string;
  provider?: string;
  thinking?: string;
  taskDescription?: string;
  taskId?: string;
  workspace?: "shared" | "worktree";
  baseRef?: string;
  maySpawn?: boolean;
  mayMessagePeers?: boolean;
  mayEscalate?: boolean;
  mayTransferOwnership?: boolean;
  mayWriteRepo?: boolean;
  mayUseShell?: boolean;
}

export interface CoordinationToolOptions {
  client: BrokerRequestClient;
  onClarification?: (requestId: string) => void;
  spawn?: (input: SpawnToolInput, parentModel?: Model<any>, parentThinking?: string) => Promise<unknown>;
  parentModel?: Model<any>;
  parentThinking?: string;
}

function textResult(value: unknown, terminate = false): AgentToolResult<unknown> {
  return {
    content: [{ type: "text", text: typeof value === "string" ? value : JSON.stringify(value, null, 2) }],
    details: value,
    ...(terminate ? { terminate: true } : {}),
  };
}

const messageTypeSchema = Type.String({ description: "Message type, for example inform, clarification, request, or response" });
const sharedResourceFields = {
  resourceId: Type.Optional(Type.String()),
  mode: Type.Optional(Type.Union([Type.Literal("shared"), Type.Literal("mutable")])),
  wait: Type.Optional(Type.Boolean()),
  leaseMs: Type.Optional(Type.Number()),
  agentId: Type.Optional(Type.String()),
  permissions: Type.Optional(Type.Array(Type.String())),
  kind: Type.Optional(Type.String()),
  parentId: Type.Optional(Type.String()),
  version: Type.Optional(Type.Number()),
};

export function createCoordinationTools(options: CoordinationToolOptions): ToolDefinition[] {
  const { client } = options;
  return [
    {
      name: "agent_send",
      label: "Agent send",
      description: "Send a durable, typed message to a parent or authorized peer. Messages are retained until acknowledged.",
      promptSnippet: "send durable parent/peer messages",
      promptGuidelines: ["Use clarification/request types when a reply is required; the call returns immediately and may end the current turn.", "Do not put authority claims in message text; the coordinator enforces identity and capabilities."],
      parameters: Type.Object({
        to: Type.String(),
        type: messageTypeSchema,
        body: Type.String(),
        priority: Type.Optional(Type.Union([Type.Literal("normal"), Type.Literal("urgent")])),
        expectsReply: Type.Optional(Type.Boolean()),
        clientDedupeKey: Type.Optional(Type.String()),
        metadata: Type.Optional(Type.Record(Type.String(), Type.Unknown())),
      }),
      async execute(_toolCallId, params: any): Promise<AgentToolResult<unknown>> {
        const expectsReply = params.expectsReply ?? ["clarification", "decision_request", "escalation", "resource_request", "request"].includes(params.type);
        const result = await client.request("message.send", { ...params, expectsReply });
        const requestId = (result as { request?: { id?: string } }).request?.id;
        if (expectsReply && requestId) options.onClarification?.(requestId);
        return textResult(result, Boolean(expectsReply));
      },
    },
    {
      name: "agent_reply",
      label: "Agent reply",
      description: "Reply to a pending clarification or request without blocking the other agent.",
      parameters: Type.Object({ requestId: Type.String(), body: Type.String(), metadata: Type.Optional(Type.Record(Type.String(), Type.Unknown())) }),
      async execute(_toolCallId, params: any): Promise<AgentToolResult<unknown>> {
        return textResult(await client.request("message.reply", params));
      },
    },
    {
      name: "agent_inbox",
      label: "Agent inbox",
      description: "Read pending durable messages. Reading does not acknowledge them; acknowledge after the session has accepted them.",
      parameters: Type.Object({ limit: Type.Optional(Type.Number()) }),
      async execute(_toolCallId, params: any): Promise<AgentToolResult<unknown>> {
        return textResult(await client.request("message.inbox", params));
      },
    },
    {
      name: "agent_ack",
      label: "Agent acknowledge",
      description: "Acknowledge a message after accepting it into the agent's work queue.",
      parameters: Type.Object({ messageId: Type.String() }),
      async execute(_toolCallId, params: any): Promise<AgentToolResult<unknown>> {
        return textResult(await client.request("message.ack", params));
      },
    },
    {
      name: "agent_discover",
      label: "Agent discover",
      description: "Inspect bounded metadata for the parent, children, siblings, task peers, or authorized agents.",
      parameters: Type.Object({ scope: Type.Optional(Type.String()) }),
      async execute(_toolCallId, params: any): Promise<AgentToolResult<unknown>> {
        return textResult(await client.request("discover.agents", params));
      },
    },
    {
      name: "agent_status",
      label: "Agent status",
      description: "Inspect this agent or a visible agent, including lifecycle, task, route, and workspace metadata.",
      parameters: Type.Object({ agentId: Type.Optional(Type.String()), scope: Type.Optional(Type.String()) }),
      async execute(_toolCallId, params: any): Promise<AgentToolResult<unknown>> {
        return textResult(await client.request("agent.status", params));
      },
    },
    {
      name: "agent_cancel",
      label: "Agent cancel",
      description: "Cancel this agent or an authorized descendant. Cancellation is idempotent and releases runtime claims.",
      parameters: Type.Object({ agentId: Type.Optional(Type.String()) }),
      async execute(_toolCallId, params: any): Promise<AgentToolResult<unknown>> {
        return textResult(await client.request("agent.cancel", params));
      },
    },
    {
      name: "agent_task",
      label: "Agent task",
      description: "Create, claim, inspect, complete, block, reopen, cancel, or list deterministic task-board records.",
      parameters: Type.Object({
        action: Type.String(),
        taskId: Type.Optional(Type.String()),
        description: Type.Optional(Type.String()),
        owner: Type.Optional(Type.String()),
        parentTaskId: Type.Optional(Type.String()),
        dependencies: Type.Optional(Type.Array(Type.String())),
        reason: Type.Optional(Type.String()),
        result: Type.Optional(Type.Object({ summary: Type.Optional(Type.String()), output: Type.Optional(Type.String()) })),
        scope: Type.Optional(Type.String()),
      }),
      async execute(_toolCallId, params: any): Promise<AgentToolResult<unknown>> {
        const { action, ...rest } = params;
        return textResult(await client.request(action === "claim" ? "task.claim" : action === "list" ? "task.list" : action === "show" ? "task.show" : action === "create" ? "task.create" : "task.update", action === "list" || action === "show" ? rest : { ...rest, action }));
      },
    },
    {
      name: "agent_resource",
      label: "Agent resource",
      description: "Define, inspect, snapshot, claim, borrow, transfer, grant, release, or check a hierarchical resource.",
      parameters: Type.Object({ action: Type.String(), ...sharedResourceFields }),
      async execute(_toolCallId, params: any): Promise<AgentToolResult<unknown>> {
        const { action, ...rest } = params;
        const operation = action === "define" ? "resource.define" : action === "inspect" ? "resource.inspect" : action === "snapshot" ? "resource.snapshot" : action === "grant" ? "resource.grant" : action === "claim" || action === "own" ? "resource.claim" : action === "borrow" ? "resource.borrow" : action === "transfer" ? "resource.transfer" : action === "release" ? "resource.release" : action === "check_write" ? "resource.check_write" : action === "list" ? "resource.list" : undefined;
        if (!operation) throw new FabricError("INVALID_ARGUMENT", `Unknown resource action ${action}`);
        return textResult(await client.request(operation, rest));
      },
    },
    {
      name: "agent_spawn",
      label: "Agent spawn",
      description: "Ask the coordinator to create a bounded child agent. The child starts independently; this call does not wait for its model turn.",
      parameters: Type.Object({
        role: Type.Optional(Type.String()),
        provider: Type.Optional(Type.String()),
        model: Type.Optional(Type.String()),
        thinking: Type.Optional(Type.String()),
        taskDescription: Type.Optional(Type.String()),
        taskId: Type.Optional(Type.String()),
        workspace: Type.Optional(Type.Union([Type.Literal("shared"), Type.Literal("worktree")])),
        baseRef: Type.Optional(Type.String()),
        maySpawn: Type.Optional(Type.Boolean()),
        mayMessagePeers: Type.Optional(Type.Boolean()),
        mayEscalate: Type.Optional(Type.Boolean()),
        mayTransferOwnership: Type.Optional(Type.Boolean()),
        mayWriteRepo: Type.Optional(Type.Boolean()),
        mayUseShell: Type.Optional(Type.Boolean()),
      }),
      async execute(_toolCallId, params: SpawnToolInput, _signal, _onUpdate, ctx: any): Promise<AgentToolResult<unknown>> {
        if (!options.spawn) throw new FabricError("CAPABILITY_DENIED", "This agent cannot spawn children");
        return textResult(await options.spawn(params, ctx?.model ?? options.parentModel, ctx?.thinkingLevel ?? options.parentThinking));
      },
    },
  ];
}

export function routeFromSpawnInput(input: SpawnToolInput, fallback: ModelRoute): { route: ModelRoute; capabilities: Record<string, boolean> } {
  const route: ModelRoute = {
    provider: input.provider ?? fallback.provider,
    model: input.model ?? fallback.model,
    thinking: (input.thinking as ModelRoute["thinking"] | undefined) ?? fallback.thinking,
  };
  const capabilities: Record<string, boolean> = {};
  for (const key of ["maySpawn", "mayMessagePeers", "mayEscalate", "mayTransferOwnership", "mayWriteRepo", "mayUseShell"] as const) {
    if (input[key] !== undefined) capabilities[key] = Boolean(input[key]);
  }
  return { route, capabilities };
}
