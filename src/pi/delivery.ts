import type { AgentMessage } from "../core/types.ts";

export interface RootDeliveryDecision {
  triggerTurn: boolean;
  deliverAs: "steer" | "followUp" | "nextTurn";
  modelVisible: boolean;
  display: boolean;
}

export interface RootDeliveryContext {
  hasPendingRootRequest?: boolean;
  /** Root session is currently compacting; durable delivery must not wake it. */
  rootCompactionInFlight?: boolean;
  /** Provider/context is degraded after an exhausted capacity failure. */
  rootContextHealth?: "healthy" | "degraded";
}

/**
 * Pure policy for classifying fabric message delivery to the root Pi session.
 * High-signal messages (urgent, escalation, blocked, clarification, decisions, results)
 * trigger model turns, while routine informational messages (progress, inform)
 * remain model-visible and displayed without waking the root model unnecessarily.
 */
export function classifyRootDelivery(
  message: AgentMessage,
  state?: RootDeliveryContext,
): RootDeliveryDecision {
  if (state?.rootCompactionInFlight || state?.rootContextHealth === "degraded") {
    return {
      triggerTurn: false,
      deliverAs: "nextTurn",
      modelVisible: true,
      display: true,
    };
  }
  if (message.priority === "urgent") {
    return {
      triggerTurn: true,
      deliverAs: "steer",
      modelVisible: true,
      display: true,
    };
  }

  switch (message.type) {
    case "progress":
    case "inform":
      return {
        triggerTurn: false,
        deliverAs: "followUp",
        modelVisible: true,
        display: true,
      };

    case "resource_granted":
      return {
        triggerTurn: true,
        deliverAs: "steer",
        modelVisible: true,
        display: true,
      };

    case "steer":
      return {
        triggerTurn: true,
        deliverAs: "steer",
        modelVisible: true,
        display: true,
      };

    case "clarification":
    case "decision_request":
    case "escalation":
    case "blocked":
    case "request":
    case "response":
    case "resource_request":
    case "result":
    case "task_result":
    case "handoff":
    case "agent_failed":
    case "cancel":
    default:
      return {
        triggerTurn: true,
        deliverAs: "followUp",
        modelVisible: true,
        display: true,
      };
  }
}
