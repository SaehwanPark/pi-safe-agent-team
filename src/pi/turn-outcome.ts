import { isContextOverflow, type AssistantMessage } from "@earendil-works/pi-ai";

/** Lifecycle-relevant result of one logical Pi model run. */
export type ModelTurnOutcomeKind =
  | "success"
  | "aborted"
  | "context_overflow"
  | "compaction_failed"
  | "prefill_capacity"
  | "runtime_memory_pressure"
  | "transient_error_exhausted"
  | "fatal_provider";

export interface ModelTurnOutcome {
  kind: ModelTurnOutcomeKind;
  lifecycle: "healthy" | "ready" | "blocked" | "failed";
  stopReason?: string;
  errorMessage?: string;
  provider?: string;
  model?: string;
  contextTokens?: number;
  contextWindow?: number;
}

interface AssistantLike {
  role?: unknown;
  stopReason?: unknown;
  errorMessage?: unknown;
  provider?: unknown;
  model?: unknown;
  usage?: { input?: unknown; cacheRead?: unknown; output?: unknown };
  diagnostics?: ReadonlyArray<{ error?: { message?: unknown; code?: unknown }; details?: Record<string, unknown> }>;
}

const CAPACITY_PATTERNS = [
  /prefill[_ -]?memory[_ -]?exceeded/i,
  /prefill.{0,80}(?:memory|kv|cache).{0,80}(?:exceed|insufficient|full|allocat|capacity)/i,
  /(?:kv|key.value).{0,80}(?:cache|memory).{0,80}(?:exceed|insufficient|full|allocat|capacity)/i,
  /(?:memory|ram|gpu).{0,80}(?:insufficient|allocat|capacity)/i,
  /failed to allocate.{0,80}(?:memory|kv|cache)/i,
];

const RUNTIME_MEMORY_PATTERNS = [
  /out of memory/i,
  /\boom\b/i,
  /memory pressure/i,
  /model (?:was )?evict(?:ed|ion)/i,
  /process memory pressure/i,
  /cuda.*out of memory/i,
  /metal.*memory/i,
];

const TRANSIENT_PATTERNS = [
  /rate limit/i,
  /too many requests/i,
  /(?:service|server) unavailable/i,
  /temporarily unavailable/i,
  /overloaded/i,
  /timeout/i,
  /timed out/i,
  /\b(?:502|503|504|529)\b/,
  /connection (?:reset|closed| refused)/i,
  /network error/i,
];

function text(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function usageNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined;
}

function diagnosticText(message: AssistantLike): string | undefined {
  const direct = text(message.errorMessage);
  if (direct) return direct;
  for (const diagnostic of message.diagnostics ?? []) {
    const candidate = text(diagnostic.error?.message) ?? text((diagnostic as { message?: unknown }).message);
    const code = text(diagnostic.error?.code);
    if (candidate || code) return [code, candidate].filter(Boolean).join(": ");
  }
  return undefined;
}

function failureKind(errorMessage: string | undefined): ModelTurnOutcomeKind {
  if (!errorMessage) return "fatal_provider";
  if (RUNTIME_MEMORY_PATTERNS.some((pattern) => pattern.test(errorMessage))) return "runtime_memory_pressure";
  if (CAPACITY_PATTERNS.some((pattern) => pattern.test(errorMessage))) return "prefill_capacity";
  if (TRANSIENT_PATTERNS.some((pattern) => pattern.test(errorMessage))) return "transient_error_exhausted";
  return "fatal_provider";
}

function outcome(
  kind: ModelTurnOutcomeKind,
  message: AssistantLike,
  contextWindow?: number,
  errorMessage?: string,
): ModelTurnOutcome {
  const contextTokens = (usageNumber(message.usage?.input) ?? 0) + (usageNumber(message.usage?.cacheRead) ?? 0);
  const provider = text(message.provider);
  const model = text(message.model);
  const lifecycle = kind === "success" ? "healthy" : kind === "aborted" ? "ready" : kind === "fatal_provider" ? "failed" : "blocked";
  return {
    kind,
    lifecycle,
    stopReason: text(message.stopReason),
    errorMessage,
    provider,
    model,
    contextTokens: contextTokens > 0 ? contextTokens : undefined,
    contextWindow: contextWindow && Number.isFinite(contextWindow) && contextWindow > 0 ? contextWindow : undefined,
  };
}

/** Classify a final assistant response without changing Pi's native recovery policy. */
export function classifyAssistantMessage(message: unknown, contextWindow?: number): ModelTurnOutcome {
  const candidate = (message && typeof message === "object" ? message : {}) as AssistantLike;
  const stopReason = text(candidate.stopReason);
  const errorMessage = diagnosticText(candidate);
  if (stopReason === "aborted") return outcome("aborted", candidate, contextWindow, errorMessage);
  if (stopReason === "error") {
    let kind = failureKind(errorMessage);
    // Pi's provider-aware detector remains authoritative for genuine logical
    // context overflow. Runtime-memory and prefill-capacity patterns are
    // handled first so backend pressure is never mislabeled as a token-window
    // overflow.
    if (kind === "fatal_provider" && errorMessage) {
      try {
        if (isContextOverflow(candidate as AssistantMessage, contextWindow)) kind = "context_overflow";
      } catch {
        // A malformed provider response must still become a visible failure.
      }
    }
    return outcome(kind, candidate, contextWindow, errorMessage);
  }
  try {
    if (isContextOverflow(candidate as AssistantMessage, contextWindow)) return outcome("context_overflow", candidate, contextWindow, errorMessage);
  } catch {
    // Fall through to the conservative success/length handling below.
  }
  return outcome("success", candidate, contextWindow, errorMessage);
}

/** Convert a failed compaction event into the same lifecycle taxonomy. */
export function classifyCompactionFailure(errorMessage?: string, aborted = false, contextWindow?: number): ModelTurnOutcome {
  const candidate: AssistantLike = { role: "assistant", stopReason: aborted ? "aborted" : "error", errorMessage };
  if (aborted) return outcome("aborted", candidate, contextWindow, errorMessage);
  return outcome("compaction_failed", candidate, contextWindow, errorMessage ?? "Pi compaction failed after the native recovery attempt");
}

export function findFinalAssistantMessage(messages: readonly unknown[]): unknown | undefined {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message && typeof message === "object" && (message as AssistantLike).role === "assistant") return message;
  }
  return undefined;
}

export function isBlockingOutcome(outcomeValue: ModelTurnOutcome): boolean {
  return outcomeValue.lifecycle === "blocked" || outcomeValue.lifecycle === "failed";
}

/** Stable bounded text suitable for statusReason and parent handoff messages. */
export function describeTurnOutcome(outcomeValue: ModelTurnOutcome): string {
  const route = outcomeValue.provider && outcomeValue.model ? ` provider/model=${outcomeValue.provider}/${outcomeValue.model}` : "";
  const context = outcomeValue.contextTokens !== undefined ? ` contextTokens=${outcomeValue.contextTokens}${outcomeValue.contextWindow ? `/${outcomeValue.contextWindow}` : ""}` : "";
  const detail = outcomeValue.errorMessage ? `: ${outcomeValue.errorMessage.replace(/[\u0000-\u001f\u007f]/g, " ").slice(0, 1200)}` : "";
  return `model turn ${outcomeValue.kind}${route}${context}${detail}`.slice(0, 1900);
}
