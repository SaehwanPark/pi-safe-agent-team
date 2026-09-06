export type FabricErrorCode =
  | "PROTOCOL_VERSION_UNSUPPORTED"
  | "INVALID_ARGUMENT"
  | "IDENTITY_CONFLICT"
  | "CAPABILITY_DENIED"
  | "AGENT_NOT_FOUND"
  | "AGENT_LIMIT_REACHED"
  | "MAILBOX_FULL"
  | "LIFECYCLE_CONFLICT"
  | "TASK_NOT_FOUND"
  | "TASK_BUSY"
  | "TASK_BLOCKED"
  | "TASK_NOT_OWNER"
  | "RESOURCE_NOT_FOUND"
  | "RESOURCE_CONFLICT"
  | "RESOURCE_NOT_OWNER"
  | "LEASE_EXPIRED"
  | "MESSAGE_NOT_FOUND"
  | "MESSAGE_NOT_VISIBLE"
  | "DUPLICATE_REQUEST"
  | "REQUEST_NOT_FOUND"
  | "REQUEST_ALREADY_RESOLVED"
  | "BROKER_UNAVAILABLE"
  | "PERSISTENCE_FAILURE"
  | "IDEMPOTENCY_CONFLICT"
  | "WORKSPACE_FAILURE"
  | "MODEL_NOT_FOUND"
  | "MODEL_ROUTE_INVALID"
  | "CHILD_SESSION_FAILURE";

export interface FabricErrorShape {
  code: FabricErrorCode;
  message: string;
  details?: Record<string, unknown>;
}

export class FabricError extends Error {
  readonly code: FabricErrorCode;
  readonly details?: Record<string, unknown>;

  constructor(code: FabricErrorCode, message: string, details?: Record<string, unknown>) {
    super(message);
    this.name = "FabricError";
    this.code = code;
    this.details = details;
  }

  toJSON(): FabricErrorShape {
    return { code: this.code, message: this.message, details: this.details };
  }
}

export function asFabricError(error: unknown, fallbackCode: FabricErrorCode = "INVALID_ARGUMENT"): FabricError {
  if (error instanceof FabricError) return error;
  if (error instanceof Error) return new FabricError(fallbackCode, error.message);
  return new FabricError(fallbackCode, String(error));
}

export function assertCondition(condition: unknown, code: FabricErrorCode, message: string, details?: Record<string, unknown>): asserts condition {
  if (!condition) throw new FabricError(code, message, details);
}
