import test from "node:test";
import assert from "node:assert/strict";
import { createCoordinationTools } from "../src/pi/tools.ts";
import { FabricRuntime } from "../src/pi/runtime.ts";

test("clarification tool returns a correlated request and terminate hint instead of waiting", async () => {
  const calls: Array<{ operation: string; args: Record<string, unknown> }> = [];
  const client = {
    async request<T = unknown>(operation: string, args: Record<string, unknown> = {}): Promise<T> {
      calls.push({ operation, args });
      return { message: { id: "message-1" }, request: { id: "request-1" } } as T;
    },
  };
  let requestId: string | undefined;
  const tool = createCoordinationTools({ client, onClarification: (id) => { requestId = id; } }).find((candidate) => candidate.name === "agent_send");
  assert.ok(tool);
  const result = await tool.execute("call-1", { to: "parent", type: "clarification", body: "need an answer" }, undefined, undefined, {} as never);
  assert.equal(calls[0].operation, "message.send");
  assert.equal(calls[0].args.expectsReply, true);
  assert.equal(requestId, "request-1");
  assert.equal(result.terminate, true);
});

test("root agent_task create invokes requestIdempotent through the root lazyClient binding", async () => {
  const idempotentCalls: Array<{ operation: string; args: Record<string, unknown>; operationId?: string; timeoutMs?: number }> = [];
  const normalCalls: Array<{ operation: string; args: Record<string, unknown> }> = [];

  const mockClient = {
    async request<T = unknown>(operation: string, args: Record<string, unknown> = {}): Promise<T> {
      normalCalls.push({ operation, args });
      return { task: { id: "task-1" } } as T;
    },
    async requestIdempotent<T = unknown>(operation: string, args: Record<string, unknown> = {}, operationId?: string, timeoutMs?: number): Promise<T> {
      idempotentCalls.push({ operation, args, operationId, timeoutMs });
      return { task: { id: "task-idempotent-1" } } as T;
    },
  };

  const runtime = new FabricRuntime();
  (runtime as any).root = { client: mockClient };

  const lazyClient = {
    request: <T = unknown>(operation: string, args: Record<string, unknown> = {}) => runtime.request<T>(operation, args),
    requestIdempotent: <T = unknown>(operation: string, args: Record<string, unknown> = {}, operationId?: string, timeoutMs?: number) =>
      runtime.requestIdempotent<T>(operation, args, operationId, timeoutMs),
  };

  const tools = createCoordinationTools({ client: lazyClient });
  const taskTool = tools.find((tool) => tool.name === "agent_task");
  assert.ok(taskTool);

  const result = await taskTool.execute("call-1", { action: "create", description: "Audit item test" }, undefined, undefined, {} as never);
  assert.equal(idempotentCalls.length, 1);
  assert.equal(idempotentCalls[0].operation, "task.create");
  assert.equal(idempotentCalls[0].args.description, "Audit item test");
  assert.equal(normalCalls.length, 0);
  assert.deepEqual(result.details, { task: { id: "task-idempotent-1" } });

  // Non-create actions like list still use normal request
  await taskTool.execute("call-2", { action: "list" }, undefined, undefined, {} as never);
  assert.equal(normalCalls.length, 1);
  assert.equal(normalCalls[0].operation, "task.list");
});
