import test from "node:test";
import assert from "node:assert/strict";
import { createCoordinationTools } from "../src/pi/tools.ts";

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
