import test from "node:test";
import assert from "node:assert/strict";
import { classifyRootDelivery } from "../src/pi/delivery.ts";
import { MESSAGE_TYPES, type AgentMessage, type MessageType } from "../src/core/types.ts";

function makeMessage(type: MessageType, priority: "normal" | "urgent" = "normal"): AgentMessage {
  return {
    id: "msg-1",
    from: "agent-1",
    to: "root",
    type,
    body: "test message body",
    priority,
    senderSequence: 1,
    createdAt: Date.now(),
  };
}

test("message-delivery: table-driven classification over all MESSAGE_TYPES", () => {
  const expectedPolicy: Record<MessageType, { triggerTurn: boolean; deliverAs: "steer" | "followUp" }> = {
    progress: { triggerTurn: false, deliverAs: "followUp" },
    inform: { triggerTurn: false, deliverAs: "followUp" },
    clarification: { triggerTurn: true, deliverAs: "followUp" },
    decision_request: { triggerTurn: true, deliverAs: "followUp" },
    escalation: { triggerTurn: true, deliverAs: "followUp" },
    blocked: { triggerTurn: true, deliverAs: "followUp" },
    request: { triggerTurn: true, deliverAs: "followUp" },
    response: { triggerTurn: true, deliverAs: "followUp" },
    resource_request: { triggerTurn: true, deliverAs: "followUp" },
    resource_granted: { triggerTurn: false, deliverAs: "followUp" },
    result: { triggerTurn: true, deliverAs: "followUp" },
    task_result: { triggerTurn: true, deliverAs: "followUp" },
    handoff: { triggerTurn: true, deliverAs: "followUp" },
    agent_failed: { triggerTurn: true, deliverAs: "followUp" },
    cancel: { triggerTurn: true, deliverAs: "followUp" },
    steer: { triggerTurn: true, deliverAs: "steer" },
  };

  for (const type of MESSAGE_TYPES) {
    const message = makeMessage(type);
    const decision = classifyRootDelivery(message);
    const expected = expectedPolicy[type];

    assert.equal(
      decision.triggerTurn,
      expected.triggerTurn,
      `Expected triggerTurn=${expected.triggerTurn} for type=${type}, got ${decision.triggerTurn}`
    );
    assert.equal(
      decision.deliverAs,
      expected.deliverAs,
      `Expected deliverAs=${expected.deliverAs} for type=${type}, got ${decision.deliverAs}`
    );
    assert.equal(decision.modelVisible, true);
    assert.equal(decision.display, true);
  }
});

test("message-delivery: urgent priority always becomes steer + triggerTurn regardless of type", () => {
  for (const type of MESSAGE_TYPES) {
    const message = makeMessage(type, "urgent");
    const decision = classifyRootDelivery(message);

    assert.equal(decision.triggerTurn, true, `Urgent priority must triggerTurn for type=${type}`);
    assert.equal(decision.deliverAs, "steer", `Urgent priority must deliverAs steer for type=${type}`);
    assert.equal(decision.modelVisible, true);
    assert.equal(decision.display, true);
  }
});

test("message-delivery: resource_granted triggers turn only when resolving pending root request", () => {
  const message = makeMessage("resource_granted");

  // Default: unsolicited resource_granted does not trigger turn
  const normalDecision = classifyRootDelivery(message, { hasPendingRootRequest: false });
  assert.equal(normalDecision.triggerTurn, false);
  assert.equal(normalDecision.deliverAs, "followUp");

  // When resolving a pending root request: triggers turn
  const resolvingDecision = classifyRootDelivery(message, { hasPendingRootRequest: true });
  assert.equal(resolvingDecision.triggerTurn, true);
  assert.equal(resolvingDecision.deliverAs, "followUp");
});
