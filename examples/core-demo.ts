import { Coordinator } from "../src/core/coordinator.ts";

const fabric = new Coordinator({ rootId: "demo" });
fabric.dispatch("root", "agent.register", {
  rootId: "demo",
  role: "root",
  route: { provider: "local", model: "qwen", thinking: "medium" },
  capabilities: { maySpawn: true, mayMessagePeers: true, mayWriteRepo: true },
});

const task = fabric.dispatch("root", "task.create", { description: "inspect the API" }).value;
const child = fabric.dispatch("root", "agent.spawn", {
  role: "scout",
  route: { provider: "local", model: "qwen", thinking: "low" },
  taskId: task.id,
  capabilities: { mayMessagePeers: true, mayWriteRepo: false, mayUseShell: false },
}).value;

console.log(JSON.stringify({ child: child.agent.id, task: child.taskId, reconnectCredentialIssued: Boolean(child.token) }, null, 2));
