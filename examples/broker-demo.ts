import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BrokerClient } from "../src/broker/client.ts";
import { BrokerServer } from "../src/broker/server.ts";

const directory = await mkdtemp(join(tmpdir(), "safe-agents-demo-"));
const server = new BrokerServer({ directory, rootId: "demo" });
const root = new BrokerClient({ endpoint: server.endpoint, agentId: "root" });
try {
  await server.start();
  await root.connect();
  await root.request("agent.register", {
    rootId: "demo",
    route: { provider: "local", model: "qwen", thinking: "medium" },
    capabilities: { maySpawn: true, mayMessagePeers: true },
  });
  const child = await root.request<{ agent: { id: string }; token: string }>("agent.spawn", {
    route: { provider: "local", model: "qwen", thinking: "low" },
    taskDescription: "send a hello",
  });
  const childClient = new BrokerClient({ endpoint: server.endpoint, agentId: child.agent.id, token: child.token });
  await childClient.connect();
  await childClient.request("agent.register", { rootId: "demo", parentId: "root", route: { provider: "local", model: "qwen", thinking: "low" }, token: child.token });
  await root.request("message.send", { to: child.agent.id, type: "inform", body: "hello from the broker" });
  console.log(await childClient.request("message.inbox", {}));
  childClient.close();
} finally {
  root.close();
  await server.stop();
  await rm(directory, { recursive: true, force: true });
}
