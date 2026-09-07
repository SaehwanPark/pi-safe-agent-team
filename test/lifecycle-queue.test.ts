import test from "node:test";
import assert from "node:assert/strict";
import { LifecycleQueue } from "../src/pi/lifecycle.ts";

test("lifecycle queue serializes operations and drops stale generations", async () => {
  const queue = new LifecycleQueue();
  const generation = queue.beginSession();
  const events: string[] = [];
  let releaseFirst!: () => void;
  const firstReleased = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });

  const first = queue.enqueue(generation, async () => {
    events.push("first-start");
    await firstReleased;
    events.push("first-end");
  });
  const stale = queue.enqueue(generation, async () => {
    events.push("stale");
  });

  await new Promise<void>((resolve) => setImmediate(resolve));
  const shutdown = queue.shutdown();
  releaseFirst();
  await shutdown;
  await first;
  await stale;

  assert.deepEqual(events, ["first-start", "first-end"]);
});

test("a new session gets a fresh generation after shutdown", async () => {
  const queue = new LifecycleQueue();
  const firstGeneration = queue.beginSession();
  await queue.shutdown();
  const secondGeneration = queue.beginSession();
  const events: string[] = [];

  await queue.enqueue(firstGeneration, async () => {
    events.push("stale");
  });
  await queue.enqueue(secondGeneration, async () => {
    events.push("current");
  });

  assert.deepEqual(events, ["current"]);
});
