export class LifecycleQueue {
  private tail: Promise<void> = Promise.resolve();
  private generation = 0;

  beginSession(): number {
    this.generation += 1;
    return this.generation;
  }

  get currentGeneration(): number {
    return this.generation;
  }

  enqueue(generation: number, operation: () => Promise<void>): Promise<void> {
    const next = this.tail.then(async () => {
      if (generation !== this.generation) return;
      await operation();
    });
    this.tail = next.catch(() => undefined);
    return next;
  }

  shutdown(): Promise<void> {
    this.generation += 1;
    const pending = this.tail;
    this.tail = Promise.resolve();
    return pending;
  }
}
