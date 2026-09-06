import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import { dirname, join } from "node:path";
import type { Coordinator } from "../core/coordinator.ts";
import type { CoordinatorEvent, IdempotencyRecord, PersistedCoordinatorState } from "../core/types.ts";

export type JournalRecord =
  | { kind: "begin"; txId: string; at: number }
  | { kind: "events"; txId: string; events: CoordinatorEvent[]; idempotency?: IdempotencyRecord }
  | { kind: "commit"; txId: string; at: number }
  | { kind: "checkpoint"; state: PersistedCoordinatorState; at: number };

export interface JournalOptions {
  directory: string;
  filename?: string;
}

/**
 * Append-only, transaction-framed local journal. It intentionally stores
 * coordinator events rather than model transcripts or arbitrary client data.
 */
export class Journal {
  readonly directory: string;
  readonly filePath: string;
  private tail: Promise<void> = Promise.resolve();

  constructor(options: JournalOptions) {
    this.directory = options.directory;
    this.filePath = join(options.directory, options.filename ?? "events.jsonl");
  }

  async open(): Promise<void> {
    await fs.mkdir(this.directory, { recursive: true, mode: 0o700 });
    await fs.chmod(this.directory, 0o700).catch(() => undefined);
    try {
      await fs.access(this.filePath);
    } catch {
      await fs.writeFile(this.filePath, "", { encoding: "utf8", mode: 0o600 });
    }
    await fs.chmod(this.filePath, 0o600).catch(() => undefined);
  }

  async append(events: readonly CoordinatorEvent[], idempotency?: IdempotencyRecord, at = Date.now()): Promise<string> {
    const txId = `tx-${randomUUID()}`;
    const records: JournalRecord[] = [
      { kind: "begin", txId, at },
      { kind: "events", txId, events: events as CoordinatorEvent[], ...(idempotency ? { idempotency } : {}) },
      { kind: "commit", txId, at: Date.now() },
    ];
    await this.enqueueWrite(records);
    return txId;
  }

  async checkpoint(state: PersistedCoordinatorState, at = Date.now()): Promise<void> {
    const record: JournalRecord = { kind: "checkpoint", state, at };
    const operation = this.tail.then(async () => {
      await fs.mkdir(dirname(this.filePath), { recursive: true });
      const temporary = `${this.filePath}.${process.pid}.${randomUUID()}.tmp`;
      await fs.writeFile(temporary, `${JSON.stringify(record)}\n`, { encoding: "utf8", mode: 0o600 });
      await fs.chmod(temporary, 0o600).catch(() => undefined);
      const handle = await fs.open(temporary, "r+");
      try {
        await handle.sync();
      } finally {
        await handle.close();
      }
      await fs.rename(temporary, this.filePath);
    });
    this.tail = operation.catch(() => undefined);
    await operation;
  }

  async replay(coordinator: Coordinator): Promise<{ committedTransactions: number; ignoredTail: boolean; checkpoints: number }> {
    await this.open();
    const content = await fs.readFile(this.filePath, "utf8");
    if (!content) return { committedTransactions: 0, ignoredTail: false, checkpoints: 0 };
    const lines = content.split("\n");
    const hasTrailingNewline = content.endsWith("\n");
    if (hasTrailingNewline) lines.pop();
    const pending = new Map<string, { events: CoordinatorEvent[]; idempotency?: IdempotencyRecord }>();
    let committedTransactions = 0;
    let checkpoints = 0;
    let ignoredTail = false;

    for (let index = 0; index < lines.length; index += 1) {
      const raw = lines[index];
      if (!raw.trim()) continue;
      let record: JournalRecord;
      try {
        record = JSON.parse(raw) as JournalRecord;
      } catch (error) {
        if (index === lines.length - 1) {
          ignoredTail = true;
          break;
        }
        throw new Error(`Malformed journal record at line ${index + 1}: ${error instanceof Error ? error.message : String(error)}`);
      }
      if (record.kind === "checkpoint") {
        coordinator.restoreState(record.state);
        pending.clear();
        checkpoints += 1;
      } else if (record.kind === "begin") {
        pending.set(record.txId, { events: [] });
      } else if (record.kind === "events") {
        const transaction = pending.get(record.txId);
        if (!transaction) throw new Error(`Journal events record ${record.txId} has no begin`);
        transaction.events.push(...record.events);
        if (record.idempotency) transaction.idempotency = record.idempotency;
      } else if (record.kind === "commit") {
        const transaction = pending.get(record.txId);
        if (!transaction) throw new Error(`Journal commit record ${record.txId} has no begin`);
        coordinator.applyEvents(transaction.events);
        if (transaction.idempotency) coordinator.restoreIdempotency(transaction.idempotency);
        pending.delete(record.txId);
        committedTransactions += 1;
      } else {
        throw new Error(`Unknown journal record at line ${index + 1}`);
      }
    }
    // Any transaction left in pending has no commit marker and is deliberately ignored.
    if (pending.size > 0) ignoredTail = true;
    return { committedTransactions, ignoredTail, checkpoints };
  }

  private async enqueueWrite(records: JournalRecord[]): Promise<void> {
    const operation = this.tail.then(async () => {
      await fs.mkdir(dirname(this.filePath), { recursive: true });
      const payload = records.map((record) => JSON.stringify(record)).join("\n") + "\n";
      const handle = await fs.open(this.filePath, "a");
      try {
        await handle.write(payload, undefined, "utf8");
        await handle.sync();
      } finally {
        await handle.close();
      }
    });
    this.tail = operation.catch(() => undefined);
    await operation;
  }
}
