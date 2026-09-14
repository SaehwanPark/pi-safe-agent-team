import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import { platform } from "node:os";
import { dirname, join } from "node:path";
import type { AgentId, AgentMessage, MessageAckProofLookup, MessageAckTombstone, MessageId } from "../core/types.ts";
import type { CoordinatorEvent } from "../core/types.ts";

type AckProofRecord = {
  version: 1;
  id: MessageId;
  to: AgentId;
  revision: number;
  acknowledgedAt: number;
};

function key(to: AgentId, id: MessageId, revision: number): string {
  return `${to}\u0000${id}\u0000${revision}`;
}

function messageKey(to: AgentId, id: MessageId): string {
  return `${to}\u0000${id}`;
}

function cloneProof(proof: MessageAckTombstone): MessageAckTombstone {
  return { id: proof.id, to: proof.to, revision: proof.revision, acknowledgedAt: proof.acknowledgedAt, acknowledged: true };
}

function toRecord(proof: MessageAckTombstone): AckProofRecord {
  return {
    version: 1,
    id: proof.id,
    to: proof.to,
    revision: proof.revision,
    acknowledgedAt: proof.acknowledgedAt,
  };
}

function parseRecord(value: unknown): MessageAckTombstone {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("ACK proof record must be an object");
  const record = value as Partial<AckProofRecord>;
  if (record.version !== 1 || typeof record.id !== "string" || record.id.length === 0 || record.id.length > 512 || record.id.includes("\u0000")) {
    throw new Error("ACK proof record has an invalid version or message ID");
  }
  if (typeof record.to !== "string" || record.to.length === 0 || record.to.length > 512 || record.to.includes("\u0000")) {
    throw new Error("ACK proof record has an invalid recipient");
  }
  if (!Number.isInteger(record.revision) || (record.revision as number) <= 0 || (record.revision as number) > Number.MAX_SAFE_INTEGER) {
    throw new Error("ACK proof record has an invalid revision");
  }
  if (!Number.isFinite(record.acknowledgedAt)) throw new Error("ACK proof record has an invalid timestamp");
  return {
    id: record.id,
    to: record.to,
    revision: record.revision as number,
    acknowledgedAt: record.acknowledgedAt as number,
    acknowledged: true,
  };
}

/**
 * Append-oriented cold storage for exact mailbox ACK proofs.
 *
 * The coordinator uses this object only through its synchronous lookup index;
 * it never exports or clones the proof collection as part of a hot rollback
 * image. New proofs are queued only after their transaction is in the journal,
 * so a proof cannot make an uncommitted ACK appear successful.
 */
export class AckProofStore implements MessageAckProofLookup {
  readonly directory: string;
  readonly filePath: string;
  private proofsByKey = new Map<string, MessageAckTombstone>();
  private latestByMessage = new Map<string, MessageAckTombstone>();
  private pending = new Map<string, MessageAckTombstone>();
  private tail: Promise<void> = Promise.resolve();
  private recordsSinceCompaction = 0;

  constructor(options: { directory: string; filename?: string }) {
    this.directory = options.directory;
    this.filePath = join(options.directory, options.filename ?? "ack-proofs.jsonl");
  }

  async open(): Promise<void> {
    await fs.mkdir(this.directory, { recursive: true, mode: 0o700 });
    await fs.chmod(this.directory, 0o700).catch(() => undefined);
    this.proofsByKey.clear();
    this.latestByMessage.clear();
    this.pending.clear();
    this.recordsSinceCompaction = 0;
    try {
      const content = await fs.readFile(this.filePath, "utf8");
      const lines = content.split("\n");
      let validBytes = 0;
      for (let index = 0; index < lines.length; index += 1) {
        const line = lines[index]!;
        const lineBytes = Buffer.byteLength(line) + (index < lines.length - 1 ? 1 : 0);
        if (!line.trim()) {
          validBytes += lineBytes;
          continue;
        }
        try {
          this.remember(parseRecord(JSON.parse(line)));
          validBytes += lineBytes;
        } catch (error) {
          // A process crash can leave one partial final JSONL record. Repair
          // that tail before future appends; an invalid complete record in the
          // middle or a newline-terminated final record is corruption.
          const isPartialTail = index === lines.length - 1 && !content.endsWith("\n");
          if (!isPartialTail) throw new Error(`Malformed ACK proof record at line ${index + 1}: ${error instanceof Error ? error.message : String(error)}`);
          await fs.truncate(this.filePath, validBytes);
          break;
        }
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        throw new Error(`Could not read ACK proof store: ${error instanceof Error ? error.message : String(error)}`);
      }
      await this.ensureFile();
    }
    await fs.chmod(this.filePath, 0o600).catch(() => undefined);
  }

  findExact(to: AgentId, id: MessageId, revision: number): MessageAckTombstone | undefined {
    const proof = this.proofsByKey.get(key(to, id, revision)) ?? this.pending.get(key(to, id, revision));
    return proof ? cloneProof(proof) : undefined;
  }

  findMessage(to: AgentId, id: MessageId): MessageAckTombstone | undefined {
    const proof = this.latestByMessage.get(messageKey(to, id));
    return proof ? cloneProof(proof) : undefined;
  }

  /** Queue proofs after their journal transaction has been durably appended. */
  queue(proofs: readonly MessageAckTombstone[]): void {
    for (const proof of proofs) {
      const normalized = cloneProof(proof);
      const proofKey = key(normalized.to, normalized.id, normalized.revision);
      const alreadyDurable = this.proofsByKey.has(proofKey);
      this.remember(normalized);
      if (!alreadyDurable) this.pending.set(proofKey, normalized);
    }
  }

  queueFromEvents(events: readonly CoordinatorEvent[]): void {
    const proofs: MessageAckTombstone[] = [];
    for (const event of events) {
      if (event.type !== "message_acknowledged") continue;
      const message = event.message as AgentMessage;
      if (message.acknowledgedAt === undefined) continue;
      proofs.push({
        id: message.id,
        to: message.to,
        revision: message.revision ?? 1,
        acknowledgedAt: message.acknowledgedAt,
        acknowledged: true,
      });
    }
    this.queue(proofs);
  }

  hasPending(): boolean {
    return this.pending.size > 0;
  }

  /**
   * Flush queued records in one append and occasionally rewrite a compact
   * unique index. The normal ACK path therefore performs one append per
   * transaction batch, not a full manifest replacement per acknowledgement.
   */
  async flush(): Promise<void> {
    const operation = this.tail.then(async () => {
      const pending = [...this.pending.values()];
      if (pending.length === 0) return;
      await this.ensureFile();
      const payload = pending.map((proof) => JSON.stringify(toRecord(proof))).join("\n") + "\n";
      const handle = await fs.open(this.filePath, "a");
      try {
        await handle.write(payload, undefined, "utf8");
        await handle.sync();
      } finally {
        await handle.close();
      }
      this.recordsSinceCompaction += pending.length;
      for (const proof of pending) {
        const proofKey = key(proof.to, proof.id, proof.revision);
        if (this.pending.get(proofKey) === proof) this.pending.delete(proofKey);
      }
      const size = await this.fileSize();
      if (this.recordsSinceCompaction >= 4_096 || size >= 16 * 1024 * 1024) {
        await this.compact();
      }
    });
    this.tail = operation.catch(() => undefined);
    await operation;
  }

  private remember(proof: MessageAckTombstone): void {
    const normalized = cloneProof(proof);
    const proofKey = key(normalized.to, normalized.id, normalized.revision);
    this.proofsByKey.set(proofKey, normalized);
    const perMessageKey = messageKey(normalized.to, normalized.id);
    const current = this.latestByMessage.get(perMessageKey);
    if (!current || normalized.revision > current.revision || normalized.revision === current.revision && normalized.acknowledgedAt >= current.acknowledgedAt) {
      this.latestByMessage.set(perMessageKey, normalized);
    }
  }

  private async ensureFile(): Promise<void> {
    await fs.mkdir(dirname(this.filePath), { recursive: true, mode: 0o700 });
    try {
      await fs.access(this.filePath);
    } catch {
      await fs.writeFile(this.filePath, "", { encoding: "utf8", mode: 0o600 });
    }
  }

  private async fileSize(): Promise<number> {
    try {
      return (await fs.stat(this.filePath)).size;
    } catch {
      return 0;
    }
  }

  private async compact(): Promise<void> {
    const proofs = [...this.proofsByKey.values()].sort((left, right) => key(left.to, left.id, left.revision).localeCompare(key(right.to, right.id, right.revision)));
    const temporary = `${this.filePath}.${process.pid}.${randomUUID()}.tmp`;
    const payload = proofs.length === 0 ? "" : `${proofs.map((proof) => JSON.stringify(toRecord(proof))).join("\n")}\n`;
    await fs.writeFile(temporary, payload, { encoding: "utf8", mode: 0o600 });
    await fs.chmod(temporary, 0o600).catch(() => undefined);
    const handle = await fs.open(temporary, "r+");
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
    await fs.rename(temporary, this.filePath);
    if (platform() !== "win32") {
      const directory = await fs.open(dirname(this.filePath), "r");
      try {
        await directory.sync();
      } finally {
        await directory.close();
      }
    }
    this.recordsSinceCompaction = 0;
  }
}
