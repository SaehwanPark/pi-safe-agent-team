import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import { platform } from "node:os";
import { dirname, join } from "node:path";
import type { RetainedArtifactRecord } from "../core/types.ts";

interface RetainedArtifactManifest {
  version: 1;
  records: RetainedArtifactRecord[];
}

function cloneRecords(records: readonly RetainedArtifactRecord[]): RetainedArtifactRecord[] {
  return records.map((record) => ({
    ...record,
    workspace: record.workspace ? { ...record.workspace } : undefined,
  }));
}

/**
 * Durable cold metadata for retained child artifacts.
 *
 * The coordinator still has a small in-process lookup cache so resolve/inspect
 * operations remain synchronous, but the broker never copies this collection
 * into its per-request rollback image or its journal checkpoint. Writes are
 * atomic and ordered independently of the event journal.
 */
export class RetainedArtifactStore {
  readonly directory: string;
  readonly filePath: string;
  private recordsById = new Map<string, RetainedArtifactRecord>();
  private tail: Promise<void> = Promise.resolve();

  constructor(options: { directory: string; filename?: string }) {
    this.directory = options.directory;
    this.filePath = join(options.directory, options.filename ?? "retained-artifacts.json");
  }

  async open(): Promise<void> {
    await fs.mkdir(this.directory, { recursive: true, mode: 0o700 });
    await fs.chmod(this.directory, 0o700).catch(() => undefined);
    try {
      const raw = await fs.readFile(this.filePath, "utf8");
      if (!raw.trim()) return;
      const parsed = JSON.parse(raw) as RetainedArtifactManifest | RetainedArtifactRecord[];
      const records = Array.isArray(parsed) ? parsed : parsed?.version === 1 && Array.isArray(parsed.records) ? parsed.records : undefined;
      if (!records) throw new Error("retained artifact manifest must contain a records array");
      this.recordsById = new Map(records.map((record) => [record.id, { ...record, workspace: record.workspace ? { ...record.workspace } : undefined }]));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        await this.replace([]);
        return;
      }
      throw new Error(`Could not read retained artifact manifest: ${error instanceof Error ? error.message : String(error)}`);
    }
    await fs.chmod(this.filePath, 0o600).catch(() => undefined);
  }

  records(): RetainedArtifactRecord[] {
    return cloneRecords([...this.recordsById.values()]);
  }

  async replace(records: readonly RetainedArtifactRecord[]): Promise<void> {
    const next = cloneRecords(records);
    const operation = this.tail.then(async () => {
      await fs.mkdir(dirname(this.filePath), { recursive: true, mode: 0o700 });
      const temporary = `${this.filePath}.${process.pid}.${randomUUID()}.tmp`;
      const manifest: RetainedArtifactManifest = { version: 1, records: next };
      await fs.writeFile(temporary, `${JSON.stringify(manifest)}\n`, { encoding: "utf8", mode: 0o600 });
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
      this.recordsById = new Map(next.map((record) => [record.id, { ...record, workspace: record.workspace ? { ...record.workspace } : undefined }]));
    });
    this.tail = operation.catch(() => undefined);
    await operation;
  }
}
