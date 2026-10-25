import { createHash } from "node:crypto";

export type TaskVersionRef = `v_${string}`;

/** Stable model-facing version token. */
export function taskVersionRef(sourceRevision: string): TaskVersionRef {
  return `v_${createHash("sha256").update(sourceRevision).digest("hex").slice(0, 16)}`;
}
