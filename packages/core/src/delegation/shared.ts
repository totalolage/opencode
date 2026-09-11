import { eq } from "drizzle-orm"
import { Effect } from "effect"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { SessionID } from "@opencode-ai/schema/session-id"
import { Database } from "../database/database"
import { SessionSchema } from "../session/schema"
import {
  DelegationGenerationTable,
  DelegationResolutionTable,
  DelegationRevocationTable,
} from "./sql"

export type DB = Database.Interface["db"]
export type Transaction = Parameters<Parameters<DB["transaction"]>[0]>[0]

export function generation(row: typeof DelegationGenerationTable.$inferSelect): Delegation.Generation {
  return Delegation.Generation.make({
    id: Delegation.ID.make(row.id),
    parentID: SessionID.make(row.parent_id),
    childID: SessionID.make(row.child_id),
    origin: {
      messageID: SessionMessage.ID.make(row.origin.messageID),
      partID: Delegation.OriginPartID.make(row.origin.partID),
      callID: row.origin.callID,
    },
    ...(row.parent_generation_id === null
      ? {}
      : { parentGenerationID: Delegation.ID.make(row.parent_generation_id) }),
    mode: row.mode,
    state: row.state,
    timeCreated: row.time_created,
    ...(row.time_closed === null ? {} : { timeClosed: row.time_closed }),
  })
}

export function resolution(row: typeof DelegationResolutionTable.$inferSelect): Delegation.Resolution {
  return Delegation.Resolution.make({
    id: Delegation.ResolutionID.make(row.id),
    generationID: Delegation.ID.make(row.generation_id),
    source: {
      kind: row.source_kind,
      id: row.source_id,
    },
    payload: row.payload,
    outcome: row.outcome,
    historyCutoff: row.history_cutoff,
    consumed: row.consumed.map((id) => Delegation.ResolutionID.make(id)),
    parentID: SessionID.make(row.parent_id),
    childID: SessionID.make(row.child_id),
    ...(row.recipient_generation_id === null
      ? {}
      : { recipientGenerationID: Delegation.ID.make(row.recipient_generation_id) }),
    messageID: SessionMessage.ID.make(row.message_id),
    timeCreated: row.time_created,
    status: row.status,
    ...(row.time_admitted === null ? {} : { timeAdmitted: row.time_admitted }),
    ...(row.time_consumed === null ? {} : { timeConsumed: row.time_consumed }),
    ...(row.time_resolved === null ? {} : { timeResolved: row.time_resolved }),
    ...(row.resolved_source_id === null
      ? {}
      : { resolvedSourceID: Delegation.SourceID.make(row.resolved_source_id) }),
    ...(row.envelope === null ? {} : { envelope: row.envelope }),
  })
}

export const requireGeneration = Effect.fn("Delegation.requireGeneration")(function* (
  tx: Transaction,
  id: Delegation.ID,
) {
  const row = yield* tx.select().from(DelegationGenerationTable).where(eq(DelegationGenerationTable.id, id)).get()
  if (row !== undefined) return generation(row)
  return yield* new Delegation.Error({
    code: "generation_not_found",
    message: `Delegation generation not found: ${id}`,
  })
})

export const requireActive = Effect.fn("Delegation.requireActive")(function* (
  tx: Transaction,
  id: Delegation.ID,
) {
  const initial = yield* requireGeneration(tx, id)
  const visited = new Set<Delegation.ID>()
  let current = initial
  let expectedChildID: SessionSchema.ID | undefined

  while (true) {
    if (visited.has(current.id)) {
      return yield* new Delegation.Error({
        code: "generation_ancestry_cycle",
        message: `Delegation generation ancestry contains a cycle at ${current.id}`,
      })
    }
    visited.add(current.id)

    if (current.parentID === current.childID) {
      return yield* new Delegation.Error({
        code: "generation_ancestry_cycle",
        message: `Delegation generation cannot parent itself: ${current.id}`,
      })
    }
    if (expectedChildID !== undefined && current.childID !== expectedChildID) {
      return yield* new Delegation.Error({
        code: "generation_ancestry_mismatch",
        message: `Delegation generation ${current.id} does not own session ${expectedChildID}`,
      })
    }
    if (current.state !== "active") {
      return yield* new Delegation.Error({
        code: "generation_not_active",
        message: `Delegation generation is not active: ${current.id}`,
      })
    }

    const fence = yield* tx
      .select({ sessionID: DelegationRevocationTable.session_id })
      .from(DelegationRevocationTable)
      .where(eq(DelegationRevocationTable.session_id, current.parentID))
      .get()
    if (fence !== undefined) {
      return yield* new Delegation.Error({
        code: "session_revoked",
        message: `Delegation parent session is fenced: ${current.parentID}`,
      })
    }

    if (current.parentGenerationID === undefined) return initial
    expectedChildID = current.parentID
    current = yield* requireGeneration(tx, current.parentGenerationID)
  }
})
