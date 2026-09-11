import { createHash } from "node:crypto"
import { eq, inArray } from "drizzle-orm"
import { Effect } from "effect"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { SessionTable } from "../session/sql"
import { DelegationResolutionTable, DelegationRevocationTable, DelegationSourceTable } from "./sql"
import { type Transaction, requireActive, resolution } from "./shared"

export function captureInTransaction(tx: Transaction, generationID: Delegation.ID, input: Delegation.Capture) {
  if (input.source.id.length === 0)
    return fail<Delegation.Resolution>("invalid_source", "Resolution source ID cannot be empty")
  if (input.historyCutoff.length === 0) {
    return fail<Delegation.Resolution>("invalid_cutoff", "Resolution history cutoff cannot be empty")
  }
  if (input.source.kind === "terminal" && input.outcome === "reply") {
    return fail<Delegation.Resolution>("invalid_outcome", "Terminal sources cannot produce reply outcomes")
  }
  if (new Set(input.consumed).size !== input.consumed.length) {
    return fail<Delegation.Resolution>("invalid_consumed", "Consumed resolution IDs must be unique")
  }

  return Effect.gen(function* () {
    const identity = stableIdentity(generationID, input.source)
    const existingRow = yield* tx
      .select()
      .from(DelegationResolutionTable)
      .where(eq(DelegationResolutionTable.id, identity.resolutionID))
      .get()
    if (existingRow !== undefined) {
      const existing = resolution(existingRow)
      if (!sameCapture(existing, input, identity, generationID)) {
        return yield* fail<Delegation.Resolution>(
          "identity_conflict",
          "Delegation resolution source identity already has different immutable content",
        )
      }
      return existing
    }

    const generation = yield* requireActive(tx, generationID)
    if (generation.mode !== "background") {
      return yield* fail<Delegation.Resolution>(
        "foreground_capture",
        "Only background generations can capture asynchronous returns",
      )
    }

    const parent = yield* tx
      .select({ id: SessionTable.id })
      .from(SessionTable)
      .where(eq(SessionTable.id, generation.parentID))
      .get()
    if (parent === undefined)
      return yield* fail<Delegation.Resolution>("recipient_missing", "Delegation parent session does not exist")

    const childFence = yield* tx
      .select({ id: DelegationRevocationTable.session_id })
      .from(DelegationRevocationTable)
      .where(eq(DelegationRevocationTable.session_id, generation.childID))
      .get()
    if (childFence !== undefined && input.outcome !== "cancelled") {
      return yield* fail<Delegation.Resolution>("child_revoked", "Revoked child generations cannot publish new returns")
    }

    const sourceID = stableSourceID(generation.childID, generationID, input.source)
    const source = yield* tx.select().from(DelegationSourceTable).where(eq(DelegationSourceTable.id, sourceID)).get()
    if (source === undefined) {
      return yield* fail<Delegation.Resolution>(
        "source_not_reserved",
        `Delegation source reservation was not found: ${sourceID}`,
      )
    }
    if (
      source.session_id !== generation.childID ||
      source.generation_id !== generationID ||
      source.source_kind !== input.source.kind ||
      source.source_id !== input.source.id
    ) {
      return yield* fail<Delegation.Resolution>(
        "source_mismatch",
        "Delegation source reservation does not match the capture",
      )
    }
    if (source.state !== "finalized") {
      return yield* fail<Delegation.Resolution>(
        "source_not_finalized",
        `Delegation source is not logically finalized: ${sourceID}`,
      )
    }
    if (
      source.payload !== input.payload ||
      source.outcome !== input.outcome ||
      source.history_cutoff !== input.historyCutoff ||
      !sameIDs(source.consumed, input.consumed)
    ) {
      return yield* fail<Delegation.Resolution>(
        "source_mismatch",
        "Delegation source finalization differs from the capture",
      )
    }

    const consumed = yield* resolvedContributions(
      tx,
      generation,
      generationID,
      source.id,
      input.consumed,
      childFence !== undefined && input.outcome === "cancelled",
    )
    const now = Date.now()
    yield* tx
      .insert(DelegationResolutionTable)
      .values({
        id: identity.resolutionID,
        generation_id: generationID,
        source_kind: input.source.kind,
        source_id: input.source.id,
        payload: input.payload,
        outcome: input.outcome,
        history_cutoff: input.historyCutoff,
        consumed,
        parent_id: generation.parentID,
        child_id: generation.childID,
        recipient_generation_id: generation.parentGenerationID ?? null,
        message_id: identity.messageID,
        time_created: now,
        status: "pending",
        time_admitted: null,
        time_consumed: null,
        time_resolved: null,
        resolved_source_id: null,
        envelope: null,
      })
      .run()

    const inserted = yield* tx
      .select()
      .from(DelegationResolutionTable)
      .where(eq(DelegationResolutionTable.id, identity.resolutionID))
      .get()
    if (inserted === undefined)
      return yield* fail<Delegation.Resolution>("insert_failed", "Delegation resolution was not persisted")
    return resolution(inserted)
  })
}

function resolvedContributions(
  tx: Transaction,
  generation: Delegation.Generation,
  generationID: Delegation.ID,
  sourceID: Delegation.SourceID,
  ids: readonly Delegation.ResolutionID[],
  allowRevoked: boolean,
) {
  if (ids.length === 0) return Effect.succeed(Array.from(ids))

  return Effect.gen(function* () {
    const rows = yield* tx
      .select()
      .from(DelegationResolutionTable)
      .where(inArray(DelegationResolutionTable.id, ids))
      .all()
    if (rows.length !== ids.length) {
      return yield* fail<Delegation.ResolutionID[]>(
        "consumed_missing",
        "A captured delegation resolution contribution does not exist",
      )
    }
    if (
      rows.some(
        (row) =>
          row.parent_id !== generation.childID ||
          row.recipient_generation_id !== generationID ||
          !contributionSettledBy(row, sourceID, allowRevoked),
      )
    ) {
      return yield* fail<Delegation.ResolutionID[]>(
        "source_not_resolved",
        "Captured contributions must be resolved by the matching finalized source",
      )
    }
    return Array.from(ids)
  })
}

function contributionSettledBy(
  row: typeof DelegationResolutionTable.$inferSelect,
  sourceID: Delegation.SourceID,
  allowRevoked: boolean,
) {
  if (row.resolved_source_id !== sourceID) return false
  if (row.status === "resolved") return true
  return allowRevoked && row.status === "revoked"
}

function sameCapture(
  existing: Delegation.Resolution,
  input: Delegation.Capture,
  identity: ReturnType<typeof stableIdentity>,
  generationID: Delegation.ID,
) {
  return (
    existing.id === identity.resolutionID &&
    existing.messageID === identity.messageID &&
    existing.generationID === generationID &&
    existing.source.kind === input.source.kind &&
    existing.source.id === input.source.id &&
    existing.payload === input.payload &&
    existing.outcome === input.outcome &&
    existing.historyCutoff === input.historyCutoff &&
    sameIDs(existing.consumed, input.consumed)
  )
}

function sameIDs(left: readonly string[], right: readonly string[]) {
  return left.length === right.length && left.every((id, index) => id === right[index])
}

function stableIdentity(generationID: Delegation.ID, source: Delegation.Source) {
  const digest = createHash("sha256")
    .update(JSON.stringify([generationID, source.kind, source.id]))
    .digest("hex")
  return {
    resolutionID: Delegation.ResolutionID.make(`res_${digest}`),
    messageID: SessionMessage.ID.make(`msg_${digest}`),
  }
}

function stableSourceID(
  sessionID: Delegation.Generation["childID"],
  generationID: Delegation.ID,
  source: Delegation.Source,
) {
  const digest = createHash("sha256")
    .update(JSON.stringify([sessionID, generationID, source.kind, source.id]))
    .digest("hex")
  return Delegation.SourceID.make(`dsrc_${digest}`)
}

function fail<A = never>(code: string, message: string): Effect.Effect<A, Delegation.Error> {
  return Effect.fail(new Delegation.Error({ code, message }))
}
