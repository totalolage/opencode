import { createHash } from "node:crypto"
import { and, asc, eq, inArray, isNull, or } from "drizzle-orm"
import { Effect } from "effect"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionSchema } from "../session/schema"
import { SessionTable } from "../session/sql"
import { captureInTransaction } from "./capture"
import {
  DelegationGenerationTable,
  DelegationResolutionTable,
  DelegationRevocationTable,
  DelegationSourceTable,
  DelegationWorkTable,
} from "./sql"
import { type DB, type Transaction, requireActive, requireGeneration, resolution } from "./shared"

type SourceIdentityInput = {
  readonly sessionID: SessionSchema.ID
  readonly generationID?: Delegation.ID
  readonly source: Delegation.Source
}

type ReserveSourceInput = SourceIdentityInput & {
  readonly historyCutoff: string
  readonly consumed: readonly Delegation.ResolutionID[]
}

type SourceOutput = Pick<Delegation.Capture, "payload" | "outcome">

type FinalizeResult = {
  readonly source: Delegation.SourceRecord
  readonly resolution?: Delegation.Resolution
}

export function sourceIdentity(input: SourceIdentityInput): Delegation.SourceID {
  return Delegation.SourceID.make(`dsrc_${identityDigest(input)}`)
}

export function makeSources(db: DB) {
  const reserveSource = Effect.fn("DelegationSource.reserveSource")(function* (input: ReserveSourceInput) {
    if (input.source.id.length === 0) {
      return yield* fail("invalid_source", "Delegation source ID cannot be empty")
    }
    if (input.historyCutoff.length === 0) {
      return yield* fail("invalid_cutoff", "Delegation source history cutoff cannot be empty")
    }
    if (new Set(input.consumed).size !== input.consumed.length) {
      return yield* fail("invalid_consumed", "Consumed resolution IDs must be unique")
    }

    return yield* db.transaction(
      Effect.fnUntraced(function* (tx) {
        const id = sourceIdentity(input)
        const existingRow = yield* tx.select().from(DelegationSourceTable).where(eq(DelegationSourceTable.id, id)).get()
        if (existingRow !== undefined) {
          if (!sameReservation(existingRow, input)) {
            return yield* fail<{
              readonly source: Delegation.SourceRecord
              readonly created: boolean
            }>("source_conflict", `Delegation source identity is already bound to different content: ${id}`)
          }
          return { source: sourceRecord(existingRow), created: false as const }
        }

        const session = yield* tx
          .select({ id: SessionTable.id })
          .from(SessionTable)
          .where(eq(SessionTable.id, input.sessionID))
          .get()
        if (session === undefined) {
          return yield* fail("session_not_found", `Delegation source session not found: ${input.sessionID}`)
        }

        const generationValue =
          input.generationID === undefined
            ? undefined
            : yield* requireIncomingGeneration(tx, input.sessionID, input.generationID)
        if (generationValue === undefined) {
          const incoming = yield* activeIncoming(tx, input.sessionID)
          if (incoming !== undefined) {
            return yield* fail(
              "generation_required",
              `Session ${input.sessionID} has an active incoming delegation generation`,
            )
          }
        }
        yield* requireOwnFence(tx, input.sessionID, input.source)
        yield* validateReservationContributions(tx, input.sessionID, input.generationID, input.consumed)

        const workID = workIdentity(input)
        const now = Date.now()
        yield* tx
          .insert(DelegationWorkTable)
          .values({
            id: workID,
            session_id: input.sessionID,
            generation_id: input.generationID ?? null,
            kind: input.source.kind === "terminal" ? "runtime" : "provider",
            state: "active",
            time_created: now,
          })
          .run()
        yield* tx
          .insert(DelegationSourceTable)
          .values({
            id,
            session_id: input.sessionID,
            generation_id: input.generationID ?? null,
            source_kind: input.source.kind,
            source_id: input.source.id,
            history_cutoff: input.historyCutoff,
            consumed: Array.from(input.consumed),
            work_id: workID,
            state: "reserved",
            payload: null,
            outcome: null,
            time_created: now,
            time_finalized: null,
          })
          .run()

        if (input.consumed.length > 0) {
          yield* tx
            .update(DelegationResolutionTable)
            .set({ status: "consumed", time_consumed: now })
            .where(
              and(
                inArray(DelegationResolutionTable.id, input.consumed),
                eq(DelegationResolutionTable.status, "admitted"),
              ),
            )
            .run()
        }

        const inserted = yield* tx.select().from(DelegationSourceTable).where(eq(DelegationSourceTable.id, id)).get()
        if (inserted === undefined) {
          return yield* failAdapter("source_insert_failed", `Delegation source was not persisted: ${id}`)
        }
        return { source: sourceRecord(inserted), created: true as const }
      }),
      { behavior: "immediate" },
    )
  })

  const finalizeSource = Effect.fn("DelegationSource.finalizeSource")(function* (
    sourceID: Delegation.SourceID,
    output: SourceOutput,
  ) {
    return yield* db.transaction(
      (tx) => finalizeInTransaction(tx, sourceID, output),
      { behavior: "immediate" },
    )
  })

  const discardSource = Effect.fn("DelegationSource.discardSource")(function* (sourceID: Delegation.SourceID) {
    return yield* db.transaction(
      Effect.fnUntraced(function* (tx) {
        const row = yield* tx.select().from(DelegationSourceTable).where(eq(DelegationSourceTable.id, sourceID)).get()
        if (row === undefined) {
          return yield* fail<Delegation.SourceRecord>(
            "source_not_reserved",
            `Delegation source reservation was not found: ${sourceID}`,
          )
        }
        if (!sourceIdentityMatches(row, sourceID)) {
          return yield* failAdapter<Delegation.SourceRecord>(
            "source_corrupt",
            `Delegation source identity does not match its record: ${sourceID}`,
          )
        }
        if (row.state === "discarded") return sourceRecord(row)
        if (row.state === "finalized") {
          return yield* fail<Delegation.SourceRecord>(
            "source_finalized",
            `Delegation source is already finalized: ${sourceID}`,
          )
        }

        const reservation = sourceRecord(row)
        yield* requireSourceWork(tx, reservation)
        yield* tx
          .update(DelegationSourceTable)
          .set({ state: "discarded" })
          .where(
            and(
              eq(DelegationSourceTable.id, sourceID),
              eq(DelegationSourceTable.state, "reserved"),
            ),
          )
          .run()
        yield* tx
          .update(DelegationWorkTable)
          .set({ state: "finished" })
          .where(eq(DelegationWorkTable.id, reservation.workID))
          .run()

        const discarded = yield* tx.select().from(DelegationSourceTable).where(eq(DelegationSourceTable.id, sourceID)).get()
        if (discarded === undefined) {
          return yield* failAdapter("source_discard_failed", `Delegation source was not persisted: ${sourceID}`)
        }
        return sourceRecord(discarded)
      }),
      { behavior: "immediate" },
    )
  })

  const sources = Effect.fn("DelegationSource.sources")(function* (sessionID: SessionSchema.ID) {
    const rows = yield* db
      .select()
      .from(DelegationSourceTable)
      .where(eq(DelegationSourceTable.session_id, sessionID))
      .orderBy(asc(DelegationSourceTable.time_created), asc(DelegationSourceTable.id))
      .all()
    return rows.map(sourceRecord)
  })

  const unfinishedSources = Effect.fn("DelegationSource.unfinishedSources")(function* (sessionID: SessionSchema.ID) {
    const rows = yield* db
      .select()
      .from(DelegationSourceTable)
      .where(
        and(
          eq(DelegationSourceTable.session_id, sessionID),
          eq(DelegationSourceTable.state, "reserved"),
        ),
      )
      .orderBy(asc(DelegationSourceTable.time_created), asc(DelegationSourceTable.id))
      .all()
    return rows.map(sourceRecord)
  })

  return { reserveSource, finalizeSource, discardSource, sources, unfinishedSources }
}

export function finalizeInTransaction(
  tx: Transaction,
  sourceID: Delegation.SourceID,
  output: SourceOutput,
  expectedCapture?: Delegation.Capture,
) {
  return Effect.gen(function* () {
    const row = yield* tx.select().from(DelegationSourceTable).where(eq(DelegationSourceTable.id, sourceID)).get()
    if (row === undefined) {
      return yield* fail<FinalizeResult>(
        "source_not_reserved",
        `Delegation source reservation was not found: ${sourceID}`,
      )
    }
    if (!sourceIdentityMatches(row, sourceID)) {
      return yield* failAdapter<FinalizeResult>(
        "source_corrupt",
        `Delegation source identity does not match its record: ${sourceID}`,
      )
    }

    const reservation = sourceRecord(row)
    if (expectedCapture !== undefined && !sameExpectedCapture(reservation, output, expectedCapture)) {
      return yield* fail<FinalizeResult>(
        "source_mismatch",
        `Delegation source finalization does not match its reservation: ${sourceID}`,
      )
    }

    if (row.state === "finalized") {
      if (row.payload === null || row.outcome === null) {
        return yield* failAdapter<FinalizeResult>(
          "source_corrupt",
          `Finalized delegation source has no immutable output: ${sourceID}`,
        )
      }
      if (row.payload !== output.payload || row.outcome !== output.outcome) {
        return yield* fail<FinalizeResult>(
          "identity_conflict",
          `Delegation source was finalized with different output: ${sourceID}`,
        )
      }
      return yield* finalizedResult(tx, reservation, expectedCapture !== undefined)
    }
    if (row.state === "discarded") {
      return yield* fail<FinalizeResult>(
        "source_discarded",
        `Discarded delegation sources cannot be finalized: ${sourceID}`,
      )
    }

    yield* validateOutput(reservation.source, output)
    const generationValue =
      reservation.generationID === undefined
        ? undefined
        : yield* requireIncomingGeneration(tx, reservation.sessionID, reservation.generationID)
    const ownFence = yield* requireOwnFence(tx, reservation.sessionID, reservation.source, output.outcome)
    const allowRevoked = output.outcome === "cancelled" && ownFence
    yield* requireSourceWork(tx, reservation)
    yield* validateFinalizationContributions(
      tx,
      reservation.sessionID,
      reservation.generationID,
      reservation.consumed,
      reservation.id,
      allowRevoked,
    )

    const now = Date.now()
    yield* tx
      .update(DelegationSourceTable)
      .set({
        state: "finalized",
        payload: output.payload,
        outcome: output.outcome,
        time_finalized: now,
      })
      .where(
        and(
          eq(DelegationSourceTable.id, sourceID),
          eq(DelegationSourceTable.state, "reserved"),
        ),
      )
      .run()
    yield* tx
      .update(DelegationWorkTable)
      .set({ state: "finished" })
      .where(eq(DelegationWorkTable.id, reservation.workID))
      .run()
    yield* resolveContributions(tx, reservation, now, allowRevoked)

    if (output.outcome === "cancelled") yield* abandonReservedSources(tx, reservation)

    const finalized = yield* tx.select().from(DelegationSourceTable).where(eq(DelegationSourceTable.id, sourceID)).get()
    if (finalized === undefined) {
      return yield* failAdapter<FinalizeResult>("source_finalize_failed", `Delegation source was not persisted: ${sourceID}`)
    }

    const value = sourceRecord(finalized)
    if (generationValue?.mode !== "background") return { source: value }

    const resolutionValue = yield* captureInTransaction(tx, generationValue.id, {
      source: value.source,
      payload: output.payload,
      outcome: output.outcome,
      historyCutoff: value.historyCutoff,
      consumed: Array.from(value.consumed),
    })
    return { source: value, resolution: resolutionValue }
  })
}

function identityDigest(input: SourceIdentityInput) {
  return createHash("sha256")
    .update(JSON.stringify([input.sessionID, input.generationID ?? null, input.source.kind, input.source.id]))
    .digest("hex")
}

function workIdentity(input: SourceIdentityInput): Delegation.WorkID {
  return Delegation.WorkID.make(`dwk_${identityDigest(input)}`)
}

function sourceRecord(row: typeof DelegationSourceTable.$inferSelect): Delegation.SourceRecord {
  return immutableSource(
    Delegation.SourceRecord.make({
      id: Delegation.SourceID.make(row.id),
      sessionID: SessionSchema.ID.make(row.session_id),
      ...(row.generation_id === null ? {} : { generationID: Delegation.ID.make(row.generation_id) }),
      source: {
        kind: row.source_kind,
        id: row.source_id,
      },
      historyCutoff: row.history_cutoff,
      consumed: row.consumed.map((id) => Delegation.ResolutionID.make(id)),
      workID: Delegation.WorkID.make(row.work_id),
      state: row.state,
      ...(row.payload === null ? {} : { payload: row.payload }),
      ...(row.outcome === null ? {} : { outcome: row.outcome }),
      timeCreated: row.time_created,
      ...(row.time_finalized === null ? {} : { timeFinalized: row.time_finalized }),
    }),
  )
}

function immutableSource(value: Delegation.SourceRecord): Delegation.SourceRecord {
  return Object.freeze({
    ...value,
    source: Object.freeze({ ...value.source }),
    consumed: Object.freeze([...value.consumed]),
  })
}

function sourceIdentityMatches(row: typeof DelegationSourceTable.$inferSelect, sourceID: Delegation.SourceID) {
  return (
    row.id === sourceID &&
    sourceIdentity({
      sessionID: SessionSchema.ID.make(row.session_id),
      ...(row.generation_id === null ? {} : { generationID: Delegation.ID.make(row.generation_id) }),
      source: { kind: row.source_kind, id: row.source_id },
    }) === sourceID
  )
}

function sameReservation(row: typeof DelegationSourceTable.$inferSelect, input: ReserveSourceInput) {
  return (
    row.session_id === input.sessionID &&
    row.generation_id === (input.generationID ?? null) &&
    row.source_kind === input.source.kind &&
    row.source_id === input.source.id &&
    row.history_cutoff === input.historyCutoff &&
    sameIDs(row.consumed, input.consumed) &&
    row.work_id === workIdentity(input)
  )
}

function sameExpectedCapture(
  reservation: Delegation.SourceRecord,
  output: SourceOutput,
  expectedCapture: Delegation.Capture,
) {
  return (
    reservation.source.kind === expectedCapture.source.kind &&
    reservation.source.id === expectedCapture.source.id &&
    reservation.historyCutoff === expectedCapture.historyCutoff &&
    sameIDs(reservation.consumed, expectedCapture.consumed) &&
    expectedCapture.payload === output.payload &&
    expectedCapture.outcome === output.outcome
  )
}

function sameIDs(left: readonly string[], right: readonly string[]) {
  return left.length === right.length && left.every((id, index) => id === right[index])
}

function activeIncoming(tx: Transaction, sessionID: SessionSchema.ID) {
  return tx
    .select()
    .from(DelegationGenerationTable)
    .where(
      and(
        eq(DelegationGenerationTable.child_id, sessionID),
        eq(DelegationGenerationTable.state, "active"),
      ),
    )
    .get()
}

function requireIncomingGeneration(tx: Transaction, sessionID: SessionSchema.ID, generationID: Delegation.ID) {
  return Effect.gen(function* () {
    const value = yield* requireGeneration(tx, generationID)
    if (value.childID !== sessionID) {
      return yield* fail(
        "source_session_mismatch",
        `Delegation generation ${generationID} does not own session ${sessionID}`,
      )
    }

    const incoming = yield* activeIncoming(tx, sessionID)
    if (incoming?.id !== generationID) {
      return yield* fail(
        "generation_not_incoming",
        `Delegation generation ${generationID} is not the active incoming generation for ${sessionID}`,
      )
    }
    return yield* requireActive(tx, generationID)
  })
}

function requireOwnFence(
  tx: Transaction,
  sessionID: SessionSchema.ID,
  source: Delegation.Source,
  outcome?: Delegation.Outcome,
) {
  return Effect.gen(function* () {
    const fence = yield* tx
      .select({ id: DelegationRevocationTable.session_id })
      .from(DelegationRevocationTable)
      .where(eq(DelegationRevocationTable.session_id, sessionID))
      .get()
    if (fence === undefined) return false
    if (outcome === undefined && source.kind === "terminal") return true
    if (outcome === "cancelled") return true
    return yield* fail("child_revoked", `Fenced session cannot publish this delegation source: ${sessionID}`)
  })
}

function validateReservationContributions(
  tx: Transaction,
  sessionID: SessionSchema.ID,
  generationID: Delegation.ID | undefined,
  consumed: readonly Delegation.ResolutionID[],
) {
  if (consumed.length === 0) return Effect.succeed(undefined)

  return Effect.gen(function* () {
    const rows = yield* tx
      .select()
      .from(DelegationResolutionTable)
      .where(inArray(DelegationResolutionTable.id, consumed))
      .all()
    if (rows.length !== consumed.length) {
      return yield* fail("consumed_missing", "A delegation source contribution does not exist")
    }
    if (
      rows.some(
        (row) =>
          row.parent_id !== sessionID ||
          row.recipient_generation_id !== (generationID ?? null),
      )
    ) {
      return yield* fail(
        "consumed_owner_mismatch",
        "A delegation source contribution belongs to another session or generation",
      )
    }
    if (rows.some((row) => row.status !== "admitted" && row.status !== "consumed")) {
      return yield* fail(
        "consumed_invalid_status",
        "Delegation source contributions must be admitted or consumed",
      )
    }
  })
}

function validateFinalizationContributions(
  tx: Transaction,
  sessionID: SessionSchema.ID,
  generationID: Delegation.ID | undefined,
  consumed: readonly Delegation.ResolutionID[],
  sourceID: Delegation.SourceID,
  allowRevoked: boolean,
) {
  if (consumed.length === 0) return Effect.succeed(undefined)

  return Effect.gen(function* () {
    const rows = yield* tx
      .select()
      .from(DelegationResolutionTable)
      .where(inArray(DelegationResolutionTable.id, consumed))
      .all()
    if (rows.length !== consumed.length) {
      return yield* fail("consumed_missing", "A delegation source contribution does not exist")
    }
    if (
      rows.some(
        (row) =>
          row.parent_id !== sessionID ||
          row.recipient_generation_id !== (generationID ?? null),
      )
    ) {
      return yield* fail(
        "consumed_owner_mismatch",
        "A delegation source contribution belongs to another session or generation",
      )
    }
    if (rows.some((row) => !contributionCanBeFinalized(row, sourceID, allowRevoked))) {
      return yield* fail(
        "contribution_already_resolved",
        "A delegation source contribution cannot be finalized by this source",
      )
    }
  })
}

function resolveContributions(
  tx: Transaction,
  reservation: Delegation.SourceRecord,
  now: number,
  allowRevoked: boolean,
) {
  if (reservation.consumed.length === 0) return Effect.succeed(undefined)

  return Effect.gen(function* () {
    yield* tx
      .update(DelegationResolutionTable)
      .set({
        status: "resolved",
        time_resolved: now,
        resolved_source_id: reservation.id,
      })
      .where(
        and(
          inArray(DelegationResolutionTable.id, reservation.consumed),
          eq(DelegationResolutionTable.status, "consumed"),
          isNull(DelegationResolutionTable.resolved_source_id),
        ),
      )
      .run()
    if (allowRevoked) {
      yield* tx
        .update(DelegationResolutionTable)
        .set({
          time_resolved: now,
          resolved_source_id: reservation.id,
        })
        .where(
          and(
            inArray(DelegationResolutionTable.id, reservation.consumed),
            eq(DelegationResolutionTable.status, "revoked"),
            or(
              isNull(DelegationResolutionTable.resolved_source_id),
              eq(DelegationResolutionTable.resolved_source_id, reservation.id),
            ),
          ),
        )
        .run()
    }

    const rows = yield* tx
      .select()
      .from(DelegationResolutionTable)
      .where(inArray(DelegationResolutionTable.id, reservation.consumed))
      .all()
    if (
      rows.length !== reservation.consumed.length ||
      rows.some((row) => !contributionSettledBy(row, reservation.id, allowRevoked))
    ) {
      return yield* fail(
        "contribution_resolution_conflict",
        "A delegation source contribution could not be resolved by this source",
      )
    }
  })
}

function requireSourceWork(tx: Transaction, reservation: Delegation.SourceRecord) {
  return Effect.gen(function* () {
    const work = yield* tx.select().from(DelegationWorkTable).where(eq(DelegationWorkTable.id, reservation.workID)).get()
    if (work === undefined) {
      return yield* failAdapter(
        "source_work_missing",
        `Delegation source work was not found: ${reservation.workID}`,
      )
    }
    if (
      work.session_id !== reservation.sessionID ||
      work.generation_id !== (reservation.generationID ?? null) ||
      work.kind !== (reservation.source.kind === "terminal" ? "runtime" : "provider")
    ) {
      return yield* failAdapter(
        "source_work_mismatch",
        `Delegation source work does not match its reservation: ${reservation.workID}`,
      )
    }
  })
}

function validateOutput(source: Delegation.Source, output: SourceOutput) {
  if (source.kind === "terminal" && output.outcome === "reply") {
    return fail("invalid_outcome", "Terminal delegation sources cannot produce reply outcomes")
  }
  return Effect.succeed(undefined)
}

function contributionCanBeFinalized(
  row: typeof DelegationResolutionTable.$inferSelect,
  sourceID: Delegation.SourceID,
  allowRevoked: boolean,
) {
  if (row.status === "consumed") return row.resolved_source_id === null
  if (row.status === "resolved") return row.resolved_source_id === sourceID
  return (
    allowRevoked &&
    row.status === "revoked" &&
    (row.resolved_source_id === null || row.resolved_source_id === sourceID)
  )
}

function contributionSettledBy(
  row: typeof DelegationResolutionTable.$inferSelect,
  sourceID: Delegation.SourceID,
  allowRevoked: boolean,
) {
  if (row.status === "resolved") return row.resolved_source_id === sourceID
  return allowRevoked && row.status === "revoked" && row.resolved_source_id === sourceID
}

function finalizedResult(tx: Transaction, reservation: Delegation.SourceRecord, repairCapture: boolean) {
  return Effect.gen(function* () {
    if (reservation.generationID === undefined) return { source: reservation }

    const existing = yield* tx
      .select()
      .from(DelegationResolutionTable)
      .where(
        and(
          eq(DelegationResolutionTable.generation_id, reservation.generationID),
          eq(DelegationResolutionTable.source_kind, reservation.source.kind),
          eq(DelegationResolutionTable.source_id, reservation.source.id),
        ),
      )
      .get()
    if (existing !== undefined) return { source: reservation, resolution: resolution(existing) }
    if (!repairCapture) return { source: reservation }

    const generationValue = yield* requireGeneration(tx, reservation.generationID)
    if (generationValue.childID !== reservation.sessionID) {
      return yield* failAdapter(
        "source_generation_mismatch",
        `Delegation source does not belong to generation ${reservation.generationID}`,
      )
    }
    if (generationValue.mode !== "background") return { source: reservation }
    if (reservation.payload === undefined || reservation.outcome === undefined) {
      return yield* failAdapter(
        "source_corrupt",
        `Finalized delegation source has no immutable output: ${reservation.id}`,
      )
    }
    const captured = yield* captureInTransaction(tx, generationValue.id, {
      source: reservation.source,
      payload: reservation.payload,
      outcome: reservation.outcome,
      historyCutoff: reservation.historyCutoff,
      consumed: Array.from(reservation.consumed),
    })
    return { source: reservation, resolution: captured }
  })
}

function abandonReservedSources(tx: Transaction, reservation: Delegation.SourceRecord) {
  return Effect.gen(function* () {
    const generationScope =
      reservation.generationID === undefined
        ? isNull(DelegationSourceTable.generation_id)
        : eq(DelegationSourceTable.generation_id, reservation.generationID)
    const rows = yield* tx
      .select()
      .from(DelegationSourceTable)
      .where(
        and(
          eq(DelegationSourceTable.session_id, reservation.sessionID),
          generationScope,
          eq(DelegationSourceTable.state, "reserved"),
        ),
      )
      .all()
    const stale = rows.filter((row) => row.id !== reservation.id)
    if (stale.length === 0) return

    yield* tx
      .update(DelegationSourceTable)
      .set({ state: "discarded" })
      .where(
        and(
          inArray(DelegationSourceTable.id, stale.map((row) => row.id)),
          eq(DelegationSourceTable.state, "reserved"),
        ),
      )
      .run()
    yield* tx
      .update(DelegationWorkTable)
      .set({ state: "finished" })
      .where(inArray(DelegationWorkTable.id, stale.map((row) => row.work_id)))
      .run()
  })
}

function fail<A = never>(code: string, message: string): Effect.Effect<A, Delegation.Error> {
  return Effect.fail(new Delegation.Error({ code, message }))
}

function failAdapter<A = never>(code: string, message: string): Effect.Effect<A, Delegation.AdapterError> {
  return Effect.fail(new Delegation.AdapterError({ code, message }))
}
