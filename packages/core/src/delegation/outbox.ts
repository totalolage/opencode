import { isDeepStrictEqual } from "node:util"
import { and, asc, eq, inArray, isNull, or } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { Delegation } from "@opencode-ai/schema/delegation"
import { Database } from "../database/database"
import { SessionTable } from "../session/sql"
import { captureInTransaction } from "./capture"
import { finalizeInTransaction, sourceIdentity } from "./source"
import {
  DelegationGenerationTable,
  DelegationResolutionTable,
  DelegationRevocationTable,
  DelegationSourceTable,
  DelegationWorkTable,
} from "./sql"
import { type Transaction, requireActive, requireGeneration, resolution } from "./shared"

export type AdmitResult =
  | { readonly status: "admitted" }
  | { readonly status: "consumed" }
  | { readonly status: "resolved" }
  | { readonly status: "blocked"; readonly reason: string }
  | { readonly status: "revoked" }

export type CloseResult = {
  readonly generation: Delegation.Generation
  readonly closed: boolean
  readonly blockers: readonly string[]
  readonly resolutions: readonly Delegation.Resolution[]
}

export type PreparedResolution = Delegation.Resolution & { readonly envelope: Delegation.Envelope }

type AdmitCallbackResult = { readonly status: "admitted" } | { readonly status: "blocked"; readonly reason: string }

type ReconcileCallbackResult = {
  readonly quiescent: boolean
  readonly sources: readonly Delegation.Capture[]
}

export function makeOutbox(db: Database.Interface["db"]) {
  const capture = (generationID: Delegation.ID, input: Delegation.Capture) =>
    db
      .transaction((tx) => captureInTransaction(tx, generationID, input), { behavior: "immediate" })
      .pipe(Effect.map(immutableResolution))

  const pending = (parentID?: Delegation.Generation["parentID"]) =>
    db
      .select()
      .from(DelegationResolutionTable)
      .where(
        parentID === undefined
          ? eq(DelegationResolutionTable.status, "pending")
          : and(eq(DelegationResolutionTable.parent_id, parentID), eq(DelegationResolutionTable.status, "pending")),
      )
      .orderBy(asc(DelegationResolutionTable.time_created), asc(DelegationResolutionTable.id))
      .all()
      .pipe(Effect.map((rows) => rows.map((row) => immutableResolution(resolution(row)))))

  const incoming = (parentID: Delegation.Generation["parentID"]) =>
    db
      .select()
      .from(DelegationResolutionTable)
      .where(
        and(
          eq(DelegationResolutionTable.parent_id, parentID),
          inArray(DelegationResolutionTable.status, ["pending", "admitted", "consumed"]),
        ),
      )
      .orderBy(asc(DelegationResolutionTable.time_created), asc(DelegationResolutionTable.id))
      .all()
      .pipe(Effect.map((rows) => rows.map((row) => immutableResolution(resolution(row)))))

  const getResolution = (id: Delegation.ResolutionID) =>
    db
      .select()
      .from(DelegationResolutionTable)
      .where(eq(DelegationResolutionTable.id, id))
      .get()
      .pipe(Effect.map((row) => (row === undefined ? undefined : immutableResolution(resolution(row)))))

  const prepare = (id: Delegation.ResolutionID, content: Delegation.RecipientContent) =>
    Schema.decodeUnknownEffect(Delegation.RecipientContent)(content).pipe(
      Effect.mapError((error) => new Delegation.Error({ code: "invalid_content", message: error.message })),
      Effect.flatMap((content) =>
        db.transaction(
          Effect.fnUntraced(function* (tx) {
            const row = yield* tx
              .select()
              .from(DelegationResolutionTable)
              .where(eq(DelegationResolutionTable.id, id))
              .get()
            if (row === undefined) {
              return yield* fail<Delegation.Resolution>("not_found", `Delegation resolution ${id} was not found`)
            }

            const value = resolution(row)
            const generation = yield* requireGeneration(tx, value.generationID)
            const envelope = prepareEnvelope(generation, value, content)

            if (row.envelope !== null) {
              if (!isDeepStrictEqual(row.envelope, envelope)) {
                return yield* fail<Delegation.Resolution>(
                  "identity_conflict",
                  "Delegation resolution preparation has different immutable content",
                )
              }
              return immutableResolution(value)
            }

            if (row.status !== "pending") {
              return yield* fail<Delegation.Resolution>(
                "not_preparable",
                `Delegation resolution ${id} has no prepared envelope at status ${row.status}`,
              )
            }
            if (content.parts.length === 0) {
              return yield* fail<Delegation.Resolution>(
                "empty_parts",
                "Delegation recipient content requires at least one part",
              )
            }

            yield* tx
              .update(DelegationResolutionTable)
              .set({ envelope })
              .where(and(eq(DelegationResolutionTable.id, id), eq(DelegationResolutionTable.status, "pending")))
              .run()

            const prepared = yield* tx
              .select()
              .from(DelegationResolutionTable)
              .where(eq(DelegationResolutionTable.id, id))
              .get()
            if (prepared === undefined || prepared.envelope === null) {
              return yield* fail<Delegation.Resolution>(
                "prepare_failed",
                "Delegation resolution envelope was not persisted",
              )
            }
            return immutableResolution(resolution(prepared))
          }),
          { behavior: "immediate" },
        ),
      ),
    )

  const admit = <E, R>(
    id: Delegation.ResolutionID,
    callback: (tx: Transaction, resolution: PreparedResolution) => Effect.Effect<AdmitCallbackResult, E, R>,
  ) =>
    db
      .transaction(
        Effect.fnUntraced(function* (tx) {
          const row = yield* tx
            .select()
            .from(DelegationResolutionTable)
            .where(eq(DelegationResolutionTable.id, id))
            .get()
          if (row === undefined)
            return yield* fail<AdmitResult>("not_found", `Delegation resolution ${id} was not found`)

          if (row.status === "admitted") return { status: "admitted" } as const
          if (row.status === "consumed") return { status: "consumed" } as const
          if (row.status === "resolved") return { status: "resolved" } as const
          if (row.status === "revoked") return { status: "revoked" } as const
          if (row.envelope === null) {
            return yield* fail<AdmitResult>("not_prepared", `Delegation resolution ${id} has no prepared envelope`)
          }

          const value = resolution(row)
          if (value.envelope === undefined) {
            return yield* fail<AdmitResult>("not_prepared", `Delegation resolution ${id} has no prepared envelope`)
          }

          const recipient = yield* tx
            .select({ id: SessionTable.id })
            .from(SessionTable)
            .where(eq(SessionTable.id, row.parent_id))
            .get()
          if (recipient === undefined) return { status: "blocked", reason: "recipient session does not exist" } as const

          const parentFence = yield* tx
            .select({ id: DelegationRevocationTable.session_id })
            .from(DelegationRevocationTable)
            .where(eq(DelegationRevocationTable.session_id, row.parent_id))
            .get()
          if (parentFence !== undefined) return { status: "blocked", reason: "recipient session is revoked" } as const

          if (value.recipientGenerationID !== undefined) {
            const recipientGeneration = yield* requireActive(tx, value.recipientGenerationID)
            if (recipientGeneration.childID !== row.parent_id) {
              return {
                status: "blocked",
                reason: "recipient generation does not address the recipient session",
              } as const
            }
          } else {
            const activeIncoming = yield* tx
              .select({ id: DelegationGenerationTable.id })
              .from(DelegationGenerationTable)
              .where(
                and(
                  eq(DelegationGenerationTable.child_id, row.parent_id),
                  eq(DelegationGenerationTable.state, "active"),
                ),
              )
              .get()
            if (activeIncoming !== undefined) {
              return { status: "blocked", reason: "recipient session has an active incoming generation" } as const
            }
          }

          const prepared = Object.freeze({
            ...immutableResolution(value),
            envelope: immutableEnvelope(value.envelope),
          })
          const result = yield* callback(tx, prepared)
          if (result.status === "blocked") return yield* Effect.fail(new AdmissionBlocked(result.reason))

          yield* tx
            .update(DelegationResolutionTable)
            .set({ status: "admitted", time_admitted: Date.now() })
            .where(and(eq(DelegationResolutionTable.id, id), eq(DelegationResolutionTable.status, "pending")))
            .run()
          return result
        }),
        { behavior: "immediate" },
      )
      .pipe(
        Effect.catchIf(
          (error): error is AdmissionBlocked => error instanceof AdmissionBlocked,
          (error) => Effect.succeed({ status: "blocked", reason: error.reason } as const),
        ),
      )

  const markConsumed = (input: {
    readonly parentID: Delegation.Generation["parentID"]
    readonly ids: readonly Delegation.ResolutionID[]
  }) => {
    if (input.ids.length === 0) return Effect.succeed(undefined)

    return db.transaction(
      Effect.fnUntraced(function* (tx) {
        const ids = [...new Set(input.ids)]
        const rows = yield* tx
          .select()
          .from(DelegationResolutionTable)
          .where(inArray(DelegationResolutionTable.id, ids))
          .all()

        if (rows.length !== ids.length) {
          return yield* fail("not_found", "One or more delegation resolutions were not found")
        }
        if (rows.some((row) => row.parent_id !== input.parentID)) {
          return yield* fail("recipient_mismatch", "Delegation resolution is addressed to another session")
        }
        if (rows.some((row) => row.status !== "admitted" && row.status !== "consumed")) {
          return yield* fail("invalid_status", "Only admitted delegation resolutions can be consumed")
        }

        yield* tx
          .update(DelegationResolutionTable)
          .set({ status: "consumed", time_consumed: Date.now() })
          .where(and(inArray(DelegationResolutionTable.id, ids), eq(DelegationResolutionTable.status, "admitted")))
          .run()
      }),
      { behavior: "immediate" },
    )
  }

  const reconcileAndClose = <E, R>(
    id: Delegation.ID,
    callback: (tx: Transaction, generation: Delegation.Generation) => Effect.Effect<ReconcileCallbackResult, E, R>,
  ) =>
    db.transaction(
      Effect.fnUntraced(function* (tx) {
        const initial = yield* requireGeneration(tx, id)
        if (initial.state === "closed") {
          return { generation: initial, closed: true, blockers: [], resolutions: [] } satisfies CloseResult
        }
        if (initial.state === "revoked") {
          return { generation: initial, closed: false, blockers: ["revoked"], resolutions: [] } satisfies CloseResult
        }

        const result = yield* callback(tx, immutableGeneration(initial))

        const finalized = yield* Effect.forEach(result.sources, (source) => finalizeCapture(tx, initial, source))
        const resolutions = finalized.flatMap((result) =>
          result.resolution === undefined ? [] : [immutableResolution(result.resolution)],
        )
        const current = yield* requireGeneration(tx, id)
        if (current.state === "closed") {
          return { generation: current, closed: true, blockers: [], resolutions } satisfies CloseResult
        }
        if (current.state === "revoked") {
          return { generation: current, closed: false, blockers: ["revoked"], resolutions } satisfies CloseResult
        }

        const blockers = yield* closureBlockers(tx, id, current.childID, result.quiescent)
        if (blockers.length > 0) {
          return { generation: current, closed: false, blockers, resolutions } satisfies CloseResult
        }

        yield* tx
          .update(DelegationGenerationTable)
          .set({ state: "closed", time_closed: Date.now() })
          .where(and(eq(DelegationGenerationTable.id, id), eq(DelegationGenerationTable.state, "active")))
          .run()
        const closed = yield* requireGeneration(tx, id)
        return { generation: closed, closed: true, blockers: [], resolutions } satisfies CloseResult
      }),
      { behavior: "immediate" },
    )

  return { capture, pending, incoming, getResolution, prepare, admit, markConsumed, reconcileAndClose }
}

function finalizeCapture(tx: Transaction, generation: Delegation.Generation, input: Delegation.Capture) {
  return Effect.gen(function* () {
    const id = sourceIdentity({
      sessionID: generation.childID,
      generationID: generation.id,
      source: input.source,
    })
    const reservation = yield* tx.select().from(DelegationSourceTable).where(eq(DelegationSourceTable.id, id)).get()
    if (reservation === undefined) {
      return yield* fail("source_not_reserved", `Delegation source reservation was not found: ${id}`)
    }
    if (reservation.session_id !== generation.childID || reservation.generation_id !== generation.id) {
      return yield* fail(
        "source_mismatch",
        `Delegation source reservation does not belong to generation ${generation.id}`,
      )
    }

    return yield* finalizeInTransaction(tx, id, { payload: input.payload, outcome: input.outcome }, input)
  })
}

function closureBlockers(
  tx: Transaction,
  id: Delegation.ID,
  childID: Delegation.Generation["childID"],
  quiescent: boolean,
) {
  return Effect.gen(function* () {
    const blockers: string[] = []
    if (!quiescent) blockers.push("runtime")

    const activeWork = yield* tx
      .select({ id: DelegationWorkTable.id })
      .from(DelegationWorkTable)
      .where(
        and(
          eq(DelegationWorkTable.state, "active"),
          or(
            eq(DelegationWorkTable.generation_id, id),
            and(eq(DelegationWorkTable.session_id, childID), isNull(DelegationWorkTable.generation_id)),
          ),
        ),
      )
      .all()
    blockers.push(...activeWork.map((row) => `work:${row.id}`))

    const reservedSources = yield* tx
      .select({ id: DelegationSourceTable.id })
      .from(DelegationSourceTable)
      .where(and(eq(DelegationSourceTable.generation_id, id), eq(DelegationSourceTable.state, "reserved")))
      .all()
    blockers.push(...reservedSources.map((row) => `source:${row.id}`))

    const generations = yield* tx.select().from(DelegationGenerationTable).all()
    const byID = new Map(generations.map((row) => [row.id, row]))
    blockers.push(
      ...generations
        .filter((row) => row.state === "active" && row.id !== id && hasAncestor(row, id, byID))
        .map((row) => `descendant:${row.id}`),
    )

    const incoming = yield* tx
      .select({ id: DelegationResolutionTable.id })
      .from(DelegationResolutionTable)
      .where(
        and(
          eq(DelegationResolutionTable.recipient_generation_id, id),
          inArray(DelegationResolutionTable.status, ["pending", "admitted", "consumed"]),
        ),
      )
      .all()
    blockers.push(...incoming.map((row) => `incoming:${row.id}`))

    const outgoing = yield* tx
      .select({ id: DelegationResolutionTable.id })
      .from(DelegationResolutionTable)
      .where(and(eq(DelegationResolutionTable.generation_id, id), eq(DelegationResolutionTable.status, "pending")))
      .all()
    blockers.push(...outgoing.map((row) => `outgoing:${row.id}`))

    return blockers
  })
}

function hasAncestor(
  row: typeof DelegationGenerationTable.$inferSelect,
  ancestorID: Delegation.ID,
  byID: ReadonlyMap<string, typeof DelegationGenerationTable.$inferSelect>,
) {
  const visited = new Set<string>()
  let parentID = row.parent_generation_id
  while (parentID !== null && parentID !== undefined) {
    if (parentID === ancestorID) return true
    if (visited.has(parentID)) return false
    visited.add(parentID)
    parentID = byID.get(parentID)?.parent_generation_id ?? null
  }
  return false
}

function prepareEnvelope(
  generation: Delegation.Generation,
  value: Delegation.Resolution,
  content: Delegation.RecipientContent,
) {
  return immutableEnvelope({
    message: {
      id: value.messageID,
      data: freezeJson(structuredClone(content.message)),
    },
    parts: content.parts.map((data, index) => ({
      id: Delegation.OriginPartID.make(`prt_${value.id.slice(4)}_${index}`),
      data: freezeJson(structuredClone(data)),
    })),
    provenance: {
      generationID: generation.id,
      parentID: generation.parentID,
      childID: generation.childID,
      ...(value.recipientGenerationID === undefined ? {} : { recipientGenerationID: value.recipientGenerationID }),
      origin: generation.origin,
      source: value.source,
      historyCutoff: value.historyCutoff,
      consumed: [...value.consumed],
    },
  })
}

function freezeJson(value: Delegation.RecipientContent["message"]): Delegation.RecipientContent["message"] {
  if (Array.isArray(value)) {
    value.forEach(freezeJson)
    return Object.freeze(value)
  }
  if (value !== null && typeof value === "object") {
    Object.values(value).forEach(freezeJson)
    return Object.freeze(value)
  }
  return value
}

function immutableEnvelope(value: Delegation.Envelope): Delegation.Envelope {
  return Object.freeze({
    message: Object.freeze({
      id: value.message.id,
      data: freezeJson(value.message.data),
    }),
    parts: Object.freeze(
      value.parts.map((part) =>
        Object.freeze({
          id: part.id,
          data: freezeJson(part.data),
        }),
      ),
    ),
    provenance: Object.freeze({
      ...value.provenance,
      origin: Object.freeze({ ...value.provenance.origin }),
      source: Object.freeze({ ...value.provenance.source }),
      consumed: Object.freeze([...value.provenance.consumed]),
    }),
  })
}

function immutableGeneration(value: Delegation.Generation): Delegation.Generation {
  return Object.freeze({
    ...value,
    origin: Object.freeze({ ...value.origin }),
  })
}

function immutableResolution(value: Delegation.Resolution): Delegation.Resolution {
  return Object.freeze({
    ...value,
    source: Object.freeze({ ...value.source }),
    consumed: Object.freeze([...value.consumed]),
    ...(value.envelope === undefined ? {} : { envelope: immutableEnvelope(value.envelope) }),
  })
}

class AdmissionBlocked extends Error {
  readonly _tag = "DelegationAdmissionBlocked"

  constructor(readonly reason: string) {
    super(reason)
  }
}

function fail<A = never>(code: string, message: string): Effect.Effect<A, Delegation.Error> {
  return Effect.fail(new Delegation.Error({ code, message }))
}
