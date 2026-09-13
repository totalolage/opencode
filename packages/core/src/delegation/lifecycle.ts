import { isDeepStrictEqual } from "node:util"
import { and, asc, eq, inArray, isNull, or } from "drizzle-orm"
import { Effect } from "effect"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionSchema } from "../session/schema"
import { SessionTable } from "../session/sql"
import {
  DelegationGenerationTable,
  DelegationRegistrationTable,
  DelegationResolutionTable,
  DelegationRevocationTable,
  DelegationSourceTable,
  DelegationWorkTable,
} from "./sql"
import type { DB, Transaction } from "./shared"
import {
  generation,
  requireActive,
  requireGeneration,
} from "./shared"

type RuntimeWorkKind = Extract<Delegation.WorkKind, "runtime" | "provider" | "tool" | "input">
const runtimeWorkKinds = new Set(["runtime", "provider", "tool", "input"])

type StartWorkInput =
  | {
      readonly id: Delegation.WorkID
      readonly generationID: Delegation.ID
      readonly sessionID?: SessionSchema.ID
      readonly kind: RuntimeWorkKind
    }
  | {
      readonly id: Delegation.WorkID
      readonly sessionID: SessionSchema.ID
      readonly generationID?: never
      readonly kind: RuntimeWorkKind
    }

export function makeLifecycle(db: DB) {
  const get = Effect.fn("DelegationLifecycle.get")(function* (id: Delegation.ID) {
    const row = yield* db.select().from(DelegationGenerationTable).where(eq(DelegationGenerationTable.id, id)).get()
    return row === undefined ? undefined : generation(row)
  })

  const active = Effect.fn("DelegationLifecycle.active")(function* (childID: SessionSchema.ID) {
    const row = yield* db
      .select()
      .from(DelegationGenerationTable)
      .where(and(eq(DelegationGenerationTable.child_id, childID), eq(DelegationGenerationTable.state, "active")))
      .get()
    return row === undefined ? undefined : generation(row)
  })

  const listActive = Effect.fn("DelegationLifecycle.listActive")(function* () {
    const rows = yield* db
      .select()
      .from(DelegationGenerationTable)
      .where(eq(DelegationGenerationTable.state, "active"))
      .orderBy(asc(DelegationGenerationTable.time_created), asc(DelegationGenerationTable.id))
      .all()
    return rows.map(generation)
  })

  const listWork = Effect.fn("DelegationLifecycle.listWork")(function* (generationID: Delegation.ID) {
    const rows = yield* db
      .select()
      .from(DelegationWorkTable)
      .where(eq(DelegationWorkTable.generation_id, generationID))
      .orderBy(asc(DelegationWorkTable.time_created), asc(DelegationWorkTable.id))
      .all()
    return rows.map(work)
  })

  const unfinished = Effect.fn("DelegationLifecycle.unfinished")(function* (sessionID: SessionSchema.ID) {
    const rows = yield* db
      .select()
      .from(DelegationWorkTable)
      .where(and(eq(DelegationWorkTable.session_id, sessionID), eq(DelegationWorkTable.state, "active")))
      .orderBy(asc(DelegationWorkTable.time_created), asc(DelegationWorkTable.id))
      .all()
    return rows.map(work)
  })

  const register = Effect.fn("DelegationLifecycle.register")(function* (input: Delegation.Registration) {
    const request = canonicalRegistration(input)
    return yield* db.transaction(
      (tx) =>
        Effect.gen(function* () {
          const recorded = yield* tx
            .select()
            .from(DelegationRegistrationTable)
            .where(eq(DelegationRegistrationTable.request_id, request.requestID))
            .get()

          if (recorded !== undefined) {
            if (!isDeepStrictEqual(canonicalRegistration(recorded.request), request)) {
              return yield* new Delegation.Error({
                code: "registration_conflict",
                message: `Delegation request has already been registered with different contents: ${request.requestID}`,
              })
            }

            const recordedGeneration = yield* requireGeneration(
              tx,
              Delegation.ID.make(recorded.generation_id),
            )
            const recordedWork = yield* tx
              .select()
              .from(DelegationWorkTable)
              .where(eq(DelegationWorkTable.id, recorded.work_id))
              .get()
            if (
              recordedWork === undefined ||
              recordedWork.session_id !== recordedGeneration.childID ||
              recordedWork.generation_id !== recordedGeneration.id
            ) {
              return yield* new Delegation.AdapterError({
                code: "registration_corrupt",
                message: `Delegation request refers to a missing or mismatched work token: ${request.requestID}`,
              })
            }

            return {
              generation: recordedGeneration,
              workID: Delegation.WorkID.make(recordedWork.id),
              workState: recordedWork.state,
            }
          }

          const parent = yield* tx
            .select({ id: SessionTable.id })
            .from(SessionTable)
            .where(eq(SessionTable.id, request.parentID))
            .get()
          if (parent === undefined) {
            return yield* new Delegation.Error({
              code: "parent_session_not_found",
              message: `Delegation parent session not found: ${request.parentID}`,
            })
          }

          const child = yield* tx
            .select({ id: SessionTable.id, parentID: SessionTable.parent_id })
            .from(SessionTable)
            .where(eq(SessionTable.id, request.childID))
            .get()
          if (child === undefined) {
            return yield* new Delegation.Error({
              code: "child_session_not_found",
              message: `Delegation child session not found: ${request.childID}`,
            })
          }
          if (request.parentID === request.childID) {
            return yield* new Delegation.Error({
              code: "self_delegation",
              message: `Delegation parent and child must differ: ${request.parentID}`,
            })
          }
          if (child.parentID !== request.parentID) {
            return yield* new Delegation.Error({
              code: "child_parent_mismatch",
              message: `Delegation child ${request.childID} is not parented by ${request.parentID}`,
            })
          }

          yield* requireUnfenced(tx, request.parentID, "parent")
          yield* requireUnfenced(tx, request.childID, "child")

          const priorRows = yield* tx
            .select()
            .from(DelegationGenerationTable)
            .where(eq(DelegationGenerationTable.child_id, request.childID))
            .all()
          if (priorRows.some((row) => row.parent_id !== request.parentID)) {
            return yield* new Delegation.Error({
              code: "child_parent_conflict",
              message: `Delegation child ${request.childID} already belongs to another parent`,
            })
          }

          const incoming = yield* validateIncoming(tx, request.parentID, request.parentGenerationID)
          const current = priorRows.find((row) => row.state === "active")

          if (current !== undefined) {
            if (!request.explicitReuse) {
              return yield* new Delegation.Error({
                code: "active_generation_requires_reuse",
                message: `Active delegation child ${request.childID} requires explicit reuse`,
              })
            }

            const existing = yield* requireActive(tx, Delegation.ID.make(current.id))
            if (existing.parentGenerationID !== request.parentGenerationID) {
              return yield* new Delegation.Error({
                code: "generation_ancestry_conflict",
                message: `Active delegation generation ${existing.id} cannot change ancestry`,
              })
            }
            if (request.mode !== undefined && request.mode !== existing.mode) {
              return yield* new Delegation.Error({
                code: "generation_mode_conflict",
                message: `Active delegation generation ${existing.id} cannot change mode`,
              })
            }
            if (incoming !== undefined && incoming.id !== existing.parentGenerationID) {
              return yield* new Delegation.Error({
                code: "generation_ancestry_conflict",
                message: `Active delegation generation ${existing.id} cannot change ancestry`,
              })
            }

            const proposed = yield* tx
              .select({ id: DelegationGenerationTable.id })
              .from(DelegationGenerationTable)
              .where(eq(DelegationGenerationTable.id, request.generationID))
              .get()
            if (proposed !== undefined) {
              return yield* new Delegation.Error({
                code: "generation_id_collision",
                message: `Delegation generation ID is already in use: ${request.generationID}`,
              })
            }

            const workID = Delegation.WorkID.create()
            const now = Date.now()
            yield* tx
              .insert(DelegationWorkTable)
              .values({
                id: workID,
                session_id: existing.childID,
                generation_id: existing.id,
                kind: "update",
                state: "active",
                time_created: now,
              })
              .run()
            yield* tx
              .insert(DelegationRegistrationTable)
              .values({
                request_id: request.requestID,
                generation_id: existing.id,
                request,
                work_id: workID,
             })
             .run()
            return { generation: existing, workID, workState: "active" as const }
          }

          if (priorRows.length > 0 && !request.explicitReuse) {
            return yield* new Delegation.Error({
              code: "closed_generation_requires_reuse",
              message: `Closed delegation child ${request.childID} requires explicit reuse`,
            })
          }

          const sessionWork = yield* tx
            .select({ id: DelegationWorkTable.id })
            .from(DelegationWorkTable)
            .where(
              and(
                eq(DelegationWorkTable.session_id, request.childID),
                isNull(DelegationWorkTable.generation_id),
                eq(DelegationWorkTable.state, "active"),
              ),
            )
            .get()
          if (sessionWork !== undefined) {
            return yield* new Delegation.Error({
              code: "session_work_active",
              message: `Delegation child ${request.childID} has unfinished session work: ${sessionWork.id}`,
            })
          }

          yield* requireNewGenerationOwnership(tx, request.childID)

          const proposed = yield* tx
            .select({ id: DelegationGenerationTable.id })
            .from(DelegationGenerationTable)
            .where(eq(DelegationGenerationTable.id, request.generationID))
            .get()
          if (proposed !== undefined) {
            return yield* new Delegation.Error({
              code: "generation_id_collision",
              message: `Delegation generation ID is already in use: ${request.generationID}`,
            })
          }

          const mode = request.mode ?? "foreground"
          if (mode === "background" && incoming?.mode === "foreground") {
            return yield* new Delegation.Error({
              code: "background_parent_forbidden",
              message: `Foreground delegation parent ${request.parentID} cannot launch a background child`,
            })
          }

          if (incoming !== undefined) yield* requireActive(tx, incoming.id)

          const now = Date.now()
          const generationID = request.generationID
          yield* tx
            .insert(DelegationGenerationTable)
            .values({
              id: generationID,
              parent_id: request.parentID,
              child_id: request.childID,
              origin: request.origin,
              parent_generation_id: request.parentGenerationID ?? null,
              mode,
              state: "active",
              time_created: now,
              time_closed: null,
            })
            .run()

          const workID = Delegation.WorkID.create()
          yield* tx
            .insert(DelegationWorkTable)
            .values({
              id: workID,
              session_id: request.childID,
              generation_id: generationID,
              kind: "launch",
              state: "active",
              time_created: now,
            })
            .run()
          yield* tx
            .insert(DelegationRegistrationTable)
            .values({
              request_id: request.requestID,
              generation_id: generationID,
              request,
              work_id: workID,
            })
            .run()

          return {
            generation: Delegation.Generation.make({
              id: generationID,
              parentID: request.parentID,
              childID: request.childID,
              origin: request.origin,
              ...(request.parentGenerationID === undefined
                ? {}
                : { parentGenerationID: request.parentGenerationID }),
              mode,
              state: "active",
              timeCreated: now,
            }),
            workID,
            workState: "active" as const,
          }
        }),
      { behavior: "immediate" },
    )
  })

  const startWork = Effect.fn("DelegationLifecycle.startWork")(function* (input: StartWorkInput) {
    return yield* db.transaction(
      (tx) =>
        Effect.gen(function* () {
          if (!runtimeWorkKinds.has(input.kind)) {
            return yield* new Delegation.Error({
              code: "invalid_work_kind",
              message: `Delegation runtime work has an unsupported kind: ${input.kind}`,
            })
          }

          const current =
            input.generationID === undefined ? undefined : yield* requireGeneration(tx, input.generationID)
          const sessionID = input.sessionID ?? current?.childID
          if (sessionID === undefined) {
            return yield* new Delegation.Error({
              code: "session_required",
              message: "Root delegation work requires a session ID",
            })
          }
          if (current !== undefined && current.childID !== sessionID) {
            return yield* new Delegation.Error({
              code: "work_owner_conflict",
              message: `Delegation work session does not own generation ${current.id}`,
            })
          }

          const existing = yield* tx
            .select()
            .from(DelegationWorkTable)
            .where(eq(DelegationWorkTable.id, input.id))
            .get()
          if (existing !== undefined) {
            if (
              existing.session_id !== sessionID ||
              existing.generation_id !== (current?.id ?? null) ||
              existing.kind !== input.kind
            ) {
              return yield* new Delegation.Error({
                code: "work_conflict",
                message: `Delegation work ID is already bound to another operation: ${input.id}`,
              })
            }
            return work(existing)
          }

          const session = yield* tx
            .select({ id: SessionTable.id })
            .from(SessionTable)
            .where(eq(SessionTable.id, sessionID))
            .get()
          if (session === undefined) {
            return yield* new Delegation.Error({
              code: "session_not_found",
              message: `Delegation work session not found: ${sessionID}`,
            })
          }
          yield* requireUnfenced(tx, sessionID, "child")

          if (current !== undefined) {
            const activeIncoming = yield* tx
              .select({ id: DelegationGenerationTable.id })
              .from(DelegationGenerationTable)
              .where(
                and(
                  eq(DelegationGenerationTable.child_id, sessionID),
                  eq(DelegationGenerationTable.state, "active"),
                ),
              )
              .get()
            if (activeIncoming?.id !== current.id) {
              return yield* new Delegation.Error({
                code: "generation_not_incoming",
                message: `Delegation generation ${current.id} is not the active incoming generation for ${sessionID}`,
              })
            }
            const active = yield* requireActive(tx, current.id)
            yield* requireUnfenced(tx, active.childID, "child")

            const now = Date.now()
            yield* tx
              .insert(DelegationWorkTable)
              .values({
                id: input.id,
                session_id: sessionID,
                generation_id: current.id,
                kind: input.kind,
                state: "active",
                time_created: now,
              })
              .run()
            return Delegation.Work.make({
              id: input.id,
              sessionID,
              generationID: current.id,
              kind: input.kind,
              state: "active",
              timeCreated: now,
            })
          }

          const activeIncoming = yield* tx
            .select({ id: DelegationGenerationTable.id })
            .from(DelegationGenerationTable)
            .where(
              and(
                eq(DelegationGenerationTable.child_id, sessionID),
                eq(DelegationGenerationTable.state, "active"),
              ),
            )
            .get()
          if (activeIncoming !== undefined) {
            return yield* new Delegation.Error({
              code: "generation_required",
              message: `Session ${sessionID} has an active incoming generation`,
            })
          }

          const now = Date.now()
          yield* tx
            .insert(DelegationWorkTable)
            .values({
              id: input.id,
              session_id: sessionID,
              generation_id: null,
              kind: input.kind,
              state: "active",
              time_created: now,
            })
            .run()
          return Delegation.Work.make({
            id: input.id,
            sessionID,
            kind: input.kind,
            state: "active",
            timeCreated: now,
          })
        }),
      { behavior: "immediate" },
    )
  })

  const finishWork = Effect.fn("DelegationLifecycle.finishWork")(function* (id: Delegation.WorkID) {
    yield* db.transaction(
      (tx) =>
        Effect.gen(function* () {
          const existing = yield* tx.select().from(DelegationWorkTable).where(eq(DelegationWorkTable.id, id)).get()
          if (existing === undefined) {
            return yield* new Delegation.Error({
              code: "work_not_found",
              message: `Delegation work not found: ${id}`,
            })
          }
          yield* tx
            .update(DelegationWorkTable)
            .set({ state: "finished" })
            .where(eq(DelegationWorkTable.id, id))
            .run()
        }),
      { behavior: "immediate" },
    )
  })

  const promote = Effect.fn("DelegationLifecycle.promote")(function* (id: Delegation.ID) {
    return yield* db.transaction(
      (tx) =>
        Effect.gen(function* () {
          const current = yield* requireActive(tx, id)
          yield* requireUnfenced(tx, current.childID, "child")
          const incoming = yield* validateIncoming(tx, current.parentID, current.parentGenerationID)
          if (incoming?.mode === "foreground") {
            return yield* new Delegation.Error({
              code: "background_parent_forbidden",
              message: `Foreground delegation parent ${current.parentID} cannot promote a background child`,
            })
          }
          if (current.mode === "background") return current

          yield* tx
            .update(DelegationGenerationTable)
            .set({ mode: "background" })
            .where(eq(DelegationGenerationTable.id, current.id))
            .run()
          return Delegation.Generation.make({ ...current, mode: "background" })
        }),
      { behavior: "immediate" },
    )
  })

  const revokeDescendants = Effect.fn("DelegationLifecycle.revokeDescendants")(function* (
    sessionID: SessionSchema.ID,
  ) {
    return yield* db.transaction(
      (tx) =>
        Effect.gen(function* () {
          const rows = yield* tx.select().from(DelegationGenerationTable).all()
          const incoming = rows.find((row) => row.child_id === sessionID && row.state === "active")
          const affected = new Set<Delegation.ID>()
          const visited = new Set<Delegation.ID>()
          const visit = (row: (typeof rows)[number]) => {
            if (visited.has(row.id)) return
            visited.add(row.id)
            if (row.id !== incoming?.id && row.state === "active") affected.add(row.id)
            rows.filter((candidate) => candidate.parent_generation_id === row.id).forEach(visit)
          }
          rows.filter((row) => row.parent_id === sessionID).forEach(visit)

          const affectedIDs = Array.from(affected)
          const activeSessionIDs = new Set<SessionSchema.ID>(
            rows.filter((row) => affected.has(row.id)).map((row) => row.child_id),
          )
          const fencedSessionIDs = new Set<SessionSchema.ID>([sessionID, ...activeSessionIDs])
          const existingFence = yield* tx
            .select({ id: DelegationRevocationTable.id })
            .from(DelegationRevocationTable)
            .where(eq(DelegationRevocationTable.session_id, sessionID))
            .get()
          const revocationID = existingFence?.id ?? Delegation.RevocationID.create()
          const now = Date.now()

          yield* tx
            .insert(DelegationRevocationTable)
            .values(
              Array.from(fencedSessionIDs, (id) => ({
                session_id: id,
                id: revocationID,
                time_revoked: now,
              })),
            )
            .onConflictDoNothing()
            .run()

          if (affectedIDs.length > 0) {
            yield* tx
              .update(DelegationGenerationTable)
              .set({ state: "revoked", time_closed: now })
              .where(
                and(
                  inArray(DelegationGenerationTable.id, affectedIDs),
                  eq(DelegationGenerationTable.state, "active"),
                ),
              )
              .run()
          }

          const workSessionIDs = Array.from(fencedSessionIDs)
          if (workSessionIDs.length > 0) {
            yield* tx
              .update(DelegationWorkTable)
              .set({ state: "finished" })
              .where(
                and(
                  inArray(DelegationWorkTable.session_id, workSessionIDs),
                  eq(DelegationWorkTable.state, "active"),
                ),
              )
              .run()
          }

          const resolutionScopes = [
            eq(DelegationResolutionTable.parent_id, sessionID),
            ...(affectedIDs.length === 0
              ? []
              : [
                  inArray(DelegationResolutionTable.generation_id, affectedIDs),
                  inArray(DelegationResolutionTable.recipient_generation_id, affectedIDs),
                ]),
            ...(activeSessionIDs.size === 0
              ? []
              : [inArray(DelegationResolutionTable.parent_id, Array.from(activeSessionIDs))]),
          ]
          yield* tx
            .update(DelegationResolutionTable)
            .set({ status: "revoked" })
            .where(
              and(
                or(...resolutionScopes),
                inArray(DelegationResolutionTable.status, ["pending", "admitted", "consumed"]),
              ),
            )
            .run()

          const latest = yield* tx.select().from(DelegationGenerationTable).all()
          return {
            revocationID,
            cancellationSource: {
              kind: "terminal" as const,
              id: `delegation-cancel:${revocationID}`,
            },
            generations: latest.filter((row) => affected.has(row.id)).map(generation),
            sessionIDs: Array.from(activeSessionIDs),
          }
        }),
      { behavior: "immediate" },
    )
  })

  const allowSession = Effect.fn("DelegationLifecycle.allowSession")(function* (sessionID: SessionSchema.ID) {
    yield* db.transaction(
      (tx) =>
        Effect.gen(function* () {
          const fence = yield* tx
            .select({ id: DelegationRevocationTable.session_id })
            .from(DelegationRevocationTable)
            .where(eq(DelegationRevocationTable.session_id, sessionID))
            .get()
          if (fence === undefined) return

          const incoming = yield* tx
            .select({ id: DelegationGenerationTable.id })
            .from(DelegationGenerationTable)
            .where(and(eq(DelegationGenerationTable.child_id, sessionID), eq(DelegationGenerationTable.state, "active")))
            .get()
          if (incoming !== undefined) {
            return yield* new Delegation.Error({
              code: "cancellation_pending",
              message: `Session ${sessionID} still has an active incoming delegation generation`,
            })
          }

          yield* tx
            .delete(DelegationRevocationTable)
            .where(eq(DelegationRevocationTable.session_id, sessionID))
            .run()
        }),
      { behavior: "immediate" },
    )
  })

  return {
    get,
    active,
    listActive,
    listWork,
    unfinished,
    register,
    startWork,
    finishWork,
    promote,
    revokeDescendants,
    allowSession,
  }
}

function canonicalRegistration(input: Delegation.Registration): Delegation.Registration {
  return {
    requestID: input.requestID,
    generationID: input.generationID,
    parentID: input.parentID,
    childID: input.childID,
    origin: input.origin,
    ...(input.parentGenerationID === undefined ? {} : { parentGenerationID: input.parentGenerationID }),
    ...(input.mode === undefined ? {} : { mode: input.mode }),
    explicitReuse: input.explicitReuse,
  }
}

function work(row: typeof DelegationWorkTable.$inferSelect): Delegation.Work {
  return Delegation.Work.make({
    id: Delegation.WorkID.make(row.id),
    sessionID: SessionSchema.ID.make(row.session_id),
    ...(row.generation_id === null ? {} : { generationID: Delegation.ID.make(row.generation_id) }),
    state: row.state,
    kind: row.kind,
    timeCreated: row.time_created,
  })
}

function requireNewGenerationOwnership(tx: Transaction, childID: SessionSchema.ID) {
  return Effect.gen(function* () {
    const source = yield* tx
      .select({ id: DelegationSourceTable.id })
      .from(DelegationSourceTable)
      .where(
        and(
          eq(DelegationSourceTable.session_id, childID),
          isNull(DelegationSourceTable.generation_id),
          eq(DelegationSourceTable.state, "reserved"),
        ),
      )
      .get()
    if (source !== undefined) {
      return yield* new Delegation.Error({
        code: "root_source_unresolved",
        message: `Delegation child ${childID} has an unresolved root source reservation: ${source.id}`,
      })
    }

    const outgoing = yield* tx
      .select({ id: DelegationGenerationTable.id })
      .from(DelegationGenerationTable)
      .where(
        and(
          eq(DelegationGenerationTable.parent_id, childID),
          eq(DelegationGenerationTable.state, "active"),
        ),
      )
      .get()
    if (outgoing !== undefined) {
      return yield* new Delegation.Error({
        code: "active_outgoing_generation",
        message: `Delegation child ${childID} has an active outgoing generation: ${outgoing.id}`,
      })
    }

    const incoming = yield* tx
      .select({ id: DelegationResolutionTable.id })
      .from(DelegationResolutionTable)
      .where(
        and(
          eq(DelegationResolutionTable.parent_id, childID),
          inArray(DelegationResolutionTable.status, ["pending", "admitted", "consumed"]),
        ),
      )
      .get()
    if (incoming !== undefined) {
      return yield* new Delegation.Error({
        code: "incoming_resolution_unclaimed",
        message: `Delegation child ${childID} has an incoming resolution without an active incoming generation: ${incoming.id}`,
      })
    }
  })
}

function requireUnfenced(tx: Transaction, sessionID: SessionSchema.ID, role: "parent" | "child") {
  return Effect.gen(function* () {
    const fence = yield* tx
      .select({ id: DelegationRevocationTable.session_id })
      .from(DelegationRevocationTable)
      .where(eq(DelegationRevocationTable.session_id, sessionID))
      .get()
    if (fence === undefined) return
    return yield* new Delegation.Error({
      code: "session_revoked",
      message: `Delegation ${role} session is fenced: ${sessionID}`,
    })
  })
}

function validateIncoming(
  tx: Transaction,
  parentID: SessionSchema.ID,
  parentGenerationID: Delegation.ID | undefined,
) {
  return Effect.gen(function* () {
    const incoming = yield* tx
      .select()
      .from(DelegationGenerationTable)
      .where(and(eq(DelegationGenerationTable.child_id, parentID), eq(DelegationGenerationTable.state, "active")))
      .get()
    if (incoming === undefined) {
      if (parentGenerationID !== undefined) {
        return yield* new Delegation.Error({
          code: "parent_generation_not_active",
          message: `Parent session ${parentID} has no active incoming generation`,
        })
      }
      return undefined
    }

    const incomingID = Delegation.ID.make(incoming.id)
    if (parentGenerationID !== incomingID) {
      return yield* new Delegation.Error({
        code: "parent_generation_mismatch",
        message: `Parent session ${parentID} requires incoming generation ${incomingID}`,
      })
    }
    return yield* requireActive(tx, incomingID)
  })
}
