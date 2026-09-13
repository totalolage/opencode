export * as SessionDelegation from "./delegation"

import { Buffer } from "node:buffer"
import { and, asc, desc, eq, inArray, or } from "drizzle-orm"
import { Context, Effect, Layer, Schema } from "effect"
import type { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import type { SqlError } from "effect/unstable/sql/SqlError"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { finalizeInTransaction } from "@opencode-ai/core/delegation/source"
import { DelegationGenerationTable, DelegationSourceTable } from "@opencode-ai/core/delegation/sql"
import { Delegation } from "@opencode-ai/schema/delegation"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { MessageTable, PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import type { SessionID } from "@opencode-ai/schema/session-id"

type DB = Database.Interface["db"]
type AdmissionCallback = Parameters<DelegationStore.Interface["admit"]>[1]
type Transaction = Parameters<AdmissionCallback>[0]
type AdmissionResolution = Parameters<AdmissionCallback>[1]
type Query = Pick<DB, "select">
type SessionDelegationError = Delegation.Error | Delegation.AdapterError | EffectDrizzleQueryError | SqlError
type Assembled = Effect.Success<ReturnType<typeof assemble>>
type JsonValue = null | boolean | number | string | JsonValue[] | { readonly [key: string]: JsonValue }
type FinalPart = {
  readonly id: SessionV1.PartID
  readonly part: SessionV1.Part
  readonly data: typeof PartTable.$inferInsert.data
}
type FinalSnapshot = {
  readonly info: SessionV1.Assistant
  readonly messageData: typeof MessageTable.$inferInsert.data
  readonly parts: readonly FinalPart[]
  readonly partIDs: readonly SessionV1.PartID[]
  readonly payload: string
}
type PersistedPart = {
  readonly row: typeof PartTable.$inferSelect
  readonly part: SessionV1.Part
}

export interface Interface {
  readonly deliver: (parentID: SessionID) => Effect.Effect<readonly SessionID[], SessionDelegationError>
  readonly finalize: (
    source: Delegation.SourceRecord,
    message: SessionV1.WithParts,
    outcome: Delegation.Outcome,
  ) => Effect.Effect<void, SessionDelegationError>
  readonly failInput: (
    generation: Delegation.Generation,
    workID: Delegation.WorkID,
    error: string,
  ) => Effect.Effect<void, SessionDelegationError>
  readonly close: (sessionID: SessionID, quiescent: boolean) => Effect.Effect<void, SessionDelegationError>
  readonly blocked: (sessionID: SessionID) => Effect.Effect<boolean, SessionDelegationError>
  readonly history: (
    sessionID: SessionID,
    messages: readonly SessionV1.WithParts[],
  ) => Effect.Effect<
    {
      readonly messages: SessionV1.WithParts[]
      readonly incoming: readonly Delegation.Resolution[]
    },
    SessionDelegationError
  >
}

export function make(db: DB, core: DelegationStore.Interface): Interface {
  const deliver = Effect.fn("SessionDelegation.deliver")(function* (parentID: SessionID) {
    const resolutions = yield* core.pending(parentID)
    const admitted: SessionID[] = []

    for (const resolution of resolutions) {
      const wasAdmitted = yield* db.transaction(
        (tx) =>
          Effect.gen(function* () {
            const fresh = yield* core.getResolution(resolution.id)
            if (fresh === undefined || fresh.parentID !== parentID) return false

            const recipient = yield* tx
              .select({ id: SessionTable.id })
              .from(SessionTable)
              .where(eq(SessionTable.id, fresh.parentID))
              .get()
            if (recipient === undefined) return false
            if (fresh.status === "revoked" || fresh.status === "consumed" || fresh.status === "resolved") return false
            if (fresh.status === "admitted") return true

            const prepared = yield* prepareRecipient(tx, core, fresh)
            if (prepared === undefined) return false
            const result = yield* core.admit(prepared.id, receive)
            return result.status === "admitted"
          }),
        { behavior: "immediate" },
      )
      if (wasAdmitted) admitted.push(parentID)
    }

    return [...new Set(admitted)]
  })

  const finalize = Effect.fn("SessionDelegation.finalize")(function* (
    source: Delegation.SourceRecord,
    message: SessionV1.WithParts,
    outcome: Delegation.Outcome,
  ) {
    yield* db.transaction(
      (tx) =>
        Effect.gen(function* () {
          const authoritative = yield* authoritativeSource(core, source)
          if (authoritative.source.kind !== "assistant") {
            return yield* fail(
              "source_mismatch",
              `Assistant finalization requires an assistant source: ${authoritative.id}`,
            )
          }
          const finalOutcome = yield* decodeOutcome(outcome)
          const snapshot = yield* finalSnapshot(authoritative, message, finalOutcome)

          if (authoritative.state !== "reserved") {
            yield* finalizeInTransaction(tx, authoritative.id, { payload: snapshot.payload, outcome: finalOutcome })
            return
          }

          yield* rejectAssistantBinding(tx, authoritative, snapshot.info.id)
          const existingMessage = yield* tx
            .select()
            .from(MessageTable)
            .where(eq(MessageTable.id, snapshot.info.id))
            .get()
          const existingParts = yield* tx
            .select()
            .from(PartTable)
            .where(
              snapshot.partIDs.length === 0
                ? eq(PartTable.message_id, snapshot.info.id)
                : or(eq(PartTable.message_id, snapshot.info.id), inArray(PartTable.id, snapshot.partIDs)),
            )
            .all()

          const persistedParts = yield* validateExistingProjection(source, snapshot, existingMessage, existingParts)
          yield* persistFinalProjection(tx, source, snapshot, existingMessage, persistedParts)
          yield* finalizeInTransaction(tx, authoritative.id, { payload: snapshot.payload, outcome: finalOutcome })
        }),
      { behavior: "immediate" },
    )
  })

  const failInput = Effect.fn("SessionDelegation.failInput")(function* (
    generation: Delegation.Generation,
    workID: Delegation.WorkID,
    error: string,
  ) {
    yield* db.transaction(
      () =>
        Effect.gen(function* () {
          const current = yield* core.get(generation.id)
          if (current === undefined || !sameGeneration(current, generation)) {
            return yield* fail(
              "generation_mismatch",
              `Delegation generation ${generation.id} does not match durable ownership`,
            )
          }

          const work = (yield* core.listWork(generation.id)).find((item) => item.id === workID)
          if (
            work === undefined ||
            work.sessionID !== generation.childID ||
            work.generationID !== generation.id ||
            (work.kind !== "launch" && work.kind !== "update")
          ) {
            return yield* fail(
              "work_mismatch",
              `Delegation input work ${workID} does not belong to generation ${generation.id}`,
            )
          }

          const reserved = yield* core.reserveSource({
            sessionID: generation.childID,
            generationID: generation.id,
            source: { kind: "terminal", id: `delegation-input:${workID}` },
            historyCutoff: `input:${workID}`,
            consumed: [],
          })
          yield* core.finishWork(workID)
          yield* core.finalizeSource(reserved.source.id, { payload: error, outcome: "error" })
        }),
      { behavior: "immediate" },
    )
  })

  const close = Effect.fn("SessionDelegation.close")(function* (sessionID: SessionID, quiescent: boolean) {
    const generation = yield* core.active(sessionID)
    if (generation === undefined) return
    yield* core.reconcileAndClose(generation.id, () => Effect.succeed({ quiescent, sources: [] }))
  })

  const blocked = Effect.fn("SessionDelegation.blocked")(function* (sessionID: SessionID) {
    return (yield* core.unfinished(sessionID)).length > 0
  })

  const history = Effect.fn("SessionDelegation.history")(function* (
    sessionID: SessionID,
    messages: readonly SessionV1.WithParts[],
  ) {
    const incoming = (yield* core.incoming(sessionID)).filter(
      (resolution) => resolution.status === "admitted" || resolution.status === "consumed",
    )
    if (incoming.length === 0) return { messages: [...messages], incoming }

    const expected = yield* Effect.forEach(incoming, (resolution) => assemble(db, resolution))
    const messageIDs = expected.map((item) => item.messageID)
    const partIDs = expected.flatMap((item) => item.parts.map((part) => part.id))
    const storedMessages = yield* db.select().from(MessageTable).where(inArray(MessageTable.id, messageIDs)).all()
    const storedParts = yield* db
      .select()
      .from(PartTable)
      .where(or(inArray(PartTable.message_id, messageIDs), inArray(PartTable.id, partIDs)))
      .orderBy(asc(PartTable.message_id), asc(PartTable.id))
      .all()

    const messageByID = new Map(storedMessages.map((row) => [row.id, row]))
    const partsByMessage = new Map<string, typeof storedParts>()
    const partByID = new Map(storedParts.map((row) => [row.id, row]))
    for (const row of storedParts) {
      const parts = partsByMessage.get(row.message_id)
      if (parts === undefined) partsByMessage.set(row.message_id, [row])
      else parts.push(row)
    }

    const loaded = yield* Effect.forEach(expected, (item) => load(item, messageByID, partsByMessage, partByID))
    const ids = new Set(incoming.map((resolution) => SessionV1.MessageID.make(resolution.messageID)))
    return {
      messages: [...messages.filter((message) => !ids.has(message.info.id)), ...loaded],
      incoming,
    }
  })

  return { deliver, finalize, failInput, close, blocked, history }
}

function prepareRecipient(query: Query, core: DelegationStore.Interface, resolution: Delegation.Resolution) {
  return Effect.gen(function* () {
    if (resolution.envelope !== undefined) return resolution
    const content = yield* recipientContent(query, core, resolution)
    if (content === undefined) return undefined
    return yield* core.prepare(resolution.id, content)
  })
}

export const receive = Effect.fn("SessionDelegation.receive")(function* (
  tx: Transaction,
  resolution: AdmissionResolution,
) {
  const assembled = yield* assemble(tx, resolution)
  const existingMessage = yield* tx.select().from(MessageTable).where(eq(MessageTable.id, assembled.messageID)).get()
  const existingParts = yield* tx
    .select()
    .from(PartTable)
    .where(or(eq(PartTable.message_id, assembled.messageID), inArray(PartTable.id, assembled.partIDs)))
    .all()

  if (
    existingMessage !== undefined &&
    (existingMessage.session_id !== resolution.parentID ||
      existingMessage.time_created !== resolution.timeCreated ||
      !sameJson(existingMessage.data, assembled.messageData))
  ) {
    return yield* fail("receiver_conflict", `Receiver message conflicts with persisted message ${assembled.messageID}`)
  }

  const expectedParts = new Map(assembled.parts.map((part) => [part.id, part.data]))
  for (const row of existingParts) {
    const expected = expectedParts.get(row.id)
    if (
      expected === undefined ||
      row.message_id !== assembled.messageID ||
      row.session_id !== resolution.parentID ||
      row.time_created !== resolution.timeCreated ||
      !sameJson(row.data, expected)
    ) {
      return yield* fail(
        "receiver_conflict",
        `Receiver part conflicts with persisted part ${row.id} for message ${assembled.messageID}`,
      )
    }
  }

  if (existingMessage === undefined) {
    yield* tx
      .insert(MessageTable)
      .values({
        id: assembled.messageID,
        session_id: resolution.parentID,
        time_created: resolution.timeCreated,
        data: assembled.messageData,
      })
      .run()
  }

  const existingPartIDs = new Set(existingParts.map((part) => part.id))
  const missingParts = assembled.parts.filter((part) => !existingPartIDs.has(part.id))
  if (missingParts.length > 0) {
    yield* tx
      .insert(PartTable)
      .values(
        missingParts.map((part) => ({
          id: part.id,
          message_id: assembled.messageID,
          session_id: resolution.parentID,
          time_created: resolution.timeCreated,
          data: part.data,
        })),
      )
      .run()
  }

  return { status: "admitted" as const }
})

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionDelegation") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const database = yield* Database.Service
    const core = yield* DelegationStore.Service
    return make(database.db, core)
  }),
)

export const node = LayerNode.make({ service: Service, layer, deps: [Database.node, DelegationStore.node] })

function recipientContent(query: Query, core: DelegationStore.Interface, resolution: Delegation.Resolution) {
  return Effect.gen(function* () {
    const session = yield* query
      .select({ agent: SessionTable.agent })
      .from(SessionTable)
      .where(eq(SessionTable.id, resolution.parentID))
      .get()
    if (session === undefined) return undefined
    const agent = yield* recipientAgent(query, core, resolution, session.agent)
    if (agent === undefined) {
      return yield* fail("agent_unavailable", `No agent is available for delegation recipient ${resolution.parentID}`)
    }

    const model = yield* recipientModel(query, core, resolution)
    return {
      message: {
        role: "user",
        time: { created: resolution.timeCreated },
        agent,
        model,
      },
      parts: [
        {
          type: "text",
          synthetic: true,
          text: renderResult(resolution),
        },
      ],
    }
  })
}

function recipientAgent(
  query: Query,
  core: DelegationStore.Interface,
  resolution: Delegation.Resolution,
  sessionAgent: string | null | undefined,
) {
  return Effect.gen(function* () {
    if (sessionAgent !== undefined && sessionAgent !== null && sessionAgent.length > 0) return sessionAgent

    const rows = yield* query
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.session_id, resolution.parentID))
      .orderBy(desc(MessageTable.time_created), desc(MessageTable.id))
      .all()
    const user = rows.find((row) => row.data.role === "user")
    if (user !== undefined) {
      const info = yield* decodeInfo(user, "parent user")
      if (info.role === "user" && info.agent !== undefined && info.agent.length > 0) return info.agent
    }

    const generation = yield* core.get(resolution.generationID)
    if (generation === undefined) return undefined
    const assistant = yield* query
      .select()
      .from(MessageTable)
      .where(
        and(
          eq(MessageTable.id, SessionV1.MessageID.make(generation.origin.messageID)),
          eq(MessageTable.session_id, resolution.parentID),
        ),
      )
      .get()
    if (assistant === undefined || assistant.data.role !== "assistant") return undefined
    const info = yield* decodeInfo(assistant, "Task origin assistant")
    if (info.role !== "assistant" || info.agent === undefined || info.agent.length === 0) return undefined
    return info.agent
  })
}

function authoritativeSource(core: DelegationStore.Interface, provided: Delegation.SourceRecord) {
  return Effect.gen(function* () {
    const source = (yield* core.sources(provided.sessionID)).find((item) => item.id === provided.id)
    if (source === undefined) {
      return yield* fail("source_not_found", `Delegation source was not found: ${provided.id}`)
    }
    if (
      source.id !== provided.id ||
      source.sessionID !== provided.sessionID ||
      source.generationID !== provided.generationID ||
      !sameJson(source.source, provided.source) ||
      source.historyCutoff !== provided.historyCutoff ||
      !sameJson(source.consumed, provided.consumed) ||
      source.workID !== provided.workID
    ) {
      return yield* fail("source_mismatch", `Delegation source does not match durable ownership: ${provided.id}`)
    }
    return source
  })
}

function finalSnapshot(source: Delegation.SourceRecord, message: SessionV1.WithParts, outcome: Delegation.Outcome) {
  return Effect.gen(function* () {
    const normalizedInfo = yield* normalizeJson(message.info, "message.info")
    const decodedInfo = yield* decodeFinalInfo(normalizedInfo, "finalization message")
    if (decodedInfo.role !== "assistant") {
      return yield* fail("invalid_message", "Delegation finalization requires an assistant message")
    }
    if (decodedInfo.id !== source.source.id || decodedInfo.sessionID !== source.sessionID) {
      return yield* fail(
        "source_mismatch",
        `Assistant message ${decodedInfo.id} does not belong to source ${source.id}`,
      )
    }
    const canonicalInfo = yield* normalizeJson(mutableAssistant(decodedInfo), "message.info")
    if (!sameJson(normalizedInfo, canonicalInfo)) {
      return yield* fail("invalid_message", `Finalization message ${decodedInfo.id} is not canonical V1 data`)
    }
    const info = mutableAssistant(decodedInfo)
    const inputParts = Array.isArray(message.parts) ? message.parts : undefined
    if (inputParts === undefined)
      return yield* fail("invalid_part", "Delegation finalization requires an array of parts")

    const seen = new Set<SessionV1.PartID>()
    const parts = yield* Effect.forEach(inputParts, (input, index) => {
      return Effect.gen(function* () {
        const normalized = yield* normalizeJson(input, `message.parts[${index}]`)
        const decoded = yield* decodeFinalPart(normalized, `finalization part ${index}`)
        const canonical = yield* normalizeJson(mutablePart(decoded), `message.parts[${index}]`)
        if (!sameJson(normalized, canonical)) {
          return yield* fail("invalid_part", `Finalization part ${decoded.id} is not canonical V1 data`)
        }
        if (decoded.sessionID !== info.sessionID || decoded.messageID !== info.id) {
          return yield* fail("receiver_conflict", `Delegation part ${decoded.id} has conflicting ownership`)
        }
        if (seen.has(decoded.id))
          return yield* fail("receiver_conflict", `Delegation finalization repeats part ${decoded.id}`)
        seen.add(decoded.id)
        const part = mutablePart(decoded)
        const { id: _, messageID: __, sessionID: ___, ...data } = part
        return { id: part.id, part, data }
      })
    })
    const ordered = [...parts].sort((left, right) => comparePartID(left.id, right.id))
    const { id: _, sessionID: __, ...messageData } = info
    const payload = yield* finalPayload(
      info,
      ordered.map((item) => item.part),
      outcome,
    )
    return {
      info,
      messageData,
      parts: ordered,
      partIDs: ordered.map((item) => item.id),
      payload,
    }
  })
}

function finalPayload(info: SessionV1.Assistant, parts: readonly SessionV1.Part[], outcome: Delegation.Outcome) {
  return Effect.gen(function* () {
    if (outcome !== "reply") {
      const message = assistantErrorMessage(info.error)
      if (message !== undefined) return message
    }

    if (info.structured !== undefined) {
      const structured = yield* normalizeJson(info.structured, "message.structured", new Set(), "strict")
      const serialized = JSON.stringify(structured)
      if (serialized === undefined) return yield* fail("invalid_message", "Structured assistant output is not JSON")
      return serialized
    }

    const text = parts.findLast((part) => part.type === "text")
    if (text?.type === "text" && text.text.length > 0) return text.text
    if (outcome !== "reply") return outcome
    return text?.type === "text" ? text.text : ""
  })
}

function rejectAssistantBinding(tx: Transaction, source: Delegation.SourceRecord, messageID: SessionV1.MessageID) {
  return Effect.gen(function* () {
    const bindings = yield* tx
      .select({ id: DelegationSourceTable.id })
      .from(DelegationSourceTable)
      .where(and(eq(DelegationSourceTable.source_kind, "assistant"), eq(DelegationSourceTable.source_id, messageID)))
      .all()
    if (bindings.some((binding) => binding.id !== source.id)) {
      return yield* fail("source_conflict", `Assistant message ${messageID} is already bound to another source`)
    }
  })
}

function validateExistingProjection(
  source: Delegation.SourceRecord,
  snapshot: FinalSnapshot,
  existingMessage: typeof MessageTable.$inferSelect | undefined,
  existingParts: readonly (typeof PartTable.$inferSelect)[],
) {
  return Effect.gen(function* () {
    if (existingMessage !== undefined) {
      if (existingMessage.session_id !== source.sessionID || existingMessage.data.role !== "assistant") {
        return yield* fail(
          "receiver_conflict",
          `Persisted assistant message ${snapshot.info.id} has conflicting ownership`,
        )
      }
      const info = yield* decodeInfo(existingMessage, "persisted assistant")
      if (info.role !== "assistant") {
        return yield* fail("receiver_conflict", `Persisted message ${snapshot.info.id} is not an assistant message`)
      }
    }

    const persisted = (yield* Effect.forEach(existingParts, (row) => {
      if (row.message_id !== snapshot.info.id || row.session_id !== source.sessionID) {
        return fail<PersistedPart>("receiver_conflict", `Persisted part ${row.id} has conflicting ownership`)
      }
      return persistedPart(row)
    })).sort((left, right) => comparePartID(left.row.id, right.row.id))
    const persistedStepParts = persisted
      .filter(
        (item): item is PersistedPart & { readonly part: Extract<SessionV1.Part, { type: "step-finish" }> } =>
          item.part.type === "step-finish",
      )
      .map((item) => item.part)
    const aggregate = aggregateStepFinishUsage(persistedStepParts)
    if (!sameJson(aggregateStepFinishUsage(snapshot.parts.map((item) => item.part)), aggregate)) {
      return yield* fail(
        "usage_conflict",
        `Assistant message ${snapshot.info.id} has step-finish usage different from projected parts`,
      )
    }
    const incoming = new Map(snapshot.parts.map((item) => [item.id, item.part]))
    for (const item of persisted) {
      const next = incoming.get(item.row.id)
      if (item.part.type === "step-finish" && next === undefined && hasStepFinishUsage(item.part)) {
        return yield* fail(
          "usage_conflict",
          `Persisted step-finish part ${item.row.id} cannot be removed without changing session usage`,
        )
      }
      if (next === undefined) continue
      if (item.part.type !== next.type) {
        return yield* fail("receiver_conflict", `Persisted part ${item.row.id} changed type during finalization`)
      }
      if (
        item.part.type === "step-finish" &&
        next.type === "step-finish" &&
        !sameJson(stepFinishUsage(item.part), stepFinishUsage(next))
      ) {
        return yield* fail("usage_conflict", `Persisted step-finish part ${item.row.id} has different session usage`)
      }
    }
    return persisted
  })
}

function persistFinalProjection(
  tx: Transaction,
  source: Delegation.SourceRecord,
  snapshot: FinalSnapshot,
  existingMessage: typeof MessageTable.$inferSelect | undefined,
  existingParts: readonly PersistedPart[],
) {
  return Effect.gen(function* () {
    if (existingMessage === undefined) {
      yield* tx
        .insert(MessageTable)
        .values({
          id: snapshot.info.id,
          session_id: source.sessionID,
          time_created: snapshot.info.time.created,
          data: snapshot.messageData,
        })
        .run()
    } else if (!sameJson(existingMessage.data, snapshot.messageData)) {
      yield* tx
        .update(MessageTable)
        .set({ data: snapshot.messageData })
        .where(and(eq(MessageTable.id, snapshot.info.id), eq(MessageTable.session_id, source.sessionID)))
        .run()
    }

    const existingByID = new Map(existingParts.map((item) => [item.row.id, item.row]))
    for (const part of snapshot.parts) {
      const existing = existingByID.get(part.id)
      if (existing !== undefined) {
        if (sameJson(existing.data, part.data)) continue
        yield* tx
          .update(PartTable)
          .set({ data: part.data })
          .where(
            and(
              eq(PartTable.id, part.id),
              eq(PartTable.message_id, snapshot.info.id),
              eq(PartTable.session_id, source.sessionID),
            ),
          )
          .run()
        continue
      }
      yield* tx
        .insert(PartTable)
        .values({
          id: part.id,
          message_id: snapshot.info.id,
          session_id: source.sessionID,
          time_created: snapshot.info.time.created,
          data: part.data,
        })
        .run()
    }

    const incomingIDs = new Set(snapshot.partIDs)
    const removed = existingParts.filter((item) => !incomingIDs.has(item.row.id)).map((item) => item.row.id)
    if (removed.length > 0) {
      yield* tx
        .delete(PartTable)
        .where(
          and(
            eq(PartTable.message_id, snapshot.info.id),
            eq(PartTable.session_id, source.sessionID),
            inArray(PartTable.id, removed),
          ),
        )
        .run()
    }
  })
}

function persistedPart(row: typeof PartTable.$inferSelect) {
  return decodeFinalPart(
    {
      ...row.data,
      id: SessionV1.PartID.make(row.id),
      sessionID: row.session_id,
      messageID: SessionV1.MessageID.make(row.message_id),
    },
    `persisted part ${row.id}`,
  ).pipe(Effect.map((part) => ({ row, part: mutablePart(part) })))
}

function decodeFinalInfo(input: unknown, source: string) {
  return Schema.decodeUnknownEffect(SessionV1.Info)(input).pipe(
    Effect.mapError(
      (error) =>
        new Delegation.AdapterError({
          code: "invalid_message",
          message: `Invalid ${source}: ${error.message}`,
        }),
    ),
  )
}

function decodeFinalPart(input: unknown, source: string) {
  return Schema.decodeUnknownEffect(SessionV1.Part)(input).pipe(
    Effect.mapError(
      (error) =>
        new Delegation.AdapterError({
          code: "invalid_part",
          message: `Invalid ${source}: ${error.message}`,
        }),
    ),
  )
}

function decodeOutcome(outcome: unknown) {
  return Schema.decodeUnknownEffect(Delegation.Outcome)(outcome).pipe(
    Effect.mapError(
      (error) =>
        new Delegation.AdapterError({
          code: "invalid_outcome",
          message: `Invalid delegation outcome: ${error.message}`,
        }),
    ),
  )
}

function normalizeJson(
  value: unknown,
  path: string,
  ancestors = new Set<object>(),
  mode: "v1" | "strict" = "v1",
): Effect.Effect<JsonValue, Delegation.AdapterError> {
  if (value === null) return Effect.succeed(null)
  if (typeof value === "string" || typeof value === "boolean") return Effect.succeed(value)
  if (typeof value === "number") {
    if (Number.isFinite(value)) return Effect.succeed(value)
    return fail("invalid_json", `Non-finite JSON value at ${path}`)
  }
  if (typeof value === "undefined") return fail("invalid_json", `Undefined JSON value at ${path}`)
  if (typeof value !== "object") return fail("invalid_json", `Unsupported JSON value at ${path}`)
  if (ancestors.has(value)) return fail("invalid_json", `Cyclic JSON value at ${path}`)
  if (Object.getOwnPropertySymbols(value).length > 0) return fail("invalid_json", `Symbol JSON key at ${path}`)

  const nextAncestors = new Set(ancestors)
  nextAncestors.add(value)
  if (Array.isArray(value)) {
    return Effect.gen(function* () {
      const normalized: JsonValue[] = []
      for (let index = 0; index < value.length; index++) {
        normalized.push(yield* normalizeJson(value[index], `${path}[${index}]`, nextAncestors, mode))
      }
      return normalized
    })
  }
  if (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) {
    return fail("invalid_json", `Unsupported JSON object at ${path}`)
  }
  if (!isRecord(value)) return fail("invalid_json", `Unsupported JSON object at ${path}`)
  return Effect.gen(function* () {
    const entries: Array<[string, JsonValue]> = []
    for (const key of Object.keys(value).sort()) {
      const item = value[key]
      if (item === undefined) {
        if (mode === "strict") return yield* fail("invalid_json", `Undefined JSON value at ${path}.${key}`)
        continue
      }
      const childMode =
        mode === "strict" || key === "structured" || key === "metadata" || key === "input" ? "strict" : "v1"
      entries.push([key, yield* normalizeJson(item, `${path}.${key}`, nextAncestors, childMode)])
    }
    return Object.fromEntries(entries)
  })
}

function assistantErrorMessage(error: unknown) {
  if (
    !isRecord(error) ||
    !isRecord(error.data) ||
    typeof error.data.message !== "string" ||
    error.data.message.length === 0
  ) {
    return undefined
  }
  return error.data.message
}

function sameGeneration(left: Delegation.Generation, right: Delegation.Generation) {
  return (
    left.id === right.id &&
    left.parentID === right.parentID &&
    left.childID === right.childID &&
    sameJson(left.origin, right.origin) &&
    left.parentGenerationID === right.parentGenerationID &&
    left.timeCreated === right.timeCreated
  )
}

function stepFinishUsage(part: Extract<SessionV1.Part, { type: "step-finish" }>) {
  return {
    cost: part.cost,
    tokens: {
      total: part.tokens.total,
      input: part.tokens.input,
      output: part.tokens.output,
      reasoning: part.tokens.reasoning,
      cache: {
        read: part.tokens.cache.read,
        write: part.tokens.cache.write,
      },
    },
  }
}

function aggregateStepFinishUsage(parts: readonly SessionV1.Part[]) {
  const steps = parts
    .filter((part): part is Extract<SessionV1.Part, { type: "step-finish" }> => part.type === "step-finish")
    .sort((left, right) => comparePartID(left.id, right.id))
  return {
    cost: steps.reduce((total, part) => total + part.cost, 0),
    tokens: {
      input: steps.reduce((total, part) => total + part.tokens.input, 0),
      output: steps.reduce((total, part) => total + part.tokens.output, 0),
      reasoning: steps.reduce((total, part) => total + part.tokens.reasoning, 0),
      cache: {
        read: steps.reduce((total, part) => total + part.tokens.cache.read, 0),
        write: steps.reduce((total, part) => total + part.tokens.cache.write, 0),
      },
    },
  }
}

function hasStepFinishUsage(part: Extract<SessionV1.Part, { type: "step-finish" }>) {
  const tokens = part.tokens
  return (
    part.cost !== 0 ||
    (tokens.total !== undefined && tokens.total !== 0) ||
    tokens.input !== 0 ||
    tokens.output !== 0 ||
    tokens.reasoning !== 0 ||
    tokens.cache.read !== 0 ||
    tokens.cache.write !== 0
  )
}

function mutableAssistant(info: Schema.Schema.Type<typeof SessionV1.Assistant>): SessionV1.Assistant {
  const { error, summary, structured, variant, finish, time, ...rest } = info
  return {
    ...rest,
    time: time.completed === undefined ? { created: time.created } : { ...time },
    path: { ...info.path },
    tokens: { ...info.tokens, cache: { ...info.tokens.cache } },
    ...(error === undefined ? {} : { error }),
    ...(summary === undefined ? {} : { summary }),
    ...(structured === undefined ? {} : { structured }),
    ...(variant === undefined ? {} : { variant }),
    ...(finish === undefined ? {} : { finish }),
  }
}

function recipientModel(query: Query, core: DelegationStore.Interface, resolution: Delegation.Resolution) {
  return Effect.gen(function* () {
    const rows = yield* query
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.session_id, resolution.parentID))
      .orderBy(desc(MessageTable.time_created), desc(MessageTable.id))
      .all()
    const user = rows.find((row) => row.data.role === "user")
    if (user !== undefined) {
      const info = yield* decodeInfo(user, "parent user")
      if (info.role === "user") return modelRef(info.model)
      return yield* modelUnavailable(resolution.parentID)
    }

    const generation = yield* core.get(resolution.generationID)
    if (generation === undefined) return yield* modelUnavailable(resolution.parentID)
    const assistant = yield* query
      .select()
      .from(MessageTable)
      .where(
        and(
          eq(MessageTable.id, SessionV1.MessageID.make(generation.origin.messageID)),
          eq(MessageTable.session_id, resolution.parentID),
        ),
      )
      .get()
    if (assistant === undefined || assistant.data.role !== "assistant") {
      return yield* modelUnavailable(resolution.parentID)
    }
    const info = yield* decodeInfo(assistant, "Task origin assistant")
    if (info.role === "assistant") return modelRef(info)
    return yield* modelUnavailable(resolution.parentID)
  })
}

function modelRef(model: { readonly providerID: string; readonly modelID: string; readonly variant?: string }) {
  return {
    providerID: model.providerID,
    modelID: model.modelID,
    ...(model.variant === undefined ? {} : { variant: model.variant }),
  }
}

function decodeInfo(row: typeof MessageTable.$inferSelect, source: string) {
  return Schema.decodeUnknownEffect(SessionV1.Info)({
    ...row.data,
    id: SessionV1.MessageID.make(row.id),
    sessionID: row.session_id,
  }).pipe(
    Effect.mapError(
      (error) =>
        new Delegation.AdapterError({
          code: "invalid_message",
          message: `Invalid ${source} message ${row.id}: ${error.message}`,
        }),
    ),
  )
}

function assemble(query: Query, resolution: Delegation.Resolution) {
  return Effect.gen(function* () {
    if (resolution.envelope === undefined) {
      return yield* fail("missing_envelope", `Delegation resolution ${resolution.id} has no prepared envelope`)
    }
    const envelope = yield* Schema.decodeUnknownEffect(Delegation.Envelope)(resolution.envelope).pipe(
      Effect.mapError(
        (error) =>
          new Delegation.AdapterError({
            code: "invalid_envelope",
            message: `Invalid delegation envelope ${resolution.id}: ${error.message}`,
          }),
      ),
    )
    const messageID = yield* Schema.decodeUnknownEffect(SessionV1.MessageID)(envelope.message.id).pipe(
      Effect.mapError(
        (error) =>
          new Delegation.AdapterError({
            code: "invalid_message",
            message: `Invalid delegation message ID ${envelope.message.id}: ${error.message}`,
          }),
      ),
    )
    const parentID = resolution.parentID
    const generation = yield* query
      .select({
        origin: DelegationGenerationTable.origin,
        parent_id: DelegationGenerationTable.parent_id,
        child_id: DelegationGenerationTable.child_id,
      })
      .from(DelegationGenerationTable)
      .where(eq(DelegationGenerationTable.id, resolution.generationID))
      .get()
    if (
      generation === undefined ||
      generation.parent_id !== resolution.parentID ||
      generation.child_id !== resolution.childID ||
      envelope.message.id !== resolution.messageID ||
      envelope.provenance.generationID !== resolution.generationID ||
      envelope.provenance.parentID !== resolution.parentID ||
      envelope.provenance.childID !== resolution.childID ||
      envelope.provenance.recipientGenerationID !== resolution.recipientGenerationID ||
      !sameJson(envelope.provenance.origin, generation.origin) ||
      !sameJson(envelope.provenance.source, resolution.source) ||
      envelope.provenance.historyCutoff !== resolution.historyCutoff ||
      !sameJson(envelope.provenance.consumed, resolution.consumed)
    ) {
      return yield* fail("receiver_conflict", `Delegation envelope ${resolution.id} has conflicting ownership`)
    }

    const messageDataRecord = isRecord(envelope.message.data) ? envelope.message.data : undefined
    if (messageDataRecord === undefined) {
      return yield* fail("invalid_message", `Delegation envelope ${resolution.id} has a non-object message`)
    }
    const messageIdentity = identityKey(messageDataRecord, ["id", "sessionID"])
    if (messageIdentity !== undefined) {
      return yield* fail("receiver_conflict", `Delegation message data contains ${messageIdentity}`)
    }
    const decodedInfo = yield* Schema.decodeUnknownEffect(SessionV1.Info)({
      ...messageDataRecord,
      id: messageID,
      sessionID: parentID,
    }).pipe(
      Effect.mapError(
        (error) =>
          new Delegation.AdapterError({
            code: "invalid_message",
            message: `Invalid delegation message ${messageID}: ${error.message}`,
          }),
      ),
    )
    if (decodedInfo.role !== "user")
      return yield* fail("invalid_message", `Delegation message ${messageID} is not a user message`)
    const info = mutableUser(decodedInfo)
    if (info.time.created !== resolution.timeCreated) {
      return yield* fail(
        "receiver_conflict",
        `Delegation message ${messageID} has a creation time different from its resolution`,
      )
    }
    const { id: _, sessionID: __, ...messageData } = info
    if (!sameJson(messageDataRecord, messageData)) {
      return yield* fail("invalid_message", `Delegation message ${messageID} is not canonical V1 data`)
    }

    const decodedParts: Array<Schema.Schema.Type<typeof SessionV1.Part>> = []
    const partIDs = new Set<SessionV1.PartID>()
    for (const item of envelope.parts) {
      const partID = yield* Schema.decodeUnknownEffect(SessionV1.PartID)(item.id).pipe(
        Effect.mapError(
          (error) =>
            new Delegation.AdapterError({
              code: "invalid_part",
              message: `Invalid delegation part ID ${item.id}: ${error.message}`,
            }),
        ),
      )
      if (partIDs.has(partID)) return yield* fail("receiver_conflict", `Delegation envelope repeats part ${item.id}`)
      partIDs.add(partID)
      const data = isRecord(item.data) ? item.data : undefined
      if (data === undefined) return yield* fail("invalid_part", `Delegation part ${item.id} is not an object`)
      const partIdentity = identityKey(data, ["id", "sessionID", "messageID"])
      if (partIdentity !== undefined) {
        return yield* fail("receiver_conflict", `Delegation part data contains ${partIdentity}`)
      }
      const part = yield* Schema.decodeUnknownEffect(SessionV1.Part)({
        ...data,
        id: partID,
        sessionID: parentID,
        messageID,
      }).pipe(
        Effect.mapError(
          (error) =>
            new Delegation.AdapterError({
              code: "invalid_part",
              message: `Invalid delegation part ${item.id}: ${error.message}`,
            }),
        ),
      )
      decodedParts.push(part)
      const normalizedPart = mutablePart(part)
      const { id: _, messageID: __, sessionID: ___, ...partData } = normalizedPart
      if (!sameJson(data, partData)) {
        return yield* fail("invalid_part", `Delegation part ${item.id} is not canonical V1 data`)
      }
    }

    const syntheticTextIndexes = decodedParts.flatMap((part, index) =>
      part.type === "text" && part.synthetic === true ? [index] : [],
    )
    if (syntheticTextIndexes.length !== 1) {
      return yield* fail("invalid_part", `Delegation message ${messageID} requires exactly one synthetic text part`)
    }
    const syntheticTextIndex = syntheticTextIndexes[0]
    if (syntheticTextIndex === undefined)
      return yield* fail("invalid_part", `Delegation message ${messageID} has no text part`)
    const parts = decodedParts.map((part, index) => {
      if (index !== syntheticTextIndex || part.type !== "text") return mutablePart(part)
      if (
        part.metadata !== undefined &&
        isRecord(part.metadata) &&
        Object.hasOwn(part.metadata, "delegation") &&
        !sameJson(part.metadata.delegation, envelope.provenance)
      ) {
        return mutablePart(part)
      }
      return mutablePart({
        ...part,
        metadata: {
          ...(part.metadata ?? {}),
          delegation: envelope.provenance,
        },
      })
    })
    const conflictingMetadata = parts[syntheticTextIndex]
    if (
      conflictingMetadata?.type === "text" &&
      conflictingMetadata.metadata !== undefined &&
      isRecord(conflictingMetadata.metadata) &&
      Object.hasOwn(conflictingMetadata.metadata, "delegation") &&
      !sameJson(conflictingMetadata.metadata.delegation, envelope.provenance)
    ) {
      return yield* fail("receiver_conflict", `Delegation part ${conflictingMetadata.id} has conflicting provenance`)
    }

    const expectedParts = parts.map((part) => {
      const { id: _, messageID: __, sessionID: ___, ...data } = part
      return { id: part.id, data }
    })
    return { messageID, messageData, info, parts: expectedParts, partIDs: [...partIDs] }
  })
}

function load(
  expected: Assembled,
  messageByID: ReadonlyMap<string, typeof MessageTable.$inferSelect>,
  partsByMessage: ReadonlyMap<string, (typeof PartTable.$inferSelect)[]>,
  partByID: ReadonlyMap<string, typeof PartTable.$inferSelect>,
) {
  return Effect.gen(function* () {
    const message = messageByID.get(expected.messageID)
    if (message === undefined)
      return yield* fail("message_missing", `Delegation message ${expected.messageID} was not persisted`)
    if (
      message.session_id !== expected.info.sessionID ||
      message.time_created !== expected.info.time.created ||
      !sameJson(message.data, expected.messageData) ||
      identityKey(message.data, ["id", "sessionID"]) !== undefined
    ) {
      return yield* fail("receiver_conflict", `Persisted delegation message ${expected.messageID} is not canonical`)
    }
    const decodedInfo = yield* decodeInfo(message, "persisted delegation")
    if (decodedInfo.role !== "user")
      return yield* fail("invalid_message", `Persisted delegation message ${message.id} is not a user message`)
    const info = mutableUser(decodedInfo)

    const rows = partsByMessage.get(expected.messageID) ?? []
    const rowsByID = new Map(rows.map((row) => [row.id, row]))
    if (rows.length !== expected.parts.length) {
      return yield* fail(
        "receiver_conflict",
        `Persisted delegation message ${expected.messageID} has extra or missing parts`,
      )
    }
    const parts: SessionV1.Part[] = []
    for (const expectedPart of expected.parts) {
      const row = partByID.get(expectedPart.id)
      if (
        row === undefined ||
        rowsByID.get(expectedPart.id) !== row ||
        row.message_id !== expected.messageID ||
        row.session_id !== expected.info.sessionID ||
        row.time_created !== expected.info.time.created ||
        !sameJson(row.data, expectedPart.data) ||
        identityKey(row.data, ["id", "sessionID", "messageID"]) !== undefined
      ) {
        return yield* fail("receiver_conflict", `Persisted delegation part ${expectedPart.id} is not canonical`)
      }
      const part = yield* Schema.decodeUnknownEffect(SessionV1.Part)({
        ...row.data,
        id: SessionV1.PartID.make(row.id),
        sessionID: row.session_id,
        messageID: SessionV1.MessageID.make(row.message_id),
      }).pipe(
        Effect.mapError(
          (error) =>
            new Delegation.AdapterError({
              code: "invalid_part",
              message: `Invalid persisted delegation part ${row.id}: ${error.message}`,
            }),
        ),
      )
      parts.push(mutablePart(part))
    }
    return { info, parts }
  })
}

function mutableUser(info: Schema.Schema.Type<typeof SessionV1.User>): SessionV1.User {
  const { summary, ...rest } = info
  return {
    ...rest,
    ...(summary === undefined
      ? {}
      : {
          summary: {
            ...summary,
            diffs: summary.diffs.map((diff) => ({ ...diff })),
          },
        }),
  }
}

function mutablePart(part: Schema.Schema.Type<typeof SessionV1.Part>): SessionV1.Part {
  if (part.type === "patch") return { ...part, files: [...part.files] }
  if (part.type === "tool") return { ...part, state: mutableToolState(part.state) }
  return part
}

function mutableToolState(state: Schema.Schema.Type<typeof SessionV1.ToolState>): SessionV1.ToolState {
  if (state.status === "pending") return { ...state, input: { ...state.input } }
  if (state.status === "running") {
    const { metadata, ...rest } = state
    return {
      ...rest,
      input: { ...state.input },
      ...(metadata === undefined ? {} : { metadata: { ...metadata } }),
      time: { ...state.time },
    }
  }
  if (state.status === "completed") {
    const { attachments, ...rest } = state
    return {
      ...rest,
      input: { ...state.input },
      metadata: { ...state.metadata },
      time: { ...state.time },
      ...(attachments === undefined ? {} : { attachments: attachments.map((attachment) => ({ ...attachment })) }),
    }
  }
  const { metadata, ...rest } = state
  return {
    ...rest,
    input: { ...state.input },
    ...(metadata === undefined ? {} : { metadata: { ...metadata } }),
    time: { ...state.time },
  }
}

function renderResult(resolution: Delegation.Resolution) {
  const tag =
    resolution.outcome === "reply" ? "task_result" : resolution.outcome === "error" ? "task_error" : "task_cancelled"
  return [
    "<task>",
    `<task_id>${resolution.childID}</task_id>`,
    `<${tag}>`,
    resolution.payload,
    `</${tag}>`,
    "</task>",
  ].join("\n")
}

function identityKey(value: Record<string, unknown>, keys: readonly string[]) {
  return keys.find((key) => Object.hasOwn(value, key))
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function sameJson(left: unknown, right: unknown) {
  return canonicalJson(left) === canonicalJson(right)
}

function canonicalJson(value: unknown): string {
  const result = JSON.stringify(canonicalize(value))
  return result === undefined ? "undefined" : result
}

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize)
  if (!isRecord(value)) return value
  return Object.fromEntries(
    Object.entries(value)
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
      .map(([key, item]) => [key, canonicalize(item)]),
  )
}

function comparePartID(left: string, right: string) {
  return Buffer.compare(Buffer.from(left), Buffer.from(right))
}

function modelUnavailable(sessionID: SessionID): Effect.Effect<never, Delegation.AdapterError> {
  return fail("model_unavailable", `No model is available for delegation recipient ${sessionID}`)
}

function fail<A = never>(code: string, message: string): Effect.Effect<A, Delegation.AdapterError> {
  return Effect.fail(new Delegation.AdapterError({ code, message }))
}
