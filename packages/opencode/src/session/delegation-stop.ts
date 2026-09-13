export * as SessionDelegationStop from "./delegation-stop"

import { and, eq, sql } from "drizzle-orm"
import { Context, Effect, Layer } from "effect"
import type { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import type { SqlError } from "effect/unstable/sql/SqlError"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Delegation } from "@opencode-ai/schema/delegation"
import type { SessionID } from "@opencode-ai/schema/session-id"
import { MessageTable, PartTable } from "@opencode-ai/core/session/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { NotFoundError } from "@/storage/storage"
import type { MessageID } from "@/session/schema"
import { MessageV2 } from "@/session/message-v2"
import { SessionDelegation } from "@/session/delegation"

type DB = Database.Interface["db"]
type SessionDelegationStopError =
  | Delegation.Error
  | Delegation.AdapterError
  | EffectDrizzleQueryError
  | SqlError
  | NotFoundError

export interface Interface {
  readonly finish: (
    sessionID: SessionID,
    stop: Effect.Success<ReturnType<DelegationStore.Interface["revokeDescendants"]>>,
    reserved: readonly Delegation.SourceRecord[],
  ) => Effect.Effect<readonly SessionID[], SessionDelegationStopError>
  readonly projectRevoked: (sessionID: SessionID) => Effect.Effect<readonly MessageID[], SessionDelegationStopError>
}

export function make(db: DB, core: DelegationStore.Interface, adapter: SessionDelegation.Interface): Interface {
  const finish = Effect.fn("SessionDelegationStop.finish")(function* (
    sessionID: SessionID,
    stop: Effect.Success<ReturnType<DelegationStore.Interface["revokeDescendants"]>>,
    reserved: readonly Delegation.SourceRecord[],
  ) {
    const generation = yield* core.active(sessionID)
    const candidate = reserved.findLast(
      (source) =>
        source.sessionID === sessionID && source.source.kind === "assistant" && source.generationID === generation?.id,
    )
    const source =
      candidate === undefined ? undefined : (yield* core.sources(sessionID)).find((item) => item.id === candidate.id)

    if (source?.state === "finalized" && source.outcome === "cancelled") {
      return generation?.mode === "background" ? [generation.parentID] : []
    }

    if (source?.state === "reserved") {
      const legacy = yield* db
        .select({ id: MessageTable.id })
        .from(MessageTable)
        .where(sql`${MessageTable.id} = ${source.source.id}`)
        .get()
      if (legacy !== undefined) {
        const message = yield* MessageV2.get({ sessionID, messageID: legacy.id }).pipe(
          Effect.provideService(Database.Service, { db }),
        )
        const snapshot = interruptedSnapshot(message)
        if (snapshot !== undefined) {
          const result = yield* adapter.finalize(source, snapshot, "cancelled").pipe(
            Effect.map(() => "recorded" as const),
            Effect.catchIf(
              (error): error is Delegation.Error => error instanceof Delegation.Error,
              (error) => (isIneligible(error) ? Effect.succeed("ineligible" as const) : Effect.fail(error)),
            ),
          )
          if (result === "ineligible") return []
          return generation?.mode === "background" ? [generation.parentID] : []
        }
      }
    }

    if (generation === undefined) return []

    const fallback = yield* core
      .reserveSource({
        sessionID,
        generationID: generation.id,
        source: stop.cancellationSource,
        historyCutoff: `cancel:${stop.revocationID}`,
        consumed: [],
      })
      .pipe(
        Effect.catchIf(
          (error): error is Delegation.Error => error instanceof Delegation.Error,
          (error) => (isIneligible(error) ? Effect.succeed(undefined) : Effect.fail(error)),
        ),
      )
    if (fallback === undefined) return []
    const result = yield* core
      .finalizeSource(fallback.source.id, { payload: "Task cancelled", outcome: "cancelled" })
      .pipe(
        Effect.map(() => "recorded" as const),
        Effect.catchIf(
          (error): error is Delegation.Error => error instanceof Delegation.Error,
          (error) => (isIneligible(error) ? Effect.succeed("ineligible" as const) : Effect.fail(error)),
        ),
      )
    if (result === "ineligible") return []
    return generation.mode === "background" ? [generation.parentID] : []
  })

  const projectRevoked = Effect.fn("SessionDelegationStop.projectRevoked")(function* (sessionID: SessionID) {
    const sources = (yield* core.unfinishedSources(sessionID)).filter(
      (source) => source.source.kind === "assistant" && source.generationID !== undefined,
    )
    const messageIDs = yield* Effect.forEach(sources, (source) => projectRevokedSource(db, core, sessionID, source))
    return messageIDs.filter((messageID): messageID is MessageID => messageID !== undefined)
  })

  return { finish, projectRevoked }
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionDelegationStop") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const database = yield* Database.Service
    const core = yield* DelegationStore.Service
    const adapter = yield* SessionDelegation.Service
    return make(database.db, core, adapter)
  }),
)

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [Database.node, DelegationStore.node, SessionDelegation.node],
})

function isIneligible(error: Delegation.Error) {
  return error.code === "generation_not_active" || error.code === "session_revoked"
}

type InterruptedSnapshot = {
  readonly info: SessionV1.Assistant
  readonly parts: SessionV1.Part[]
  readonly toolParts: readonly SessionV1.ToolPart[]
}

function interruptedSnapshot(message: SessionV1.WithParts): InterruptedSnapshot | undefined {
  if (message.info.role !== "assistant") return undefined

  const now = Date.now()
  const toolParts: SessionV1.ToolPart[] = []
  const parts: SessionV1.Part[] = message.parts.map((part) => {
    if (part.type !== "tool" || (part.state.status !== "pending" && part.state.status !== "running")) return part
    const end = Date.now()
    const interrupted = {
      ...part,
      state: {
        status: "error" as const,
        input: part.state.input,
        error: "Tool execution aborted",
        metadata:
          part.state.status === "running" ? { ...part.state.metadata, interrupted: true } : { interrupted: true },
        time: {
          start: part.state.status === "running" ? part.state.time.start : end,
          end,
        },
      },
    } satisfies SessionV1.ToolPart
    toolParts.push(interrupted)
    return interrupted
  })

  return {
    info: {
      ...message.info,
      time: {
        ...message.info.time,
        completed: message.info.time.completed ?? now,
      },
      error: MessageV2.fromError(new DOMException("Aborted", "AbortError"), {
        providerID: message.info.providerID,
        aborted: true,
      }),
    },
    parts,
    toolParts,
  }
}

function projectRevokedSource(
  db: DB,
  core: DelegationStore.Interface,
  sessionID: SessionID,
  source: Delegation.SourceRecord,
) {
  return Effect.gen(function* () {
    if (source.generationID === undefined) return undefined
    const generation = yield* core.get(source.generationID)
    if (generation?.state !== "revoked" || generation.childID !== sessionID) return undefined

    const legacy = yield* db
      .select({ id: MessageTable.id })
      .from(MessageTable)
      .where(sql`${MessageTable.id} = ${source.source.id} AND ${MessageTable.session_id} = ${sessionID}`)
      .get()
    if (legacy === undefined) return undefined

    const message = yield* MessageV2.get({ sessionID, messageID: legacy.id }).pipe(
      Effect.provideService(Database.Service, { db }),
      Effect.catchIf(NotFoundError.isInstance, () => Effect.succeed(undefined)),
    )
    if (message === undefined) return undefined
    if (message.info.role !== "assistant") return undefined
    const snapshot = interruptedSnapshot(message)
    if (snapshot === undefined) return undefined
    const alreadyAborted =
      message.info.time.completed !== undefined && message.info.error?.name === "MessageAbortedError"

    const updated = yield* db.transaction(
      (tx) =>
        Effect.gen(function* () {
          const existing = yield* tx
            .select({ id: MessageTable.id })
            .from(MessageTable)
            .where(and(eq(MessageTable.id, message.info.id), eq(MessageTable.session_id, sessionID)))
            .get()
          if (existing === undefined) return false

          if (!alreadyAborted) {
            const { id: _, sessionID: __, ...data } = snapshot.info
            yield* tx
              .update(MessageTable)
              .set({ data })
              .where(and(eq(MessageTable.id, message.info.id), eq(MessageTable.session_id, sessionID)))
              .run()
          }

          for (const part of snapshot.toolParts) {
            const { id: _, messageID: __, sessionID: ___, ...data } = part
            yield* tx
              .update(PartTable)
              .set({ data })
              .where(
                and(
                  eq(PartTable.id, part.id),
                  eq(PartTable.message_id, message.info.id),
                  eq(PartTable.session_id, sessionID),
                ),
              )
              .run()
          }
          return true
        }),
      { behavior: "immediate" },
    )
    return updated ? message.info.id : undefined
  })
}
