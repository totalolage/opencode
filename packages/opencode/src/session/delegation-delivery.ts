export * as SessionDelegationDelivery from "./delegation-delivery"

import { and, eq } from "drizzle-orm"
import { Context, Effect, Layer } from "effect"
import type { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import type { SqlError } from "effect/unstable/sql/SqlError"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Delegation } from "@opencode-ai/schema/delegation"
import { MessageTable, SessionTable } from "@opencode-ai/core/session/sql"
import { GlobalBus } from "@/bus/global"
import { NotFoundError } from "@/storage/storage"
import { MessageV2 } from "./message-v2"
import { MessageID } from "./schema"
import type { SessionID } from "./schema"
import { SessionDelegation } from "./delegation"

type DB = Database.Interface["db"]
type SessionDelegationDeliveryError = Delegation.Error | Delegation.AdapterError | EffectDrizzleQueryError | SqlError

export interface Interface {
  readonly deliver: (parentID: SessionID) => Effect.Effect<readonly SessionID[], SessionDelegationDeliveryError>
  readonly announce: (sessionID: SessionID, messageID: MessageID) => Effect.Effect<void, SessionDelegationDeliveryError>
}

export function make(db: DB, core: DelegationStore.Interface, adapter: SessionDelegation.Interface): Interface {
  const deliver = Effect.fn("SessionDelegationDelivery.deliver")(function* (parentID: SessionID) {
    const resolutionIDs = (yield* core.pending(parentID)).map((resolution) => resolution.id)
    const parentIDs = yield* adapter.deliver(parentID)

    yield* Effect.forEach(resolutionIDs, (resolutionID) => notify(db, core, parentID, resolutionID), { discard: true })
    return parentIDs
  })

  return {
    deliver,
    announce: (sessionID, messageID) => announce(db, sessionID, messageID),
  }
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionDelegationDelivery") {}

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

function notify(db: DB, core: DelegationStore.Interface, parentID: SessionID, resolutionID: Delegation.ResolutionID) {
  return Effect.gen(function* () {
    const resolution = yield* core.getResolution(resolutionID)
    if (resolution === undefined || resolution.parentID !== parentID) return
    if (resolution.status !== "admitted" && resolution.status !== "consumed" && resolution.status !== "resolved") return
    if (resolution.envelope === undefined) return

    yield* announce(db, parentID, MessageID.make(resolution.messageID))
  })
}

function announce(db: DB, sessionID: SessionID, messageID: MessageID) {
  return Effect.gen(function* () {
    const recipient = yield* db
      .select({
        directory: SessionTable.directory,
        project_id: SessionTable.project_id,
        workspace_id: SessionTable.workspace_id,
      })
      .from(SessionTable)
      .where(eq(SessionTable.id, sessionID))
      .get()
    if (recipient === undefined) return

    const message = yield* db
      .select({ id: MessageTable.id })
      .from(MessageTable)
      .where(and(eq(MessageTable.id, messageID), eq(MessageTable.session_id, sessionID)))
      .get()
    if (message === undefined) return

    const snapshot = yield* MessageV2.get({ sessionID, messageID }).pipe(
      Effect.provideService(Database.Service, { db }),
      Effect.catchIf(NotFoundError.isInstance, () => Effect.succeed(undefined)),
    )
    if (snapshot === undefined) return

    yield* Effect.sync(() => {
      GlobalBus.emit("event", {
        directory: recipient.directory,
        project: recipient.project_id,
        workspace: recipient.workspace_id ?? undefined,
        payload: {
          type: "message.updated",
          properties: { sessionID, info: snapshot.info },
        },
      })
      for (const part of snapshot.parts) {
        GlobalBus.emit("event", {
          directory: recipient.directory,
          project: recipient.project_id,
          workspace: recipient.workspace_id ?? undefined,
          payload: {
            type: "message.part.updated",
            properties: { sessionID, part, time: Date.now() },
          },
        })
      }
    })
  })
}
