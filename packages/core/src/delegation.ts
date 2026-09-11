export * as DelegationStore from "./delegation"

import { Context, Effect, Layer } from "effect"
import { Database } from "./database/database"
import { makeLifecycle } from "./delegation/lifecycle"
import { makeOutbox } from "./delegation/outbox"
import { makeSources } from "./delegation/source"
import { makeGlobalNode } from "./effect/app-node"

export function make(db: Database.Interface["db"]) {
  return { ...makeLifecycle(db), ...makeOutbox(db), ...makeSources(db) }
}

export type Interface = ReturnType<typeof make>

export class Service extends Context.Service<Service, Interface>()("@opencode/DelegationStore") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const database = yield* Database.Service
    return make(database.db)
  }),
)

export const node = makeGlobalNode({ service: Service, layer, deps: [Database.node] })
