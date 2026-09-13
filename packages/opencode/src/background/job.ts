import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import {
  make,
  Service,
  type ExtendInput,
  type Info,
  type Interface,
  type StartInput,
  type Status,
  type WaitInput,
  type WaitResult,
} from "@opencode-ai/core/background-job"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { Delegation } from "@opencode-ai/schema/delegation"
import { InstanceState } from "@/effect/instance-state"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Effect, Layer, Schema } from "effect"

export {
  Service,
  type ExtendInput,
  type Info,
  type Interface,
  type StartInput,
  type Status,
  type WaitInput,
  type WaitResult,
}

/** Keeps the legacy service instance-scoped while sharing the core registry engine. */
const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const state = yield* InstanceState.make(() => make)
    const flags = yield* RuntimeFlags.Service
    const delegation = yield* DelegationStore.Service
    const promote: Interface["promote"] = Effect.fn("BackgroundJob.promote")(function* (id) {
      const job = yield* InstanceState.useEffect(state, (jobs) => jobs.get(id))
      if (job === undefined || job.status !== "running" || job.type !== "task") {
        return yield* InstanceState.useEffect(state, (jobs) => jobs.promote(id))
      }
      if (!flags.experimentalBackgroundSubagents) {
        return yield* Effect.die(
          new Error("Background subagents require OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS=true"),
        )
      }

      const delegationID = job.metadata?.delegationID
      if (typeof delegationID !== "string") {
        return yield* Effect.die(new Error(`Task job ${id} requires a valid delegationID`))
      }
      const generationID = yield* Schema.decodeUnknownEffect(Delegation.ID)(delegationID).pipe(
        Effect.mapError(() => new Error(`Task job ${id} requires a valid delegationID`)),
        Effect.orDie,
      )
      yield* delegation.promote(generationID).pipe(Effect.orDie)
      return yield* InstanceState.useEffect(state, (jobs) => jobs.promote(id))
    })

    return Service.of({
      list: () => InstanceState.useEffect(state, (jobs) => jobs.list()),
      get: (id) => InstanceState.useEffect(state, (jobs) => jobs.get(id)),
      start: (input) => InstanceState.useEffect(state, (jobs) => jobs.start(input)),
      extend: (input) => InstanceState.useEffect(state, (jobs) => jobs.extend(input)),
      wait: (input) => InstanceState.useEffect(state, (jobs) => jobs.wait(input)),
      waitForPromotion: (id) => InstanceState.useEffect(state, (jobs) => jobs.waitForPromotion(id)),
      promote,
      cancel: (id) => InstanceState.useEffect(state, (jobs) => jobs.cancel(id)),
    })
  }),
)

export const node = LayerNode.make({ service: Service, layer, deps: [RuntimeFlags.node, DelegationStore.node] })

export * as BackgroundJob from "./job"
