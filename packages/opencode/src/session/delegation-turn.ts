export * as SessionDelegationTurn from "./delegation-turn"

import { Context, Effect, Layer, Option, Schema } from "effect"
import type { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import type { SqlError } from "effect/unstable/sql/SqlError"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Delegation } from "@opencode-ai/schema/delegation"
import type { MessageID, SessionID } from "./schema"
import { SessionDelegation } from "./delegation"

type SessionDelegationTurnError = Delegation.Error | Delegation.AdapterError | EffectDrizzleQueryError | SqlError
type Admission = {
  readonly generation: Delegation.Generation
  readonly workID: Delegation.WorkID
}

const InputReceipt = Schema.Struct({
  type: Schema.Literal("text"),
  text: Schema.Literal(""),
  synthetic: Schema.Literal(true),
  ignored: Schema.Literal(true),
  metadata: Schema.Struct({
    delegationInput: Schema.Struct({
      workID: Delegation.WorkID,
    }),
  }),
})
const decodeInputReceipt = Schema.decodeUnknownOption(InputReceipt)

const HistoryCutoff = Schema.Struct({
  messages: Schema.Array(Schema.String),
  work: Schema.Array(Delegation.WorkID),
})
const decodeJson = Schema.decodeUnknownOption(Schema.UnknownFromJsonString)
const decodeHistoryCutoff = Schema.decodeUnknownOption(HistoryCutoff)

export interface Interface {
  readonly input: (
    sessionID: SessionID,
    messageID: MessageID,
    admission?: Admission,
  ) => Effect.Effect<
    {
      readonly workID: Delegation.WorkID
      readonly part: SessionV1.TextPart
    },
    SessionDelegationTurnError
  >
  readonly remember: (
    sessionID: SessionID,
    operationID: string,
    messages: readonly SessionV1.WithParts[],
  ) => Effect.Effect<Delegation.SourceRecord, SessionDelegationTurnError>
  readonly reserve: (
    message: SessionV1.Assistant,
    messages: readonly SessionV1.WithParts[],
    incoming: readonly Delegation.Resolution[],
  ) => Effect.Effect<Delegation.SourceRecord, SessionDelegationTurnError>
  readonly complete: (
    source: Delegation.SourceRecord,
    message: SessionV1.WithParts,
    outcome: Delegation.Outcome,
  ) => Effect.Effect<void, SessionDelegationTurnError>
  readonly discard: (source: Delegation.SourceRecord) => Effect.Effect<void, SessionDelegationTurnError>
  readonly reconcile: (sessionID: SessionID) => Effect.Effect<void, SessionDelegationTurnError>
}

export function make(core: DelegationStore.Interface, adapter: SessionDelegation.Interface): Interface {
  const input = Effect.fn("SessionDelegationTurn.input")(function* (
    sessionID: SessionID,
    messageID: MessageID,
    admission?: Admission,
  ) {
    const work =
      admission === undefined
        ? yield* startInput(core, sessionID, messageID)
        : yield* admittedInput(core, sessionID, admission)
    if (work.state !== "active") {
      return yield* fail("work_finished", `Delegation input work is already finished: ${work.id}`)
    }
    return {
      workID: work.id,
      part: inputReceipt(sessionID, messageID, work.id),
    }
  })

  const remember = Effect.fn("SessionDelegationTurn.remember")(function* (
    sessionID: SessionID,
    operationID: string,
    messages: readonly SessionV1.WithParts[],
  ) {
    const boundary = yield* boundaryWork(core, sessionID, messages)
    const reserved = yield* core.reserveSource({
      sessionID,
      ...(boundary.generation === undefined ? {} : { generationID: boundary.generation.id }),
      source: { kind: "terminal", id: `delegation-compaction:${operationID}` },
      historyCutoff: JSON.stringify({
        messages: messages.map((item) => item.info.id),
        work: boundary.workIDs,
      }),
      consumed: [],
    })
    if (!reserved.created) {
      return yield* fail(
        "source_replay",
        `Delegation compaction source has already been reserved: ${reserved.source.id}`,
      )
    }
    return reserved.source
  })

  const reserve = Effect.fn("SessionDelegationTurn.reserve")(function* (
    message: SessionV1.Assistant,
    messages: readonly SessionV1.WithParts[],
    incoming: readonly Delegation.Resolution[],
  ) {
    const selectedIDs = new Set<string>(messages.map((item) => item.info.id))
    const consumed = incoming
      .filter(
        (resolution) =>
          (resolution.status === "admitted" || resolution.status === "consumed") &&
          selectedIDs.has(resolution.messageID),
      )
      .map((resolution) => resolution.id)
    const boundary = yield* boundaryWork(core, message.sessionID, messages)
    const historyCutoff = JSON.stringify({
      messages: messages.map((item) => item.info.id),
      work: boundary.workIDs,
    })
    const reserved = yield* core.reserveSource({
      sessionID: message.sessionID,
      ...(boundary.generation === undefined ? {} : { generationID: boundary.generation.id }),
      source: { kind: "assistant", id: message.id },
      historyCutoff,
      consumed,
    })
    if (!reserved.created) {
      return yield* fail(
        "source_replay",
        `Delegation assistant source has already been reserved: ${reserved.source.id}`,
      )
    }
    return reserved.source
  })

  const complete = Effect.fn("SessionDelegationTurn.complete")(function* (
    source: Delegation.SourceRecord,
    message: SessionV1.WithParts,
    outcome: Delegation.Outcome,
  ) {
    yield* adapter.finalize(source, message, outcome)
    yield* settle(core, source)
  })

  const discard = Effect.fn("SessionDelegationTurn.discard")(function* (source: Delegation.SourceRecord) {
    yield* core.discardSource(source.id)
  })

  const reconcile = Effect.fn("SessionDelegationTurn.reconcile")(function* (sessionID: SessionID) {
    const sources = yield* core.sources(sessionID)
    yield* Effect.forEach(
      sources.filter((source) => source.state === "finalized"),
      (source) => settle(core, source),
      { discard: true },
    )
  })

  return { input, remember, reserve, complete, discard, reconcile }
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionDelegationTurn") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const core = yield* DelegationStore.Service
    const adapter = yield* SessionDelegation.Service
    return make(core, adapter)
  }),
)

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [DelegationStore.node, SessionDelegation.node],
})

function startInput(core: DelegationStore.Interface, sessionID: SessionID, messageID: MessageID) {
  return Effect.gen(function* () {
    const generation = yield* core.active(sessionID)
    return yield* core.startWork({
      id: Delegation.WorkID.make(`dwk_input_${messageID}`),
      kind: "input",
      sessionID,
      ...(generation === undefined ? {} : { generationID: generation.id }),
    })
  })
}

function admittedInput(core: DelegationStore.Interface, sessionID: SessionID, admission: Admission) {
  return Effect.gen(function* () {
    const active = yield* core.active(sessionID)
    if (active === undefined || !sameGeneration(active, admission.generation)) {
      return yield* fail(
        "generation_mismatch",
        `Delegation generation ${admission.generation.id} is not the active generation for ${sessionID}`,
      )
    }
    const work = (yield* core.listWork(admission.generation.id)).find((item) => item.id === admission.workID)
    if (work === undefined || work.sessionID !== sessionID || work.generationID !== admission.generation.id) {
      return yield* fail(
        "work_mismatch",
        `Delegation input work ${admission.workID} does not belong to generation ${admission.generation.id}`,
      )
    }
    return work
  })
}

function inputReceipt(sessionID: SessionID, messageID: MessageID, workID: Delegation.WorkID): SessionV1.TextPart {
  return {
    id: SessionV1.PartID.make(`prt_input_${messageID}`),
    sessionID,
    messageID: SessionV1.MessageID.make(messageID),
    type: "text",
    text: "",
    synthetic: true,
    ignored: true,
    metadata: { delegationInput: { workID } },
  }
}

function inputReceiptWorkIDs(
  messages: readonly SessionV1.WithParts[],
  sessionID: SessionID,
  generationID: Delegation.ID | undefined,
  unfinishedByID: ReadonlyMap<Delegation.WorkID, Delegation.Work>,
) {
  const seen = new Set<Delegation.WorkID>()
  const workIDs: Delegation.WorkID[] = []
  for (const item of messages) {
    if (item.info.role !== "user" || item.info.sessionID !== sessionID) continue
    const expectedPartID = `prt_input_${item.info.id}`
    for (const part of item.parts) {
      if (
        part.id !== expectedPartID ||
        part.type !== "text" ||
        part.sessionID !== item.info.sessionID ||
        part.messageID !== item.info.id
      )
        continue
      const receipt = Option.getOrUndefined(decodeInputReceipt(part))
      if (receipt === undefined) continue
      const work = unfinishedByID.get(receipt.metadata.delegationInput.workID)
      if (
        work === undefined ||
        work.sessionID !== sessionID ||
        work.generationID !== generationID ||
        !isInputWorkKind(work.kind) ||
        seen.has(work.id)
      )
        continue
      seen.add(work.id)
      workIDs.push(work.id)
    }
  }
  return workIDs
}

function boundaryWork(core: DelegationStore.Interface, sessionID: SessionID, messages: readonly SessionV1.WithParts[]) {
  return Effect.gen(function* () {
    const generation = yield* core.active(sessionID)
    const unfinished = yield* core.unfinished(sessionID)
    const unfinishedByID = new Map(unfinished.map((work) => [work.id, work]))
    const workIDs = inputReceiptWorkIDs(messages, sessionID, generation?.id, unfinishedByID)
    const seen = new Set(workIDs)
    for (const source of yield* core.sources(sessionID)) {
      if (source.state !== "discarded" || source.generationID !== generation?.id) continue
      const cutoff = parseHistoryCutoff(source.historyCutoff)
      if (cutoff === undefined) continue
      for (const workID of cutoff.work) {
        const work = unfinishedByID.get(workID)
        if (
          work === undefined ||
          work.sessionID !== sessionID ||
          work.generationID !== source.generationID ||
          work.timeCreated > source.timeCreated ||
          !isInputWorkKind(work.kind) ||
          seen.has(work.id)
        )
          continue
        seen.add(work.id)
        workIDs.push(work.id)
      }
    }
    return { generation, workIDs }
  })
}

function settle(core: DelegationStore.Interface, source: Delegation.SourceRecord) {
  return Effect.gen(function* () {
    const cutoff = parseHistoryCutoff(source.historyCutoff)
    if (cutoff === undefined) return
    const unfinished = yield* core.unfinished(source.sessionID)
    const workIDs = new Set(cutoff.work)
    yield* Effect.forEach(
      unfinished.filter(
        (work) =>
          workIDs.has(work.id) &&
          work.sessionID === source.sessionID &&
          work.generationID === source.generationID &&
          work.timeCreated <= source.timeCreated &&
          isInputWorkKind(work.kind),
      ),
      (work) => core.finishWork(work.id),
      { discard: true },
    )
  })
}

function parseHistoryCutoff(input: string) {
  const json = Option.getOrUndefined(decodeJson(input))
  if (json === undefined) return undefined
  return Option.getOrUndefined(decodeHistoryCutoff(json))
}

function isInputWorkKind(kind: Delegation.WorkKind) {
  return kind === "input" || kind === "launch" || kind === "update"
}

function sameGeneration(left: Delegation.Generation, right: Delegation.Generation) {
  return (
    left.id === right.id &&
    left.parentID === right.parentID &&
    left.childID === right.childID &&
    left.origin.messageID === right.origin.messageID &&
    left.origin.partID === right.origin.partID &&
    left.origin.callID === right.origin.callID &&
    left.parentGenerationID === right.parentGenerationID &&
    left.timeCreated === right.timeCreated
  )
}

function fail<A = never>(code: string, message: string): Effect.Effect<A, Delegation.Error> {
  return Effect.fail(new Delegation.Error({ code, message }))
}
