import { describe, expect } from "bun:test"
import { asc, eq } from "drizzle-orm"
import { Cause, Effect, Exit, Layer, Schema } from "effect"
import * as Scope from "effect/Scope"
import path from "node:path"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { DelegationResolutionTable, DelegationSourceTable, DelegationWorkTable } from "@opencode-ai/core/delegation/sql"
import { EventTable } from "@opencode-ai/core/event/sql"
import { Database } from "@opencode-ai/core/database/database"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { MessageTable, PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Delegation } from "@opencode-ai/schema/delegation"
import { Model } from "@opencode-ai/schema/model"
import { Provider } from "@opencode-ai/schema/provider"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { SessionID } from "@opencode-ai/schema/session-id"
import { GlobalBus, type GlobalEvent } from "../../src/bus/global"
import { SessionDelegationDelivery } from "../../src/session/delegation-delivery"
import { SessionDelegation } from "../../src/session/delegation"
import { SessionDelegationStop } from "../../src/session/delegation-stop"
import { SessionDelegationTurn } from "../../src/session/delegation-turn"
import { MessageID } from "../../src/session/schema"
import { MessageV2 } from "../../src/session/message-v2"
import { tmpdir } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(Layer.empty)
const scopedTmpdir = Effect.acquireRelease(
  Effect.promise(() => tmpdir()),
  (temporary) => Effect.promise(() => temporary[Symbol.asyncDispose]()),
)

type DB = Database.Interface["db"]
type Core = DelegationStore.Interface
type Adapter = SessionDelegation.Interface
type Harness = {
  readonly db: DB
  readonly core: Core
  readonly adapter: Adapter
  readonly turn: SessionDelegationTurn.Interface
  readonly stop: SessionDelegationStop.Interface
}
type ModelRef = SessionV1.User["model"]
type StoredModel = {
  readonly id: string
  readonly providerID: string
  readonly variant?: string
}
type SessionFixture = {
  readonly id: SessionID
  readonly parentID?: SessionID
  readonly agent?: string
  readonly model?: StoredModel | null
}

const runtimeProviderID = Provider.ID.make("delegation_runtime_provider")
const runtimeModelID = Model.ID.make("delegation_runtime_model")
const runtimeModel = {
  providerID: runtimeProviderID,
  modelID: runtimeModelID,
} satisfies ModelRef

const HistoryCutoff = Schema.Struct({
  messages: Schema.Array(SessionV1.MessageID),
  work: Schema.Array(Delegation.WorkID),
})

describe("delegation runtime boundaries", () => {
  it.live("records root and child input receipts, reserves owned work, and discards without settling input", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-runtime-input.sqlite")
      const root = sessionID("input-root")
      const child = sessionID("input-child")
      const grandchild = sessionID("input-grandchild")

      yield* openStore(
        filename,
        (harness) =>
          Effect.gen(function* () {
            const rootInput = yield* harness.turn.input(root, inputMessageID("input-root"))
            const rootInputMessage = inputMessage(root, "input-root", rootInput.part)
            yield* seedProjected(harness.db, rootInputMessage)
            expect(rootInput.part.metadata?.delegationInput.workID).toBe(rootInput.workID)
            expect(rootInput.part.messageID).toBe(rootInputMessage.info.id)

            const childGeneration = yield* createGeneration(
              harness.core,
              "input-child-generation",
              root,
              child,
              "background",
            )
            yield* seedProjected(harness.db, historyMessage(child, "input-child-history", 10))

            const grandchildGeneration = yield* createGeneration(
              harness.core,
              "input-grandchild-generation",
              child,
              grandchild,
              "background",
              childGeneration.generation.id,
            )
            const grandchildAssistant = assistantMessage("input-grandchild-assistant", grandchild)
            const grandchildSource = yield* harness.core.reserveSource({
              sessionID: grandchild,
              generationID: grandchildGeneration.generation.id,
              source: assistantSource("input-grandchild-assistant"),
              historyCutoff: "history-input-grandchild",
              consumed: [],
            })
            yield* harness.adapter.finalize(grandchildSource.source, grandchildAssistant, "reply")

            const grandchildReturn = yield* harness.core.pending(child)
            expect(grandchildReturn).toHaveLength(1)
            const returnResolution = grandchildReturn[0]
            if (returnResolution === undefined) return yield* Effect.die("expected the grandchild return")
            expect(yield* harness.adapter.deliver(child)).toEqual([child])
            const returnedMessage = yield* readMessage(harness.db, child, MessageID.make(returnResolution.messageID))

            const childInput = yield* harness.turn.input(child, inputMessageID("input-child"))
            const childInputMessage = inputMessage(child, "input-child", childInput.part)
            yield* seedProjected(harness.db, childInputMessage)
            const childAssistant = assistantMessage("input-child-assistant", child)
            const incoming = yield* harness.core.incoming(child)
            const reserved = yield* harness.turn.reserve(
              childAssistant.info,
              [childInputMessage, returnedMessage],
              incoming,
            )
            const cutoff = Schema.decodeUnknownSync(HistoryCutoff)(
              Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(reserved.historyCutoff),
            )

            expect(cutoff.messages).toEqual([childInputMessage.info.id, returnedMessage.info.id])
            expect(cutoff.work).toEqual([childInput.workID])
            expect(reserved.consumed).toEqual([returnResolution.id])
            expect((yield* harness.core.getResolution(returnResolution.id))?.status).toBe("consumed")

            const duplicate = yield* harness.turn
              .reserve(childAssistant.info, [childInputMessage, returnedMessage], incoming)
              .pipe(Effect.exit)
            expectFailureCode(duplicate, "source_replay")

            yield* harness.turn.discard(reserved)
            const discarded = (yield* harness.core.sources(child)).find((source) => source.id === reserved.id)
            expect(discarded).toMatchObject({
              id: reserved.id,
              source: { kind: "assistant", id: childAssistant.info.id },
              state: "discarded",
              workID: reserved.workID,
            })
            expect((yield* harness.core.getResolution(returnResolution.id))?.status).toBe("consumed")
            expect((yield* harness.core.getResolution(returnResolution.id))?.timeResolved).toBeUndefined()
            expect((yield* harness.core.getResolution(returnResolution.id))?.resolvedSourceID).toBeUndefined()

            const rootWork = (yield* harness.core.unfinished(root)).find((work) => work.id === rootInput.workID)
            expect(rootWork).toMatchObject({ id: rootInput.workID, sessionID: root, kind: "input", state: "active" })
            expect(rootWork?.generationID).toBeUndefined()

            const childWork = (yield* harness.core.unfinished(child)).find((work) => work.id === childInput.workID)
            expect(childWork).toMatchObject({
              id: childInput.workID,
              sessionID: child,
              generationID: childGeneration.generation.id,
              kind: "input",
              state: "active",
            })
            expect((yield* harness.core.unfinished(child)).find((work) => work.id === reserved.workID)).toBeUndefined()

            const sourceRow = yield* harness.db
              .select()
              .from(DelegationSourceTable)
              .where(eq(DelegationSourceTable.id, reserved.id))
              .get()
            expect(sourceRow).toMatchObject({
              id: reserved.id,
              work_id: reserved.workID,
              source_kind: "assistant",
              source_id: childAssistant.info.id,
              state: "discarded",
              consumed: [returnResolution.id],
            })
            const childInputRows = yield* harness.db
              .select()
              .from(DelegationWorkTable)
              .where(eq(DelegationWorkTable.id, childInput.workID))
              .all()
            expect(childInputRows).toHaveLength(1)
            expect(childInputRows[0]?.state).toBe("active")
          }),
        [
          { id: root, agent: "root-agent" },
          { id: child, parentID: root, agent: "child-agent" },
          { id: grandchild, parentID: child, agent: "grandchild-agent" },
        ],
      )
    }),
  )

  it.live("keeps reservations across physical reopen and reconciles only the captured prior input", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const reservedFilename = path.join(temporary.path, "delegation-runtime-reserved.sqlite")
      const reservedRoot = sessionID("reserved-root")
      const reservedChild = sessionID("reserved-child")

      const reserved = yield* openStore(
        reservedFilename,
        (harness) =>
          Effect.gen(function* () {
            const generation = yield* createGeneration(
              harness.core,
              "reserved-generation",
              reservedRoot,
              reservedChild,
              "background",
            )
            const assistant = assistantMessage("reserved-assistant", reservedChild)
            yield* seedProjected(harness.db, assistant)
            const source = yield* harness.turn.reserve(assistant.info, [], [])
            expect(yield* harness.adapter.blocked(reservedChild)).toBe(true)
            const sourceRows = yield* harness.db.select().from(DelegationSourceTable).all()
            const workRows = yield* harness.db.select().from(DelegationWorkTable).all()
            const resolutionRows = yield* harness.db.select().from(DelegationResolutionTable).all()
            return {
              generationID: generation.generation.id,
              source,
              projection: yield* projection(harness.db),
              sourceRows,
              workRows,
              resolutionRows,
            }
          }),
        [
          { id: reservedRoot, agent: "reserved-root-agent" },
          { id: reservedChild, parentID: reservedRoot, agent: "reserved-child-agent" },
        ],
      )

      yield* openStore(reservedFilename, (harness) =>
        Effect.gen(function* () {
          expect((yield* harness.core.get(reserved.generationID))?.state).toBe("active")
          expect(yield* harness.core.sources(reservedChild)).toEqual([reserved.source])
          expect(yield* harness.adapter.blocked(reservedChild)).toBe(true)
          expect(yield* projection(harness.db)).toEqual(reserved.projection)
          expect(yield* harness.db.select().from(DelegationSourceTable).all()).toEqual(reserved.sourceRows)
          expect(yield* harness.db.select().from(DelegationWorkTable).all()).toEqual(reserved.workRows)
          expect(yield* harness.db.select().from(DelegationResolutionTable).all()).toEqual(reserved.resolutionRows)

          yield* harness.turn.reconcile(reservedChild)
          expect(yield* harness.core.sources(reservedChild)).toEqual([reserved.source])
          expect(yield* harness.adapter.blocked(reservedChild)).toBe(true)
          expect(yield* projection(harness.db)).toEqual(reserved.projection)
          expect(yield* harness.db.select().from(DelegationSourceTable).all()).toEqual(reserved.sourceRows)
          expect(yield* harness.db.select().from(DelegationWorkTable).all()).toEqual(reserved.workRows)
          expect(yield* harness.db.select().from(DelegationResolutionTable).all()).toEqual(reserved.resolutionRows)
        }),
      )

      const settlementFilename = path.join(temporary.path, "delegation-runtime-settlement.sqlite")
      const settlementRoot = sessionID("settlement-root")
      const settlementChild = sessionID("settlement-child")
      const settlementOther = sessionID("settlement-other")

      const settlement = yield* openStore(
        settlementFilename,
        (harness) =>
          Effect.gen(function* () {
            const generation = yield* createGeneration(
              harness.core,
              "settlement-generation",
              settlementRoot,
              settlementChild,
              "background",
            )
            const boundInput = yield* harness.turn.input(settlementChild, inputMessageID("settlement-bound"))
            const boundMessage = inputMessage(settlementChild, "settlement-bound", boundInput.part)
            yield* seedProjected(harness.db, boundMessage)
            const assistant = assistantMessage("settlement-assistant", settlementChild)
            const source = yield* harness.turn.reserve(assistant.info, [boundMessage], [])

            const laterInput = yield* harness.turn.input(settlementChild, inputMessageID("settlement-later"))
            const otherGeneration = yield* createGeneration(
              harness.core,
              "settlement-other-generation",
              settlementRoot,
              settlementOther,
              "background",
            )
            const otherInput = yield* harness.turn.input(settlementOther, inputMessageID("settlement-other-input"))

            yield* harness.db
              .update(DelegationWorkTable)
              .set({ time_created: source.timeCreated + 1 })
              .where(eq(DelegationWorkTable.id, laterInput.workID))
              .run()

            // This is the crash cut: adapter.finalize commits the source and outbox, but turn.complete has not settled input work.
            yield* harness.adapter.finalize(source, assistant, "reply")
            const sourceAfterFinalize = (yield* harness.core.sources(settlementChild)).find(
              (item) => item.id === source.id,
            )
            if (sourceAfterFinalize === undefined) return yield* Effect.die("expected the finalized source")
            const outbox = yield* harness.core.pending(settlementRoot)
            expect(outbox).toHaveLength(1)
            const resolution = outbox[0]
            if (resolution === undefined) return yield* Effect.die("expected the captured outbox resolution")

            return {
              generationID: generation.generation.id,
              otherGenerationID: otherGeneration.generation.id,
              source: sourceAfterFinalize,
              resolution,
              boundWorkID: boundInput.workID,
              laterWorkID: laterInput.workID,
              otherWorkID: otherInput.workID,
              sourceRows: yield* harness.db.select().from(DelegationSourceTable).all(),
              resolutionRows: yield* harness.db.select().from(DelegationResolutionTable).all(),
            }
          }),
        [
          { id: settlementRoot, agent: "settlement-root-agent" },
          { id: settlementChild, parentID: settlementRoot, agent: "settlement-child-agent" },
          { id: settlementOther, parentID: settlementRoot, agent: "settlement-other-agent" },
        ],
      )

      yield* openStore(settlementFilename, (harness) =>
        Effect.gen(function* () {
          expect(
            (yield* harness.core.sources(settlementChild)).find((item) => item.id === settlement.source.id),
          ).toEqual(settlement.source)
          expect(
            (yield* harness.core.pending(settlementRoot)).find((item) => item.id === settlement.resolution.id),
          ).toEqual(settlement.resolution)
          expect(yield* harness.db.select().from(DelegationSourceTable).all()).toEqual(settlement.sourceRows)
          expect(yield* harness.db.select().from(DelegationResolutionTable).all()).toEqual(settlement.resolutionRows)

          const before = yield* harness.core.listWork(settlement.generationID)
          expect(before.find((work) => work.id === settlement.boundWorkID)?.state).toBe("active")
          expect(before.find((work) => work.id === settlement.laterWorkID)?.state).toBe("active")
          expect(
            (yield* harness.core.unfinished(settlementOther)).find((work) => work.id === settlement.otherWorkID),
          ).toMatchObject({
            id: settlement.otherWorkID,
            generationID: settlement.otherGenerationID,
            state: "active",
          })

          yield* harness.turn.reconcile(settlementChild)

          expect(
            (yield* harness.core.listWork(settlement.generationID)).find((work) => work.id === settlement.boundWorkID)
              ?.state,
          ).toBe("finished")
          expect(
            (yield* harness.core.listWork(settlement.generationID)).find((work) => work.id === settlement.laterWorkID)
              ?.state,
          ).toBe("active")
          expect(
            (yield* harness.core.unfinished(settlementOther)).find((work) => work.id === settlement.otherWorkID),
          ).toMatchObject({
            id: settlement.otherWorkID,
            generationID: settlement.otherGenerationID,
            state: "active",
          })
          expect(
            (yield* harness.core.sources(settlementChild)).find((item) => item.id === settlement.source.id),
          ).toEqual(settlement.source)
          expect(
            (yield* harness.core.pending(settlementRoot)).find((item) => item.id === settlement.resolution.id),
          ).toEqual(settlement.resolution)
          expect(yield* harness.db.select().from(DelegationSourceTable).all()).toEqual(settlement.sourceRows)
          expect(yield* harness.db.select().from(DelegationResolutionTable).all()).toEqual(settlement.resolutionRows)
        }),
      )
    }),
  )

  it.live("accepts a stale foreground admission after promotion and does not replay finished input work", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-runtime-promotion.sqlite")
      const root = sessionID("promotion-root")
      const child = sessionID("promotion-child")

      yield* openStore(
        filename,
        (harness) =>
          Effect.gen(function* () {
            const registered = yield* harness.core.register(
              registration("promotion-generation", root, child, "foreground"),
            )
            const staleAdmission = { generation: registered.generation, workID: registered.workID }
            const promoted = yield* harness.core.promote(registered.generation.id)
            expect(registered.generation.mode).toBe("foreground")
            expect(promoted.mode).toBe("background")
            expect(promoted.id).toBe(registered.generation.id)
            expect(promoted.parentID).toBe(registered.generation.parentID)
            expect(promoted.childID).toBe(registered.generation.childID)
            expect(promoted.parentGenerationID).toBe(registered.generation.parentGenerationID)
            expect(promoted.timeCreated).toBe(registered.generation.timeCreated)

            const admitted = yield* harness.turn.input(child, inputMessageID("promotion-input"), staleAdmission)
            yield* seedProjected(harness.db, inputMessage(child, "promotion-input", admitted.part))
            expect(admitted.workID).toBe(registered.workID)
            expect(admitted.part.metadata?.delegationInput.workID).toBe(registered.workID)
            expect(yield* harness.db.select().from(DelegationWorkTable).all()).toHaveLength(1)
            expect((yield* harness.core.listWork(registered.generation.id))[0]).toMatchObject({
              id: registered.workID,
              kind: "launch",
              state: "active",
              generationID: registered.generation.id,
              sessionID: child,
            })

            const beforeRetryProjection = yield* projection(harness.db)
            yield* harness.core.finishWork(registered.workID)
            const retry = yield* harness.turn
              .input(child, inputMessageID("promotion-input"), staleAdmission)
              .pipe(Effect.exit)
            expectFailureCode(retry, "work_finished")
            expect(yield* harness.db.select().from(DelegationWorkTable).all()).toHaveLength(1)
            expect(yield* harness.core.sources(child)).toEqual([])
            expect(yield* projection(harness.db)).toEqual(beforeRetryProjection)
          }),
        [
          { id: root, agent: "promotion-root-agent" },
          { id: child, parentID: root, agent: "promotion-child-agent" },
        ],
      )
    }),
  )

  it.live("finishes a revoked background assistant from its durable identity without a terminal fallback", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-runtime-stop.sqlite")
      const root = sessionID("stop-root")
      const child = sessionID("stop-child")
      const revokedRoot = sessionID("stop-revoked-root")
      const revokedChild = sessionID("stop-revoked-child")

      const recorded = yield* openStore(
        filename,
        (harness) =>
          Effect.gen(function* () {
            const generation = yield* createGeneration(harness.core, "stop-generation", root, child, "background")
            const assistant = assistantMessage("stop-assistant", child)
            yield* seedProjected(harness.db, assistant)
            const reservedResult = yield* harness.core.reserveSource({
              sessionID: child,
              generationID: generation.generation.id,
              source: assistantSource("stop-assistant"),
              historyCutoff: "history-stop-assistant",
              consumed: [],
            })
            const reserved = reservedResult.source
            const stop = yield* harness.core.revokeDescendants(child)
            expect((yield* harness.core.sources(child)).find((item) => item.id === reserved.id)).toMatchObject({
              id: reserved.id,
              state: "reserved",
              source: { kind: "assistant", id: assistant.info.id },
            })

            const revokedGeneration = yield* createGeneration(
              harness.core,
              "stop-parent-revoked-generation",
              revokedRoot,
              revokedChild,
              "background",
            )
            const revokedAssistant = assistantMessage("stop-parent-revoked-assistant", revokedChild)
            yield* seedProjected(harness.db, revokedAssistant)
            const revokedReservedResult = yield* harness.core.reserveSource({
              sessionID: revokedChild,
              generationID: revokedGeneration.generation.id,
              source: assistantSource("stop-parent-revoked-assistant"),
              historyCutoff: "history-stop-parent-revoked",
              consumed: [],
            })
            const revokedReserved = revokedReservedResult.source
            const revokedStop = yield* harness.core.revokeDescendants(revokedChild)

            return { reserved, stop, revokedReserved, revokedStop }
          }),
        [
          { id: root, agent: "stop-root-agent" },
          { id: child, parentID: root, agent: "stop-child-agent" },
          { id: revokedRoot, agent: "stop-revoked-root-agent" },
          { id: revokedChild, parentID: revokedRoot, agent: "stop-revoked-child-agent" },
        ],
      )

      yield* openStore(filename, (harness) =>
        Effect.gen(function* () {
          const parentIDs = yield* harness.stop.finish(child, recorded.stop, [recorded.reserved])
          expect(parentIDs).toEqual([root])

          const finalized = (yield* harness.core.sources(child)).find((item) => item.id === recorded.reserved.id)
          expect(finalized).toMatchObject({
            id: recorded.reserved.id,
            sessionID: child,
            generationID: recorded.reserved.generationID,
            source: { kind: "assistant", id: recorded.reserved.source.id },
            state: "finalized",
            outcome: "cancelled",
            workID: recorded.reserved.workID,
          })
          expect((yield* harness.core.sources(child)).some((item) => item.source.kind === "terminal")).toBe(false)

          const pending = yield* harness.core.pending(root)
          expect(pending).toHaveLength(1)
          const resolution = pending[0]
          if (resolution === undefined) return yield* Effect.die("expected the cancellation resolution")
          expect(resolution).toMatchObject({
            source: { kind: "assistant", id: recorded.reserved.source.id },
            childID: child,
            parentID: root,
            outcome: "cancelled",
            status: "pending",
          })

          const childMessages = yield* harness.db
            .select()
            .from(MessageTable)
            .where(eq(MessageTable.session_id, child))
            .all()
          expect(childMessages.map((message) => message.id)).toEqual([
            SessionV1.MessageID.make(recorded.reserved.source.id),
          ])
          expect(yield* harness.db.select().from(MessageTable).where(eq(MessageTable.session_id, root)).all()).toEqual(
            [],
          )

          const retriedParentIDs = yield* harness.stop.finish(child, recorded.stop, [recorded.reserved])
          expect(retriedParentIDs).toEqual([root])
          expect(yield* harness.core.pending(root)).toEqual(pending)
          expect(yield* harness.db.select().from(DelegationResolutionTable).all()).toHaveLength(1)

          const parentStop = yield* harness.core.revokeDescendants(revokedRoot)
          expect(yield* harness.stop.finish(revokedChild, recorded.revokedStop, [recorded.revokedReserved])).toEqual([])
          expect(yield* harness.stop.finish(revokedRoot, parentStop, [])).toEqual([])
          expect(
            (yield* harness.core.sources(revokedChild)).find((item) => item.id === recorded.revokedReserved.id),
          ).toMatchObject({
            id: recorded.revokedReserved.id,
            source: { kind: "assistant", id: recorded.revokedReserved.source.id },
            state: "reserved",
          })
          expect(yield* harness.core.pending(revokedRoot)).toEqual([])
          expect(
            yield* harness.db.select().from(MessageTable).where(eq(MessageTable.session_id, revokedRoot)).all(),
          ).toEqual([])
          expect(
            (yield* harness.db.select().from(DelegationSourceTable).all()).some(
              (source) => source.source_kind === "terminal",
            ),
          ).toBe(false)
        }),
      )
    }),
  )

  it.live("projects revoked assistant tools as canonical errors across disk reopen", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-runtime-revoked-tools.sqlite")
      const root = sessionID("revoked-tools-root")
      const child = sessionID("revoked-tools-child")

      const recorded = yield* openStore(
        filename,
        (harness) =>
          Effect.gen(function* () {
            const generation = yield* createGeneration(
              harness.core,
              "revoked-tools-generation",
              root,
              child,
              "background",
            )
            const assistant = assistantWithTools("revoked-tools-assistant", child)
            yield* seedProjected(harness.db, assistant)
            yield* harness.db
              .update(SessionTable)
              .set({
                cost: 12.5,
                tokens_input: 101,
                tokens_output: 202,
                tokens_reasoning: 303,
                tokens_cache_read: 404,
                tokens_cache_write: 505,
              })
              .where(eq(SessionTable.id, child))
              .run()
            const usage = yield* readUsage(harness.db, child)
            const reservedResult = yield* harness.core.reserveSource({
              sessionID: child,
              generationID: generation.generation.id,
              source: assistantSource("revoked-tools-assistant"),
              historyCutoff: "history-revoked-tools",
              consumed: [],
            })
            const reserved = reservedResult.source
            const stop = yield* harness.core.revokeDescendants(root)
            expect((yield* harness.core.get(generation.generation.id))?.state).toBe("revoked")
            expect(
              (yield* harness.core.listWork(generation.generation.id)).every((work) => work.state === "finished"),
            ).toBe(true)
            expect((yield* harness.core.sources(child)).find((source) => source.id === reserved.id)).toMatchObject({
              id: reserved.id,
              state: "reserved",
              workID: reserved.workID,
            })

            return {
              generationID: generation.generation.id,
              assistantID: MessageID.make(assistant.info.id),
              assistantMessageID: assistant.info.id,
              reserved,
              stop,
              usage,
              projection: yield* projection(harness.db),
            }
          }),
        [
          { id: root, agent: "revoked-tools-root-agent" },
          { id: child, parentID: root, agent: "revoked-tools-child-agent" },
        ],
      )

      const firstProjection = yield* openStore(filename, (harness) =>
        Effect.gen(function* () {
          expect((yield* harness.core.get(recorded.generationID))?.state).toBe("revoked")
          expect(yield* harness.stop.projectRevoked(child)).toEqual([recorded.assistantID])

          const message = yield* readMessage(harness.db, child, recorded.assistantID)
          if (message.info.role !== "assistant") return yield* Effect.die("expected the revoked assistant message")
          expect(message.info.id).toBe(recorded.assistantMessageID)
          expect(message.info.time.completed).toBeDefined()
          expect(message.info.error?.name).toBe("MessageAbortedError")

          const tools = message.parts.filter((part): part is SessionV1.ToolPart => part.type === "tool")
          expect(tools).toHaveLength(2)
          const running = tools.find((part) => part.callID === "call-revoked-tools-running")
          const pending = tools.find((part) => part.callID === "call-revoked-tools-pending")
          if (running === undefined || pending === undefined) {
            return yield* Effect.die("expected running and pending revoked tool parts")
          }
          if (running.state.status !== "error" || pending.state.status !== "error") {
            return yield* Effect.die("expected all revoked tools to be error states")
          }

          expect(Schema.decodeUnknownSync(SessionV1.ToolStateError)(running.state)).toEqual(running.state)
          expect(Schema.decodeUnknownSync(SessionV1.ToolStateError)(pending.state)).toEqual(pending.state)
          expect(running.state.input).toEqual({ command: "build", cursor: 17 })
          expect(running.state.error).toBe("Tool execution aborted")
          expect(running.state.metadata).toEqual({ progress: "half", output: "partial", interrupted: true })
          expect(running.state.time.start).toBe(123)
          expect(running.state.time.end).toBeGreaterThanOrEqual(running.state.time.start)
          expect(Object.hasOwn(running.state, "title")).toBe(false)
          expect(Object.hasOwn(running.state, "raw")).toBe(false)

          expect(pending.state.input).toEqual({ query: "pending", attempt: 2 })
          expect(pending.state.error).toBe("Tool execution aborted")
          expect(pending.state.metadata).toEqual({ interrupted: true })
          expect(pending.state.time.start).toBe(pending.state.time.end)
          expect(Object.hasOwn(pending.state, "title")).toBe(false)
          expect(Object.hasOwn(pending.state, "raw")).toBe(false)
          expect(
            tools.filter((part) => part.state.status === "running" || part.state.status === "pending"),
          ).toHaveLength(0)

          const raw = yield* readProjected(harness.db, recorded.assistantMessageID)
          expect(raw.message?.data.role).toBe("assistant")
          if (raw.message?.data.role !== "assistant") return yield* Effect.die("expected raw assistant projection")
          const completionTime = message.info.time.completed
          if (completionTime === undefined) return yield* Effect.die("expected a persisted completion timestamp")
          expect(yield* harness.core.pending(root)).toEqual([])
          expect(yield* harness.core.incoming(root)).toEqual([])
          expect(yield* harness.db.select().from(DelegationResolutionTable).all()).toEqual([])
          expect(yield* harness.core.sources(child)).toEqual([recorded.reserved])
          expect(yield* readUsage(harness.db, child)).toEqual(recorded.usage)

          return {
            projection: yield* projection(harness.db),
            assistantProjection: raw,
            completionTime,
          }
        }),
      )

      expect(firstProjection.projection).not.toEqual(recorded.projection)
      expect(firstProjection.completionTime).toBeGreaterThanOrEqual(0)

      yield* openStore(filename, (harness) =>
        Effect.gen(function* () {
          expect(yield* harness.stop.projectRevoked(child)).toEqual([recorded.assistantID])
          expect(yield* readProjected(harness.db, recorded.assistantMessageID)).toEqual(
            firstProjection.assistantProjection,
          )
          expect(yield* harness.core.sources(child)).toEqual([recorded.reserved])
          expect(yield* readUsage(harness.db, child)).toEqual(recorded.usage)
          expect(yield* harness.core.pending(root)).toEqual([])
          expect(yield* harness.core.incoming(root)).toEqual([])
          expect(yield* harness.db.select().from(DelegationResolutionTable).all()).toEqual([])
          expect((yield* harness.core.get(recorded.generationID))?.state).toBe("revoked")

          const message = yield* readMessage(harness.db, child, recorded.assistantID)
          if (message.info.role !== "assistant") return yield* Effect.die("expected the repeated assistant message")
          expect(message.info.time.completed).toBe(firstProjection.completionTime)
          expect(message.info.error?.name).toBe("MessageAbortedError")
        }),
      )
    }),
  )

  it.live("delivers committed receiver rows and provenance through the delivery service layer exactly once", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-runtime-delivery.sqlite")
      const root = sessionID("delivery-root")
      const child = sessionID("delivery-child")
      const directory = "/delegation-runtime"

      yield* openStore(
        filename,
        (harness) =>
          Effect.gen(function* () {
            yield* seedProjected(harness.db, historyMessage(root, "delivery-history", 10, runtimeModel))
            const generation = yield* createGeneration(harness.core, "delivery-generation", root, child, "background")
            const assistant = assistantMessage("delivery-assistant", child)
            const source = yield* harness.turn.reserve(assistant.info, [], [])
            yield* harness.turn.complete(source, assistant, "reply")

            const pendingBeforeDelivery = yield* harness.core.pending(root)
            expect(pendingBeforeDelivery).toHaveLength(1)
            const pendingResolution = pendingBeforeDelivery[0]
            if (pendingResolution === undefined) return yield* Effect.die("expected a pending delivery resolution")
            const eventRowsBefore = yield* harness.db.select().from(EventTable).all()
            const events: GlobalEvent[] = []
            const listener = (event: GlobalEvent) => {
              if (event.directory !== directory) return
              if (event.payload.type !== "message.updated" && event.payload.type !== "message.part.updated") return
              events.push(event)
            }
            GlobalBus.on("event", listener)
            yield* Effect.addFinalizer(() => Effect.sync(() => GlobalBus.off("event", listener)))

            const delivery = yield* deliveryFromLayer(harness)
            expect(yield* delivery.deliver(root)).toEqual([root])
            expect(events.some((event) => event.payload.type === "message.updated")).toBe(true)
            expect(events.some((event) => event.payload.type === "message.part.updated")).toBe(true)

            // GlobalBus is synchronous here; query only after the callback flag proves notify ran after receive committed.
            const receivedMessage = yield* readProjected(
              harness.db,
              SessionV1.MessageID.make(pendingResolution.messageID),
            )
            expect(receivedMessage.message?.session_id).toBe(root)
            expect(receivedMessage.parts).toHaveLength(1)
            const admitted = yield* harness.core.getResolution(pendingResolution.id)
            if (admitted?.envelope === undefined) return yield* Effect.die("expected the admitted envelope")
            expect(receivedMessage.parts[0]?.data).toMatchObject({
              type: "text",
              synthetic: true,
              metadata: { delegation: admitted.envelope.provenance },
            })

            const messageEvent = events.find((event) => event.payload.type === "message.updated")
            expect(messageEvent).toMatchObject({
              directory,
              project: Project.ID.global,
              payload: { type: "message.updated", properties: { sessionID: root } },
            })
            const partEvent = events.find((event) => event.payload.type === "message.part.updated")
            expect(partEvent).toMatchObject({
              directory,
              project: Project.ID.global,
              payload: {
                type: "message.part.updated",
                properties: {
                  sessionID: root,
                  part: {
                    sessionID: root,
                    messageID: SessionV1.MessageID.make(pendingResolution.messageID),
                    metadata: { delegation: admitted.envelope.provenance },
                  },
                },
              },
            })

            const projectionAfterDelivery = yield* projection(harness.db)
            const eventCountAfterDelivery = events.length
            expect((yield* harness.core.getResolution(pendingResolution.id))?.status).toBe("admitted")
            expect((yield* harness.db.select().from(EventTable).all()).length).toBe(eventRowsBefore.length)

            expect(yield* delivery.deliver(root)).toEqual([])
            expect(yield* projection(harness.db)).toEqual(projectionAfterDelivery)
            expect(events).toHaveLength(eventCountAfterDelivery)
            expect(yield* harness.db.select().from(EventTable).all()).toEqual(eventRowsBefore)
            expect(yield* harness.core.pending(root)).toEqual([])
            expect(
              yield* harness.db.select().from(MessageTable).where(eq(MessageTable.session_id, root)).all(),
            ).toHaveLength(2)
            expect((yield* harness.core.get(generation.generation.id))?.mode).toBe("background")
          }),
        [
          { id: root, agent: "delivery-root-agent" },
          { id: child, parentID: root, agent: "delivery-child-agent" },
        ],
      )
    }),
  )
})

function sessionID(name: string) {
  return SessionID.make(`ses_delegation_runtime_${name}`)
}

function messageID(name: string) {
  return SessionV1.MessageID.make(`msg_delegation_runtime_${name}`)
}

function inputMessageID(name: string) {
  return MessageID.make(`msg_delegation_runtime_${name}`)
}

function partID(name: string) {
  return SessionV1.PartID.make(`prt_delegation_runtime_${name}`)
}

function assistantSource(name: string): Delegation.Source {
  return { kind: "assistant", id: messageID(name) }
}

function model(providerID: string, modelID: string, variant?: string): ModelRef {
  return {
    providerID: Provider.ID.make(providerID),
    modelID: Model.ID.make(modelID),
    ...(variant === undefined ? {} : { variant }),
  }
}

function registration(
  name: string,
  parentID: SessionID,
  childID: SessionID,
  mode: Delegation.Mode,
  parentGenerationID?: Delegation.ID,
) {
  return {
    requestID: Delegation.RequestID.make(`drq_delegation_runtime_${name}`),
    generationID: Delegation.ID.make(`dlg_delegation_runtime_${name}`),
    parentID,
    childID,
    origin: {
      messageID: SessionMessage.ID.make(`msg_delegation_runtime_origin_${name}`),
      partID: Delegation.OriginPartID.make(`prt_delegation_runtime_origin_${name}`),
      callID: `call_delegation_runtime_${name}`,
    },
    mode,
    explicitReuse: false,
    ...(parentGenerationID === undefined ? {} : { parentGenerationID }),
  } satisfies Delegation.Registration
}

function createGeneration(
  core: Core,
  name: string,
  parentID: SessionID,
  childID: SessionID,
  mode: Delegation.Mode,
  parentGenerationID?: Delegation.ID,
) {
  return Effect.gen(function* () {
    const registered = yield* core.register(registration(name, parentID, childID, mode, parentGenerationID))
    yield* core.finishWork(registered.workID)
    return registered
  })
}

function userInfo(sessionID: SessionID, id: string, created: number, infoModel: ModelRef, agent = "build") {
  const info: SessionV1.User = {
    id: messageID(id),
    sessionID,
    role: "user",
    time: { created },
    agent,
    model: infoModel,
  }
  return info
}

function historyMessage(sessionID: SessionID, name: string, created: number, infoModel = runtimeModel) {
  const info = userInfo(sessionID, name, created, infoModel)
  const part: SessionV1.TextPart = {
    id: partID(`${name}-text`),
    sessionID,
    messageID: info.id,
    type: "text",
    text: `history-${name}`,
  }
  return {
    info,
    parts: [part],
  } satisfies SessionV1.WithParts
}

function inputMessage(sessionID: SessionID, name: string, receipt: SessionV1.TextPart) {
  const info = userInfo(sessionID, name, 20, runtimeModel)
  return { info, parts: [receipt] } satisfies SessionV1.WithParts
}

function assistantMessage(name: string, sessionID: SessionID) {
  const id = messageID(name)
  const info: SessionV1.Assistant = {
    id,
    sessionID,
    role: "assistant",
    time: { created: 100, completed: 200 },
    parentID: messageID(`${name}-parent`),
    modelID: runtimeModelID,
    providerID: runtimeProviderID,
    mode: "build",
    agent: "build",
    path: { cwd: "/delegation-runtime", root: "/delegation-runtime" },
    cost: 0,
    tokens: { total: 0, input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    finish: "stop",
  }
  const part: SessionV1.TextPart = {
    id: partID(`${name}-text`),
    sessionID,
    messageID: id,
    type: "text",
    text: `assistant-${name}`,
  }
  return {
    info,
    parts: [part],
  } satisfies SessionV1.WithParts
}

function assistantWithTools(name: string, sessionID: SessionID) {
  const base = assistantMessage(name, sessionID)
  const info: SessionV1.Assistant = {
    ...base.info,
    time: { created: base.info.time.created },
  }
  const running: SessionV1.ToolPart = {
    id: partID(`${name}-running`),
    sessionID,
    messageID: info.id,
    type: "tool",
    callID: "call-revoked-tools-running",
    tool: "bash",
    state: {
      status: "running",
      input: { command: "build", cursor: 17 },
      title: "running title",
      metadata: { progress: "half", output: "partial" },
      time: { start: 123 },
    },
  }
  const pending: SessionV1.ToolPart = {
    id: partID(`${name}-pending`),
    sessionID,
    messageID: info.id,
    type: "tool",
    callID: "call-revoked-tools-pending",
    tool: "grep",
    state: {
      status: "pending",
      input: { query: "pending", attempt: 2 },
      raw: "raw-pending-tool-call",
    },
  }
  return {
    info,
    parts: [...base.parts, running, pending],
  } satisfies SessionV1.WithParts
}

function messageData(info: SessionV1.Info) {
  const { id: _, sessionID: __, ...data } = info
  return data
}

function partData(part: SessionV1.Part) {
  const { id: _, sessionID: __, messageID: ___, ...data } = part
  return data
}

function seedProjected(db: DB, message: SessionV1.WithParts) {
  return Effect.gen(function* () {
    yield* db
      .insert(MessageTable)
      .values({
        id: message.info.id,
        session_id: message.info.sessionID,
        time_created: message.info.time.created,
        data: messageData(message.info),
      })
      .run()
    yield* db
      .insert(PartTable)
      .values(
        message.parts.map((part) => ({
          id: part.id,
          message_id: part.messageID,
          session_id: part.sessionID,
          time_created: message.info.time.created,
          data: partData(part),
        })),
      )
      .run()
  })
}

function projection(db: DB) {
  return Effect.gen(function* () {
    const messages = yield* db.select().from(MessageTable).orderBy(asc(MessageTable.id)).all()
    const parts = yield* db.select().from(PartTable).orderBy(asc(PartTable.id)).all()
    return { messages, parts }
  })
}

function readProjected(db: DB, messageID: SessionV1.MessageID) {
  return Effect.gen(function* () {
    const message = yield* db.select().from(MessageTable).where(eq(MessageTable.id, messageID)).get()
    const parts = yield* db
      .select()
      .from(PartTable)
      .where(eq(PartTable.message_id, messageID))
      .orderBy(asc(PartTable.id))
      .all()
    return { message, parts }
  })
}

function readUsage(db: DB, sessionID: SessionID) {
  return db
    .select({
      cost: SessionTable.cost,
      tokens_input: SessionTable.tokens_input,
      tokens_output: SessionTable.tokens_output,
      tokens_reasoning: SessionTable.tokens_reasoning,
      tokens_cache_read: SessionTable.tokens_cache_read,
      tokens_cache_write: SessionTable.tokens_cache_write,
    })
    .from(SessionTable)
    .where(eq(SessionTable.id, sessionID))
    .get()
}

function readMessage(db: DB, sessionID: SessionID, messageID: MessageID) {
  return MessageV2.get({ sessionID, messageID }).pipe(Effect.provideService(Database.Service, { db }))
}

function seedSessions(db: DB, sessions: readonly SessionFixture[]) {
  return Effect.gen(function* () {
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/delegation-runtime"), sandboxes: [] })
      .run()
    yield* db
      .insert(SessionTable)
      .values(
        sessions.map((session) => ({
          id: session.id,
          project_id: Project.ID.global,
          parent_id: session.parentID ?? null,
          slug: session.id,
          directory: AbsolutePath.make("/delegation-runtime"),
          title: session.id,
          version: "test",
          agent: session.agent ?? "build",
          model: session.model ?? null,
        })),
      )
      .run()
  })
}

function openStore<A, E>(
  filename: string,
  fn: (harness: Harness) => Effect.Effect<A, E, Scope.Scope>,
  sessions?: readonly SessionFixture[],
) {
  return Effect.gen(function* () {
    const database = yield* Database.Service
    const db = database.db
    if (sessions !== undefined) yield* seedSessions(db, sessions)
    const core = DelegationStore.make(db)
    const adapter = SessionDelegation.make(db, core)
    return yield* fn({
      db,
      core,
      adapter,
      turn: SessionDelegationTurn.make(core, adapter),
      stop: SessionDelegationStop.make(db, core, adapter),
    })
  }).pipe(Effect.scoped, Effect.provide(Database.layerFromPath(filename)))
}

function deliveryFromLayer(harness: Harness) {
  return SessionDelegationDelivery.Service.pipe(
    Effect.provide(
      SessionDelegationDelivery.layer.pipe(
        Layer.provide(Layer.succeed(Database.Service, { db: harness.db })),
        Layer.provide(Layer.succeed(DelegationStore.Service, harness.core)),
        Layer.provide(Layer.succeed(SessionDelegation.Service, harness.adapter)),
      ),
    ),
  )
}

function expectFailureCode<A, E>(exit: Exit.Exit<A, E>, code: string) {
  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toMatchObject({ code })
}
