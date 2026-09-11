import { describe, expect, test } from "bun:test"
import { and, asc, eq, sql } from "drizzle-orm"
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { Cause, Deferred, Effect, Exit, Fiber, ManagedRuntime } from "effect"
import { isDeepStrictEqual } from "node:util"
import path from "node:path"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { DelegationGenerationTable } from "@opencode-ai/core/delegation/sql"
import { Database } from "@opencode-ai/core/database/database"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { SessionID } from "@opencode-ai/schema/session-id"
import { tmpdir } from "./fixture/tmpdir"

const SourceEvidenceTable = sqliteTable("delegation_outbox_test_source", {
  id: text().primaryKey(),
  session_id: text().$type<SessionID>().notNull(),
  generation_id: text().$type<Delegation.ID>(),
  source_kind: text().$type<Delegation.Source["kind"]>().notNull(),
  source_id: text().notNull(),
  payload: text().notNull(),
  outcome: text().$type<Delegation.Outcome>().notNull(),
  history_cutoff: text().notNull(),
  consumed: text({ mode: "json" }).$type<Delegation.ResolutionID[]>().notNull(),
  logical_finalized: integer({ mode: "boolean" }).notNull(),
})

const ReceiverMessageTable = sqliteTable("delegation_outbox_test_message", {
  id: text().$type<SessionMessage.ID>().primaryKey(),
  session_id: text().$type<SessionID>().notNull(),
  data: text({ mode: "json" }).$type<unknown>().notNull(),
  provenance: text({ mode: "json" }).$type<unknown>().notNull(),
})

const ReceiverPartTable = sqliteTable("delegation_outbox_test_part", {
  id: text().$type<Delegation.OriginPartID>().primaryKey(),
  message_id: text().$type<SessionMessage.ID>().notNull(),
  position: integer().notNull(),
  data: text({ mode: "json" }).$type<unknown>().notNull(),
})

type DB = Database.Interface["db"]
type Transaction = Parameters<Parameters<DB["transaction"]>[0]>[0]
type Store = DelegationStore.Interface

const rootID = SessionID.make("ses_delegation_outbox_root")
const otherRootID = SessionID.make("ses_delegation_outbox_other_root")
const childID = SessionID.make("ses_delegation_outbox_child")
const siblingID = SessionID.make("ses_delegation_outbox_sibling")
const intermediateID = SessionID.make("ses_delegation_outbox_intermediate")
const grandchildID = SessionID.make("ses_delegation_outbox_grandchild")
const otherChildID = SessionID.make("ses_delegation_outbox_other_child")
const raceChildID = SessionID.make("ses_delegation_outbox_race_child")
const raceGrandchildID = SessionID.make("ses_delegation_outbox_race_grandchild")

describe("DelegationStore outbox", () => {
  test("keeps capture identity and prepared envelopes stable across disk reopen", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    const recorded = await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          expect(yield* db.get(sql`SELECT parent_id FROM session WHERE id = ${childID}`)).toEqual({ parent_id: rootID })

          const registered = yield* store.register(
            registration("disk-capture", rootID, childID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)

          const sourceInput = {
            sessionID: childID,
            generationID: registered.generation.id,
            source: assistantSource("disk-capture"),
            historyCutoff: "history-disk-capture",
            consumed: [],
          }
          const reserved = yield* store.reserveSource(sourceInput)
          expect(reserved.created).toBe(true)
          const finalized = yield* store.finalizeSource(reserved.source.id, {
            payload: "disk reply",
            outcome: "reply",
          })
          if (finalized.resolution === undefined) return yield* Effect.die("expected a background resolution")

          const prepared = yield* store.prepare(finalized.resolution.id, content("disk-capture"))
          expect(prepared.envelope?.message.id).toBe(prepared.messageID)
          expect(yield* rawResolution(db, prepared.id)).toMatchObject({ status: "pending", has_envelope: 1 })
          expect(yield* countRows(db, "delegation_resolution")).toBe(1)

          return {
            generationID: registered.generation.id,
            sourceInput,
            sourceID: reserved.source.id,
            resolutionID: prepared.id,
            messageID: prepared.messageID,
            envelope: prepared.envelope,
          }
        }),
      true,
    )

    await withStore(filename, (db, store) =>
      Effect.gen(function* () {
        const retriedReservation = yield* store.reserveSource(recorded.sourceInput)
        expect(retriedReservation).toMatchObject({ created: false, source: { id: recorded.sourceID, state: "finalized" } })

        const retriedFinalization = yield* store.finalizeSource(recorded.sourceID, {
          payload: "disk reply",
          outcome: "reply",
        })
        expect(retriedFinalization.resolution?.id).toBe(recorded.resolutionID)
        expect(retriedFinalization.resolution?.messageID).toBe(recorded.messageID)

        const preparedRetry = yield* store.prepare(recorded.resolutionID, {
          message: { text: "delegation disk-capture", kind: "synthetic" },
          parts: [
            { text: "delegation disk-capture", kind: "text" },
            { name: "disk-capture", kind: "metadata" },
          ],
        })
        expect(preparedRetry.envelope).toEqual(recorded.envelope)
        expect(yield* countRows(db, "delegation_resolution")).toBe(1)
      }),
      false,
    )
  })

  test("reserves source work before provider execution and publishes only after a logical marker", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    const recorded = await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("reconcile", rootID, childID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)

          const assistant = assistantSource("reconcile")
          const reserved = yield* store.reserveSource({
            sessionID: childID,
            generationID: registered.generation.id,
            source: assistant,
            historyCutoff: "history-reconcile",
            consumed: [],
          })
          expect(reserved.source.state).toBe("reserved")
          expect(yield* store.unfinishedSources(childID)).toEqual([reserved.source])
          expect(yield* store.unfinished(childID)).toEqual([
            expect.objectContaining({
              id: reserved.source.workID,
              sessionID: childID,
              generationID: registered.generation.id,
              kind: "provider",
              state: "active",
            }),
          ])

          const tool = yield* store.reserveSource({
            sessionID: childID,
            generationID: registered.generation.id,
            source: terminalSource("unfinished-tool"),
            historyCutoff: "history-unfinished-tool",
            consumed: [],
          })
          expect((yield* store.unfinished(childID)).map((work) => work.kind)).toEqual(["provider", "runtime"])

          yield* addEvidence(db, {
            id: "evidence-reconcile",
            sessionID: childID,
            generationID: registered.generation.id,
            source: assistant,
            payload: "reconciled reply",
            outcome: "reply",
            historyCutoff: "history-reconcile",
            logicalFinalized: false,
          })

          const beforeMarker = yield* store.reconcileAndClose(
            registered.generation.id,
            reconcileEvidence,
          )
          expect(beforeMarker.resolutions).toEqual([])
          expect(beforeMarker.closed).toBe(false)
          expect(beforeMarker.blockers).toContain(`source:${reserved.source.id}`)
          expect(yield* store.pending(rootID)).toEqual([])
          expect(yield* countRows(db, "delegation_resolution")).toBe(0)

          return {
            generationID: registered.generation.id,
            sourceID: reserved.source.id,
            source: assistant,
            toolID: tool.source.id,
          }
        }),
      true,
    )

    await withStore(filename, (db, store) =>
      Effect.gen(function* () {
        const unfinishedSources = yield* store.unfinishedSources(childID)
        expect(unfinishedSources).toHaveLength(2)
        expect(unfinishedSources.map((source) => source.id)).toContain(recorded.sourceID)
        const unfinished = yield* store.unfinished(childID)
        expect(unfinished.map((work) => work.kind)).toEqual(["provider", "runtime"])
        expect(yield* countRows(db, "delegation_resolution")).toBe(0)

        yield* db
          .update(SourceEvidenceTable)
          .set({ logical_finalized: true })
          .where(eq(SourceEvidenceTable.id, "evidence-reconcile"))
          .run()

        const reconciled = yield* store.reconcileAndClose(recorded.generationID, reconcileEvidence)
        expect(reconciled.resolutions).toHaveLength(1)
        expect(reconciled.resolutions[0]?.status).toBe("pending")
        expect(reconciled.closed).toBe(false)
        expect(yield* store.sources(childID)).toEqual(
          expect.arrayContaining([expect.objectContaining({ id: recorded.sourceID, state: "finalized" })]),
        )
        expect(yield* countRows(db, "delegation_resolution")).toBe(1)

        const exactReconciliation = yield* store.reconcileAndClose(recorded.generationID, reconcileEvidence)
        expect(exactReconciliation.resolutions[0]?.id).toBe(reconciled.resolutions[0]?.id)
        expect(yield* countRows(db, "delegation_resolution")).toBe(1)
        expect(yield* store.pending(rootID)).toHaveLength(1)
      }),
      false,
    )
  })

  test("does not adopt completed-like evidence without a registered source reservation", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("missing-reservation", rootID, childID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)
          yield* addEvidence(db, {
            id: "unregistered-finalized-evidence",
            sessionID: childID,
            generationID: registered.generation.id,
            source: assistantSource("unregistered"),
            payload: "historical reply",
            outcome: "reply",
            historyCutoff: "history-unregistered",
            logicalFinalized: true,
          })

          const failed = yield* store.reconcileAndClose(registered.generation.id, reconcileEvidence).pipe(Effect.exit)
          expectFailure(failed)
          expect(yield* store.get(registered.generation.id)).toMatchObject({ state: "active" })
          expect(yield* store.pending(rootID)).toEqual([])
          expect(yield* countRows(db, "delegation_resolution")).toBe(0)

          yield* db
            .delete(SourceEvidenceTable)
            .where(eq(SourceEvidenceTable.id, "unregistered-finalized-evidence"))
            .run()
          yield* addEvidence(db, {
            id: "completed-like-only",
            sessionID: childID,
            generationID: registered.generation.id,
            source: assistantSource("completed-like-only"),
            payload: "completion update only",
            outcome: "reply",
            historyCutoff: "history-completed-like-only",
            logicalFinalized: false,
          })

          const noLogicalFinalization = yield* store.reconcileAndClose(registered.generation.id, reconcileEvidence)
          expect(noLogicalFinalization.resolutions).toEqual([])
          expect(noLogicalFinalization.closed).toBe(true)
          expect(yield* countRows(db, "delegation_resolution")).toBe(0)
        }),
      true,
    )
  })

  test("reconciles a foreground logical source without creating an async resolution", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(registration("foreground-reconcile", rootID, childID))
          yield* store.finishWork(registered.workID)

          const source = assistantSource("foreground-reconcile")
          const reserved = yield* store.reserveSource({
            sessionID: childID,
            generationID: registered.generation.id,
            source,
            historyCutoff: "history-foreground-reconcile",
            consumed: [],
          })
          yield* addEvidence(db, {
            id: "foreground-reconcile-evidence",
            sessionID: childID,
            generationID: registered.generation.id,
            source,
            payload: "foreground reply",
            outcome: "reply",
            historyCutoff: "history-foreground-reconcile",
            logicalFinalized: true,
          })

          const reconciled = yield* store.reconcileAndClose(registered.generation.id, reconcileEvidence)
          expect(reconciled.closed).toBe(true)
          expect(reconciled.resolutions).toEqual([])
          expect(reconciled.generation.state).toBe("closed")
          expect(yield* store.sources(childID)).toEqual([
            expect.objectContaining({
              id: reserved.source.id,
              state: "finalized",
              payload: "foreground reply",
              outcome: "reply",
            }),
          ])
          expect(yield* store.pending(rootID)).toEqual([])
          expect(yield* countRows(db, "delegation_resolution")).toBe(0)
          expect(yield* receiverCounts(db)).toEqual({ messages: 0, parts: 0 })
        }),
      true,
    )
  })

  test("reconciles an original assistant error into one pending background return", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("reconcile-assistant-error", rootID, childID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)
          const sourceInput = {
            sessionID: childID,
            generationID: registered.generation.id,
            source: assistantSource("reconcile-assistant-error"),
            historyCutoff: "history-reconcile-assistant-error",
            consumed: [],
          }
          const reserved = yield* store.reserveSource(sourceInput)
          yield* addEvidence(db, {
            id: "reconcile-assistant-error-evidence",
            sessionID: childID,
            generationID: registered.generation.id,
            source: sourceInput.source,
            payload: "assistant error",
            outcome: "error",
            historyCutoff: sourceInput.historyCutoff,
            logicalFinalized: true,
          })

          const reconciled = yield* store.reconcileAndClose(registered.generation.id, reconcileEvidence)
          expect(reconciled.closed).toBe(false)
          expect(reconciled.resolutions).toHaveLength(1)
          const resolution = reconciled.resolutions[0]
          if (resolution === undefined) return yield* Effect.die("expected assistant error resolution")
          expect(reconciled.blockers).toContain(`outgoing:${resolution.id}`)
          expect(resolution.source).toEqual(sourceInput.source)
          expect(resolution.outcome).toBe("error")
          expect(yield* db.get(sql`
            SELECT logical_finalized
            FROM delegation_outbox_test_source
            WHERE id = ${"reconcile-assistant-error-evidence"}
          `)).toEqual({ logical_finalized: 1 })
          expect(yield* store.sources(childID)).toEqual([
            expect.objectContaining({
              id: reserved.source.id,
              source: sourceInput.source,
              historyCutoff: sourceInput.historyCutoff,
              consumed: sourceInput.consumed,
              state: "finalized",
              payload: "assistant error",
              outcome: "error",
            }),
          ])
          expect(yield* countRows(db, "delegation_resolution")).toBe(1)
          expect(yield* store.pending(rootID)).toEqual([expect.objectContaining({ id: resolution.id, outcome: "error" })])

          const repeated = yield* store.reconcileAndClose(registered.generation.id, reconcileEvidence)
          expect(repeated.closed).toBe(false)
          expect(repeated.resolutions.map((item) => item.id)).toEqual([resolution.id])
          expect(yield* countRows(db, "delegation_resolution")).toBe(1)

          const conflict = yield* store
            .reconcileAndClose(registered.generation.id, () =>
              Effect.succeed({
                quiescent: true,
                sources: [
                  capture(
                    sourceInput.source,
                    "changed assistant error",
                    sourceInput.historyCutoff,
                    sourceInput.consumed,
                    "error",
                  ),
                ],
              }),
            )
            .pipe(Effect.exit)
          expectFailure(conflict)
          expect(yield* countRows(db, "delegation_resolution")).toBe(1)
          expect((yield* store.getResolution(resolution.id))?.payload).toBe("assistant error")

          const prepared = yield* store.prepare(resolution.id, content("reconcile-assistant-error"))
          expect(yield* store.admit(prepared.id, persistEnvelope)).toEqual({ status: "admitted" })
          expect(yield* receiverCounts(db)).toEqual({ messages: 1, parts: 2 })
          expect((yield* close(store, registered.generation.id)).closed).toBe(true)
        }),
      true,
    )
  })

  test("reconciles an original assistant cancellation after child stop into one pending background return", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("reconcile-assistant-cancelled", rootID, childID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)
          const sourceInput = {
            sessionID: childID,
            generationID: registered.generation.id,
            source: assistantSource("reconcile-assistant-cancelled"),
            historyCutoff: "history-reconcile-assistant-cancelled",
            consumed: [],
          }
          const reserved = yield* store.reserveSource(sourceInput)
          yield* store.revokeDescendants(childID)
          yield* addEvidence(db, {
            id: "reconcile-assistant-cancelled-evidence",
            sessionID: childID,
            generationID: registered.generation.id,
            source: sourceInput.source,
            payload: "assistant cancelled",
            outcome: "cancelled",
            historyCutoff: sourceInput.historyCutoff,
            logicalFinalized: true,
          })

          const reconciled = yield* store.reconcileAndClose(registered.generation.id, reconcileEvidence)
          expect(reconciled.closed).toBe(false)
          expect(reconciled.resolutions).toHaveLength(1)
          const resolution = reconciled.resolutions[0]
          if (resolution === undefined) return yield* Effect.die("expected assistant cancellation resolution")
          expect(reconciled.blockers).toContain(`outgoing:${resolution.id}`)
          expect(resolution.source).toEqual(sourceInput.source)
          expect(resolution.outcome).toBe("cancelled")
          expect(yield* db.get(sql`
            SELECT logical_finalized
            FROM delegation_outbox_test_source
            WHERE id = ${"reconcile-assistant-cancelled-evidence"}
          `)).toEqual({ logical_finalized: 1 })
          expect(yield* store.sources(childID)).toEqual([
            expect.objectContaining({
              id: reserved.source.id,
              source: sourceInput.source,
              historyCutoff: sourceInput.historyCutoff,
              consumed: sourceInput.consumed,
              state: "finalized",
              payload: "assistant cancelled",
              outcome: "cancelled",
            }),
          ])
          expect(yield* countRows(db, "delegation_resolution")).toBe(1)
          expect(yield* store.pending(rootID)).toEqual([
            expect.objectContaining({ id: resolution.id, outcome: "cancelled" }),
          ])

          const repeated = yield* store.reconcileAndClose(registered.generation.id, reconcileEvidence)
          expect(repeated.closed).toBe(false)
          expect(repeated.resolutions.map((item) => item.id)).toEqual([resolution.id])
          expect(yield* countRows(db, "delegation_resolution")).toBe(1)

          const conflict = yield* store
            .reconcileAndClose(registered.generation.id, () =>
              Effect.succeed({
                quiescent: true,
                sources: [
                  capture(
                    sourceInput.source,
                    "changed assistant cancellation",
                    sourceInput.historyCutoff,
                    sourceInput.consumed,
                    "cancelled",
                  ),
                ],
              }),
            )
            .pipe(Effect.exit)
          expectFailure(conflict)
          expect(yield* countRows(db, "delegation_resolution")).toBe(1)
          expect((yield* store.getResolution(resolution.id))?.payload).toBe("assistant cancelled")

          const prepared = yield* store.prepare(resolution.id, content("reconcile-assistant-cancelled"))
          expect(yield* store.admit(prepared.id, persistEnvelope)).toEqual({ status: "admitted" })
          expect(yield* receiverCounts(db)).toEqual({ messages: 1, parts: 2 })
          expect((yield* close(store, registered.generation.id)).closed).toBe(true)
        }),
      true,
    )
  })

  test("keeps foreground assistant error and cancellation local without async control", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const errorGeneration = yield* store.register(
            registration("foreground-assistant-error", rootID, childID),
          )
          yield* store.finishWork(errorGeneration.workID)
          const errorSource = assistantSource("foreground-assistant-error")
          const errorReservation = yield* store.reserveSource({
            sessionID: childID,
            generationID: errorGeneration.generation.id,
            source: errorSource,
            historyCutoff: "history-foreground-assistant-error",
            consumed: [],
          })
          yield* addEvidence(db, {
            id: "foreground-assistant-error-evidence",
            sessionID: childID,
            generationID: errorGeneration.generation.id,
            source: errorSource,
            payload: "foreground assistant error",
            outcome: "error",
            historyCutoff: "history-foreground-assistant-error",
            logicalFinalized: true,
          })

          const cancelledGeneration = yield* store.register(
            registration("foreground-assistant-cancelled", rootID, siblingID),
          )
          yield* store.finishWork(cancelledGeneration.workID)
          const cancelledSource = assistantSource("foreground-assistant-cancelled")
          const cancelledReservation = yield* store.reserveSource({
            sessionID: siblingID,
            generationID: cancelledGeneration.generation.id,
            source: cancelledSource,
            historyCutoff: "history-foreground-assistant-cancelled",
            consumed: [],
          })
          yield* store.revokeDescendants(siblingID)
          yield* addEvidence(db, {
            id: "foreground-assistant-cancelled-evidence",
            sessionID: siblingID,
            generationID: cancelledGeneration.generation.id,
            source: cancelledSource,
            payload: "foreground assistant cancelled",
            outcome: "cancelled",
            historyCutoff: "history-foreground-assistant-cancelled",
            logicalFinalized: true,
          })

          const errorReconciled = yield* store.reconcileAndClose(
            errorGeneration.generation.id,
            reconcileEvidence,
          )
          const cancelledReconciled = yield* store.reconcileAndClose(
            cancelledGeneration.generation.id,
            reconcileEvidence,
          )
          expect(errorReconciled.closed).toBe(true)
          expect(errorReconciled.resolutions).toEqual([])
          expect(cancelledReconciled.closed).toBe(true)
          expect(cancelledReconciled.resolutions).toEqual([])
          expect(yield* store.sources(childID)).toEqual([
            expect.objectContaining({
              id: errorReservation.source.id,
              source: errorSource,
              state: "finalized",
              payload: "foreground assistant error",
              outcome: "error",
            }),
          ])
          expect(yield* store.sources(siblingID)).toEqual([
            expect.objectContaining({
              id: cancelledReservation.source.id,
              source: cancelledSource,
              state: "finalized",
              payload: "foreground assistant cancelled",
              outcome: "cancelled",
            }),
          ])
          expect(yield* countRows(db, "delegation_resolution")).toBe(0)
          expect(yield* receiverCounts(db)).toEqual({ messages: 0, parts: 0 })
          expect(yield* store.pending(rootID)).toEqual([])
        }),
      true,
    )
  })

  test("requires preparation, freezes multipart content, and keeps admission separate from consumption", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("envelope", rootID, childID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)
          const source = assistantSource("envelope")
          const reserved = yield* store.reserveSource({
            sessionID: childID,
            generationID: registered.generation.id,
            source,
            historyCutoff: "history-envelope",
            consumed: [],
          })
          const finalized = yield* store.finalizeSource(reserved.source.id, {
            payload: "envelope reply",
            outcome: "reply",
          })
          if (finalized.resolution === undefined) return yield* Effect.die("expected an envelope resolution")

          const notPrepared = yield* store.admit(finalized.resolution.id, persistEnvelope).pipe(Effect.exit)
          expectFailure(notPrepared)
          expect(yield* receiverCounts(db)).toEqual({ messages: 0, parts: 0 })
          expect(yield* rawResolution(db, finalized.resolution.id)).toMatchObject({ status: "pending", has_envelope: 0 })
          expect(yield* store.pending(rootID)).toEqual([finalized.resolution])

          const recipient = content("envelope")
          const prepared = yield* store.prepare(finalized.resolution.id, recipient)
          if (prepared.envelope === undefined) return yield* Effect.die("expected a prepared envelope")
          expect(prepared.envelope.message.id).toBe(prepared.messageID)
          expect(prepared.envelope.parts.map((part) => part.id)).toEqual([
            Delegation.OriginPartID.make(`prt_${prepared.id.slice(4)}_0`),
            Delegation.OriginPartID.make(`prt_${prepared.id.slice(4)}_1`),
          ])
          expect(prepared.envelope.provenance).toMatchObject({
            generationID: registered.generation.id,
            parentID: rootID,
            childID,
            origin: registered.generation.origin,
            source,
            historyCutoff: "history-envelope",
            consumed: [],
          })

          const reordered: Delegation.RecipientContent = {
            message: { text: "delegation envelope", kind: "synthetic" },
            parts: [
              { text: "delegation envelope", kind: "text" },
              { name: "envelope", kind: "metadata" },
            ],
          }
          expect(yield* store.prepare(prepared.id, reordered)).toEqual(prepared)

          const changedPart = yield* store
            .prepare(prepared.id, { ...recipient, parts: [{ kind: "changed", text: "different" }, recipient.parts[1]] })
            .pipe(Effect.exit)
          expectFailure(changedPart)
          const changedOrder = yield* store
            .prepare(prepared.id, { ...recipient, parts: [...recipient.parts].reverse() })
            .pipe(Effect.exit)
          expectFailure(changedOrder)
          const changedPayload = yield* store
            .prepare(prepared.id, {
              ...recipient,
              message: { kind: "synthetic", text: "different payload" },
            })
            .pipe(Effect.exit)
          expectFailure(changedPayload)

          expect(yield* store.admit(prepared.id, persistEnvelope)).toEqual({ status: "admitted" })
          expect(yield* receiverCounts(db)).toEqual({ messages: 1, parts: 2 })
          expect((yield* store.getResolution(prepared.id))?.status).toBe("admitted")
          expect(yield* store.incoming(rootID)).toEqual([expect.objectContaining({ id: prepared.id, status: "admitted" })])

          const skippedCallback = yield* store.admit(prepared.id, (tx, resolution) =>
            persistEnvelope(tx, resolution, 0),
          )
          expect(skippedCallback).toEqual({ status: "admitted" })
          expect(yield* receiverCounts(db)).toEqual({ messages: 1, parts: 2 })
        }),
      true,
    )
  })

  test("rejects invalid runtime JSON RecipientContent before persisting its envelope", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("invalid-runtime-content", rootID, childID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)
          const reserved = yield* store.reserveSource({
            sessionID: childID,
            generationID: registered.generation.id,
            source: assistantSource("invalid-runtime-content"),
            historyCutoff: "history-invalid-runtime-content",
            consumed: [],
          })
          const finalized = yield* store.finalizeSource(reserved.source.id, {
            payload: "invalid runtime content",
            outcome: "reply",
          })
          if (finalized.resolution === undefined) return yield* Effect.die("expected invalid-content resolution")

          // Simulate an untyped runtime payload crossing the adapter boundary.
          const invalidContent = {
            message: { kind: "synthetic", text: "invalid runtime content" },
            parts: [undefined],
          } as unknown as Delegation.RecipientContent
          const failed = yield* store.prepare(finalized.resolution.id, invalidContent).pipe(Effect.exit)
          expectFailure(failed)
          if (Exit.isFailure(failed)) {
            expect(Cause.squash(failed.cause)).toMatchObject({ code: "invalid_content" })
          }
          expect(yield* rawResolution(db, finalized.resolution.id)).toMatchObject({ status: "pending", has_envelope: 0 })
          expect(yield* receiverCounts(db)).toEqual({ messages: 0, parts: 0 })
          expect(yield* store.pending(rootID)).toEqual([finalized.resolution])
        }),
      true,
    )
  })

  test("rolls back same-DB message and part visibility on typed failure or blocked admission, then retries after reopen", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    const recorded = await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("multipart-retry", rootID, childID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)
          const reserved = yield* store.reserveSource({
            sessionID: childID,
            generationID: registered.generation.id,
            source: assistantSource("multipart-retry"),
            historyCutoff: "history-multipart-retry",
            consumed: [],
          })
          const finalized = yield* store.finalizeSource(reserved.source.id, {
            payload: "multipart retry",
            outcome: "reply",
          })
          if (finalized.resolution === undefined) return yield* Effect.die("expected a multipart resolution")
          const prepared = yield* store.prepare(finalized.resolution.id, multipartContent("multipart-retry"))

          const failedAfterPart = yield* store
            .admit(prepared.id, (tx, resolution) => persistEnvelope(tx, resolution, 1))
            .pipe(Effect.exit)
          expectFailure(failedAfterPart)
          expect(yield* receiverCounts(db)).toEqual({ messages: 0, parts: 0 })
          expect(yield* rawResolution(db, prepared.id)).toMatchObject({ status: "pending", has_envelope: 1 })

          const blockedAfterPartialWrite = yield* store.admit(prepared.id, blockedAfterWrite)
          expect(blockedAfterPartialWrite).toEqual({ status: "blocked", reason: "recipient is busy" })
          expect(yield* receiverCounts(db)).toEqual({ messages: 0, parts: 0 })
          expect(yield* rawResolution(db, prepared.id)).toMatchObject({ status: "pending", has_envelope: 1 })

          return { resolutionID: prepared.id, messageID: prepared.messageID }
        }),
      true,
    )

    await withStore(filename, (db, store) =>
      Effect.gen(function* () {
        const retried = yield* store.admit(recorded.resolutionID, persistEnvelope)
        expect(retried).toEqual({ status: "admitted" })
        expect(yield* receiverCounts(db)).toEqual({ messages: 1, parts: 3 })
        expect(yield* db.get(sql`SELECT id FROM delegation_outbox_test_message WHERE id = ${recorded.messageID}`)).toEqual({
          id: recorded.messageID,
        })
        expect(yield* db.get(sql`SELECT COUNT(*) AS count FROM delegation_outbox_test_part WHERE message_id = ${recorded.messageID}`)).toEqual({
          count: 3,
        })
        expect((yield* store.getResolution(recorded.resolutionID))?.status).toBe("admitted")
      }),
      false,
    )

    await withStore(filename, (db, store) =>
      Effect.gen(function* () {
        const admittedRetry = yield* store.admit(recorded.resolutionID, (tx, resolution) =>
          persistEnvelope(tx, resolution, 0),
        )
        expect(admittedRetry).toEqual({ status: "admitted" })
        expect(yield* receiverCounts(db)).toEqual({ messages: 1, parts: 3 })
        expect(yield* rawResolution(db, recorded.resolutionID)).toMatchObject({ status: "admitted", has_envelope: 1 })
      }),
      false,
    )
  })

  test("acknowledges exact preexisting envelopes and rejects payload, part, and provenance collisions", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    const recorded = await withStore(
      filename,
      (_db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("collision", rootID, childID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)

          const makePrepared = (name: string) =>
            Effect.gen(function* () {
              const reserved = yield* store.reserveSource({
                sessionID: childID,
                generationID: registered.generation.id,
                source: assistantSource(name),
                historyCutoff: `history-collision-${name}`,
                consumed: [],
              })
              const finalized = yield* store.finalizeSource(reserved.source.id, {
                payload: `payload-${name}`,
                outcome: "reply",
              })
              if (finalized.resolution === undefined) return yield* Effect.die(`expected ${name} resolution`)
              return yield* store.prepare(finalized.resolution.id, content(name))
            })

          const exact = yield* makePrepared("exact")
          const payload = yield* makePrepared("payload")
          const provenance = yield* makePrepared("provenance")
          const part = yield* makePrepared("part")
          if (exact.envelope === undefined || payload.envelope === undefined || provenance.envelope === undefined || part.envelope === undefined)
            return yield* Effect.die("expected prepared collision envelopes")

          yield* insertEnvelope(_db, exact)
          yield* insertEnvelope(_db, payload, {
            messageData: { kind: "synthetic", text: "wrong payload" },
          })
          yield* insertEnvelope(_db, provenance, {
            provenance: { ...provenance.envelope.provenance, source: { kind: "assistant", id: "wrong-source" } },
          })
          yield* insertEnvelope(_db, part, {
            parts: part.envelope.parts.map((item, index) =>
              index === 1 ? { id: item.id, data: { kind: "wrong-part" } } : { id: item.id, data: item.data },
            ),
          })

          return {
            exactID: exact.id,
            payloadID: payload.id,
            provenanceID: provenance.id,
            partID: part.id,
          }
        }),
      true,
    )

    await withStore(filename, (db, store) =>
      Effect.gen(function* () {
        expect(yield* store.admit(recorded.exactID, (tx, resolution) => persistEnvelope(tx, resolution, 0))).toEqual({
          status: "admitted",
        })

        const payloadCollision = yield* store
          .admit(recorded.payloadID, persistEnvelope)
          .pipe(Effect.exit)
        const provenanceCollision = yield* store
          .admit(recorded.provenanceID, persistEnvelope)
          .pipe(Effect.exit)
        const partCollision = yield* store.admit(recorded.partID, persistEnvelope).pipe(Effect.exit)
        expectFailure(payloadCollision)
        expectFailure(provenanceCollision)
        expectFailure(partCollision)
        expect((yield* store.getResolution(recorded.payloadID))?.status).toBe("pending")
        expect((yield* store.getResolution(recorded.provenanceID))?.status).toBe("pending")
        expect((yield* store.getResolution(recorded.partID))?.status).toBe("pending")
        expect(yield* receiverCounts(db)).toEqual({ messages: 4, parts: 8 })
      }),
      false,
    )
  })

  test("keeps immutable capture conflicts rejected after admission and closure", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("capture-conflict", rootID, childID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)
          const source = terminalSource("capture-conflict")
          const reservation = yield* store.reserveSource({
            sessionID: childID,
            generationID: registered.generation.id,
            source,
            historyCutoff: "history-capture-conflict",
            consumed: [],
          })
          const finalized = yield* store.finalizeSource(reservation.source.id, {
            payload: "terminal error",
            outcome: "error",
          })
          if (finalized.resolution === undefined) return yield* Effect.die("expected terminal resolution")
          const prepared = yield* store.prepare(finalized.resolution.id, content("capture-conflict"))
          expect(yield* store.admit(prepared.id, persistEnvelope)).toEqual({ status: "admitted" })

          const exact = yield* store.capture(registered.generation.id, capture(source, "terminal error", "history-capture-conflict", [], "error"))
          expect(exact.id).toBe(prepared.id)
          expect(exact.messageID).toBe(prepared.messageID)

          const variants = [
            capture(source, "changed payload", "history-capture-conflict", [], "error"),
            capture(source, "terminal error", "history-capture-conflict", [], "cancelled"),
            capture(source, "terminal error", "history-changed", [], "error"),
            capture(
              source,
              "terminal error",
              "history-capture-conflict",
              [Delegation.ResolutionID.make("res_unrelated_contribution")],
              "error",
            ),
          ]
          for (const variant of variants) {
            const failed = yield* store.capture(registered.generation.id, variant).pipe(Effect.exit)
            expectFailure(failed)
          }

          const closed = yield* close(store, registered.generation.id)
          expect(closed.closed).toBe(true)
          expect(yield* store.capture(registered.generation.id, capture(source, "terminal error", "history-capture-conflict", [], "error"))).toMatchObject({
            id: prepared.id,
            messageID: prepared.messageID,
          })
          for (const variant of variants) {
            const failed = yield* store.capture(registered.generation.id, variant).pipe(Effect.exit)
            expectFailure(failed)
          }
          expect(yield* countRows(db, "delegation_resolution")).toBe(1)
          expect((yield* store.getResolution(prepared.id))?.status).toBe("admitted")
        }),
      true,
    )
  })

  test("marks incoming returns consumed atomically and lets a root source resolve them without echo", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const child = yield* createAdmittedResolution(store, "root-consume", rootID, childID)
          const sibling = yield* createAdmittedResolution(store, "root-consume-sibling", rootID, siblingID)
          const other = yield* createAdmittedResolution(store, "root-consume-other", otherRootID, otherChildID)

          const wrongParent = yield* store
            .markConsumed({ parentID: rootID, ids: [child.resolution.id, other.resolution.id] })
            .pipe(Effect.exit)
          expectFailure(wrongParent)
          expect((yield* store.getResolution(child.resolution.id))?.status).toBe("admitted")
          expect((yield* store.getResolution(other.resolution.id))?.status).toBe("admitted")

          const unknown = yield* store
            .markConsumed({ parentID: rootID, ids: [sibling.resolution.id, Delegation.ResolutionID.make("res_unknown")] })
            .pipe(Effect.exit)
          expectFailure(unknown)
          expect((yield* store.getResolution(sibling.resolution.id))?.status).toBe("admitted")

          yield* store.markConsumed({ parentID: rootID, ids: [child.resolution.id] })
          expect((yield* store.getResolution(child.resolution.id))?.status).toBe("consumed")
          expect(yield* store.incoming(rootID)).toEqual([
            expect.objectContaining({ id: child.resolution.id, status: "consumed" }),
            expect.objectContaining({ id: sibling.resolution.id, status: "admitted" }),
          ])

          const rootSource = yield* store.reserveSource({
            sessionID: rootID,
            source: assistantSource("root-resolves-return"),
            historyCutoff: "history-root-resolves-return",
            consumed: [child.resolution.id],
          })
          expect((yield* store.getResolution(child.resolution.id))?.status).toBe("consumed")
          const finalized = yield* store.finalizeSource(rootSource.source.id, {
            payload: "root own reply",
            outcome: "reply",
          })
          expect(finalized.resolution).toBeUndefined()
          expect((yield* store.getResolution(child.resolution.id))?.status).toBe("resolved")
          expect((yield* store.getResolution(child.resolution.id))?.resolvedSourceID).toBe(rootSource.source.id)
          expect(yield* store.pending(rootID)).toEqual([])
          expect(yield* store.incoming(rootID)).toEqual([expect.objectContaining({ id: sibling.resolution.id })])
          expect(yield* countRows(db, "delegation_resolution")).toBe(3)
        }),
      true,
    )
  })

  test("blocks parent closure for pending, admitted, and consumed incoming returns after the child closes", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const parent = yield* store.register(
            registration("incoming-parent", rootID, intermediateID, { mode: "background" }),
          )
          yield* store.finishWork(parent.workID)
          const child = yield* store.register(
            registration("incoming-child", intermediateID, grandchildID, {
              mode: "background",
              parentGenerationID: parent.generation.id,
            }),
          )
          yield* store.finishWork(child.workID)

          const source = yield* store.reserveSource({
            sessionID: grandchildID,
            generationID: child.generation.id,
            source: assistantSource("incoming-child-return"),
            historyCutoff: "history-incoming-child-return",
            consumed: [],
          })
          const childFinalized = yield* store.finalizeSource(source.source.id, {
            payload: "child return",
            outcome: "reply",
          })
          if (childFinalized.resolution === undefined) return yield* Effect.die("expected child return")
          const incoming = yield* store.prepare(childFinalized.resolution.id, content("incoming-child-return"))

          const pendingBlocked = yield* close(store, parent.generation.id)
          expect(pendingBlocked.closed).toBe(false)
          expect(pendingBlocked.blockers).toContain(`incoming:${incoming.id}`)

          expect(yield* store.admit(incoming.id, persistEnvelope)).toEqual({ status: "admitted" })
          expect((yield* close(store, child.generation.id)).closed).toBe(true)
          const admittedBlocked = yield* close(store, parent.generation.id)
          expect(admittedBlocked.closed).toBe(false)
          expect(admittedBlocked.blockers).toContain(`incoming:${incoming.id}`)

          yield* store.markConsumed({ parentID: intermediateID, ids: [incoming.id] })
          const consumedBlocked = yield* close(store, parent.generation.id)
          expect(consumedBlocked.closed).toBe(false)
          expect(consumedBlocked.blockers).toContain(`incoming:${incoming.id}`)

          const parentSource = yield* store.reserveSource({
            sessionID: intermediateID,
            generationID: parent.generation.id,
            source: assistantSource("incoming-parent-final"),
            historyCutoff: "history-incoming-parent-final",
            consumed: [incoming.id],
          })
          const parentFinalized = yield* store.finalizeSource(parentSource.source.id, {
            payload: "parent final",
            outcome: "reply",
          })
          if (parentFinalized.resolution === undefined) return yield* Effect.die("expected parent return")
          const parentEnvelope = yield* store.prepare(parentFinalized.resolution.id, content("incoming-parent-final"))
          yield* store.admit(parentEnvelope.id, persistEnvelope)
          expect((yield* store.getResolution(incoming.id))?.status).toBe("resolved")
          expect((yield* close(store, parent.generation.id)).closed).toBe(true)
          expect(yield* countRows(db, "delegation_resolution")).toBe(2)
        }),
      true,
    )
  })

  test("discards a tool-turn source without resolving its consumed return", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const incoming = yield* createAdmittedResolution(store, "discard-incoming", rootID, childID)
          yield* store.markConsumed({ parentID: rootID, ids: [incoming.resolution.id] })

          const discardedReservation = yield* store.reserveSource({
            sessionID: rootID,
            source: terminalSource("discarded-tool"),
            historyCutoff: "history-discarded-tool",
            consumed: [incoming.resolution.id],
          })
          expect((yield* store.getResolution(incoming.resolution.id))?.status).toBe("consumed")
          const discarded = yield* store.discardSource(discardedReservation.source.id)
          expect(discarded.state).toBe("discarded")
          expect((yield* store.getResolution(incoming.resolution.id))?.status).toBe("consumed")
          expect(yield* store.unfinished(rootID)).toEqual([])
          expect(yield* store.pending(rootID)).toEqual([])

          const finalized = yield* store
            .finalizeSource(discarded.id, { payload: "discarded must not publish", outcome: "error" })
            .pipe(Effect.exit)
          expectFailure(finalized)
          expect(yield* store.discardSource(discarded.id)).toEqual(discarded)
          expect(yield* countRows(db, "delegation_resolution")).toBe(1)
        }),
      true,
    )
  })

  test("accounts for runtime, active work, and active descendants before closing", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (_db, store) =>
        Effect.gen(function* () {
          const parent = yield* store.register(
            registration("close-blockers-parent", rootID, intermediateID, { mode: "background" }),
          )
          const child = yield* store.register(
            registration("close-blockers-child", intermediateID, grandchildID, {
              parentGenerationID: parent.generation.id,
            }),
          )

          const beforeFinish = yield* close(store, parent.generation.id)
          expect(beforeFinish.closed).toBe(false)
          expect(beforeFinish.blockers).toEqual(
            expect.arrayContaining([
              `work:${parent.workID}`,
              `descendant:${child.generation.id}`,
            ]),
          )

          yield* store.finishWork(parent.workID)
          const childWorkBlocked = yield* close(store, child.generation.id)
          expect(childWorkBlocked.closed).toBe(false)
          expect(childWorkBlocked.blockers).toContain(`work:${child.workID}`)

          yield* store.finishWork(child.workID)
          expect((yield* close(store, child.generation.id)).closed).toBe(true)

          const runtimeBlocked = yield* store.reconcileAndClose(parent.generation.id, () =>
            Effect.succeed({ quiescent: false, sources: [] }),
          )
          expect(runtimeBlocked.closed).toBe(false)
          expect(runtimeBlocked.blockers).toContain("runtime")
          expect((yield* close(store, parent.generation.id)).closed).toBe(true)
        }),
      true,
    )
  })

  test("lets closure win against queued update, input, and descendant work", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("lock-wins", rootID, raceChildID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)

          const oldSource = assistantSource("lock-wins-old-return")
          const oldReservation = yield* store.reserveSource({
            sessionID: raceChildID,
            generationID: registered.generation.id,
            source: oldSource,
            historyCutoff: "history-lock-wins-old-return",
            consumed: [],
          })
          const oldFinalized = yield* store.finalizeSource(oldReservation.source.id, {
            payload: "old generation reply",
            outcome: "reply",
          })
          if (oldFinalized.resolution === undefined) return yield* Effect.die("expected old generation resolution")
          const oldEnvelope = yield* store.prepare(oldFinalized.resolution.id, content("lock-wins-old-return"))
          yield* store.admit(oldEnvelope.id, persistEnvelope)

          const reconcileEntered = yield* Deferred.make<void>()
          const reconcileRelease = yield* Deferred.make<void>()
          const reconcileDone = yield* Deferred.make<void>()
          const closeFiber = yield* pausedReconcile(
            store,
            registered.generation.id,
            reconcileEntered,
            reconcileRelease,
            reconcileDone,
          )
          yield* Deferred.await(reconcileEntered)

          const updateAttempted = yield* Deferred.make<void>()
          const updateDone = yield* Deferred.make<void>()
          const updateFiber = yield* Effect.gen(function* () {
            yield* Deferred.succeed(updateAttempted, undefined)
            const result = yield* store
              .register(
                registration("lock-wins-update", rootID, raceChildID, {
                  explicitReuse: true,
                  mode: "background",
                }),
              )
              .pipe(Effect.exit)
            yield* Deferred.succeed(updateDone, undefined)
            return result
          }).pipe(Effect.forkChild)
          yield* Deferred.await(updateAttempted)

          const inputAttempted = yield* Deferred.make<void>()
          const inputDone = yield* Deferred.make<void>()
          const inputFiber = yield* Effect.gen(function* () {
            yield* Deferred.succeed(inputAttempted, undefined)
            const result = yield* store
              .startWork({
                id: Delegation.WorkID.make("dwk_delegation_outbox_lock_wins_input"),
                sessionID: raceChildID,
                generationID: registered.generation.id,
                kind: "input",
              })
              .pipe(Effect.exit)
            yield* Deferred.succeed(inputDone, undefined)
            return result
          }).pipe(Effect.forkChild)
          yield* Deferred.await(inputAttempted)

          const descendantAttempted = yield* Deferred.make<void>()
          const descendantDone = yield* Deferred.make<void>()
          const descendantFiber = yield* Effect.gen(function* () {
            yield* Deferred.succeed(descendantAttempted, undefined)
            const result = yield* store
              .register(
                registration("lock-wins-descendant", raceChildID, raceGrandchildID, {
                  mode: "background",
                  parentGenerationID: registered.generation.id,
                }),
              )
              .pipe(Effect.exit)
            yield* Deferred.succeed(descendantDone, undefined)
            return result
          }).pipe(Effect.forkChild)
          yield* Deferred.await(descendantAttempted)

          yield* Effect.yieldNow
          expect(yield* Deferred.isDone(reconcileDone)).toBe(false)
          expect(yield* Deferred.isDone(updateDone)).toBe(false)
          expect(yield* Deferred.isDone(inputDone)).toBe(false)
          expect(yield* Deferred.isDone(descendantDone)).toBe(false)

          yield* Deferred.succeed(reconcileRelease, undefined)
          const closed = yield* Fiber.join(closeFiber)
          const update = yield* Fiber.join(updateFiber)
          const input = yield* Fiber.join(inputFiber)
          const descendant = yield* Fiber.join(descendantFiber)

          expect(closed.closed).toBe(true)
          expect(closed.blockers).toEqual([])
          expect(yield* store.get(registered.generation.id)).toMatchObject({ state: "closed" })
          expect(Exit.isSuccess(update)).toBe(true)
          if (Exit.isFailure(update)) return yield* Effect.die("expected explicit update after closure")
          expect(update.value.generation.id).not.toBe(registered.generation.id)
          expect((yield* store.active(raceChildID))?.id).toBe(update.value.generation.id)
          expectFailure(input)
          expectFailure(descendant)

          const staleCapture = yield* store
            .capture(update.value.generation.id, capture(oldSource, "old generation reply", "history-lock-wins-old-return"))
            .pipe(Effect.exit)
          expectFailure(staleCapture)
          expect(yield* countRows(db, "delegation_resolution")).toBe(1)
        }),
      true,
    )
  })

  test("keeps closure active when committed input work wins first", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (_db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("input-wins", rootID, raceChildID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)
          const input = yield* store.startWork({
            id: Delegation.WorkID.make("dwk_delegation_outbox_input_wins"),
            sessionID: raceChildID,
            generationID: registered.generation.id,
            kind: "input",
          })

          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const done = yield* Deferred.make<void>()
          const closeFiber = yield* pausedReconcile(store, registered.generation.id, entered, release, done)
          yield* Deferred.await(entered)
          expect(yield* Deferred.isDone(done)).toBe(false)
          yield* Deferred.succeed(release, undefined)
          const blocked = yield* Fiber.join(closeFiber)

          expect(blocked.closed).toBe(false)
          expect(blocked.blockers).toContain(`work:${input.id}`)
          expect(blocked.generation.id).toBe(registered.generation.id)
          expect(yield* store.get(registered.generation.id)).toMatchObject({ state: "active" })
        }),
      true,
    )
  })

  test("keeps closure active when an explicit update commits first", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (_db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("update-wins", rootID, siblingID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)
          const update = yield* store.register(
            registration("update-wins-update", rootID, siblingID, {
              explicitReuse: true,
              mode: "background",
            }),
          )
          expect(update.generation.id).toBe(registered.generation.id)
          expect(update.workState).toBe("active")

          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const done = yield* Deferred.make<void>()
          const closeFiber = yield* pausedReconcile(store, registered.generation.id, entered, release, done)
          yield* Deferred.await(entered)
          expect(yield* Deferred.isDone(done)).toBe(false)
          yield* Deferred.succeed(release, undefined)
          const blocked = yield* Fiber.join(closeFiber)

          expect(blocked.closed).toBe(false)
          expect(blocked.blockers).toContain(`work:${update.workID}`)
          expect(blocked.generation.id).toBe(registered.generation.id)
          expect(yield* store.get(registered.generation.id)).toMatchObject({ state: "active" })
        }),
      true,
    )
  })

  test("keeps closure active when a descendant commits first", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (_db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("descendant-wins", rootID, raceChildID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)
          const descendant = yield* store.register(
            registration("descendant-wins-child", raceChildID, raceGrandchildID, {
              mode: "background",
              parentGenerationID: registered.generation.id,
            }),
          )

          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const done = yield* Deferred.make<void>()
          const closeFiber = yield* pausedReconcile(store, registered.generation.id, entered, release, done)
          yield* Deferred.await(entered)
          expect(yield* Deferred.isDone(done)).toBe(false)
          yield* Deferred.succeed(release, undefined)
          const blocked = yield* Fiber.join(closeFiber)

          expect(blocked.closed).toBe(false)
          expect(blocked.blockers).toContain(`descendant:${descendant.generation.id}`)
          expect(blocked.generation.id).toBe(registered.generation.id)
          expect(yield* store.get(registered.generation.id)).toMatchObject({ state: "active" })
        }),
      true,
    )
  })

  test("captures logical finalization before queued admission and retains both returns", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const parent = yield* store.register(
            registration("finalize-admission-race-parent", rootID, intermediateID, { mode: "background" }),
          )
          yield* store.finishWork(parent.workID)
          const child = yield* store.register(
            registration("finalize-admission-race-child", intermediateID, grandchildID, {
              mode: "background",
              parentGenerationID: parent.generation.id,
            }),
          )
          yield* store.finishWork(child.workID)

          const incomingSource = yield* store.reserveSource({
            sessionID: grandchildID,
            generationID: child.generation.id,
            source: assistantSource("finalize-admission-race-incoming"),
            historyCutoff: "history-finalize-admission-race-incoming",
            consumed: [],
          })
          const incomingFinalized = yield* store.finalizeSource(incomingSource.source.id, {
            payload: "incoming return",
            outcome: "reply",
          })
          if (incomingFinalized.resolution === undefined) return yield* Effect.die("expected incoming race resolution")
          const incoming = yield* store.prepare(
            incomingFinalized.resolution.id,
            content("finalize-admission-race-incoming"),
          )
          const incomingBlocked = yield* close(store, parent.generation.id)
          expect(incomingBlocked.closed).toBe(false)
          expect(incomingBlocked.blockers).toContain(`incoming:${incoming.id}`)

          const ownSource = assistantSource("finalize-admission-race-own")
          const ownReservation = yield* store.reserveSource({
            sessionID: intermediateID,
            generationID: parent.generation.id,
            source: ownSource,
            historyCutoff: "history-finalize-admission-race-own",
            consumed: [],
          })
          yield* addEvidence(db, {
            id: "finalize-admission-race-own-evidence",
            sessionID: intermediateID,
            generationID: parent.generation.id,
            source: ownSource,
            payload: "own return",
            outcome: "reply",
            historyCutoff: "history-finalize-admission-race-own",
            logicalFinalized: true,
          })

          const reconcileEntered = yield* Deferred.make<void>()
          const reconcileRelease = yield* Deferred.make<void>()
          const reconcileDone = yield* Deferred.make<void>()
          const reconcileFiber = yield* Effect.gen(function* () {
            const result = yield* store.reconcileAndClose(parent.generation.id, (tx, generation) =>
              Effect.gen(function* () {
                const evidence = yield* reconcileEvidence(tx, generation)
                yield* Deferred.succeed(reconcileEntered, undefined)
                yield* Deferred.await(reconcileRelease)
                return evidence
              }),
            )
            yield* Deferred.succeed(reconcileDone, undefined)
            return result
          }).pipe(Effect.forkChild)
          yield* Deferred.await(reconcileEntered)

          const admissionAttempted = yield* Deferred.make<void>()
          const admissionDone = yield* Deferred.make<void>()
          const admissionFiber = yield* Effect.gen(function* () {
            yield* Deferred.succeed(admissionAttempted, undefined)
            const result = yield* store.admit(incoming.id, persistEnvelope)
            yield* Deferred.succeed(admissionDone, undefined)
            return result
          }).pipe(Effect.forkChild)
          yield* Deferred.await(admissionAttempted)
          yield* Effect.yieldNow
          expect(yield* Deferred.isDone(admissionDone)).toBe(false)
          expect(yield* Deferred.isDone(reconcileDone)).toBe(false)

          yield* Deferred.succeed(reconcileRelease, undefined)
          const reconciled = yield* Fiber.join(reconcileFiber)
          const admitted = yield* Fiber.join(admissionFiber)
          expect(reconciled.closed).toBe(false)
          expect(reconciled.blockers).toContain(`incoming:${incoming.id}`)
          expect(reconciled.resolutions).toHaveLength(1)
          expect(reconciled.resolutions[0]?.source).toEqual(ownSource)
          expect(ownReservation.source.state).toBe("reserved")
          expect(admitted).toEqual({ status: "admitted" })
          expect((yield* store.getResolution(incoming.id))?.status).toBe("admitted")
          expect(yield* receiverCounts(db)).toEqual({ messages: 1, parts: 2 })
          expect(yield* countRows(db, "delegation_resolution")).toBe(2)

          const retried = yield* store.reconcileAndClose(parent.generation.id, reconcileEvidence)
          expect(retried.closed).toBe(false)
          expect(retried.blockers).toContain(`incoming:${incoming.id}`)
          expect(retried.resolutions.map((resolution) => resolution.id)).toEqual([
            reconciled.resolutions[0]?.id,
          ])
          expect(yield* countRows(db, "delegation_resolution")).toBe(2)
          expect(yield* receiverCounts(db)).toEqual({ messages: 1, parts: 2 })
        }),
      true,
    )
  })

  test("queues close behind a receiver transaction and sees the committed admitted return", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const parent = yield* store.register(
            registration("receiver-close-race-parent", rootID, intermediateID, { mode: "background" }),
          )
          yield* store.finishWork(parent.workID)
          const child = yield* store.register(
            registration("receiver-close-race-child", intermediateID, grandchildID, {
              mode: "background",
              parentGenerationID: parent.generation.id,
            }),
          )
          yield* store.finishWork(child.workID)
          const source = yield* store.reserveSource({
            sessionID: grandchildID,
            generationID: child.generation.id,
            source: assistantSource("receiver-close-race"),
            historyCutoff: "history-receiver-close-race",
            consumed: [],
          })
          const finalized = yield* store.finalizeSource(source.source.id, {
            payload: "receiver close race",
            outcome: "reply",
          })
          if (finalized.resolution === undefined) return yield* Effect.die("expected receiver race resolution")
          const prepared = yield* store.prepare(finalized.resolution.id, content("receiver-close-race"))

          const receiverEntered = yield* Deferred.make<void>()
          const receiverRelease = yield* Deferred.make<void>()
          const admissionDone = yield* Deferred.make<void>()
          const admissionFiber = yield* Effect.gen(function* () {
            const admitted = yield* store.admit(prepared.id, (tx, resolution) =>
              Effect.gen(function* () {
                const result = yield* persistEnvelope(tx, resolution)
                yield* Deferred.succeed(receiverEntered, undefined)
                yield* Deferred.await(receiverRelease)
                return result
              }),
            )
            yield* Deferred.succeed(admissionDone, undefined)
            return admitted
          }).pipe(Effect.forkChild)
          yield* Deferred.await(receiverEntered)

          const closeAttempted = yield* Deferred.make<void>()
          const closeDone = yield* Deferred.make<void>()
          const closeFiber = yield* Effect.gen(function* () {
            yield* Deferred.succeed(closeAttempted, undefined)
            const result = yield* close(store, parent.generation.id)
            yield* Deferred.succeed(closeDone, undefined)
            return result
          }).pipe(Effect.forkChild)
          yield* Deferred.await(closeAttempted)
          yield* Effect.yieldNow
          expect(yield* Deferred.isDone(closeDone)).toBe(false)
          expect(yield* Deferred.isDone(admissionDone)).toBe(false)

          yield* Deferred.succeed(receiverRelease, undefined)
          const admitted = yield* Fiber.join(admissionFiber)
          const blocked = yield* Fiber.join(closeFiber)
          expect(admitted).toEqual({ status: "admitted" })
          expect((yield* store.getResolution(prepared.id))?.status).toBe("admitted")
          expect(blocked.closed).toBe(false)
          expect(blocked.blockers).toContain(`incoming:${prepared.id}`)
          expect(yield* receiverCounts(db)).toEqual({ messages: 1, parts: 2 })
        }),
      true,
    )
  })

  test("revocation prevents late delivery after stop and delete", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("revoked-notification", rootID, siblingID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)
          const source = yield* store.reserveSource({
            sessionID: siblingID,
            generationID: registered.generation.id,
            source: assistantSource("revoked-notification"),
            historyCutoff: "history-revoked-notification",
            consumed: [],
          })
          const finalized = yield* store.finalizeSource(source.source.id, {
            payload: "must not deliver",
            outcome: "reply",
          })
          if (finalized.resolution === undefined) return yield* Effect.die("expected revoked resolution")
          const prepared = yield* store.prepare(finalized.resolution.id, content("revoked-notification"))

          const revocation = yield* store.revokeDescendants(rootID)
          expect(revocation.cancellationSource.kind).toBe("terminal")
          yield* db.delete(SessionTable).where(eq(SessionTable.id, rootID)).run()

          let receiverCalled = false
          const admission = yield* store.admit(prepared.id, (tx, resolution) => {
            receiverCalled = true
            return persistEnvelope(tx, resolution)
          })
          expect(admission).toEqual({ status: "revoked" })
          expect(receiverCalled).toBe(false)
          expect((yield* store.getResolution(prepared.id))?.status).toBe("revoked")
          expect(yield* receiverCounts(db)).toEqual({ messages: 0, parts: 0 })
        }),
      true,
    )
  })

  test("does not run a source reconciler after parent stop or delete", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("revoked-source-reconcile", rootID, childID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)
          const source = assistantSource("revoked-source-reconcile")
          const reserved = yield* store.reserveSource({
            sessionID: childID,
            generationID: registered.generation.id,
            source,
            historyCutoff: "history-revoked-source-reconcile",
            consumed: [],
          })
          yield* addEvidence(db, {
            id: "revoked-source-reconcile-evidence",
            sessionID: childID,
            generationID: registered.generation.id,
            source,
            payload: "must not reconcile",
            outcome: "error",
            historyCutoff: "history-revoked-source-reconcile",
            logicalFinalized: true,
          })
          yield* store.revokeDescendants(rootID)

          let callbackCalls = 0
          const reconciler = (tx: Transaction, generation: Delegation.Generation) => {
            callbackCalls += 1
            return reconcileEvidence(tx, generation)
          }
          const stopped = yield* store.reconcileAndClose(registered.generation.id, reconciler)
          expect(stopped.closed).toBe(false)
          expect(stopped.blockers).toEqual(["revoked"])
          expect(stopped.resolutions).toEqual([])
          expect(callbackCalls).toBe(0)
          expect((yield* store.sources(childID))[0]).toMatchObject({
            id: reserved.source.id,
            source,
            state: "reserved",
          })
          expect(yield* countRows(db, "delegation_resolution")).toBe(0)
          expect(yield* receiverCounts(db)).toEqual({ messages: 0, parts: 0 })

          yield* db.delete(SessionTable).where(eq(SessionTable.id, rootID)).run()
          const deleted = yield* store.reconcileAndClose(registered.generation.id, reconciler)
          expect(deleted.closed).toBe(false)
          expect(deleted.blockers).toEqual(["revoked"])
          expect(deleted.resolutions).toEqual([])
          expect(callbackCalls).toBe(0)
          expect((yield* store.sources(childID))[0]).toMatchObject({
            id: reserved.source.id,
            source,
            state: "reserved",
          })
          expect(yield* countRows(db, "delegation_resolution")).toBe(0)
          expect(yield* receiverCounts(db)).toEqual({ messages: 0, parts: 0 })
        }),
      true,
    )
  })

  test("blocks a root-recipient generation mismatch from a legacy/corrupt fixture", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "delegation.sqlite")

    await withStore(
      filename,
      (db, store) =>
        Effect.gen(function* () {
          const registered = yield* store.register(
            registration("root-recipient-frozen", rootID, childID, { mode: "background" }),
          )
          yield* store.finishWork(registered.workID)
          const reserved = yield* store.reserveSource({
            sessionID: childID,
            generationID: registered.generation.id,
            source: assistantSource("root-recipient-frozen"),
            historyCutoff: "history-root-recipient-frozen",
            consumed: [],
          })
          const finalized = yield* store.finalizeSource(reserved.source.id, {
            payload: "root recipient return",
            outcome: "reply",
          })
          if (finalized.resolution === undefined) return yield* Effect.die("expected root-recipient resolution")
          expect(finalized.resolution.recipientGenerationID).toBeUndefined()
          const prepared = yield* store.prepare(finalized.resolution.id, content("root-recipient-frozen"))

          const legacyIncoming = registration("legacy-root-recipient-incoming", otherRootID, rootID)
          // Legacy/corrupt fixture only: register() rejects this unsettled adoption on the valid API path.
          yield* db
            .insert(DelegationGenerationTable)
            .values({
              id: legacyIncoming.generationID,
              parent_id: legacyIncoming.parentID,
              child_id: legacyIncoming.childID,
              origin: legacyIncoming.origin,
              parent_generation_id: null,
              mode: "foreground",
              state: "active",
              time_created: Date.now(),
              time_closed: null,
            })
            .run()

          let receiverCalled = false
          const admission = yield* store.admit(prepared.id, (tx, resolution) => {
            receiverCalled = true
            return persistEnvelope(tx, resolution)
          })
          expect(admission).toEqual({
            status: "blocked",
            reason: "recipient session has an active incoming generation",
          })
          expect(receiverCalled).toBe(false)
          expect(yield* rawResolution(db, prepared.id)).toMatchObject({ status: "pending", has_envelope: 1 })
          expect(yield* receiverCounts(db)).toEqual({ messages: 0, parts: 0 })
        }),
      true,
    )
  })
})

type SessionFixture = {
  readonly id: SessionID
  readonly parentID?: SessionID
}

type RegistrationOptions = {
  readonly explicitReuse?: boolean
  readonly mode?: Delegation.Mode
  readonly parentGenerationID?: Delegation.ID
}

type EvidenceInput = {
  readonly id: string
  readonly sessionID: SessionID
  readonly generationID: Delegation.ID
  readonly source: Delegation.Source
  readonly payload: string
  readonly outcome: Delegation.Outcome
  readonly historyCutoff: string
  readonly logicalFinalized: boolean
}

type InsertEnvelopeOptions = {
  readonly messageData?: unknown
  readonly provenance?: unknown
  readonly parts?: readonly {
    readonly id: Delegation.OriginPartID
    readonly data: unknown
  }[]
}

async function withStore<A, E>(
  filename: string,
  use: (db: DB, store: Store) => Effect.Effect<A, E>,
  initialize = false,
) {
  const runtime = ManagedRuntime.make(Database.layerFromPath(filename))
  try {
    return await runtime.runPromise(
      Effect.gen(function* () {
        const database = yield* Database.Service
        yield* createTestTables(database.db).pipe(Effect.orDie)
        if (initialize) yield* seed(database.db).pipe(Effect.orDie)
        return yield* use(database.db, DelegationStore.make(database.db))
      }),
    )
  } finally {
    await runtime.dispose()
  }
}

function seed(db: DB) {
  const sessions: readonly SessionFixture[] = [
    { id: rootID },
    { id: otherRootID },
    { id: childID, parentID: rootID },
    { id: siblingID, parentID: rootID },
    { id: intermediateID, parentID: rootID },
    { id: grandchildID, parentID: intermediateID },
    { id: otherChildID, parentID: otherRootID },
    { id: raceChildID, parentID: rootID },
    { id: raceGrandchildID, parentID: raceChildID },
  ]

  return Effect.gen(function* () {
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .run()
    yield* db
      .insert(SessionTable)
      .values(
        sessions.map((session) => ({
          id: session.id,
          project_id: Project.ID.global,
          parent_id: session.parentID ?? null,
          slug: session.id,
          directory: "/project",
          title: session.id,
          version: "test",
        })),
      )
      .run()
  })
}

function createTestTables(db: DB) {
  return Effect.gen(function* () {
    yield* db.run(`
      CREATE TABLE IF NOT EXISTS delegation_outbox_test_source (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        generation_id TEXT,
        source_kind TEXT NOT NULL,
        source_id TEXT NOT NULL,
        payload TEXT NOT NULL,
        outcome TEXT NOT NULL,
        history_cutoff TEXT NOT NULL,
        consumed TEXT NOT NULL,
        logical_finalized INTEGER NOT NULL
      )
    `)
    yield* db.run(`
      CREATE TABLE IF NOT EXISTS delegation_outbox_test_message (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        data TEXT NOT NULL,
        provenance TEXT NOT NULL
      )
    `)
    yield* db.run(`
      CREATE TABLE IF NOT EXISTS delegation_outbox_test_part (
        id TEXT PRIMARY KEY,
        message_id TEXT NOT NULL,
        position INTEGER NOT NULL,
        data TEXT NOT NULL,
        UNIQUE(message_id, position)
      )
    `)
  })
}

function registration(
  name: string,
  parentID: SessionID,
  childID: SessionID,
  options: RegistrationOptions = {},
): Delegation.Registration {
  return {
    requestID: Delegation.RequestID.make(`drq_delegation_outbox_${name}`),
    generationID: Delegation.ID.make(`dlg_delegation_outbox_${name}`),
    parentID,
    childID,
    origin: {
      messageID: SessionMessage.ID.make(`msg_delegation_outbox_${name}`),
      partID: Delegation.OriginPartID.make(`part_delegation_outbox_${name}`),
      callID: `call_delegation_outbox_${name}`,
    },
    ...(options.parentGenerationID === undefined ? {} : { parentGenerationID: options.parentGenerationID }),
    ...(options.mode === undefined ? {} : { mode: options.mode }),
    explicitReuse: options.explicitReuse ?? false,
  }
}

function assistantSource(name: string): Delegation.Source {
  return { kind: "assistant", id: `assistant-delegation-outbox-${name}` }
}

function terminalSource(name: string): Delegation.Source {
  return { kind: "terminal", id: `terminal-delegation-outbox-${name}` }
}

function capture(
  source: Delegation.Source,
  payload: string,
  historyCutoff: string,
  consumed: readonly Delegation.ResolutionID[] = [],
  outcome: Delegation.Outcome = source.kind === "assistant" ? "reply" : "error",
): Delegation.Capture {
  return { source, payload, outcome, historyCutoff, consumed: [...consumed] }
}

function content(name: string): Delegation.RecipientContent {
  return {
    message: { kind: "synthetic", text: `delegation ${name}` },
    parts: [
      { kind: "text", text: `delegation ${name}` },
      { kind: "metadata", name },
    ],
  }
}

function multipartContent(name: string): Delegation.RecipientContent {
  return {
    message: { kind: "synthetic", text: `multipart ${name}` },
    parts: [
      { kind: "text", text: `multipart ${name}` },
      { kind: "metadata", name },
      { kind: "cutoff", value: `history-${name}` },
    ],
  }
}

function close(store: Store, generationID: Delegation.ID) {
  return store.reconcileAndClose(generationID, () => Effect.succeed({ quiescent: true, sources: [] }))
}

function pausedReconcile(
  store: Store,
  generationID: Delegation.ID,
  entered: Deferred.Deferred<void>,
  release: Deferred.Deferred<void>,
  done: Deferred.Deferred<void>,
) {
  return Effect.gen(function* () {
    const result = yield* store.reconcileAndClose(generationID, (tx, generation) =>
      Effect.gen(function* () {
        const row = yield* tx
          .select({ state: DelegationGenerationTable.state })
          .from(DelegationGenerationTable)
          .where(eq(DelegationGenerationTable.id, generation.id))
          .get()
        expect(row?.state).toBe("active")
        yield* Deferred.succeed(entered, undefined)
        yield* Deferred.await(release)
        return { quiescent: true, sources: [] }
      }),
    )
    yield* Deferred.succeed(done, undefined)
    return result
  }).pipe(Effect.forkChild)
}

function expectFailure<A, E>(exit: Exit.Exit<A, E>) {
  expect(Exit.isFailure(exit)).toBe(true)
}

function addEvidence(db: DB, input: EvidenceInput) {
  return db.insert(SourceEvidenceTable).values({
    id: input.id,
    session_id: input.sessionID,
    generation_id: input.generationID,
    source_kind: input.source.kind,
    source_id: input.source.id,
    payload: input.payload,
    outcome: input.outcome,
    history_cutoff: input.historyCutoff,
    consumed: [],
    logical_finalized: input.logicalFinalized,
  }).run()
}

function reconcileEvidence(tx: Transaction, generation: Delegation.Generation) {
  return tx
    .select()
    .from(SourceEvidenceTable)
    .where(
      and(
        eq(SourceEvidenceTable.session_id, generation.childID),
        eq(SourceEvidenceTable.generation_id, generation.id),
        eq(SourceEvidenceTable.logical_finalized, true),
      ),
    )
    .orderBy(asc(SourceEvidenceTable.id))
    .all()
    .pipe(
      Effect.map((rows) => ({
        quiescent: true,
        sources: rows.map((row) => ({
          source: { kind: row.source_kind, id: row.source_id },
          payload: row.payload,
          outcome: row.outcome,
          historyCutoff: row.history_cutoff,
          consumed: row.consumed,
        })),
      })),
    )
}

function persistEnvelope(tx: Transaction, resolution: Delegation.Resolution, failAtPart?: number) {
  if (resolution.envelope === undefined) {
    return Effect.fail(new Delegation.AdapterError({ code: "missing_envelope", message: "receiver requires a prepared envelope" }))
  }

  const envelope = resolution.envelope
  return Effect.gen(function* () {
    const existingMessage = yield* tx
      .select()
      .from(ReceiverMessageTable)
      .where(eq(ReceiverMessageTable.id, envelope.message.id))
      .get()
    const existingParts = yield* tx
      .select()
      .from(ReceiverPartTable)
      .where(eq(ReceiverPartTable.message_id, envelope.message.id))
      .orderBy(asc(ReceiverPartTable.position))
      .all()

    const exact =
      existingMessage !== undefined &&
      existingMessage.session_id === resolution.parentID &&
      isDeepStrictEqual(existingMessage.data, envelope.message.data) &&
      isDeepStrictEqual(existingMessage.provenance, envelope.provenance) &&
      existingParts.length === envelope.parts.length &&
      existingParts.every(
        (part, index) =>
          part.id === envelope.parts[index]?.id &&
          part.position === index &&
          isDeepStrictEqual(part.data, envelope.parts[index]?.data),
      )

    if (existingMessage !== undefined || existingParts.length > 0) {
      if (!exact) {
        return yield* new Delegation.AdapterError({
          code: "receiver_conflict",
          message: `Receiver envelope conflicts with persisted message ${envelope.message.id}`,
        })
      }
      return { status: "admitted" as const }
    }

    yield* tx
      .insert(ReceiverMessageTable)
      .values({
        id: envelope.message.id,
        session_id: resolution.parentID,
        data: envelope.message.data,
        provenance: envelope.provenance,
      })
      .run()

    yield* Effect.forEach(envelope.parts, (part, index) =>
      Effect.gen(function* () {
        yield* tx
          .insert(ReceiverPartTable)
          .values({ id: part.id, message_id: envelope.message.id, position: index, data: part.data })
          .run()
        if (index === failAtPart) {
          expect(
            yield* tx.select().from(ReceiverPartTable).where(eq(ReceiverPartTable.message_id, envelope.message.id)).all(),
          ).toHaveLength(index + 1)
          return yield* new Delegation.AdapterError({
            code: "receiver_mid_parts",
            message: "receiver failed after persisting a part",
          })
        }
      }),
    )
    return { status: "admitted" as const }
  })
}

function blockedAfterWrite(tx: Transaction, resolution: Delegation.Resolution) {
  if (resolution.envelope === undefined) {
    return Effect.fail(new Delegation.AdapterError({ code: "missing_envelope", message: "receiver requires a prepared envelope" }))
  }
  const envelope = resolution.envelope
  return Effect.gen(function* () {
    yield* tx
      .insert(ReceiverMessageTable)
      .values({
        id: envelope.message.id,
        session_id: resolution.parentID,
        data: envelope.message.data,
        provenance: envelope.provenance,
      })
      .run()
    const part = envelope.parts[0]
    if (part === undefined) return yield* new Delegation.AdapterError({ code: "empty_envelope", message: "missing part" })
    yield* tx
      .insert(ReceiverPartTable)
      .values({ id: part.id, message_id: envelope.message.id, position: 0, data: part.data })
      .run()
    return { status: "blocked" as const, reason: "recipient is busy" }
  })
}

function insertEnvelope(db: DB, resolution: Delegation.Resolution, options: InsertEnvelopeOptions = {}) {
  if (resolution.envelope === undefined) {
    return Effect.fail(new Delegation.AdapterError({ code: "missing_envelope", message: "expected a prepared envelope" }))
  }
  const envelope = resolution.envelope
  const parts = options.parts ?? envelope.parts.map((part) => ({ id: part.id, data: part.data }))
  return Effect.gen(function* () {
    yield* db
      .insert(ReceiverMessageTable)
      .values({
        id: envelope.message.id,
        session_id: resolution.parentID,
        data: options.messageData ?? envelope.message.data,
        provenance: options.provenance ?? envelope.provenance,
      })
      .run()
    yield* db
      .insert(ReceiverPartTable)
      .values(parts.map((part, index) => ({ id: part.id, message_id: envelope.message.id, position: index, data: part.data })))
      .run()
  })
}

function receiverCounts(db: DB) {
  return Effect.gen(function* () {
    const messages = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM delegation_outbox_test_message`)
    const parts = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM delegation_outbox_test_part`)
    return { messages: messages?.count ?? 0, parts: parts?.count ?? 0 }
  })
}

function rawResolution(db: DB, id: Delegation.ResolutionID) {
  return db.get<{ status: string; has_envelope: number }>(sql`
    SELECT status, CASE WHEN envelope IS NULL THEN 0 ELSE 1 END AS has_envelope
    FROM delegation_resolution
    WHERE id = ${id}
  `)
}

function countRows(db: DB, table: string) {
  return db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM ${sql.identifier(table)}`).pipe(Effect.map((row) => row?.count ?? 0))
}

function createAdmittedResolution(
  store: Store,
  name: string,
  parentID: SessionID,
  childID: SessionID,
  parentGenerationID?: Delegation.ID,
) {
  return Effect.gen(function* () {
    const registered = yield* store.register(
      registration(name, parentID, childID, {
        mode: "background",
        ...(parentGenerationID === undefined ? {} : { parentGenerationID }),
      }),
    )
    yield* store.finishWork(registered.workID)
    const source = yield* store.reserveSource({
      sessionID: childID,
      generationID: registered.generation.id,
      source: assistantSource(`${name}-return`),
      historyCutoff: `history-${name}`,
      consumed: [],
    })
    const finalized = yield* store.finalizeSource(source.source.id, {
      payload: `payload-${name}`,
      outcome: "reply",
    })
    if (finalized.resolution === undefined) return yield* Effect.die(`expected ${name} resolution`)
    const prepared = yield* store.prepare(finalized.resolution.id, content(name))
    const admitted = yield* store.admit(prepared.id, persistEnvelope)
    if (admitted.status !== "admitted") return yield* Effect.die(`expected ${name} admission`)
    return { generation: registered.generation, resolution: prepared }
  })
}
