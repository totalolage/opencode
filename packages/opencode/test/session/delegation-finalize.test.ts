import { describe, expect } from "bun:test"
import { Cause, Effect, Exit, Layer } from "effect"
import { asc, eq, sql } from "drizzle-orm"
import path from "node:path"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { DelegationResolutionTable } from "@opencode-ai/core/delegation/sql"
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
import { SessionDelegation } from "../../src/session/delegation"
import { tmpdir } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(Layer.empty)
const scopedTmpdir = Effect.acquireRelease(
  Effect.promise(() => tmpdir()),
  (temporary) => Effect.promise(() => temporary[Symbol.asyncDispose]()),
)

type DB = Database.Interface["db"]
type Core = DelegationStore.Interface
type Adapter = ReturnType<typeof SessionDelegation.make>

type SessionFixture = {
  readonly id: SessionID
  readonly parentID?: SessionID
}

type RegistrationOptions = {
  readonly explicitReuse?: boolean
  readonly mode?: Delegation.Mode
  readonly parentGenerationID?: Delegation.ID
}

type Harness = {
  readonly db: DB
  readonly core: Core
  readonly adapter: Adapter
}

const providerID = Provider.ID.make("provider_delegation_finalize")
const modelID = Model.ID.make("model_delegation_finalize")
const usage = {
  cost: 1.25,
  tokens: {
    total: 38,
    input: 10,
    output: 20,
    reasoning: 3,
    cache: { read: 4, write: 5 },
  },
}

describe("SessionDelegation finalization", () => {
  it.live(
    "atomically projects a full assistant, captures deterministic JSON, and retries after pruning legacy parts",
    () =>
      Effect.gen(function* () {
        const temporary = yield* scopedTmpdir
        const filename = path.join(temporary.path, "delegation-finalize.sqlite")
        const root = sessionID("snapshot-root")
        const child = sessionID("snapshot-child")

        const recorded = yield* openStore(
          filename,
          (h) =>
            Effect.gen(function* () {
              const registered = yield* createGeneration(h.core, "snapshot", root, child, "background")
              const reserved = yield* h.core.reserveSource({
                sessionID: child,
                generationID: registered.generation.id,
                source: assistantSource("snapshot"),
                historyCutoff: "history-snapshot",
                consumed: [],
              })
              const message = assistantMessage("snapshot", child)
              const projectedStep = message.parts.find((part) => part.type === "step-finish")
              if (projectedStep?.type !== "step-finish") {
                return yield* Effect.die("expected a projected step-finish part")
              }
              const incremental = {
                info: { ...message.info },
                parts: [projectedStep],
              } satisfies SessionV1.WithParts
              if (incremental.info.role !== "assistant") {
                return yield* Effect.die("expected incremental assistant info")
              }
              delete incremental.info.structured
              yield* seedProjected(h.db, incremental)
              yield* writeUsage(h.db, child)
              const beforeFinalize = yield* readProjected(h.db, message.info.id)
              expect(beforeFinalize.message?.data).toEqual(messageData(incremental.info))
              expect(beforeFinalize.parts.map((part) => part.id)).toEqual([projectedStep.id])
              expect(beforeFinalize.parts.map((part) => part.data)).toEqual([partData(projectedStep)])

              yield* h.adapter.finalize(reserved.source, message, "reply")

              const sources = yield* h.core.sources(child)
              const finalized = sources.find((source) => source.id === reserved.source.id)
              expect(finalized).toMatchObject({
                id: reserved.source.id,
                state: "finalized",
                outcome: "reply",
              })
              if (finalized === undefined || finalized.payload === undefined) {
                return yield* Effect.die("expected a finalized source payload")
              }
              expect(typeof finalized.payload).toBe("string")

              const pending = yield* h.core.pending(root)
              expect(pending).toHaveLength(1)
              expect(pending[0]).toMatchObject({
                source: reserved.source.source,
                outcome: "reply",
                historyCutoff: reserved.source.historyCutoff,
                consumed: [],
              })
              expect(finalized.payload).toBe('{"a":{"x":1,"y":2},"z":"last"}')

              const projected = yield* readProjected(h.db, message.info.id)
              const expectedParts = [...message.parts].sort((left, right) =>
                Buffer.compare(Buffer.from(left.id), Buffer.from(right.id)),
              )
              expect(projected.message?.session_id).toBe(child)
              expect(projected.parts.map((part) => part.id)).toEqual(expectedParts.map((part) => part.id))
              expect(projected.parts.map((part) => part.data)).toEqual(expectedParts.map((part) => partData(part)))
              expect(projected.message?.data).toEqual(messageData(message.info))
              expect(yield* readUsage(h.db, child)).toMatchObject({
                cost: usage.cost,
                tokens_input: usage.tokens.input,
                tokens_output: usage.tokens.output,
                tokens_reasoning: usage.tokens.reasoning,
                tokens_cache_read: usage.tokens.cache.read,
                tokens_cache_write: usage.tokens.cache.write,
              })

              const partToPrune = message.parts[0]
              if (partToPrune === undefined) return yield* Effect.die("expected a part to prune")

              return {
                child,
                root,
                generationID: registered.generation.id,
                sourceID: reserved.source.id,
                message,
                partToPrune: partToPrune.id,
                payload: finalized.payload,
              }
            }),
          [{ id: root }, { id: child, parentID: root }],
        )

        yield* openStore(filename, (h) =>
          Effect.gen(function* () {
            const source = (yield* h.core.sources(recorded.child)).find((item) => item.id === recorded.sourceID)
            if (source === undefined) return yield* Effect.die("expected the source after reopen")
            const before = yield* h.core.pending(recorded.root)
            expect(before).toHaveLength(1)
            expect(before[0]?.payload).toBe(recorded.payload)

            const reordered = reorderedMessage(recorded.message)
            yield* h.adapter.finalize(source, reordered, "reply")
            expect((yield* h.core.pending(recorded.root))[0]?.payload).toBe(recorded.payload)

            yield* h.db.delete(PartTable).where(eq(PartTable.id, recorded.partToPrune)).run()
            yield* h.db.run(
              sql`UPDATE message SET data = json_set(data, '$.structured', ${JSON.stringify({ legacy: true })}) WHERE id = ${recorded.message.info.id}`,
            )
            const beforeRetry = yield* readProjected(h.db, recorded.message.info.id)
            expect(beforeRetry.parts.map((part) => part.id)).not.toContain(recorded.partToPrune)

            yield* h.adapter.finalize(source, recorded.message, "reply")

            const afterRetry = yield* readProjected(h.db, recorded.message.info.id)
            expect(afterRetry.parts.map((part) => part.id)).not.toContain(recorded.partToPrune)
            expect(afterRetry.message?.data).toEqual(beforeRetry.message?.data)
            expect(yield* h.core.pending(recorded.root)).toEqual(before)

            const changedPayload = yield* h.adapter
              .finalize(source, changedMessage(recorded.message), "reply")
              .pipe(Effect.exit)
            expectFailure(changedPayload)

            const changedOutcome = yield* h.adapter.finalize(source, recorded.message, "error").pipe(Effect.exit)
            expectFailure(changedOutcome)
            expect(yield* h.core.pending(recorded.root)).toEqual(before)
            const afterConflict = yield* readProjected(h.db, recorded.message.info.id)
            expect(afterConflict.message?.data).toEqual(beforeRetry.message?.data)
            expect(afterConflict.parts.map((part) => part.id)).not.toContain(recorded.partToPrune)
          }),
        )
      }),
  )

  it.live("preserves own __proto__ keys in structured JSON and the captured payload", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-proto.sqlite")
      const root = sessionID("proto-root")
      const child = sessionID("proto-child")

      yield* openStore(
        filename,
        (h) =>
          Effect.gen(function* () {
            const registered = yield* createGeneration(h.core, "proto", root, child, "background")
            const reserved = yield* h.core.reserveSource({
              sessionID: child,
              generationID: registered.generation.id,
              source: assistantSource("proto"),
              historyCutoff: "history-proto",
              consumed: [],
            })
            const message = withoutUsage(assistantMessage("proto", child))
            if (message.info.role !== "assistant") return yield* Effect.die("expected an assistant proto snapshot")
            const structured = Object.fromEntries([
              ["__proto__", { polluted: true }],
              ["nested", { value: 1 }],
            ])
            expect(Object.hasOwn(structured, "__proto__")).toBe(true)
            message.info.structured = structured

            yield* h.adapter.finalize(reserved.source, message, "reply")
            const finalized = (yield* h.core.sources(child)).find((source) => source.id === reserved.source.id)
            expect(finalized).toMatchObject({ state: "finalized", outcome: "reply" })
            const expectedPayload = '{"__proto__":{"polluted":true},"nested":{"value":1}}'
            expect(finalized?.payload).toBe(expectedPayload)
            expect((yield* h.core.pending(root))[0]?.payload).toBe(expectedPayload)

            const projected = yield* readProjected(h.db, message.info.id)
            if (projected.message?.data.role !== "assistant") {
              return yield* Effect.die("expected a persisted assistant proto snapshot")
            }
            const persisted = Object.getOwnPropertyDescriptor(projected.message.data, "structured")?.value
            if (persisted === null || typeof persisted !== "object") {
              return yield* Effect.die("expected persisted structured JSON")
            }
            expect(Object.hasOwn(persisted, "__proto__")).toBe(true)
            expect(persisted).toEqual(structured)
          }),
        [{ id: root }, { id: child, parentID: root }],
      )
    }),
  )

  it.live("rejects nested undefined arbitrary JSON atomically but omits optional completed time", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-undefined.sqlite")
      const root = sessionID("undefined-root")
      const child = sessionID("undefined-child")

      yield* openStore(
        filename,
        (h) =>
          Effect.gen(function* () {
            const registered = yield* createGeneration(h.core, "undefined", root, child, "background")
            const structuredMessage = withoutUsage(assistantMessage("undefined-structured", child))
            if (structuredMessage.info.role !== "assistant") {
              return yield* Effect.die("expected an assistant structured snapshot")
            }
            structuredMessage.info.structured = { nested: { value: undefined } }

            const metadataMessage = withoutUsage(assistantMessage("undefined-metadata", child))
            const metadataPart = metadataMessage.parts.find((part) => part.type === "text")
            if (metadataPart?.type !== "text") return yield* Effect.die("expected a text metadata part")
            metadataPart.metadata = { nested: { value: undefined } }

            const toolMessage = withoutUsage(assistantMessage("undefined-tool-input", child))
            toolMessage.parts.push({
              id: partID("undefined-tool-input"),
              sessionID: child,
              messageID: toolMessage.info.id,
              type: "tool",
              callID: "call-undefined-tool-input",
              tool: "undefined-tool",
              state: {
                status: "completed",
                input: { nested: { value: undefined } },
                output: "",
                title: "",
                metadata: {},
                time: { start: 100, end: 200 },
              },
            })

            yield* Effect.forEach(
              [
                { name: "undefined-structured", message: structuredMessage },
                { name: "undefined-metadata", message: metadataMessage },
                { name: "undefined-tool-input", message: toolMessage },
              ],
              (item) =>
                Effect.gen(function* () {
                  const reserved = yield* h.core.reserveSource({
                    sessionID: child,
                    generationID: registered.generation.id,
                    source: assistantSource(item.name),
                    historyCutoff: `history-${item.name}`,
                    consumed: [],
                  })
                  const rejected = yield* h.adapter.finalize(reserved.source, item.message, "reply").pipe(Effect.exit)
                  expectFailure(rejected)
                  expect(
                    (yield* h.core.sources(child)).find((source) => source.id === reserved.source.id),
                  ).toMatchObject({
                    state: "reserved",
                  })
                  expect(yield* readProjected(h.db, item.message.info.id)).toEqual({ message: undefined, parts: [] })
                  expect(yield* h.core.pending(root)).toEqual([])
                }),
            )

            const optionalMessage = withoutUsage(assistantMessage("undefined-completed", child))
            if (optionalMessage.info.role !== "assistant") {
              return yield* Effect.die("expected an assistant optional snapshot")
            }
            optionalMessage.info.time.completed = undefined
            const optionalSource = yield* h.core.reserveSource({
              sessionID: child,
              generationID: registered.generation.id,
              source: assistantSource("undefined-completed"),
              historyCutoff: "history-undefined-completed",
              consumed: [],
            })
            yield* h.adapter.finalize(optionalSource.source, optionalMessage, "reply")
            const projected = yield* readProjected(h.db, optionalMessage.info.id)
            if (projected.message?.data.role !== "assistant") {
              return yield* Effect.die("expected a persisted optional snapshot")
            }
            expect(Object.hasOwn(projected.message.data.time, "completed")).toBe(false)
            expect(
              (yield* h.core.sources(child)).find((source) => source.id === optionalSource.source.id),
            ).toMatchObject({
              state: "finalized",
              payload: '{"a":{"x":1,"y":2},"z":"last"}',
            })
          }),
        [{ id: root }, { id: child, parentID: root }],
      )
    }),
  )

  it.live("uses stable error and cancellation fallbacks when text and error messages are empty", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-fallback.sqlite")
      const root = sessionID("fallback-root")
      const child = sessionID("fallback-child")

      yield* openStore(
        filename,
        (h) =>
          Effect.gen(function* () {
            const registered = yield* createGeneration(h.core, "fallback", root, child, "background")
            yield* Effect.forEach(
              [
                { name: "fallback-error", outcome: "error" as const },
                { name: "fallback-cancelled", outcome: "cancelled" as const },
              ],
              (item) =>
                Effect.gen(function* () {
                  const message = emptyOutcomeMessage(item.name, child)
                  const reserved = yield* h.core.reserveSource({
                    sessionID: child,
                    generationID: registered.generation.id,
                    source: assistantSource(item.name),
                    historyCutoff: `history-${item.name}`,
                    consumed: [],
                  })
                  yield* h.adapter.finalize(reserved.source, message, item.outcome)
                  const finalized = (yield* h.core.sources(child)).find((source) => source.id === reserved.source.id)
                  expect(finalized).toMatchObject({ state: "finalized", outcome: item.outcome, payload: item.outcome })
                  const beforeRetry = yield* readProjected(h.db, message.info.id)
                  yield* h.adapter.finalize(reserved.source, message, item.outcome)
                  expect(yield* readProjected(h.db, message.info.id)).toEqual(beforeRetry)
                  expect(
                    (yield* h.core.sources(child)).find((source) => source.id === reserved.source.id),
                  ).toMatchObject({
                    payload: item.outcome,
                    outcome: item.outcome,
                  })
                }),
            )
          }),
        [{ id: root }, { id: child, parentID: root }],
      )
    }),
  )

  it.live("uses SQLite BINARY UTF-8 part order for the last text payload", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-utf8-order.sqlite")
      const root = sessionID("utf8-root")
      const child = sessionID("utf8-child")

      yield* openStore(
        filename,
        (h) =>
          Effect.gen(function* () {
            const registered = yield* createGeneration(h.core, "utf8-order", root, child, "background")
            const message = withoutUsage(assistantMessage("utf8-order", child))
            if (message.info.role !== "assistant") return yield* Effect.die("expected an assistant UTF-8 snapshot")
            delete message.info.structured
            const astralID = SessionV1.PartID.make("prt_\u{10000}")
            const privateUseID = SessionV1.PartID.make("prt_\uE000")
            expect(astralID < privateUseID).toBe(true)
            message.parts = message.parts.filter((part) => part.type !== "text")
            message.parts.push(textPart(astralID, child, message.info.id, "astral-last"))
            message.parts.push(textPart(privateUseID, child, message.info.id, "private-use-first"))

            const reserved = yield* h.core.reserveSource({
              sessionID: child,
              generationID: registered.generation.id,
              source: assistantSource("utf8-order"),
              historyCutoff: "history-utf8-order",
              consumed: [],
            })
            yield* h.adapter.finalize(reserved.source, message, "reply")
            expect((yield* h.core.sources(child)).find((source) => source.id === reserved.source.id)).toMatchObject({
              payload: "astral-last",
            })

            const projected = yield* readProjected(h.db, message.info.id)
            expect(projected.parts.filter((part) => part.data.type === "text").map((part) => part.id)).toEqual([
              privateUseID,
              astralID,
            ])
          }),
        [{ id: root }, { id: child, parentID: root }],
      )
    }),
  )

  it.live("rolls back Core finalization and capture when a real legacy constraint rejects the snapshot", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-rollback.sqlite")
      const root = sessionID("rollback-root")
      const child = sessionID("rollback-child")

      yield* openStore(filename, (h) =>
        Effect.gen(function* () {
          yield* seedSessions(h.db, [{ id: root }, { id: child, parentID: root }])
          const registered = yield* createGeneration(h.core, "rollback", root, child, "background")
          const reserved = yield* h.core.reserveSource({
            sessionID: child,
            generationID: registered.generation.id,
            source: assistantSource("rollback"),
            historyCutoff: "history-rollback",
            consumed: [],
          })
          const message = assistantMessage("rollback", child)
          const projectedStep = message.parts.find((part) => part.type === "step-finish")
          if (projectedStep?.type !== "step-finish") {
            return yield* Effect.die("expected a projected rollback step-finish part")
          }
          const incremental = {
            info: { ...message.info },
            parts: [projectedStep],
          } satisfies SessionV1.WithParts
          if (incremental.info.role !== "assistant") {
            return yield* Effect.die("expected incremental rollback assistant info")
          }
          delete incremental.info.structured
          yield* seedProjected(h.db, incremental)
          yield* writeUsage(h.db, child)
          const beforeUsage = yield* readUsage(h.db, child)
          const beforeMessages = yield* h.db.select().from(MessageTable).all()
          const beforeParts = yield* h.db.select().from(PartTable).all()
          const rejectedPart = message.parts.find((part) => part.type === "text")
          if (rejectedPart === undefined) return yield* Effect.die("expected a part to reject")

          yield* h.db.run(`
            CREATE TRIGGER delegation_finalize_test_reject_part
            BEFORE INSERT ON part
            WHEN NEW.id = '${rejectedPart.id}'
            BEGIN
              SELECT RAISE(ABORT, 'delegation finalize test constraint');
            END
           `)

          const failed = yield* h.adapter.finalize(reserved.source, message, "reply").pipe(Effect.exit)
          expectFailureMessage(failed, "delegation finalize test constraint")
          expect((yield* h.core.sources(child))[0]).toMatchObject({ state: "reserved", id: reserved.source.id })
          expect(yield* h.core.pending(root)).toEqual([])
          expect(yield* h.core.unfinished(child)).toEqual([
            expect.objectContaining({ id: reserved.source.workID, state: "active" }),
          ])
          expect(yield* readUsage(h.db, child)).toEqual(beforeUsage)
          expect(yield* h.db.select().from(MessageTable).all()).toEqual(beforeMessages)
          expect(yield* h.db.select().from(PartTable).all()).toEqual(beforeParts)

          yield* h.db.run("DROP TRIGGER delegation_finalize_test_reject_part")
          yield* h.adapter.finalize(reserved.source, message, "reply")
          expect((yield* h.core.sources(child))[0]?.state).toBe("finalized")
          expect(yield* h.core.pending(root)).toHaveLength(1)
        }),
      )
    }),
  )

  it.live("rolls back projection and Core finalization when background capture rejects the resolution", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-resolution-rollback.sqlite")
      const root = sessionID("resolution-rollback-root")
      const child = sessionID("resolution-rollback-child")

      yield* openStore(
        filename,
        (h) =>
          Effect.gen(function* () {
            const registered = yield* createGeneration(h.core, "resolution-rollback", root, child, "background")
            const message = withoutUsage(assistantMessage("resolution-rollback", child))
            const reserved = yield* h.core.reserveSource({
              sessionID: child,
              generationID: registered.generation.id,
              source: assistantSource("resolution-rollback"),
              historyCutoff: "history-resolution-rollback",
              consumed: [],
            })
            const beforeProjection = yield* readProjected(h.db, message.info.id)
            const beforeUsage = yield* readUsage(h.db, child)

            yield* h.db.run(`
              CREATE TRIGGER delegation_finalize_test_reject_resolution
              BEFORE INSERT ON delegation_resolution
              WHEN NEW.source_id = '${message.info.id}'
              BEGIN
                SELECT RAISE(ABORT, 'delegation finalize resolution test constraint');
              END
            `)
            const failed = yield* h.adapter.finalize(reserved.source, message, "reply").pipe(Effect.exit)
            expectFailureMessage(failed, "delegation finalize resolution test constraint")
            expect(yield* readProjected(h.db, message.info.id)).toEqual(beforeProjection)
            expect(yield* readUsage(h.db, child)).toEqual(beforeUsage)
            expect((yield* h.core.sources(child)).find((source) => source.id === reserved.source.id)).toMatchObject({
              state: "reserved",
            })
            expect(
              (yield* h.core.listWork(registered.generation.id)).find((work) => work.id === reserved.source.workID),
            ).toMatchObject({ state: "active" })
            expect(yield* h.core.pending(root)).toEqual([])

            yield* h.db.run("DROP TRIGGER delegation_finalize_test_reject_resolution")
            yield* h.adapter.finalize(reserved.source, message, "reply")
            expect(yield* readProjected(h.db, message.info.id)).not.toEqual(beforeProjection)
            expect((yield* h.core.sources(child)).find((source) => source.id === reserved.source.id)).toMatchObject({
              state: "finalized",
            })
            expect(yield* h.core.pending(root)).toHaveLength(1)
          }),
        [{ id: root }, { id: child, parentID: root }],
      )
    }),
  )

  it.live(
    "rejects aggregate usage mismatches without changing projector totals, then accepts existing usage unchanged",
    () =>
      Effect.gen(function* () {
        const temporary = yield* scopedTmpdir
        const filename = path.join(temporary.path, "delegation-finalize-usage.sqlite")
        const root = sessionID("usage-root")
        const child = sessionID("usage-child")

        yield* openStore(filename, (h) =>
          Effect.gen(function* () {
            yield* seedSessions(h.db, [{ id: root }, { id: child, parentID: root }])
            const message = assistantMessage("usage", child)
            const changedBase = assistantMessage("usage-changed-step", child)
            const removedBase = assistantMessage("usage-removed-step", child)
            if (
              message.info.role !== "assistant" ||
              changedBase.info.role !== "assistant" ||
              removedBase.info.role !== "assistant"
            ) {
              return yield* Effect.die("expected assistant usage snapshots")
            }
            yield* seedProjected(h.db, message)
            yield* seedProjected(h.db, changedBase)
            yield* seedProjected(h.db, removedBase)
            yield* writeUsage(h.db, child)
            const beforeUsage = yield* readUsage(h.db, child)
            const beforeProjection = yield* readProjected(h.db, message.info.id)
            const beforeChangedProjection = yield* readProjected(h.db, changedBase.info.id)
            const beforeRemovedProjection = yield* readProjected(h.db, removedBase.info.id)
            const registered = yield* createGeneration(h.core, "usage", root, child, "background")
            const changedSource = yield* h.core.reserveSource({
              sessionID: child,
              generationID: registered.generation.id,
              source: assistantSource("usage-changed-step"),
              historyCutoff: "history-usage-changed-step",
              consumed: [],
            })
            const removedSource = yield* h.core.reserveSource({
              sessionID: child,
              generationID: registered.generation.id,
              source: assistantSource("usage-removed-step"),
              historyCutoff: "history-usage-removed-step",
              consumed: [],
            })

            const changedStep = changedBase.parts.find((part) => part.type === "step-finish")
            if (changedStep?.type !== "step-finish") {
              return yield* Effect.die("expected an existing step-finish contribution")
            }
            const removedStep = removedBase.parts.find((part) => part.type === "step-finish")
            if (removedStep?.type !== "step-finish") {
              return yield* Effect.die("expected an existing step-finish to remove")
            }
            const changedContribution = {
              ...changedStep,
              cost: changedStep.cost + 1,
              tokens: {
                ...changedStep.tokens,
                total: (changedStep.tokens.total ?? 0) + 1,
                input: changedStep.tokens.input + 1,
                output: changedStep.tokens.output + 1,
                reasoning: changedStep.tokens.reasoning + 1,
                cache: {
                  read: changedStep.tokens.cache.read + 1,
                  write: changedStep.tokens.cache.write + 1,
                },
              },
            }
            const changed = {
              ...changedBase,
              parts: changedBase.parts.map((part) => (part.id === changedStep.id ? changedContribution : part)),
            } satisfies SessionV1.WithParts
            const rejected = yield* h.adapter.finalize(changedSource.source, changed, "reply").pipe(Effect.exit)
            expectFailure(rejected)
            expect(yield* readUsage(h.db, child)).toEqual(beforeUsage)
            expect(yield* readProjected(h.db, message.info.id)).toEqual(beforeProjection)
            expect(yield* readProjected(h.db, changedBase.info.id)).toEqual(beforeChangedProjection)
            expect(
              (yield* h.core.sources(child)).find((source) => source.id === changedSource.source.id),
            ).toMatchObject({ state: "reserved" })
            expect(yield* h.core.pending(root)).toEqual([])

            const removed = {
              ...removedBase,
              parts: removedBase.parts.filter((part) => part.id !== removedStep.id),
            } satisfies SessionV1.WithParts
            const removedRejected = yield* h.adapter.finalize(removedSource.source, removed, "reply").pipe(Effect.exit)
            expectFailure(removedRejected)
            expect(yield* readUsage(h.db, child)).toEqual(beforeUsage)
            expect(yield* readProjected(h.db, removedBase.info.id)).toEqual(beforeRemovedProjection)
            expect(
              (yield* h.core.sources(child)).find((source) => source.id === removedSource.source.id),
            ).toMatchObject({ state: "reserved" })
            expect(yield* h.core.pending(root)).toEqual([])

            const validSource = yield* h.core.reserveSource({
              sessionID: child,
              generationID: registered.generation.id,
              source: assistantSource("usage"),
              historyCutoff: "history-usage",
              consumed: [],
            })
            yield* h.adapter.finalize(validSource.source, message, "reply")
            expect(yield* readUsage(h.db, child)).toEqual(beforeUsage)
            expect(yield* readProjected(h.db, message.info.id)).toEqual(beforeProjection)
            expect((yield* h.core.sources(child)).find((source) => source.id === validSource.source.id)).toMatchObject({
              state: "finalized",
            })
          }),
        )
      }),
  )

  it.live("accepts processor-style aggregate usage with last-step assistant tokens", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-aggregate-usage.sqlite")
      const root = sessionID("aggregate-usage-root")
      const child = sessionID("aggregate-usage-child")

      yield* openStore(
        filename,
        (h) =>
          Effect.gen(function* () {
            const message = assistantMessage("aggregate-usage", child)
            if (message.info.role !== "assistant") return yield* Effect.die("expected an assistant aggregate snapshot")
            const firstStep = message.parts.find((part) => part.type === "step-finish")
            if (firstStep?.type !== "step-finish") {
              return yield* Effect.die("expected the first aggregate step-finish part")
            }
            const secondStep = { ...firstStep, id: partID("m-step-aggregate-usage-second") }
            message.parts.push(secondStep)
            message.info.cost = usage.cost * 2
            const accountedUsage = {
              cost: usage.cost * 2,
              tokens: {
                total: usage.tokens.total * 2,
                input: usage.tokens.input * 2,
                output: usage.tokens.output * 2,
                reasoning: usage.tokens.reasoning * 2,
                cache: { read: usage.tokens.cache.read * 2, write: usage.tokens.cache.write * 2 },
              },
            }
            yield* seedProjected(h.db, message)
            yield* writeUsage(h.db, child, accountedUsage)
            const beforeProjection = yield* readProjected(h.db, message.info.id)
            const beforeUsage = yield* readUsage(h.db, child)
            const registered = yield* createGeneration(h.core, "aggregate-usage", root, child, "background")
            const reserved = yield* h.core.reserveSource({
              sessionID: child,
              generationID: registered.generation.id,
              source: assistantSource("aggregate-usage"),
              historyCutoff: "history-aggregate-usage",
              consumed: [],
            })

            yield* h.adapter.finalize(reserved.source, message, "reply")
            expect(yield* readUsage(h.db, child)).toEqual(beforeUsage)
            expect(yield* readProjected(h.db, message.info.id)).toEqual(beforeProjection)
            expect((yield* h.core.sources(child)).find((source) => source.id === reserved.source.id)).toMatchObject({
              state: "finalized",
              outcome: "reply",
            })
          }),
        [{ id: root }, { id: child, parentID: root }],
      )
    }),
  )

  it.live("rejects a new nonzero step-finish without projected usage and rolls back every table", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-usage-absent.sqlite")
      const root = sessionID("usage-absent-root")
      const child = sessionID("usage-absent-child")

      yield* openStore(
        filename,
        (h) =>
          Effect.gen(function* () {
            const message = assistantMessage("usage-absent", child)
            const registered = yield* createGeneration(h.core, "usage-absent", root, child, "background")
            const reserved = yield* h.core.reserveSource({
              sessionID: child,
              generationID: registered.generation.id,
              source: assistantSource("usage-absent"),
              historyCutoff: "history-usage-absent",
              consumed: [],
            })
            const beforeUsage = yield* readUsage(h.db, child)
            const beforeMessages = yield* h.db.select().from(MessageTable).all()
            const beforeParts = yield* h.db.select().from(PartTable).all()

            const rejected = yield* h.adapter.finalize(reserved.source, message, "reply").pipe(Effect.exit)
            expectFailure(rejected)
            expect((yield* h.core.sources(child)).find((source) => source.id === reserved.source.id)).toMatchObject({
              state: "reserved",
            })
            expect(yield* readUsage(h.db, child)).toEqual(beforeUsage)
            expect(yield* h.db.select().from(MessageTable).all()).toEqual(beforeMessages)
            expect(yield* h.db.select().from(PartTable).all()).toEqual(beforeParts)
            expect(yield* readProjected(h.db, message.info.id)).toEqual({ message: undefined, parts: [] })
            expect(yield* h.core.pending(root)).toEqual([])
          }),
        [{ id: root }, { id: child, parentID: root }],
      )
    }),
  )

  it.live("rejects a legacy assistant message identity reused by another generation", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-generation.sqlite")
      const root = sessionID("generation-root")
      const child = sessionID("generation-child")

      yield* openStore(filename, (h) =>
        Effect.gen(function* () {
          yield* seedSessions(h.db, [{ id: root }, { id: child, parentID: root }])
          const first = yield* createGeneration(h.core, "generation-first", root, child, "foreground")
          const message = withoutUsage(assistantMessage("generation-shared-message", child))
          const firstSource = yield* h.core.reserveSource({
            sessionID: child,
            generationID: first.generation.id,
            source: assistantSource("generation-shared-message"),
            historyCutoff: "history-generation-first",
            consumed: [],
          })
          yield* h.adapter.finalize(firstSource.source, message, "reply")
          const firstProjection = yield* readProjected(h.db, message.info.id)
          yield* h.adapter.close(child, true)
          expect((yield* h.core.get(first.generation.id))?.state).toBe("closed")

          const second = yield* h.core.register(
            registration("generation-second", root, child, { explicitReuse: true, mode: "foreground" }),
          )
          yield* h.core.finishWork(second.workID)
          const secondSource = yield* h.core.reserveSource({
            sessionID: child,
            generationID: second.generation.id,
            source: assistantSource("generation-shared-message"),
            historyCutoff: "history-generation-second",
            consumed: [],
          })

          const rejected = yield* h.adapter.finalize(secondSource.source, message, "reply").pipe(Effect.exit)
          expectFailure(rejected)
          expect(yield* readProjected(h.db, message.info.id)).toEqual(firstProjection)
          expect((yield* h.core.sources(child)).find((source) => source.id === secondSource.source.id)).toMatchObject({
            state: "reserved",
          })
          expect(yield* h.core.pending(root)).toEqual([])

          yield* h.adapter.finalize(firstSource.source, message, "reply")
          expect(yield* readProjected(h.db, message.info.id)).toEqual(firstProjection)
        }),
      )
    }),
  )

  it.live("keeps assistant identity for error and cancellation without foreground or root async returns", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-outcomes.sqlite")
      const root = sessionID("outcome-root")
      const child = sessionID("outcome-child")

      yield* openStore(filename, (h) =>
        Effect.gen(function* () {
          yield* seedSessions(h.db, [{ id: root }, { id: child, parentID: root }])
          const registered = yield* createGeneration(h.core, "outcomes", root, child, "foreground")
          const errorMessage = withoutUsage(assistantMessage("outcome-error", child, abortedError("assistant error")))
          const errorSource = yield* h.core.reserveSource({
            sessionID: child,
            generationID: registered.generation.id,
            source: assistantSource("outcome-error"),
            historyCutoff: "history-outcome-error",
            consumed: [],
          })
          yield* h.adapter.finalize(errorSource.source, errorMessage, "error")
          yield* h.adapter.finalize(errorSource.source, errorMessage, "error")

          const cancelledMessage = withoutUsage(
            assistantMessage("outcome-cancelled", child, abortedError("assistant cancelled")),
          )
          const cancelledSource = yield* h.core.reserveSource({
            sessionID: child,
            generationID: registered.generation.id,
            source: assistantSource("outcome-cancelled"),
            historyCutoff: "history-outcome-cancelled",
            consumed: [],
          })
          yield* h.adapter.finalize(cancelledSource.source, cancelledMessage, "cancelled")

          const rootMessage = withoutUsage(assistantMessage("outcome-root", root))
          const rootSource = yield* h.core.reserveSource({
            sessionID: root,
            source: assistantSource("outcome-root"),
            historyCutoff: "history-outcome-root",
            consumed: [],
          })
          yield* h.adapter.finalize(rootSource.source, rootMessage, "error")

          expect(yield* h.core.pending(root)).toEqual([])
          expect(
            yield* h.db.select({ id: MessageTable.id }).from(MessageTable).orderBy(asc(MessageTable.id)).all(),
          ).toEqual([errorMessage.info.id, cancelledMessage.info.id, rootMessage.info.id].sort().map((id) => ({ id })))
          expect((yield* h.core.sources(child)).map((source) => [source.source.id, source.outcome])).toEqual([
            [errorSource.source.source.id, "error"],
            [cancelledSource.source.source.id, "cancelled"],
          ])
        }),
      )
    }),
  )

  it.live("does not recapture a finalized foreground result after promotion", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-foreground-promotion.sqlite")
      const root = sessionID("foreground-promotion-root")
      const child = sessionID("foreground-promotion-child")

      yield* openStore(
        filename,
        (h) =>
          Effect.gen(function* () {
            const registered = yield* createGeneration(h.core, "foreground-promotion", root, child, "foreground")
            const message = withoutUsage(assistantMessage("foreground-promotion", child))
            const reserved = yield* h.core.reserveSource({
              sessionID: child,
              generationID: registered.generation.id,
              source: assistantSource("foreground-promotion"),
              historyCutoff: "history-foreground-promotion",
              consumed: [],
            })

            yield* h.adapter.finalize(reserved.source, message, "reply")
            expect((yield* h.core.get(registered.generation.id))?.state).toBe("active")
            expect(yield* h.core.pending(root)).toEqual([])
            const finalized = (yield* h.core.sources(child)).find((source) => source.id === reserved.source.id)
            expect(finalized).toMatchObject({
              source: { kind: "assistant", id: message.info.id },
              state: "finalized",
              outcome: "reply",
            })
            if (finalized === undefined) return yield* Effect.die("expected the finalized foreground source")

            const promoted = yield* h.core.promote(registered.generation.id)
            expect(promoted.mode).toBe("background")
            expect(yield* h.core.pending(root)).toEqual([])

            yield* h.adapter.finalize(reserved.source, message, "reply")
            expect(yield* h.core.pending(root)).toEqual([])
            expect(yield* h.core.sources(child)).toEqual([finalized])
          }),
        [{ id: root }, { id: child, parentID: root }],
      )
    }),
  )

  it.live("captures background error and cancellation outcomes once with stable assistant source identities", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-background-outcomes.sqlite")
      const root = sessionID("background-outcome-root")
      const child = sessionID("background-outcome-child")

      yield* openStore(
        filename,
        (h) =>
          Effect.gen(function* () {
            const registered = yield* createGeneration(h.core, "background-outcomes", root, child, "background")
            const errorMessage = withoutUsage(
              assistantMessage("background-outcome-error", child, abortedError("background assistant error")),
            )
            const errorSource = yield* h.core.reserveSource({
              sessionID: child,
              generationID: registered.generation.id,
              source: assistantSource("background-outcome-error"),
              historyCutoff: "history-background-outcome-error",
              consumed: [],
            })

            yield* h.adapter.finalize(errorSource.source, errorMessage, "error")
            const afterError = yield* h.core.pending(root)
            expect(afterError).toHaveLength(1)
            expect(afterError[0]).toMatchObject({
              source: { kind: "assistant", id: errorMessage.info.id },
              payload: "background assistant error",
              outcome: "error",
            })
            yield* h.adapter.finalize(errorSource.source, errorMessage, "error")
            expect(yield* h.core.pending(root)).toEqual(afterError)

            const cancelledMessage = withoutUsage(
              assistantMessage("background-outcome-cancelled", child, abortedError("background cancellation reason")),
            )
            const cancelledSource = yield* h.core.reserveSource({
              sessionID: child,
              generationID: registered.generation.id,
              source: assistantSource("background-outcome-cancelled"),
              historyCutoff: "history-background-outcome-cancelled",
              consumed: [],
            })

            yield* h.adapter.finalize(cancelledSource.source, cancelledMessage, "cancelled")
            const afterCancellation = yield* h.core.pending(root)
            expect(afterCancellation).toHaveLength(2)
            expect(afterCancellation).toEqual(
              expect.arrayContaining([
                expect.objectContaining({
                  source: { kind: "assistant", id: errorMessage.info.id },
                  payload: "background assistant error",
                  outcome: "error",
                }),
                expect.objectContaining({
                  source: { kind: "assistant", id: cancelledMessage.info.id },
                  payload: "background cancellation reason",
                  outcome: "cancelled",
                }),
              ]),
            )
            yield* h.adapter.finalize(cancelledSource.source, cancelledMessage, "cancelled")
            expect(yield* h.core.pending(root)).toEqual(afterCancellation)

            yield* h.adapter.close(child, true)
            expect((yield* h.core.get(registered.generation.id))?.state).toBe("active")
            expect(yield* h.core.pending(root)).toEqual(afterCancellation)
          }),
        [{ id: root }, { id: child, parentID: root }],
      )
    }),
  )

  it.live("fails input atomically, uses the terminal identity, and retries exactly after closure", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-input.sqlite")
      const root = sessionID("input-root")
      const child = sessionID("input-child")
      const sibling = sessionID("input-sibling")

      yield* openStore(filename, (h) =>
        Effect.gen(function* () {
          yield* seedSessions(h.db, [{ id: root }, { id: child, parentID: root }, { id: sibling, parentID: root }])
          const historical = assistantMessage("input-historical", child)
          const projectedStep = historical.parts.find((part) => part.type === "step-finish")
          if (projectedStep?.type !== "step-finish") {
            return yield* Effect.die("expected a projected input step-finish part")
          }
          const incremental = {
            info: { ...historical.info },
            parts: [projectedStep],
          } satisfies SessionV1.WithParts
          if (incremental.info.role !== "assistant") {
            return yield* Effect.die("expected incremental input assistant info")
          }
          delete incremental.info.structured
          yield* seedProjected(h.db, incremental)
          yield* writeUsage(h.db, child)
          const beforeHistorical = yield* readProjected(h.db, historical.info.id)
          const beforeUsage = yield* readUsage(h.db, child)
          const registered = yield* h.core.register(registration("input", root, child, { mode: "foreground" }))
          const inputWorkID = registered.workID
          const error = "input failed"

          const wrongWork = yield* h.adapter
            .failInput(registered.generation, workID("missing"), error)
            .pipe(Effect.exit)
          expectFailure(wrongWork)
          expect(
            (yield* h.core.listWork(registered.generation.id)).find((work) => work.id === inputWorkID)?.state,
          ).toBe("active")
          expect(yield* h.core.sources(child)).toEqual([])

          const other = yield* createGeneration(h.core, "input-other", root, sibling, "foreground")
          const wrongGeneration = yield* h.adapter.failInput(other.generation, inputWorkID, error).pipe(Effect.exit)
          expectFailure(wrongGeneration)
          expect(
            (yield* h.core.listWork(registered.generation.id)).find((work) => work.id === inputWorkID)?.state,
          ).toBe("active")
          expect(yield* h.core.sources(child)).toEqual([])

          yield* h.db.run(`
            CREATE TRIGGER delegation_finalize_test_fail_input
            BEFORE UPDATE OF state ON delegation_source
            WHEN NEW.source_id = 'delegation-input:${inputWorkID}' AND NEW.state = 'finalized'
            BEGIN
              SELECT RAISE(ABORT, 'delegation fail input test constraint');
            END
           `)
          const rolledBack = yield* h.adapter.failInput(registered.generation, inputWorkID, error).pipe(Effect.exit)
          expectFailureMessage(rolledBack, "delegation fail input test constraint")
          expect(
            (yield* h.core.listWork(registered.generation.id)).find((work) => work.id === inputWorkID)?.state,
          ).toBe("active")
          expect(yield* h.core.sources(child)).toEqual([])
          expect(yield* readUsage(h.db, child)).toEqual(beforeUsage)
          expect(yield* readProjected(h.db, historical.info.id)).toEqual(beforeHistorical)
          expect(yield* h.db.select().from(DelegationResolutionTable).all()).toEqual([])
          yield* h.db.run("DROP TRIGGER delegation_finalize_test_fail_input")

          yield* h.adapter.failInput(registered.generation, inputWorkID, error)
          const source = (yield* h.core.sources(child)).find(
            (item) => item.source.id === `delegation-input:${inputWorkID}`,
          )
          expect(source).toMatchObject({
            source: { kind: "terminal", id: `delegation-input:${inputWorkID}` },
            historyCutoff: `input:${inputWorkID}`,
            consumed: [],
            state: "finalized",
            outcome: "error",
          })
          expect(
            (yield* h.core.listWork(registered.generation.id)).find((work) => work.id === inputWorkID)?.state,
          ).toBe("finished")
          expect(yield* readProjected(h.db, historical.info.id)).toEqual(beforeHistorical)
          expect(yield* h.core.pending(root)).toEqual([])

          if (source === undefined) return yield* Effect.die("expected the finalized input source")
          expect((yield* h.core.get(registered.generation.id))?.state).toBe("active")
          const promoted = yield* h.core.promote(registered.generation.id)
          expect(promoted.mode).toBe("background")
          expect(registered.generation.mode).toBe("foreground")
          expect(yield* h.core.pending(root)).toEqual([])

          yield* h.adapter.failInput(registered.generation, inputWorkID, error)
          expect(yield* h.core.sources(child)).toEqual([source])
          expect(yield* h.core.pending(root)).toEqual([])

          yield* h.adapter.close(child, false)
          expect((yield* h.core.get(registered.generation.id))?.state).toBe("active")
          expect(yield* h.core.pending(root)).toEqual([])
          yield* h.adapter.close(child, true)
          expect((yield* h.core.get(registered.generation.id))?.state).toBe("closed")

          yield* h.adapter.failInput(registered.generation, inputWorkID, error)
          expect(yield* h.core.sources(child)).toEqual([source])
          expect(yield* h.core.pending(root)).toEqual([])
          expect(yield* readProjected(h.db, historical.info.id)).toEqual(beforeHistorical)

          const conflict = yield* h.adapter
            .failInput(registered.generation, inputWorkID, "different input failure")
            .pipe(Effect.exit)
          expectFailure(conflict)
          expect(yield* h.core.sources(child)).toEqual([source])
          expect(yield* readProjected(h.db, historical.info.id)).toEqual(beforeHistorical)
          expect(yield* h.core.pending(root)).toEqual([])
        }),
      )
    }),
  )

  it.live("captures background failed input as one terminal resolution and rolls back nested writes", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-background-input.sqlite")
      const root = sessionID("background-input-root")
      const child = sessionID("background-input-child")

      yield* openStore(
        filename,
        (h) =>
          Effect.gen(function* () {
            const historical = assistantMessage("background-input-historical", child)
            const projectedStep = historical.parts.find((part) => part.type === "step-finish")
            if (projectedStep?.type !== "step-finish") {
              return yield* Effect.die("expected a projected background input step-finish part")
            }
            const incremental = {
              info: { ...historical.info },
              parts: [projectedStep],
            } satisfies SessionV1.WithParts
            if (incremental.info.role !== "assistant") {
              return yield* Effect.die("expected incremental background input assistant info")
            }
            delete incremental.info.structured
            yield* seedProjected(h.db, incremental)
            const beforeHistorical = yield* readProjected(h.db, historical.info.id)
            yield* writeUsage(h.db, child)
            const registered = yield* h.core.register(
              registration("background-input", root, child, { mode: "background" }),
            )
            const inputWorkID = registered.workID
            const error = "background input failed"
            const beforeUsage = yield* readUsage(h.db, child)

            yield* h.db.run(`
              CREATE TRIGGER delegation_finalize_test_background_input
              BEFORE INSERT ON delegation_resolution
              WHEN NEW.source_id = 'delegation-input:${inputWorkID}'
              BEGIN
                SELECT RAISE(ABORT, 'delegation background input test constraint');
              END
            `)
            const rolledBack = yield* h.adapter.failInput(registered.generation, inputWorkID, error).pipe(Effect.exit)
            expectFailureMessage(rolledBack, "delegation background input test constraint")
            expect(yield* h.core.sources(child)).toEqual([])
            expect(
              (yield* h.core.listWork(registered.generation.id)).find((work) => work.id === inputWorkID)?.state,
            ).toBe("active")
            expect(yield* h.core.pending(root)).toEqual([])
            expect(yield* h.core.incoming(root)).toEqual([])
            expect(yield* readUsage(h.db, child)).toEqual(beforeUsage)
            expect(yield* readProjected(h.db, historical.info.id)).toEqual(beforeHistorical)
            expect(yield* h.db.select().from(DelegationResolutionTable).all()).toEqual([])
            yield* h.db.run("DROP TRIGGER delegation_finalize_test_background_input")

            yield* h.adapter.failInput(registered.generation, inputWorkID, error)
            const source = (yield* h.core.sources(child)).find(
              (item) => item.source.id === `delegation-input:${inputWorkID}`,
            )
            expect(source).toMatchObject({
              source: { kind: "terminal", id: `delegation-input:${inputWorkID}` },
              state: "finalized",
              outcome: "error",
              payload: error,
            })
            expect(
              (yield* h.core.listWork(registered.generation.id)).find((work) => work.id === inputWorkID)?.state,
            ).toBe("finished")
            const afterFinalize = yield* h.core.pending(root)
            expect(afterFinalize).toHaveLength(1)
            expect(afterFinalize[0]).toMatchObject({
              source: { kind: "terminal", id: `delegation-input:${inputWorkID}` },
              payload: error,
              outcome: "error",
            })
            expect(afterFinalize[0]?.messageID).not.toBe(historical.info.id)
            expect(yield* readProjected(h.db, historical.info.id)).toEqual(beforeHistorical)

            if (source === undefined) return yield* Effect.die("expected the finalized background input source")
            yield* h.adapter.failInput(registered.generation, inputWorkID, error)
            expect(yield* h.core.pending(root)).toEqual(afterFinalize)
            expect(yield* h.core.sources(child)).toEqual([source])

            yield* h.adapter.close(child, true)
            expect((yield* h.core.get(registered.generation.id))?.state).toBe("active")
            expect(yield* h.core.pending(root)).toEqual(afterFinalize)
          }),
        [{ id: root }, { id: child, parentID: root }],
      )
    }),
  )

  it.live("keeps reserved sources blocked across close and database reopen", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-close-reopen.sqlite")
      const root = sessionID("close-root")
      const child = sessionID("close-child")

      const recorded = yield* openStore(
        filename,
        (h) =>
          Effect.gen(function* () {
            const registered = yield* createGeneration(h.core, "close", root, child, "background")
            const legacy = withoutUsage(assistantMessage("close-legacy", child))
            yield* seedProjected(h.db, legacy)
            const reserved = yield* h.core.reserveSource({
              sessionID: child,
              generationID: registered.generation.id,
              source: assistantSource("close-legacy"),
              historyCutoff: "history-close-reserved",
              consumed: [],
            })
            const projection = yield* readProjected(h.db, legacy.info.id)

            expect(yield* h.adapter.blocked(child)).toBe(true)
            yield* h.adapter.close(child, true)
            expect((yield* h.core.get(registered.generation.id))?.state).toBe("active")
            expect(yield* h.core.unfinished(child)).toEqual([
              expect.objectContaining({ id: reserved.source.workID, state: "active" }),
            ])
            expect(yield* h.core.pending(root)).toEqual([])
            return {
              generationID: registered.generation.id,
              sourceID: reserved.source.id,
              messageID: legacy.info.id,
              projection,
            }
          }),
        [{ id: root }, { id: child, parentID: root }],
      )

      yield* openStore(filename, (h) =>
        Effect.gen(function* () {
          expect((yield* h.core.get(recorded.generationID))?.state).toBe("active")
          expect((yield* h.core.sources(child)).find((source) => source.id === recorded.sourceID)).toMatchObject({
            state: "reserved",
          })
          expect(yield* readProjected(h.db, recorded.messageID)).toEqual(recorded.projection)
          expect(yield* h.adapter.blocked(child)).toBe(true)
          expect(yield* h.core.unfinished(child)).toEqual([expect.objectContaining({ state: "active" })])
          yield* h.adapter.close(child, true)
          expect((yield* h.core.get(recorded.generationID))?.state).toBe("active")
          expect(yield* h.core.pending(root)).toEqual([])
          expect(yield* readProjected(h.db, recorded.messageID)).toEqual(recorded.projection)
        }),
      )
    }),
  )

  it.live("requires quiescence and Core work or incoming blockers without replaying provider or tool work", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-blockers.sqlite")
      const root = sessionID("blocker-root")
      const receiver = sessionID("blocker-receiver")
      const producer = sessionID("blocker-producer")

      yield* openStore(filename, (h) =>
        Effect.gen(function* () {
          yield* seedSessions(h.db, [
            { id: root },
            { id: receiver, parentID: root },
            { id: producer, parentID: receiver },
          ])
          const receiverGeneration = yield* createGeneration(h.core, "blocker-receiver", root, receiver, "background")
          const providerWork = yield* h.core.startWork({
            id: workID("blocker-provider"),
            generationID: receiverGeneration.generation.id,
            kind: "provider",
          })
          const toolWork = yield* h.core.startWork({
            id: workID("blocker-tool"),
            generationID: receiverGeneration.generation.id,
            kind: "tool",
          })
          const before = yield* h.core.listWork(receiverGeneration.generation.id)

          yield* h.adapter.close(receiver, false)
          expect((yield* h.core.get(receiverGeneration.generation.id))?.state).toBe("active")
          expect(yield* h.core.unfinished(receiver)).toEqual([
            expect.objectContaining({ id: providerWork.id, state: "active" }),
            expect.objectContaining({ id: toolWork.id, state: "active" }),
          ])
          expect(yield* h.core.listWork(receiverGeneration.generation.id)).toEqual(before)

          yield* h.core.finishWork(providerWork.id)
          yield* h.core.finishWork(toolWork.id)
          const producerGeneration = yield* createGeneration(
            h.core,
            "blocker-producer",
            receiver,
            producer,
            "background",
            receiverGeneration.generation.id,
          )
          const source = yield* h.core.reserveSource({
            sessionID: producer,
            generationID: producerGeneration.generation.id,
            source: assistantSource("blocker-incoming"),
            historyCutoff: "history-blocker-incoming",
            consumed: [],
          })
          yield* h.core.finalizeSource(source.source.id, { payload: "incoming", outcome: "reply" })

          const beforeClose = yield* h.core.listWork(receiverGeneration.generation.id)
          const beforeSources = yield* h.core.sources(producer)
          yield* h.adapter.close(receiver, true)
          expect((yield* h.core.get(receiverGeneration.generation.id))?.state).toBe("active")
          expect(yield* h.core.unfinished(receiver)).toEqual([])
          expect(yield* h.core.listWork(receiverGeneration.generation.id)).toEqual(beforeClose)
          expect(yield* h.core.sources(producer)).toEqual(beforeSources)
          expect(yield* h.core.incoming(receiver)).toEqual([expect.objectContaining({ status: "pending" })])
          expect((yield* h.core.get(producerGeneration.generation.id))?.state).toBe("active")
        }),
      )
    }),
  )

  it.live("keeps an unblocked generation active until quiescent close", () =>
    Effect.gen(function* () {
      const temporary = yield* scopedTmpdir
      const filename = path.join(temporary.path, "delegation-finalize-quiescence.sqlite")
      const root = sessionID("quiescence-root")
      const child = sessionID("quiescence-child")

      yield* openStore(
        filename,
        (h) =>
          Effect.gen(function* () {
            const registered = yield* createGeneration(h.core, "quiescence", root, child, "background")
            expect(yield* h.core.unfinished(child)).toEqual([])
            expect(yield* h.adapter.blocked(child)).toBe(false)

            yield* h.adapter.close(child, false)
            expect((yield* h.core.get(registered.generation.id))?.state).toBe("active")
            expect(yield* h.core.pending(root)).toEqual([])
            expect(yield* h.adapter.blocked(child)).toBe(false)

            yield* h.adapter.close(child, true)
            expect((yield* h.core.get(registered.generation.id))?.state).toBe("closed")
            expect(yield* h.core.pending(root)).toEqual([])
          }),
        [{ id: root }, { id: child, parentID: root }],
      )
    }),
  )
})

function sessionID(name: string) {
  return SessionID.make(`ses_delegation_finalize_${name}`)
}

function messageID(name: string) {
  return SessionV1.MessageID.make(`msg_delegation_finalize_${name}`)
}

function partID(name: string) {
  return SessionV1.PartID.make(`prt_delegation_finalize_${name}`)
}

function workID(name: string) {
  return Delegation.WorkID.make(`dwk_delegation_finalize_${name}`)
}

function assistantSource(name: string): Delegation.Source {
  return { kind: "assistant", id: messageID(name) }
}

function registration(name: string, parentID: SessionID, childID: SessionID, options: RegistrationOptions = {}) {
  return {
    requestID: Delegation.RequestID.make(`drq_delegation_finalize_${name}`),
    generationID: Delegation.ID.make(`dlg_delegation_finalize_${name}`),
    parentID,
    childID,
    origin: {
      messageID: SessionMessage.ID.make(`msg_delegation_finalize_origin_${name}`),
      partID: Delegation.OriginPartID.make(`prt_delegation_finalize_origin_${name}`),
      callID: `call_delegation_finalize_${name}`,
    },
    ...(options.parentGenerationID === undefined ? {} : { parentGenerationID: options.parentGenerationID }),
    ...(options.mode === undefined ? {} : { mode: options.mode }),
    explicitReuse: options.explicitReuse ?? false,
  }
}

function assistantMessage(
  name: string,
  sessionID: SessionID,
  error?: SessionV1.Assistant["error"],
): SessionV1.WithParts {
  const id = messageID(name)
  const parentID = messageID(`parent-${name}`)
  const step = {
    id: partID(`m-step-${name}`),
    sessionID,
    messageID: id,
    type: "step-finish" as const,
    reason: "stop",
    cost: usage.cost,
    tokens: usage.tokens,
  }
  const snapshot = {
    id: partID(`a-snapshot-${name}`),
    sessionID,
    messageID: id,
    type: "snapshot" as const,
    snapshot: `snapshot-${name}`,
  }
  const text = {
    id: partID(`z-text-${name}`),
    sessionID,
    messageID: id,
    type: "text" as const,
    text: `assistant-${name}`,
  }
  const info: SessionV1.Assistant = {
    id,
    sessionID,
    role: "assistant",
    time: { created: 100, completed: 200 },
    ...(error === undefined ? {} : { error }),
    parentID,
    modelID,
    providerID,
    mode: "build",
    agent: "build",
    path: { cwd: "/delegation-finalize", root: "/delegation-finalize" },
    cost: usage.cost,
    tokens: usage.tokens,
    structured: { z: "last", a: { y: 2, x: 1 } },
    finish: "stop",
  }
  return { info, parts: [text, step, snapshot] }
}

function emptyOutcomeMessage(name: string, sessionID: SessionID) {
  const message = withoutUsage(assistantMessage(name, sessionID, new SessionV1.OutputLengthError({}).toObject()))
  if (message.info.role === "assistant") delete message.info.structured
  const text = message.parts.find((part) => part.type === "text")
  if (text?.type === "text") text.text = ""
  return message
}

function withoutUsage(message: SessionV1.WithParts) {
  if (message.info.role === "assistant") {
    message.info.cost = 0
    message.info.tokens = {
      total: 0,
      input: 0,
      output: 0,
      reasoning: 0,
      cache: { read: 0, write: 0 },
    }
  }
  message.parts = message.parts.filter((part) => part.type !== "step-finish")
  return message
}

function textPart(id: SessionV1.PartID, sessionID: SessionID, messageID: SessionV1.MessageID, text: string) {
  return { id, sessionID, messageID, type: "text" as const, text }
}

function reorderedMessage(message: SessionV1.WithParts): SessionV1.WithParts {
  if (message.info.role !== "assistant") return message
  return {
    info: {
      ...message.info,
      structured: { a: { x: 1, y: 2 }, z: "last" },
    },
    parts: [...message.parts].reverse(),
  }
}

function changedMessage(message: SessionV1.WithParts): SessionV1.WithParts {
  if (message.info.role !== "assistant") return message
  return {
    info: {
      ...message.info,
      structured: { a: { x: 1, y: 3 }, z: "changed" },
    },
    parts: message.parts,
  }
}

function abortedError(message: string): SessionV1.Assistant["error"] {
  return new SessionV1.AbortedError({ message }).toObject()
}

function messageData(info: SessionV1.Info) {
  const { id, sessionID, ...data } = info
  return data
}

function partData(part: SessionV1.Part) {
  const { id, sessionID, messageID, ...data } = part
  return data
}

function seedSessions(db: DB, sessions: readonly SessionFixture[]) {
  return Effect.gen(function* () {
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/delegation-finalize"), sandboxes: [] })
      .run()
    yield* db
      .insert(SessionTable)
      .values(
        sessions.map((session) => ({
          id: session.id,
          project_id: Project.ID.global,
          parent_id: session.parentID ?? null,
          slug: session.id,
          directory: "/delegation-finalize",
          title: session.id,
          version: "test",
        })),
      )
      .run()
  })
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
    const registered = yield* core.register(
      registration(name, parentID, childID, { mode, ...(parentGenerationID ? { parentGenerationID } : {}) }),
    )
    yield* core.finishWork(registered.workID)
    return registered
  })
}

function seedProjected(db: DB, message: SessionV1.WithParts, parts: readonly SessionV1.Part[] = message.parts) {
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
        parts.map((part) => ({
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

function writeUsage(db: DB, sessionID: SessionID, value = usage) {
  return db
    .update(SessionTable)
    .set({
      cost: value.cost,
      tokens_input: value.tokens.input,
      tokens_output: value.tokens.output,
      tokens_reasoning: value.tokens.reasoning,
      tokens_cache_read: value.tokens.cache.read,
      tokens_cache_write: value.tokens.cache.write,
    })
    .where(eq(SessionTable.id, sessionID))
    .run()
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

function openStore<A, E>(
  filename: string,
  fn: (harness: Harness) => Effect.Effect<A, E>,
  sessions?: readonly SessionFixture[],
) {
  return Effect.gen(function* () {
    const database = yield* Database.Service
    const db = database.db
    if (sessions !== undefined) yield* seedSessions(db, sessions)
    const core = DelegationStore.make(db)
    const adapter = SessionDelegation.make(db, core)
    return yield* fn({ db, core, adapter })
  }).pipe(Effect.scoped, Effect.provide(Database.layerFromPath(filename)))
}

function expectFailure<A, E>(exit: Exit.Exit<A, E>) {
  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeDefined()
}

function expectFailureMessage<A, E>(exit: Exit.Exit<A, E>, message: string) {
  expectFailure(exit)
  if (Exit.isFailure(exit)) expect(Cause.pretty(exit.cause)).toContain(message)
}
