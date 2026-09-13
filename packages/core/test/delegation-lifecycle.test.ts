import { describe, expect, test } from "bun:test"
import { eq, sql } from "drizzle-orm"
import { Cause, Effect, Exit, Schema } from "effect"
import path from "path"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { DelegationGenerationTable, DelegationRevocationTable } from "@opencode-ai/core/delegation/sql"
import { Database } from "@opencode-ai/core/database/database"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { SessionID } from "@opencode-ai/schema/session-id"
import { testEffect } from "./lib/effect"
import { tmpdir } from "./fixture/tmpdir"

const it = testEffect(Database.layerFromPath(":memory:"))

type SessionFixture = {
  readonly id: SessionID
  readonly parentID?: SessionID
}

type RegistrationOptions = {
  readonly explicitReuse?: boolean
  readonly mode?: Delegation.Mode
  readonly parentGenerationID?: Delegation.ID
  readonly origin?: Delegation.Origin
}

const setup = (...sessions: readonly SessionFixture[]) =>
  Effect.gen(function* () {
    const database = yield* Database.Service
    const db = database.db

    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .run()
      .pipe(Effect.orDie)
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
      .pipe(Effect.orDie)
    yield* db
      .run(
        `CREATE TABLE delegation_test_receipt (
          id TEXT PRIMARY KEY,
          envelope TEXT NOT NULL
        )`,
      )
      .pipe(Effect.orDie)

    return { db, store: DelegationStore.make(db) }
  })

function sessionID(name: string) {
  return SessionID.make(`ses_delegation_${name}`)
}

function origin(name: string): Delegation.Origin {
  return {
    messageID: SessionMessage.ID.make(`msg_delegation_${name}`),
    partID: Delegation.OriginPartID.make(`part_delegation_${name}`),
    callID: `call_delegation_${name}`,
  }
}

function registration(
  name: string,
  parentID: SessionID,
  childID: SessionID,
  options: RegistrationOptions = {},
) {
  return {
    requestID: Delegation.RequestID.create(),
    generationID: Delegation.ID.create(),
    parentID,
    childID,
    origin: options.origin ?? origin(name),
    ...(options.parentGenerationID === undefined ? {} : { parentGenerationID: options.parentGenerationID }),
    ...(options.mode === undefined ? {} : { mode: options.mode }),
    explicitReuse: options.explicitReuse ?? false,
  }
}

function assistantSource(name: string): Delegation.Source {
  return { kind: "assistant", id: `assistant-delegation-${name}` }
}

function close(store: DelegationStore.Interface, id: Delegation.ID) {
  return store.reconcileAndClose(id, () => Effect.succeed({ quiescent: true, sources: [] }))
}

function expectFailure<A, E>(exit: Exit.Exit<A, E>) {
  expect(Exit.isFailure(exit)).toBe(true)
}

function expectFailureCode<A, E>(exit: Exit.Exit<A, E>, code: string) {
  expectFailure(exit)
  if (Exit.isFailure(exit)) expect(errorCode(Cause.squash(exit.cause))).toBe(code)
}

function errorCode(value: unknown) {
  if (typeof value !== "object" || value === null || !("code" in value) || typeof value.code !== "string") {
    return undefined
  }
  return value.code
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

function admitPrepared(store: DelegationStore.Interface, id: Delegation.ResolutionID) {
  return store.admit(id, (tx, resolution) =>
    Effect.gen(function* () {
      yield* tx.run(
        sql`INSERT INTO delegation_test_receipt (id, envelope) VALUES (${resolution.messageID}, ${JSON.stringify(resolution.envelope)})`,
      )
      return { status: "admitted" as const }
    }),
  )
}

function runDatabase<A, E>(filename: string, effect: Effect.Effect<A, E, Database.Service>) {
  return Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(Database.layerFromPath(filename))))
}

function revokeClosedOutgoingReturn(status: "admitted" | "consumed") {
  return Effect.gen(function* () {
    const root = sessionID(`closed-return-${status}-root`)
    const parent = sessionID(`closed-return-${status}-parent`)
    const child = sessionID(`closed-return-${status}-child`)
    const { db, store } = yield* setup(
      { id: root },
      { id: parent, parentID: root },
      { id: child, parentID: parent },
    )

    const parentGeneration = yield* store.register(
      registration(`closed-return-${status}-parent`, root, parent, { mode: "background" }),
    )
    yield* store.finishWork(parentGeneration.workID)
    const childGeneration = yield* store.register(
      registration(`closed-return-${status}-child`, parent, child, {
        mode: "background",
        parentGenerationID: parentGeneration.generation.id,
      }),
    )
    yield* store.finishWork(childGeneration.workID)

    const source = yield* store.reserveSource({
      sessionID: child,
      generationID: childGeneration.generation.id,
      source: assistantSource(`closed-return-${status}`),
      historyCutoff: `history-closed-return-${status}`,
      consumed: [],
    })
    const finalized = yield* store.finalizeSource(source.source.id, {
      payload: `closed return ${status}`,
      outcome: "reply",
    })
    if (finalized.resolution === undefined) return yield* Effect.die("expected a closed-generation return")
    expect(finalized.resolution.parentID).toBe(parent)
    expect(finalized.resolution.recipientGenerationID).toBe(parentGeneration.generation.id)
    const prepared = yield* store.prepare(finalized.resolution.id, content(`closed-return-${status}`))
    expect(yield* admitPrepared(store, prepared.id)).toEqual({ status: "admitted" })
    if (status === "consumed") yield* store.markConsumed({ parentID: parent, ids: [prepared.id] })
    expect((yield* store.getResolution(prepared.id))?.status).toBe(status)
    expect(yield* store.unfinished(parent)).toEqual([])
    expect(yield* store.unfinished(child)).toEqual([])

    expect((yield* close(store, childGeneration.generation.id)).closed).toBe(true)
    const revoked = yield* store.revokeDescendants(root)
    expect((yield* store.get(parentGeneration.generation.id))?.state).toBe("revoked")
    expect((yield* store.get(childGeneration.generation.id))?.state).toBe("closed")
    expect(revoked.sessionIDs).toEqual([parent])
    expect(revoked.sessionIDs).not.toContain(child)
    expect(
      yield* db
        .select()
        .from(DelegationRevocationTable)
        .where(eq(DelegationRevocationTable.session_id, child))
        .get(),
    ).toBeUndefined()
    expect((yield* store.getResolution(prepared.id))?.status).toBe("revoked")

    let receiverCalled = false
    const oldReceipt = yield* store.admit(prepared.id, () =>
      Effect.sync(() => {
        receiverCalled = true
        return { status: "admitted" as const }
      }),
    )
    expect(oldReceipt).toEqual({ status: "revoked" })
    expect(receiverCalled).toBe(false)
    expect(yield* db.all(`SELECT id, envelope FROM delegation_test_receipt`)).toHaveLength(1)

    yield* store.allowSession(root)
    yield* store.allowSession(parent)
    const fresh = yield* store.register(
      registration(`closed-return-${status}-fresh`, root, parent, {
        explicitReuse: true,
        mode: "foreground",
      }),
    )
    expect(fresh.generation.id).not.toBe(parentGeneration.generation.id)
    expect(fresh.generation.mode).toBe("foreground")
  })
}

describe("Delegation schema", () => {
  test("uses exact durable ID prefixes and omits optional registration fields", () => {
    expect(Delegation.ID.create()).toMatch(/^dlg_/)
    expect(Delegation.ResolutionID.create()).toMatch(/^res_/)
    expect(Delegation.WorkID.create()).toMatch(/^dwk_/)
    expect(Delegation.RequestID.create()).toMatch(/^drq_/)
    expect(Delegation.SourceID.create()).toMatch(/^dsrc_/)
    expect(Delegation.RevocationID.create()).toMatch(/^drvk_/)

    expect(() => Schema.decodeUnknownSync(Delegation.ID)("dlgXinvalid")).toThrow()
    expect(() => Schema.decodeUnknownSync(Delegation.WorkID)("dwkXinvalid")).toThrow()
    expect(() => Schema.decodeUnknownSync(Delegation.RequestID)("drqXinvalid")).toThrow()
    expect(() => Schema.decodeUnknownSync(Delegation.SourceID)("dsrcXinvalid")).toThrow()
    expect(() => Schema.decodeUnknownSync(Delegation.RevocationID)("drvkXinvalid")).toThrow()

    const registration = {
      requestID: Delegation.RequestID.create(),
      generationID: Delegation.ID.create(),
      parentID: SessionID.make("ses_schema_parent"),
      childID: SessionID.make("ses_schema_child"),
      origin: origin("schema"),
      explicitReuse: false,
    }
    expect(Schema.decodeUnknownSync(Delegation.Registration)(registration)).toEqual(registration)
    const encoded = Schema.encodeSync(Delegation.Registration)(registration)
    expect(Object.hasOwn(encoded, "mode")).toBe(false)
    expect(Object.hasOwn(encoded, "parentGenerationID")).toBe(false)
  })
})

describe("DelegationStore lifecycle", () => {
  it.effect("defaults omitted launches to foreground, including under a background parent", () =>
    Effect.gen(function* () {
      const root = sessionID("omitted-root")
      const backgroundParent = sessionID("omitted-parent")
      const child = sessionID("omitted-child")
      const { store } = yield* setup(
        { id: root },
        { id: backgroundParent, parentID: root },
        { id: child, parentID: backgroundParent },
      )

      const parent = yield* store.register(
        registration("omitted-parent", root, backgroundParent, { mode: "background" }),
      )
      yield* store.finishWork(parent.workID)

      const delegated = yield* store.register(
        registration("omitted-child", backgroundParent, child, {
          parentGenerationID: parent.generation.id,
        }),
      )
      expect(delegated.generation.mode).toBe("foreground")
      expect(delegated.generation.parentGenerationID).toBe(parent.generation.id)
      expect((yield* store.active(child))?.id).toBe(delegated.generation.id)
    }),
  )

  it.effect("enforces the root, foreground-parent, and background-parent mode matrix", () =>
    Effect.gen(function* () {
      const root = sessionID("matrix-root")
      const rootForegroundChild = sessionID("matrix-root-fg")
      const rootBackgroundChild = sessionID("matrix-root-bg")
      const foregroundParent = sessionID("matrix-f-parent")
      const foregroundChild = sessionID("matrix-f-child")
      const foregroundBackgroundChild = sessionID("matrix-f-bg")
      const backgroundParent = sessionID("matrix-b-parent")
      const backgroundChild = sessionID("matrix-b-child")
      const backgroundBackgroundChild = sessionID("matrix-b-bg")
      const { store } = yield* setup(
        { id: root },
        { id: rootForegroundChild, parentID: root },
        { id: rootBackgroundChild, parentID: root },
        { id: foregroundParent, parentID: root },
        { id: foregroundChild, parentID: foregroundParent },
        { id: foregroundBackgroundChild, parentID: foregroundParent },
        { id: backgroundParent, parentID: root },
        { id: backgroundChild, parentID: backgroundParent },
        { id: backgroundBackgroundChild, parentID: backgroundParent },
      )

      const rootForeground = yield* store.register(registration("matrix-root-fg", root, rootForegroundChild))
      const rootBackground = yield* store.register(
        registration("matrix-root-bg", root, rootBackgroundChild, { mode: "background" }),
      )
      const foreground = yield* store.register(registration("matrix-f-parent", root, foregroundParent))
      const background = yield* store.register(
        registration("matrix-b-parent", root, backgroundParent, { mode: "background" }),
      )

      const foregroundChildResult = yield* store.register(
        registration("matrix-f-child", foregroundParent, foregroundChild, {
          parentGenerationID: foreground.generation.id,
        }),
      )
      const foregroundBackground = yield* store
        .register(
          registration("matrix-f-bg", foregroundParent, foregroundBackgroundChild, {
            parentGenerationID: foreground.generation.id,
            mode: "background",
          }),
        )
        .pipe(Effect.exit)

      expect(rootForeground.generation.mode).toBe("foreground")
      expect(rootBackground.generation.mode).toBe("background")
      expect(foregroundChildResult.generation.mode).toBe("foreground")
      expectFailure(foregroundBackground)

      const backgroundChildResult = yield* store.register(
        registration("matrix-b-child", backgroundParent, backgroundChild, {
          parentGenerationID: background.generation.id,
          mode: "foreground",
        }),
      )
      const backgroundBackground = yield* store.register(
        registration("matrix-b-bg", backgroundParent, backgroundBackgroundChild, {
          parentGenerationID: background.generation.id,
          mode: "background",
        }),
      )
      expect(backgroundChildResult.generation.mode).toBe("foreground")
      expect(backgroundBackground.generation.mode).toBe("background")
    }),
  )

  it.effect("retains active reuse mode, origin, and ancestry while reserving update work", () =>
    Effect.gen(function* () {
      const root = sessionID("reuse-root")
      const parent = sessionID("reuse-parent")
      const child = sessionID("reuse-child")
      const { store } = yield* setup({ id: root }, { id: parent, parentID: root }, { id: child, parentID: parent })

      const parentGeneration = yield* store.register(
        registration("reuse-parent", root, parent, { mode: "background" }),
      )
      yield* store.finishWork(parentGeneration.workID)
      const original = yield* store.register(
        registration("reuse-original", parent, child, {
          parentGenerationID: parentGeneration.generation.id,
          mode: "foreground",
        }),
      )

      const update = yield* store.register(
        registration("reuse-update", parent, child, {
          parentGenerationID: parentGeneration.generation.id,
          explicitReuse: true,
          origin: origin("reuse-different-origin"),
        }),
      )
      expect(update.generation.id).toBe(original.generation.id)
      expect(update.generation.mode).toBe(original.generation.mode)
      expect(update.generation.origin).toEqual(original.generation.origin)
      expect(update.generation.parentGenerationID).toBe(parentGeneration.generation.id)
      expect(update.workID).not.toBe(original.workID)
      expect(update.workState).toBe("active")

      const contradictory = yield* store
        .register(
          registration("reuse-contradictory", parent, child, {
            parentGenerationID: parentGeneration.generation.id,
            explicitReuse: true,
            mode: "background",
          }),
        )
        .pipe(Effect.exit)
      expectFailure(contradictory)

      const implicitUpdate = yield* store
        .register(
          registration("reuse-implicit", parent, child, {
            parentGenerationID: parentGeneration.generation.id,
          }),
        )
        .pipe(Effect.exit)
      expectFailure(implicitUpdate)

      const blocked = yield* close(store, original.generation.id)
      expect(blocked.closed).toBe(false)
      yield* store.finishWork(original.workID)
      yield* store.finishWork(update.workID)
      expect((yield* close(store, original.generation.id)).closed).toBe(true)
    }),
  )

  it.effect("rejects cross-parent reuse and only adopts historical children under their recorded parent", () =>
    Effect.gen(function* () {
      const root = sessionID("parent-root")
      const parentA = sessionID("parent-a")
      const parentB = sessionID("parent-b")
      const activeChild = sessionID("parent-active-child")
      const historicalChild = sessionID("parent-historical-child")
      const unregisteredHistoricalChild = sessionID("parent-unregistered-historical-child")
      const { store } = yield* setup(
        { id: root },
        { id: parentA, parentID: root },
        { id: parentB, parentID: root },
        { id: activeChild, parentID: parentA },
        { id: historicalChild, parentID: parentA },
        { id: unregisteredHistoricalChild, parentID: parentA },
      )

      const activeParent = yield* store.register(registration("parent-a", root, parentA))
      const active = yield* store.register(
        registration("parent-active", parentA, activeChild, { parentGenerationID: activeParent.generation.id }),
      )
      const historical = yield* store.register(
        registration("parent-historical", parentA, historicalChild, {
          explicitReuse: true,
          parentGenerationID: activeParent.generation.id,
        }),
      )
      expect(historical.generation.state).toBe("active")

      const activeCrossParent = yield* store
        .register(registration("parent-active-cross", parentB, activeChild, { explicitReuse: true }))
        .pipe(Effect.exit)
      const historicalCrossParent = yield* store
        .register(registration("parent-historical-cross", parentB, historicalChild, { explicitReuse: true }))
        .pipe(Effect.exit)
      const unregisteredHistoricalCrossParent = yield* store
        .register(
          registration("parent-unregistered-historical-cross", parentB, unregisteredHistoricalChild, {
            explicitReuse: true,
          }),
        )
        .pipe(Effect.exit)
      expectFailure(activeCrossParent)
      expectFailure(historicalCrossParent)
      expectFailure(unregisteredHistoricalCrossParent)
      expect((yield* store.active(activeChild))?.id).toBe(active.generation.id)
      const adopted = yield* store.register(
        registration("parent-unregistered-historical", parentA, unregisteredHistoricalChild, {
          explicitReuse: true,
          parentGenerationID: activeParent.generation.id,
        }),
      )
      expect(adopted.generation.parentID).toBe(parentA)
    }),
  )

  it.effect("blocks new root-like generation adoption while a root source reservation is unresolved", () =>
    Effect.gen(function* () {
      const root = sessionID("new-owner-source-root")
      const child = sessionID("new-owner-source-child")
      const { store } = yield* setup({ id: root }, { id: child, parentID: root })
      const historical = yield* store.register(registration("new-owner-source-history", root, child))
      yield* store.finishWork(historical.workID)
      expect((yield* close(store, historical.generation.id)).closed).toBe(true)

      const source = yield* store.reserveSource({
        sessionID: child,
        source: assistantSource("new-owner-root-source"),
        historyCutoff: "history-new-owner-root-source",
        consumed: [],
      })
      yield* store.finishWork(source.source.workID)
      expect(yield* store.unfinished(child)).toEqual([])

      const blocked = yield* store
        .register(
          registration("new-owner-source-blocked", root, child, {
            explicitReuse: true,
            mode: "foreground",
          }),
        )
        .pipe(Effect.exit)
      expectFailureCode(blocked, "root_source_unresolved")

      const finalized = yield* store.finalizeSource(source.source.id, {
        payload: "settled root source",
        outcome: "reply",
      })
      expect(finalized.resolution).toBeUndefined()

      const fresh = yield* store.register(
        registration("new-owner-source-settled", root, child, {
          explicitReuse: true,
          mode: "foreground",
        }),
      )
      expect(fresh.generation.id).not.toBe(historical.generation.id)
      expect(fresh.generation.mode).toBe("foreground")
    }),
  )

  it.effect("blocks root-like adoption while an outgoing background generation is active", () =>
    Effect.gen(function* () {
      const root = sessionID("new-owner-outgoing-root")
      const child = sessionID("new-owner-outgoing-child")
      const grandchild = sessionID("new-owner-outgoing-grandchild")
      const { store } = yield* setup(
        { id: root },
        { id: child, parentID: root },
        { id: grandchild, parentID: child },
      )
      const historical = yield* store.register(registration("new-owner-outgoing-history", root, child))
      yield* store.finishWork(historical.workID)
      expect((yield* close(store, historical.generation.id)).closed).toBe(true)

      const outgoing = yield* store.register(
        registration("new-owner-outgoing", child, grandchild, { mode: "background" }),
      )
      yield* store.finishWork(outgoing.workID)

      const blocked = yield* store
        .register(
          registration("new-owner-outgoing-blocked", root, child, {
            explicitReuse: true,
            mode: "foreground",
          }),
        )
        .pipe(Effect.exit)
      expectFailureCode(blocked, "active_outgoing_generation")

      expect((yield* close(store, outgoing.generation.id)).closed).toBe(true)
      const fresh = yield* store.register(
        registration("new-owner-outgoing-settled", root, child, {
          explicitReuse: true,
          mode: "foreground",
        }),
      )
      expect(fresh.generation.id).not.toBe(historical.generation.id)
      expect(fresh.generation.mode).toBe("foreground")
    }),
  )

  it.effect("blocks root-like adoption for pending, admitted, and consumed incoming returns", () =>
    Effect.gen(function* () {
      const root = sessionID("new-owner-incoming-root")
      const statuses = ["pending", "admitted", "consumed"] as const
      const sessions = statuses.flatMap((status) => [
        { id: sessionID(`new-owner-${status}-child`), parentID: root },
        {
          id: sessionID(`new-owner-${status}-producer`),
          parentID: sessionID(`new-owner-${status}-child`),
        },
      ])
      const { db, store } = yield* setup({ id: root }, ...sessions)

      yield* Effect.forEach(statuses, (status) =>
        Effect.gen(function* () {
          const child = sessionID(`new-owner-${status}-child`)
          const producer = sessionID(`new-owner-${status}-producer`)
          const historical = yield* store.register(registration(`new-owner-${status}-history`, root, child))
          yield* store.finishWork(historical.workID)
          expect((yield* close(store, historical.generation.id)).closed).toBe(true)

          const outgoing = yield* store.register(
            registration(`new-owner-${status}-outgoing`, child, producer, { mode: "background" }),
          )
          yield* store.finishWork(outgoing.workID)
          const source = yield* store.reserveSource({
            sessionID: producer,
            generationID: outgoing.generation.id,
            source: assistantSource(`new-owner-${status}-return`),
            historyCutoff: `history-new-owner-${status}-return`,
            consumed: [],
          })
          const finalized = yield* store.finalizeSource(source.source.id, {
            payload: `new-owner-${status}-payload`,
            outcome: "reply",
          })
          if (finalized.resolution === undefined) return yield* Effect.die("expected an incoming return")
          expect(finalized.resolution.parentID).toBe(child)
          expect(finalized.resolution.recipientGenerationID).toBeUndefined()

          const prepared = yield* store.prepare(finalized.resolution.id, content(`new-owner-${status}-return`))
          if (status !== "pending") expect(yield* admitPrepared(store, prepared.id)).toEqual({ status: "admitted" })
          if (status === "consumed") yield* store.markConsumed({ parentID: child, ids: [prepared.id] })

          if (status === "consumed") {
            expect((yield* close(store, outgoing.generation.id)).closed).toBe(true)
          } else {
            // Pending/admitted returns normally keep their outgoing generation open; seed the durable historical state.
            yield* db
              .update(DelegationGenerationTable)
              .set({ state: "closed", time_closed: Date.now() })
              .where(eq(DelegationGenerationTable.id, outgoing.generation.id))
              .run()
              .pipe(Effect.orDie)
          }

          const blocked = yield* store
            .register(
              registration(`new-owner-${status}-blocked`, root, child, {
                explicitReuse: true,
                mode: "foreground",
              }),
            )
            .pipe(Effect.exit)
          expectFailureCode(blocked, "incoming_resolution_unclaimed")

          if (status === "pending") expect(yield* admitPrepared(store, prepared.id)).toEqual({ status: "admitted" })
          if (status !== "consumed") yield* store.markConsumed({ parentID: child, ids: [prepared.id] })
          const rootSource = yield* store.reserveSource({
            sessionID: child,
            source: assistantSource(`new-owner-${status}-settle`),
            historyCutoff: `history-new-owner-${status}-settle`,
            consumed: [prepared.id],
          })
          const settled = yield* store.finalizeSource(rootSource.source.id, {
            payload: `new-owner-${status}-settled`,
            outcome: "reply",
          })
          expect(settled.resolution).toBeUndefined()
          expect((yield* store.getResolution(prepared.id))?.status).toBe("resolved")

          const fresh = yield* store.register(
            registration(`new-owner-${status}-settled`, root, child, {
              explicitReuse: true,
              mode: "foreground",
            }),
          )
          expect(fresh.generation.id).not.toBe(historical.generation.id)
        }),
      )
    }),
  )

  it.effect("retries a request by generation and finished work across closure, while closed reuse gets a new ID", () =>
    Effect.gen(function* () {
      const root = sessionID("retry-root")
      const child = sessionID("retry-child")
      const otherChild = sessionID("retry-other-child")
      const { store } = yield* setup(
        { id: root },
        { id: child, parentID: root },
        { id: otherChild, parentID: root },
      )

      const request = registration("retry-original", root, child)
      const first = yield* store.register(request)
      const beforeClosure = yield* store.register(request)
      expect(beforeClosure.generation.id).toBe(first.generation.id)
      expect(beforeClosure.workID).toBe(first.workID)
      expect(beforeClosure.workState).toBe("active")

      yield* store.finishWork(first.workID)
      const closed = yield* close(store, first.generation.id)
      expect(closed.closed).toBe(true)
      expect(closed.generation.state).toBe("closed")

      const afterClosure = yield* store.register(request)
      expect(afterClosure.generation.id).toBe(first.generation.id)
      expect(afterClosure.workID).toBe(first.workID)
      expect(afterClosure.workState).toBe("finished")
      expect(yield* store.active(child)).toBeUndefined()
      expect((yield* store.listWork(first.generation.id))[0]?.state).toBe("finished")

      const changed = yield* store
        .register({ ...request, origin: { ...request.origin, callID: "changed-request" } })
        .pipe(Effect.exit)
      expectFailure(changed)

      const freshRequest = registration("retry-fresh", root, child, { explicitReuse: true })
      const fresh = yield* store.register(freshRequest)
      expect(fresh.generation.id).not.toBe(first.generation.id)
      expect(fresh.generation.state).toBe("active")
      expect(fresh.workState).toBe("active")

      const duplicateGeneration = yield* store
        .register({
          ...registration("retry-duplicate-generation", root, otherChild),
          generationID: first.generation.id,
        })
        .pipe(Effect.exit)
      expectFailure(duplicateGeneration)

      let staleCallbackCalled = false
      const stale = yield* store
        .reconcileAndClose(first.generation.id, () =>
          Effect.sync(() => {
            staleCallbackCalled = true
            return { quiescent: true, sources: [] }
          }),
        )
        .pipe(Effect.exit)
      expect(staleCallbackCalled).toBe(false)
      if (Exit.isSuccess(stale)) expect(stale.value.generation.state).toBe("closed")
      expect((yield* store.active(child))?.id).toBe(fresh.generation.id)

      const oldWork = yield* store
        .startWork({
          id: Delegation.WorkID.create(),
          sessionID: child,
          generationID: first.generation.id,
          kind: "runtime",
        })
        .pipe(Effect.exit)
      expectFailure(oldWork)
    }),
  )

  it.effect("serializes concurrent same-child registration and rejects a different parent", () =>
    Effect.gen(function* () {
      const root = sessionID("race-root")
      const otherParent = sessionID("race-other-parent")
      const sameChild = sessionID("race-same-child")
      const differentChild = sessionID("race-different-child")
      const { store } = yield* setup(
        { id: root },
        { id: otherParent, parentID: root },
        { id: sameChild, parentID: root },
        { id: differentChild, parentID: root },
      )

      const sameParentResults = yield* Effect.all(
        [
          store.register(registration("race-same-a", root, sameChild, { explicitReuse: true })),
          store.register(registration("race-same-b", root, sameChild, { explicitReuse: true })),
        ],
        { concurrency: "unbounded" },
      )
      expect(sameParentResults[0].generation.id).toBe(sameParentResults[1].generation.id)
      expect(sameParentResults[0].workID).not.toBe(sameParentResults[1].workID)
      expect((yield* store.listActive()).filter((generation) => generation.childID === sameChild)).toHaveLength(1)

      const differentParentResults = yield* Effect.all(
        [
          store.register(registration("race-different-root", root, differentChild, { explicitReuse: true })),
          store.register(registration("race-different-parent", otherParent, differentChild, { explicitReuse: true })),
        ].map((effect) => effect.pipe(Effect.exit)),
        { concurrency: "unbounded" },
      )
      expect(differentParentResults.filter(Exit.isSuccess)).toHaveLength(1)
      expect(differentParentResults.filter(Exit.isFailure)).toHaveLength(1)
    }),
  )

  it.effect("lists active work, blocks closure, and never reactivates a finished token", () =>
    Effect.gen(function* () {
      const root = sessionID("work-root")
      const child = sessionID("work-child")
      const otherChild = sessionID("work-other-child")
      const { store } = yield* setup(
        { id: root },
        { id: child, parentID: root },
        { id: otherChild, parentID: root },
      )

      const first = yield* store.register(registration("work-first", root, child))
      const second = yield* store.register(registration("work-second", root, otherChild))
      const runtimeID = Delegation.WorkID.create()
      const runtime = yield* store.startWork({
        id: runtimeID,
        sessionID: child,
        generationID: first.generation.id,
        kind: "runtime",
      })
      expect(runtime.sessionID).toBe(child)
      expect(runtime.generationID).toBe(first.generation.id)
      expect((yield* store.listActive()).map((generation) => generation.id)).toEqual([
        first.generation.id,
        second.generation.id,
      ])
      expect((yield* store.listWork(first.generation.id)).map((work) => work.id)).toEqual([
        first.workID,
        runtimeID,
      ])
      expect((yield* store.unfinished(child)).map((work) => work.id)).toEqual([first.workID, runtimeID])

      const blocked = yield* close(store, first.generation.id)
      expect(blocked.closed).toBe(false)
      expect(blocked.blockers.length).toBeGreaterThan(0)

      yield* store.finishWork(first.workID)
      yield* store.finishWork(runtimeID)
      yield* store.finishWork(runtimeID)
      const retry = yield* store.startWork({
        id: runtimeID,
        sessionID: child,
        generationID: first.generation.id,
        kind: "runtime",
      })
      expect(retry.state).toBe("finished")
      expect(yield* store.unfinished(child)).toEqual([])

      const conflictingOwner = yield* store
        .startWork({ id: runtimeID, sessionID: otherChild, generationID: second.generation.id, kind: "runtime" })
        .pipe(Effect.exit)
      expectFailure(conflictingOwner)

      const closed = yield* close(store, first.generation.id)
      expect(closed.closed).toBe(true)
    }),
  )

  it.effect("promotes only when the parent mode and child fence allow it", () =>
    Effect.gen(function* () {
      const root = sessionID("promote-root")
      const foregroundParent = sessionID("promote-f-parent")
      const foregroundChild = sessionID("promote-f-child")
      const backgroundParent = sessionID("promote-b-parent")
      const backgroundChild = sessionID("promote-b-child")
      const rootChild = sessionID("promote-root-child")
      const fencedChild = sessionID("promote-fenced-child")
      const { store } = yield* setup(
        { id: root },
        { id: foregroundParent, parentID: root },
        { id: foregroundChild, parentID: foregroundParent },
        { id: backgroundParent, parentID: root },
        { id: backgroundChild, parentID: backgroundParent },
        { id: rootChild, parentID: root },
        { id: fencedChild, parentID: root },
      )

      const foreground = yield* store.register(registration("promote-f-parent", root, foregroundParent))
      const foregroundChildGeneration = yield* store.register(
        registration("promote-f-child", foregroundParent, foregroundChild, {
          parentGenerationID: foreground.generation.id,
        }),
      )
      const foregroundPromotion = yield* store.promote(foregroundChildGeneration.generation.id).pipe(Effect.exit)
      expectFailure(foregroundPromotion)
      expect((yield* store.get(foregroundChildGeneration.generation.id))?.mode).toBe("foreground")

      const background = yield* store.register(
        registration("promote-b-parent", root, backgroundParent, { mode: "background" }),
      )
      const backgroundChildGeneration = yield* store.register(
        registration("promote-b-child", backgroundParent, backgroundChild, {
          parentGenerationID: background.generation.id,
        }),
      )
      yield* store.promote(backgroundChildGeneration.generation.id)
      expect((yield* store.get(backgroundChildGeneration.generation.id))?.mode).toBe("background")

      const rootChildGeneration = yield* store.register(registration("promote-root-child", root, rootChild))
      yield* store.promote(rootChildGeneration.generation.id)
      expect((yield* store.get(rootChildGeneration.generation.id))?.mode).toBe("background")

      const fenced = yield* store.register(registration("promote-fenced-child", root, fencedChild))
      yield* store.revokeDescendants(fencedChild)
      const fencedPromotion = yield* store.promote(fenced.generation.id).pipe(Effect.exit)
      expectFailure(fencedPromotion)
      expect((yield* store.get(fenced.generation.id))?.mode).toBe("foreground")
    }),
  )

  it.effect("keeps an active descendant blocking its idle parent after the launch work finishes", () =>
    Effect.gen(function* () {
      const root = sessionID("descendant-root")
      const intermediate = sessionID("descendant-intermediate")
      const grandchild = sessionID("descendant-grandchild")
      const { store } = yield* setup(
        { id: root },
        { id: intermediate, parentID: root },
        { id: grandchild, parentID: intermediate },
      )

      const intermediateGeneration = yield* store.register(
        registration("descendant-intermediate", root, intermediate, { mode: "background" }),
      )
      yield* store.finishWork(intermediateGeneration.workID)
      const grandchildGeneration = yield* store.register(
        registration("descendant-grandchild", intermediate, grandchild, {
          parentGenerationID: intermediateGeneration.generation.id,
          mode: "background",
        }),
      )

      const blocked = yield* close(store, intermediateGeneration.generation.id)
      expect(blocked.closed).toBe(false)
      expect(blocked.blockers).toContain(`descendant:${grandchildGeneration.generation.id}`)
      expect((yield* store.get(intermediateGeneration.generation.id))?.state).toBe("active")

      yield* store.finishWork(grandchildGeneration.workID)
      expect((yield* close(store, grandchildGeneration.generation.id)).closed).toBe(true)
      expect((yield* close(store, intermediateGeneration.generation.id)).closed).toBe(true)
    }),
  )

  it.effect("requires source reservation before capture and reconciles logical finalization exactly once", () =>
    Effect.gen(function* () {
      const root = sessionID("source-reconcile-root")
      const child = sessionID("source-reconcile-child")
      const { store } = yield* setup({ id: root }, { id: child, parentID: root })
      const generation = yield* store.register(
        registration("source-reconcile", root, child, { mode: "background" }),
      )
      yield* store.finishWork(generation.workID)

      const source = assistantSource("reconcile")
      const reserved = yield* store.reserveSource({
        sessionID: child,
        generationID: generation.generation.id,
        source,
        historyCutoff: "history-reconcile",
        consumed: [],
      })
      expect(reserved.created).toBe(true)
      expect(reserved.source.state).toBe("reserved")

      const capture = {
        source,
        payload: "logical reply",
        outcome: "reply" as const,
        historyCutoff: "history-reconcile",
        consumed: [],
      }
      const beforeFinalization = yield* store.capture(generation.generation.id, capture).pipe(Effect.exit)
      expectFailure(beforeFinalization)

      const missingReservation = yield* store
        .reconcileAndClose(generation.generation.id, () =>
          Effect.succeed({
            quiescent: true,
            sources: [{ ...capture, source: assistantSource("missing") }],
          }),
        )
        .pipe(Effect.exit)
      expectFailure(missingReservation)

      const reconciled = yield* store.reconcileAndClose(generation.generation.id, () =>
        Effect.succeed({ quiescent: true, sources: [capture] }),
      )
      expect(reconciled.closed).toBe(false)
      expect(reconciled.resolutions).toHaveLength(1)
      expect(reconciled.resolutions[0]?.status).toBe("pending")
      expect((yield* store.sources(child))[0]?.state).toBe("finalized")

      const retried = yield* store.reconcileAndClose(generation.generation.id, () =>
        Effect.succeed({ quiescent: true, sources: [capture] }),
      )
      expect(retried.resolutions[0]?.id).toBe(reconciled.resolutions[0]?.id)
      const finalization = yield* store.finalizeSource(reserved.source.id, {
        payload: "logical reply",
        outcome: "reply",
      })
      expect(finalization.resolution?.id).toBe(reconciled.resolutions[0]?.id)
    }),
  )

  it.effect("reserves and finalizes root and delegated sources without bypassing source ownership", () =>
    Effect.gen(function* () {
      const root = sessionID("source-root")
      const child = sessionID("source-child")
      const { store } = yield* setup({ id: root }, { id: child, parentID: root })

      const rootReservation = yield* store.reserveSource({
        sessionID: root,
        source: assistantSource("root"),
        historyCutoff: "history-root",
        consumed: [],
      })
      expect(rootReservation.source.generationID).toBeUndefined()
      expect(rootReservation.source.workID).toMatch(/^dwk_/)
      const rootRetry = yield* store.reserveSource({
        sessionID: root,
        source: assistantSource("root"),
        historyCutoff: "history-root",
        consumed: [],
      })
      expect(rootRetry.created).toBe(false)
      expect(rootRetry.source).toEqual(rootReservation.source)
      const rootFinalized = yield* store.finalizeSource(rootReservation.source.id, {
        payload: "root reply",
        outcome: "reply",
      })
      expect(rootFinalized.resolution).toBeUndefined()
      expect(rootFinalized.source.state).toBe("finalized")

      const generation = yield* store.register(
        registration("source-child", root, child, { mode: "background" }),
      )
      yield* store.finishWork(generation.workID)
      const source = assistantSource("delegated")
      const reserved = yield* store.reserveSource({
        sessionID: child,
        generationID: generation.generation.id,
        source,
        historyCutoff: "history-delegated",
        consumed: [],
      })
      expect((yield* store.unfinishedSources(child)).map((item) => item.id)).toEqual([reserved.source.id])
      const finalized = yield* store.finalizeSource(reserved.source.id, {
        payload: "delegated reply",
        outcome: "reply",
      })
      expect(finalized.source.state).toBe("finalized")
      expect(finalized.resolution?.status).toBe("pending")
      expect(yield* store.unfinishedSources(child)).toEqual([])

      const conflict = yield* store
        .finalizeSource(reserved.source.id, { payload: "changed", outcome: "reply" })
        .pipe(Effect.exit)
      expectFailure(conflict)
      const exact = yield* store.finalizeSource(reserved.source.id, {
        payload: "delegated reply",
        outcome: "reply",
      })
      expect(exact.resolution?.id).toBe(finalized.resolution?.id)
    }),
  )

  it.effect("freezes the full envelope, admits it through the same DB, then consumes and resolves it", () =>
    Effect.gen(function* () {
      const root = sessionID("envelope-root")
      const child = sessionID("envelope-child")
      const { db, store } = yield* setup({ id: root }, { id: child, parentID: root })
      const generation = yield* store.register(
        registration("envelope-child", root, child, { mode: "background" }),
      )
      yield* store.finishWork(generation.workID)
      const source = assistantSource("envelope")
      const reserved = yield* store.reserveSource({
        sessionID: child,
        generationID: generation.generation.id,
        source,
        historyCutoff: "history-envelope",
        consumed: [],
      })
      const finalized = yield* store.finalizeSource(reserved.source.id, {
        payload: "envelope reply",
        outcome: "reply",
      })
      if (finalized.resolution === undefined) return yield* Effect.die("expected a delegated resolution")

      const recipient = content("envelope")
      const prepared = yield* store.prepare(finalized.resolution.id, recipient)
      if (prepared.envelope === undefined) return yield* Effect.die("expected a prepared envelope")
      expect(prepared.envelope.message.id).toBe(prepared.messageID)
      expect(prepared.envelope.parts).toHaveLength(2)
      expect(prepared.envelope.parts[0]?.id).toBe(Delegation.OriginPartID.make(`prt_${prepared.id.slice(4)}_0`))
      expect(prepared.envelope.parts[1]?.id).toBe(Delegation.OriginPartID.make(`prt_${prepared.id.slice(4)}_1`))
      expect(prepared.envelope.provenance).toMatchObject({
        generationID: generation.generation.id,
        parentID: root,
        childID: child,
        origin: generation.generation.origin,
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
      const samePreparation = yield* store.prepare(finalized.resolution.id, reordered)
      expect(samePreparation).toEqual(prepared)
      const conflictingPreparation = yield* store
        .prepare(finalized.resolution.id, { ...recipient, parts: [...recipient.parts].reverse() })
        .pipe(Effect.exit)
      expectFailure(conflictingPreparation)

      const admitted = yield* admitPrepared(store, prepared.id)
      expect(admitted).toEqual({ status: "admitted" })
      const admittedRetry = yield* admitPrepared(store, prepared.id)
      expect(admittedRetry).toEqual({ status: "admitted" })
      expect(yield* db.all(`SELECT id, envelope FROM delegation_test_receipt`)).toHaveLength(1)

      yield* store.markConsumed({ parentID: root, ids: [prepared.id] })
      expect((yield* store.getResolution(prepared.id))?.status).toBe("consumed")
      expect((yield* store.incoming(root)).map((resolution) => resolution.id)).toEqual([prepared.id])

      const closed = yield* close(store, generation.generation.id)
      expect(closed.closed).toBe(true)

      const toolTurn = yield* store.reserveSource({
        sessionID: root,
        source: assistantSource("discarded-tool-turn"),
        historyCutoff: "history-tool-turn",
        consumed: [prepared.id],
      })
      const discarded = yield* store.discardSource(toolTurn.source.id)
      expect(discarded.state).toBe("discarded")
      expect((yield* store.getResolution(prepared.id))?.status).toBe("consumed")
      expect(yield* store.discardSource(toolTurn.source.id)).toEqual(discarded)

      const replyAfterTool = yield* store.reserveSource({
        sessionID: root,
        source: assistantSource("root-after-tool"),
        historyCutoff: "history-root-after-tool",
        consumed: [prepared.id],
      })
      const rootFinalized = yield* store.finalizeSource(replyAfterTool.source.id, {
        payload: "root processed reply",
        outcome: "reply",
      })
      expect(rootFinalized.resolution).toBeUndefined()
      const resolved = yield* store.getResolution(prepared.id)
      expect(resolved?.status).toBe("resolved")
      expect(resolved?.resolvedSourceID).toBe(replyAfterTool.source.id)
      expect(resolved?.timeResolved).toBeDefined()
    }),
  )

  it.effect("rejects closure on consumed-unresolved and then closes after source finalization resolves it", () =>
    Effect.gen(function* () {
      const root = sessionID("closure-resolution-root")
      const parent = sessionID("closure-resolution-parent")
      const child = sessionID("closure-resolution-child")
      const { store } = yield* setup(
        { id: root },
        { id: parent, parentID: root },
        { id: child, parentID: parent },
      )
      const parentGeneration = yield* store.register(
        registration("closure-resolution-parent", root, parent, { mode: "background" }),
      )
      yield* store.finishWork(parentGeneration.workID)
      const childGeneration = yield* store.register(
        registration("closure-resolution-child", parent, child, {
          mode: "background",
          parentGenerationID: parentGeneration.generation.id,
        }),
      )
      yield* store.finishWork(childGeneration.workID)
      const childSource = yield* store.reserveSource({
        sessionID: child,
        generationID: childGeneration.generation.id,
        source: assistantSource("closure-resolution"),
        historyCutoff: "history-closure-resolution",
        consumed: [],
      })
      const finalized = yield* store.finalizeSource(childSource.source.id, {
        payload: "closure resolution",
        outcome: "reply",
      })
      if (finalized.resolution === undefined) return yield* Effect.die("expected a resolution")
      const prepared = yield* store.prepare(finalized.resolution.id, content("closure-resolution"))
      yield* admitPrepared(store, prepared.id)
      yield* store.markConsumed({ parentID: parent, ids: [prepared.id] })
      const childClosed = yield* close(store, childGeneration.generation.id)
      expect(childClosed.closed).toBe(true)
      const consumedBlocked = yield* close(store, parentGeneration.generation.id)
      expect(consumedBlocked.closed).toBe(false)
      expect(consumedBlocked.blockers).toContain(`incoming:${prepared.id}`)

      const rootSource = yield* store.reserveSource({
        sessionID: parent,
        generationID: parentGeneration.generation.id,
        source: assistantSource("closure-parent-resolution"),
        historyCutoff: "history-closure-root-resolution",
        consumed: [prepared.id],
      })
      yield* store.finalizeSource(rootSource.source.id, { payload: "resolved", outcome: "reply" })
      expect((yield* store.getResolution(prepared.id))?.status).toBe("resolved")
      const parentReturn = yield* store.pending(root)
      if (parentReturn.length !== 1) return yield* Effect.die("expected a parent return")
      const parentEnvelope = yield* store.prepare(parentReturn[0].id, content("closure-parent-resolution"))
      expect(yield* admitPrepared(store, parentEnvelope.id)).toEqual({ status: "admitted" })
      yield* store.markConsumed({ parentID: root, ids: [parentEnvelope.id] })
      expect((yield* close(store, parentGeneration.generation.id)).closed).toBe(true)
    }),
  )

  it.effect("revokes an admitted return from a closed outgoing generation without fencing the closed child", () =>
    revokeClosedOutgoingReturn("admitted"),
  )

  it.effect("revokes a consumed return from a closed outgoing generation without fencing the closed child", () =>
    revokeClosedOutgoingReturn("consumed"),
  )

  it.effect("revokes an idle intermediate, its live grandchild, and sibling returns across all delivery states", () =>
    Effect.gen(function* () {
      const root = sessionID("revoke-root")
      const intermediate = sessionID("revoke-intermediate")
      const grandchild = sessionID("revoke-grandchild")
      const sibling = sessionID("revoke-sibling")
      const { db, store } = yield* setup(
        { id: root },
        { id: intermediate, parentID: root },
        { id: grandchild, parentID: intermediate },
        { id: sibling, parentID: root },
      )

      const intermediateGeneration = yield* store.register(
        registration("revoke-intermediate", root, intermediate, { mode: "background" }),
      )
      yield* store.finishWork(intermediateGeneration.workID)
      const grandchildGeneration = yield* store.register(
        registration("revoke-grandchild", intermediate, grandchild, {
          mode: "background",
          parentGenerationID: intermediateGeneration.generation.id,
        }),
      )
      const siblingGeneration = yield* store.register(
        registration("revoke-sibling", root, sibling, { mode: "background" }),
      )
      yield* store.finishWork(siblingGeneration.workID)

      const intermediateSource = yield* store.reserveSource({
        sessionID: intermediate,
        generationID: intermediateGeneration.generation.id,
        source: assistantSource("revoke-intermediate"),
        historyCutoff: "history-revoke-intermediate",
        consumed: [],
      })
      const pending = yield* store.finalizeSource(intermediateSource.source.id, {
        payload: "pending",
        outcome: "reply",
      })
      if (pending.resolution === undefined) return yield* Effect.die("expected pending resolution")

      const grandchildSource = yield* store.reserveSource({
        sessionID: grandchild,
        generationID: grandchildGeneration.generation.id,
        source: assistantSource("revoke-grandchild"),
        historyCutoff: "history-revoke-grandchild",
        consumed: [],
      })
      const consumed = yield* store.finalizeSource(grandchildSource.source.id, {
        payload: "consumed",
        outcome: "reply",
      })
      if (consumed.resolution === undefined) return yield* Effect.die("expected consumed resolution")
      const prepared = yield* store.prepare(consumed.resolution.id, content("revoke-grandchild"))
      yield* admitPrepared(store, prepared.id)
      yield* store.markConsumed({ parentID: intermediate, ids: [prepared.id] })

      const siblingSource = yield* store.reserveSource({
        sessionID: sibling,
        generationID: siblingGeneration.generation.id,
        source: assistantSource("revoke-sibling"),
        historyCutoff: "history-revoke-sibling",
        consumed: [],
      })
      const admitted = yield* store.finalizeSource(siblingSource.source.id, {
        payload: "admitted",
        outcome: "reply",
      })
      if (admitted.resolution === undefined) return yield* Effect.die("expected admitted resolution")
      const admittedEnvelope = yield* store.prepare(admitted.resolution.id, content("revoke-sibling"))
      yield* admitPrepared(store, admittedEnvelope.id)

      const revoked = yield* store.revokeDescendants(root)
      expect(revoked.cancellationSource).toEqual({
        kind: "terminal",
        id: `delegation-cancel:${revoked.revocationID}`,
      })
      expect(revoked.generations.map((generation) => generation.id)).toEqual(
        expect.arrayContaining([
          intermediateGeneration.generation.id,
          grandchildGeneration.generation.id,
          siblingGeneration.generation.id,
        ]),
      )
      expect(revoked.sessionIDs).toEqual(expect.arrayContaining([intermediate, grandchild, sibling]))
      expect((yield* store.get(intermediateGeneration.generation.id))?.state).toBe("revoked")
      expect((yield* store.get(grandchildGeneration.generation.id))?.state).toBe("revoked")
      expect((yield* store.get(siblingGeneration.generation.id))?.state).toBe("revoked")
      expect((yield* store.getResolution(pending.resolution.id))?.status).toBe("revoked")
      expect((yield* store.getResolution(prepared.id))?.status).toBe("revoked")
      expect((yield* store.getResolution(admittedEnvelope.id))?.status).toBe("revoked")
      expect(yield* store.unfinished(intermediate)).toEqual([])
      expect(yield* store.unfinished(grandchild)).toEqual([])
      expect(yield* store.unfinished(sibling)).toEqual([])

      const repeated = yield* store.revokeDescendants(root)
      expect(repeated.revocationID).toBe(revoked.revocationID)
      expect(repeated.cancellationSource).toEqual(revoked.cancellationSource)
      expect((yield* store.getResolution(pending.resolution.id))?.status).toBe("revoked")

      const laterSource = yield* store
        .reserveSource({
          sessionID: intermediate,
          generationID: intermediateGeneration.generation.id,
          source: assistantSource("after-revoke"),
          historyCutoff: "history-after-revoke",
          consumed: [],
        })
        .pipe(Effect.exit)
      expectFailure(laterSource)
      const laterWork = yield* store
        .startWork({
          id: Delegation.WorkID.create(),
          sessionID: grandchild,
          generationID: grandchildGeneration.generation.id,
          kind: "provider",
        })
        .pipe(Effect.exit)
      expectFailure(laterWork)
      const laterRegistration = yield* store
        .register(registration("after-revoke-registration", root, intermediate, { explicitReuse: true }))
        .pipe(Effect.exit)
      expectFailure(laterRegistration)

      yield* store.allowSession(root)
      yield* store.allowSession(intermediate)
      yield* store.allowSession(grandchild)
      yield* store.allowSession(sibling)
      expect((yield* store.get(intermediateGeneration.generation.id))?.state).toBe("revoked")
      expect((yield* store.getResolution(pending.resolution.id))?.status).toBe("revoked")

      const newFence = yield* store.revokeDescendants(root)
      expect(newFence.revocationID).not.toBe(revoked.revocationID)
      expect(newFence.cancellationSource.id).not.toBe(revoked.cancellationSource.id)
      yield* store.allowSession(root)

      const fresh = yield* store.register(
        registration("after-explicit-resume", root, intermediate, {
          explicitReuse: true,
          mode: "background",
        }),
      )
      expect(fresh.generation.id).not.toBe(intermediateGeneration.generation.id)
      expect(fresh.generation.state).toBe("active")
    }),
  )

  it.effect("keeps a stopped child's incoming generation for terminal cancellation and fences stale assistant work", () =>
    Effect.gen(function* () {
      const root = sessionID("child-stop-root")
      const child = sessionID("child-stop-child")
      const grandchild = sessionID("child-stop-grandchild")
      const { store } = yield* setup(
        { id: root },
        { id: child, parentID: root },
        { id: grandchild, parentID: child },
      )
      const incoming = yield* store.register(registration("child-stop-incoming", root, child))
      const grandchildGeneration = yield* store.register(
        registration("child-stop-grandchild", child, grandchild, {
          parentGenerationID: incoming.generation.id,
        }),
      )

      const revoked = yield* store.revokeDescendants(child)
      const repeated = yield* store.revokeDescendants(child)
      expect(repeated.revocationID).toBe(revoked.revocationID)
      expect(repeated.cancellationSource).toEqual(revoked.cancellationSource)
      expect((yield* store.get(incoming.generation.id))?.state).toBe("active")
      expect((yield* store.get(grandchildGeneration.generation.id))?.state).toBe("revoked")

      const promotion = yield* store.promote(incoming.generation.id).pipe(Effect.exit)
      expectFailure(promotion)
      const prematureAllow = yield* store.allowSession(child).pipe(Effect.exit)
      expectFailure(prematureAllow)

      const staleAssistant = yield* store
        .reserveSource({
          sessionID: child,
          generationID: incoming.generation.id,
          source: assistantSource("stale-assistant"),
          historyCutoff: "history-stale-assistant",
          consumed: [],
        })
        .pipe(Effect.exit)
      expectFailure(staleAssistant)

      const cancellation = yield* store.reserveSource({
        sessionID: child,
        generationID: incoming.generation.id,
        source: revoked.cancellationSource,
        historyCutoff: "history-cancel-child",
        consumed: [],
      })
      const cancellationRetry = yield* store.reserveSource({
        sessionID: child,
        generationID: incoming.generation.id,
        source: revoked.cancellationSource,
        historyCutoff: "history-cancel-child",
        consumed: [],
      })
      expect(cancellationRetry.created).toBe(false)
      const finalized = yield* store.finalizeSource(cancellation.source.id, {
        payload: "cancelled",
        outcome: "cancelled",
      })
      expect(finalized.resolution).toBeUndefined()
      expect(finalized.source.state).toBe("finalized")

      const closed = yield* close(store, incoming.generation.id)
      expect(closed.closed).toBe(true)
      yield* store.allowSession(child)
      const newFence = yield* store.revokeDescendants(child)
      expect(newFence.revocationID).not.toBe(revoked.revocationID)
    }),
  )

  it.effect("does not fence historical closed children or their independent manual work", () =>
    Effect.gen(function* () {
      const root = sessionID("historical-root")
      const closedChild = sessionID("historical-closed-child")
      const activeChild = sessionID("historical-active-child")
      const { store } = yield* setup(
        { id: root },
        { id: closedChild, parentID: root },
        { id: activeChild, parentID: root },
      )

      const closedGeneration = yield* store.register(registration("historical-closed", root, closedChild))
      yield* store.finishWork(closedGeneration.workID)
      expect((yield* close(store, closedGeneration.generation.id)).closed).toBe(true)
      const manualWorkID = Delegation.WorkID.create()
      yield* store.startWork({ id: manualWorkID, sessionID: closedChild, kind: "provider" })
      const blockedNewGeneration = yield* store
        .register(registration("historical-new-generation", root, closedChild, { explicitReuse: true }))
        .pipe(Effect.exit)
      expectFailure(blockedNewGeneration)

      const activeGeneration = yield* store.register(
        registration("historical-active", root, activeChild, { mode: "background" }),
      )
      const rootWorkID = Delegation.WorkID.create()
      yield* store.startWork({ id: rootWorkID, sessionID: root, kind: "tool" })

      const revoked = yield* store.revokeDescendants(root)
      expect(revoked.sessionIDs).not.toContain(closedChild)
      expect((yield* store.get(closedGeneration.generation.id))?.state).toBe("closed")
      expect((yield* store.get(activeGeneration.generation.id))?.state).toBe("revoked")
      expect((yield* store.unfinished(closedChild)).map((work) => work.id)).toEqual([manualWorkID])
      expect(yield* store.unfinished(root)).toEqual([])
      const fencedRootWork = yield* store
        .startWork({ id: Delegation.WorkID.create(), sessionID: root, kind: "tool" })
        .pipe(Effect.exit)
      expectFailure(fencedRootWork)

      const moreManualWork = yield* store.startWork({
        id: Delegation.WorkID.create(),
        sessionID: closedChild,
        kind: "tool",
      })
      expect(moreManualWork.state).toBe("active")
    }),
  )

  it.effect("retains revoked identity rows after the parent is deleted and never admits the pending return", () =>
    Effect.gen(function* () {
      const root = sessionID("delete-root")
      const child = sessionID("delete-child")
      const { db, store } = yield* setup({ id: root }, { id: child, parentID: root })
      const generation = yield* store.register(
        registration("delete-child", root, child, { mode: "background" }),
      )
      yield* store.finishWork(generation.workID)
      const source = yield* store.reserveSource({
        sessionID: child,
        generationID: generation.generation.id,
        source: assistantSource("delete-pending"),
        historyCutoff: "history-delete-pending",
        consumed: [],
      })
      const finalized = yield* store.finalizeSource(source.source.id, {
        payload: "delete pending",
        outcome: "reply",
      })
      if (finalized.resolution === undefined) return yield* Effect.die("expected a resolution")
      const prepared = yield* store.prepare(finalized.resolution.id, content("delete-pending"))

      yield* store.revokeDescendants(root)
      yield* db.delete(SessionTable).where(eq(SessionTable.id, root)).run().pipe(Effect.orDie)
      const retained = yield* store.getResolution(prepared.id)
      expect(retained?.status).toBe("revoked")
      expect(yield* store.incoming(root)).toEqual([])

      let receiverCalled = false
      const admission = yield* store.admit(prepared.id, () =>
        Effect.sync(() => {
          receiverCalled = true
          return { status: "admitted" as const }
        }),
      )
      expect(admission).toEqual({ status: "revoked" })
      expect(receiverCalled).toBe(false)
      expect((yield* store.sources(child))[0]?.id).toBe(source.source.id)
      expect((yield* store.get(generation.generation.id))?.state).toBe("revoked")
    }),
  )
})

test("retains unfinished foreground and root provider/tool work for recovery after a disk reopen", async () => {
  await using temporary = await tmpdir()
  const filename = path.join(temporary.path, "delegation-recovery.sqlite")
  const root = sessionID("recovery-root")
  const child = sessionID("recovery-child")
  const rootProviderID = Delegation.WorkID.create()
  const rootToolID = Delegation.WorkID.create()
  const childProviderID = Delegation.WorkID.create()

  const recovery = await runDatabase(
    filename,
    Effect.gen(function* () {
      const { store } = yield* setup({ id: root }, { id: child, parentID: root })
      const generation = yield* store.register(registration("recovery-child", root, child))
      yield* store.finishWork(generation.workID)
      yield* store.startWork({ id: rootProviderID, sessionID: root, kind: "provider" })
      yield* store.startWork({ id: rootToolID, sessionID: root, kind: "tool" })
      yield* store.startWork({
        id: childProviderID,
        sessionID: child,
        generationID: generation.generation.id,
        kind: "provider",
      })
      return { generationID: generation.generation.id, launchWorkID: generation.workID }
    }),
  )

  await runDatabase(
    filename,
    Effect.gen(function* () {
      const database = yield* Database.Service
      const store = DelegationStore.make(database.db)
      const rootProviderRetry = yield* store.startWork({
        id: rootProviderID,
        sessionID: root,
        kind: "provider",
      })
      expect(rootProviderRetry.state).toBe("active")
      const childProviderRetry = yield* store.startWork({
        id: childProviderID,
        sessionID: child,
        generationID: recovery.generationID,
        kind: "provider",
      })
      expect(childProviderRetry.state).toBe("active")
      const rootWorkConflict = yield* store
        .startWork({ id: rootProviderID, sessionID: root, kind: "tool" })
        .pipe(Effect.exit)
      expectFailure(rootWorkConflict)
      expect((yield* store.unfinished(root)).map((work) => work.id)).toEqual([rootProviderID, rootToolID])
      expect((yield* store.unfinished(child)).map((work) => work.id)).toEqual([childProviderID])
      expect((yield* store.listWork(recovery.generationID)).map((work) => work.id)).toEqual([
        recovery.launchWorkID,
        childProviderID,
      ])
      expect((yield* store.listWork(recovery.generationID))[1]?.state).toBe("active")
      const blocked = yield* close(store, recovery.generationID)
      expect(blocked.closed).toBe(false)
      expect(blocked.blockers).toContain(`work:${childProviderID}`)
    }),
  )
})
