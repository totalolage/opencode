import { describe, expect, test } from "bun:test"
import { eq, sql } from "drizzle-orm"
import { Effect, Exit } from "effect"
import path from "path"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { Database } from "@opencode-ai/core/database/database"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { SessionID } from "@opencode-ai/schema/session-id"
import { tmpdir } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"

const it = testEffect(Database.layerFromPath(":memory:"))

type SessionFixture = {
  readonly id: SessionID
  readonly parentID?: SessionID
}

type RegistrationOptions = {
  readonly explicitReuse?: boolean
  readonly mode?: Delegation.Mode
  readonly parentGenerationID?: Delegation.ID
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
        `CREATE TABLE delegation_source_test_receipt (
          id TEXT PRIMARY KEY,
          envelope TEXT NOT NULL
        )`,
      )
      .pipe(Effect.orDie)
    yield* db
      .run(
        `CREATE TABLE delegation_source_test_message (
          id TEXT PRIMARY KEY,
          session_id TEXT NOT NULL,
          data TEXT NOT NULL,
          provenance TEXT NOT NULL
        )`,
      )
      .pipe(Effect.orDie)
    yield* db
      .run(
        `CREATE TABLE delegation_source_test_part (
          id TEXT PRIMARY KEY,
          message_id TEXT NOT NULL,
          position INTEGER NOT NULL,
          data TEXT NOT NULL
        )`,
      )
      .pipe(Effect.orDie)

    return { db, store: DelegationStore.make(db) }
  })

function sessionID(name: string) {
  return SessionID.make(`ses_delegation_source_${name}`)
}

function origin(name: string): Delegation.Origin {
  return {
    messageID: SessionMessage.ID.make(`msg_delegation_source_${name}`),
    partID: Delegation.OriginPartID.make(`part_delegation_source_${name}`),
    callID: `call_delegation_source_${name}`,
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
    origin: origin(name),
    ...(options.parentGenerationID === undefined ? {} : { parentGenerationID: options.parentGenerationID }),
    ...(options.mode === undefined ? {} : { mode: options.mode }),
    explicitReuse: options.explicitReuse ?? false,
  }
}

function assistantSource(name: string): Delegation.Source {
  return { kind: "assistant", id: `assistant-delegation-source-${name}` }
}

function terminalSource(name: string): Delegation.Source {
  return { kind: "terminal", id: `terminal-delegation-source-${name}` }
}

function content(name: string): Delegation.RecipientContent {
  return {
    message: { kind: "synthetic", text: `delegation source ${name}` },
    parts: [{ kind: "text", text: `delegation source ${name}` }],
  }
}

function capture(
  source: Delegation.Source,
  payload: string,
  historyCutoff: string,
  consumed: readonly Delegation.ResolutionID[] = [],
  outcome: Delegation.Outcome = "reply",
): Delegation.Capture {
  return { source, payload, outcome, historyCutoff, consumed: [...consumed] }
}

function close(store: DelegationStore.Interface, generationID: Delegation.ID) {
  return store.reconcileAndClose(generationID, () => Effect.succeed({ quiescent: true, sources: [] }))
}

function expectFailure<A, E>(exit: Exit.Exit<A, E>) {
  expect(Exit.isFailure(exit)).toBe(true)
}

function admitWithReceipt(store: DelegationStore.Interface, id: Delegation.ResolutionID) {
  return store.admit(id, (tx, resolution) =>
    Effect.gen(function* () {
      yield* tx.run(
        sql`INSERT INTO delegation_source_test_receipt (id, envelope) VALUES (${resolution.messageID}, ${JSON.stringify(resolution.envelope)})`,
      )
      return { status: "admitted" as const }
    }),
  )
}

function admitWithMessageParts(store: DelegationStore.Interface, id: Delegation.ResolutionID) {
  return store.admit(id, (tx, resolution) =>
    Effect.gen(function* () {
      yield* tx.run(
        sql`INSERT INTO delegation_source_test_message (id, session_id, data, provenance) VALUES (${resolution.messageID}, ${resolution.parentID}, ${JSON.stringify(resolution.envelope.message.data)}, ${JSON.stringify(resolution.envelope.provenance)})`,
      )
      yield* Effect.forEach(resolution.envelope.parts, (part, position) =>
        tx.run(
          sql`INSERT INTO delegation_source_test_part (id, message_id, position, data) VALUES (${part.id}, ${resolution.messageID}, ${position}, ${JSON.stringify(part.data)})`,
        ),
      )
      yield* tx.run(
        sql`INSERT INTO delegation_source_test_receipt (id, envelope) VALUES (${resolution.id}, ${JSON.stringify(resolution.envelope)})`,
      )
      return { status: "admitted" as const }
    }),
  )
}

function runDatabase<A, E>(filename: string, effect: Effect.Effect<A, E, Database.Service>) {
  return Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(Database.layerFromPath(filename))))
}

function createBackgroundResolution(
  store: DelegationStore.Interface,
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
    if (finalized.resolution === undefined) return yield* Effect.die(`expected background resolution for ${name}`)
    return { generation: registered.generation, source: source.source, resolution: finalized.resolution }
  })
}

function createAdmittedBackgroundResolution(
  store: DelegationStore.Interface,
  name: string,
  parentID: SessionID,
  childID: SessionID,
  parentGenerationID?: Delegation.ID,
) {
  return Effect.gen(function* () {
    const created = yield* createBackgroundResolution(store, name, parentID, childID, parentGenerationID)
    const prepared = yield* store.prepare(created.resolution.id, content(name))
    const admitted = yield* admitWithReceipt(store, prepared.id)
    if (admitted.status !== "admitted") return yield* Effect.die(`expected admission for ${name}`)
    return { ...created, prepared }
  })
}

describe("Delegation source ledger", () => {
  it.effect("reserves exact source work before the provider and retries finalized or discarded records", () =>
    Effect.gen(function* () {
      const root = sessionID("reserve-root")
      const child = sessionID("reserve-child")
      const { store } = yield* setup({ id: root }, { id: child, parentID: root })
      const registered = yield* store.register(
        registration("reserve-child", root, child, { mode: "background" }),
      )
      yield* store.finishWork(registered.workID)

      const input = {
        sessionID: child,
        generationID: registered.generation.id,
        source: assistantSource("reserve"),
        historyCutoff: "history-reserve",
        consumed: [],
      }
      const reserved = yield* store.reserveSource(input)
      expect(reserved.created).toBe(true)
      expect(reserved.source).toMatchObject({
        sessionID: child,
        generationID: registered.generation.id,
        source: input.source,
        historyCutoff: input.historyCutoff,
        consumed: [],
        state: "reserved",
      })
      expect(reserved.source.id).toMatch(/^dsrc_/)
      expect(reserved.source.workID).toMatch(/^dwk_/)
      expect(yield* store.unfinishedSources(child)).toEqual([reserved.source])
      expect(yield* store.unfinished(child)).toEqual([
        expect.objectContaining({
          id: reserved.source.workID,
          sessionID: child,
          generationID: registered.generation.id,
          kind: "provider",
          state: "active",
        }),
      ])

      const retry = yield* store.reserveSource(input)
      expect(retry).toEqual({ source: reserved.source, created: false })

      const finalized = yield* store.finalizeSource(reserved.source.id, {
        payload: "finalized reserve",
        outcome: "reply",
      })
      expect(finalized.source.state).toBe("finalized")
      expect(finalized.resolution?.status).toBe("pending")
      expect(yield* store.unfinished(child)).toEqual([])

      const finalizedRetry = yield* store.reserveSource(input)
      expect(finalizedRetry).toEqual({ source: finalized.source, created: false })
      expect(yield* store.unfinished(child)).toEqual([])

      const changedCutoff = yield* store
        .reserveSource({ ...input, historyCutoff: "history-changed" })
        .pipe(Effect.exit)
      expectFailure(changedCutoff)
      const changedConsumed = yield* store
        .reserveSource({ ...input, consumed: [Delegation.ResolutionID.create()] })
        .pipe(Effect.exit)
      expectFailure(changedConsumed)

      const discardedReservation = yield* store.reserveSource({
        ...input,
        source: assistantSource("discard-retry"),
      })
      const discarded = yield* store.discardSource(discardedReservation.source.id)
      expect(discarded.state).toBe("discarded")
      expect(yield* store.unfinishedSources(child)).toEqual([])
      expect(
        yield* store.reserveSource({
          ...input,
          source: assistantSource("discard-retry"),
        }),
      ).toEqual({ source: discarded, created: false })
      expect(yield* store.discardSource(discarded.id)).toEqual(discarded)
    }),
  )

  it.effect("requires a reservation before raw capture and keeps background, foreground, and root returns distinct", () =>
    Effect.gen(function* () {
      const root = sessionID("capture-root")
      const backgroundChild = sessionID("capture-background-child")
      const foregroundChild = sessionID("capture-foreground-child")
      const { store } = yield* setup(
        { id: root },
        { id: backgroundChild, parentID: root },
        { id: foregroundChild, parentID: root },
      )
      const background = yield* store.register(
        registration("capture-background", root, backgroundChild, { mode: "background" }),
      )
      yield* store.finishWork(background.workID)
      const backgroundSource = assistantSource("capture-background")
      const backgroundCapture = capture(backgroundSource, "background payload", "history-background")

      const missingReservation = yield* store.capture(background.generation.id, backgroundCapture).pipe(Effect.exit)
      expectFailure(missingReservation)

      const reserved = yield* store.reserveSource({
        sessionID: backgroundChild,
        generationID: background.generation.id,
        source: backgroundSource,
        historyCutoff: "history-background",
        consumed: [],
      })
      const reservedCapture = yield* store.capture(background.generation.id, backgroundCapture).pipe(Effect.exit)
      expectFailure(reservedCapture)

      const finalized = yield* store.finalizeSource(reserved.source.id, {
        payload: "background payload",
        outcome: "reply",
      })
      if (finalized.resolution === undefined) return yield* Effect.die("expected pending background resolution")
      expect(finalized.resolution.status).toBe("pending")
      expect(finalized.resolution.consumed).toEqual([])
      expect(yield* store.pending(root)).toEqual([finalized.resolution])

      const exactFinalization = yield* store.finalizeSource(reserved.source.id, {
        payload: "background payload",
        outcome: "reply",
      })
      expect(exactFinalization.resolution?.id).toBe(finalized.resolution.id)
      const changedFinalization = yield* store
        .finalizeSource(reserved.source.id, { payload: "changed background payload", outcome: "reply" })
        .pipe(Effect.exit)
      expectFailure(changedFinalization)

      const assistantErrorSource = assistantSource("capture-assistant-error")
      const assistantErrorReservation = yield* store.reserveSource({
        sessionID: backgroundChild,
        generationID: background.generation.id,
        source: assistantErrorSource,
        historyCutoff: "history-assistant-error",
        consumed: [],
      })
      const assistantError = yield* store.finalizeSource(assistantErrorReservation.source.id, {
        payload: "assistant error",
        outcome: "error",
      })
      if (assistantError.resolution === undefined) return yield* Effect.die("expected assistant error resolution")
      expect(assistantError.resolution.status).toBe("pending")
      const assistantErrorCaptureRetry = yield* store.capture(
        background.generation.id,
        capture(assistantErrorSource, "assistant error", "history-assistant-error", [], "error"),
      )
      expect(assistantErrorCaptureRetry.id).toBe(assistantError.resolution.id)
      expect(assistantErrorCaptureRetry.source).toEqual(assistantError.resolution.source)
      expect(assistantErrorCaptureRetry.messageID).toBe(assistantError.resolution.messageID)
      const changedAssistantErrorPayload = yield* store
        .capture(
          background.generation.id,
          capture(assistantErrorSource, "changed assistant error", "history-assistant-error", [], "error"),
        )
        .pipe(Effect.exit)
      expectFailure(changedAssistantErrorPayload)
      const changedAssistantErrorOutcome = yield* store
        .capture(
          background.generation.id,
          capture(assistantErrorSource, "assistant error", "history-assistant-error", [], "cancelled"),
        )
        .pipe(Effect.exit)
      expectFailure(changedAssistantErrorOutcome)

      const invalidTerminalReservation = yield* store.reserveSource({
        sessionID: backgroundChild,
        generationID: background.generation.id,
        source: terminalSource("capture-invalid-terminal"),
        historyCutoff: "history-invalid-terminal",
        consumed: [],
      })
      const invalidTerminal = yield* store
        .finalizeSource(invalidTerminalReservation.source.id, { payload: "invalid", outcome: "reply" })
        .pipe(Effect.exit)
      expectFailure(invalidTerminal)

      const terminalErrorReservation = yield* store.reserveSource({
        sessionID: backgroundChild,
        generationID: background.generation.id,
        source: terminalSource("capture-terminal-error"),
        historyCutoff: "history-terminal-error",
        consumed: [],
      })
      const terminalError = yield* store.finalizeSource(terminalErrorReservation.source.id, {
        payload: "terminal error",
        outcome: "error",
      })
      expect(terminalError.resolution?.status).toBe("pending")

      const assistantCancellationReservation = yield* store.reserveSource({
        sessionID: backgroundChild,
        generationID: background.generation.id,
        source: assistantSource("capture-assistant-cancelled"),
        historyCutoff: "history-assistant-cancelled",
        consumed: [],
      })
      const assistantCancellation = yield* store.finalizeSource(assistantCancellationReservation.source.id, {
        payload: "assistant cancelled",
        outcome: "cancelled",
      })
      expect(assistantCancellation.resolution?.status).toBe("pending")

      const foreground = yield* store.register(registration("capture-foreground", root, foregroundChild))
      yield* store.finishWork(foreground.workID)
      const foregroundReservation = yield* store.reserveSource({
        sessionID: foregroundChild,
        generationID: foreground.generation.id,
        source: assistantSource("capture-foreground"),
        historyCutoff: "history-foreground",
        consumed: [],
      })
      const foregroundFinalized = yield* store.finalizeSource(foregroundReservation.source.id, {
        payload: "foreground payload",
        outcome: "error",
      })
      expect(foregroundFinalized.resolution).toBeUndefined()
      const foregroundCancellationReservation = yield* store.reserveSource({
        sessionID: foregroundChild,
        generationID: foreground.generation.id,
        source: assistantSource("capture-foreground-cancelled"),
        historyCutoff: "history-foreground-cancelled",
        consumed: [],
      })
      const foregroundCancellation = yield* store.finalizeSource(foregroundCancellationReservation.source.id, {
        payload: "foreground cancelled",
        outcome: "cancelled",
      })
      expect(foregroundCancellation.resolution).toBeUndefined()

      const rootReservation = yield* store.reserveSource({
        sessionID: root,
        source: assistantSource("capture-root-error"),
        historyCutoff: "history-root-error",
        consumed: [],
      })
      const rootFinalized = yield* store.finalizeSource(rootReservation.source.id, {
        payload: "root payload",
        outcome: "error",
      })
      expect(rootFinalized.resolution).toBeUndefined()
      const rootCancellationReservation = yield* store.reserveSource({
        sessionID: root,
        source: assistantSource("capture-root-cancelled"),
        historyCutoff: "history-root-cancelled",
        consumed: [],
      })
      const rootCancellation = yield* store.finalizeSource(rootCancellationReservation.source.id, {
        payload: "root cancelled",
        outcome: "cancelled",
      })
      expect(rootCancellation.resolution).toBeUndefined()
      expect(yield* store.pending(root)).toHaveLength(4)
    }),
  )

  it.effect("validates incoming ownership, generation, status, and duplicate contributions before consuming atomically", () =>
    Effect.gen(function* () {
      const root = sessionID("incoming-root")
      const receiver = sessionID("incoming-receiver")
      const wrongRecipientChild = sessionID("incoming-wrong-recipient-child")
      const validChild = sessionID("incoming-valid-child")
      const pendingChild = sessionID("incoming-pending-child")
      const sibling = sessionID("incoming-sibling")
      const { db, store } = yield* setup(
        { id: root },
        { id: receiver, parentID: root },
        { id: wrongRecipientChild, parentID: receiver },
        { id: validChild, parentID: receiver },
        { id: pendingChild, parentID: receiver },
        { id: sibling, parentID: root },
      )

      const wrongRecipient = yield* createAdmittedBackgroundResolution(
        store,
        "incoming-wrong-recipient",
        receiver,
        wrongRecipientChild,
      )
      expect(wrongRecipient.resolution.recipientGenerationID).toBeUndefined()

      // A root-like receiver must settle its old return before a new incoming generation is adopted.
      expect((yield* close(store, wrongRecipient.generation.id)).closed).toBe(true)
      const claimedWrongRecipient = yield* store.reserveSource({
        sessionID: receiver,
        source: assistantSource("incoming-wrong-recipient-claim"),
        historyCutoff: "history-wrong-recipient-claim",
        consumed: [wrongRecipient.resolution.id],
      })
      const claimedWrongRecipientResult = yield* store.finalizeSource(claimedWrongRecipient.source.id, {
        payload: "claimed before adoption",
        outcome: "reply",
      })
      expect(claimedWrongRecipientResult.resolution).toBeUndefined()
      expect((yield* store.getResolution(wrongRecipient.resolution.id))?.status).toBe("resolved")

      const receiverGeneration = yield* store.register(
        registration("incoming-receiver", root, receiver, { mode: "background" }),
      )
      yield* store.finishWork(receiverGeneration.workID)

      const valid = yield* createAdmittedBackgroundResolution(
        store,
        "incoming-valid",
        receiver,
        validChild,
        receiverGeneration.generation.id,
      )
      expect(valid.resolution.parentID).toBe(receiver)
      expect(valid.resolution.recipientGenerationID).toBe(receiverGeneration.generation.id)
      expect(yield* db.all("SELECT id, envelope FROM delegation_source_test_receipt")).toHaveLength(2)

      const pending = yield* createBackgroundResolution(
        store,
        "incoming-pending",
        receiver,
        pendingChild,
        receiverGeneration.generation.id,
      )
      expect(pending.resolution.status).toBe("pending")

      const siblingGeneration = yield* store.register(
        registration("incoming-sibling", root, sibling, { mode: "background" }),
      )
      yield* store.finishWork(siblingGeneration.workID)
      const receiverSourcesBeforeRejectedReservations = yield* store.sources(receiver)

      const missingSession = yield* store
        .reserveSource({
          sessionID: sessionID("incoming-missing"),
          source: assistantSource("incoming-missing-session"),
          historyCutoff: "history-missing-session",
          consumed: [],
        })
        .pipe(Effect.exit)
      expectFailure(missingSession)

      const omittedGeneration = yield* store
        .reserveSource({
          sessionID: receiver,
          source: assistantSource("incoming-omitted-generation"),
          historyCutoff: "history-omitted-generation",
          consumed: [],
        })
        .pipe(Effect.exit)
      expectFailure(omittedGeneration)

      const wrongGeneration = yield* store
        .reserveSource({
          sessionID: receiver,
          generationID: siblingGeneration.generation.id,
          source: assistantSource("incoming-wrong-generation"),
          historyCutoff: "history-wrong-generation",
          consumed: [],
        })
        .pipe(Effect.exit)
      expectFailure(wrongGeneration)

      const wrongRecipientGeneration = yield* store
        .reserveSource({
          sessionID: receiver,
          generationID: receiverGeneration.generation.id,
          source: assistantSource("incoming-wrong-recipient-generation"),
          historyCutoff: "history-wrong-recipient-generation",
          consumed: [wrongRecipient.resolution.id],
        })
        .pipe(Effect.exit)
      expectFailure(wrongRecipientGeneration)

      const wrongSession = yield* store
        .reserveSource({
          sessionID: root,
          source: assistantSource("incoming-wrong-session"),
          historyCutoff: "history-wrong-session",
          consumed: [valid.resolution.id],
        })
        .pipe(Effect.exit)
      expectFailure(wrongSession)

      const pendingContribution = yield* store
        .reserveSource({
          sessionID: receiver,
          generationID: receiverGeneration.generation.id,
          source: assistantSource("incoming-pending-status"),
          historyCutoff: "history-pending-status",
          consumed: [pending.resolution.id],
        })
        .pipe(Effect.exit)
      expectFailure(pendingContribution)

      const duplicateContribution = yield* store
        .reserveSource({
          sessionID: receiver,
          generationID: receiverGeneration.generation.id,
          source: assistantSource("incoming-duplicate"),
          historyCutoff: "history-duplicate",
          consumed: [valid.resolution.id, valid.resolution.id],
        })
        .pipe(Effect.exit)
      expectFailure(duplicateContribution)
      expect(yield* store.sources(receiver)).toEqual(receiverSourcesBeforeRejectedReservations)

      const reserved = yield* store.reserveSource({
        sessionID: receiver,
        generationID: receiverGeneration.generation.id,
        source: assistantSource("incoming-valid-source"),
        historyCutoff: "history-valid-source",
        consumed: [valid.resolution.id],
      })
      expect(reserved.source.consumed).toEqual([valid.resolution.id])
      expect((yield* store.getResolution(valid.resolution.id))?.status).toBe("consumed")
      expect((yield* store.unfinished(receiver)).map((work) => work.id)).toContain(reserved.source.workID)

      const consumedRetry = yield* store.reserveSource({
        sessionID: receiver,
        generationID: receiverGeneration.generation.id,
        source: assistantSource("incoming-consumed-source"),
        historyCutoff: "history-consumed-source",
        consumed: [valid.resolution.id],
      })
      expect(consumedRetry.created).toBe(true)
      expect((yield* store.getResolution(valid.resolution.id))?.status).toBe("consumed")
    }),
  )

  it.effect("resolves consumed contributions once and preserves the winning source on competition", () =>
    Effect.gen(function* () {
      const root = sessionID("competition-root")
      const receiver = sessionID("competition-receiver")
      const producer = sessionID("competition-producer")
      const { store } = yield* setup(
        { id: root },
        { id: receiver, parentID: root },
        { id: producer, parentID: receiver },
      )
      const receiverGeneration = yield* store.register(
        registration("competition-receiver", root, receiver, { mode: "background" }),
      )
      yield* store.finishWork(receiverGeneration.workID)
      const incoming = yield* createAdmittedBackgroundResolution(
        store,
        "competition-incoming",
        receiver,
        producer,
        receiverGeneration.generation.id,
      )

      const first = yield* store.reserveSource({
        sessionID: receiver,
        generationID: receiverGeneration.generation.id,
        source: assistantSource("competition-first"),
        historyCutoff: "history-competition-first",
        consumed: [incoming.resolution.id],
      })
      const second = yield* store.reserveSource({
        sessionID: receiver,
        generationID: receiverGeneration.generation.id,
        source: assistantSource("competition-second"),
        historyCutoff: "history-competition-second",
        consumed: [incoming.resolution.id],
      })

      const winning = yield* store.finalizeSource(first.source.id, {
        payload: "winning reply",
        outcome: "reply",
      })
      if (winning.resolution === undefined) return yield* Effect.die("expected winning background resolution")
      const resolved = yield* store.getResolution(incoming.resolution.id)
      expect(resolved?.status).toBe("resolved")
      expect(resolved?.resolvedSourceID).toBe(first.source.id)
      expect(resolved?.timeResolved).toBeDefined()
      expect(yield* store.unfinished(receiver)).toEqual([
        expect.objectContaining({ id: second.source.workID, state: "active" }),
      ])

      const competing = yield* store
        .finalizeSource(second.source.id, { payload: "losing reply", outcome: "reply" })
        .pipe(Effect.exit)
      expectFailure(competing)
      const unchanged = yield* store.getResolution(incoming.resolution.id)
      expect(unchanged?.status).toBe("resolved")
      expect(unchanged?.resolvedSourceID).toBe(first.source.id)
      expect(unchanged?.timeResolved).toBe(resolved?.timeResolved)

      const exactWinningRetry = yield* store.finalizeSource(first.source.id, {
        payload: "winning reply",
        outcome: "reply",
      })
      expect(exactWinningRetry.resolution?.id).toBe(winning.resolution.id)

      yield* store.discardSource(second.source.id)
      const resolvedContribution = yield* store
        .reserveSource({
          sessionID: receiver,
          generationID: receiverGeneration.generation.id,
          source: assistantSource("competition-resolved-contribution"),
          historyCutoff: "history-competition-resolved-contribution",
          consumed: [incoming.resolution.id],
        })
        .pipe(Effect.exit)
      expectFailure(resolvedContribution)
    }),
  )

  it.effect("discards a tool-turn source without resolving inputs, then lets the next source resolve them", () =>
    Effect.gen(function* () {
      const root = sessionID("discard-root")
      const child = sessionID("discard-child")
      const { store } = yield* setup({ id: root }, { id: child, parentID: root })
      const incoming = yield* createAdmittedBackgroundResolution(store, "discard-incoming", root, child)

      const discardedReservation = yield* store.reserveSource({
        sessionID: root,
        source: assistantSource("discarded-tool-turn"),
        historyCutoff: "history-discarded-tool-turn",
        consumed: [incoming.resolution.id],
      })
      expect((yield* store.getResolution(incoming.resolution.id))?.status).toBe("consumed")
      const discarded = yield* store.discardSource(discardedReservation.source.id)
      expect(discarded.state).toBe("discarded")
      expect((yield* store.getResolution(incoming.resolution.id))?.status).toBe("consumed")
      expect(yield* store.unfinished(root)).toEqual([])

      const discardedFinalize = yield* store
        .finalizeSource(discarded.id, { payload: "must not publish", outcome: "reply" })
        .pipe(Effect.exit)
      expectFailure(discardedFinalize)
      expect(yield* store.discardSource(discarded.id)).toEqual(discarded)

      const next = yield* store.reserveSource({
        sessionID: root,
        source: assistantSource("discard-next-source"),
        historyCutoff: "history-discard-next-source",
        consumed: [incoming.resolution.id],
      })
      const finalized = yield* store.finalizeSource(next.source.id, {
        payload: "processed after discard",
        outcome: "reply",
      })
      expect(finalized.resolution).toBeUndefined()
      const resolved = yield* store.getResolution(incoming.resolution.id)
      expect(resolved?.status).toBe("resolved")
      expect(resolved?.resolvedSourceID).toBe(next.source.id)
      expect(resolved?.timeResolved).toBeDefined()
      expect(yield* store.pending(root)).toEqual([])
    }),
  )

  it.effect("keeps a source reservation as a close blocker after its public work token is finished", () =>
    Effect.gen(function* () {
      const root = sessionID("close-source-root")
      const child = sessionID("close-source-child")
      const { store } = yield* setup({ id: root }, { id: child, parentID: root })
      const registered = yield* store.register(
        registration("close-source-child", root, child, { mode: "background" }),
      )
      yield* store.finishWork(registered.workID)
      const source = yield* store.reserveSource({
        sessionID: child,
        generationID: registered.generation.id,
        source: assistantSource("close-source"),
        historyCutoff: "history-close-source",
        consumed: [],
      })

      yield* store.finishWork(source.source.workID)
      expect(yield* store.unfinished(child)).toEqual([])
      const blocked = yield* close(store, registered.generation.id)
      expect(blocked.closed).toBe(false)
      expect(blocked.blockers).toContain(`source:${source.source.id}`)

      expect((yield* store.discardSource(source.source.id)).state).toBe("discarded")
      expect((yield* close(store, registered.generation.id)).closed).toBe(true)
    }),
  )

  it.effect("allows terminal cancellation after the own session stops and abandons other terminal sources", () =>
    Effect.gen(function* () {
      const root = sessionID("terminal-stop-root")
      const child = sessionID("terminal-stop-child")
      const { store } = yield* setup({ id: root }, { id: child, parentID: root })
      const registered = yield* store.register(registration("terminal-stop-child", root, child))
      yield* store.finishWork(registered.workID)

      const revoked = yield* store.revokeDescendants(child)
      expect((yield* store.get(registered.generation.id))?.state).toBe("active")

      const assistant = yield* store
        .reserveSource({
          sessionID: child,
          generationID: registered.generation.id,
          source: assistantSource("terminal-stop-assistant"),
          historyCutoff: "history-terminal-stop-assistant",
          consumed: [],
        })
        .pipe(Effect.exit)
      expectFailure(assistant)

      const abandoned = yield* store.reserveSource({
        sessionID: child,
        generationID: registered.generation.id,
        source: terminalSource("terminal-stop-abandoned"),
        historyCutoff: "history-terminal-stop-abandoned",
        consumed: [],
      })
      const cancellation = yield* store.reserveSource({
        sessionID: child,
        generationID: registered.generation.id,
        source: revoked.cancellationSource,
        historyCutoff: "history-terminal-stop-cancellation",
        consumed: [],
      })

      const nonCancellation = yield* store
        .finalizeSource(abandoned.source.id, { payload: "stale error", outcome: "error" })
        .pipe(Effect.exit)
      expectFailure(nonCancellation)

      const finalized = yield* store.finalizeSource(cancellation.source.id, {
        payload: "cancelled",
        outcome: "cancelled",
      })
      expect(finalized.resolution).toBeUndefined()
      const cancellationRetry = yield* store.finalizeSource(cancellation.source.id, {
        payload: "cancelled",
        outcome: "cancelled",
      })
      expect(cancellationRetry.source).toEqual(finalized.source)
      const changedCancellation = yield* store
        .finalizeSource(cancellation.source.id, { payload: "cancelled", outcome: "error" })
        .pipe(Effect.exit)
      expectFailure(changedCancellation)
      const sources = yield* store.sources(child)
      expect(sources.find((source) => source.id === abandoned.source.id)?.state).toBe("discarded")
      expect(sources.find((source) => source.id === cancellation.source.id)?.state).toBe("finalized")
      expect(yield* store.unfinished(child)).toEqual([])
    }),
  )

  it.effect("rejects stale source output after the parent session stops", () =>
    Effect.gen(function* () {
      const root = sessionID("parent-stop-root")
      const child = sessionID("parent-stop-child")
      const { store } = yield* setup({ id: root }, { id: child, parentID: root })
      const registered = yield* store.register(
        registration("parent-stop-child", root, child, { mode: "background" }),
      )
      yield* store.finishWork(registered.workID)
      const source = yield* store.reserveSource({
        sessionID: child,
        generationID: registered.generation.id,
        source: assistantSource("parent-stop-stale"),
        historyCutoff: "history-parent-stop-stale",
        consumed: [],
      })

      yield* store.revokeDescendants(root)
      expect((yield* store.get(registered.generation.id))?.state).toBe("revoked")
      expect(yield* store.unfinished(child)).toEqual([])

      const stale = yield* store
        .finalizeSource(source.source.id, { payload: "stale reply", outcome: "reply" })
        .pipe(Effect.exit)
      expectFailure(stale)
      expect(yield* store.pending(root)).toEqual([])
    }),
  )

  test("settles a pre-reserved assistant cancellation through child-only revocation after a disk reopen", async () => {
    await using temporary = await tmpdir()
    const filename = path.join(temporary.path, "delegation-source-revocation.sqlite")
    const root = sessionID("revocation-root")
    const child = sessionID("revocation-child")
    const grandchild = sessionID("revocation-grandchild")

    const state = await runDatabase(
      filename,
      Effect.gen(function* () {
        const { db, store } = yield* setup(
          { id: root },
          { id: child, parentID: root },
          { id: grandchild, parentID: child },
        )
        const childGeneration = yield* store.register(
          registration("revocation-child", root, child, { mode: "background" }),
        )
        const grandchildGeneration = yield* store.register(
          registration("revocation-grandchild", child, grandchild, {
            mode: "background",
            parentGenerationID: childGeneration.generation.id,
          }),
        )
        yield* store.finishWork(grandchildGeneration.workID)

        const grandchildSource = assistantSource("revocation-contribution")
        const grandchildReservation = yield* store.reserveSource({
          sessionID: grandchild,
          generationID: grandchildGeneration.generation.id,
          source: grandchildSource,
          historyCutoff: "history-revocation-contribution",
          consumed: [],
        })
        const grandchildFinalized = yield* store.finalizeSource(grandchildReservation.source.id, {
          payload: "grandchild return",
          outcome: "reply",
        })
        if (grandchildFinalized.resolution === undefined) {
          return yield* Effect.die("expected a grandchild return")
        }
        const preparedGrandchild = yield* store.prepare(
          grandchildFinalized.resolution.id,
          content("revocation-contribution"),
        )
        expect(yield* admitWithMessageParts(store, preparedGrandchild.id)).toEqual({ status: "admitted" })
        expect(yield* db.all("SELECT id FROM delegation_source_test_message")).toHaveLength(1)
        expect(yield* db.all("SELECT id FROM delegation_source_test_part")).toHaveLength(1)

        const childSource = assistantSource("revocation-child-original")
        const childReservation = yield* store.reserveSource({
          sessionID: child,
          generationID: childGeneration.generation.id,
          source: childSource,
          historyCutoff: "history-revocation-child-original",
          consumed: [grandchildFinalized.resolution.id],
        })
        const abandoned = yield* store.reserveSource({
          sessionID: child,
          generationID: childGeneration.generation.id,
          source: assistantSource("revocation-child-abandoned"),
          historyCutoff: "history-revocation-child-abandoned",
          consumed: [],
        })
        yield* store.startWork({
          id: Delegation.WorkID.create(),
          sessionID: child,
          generationID: childGeneration.generation.id,
          kind: "provider",
        })
        yield* store.startWork({
          id: Delegation.WorkID.create(),
          sessionID: child,
          generationID: childGeneration.generation.id,
          kind: "tool",
        })
        yield* store.startWork({
          id: Delegation.WorkID.create(),
          sessionID: child,
          generationID: childGeneration.generation.id,
          kind: "input",
        })

        const revoked = yield* store.revokeDescendants(child)
        expect((yield* store.get(childGeneration.generation.id))?.state).toBe("active")
        expect((yield* store.get(grandchildGeneration.generation.id))?.state).toBe("revoked")
        expect((yield* store.getResolution(grandchildFinalized.resolution.id))?.status).toBe("revoked")
        expect(yield* store.unfinished(child)).toEqual([])
        expect(yield* store.unfinished(grandchild)).toEqual([])

        return {
          childGenerationID: childGeneration.generation.id,
          grandchildGenerationID: grandchildGeneration.generation.id,
          contributionID: grandchildFinalized.resolution.id,
          childSourceID: childReservation.source.id,
          childSource,
          abandonedSourceID: abandoned.source.id,
          cancellationSource: revoked.cancellationSource,
          payload: "child cancelled after revocation",
          historyCutoff: "history-revocation-child-original",
        }
      }),
    )

    await runDatabase(
      filename,
      Effect.gen(function* () {
        const database = yield* Database.Service
        const db = database.db
        const store = DelegationStore.make(db)

        expect((yield* store.get(state.childGenerationID))?.state).toBe("active")
        expect((yield* store.get(state.grandchildGenerationID))?.state).toBe("revoked")
        const contribution = yield* store.getResolution(state.contributionID)
        expect(contribution?.status).toBe("revoked")

        const finalized = yield* store.finalizeSource(state.childSourceID, {
          payload: state.payload,
          outcome: "cancelled",
        })
        expect(finalized.source.id).toBe(state.childSourceID)
        expect(finalized.source.source).toEqual(state.childSource)
        if (finalized.resolution === undefined) return yield* Effect.die("expected child cancellation return")

        const resolvedContribution = yield* store.getResolution(state.contributionID)
        expect(resolvedContribution?.status).toBe("revoked")
        expect(resolvedContribution?.resolvedSourceID).toBe(state.childSourceID)
        expect(resolvedContribution?.timeResolved).toBeDefined()
        expect(yield* store.pending(root)).toEqual([
          expect.objectContaining({
            id: finalized.resolution.id,
            parentID: root,
            childID: child,
            source: state.childSource,
            outcome: "cancelled",
            consumed: [state.contributionID],
            status: "pending",
          }),
        ])

        const exactFinalization = yield* store.finalizeSource(state.childSourceID, {
          payload: state.payload,
          outcome: "cancelled",
        })
        expect(exactFinalization.source.id).toBe(state.childSourceID)
        expect(exactFinalization.resolution?.id).toBe(finalized.resolution.id)
        const captureRetry = yield* store.capture(
          state.childGenerationID,
          capture(state.childSource, state.payload, state.historyCutoff, [state.contributionID], "cancelled"),
        )
        expect(captureRetry.id).toBe(finalized.resolution.id)
        expect(captureRetry.messageID).toBe(finalized.resolution.messageID)
        expect(yield* db.all("SELECT id FROM delegation_resolution")).toHaveLength(2)

        const prepared = yield* store.prepare(finalized.resolution.id, content("revocation-child-original"))
        if (prepared.envelope === undefined) return yield* Effect.die("expected prepared child cancellation return")
        expect(prepared.envelope.parts).toHaveLength(1)
        const preparedRetry = yield* store.prepare(finalized.resolution.id, content("revocation-child-original"))
        expect(preparedRetry.envelope).toEqual(prepared.envelope)
        expect(yield* db.all("SELECT id FROM delegation_source_test_message")).toHaveLength(1)
        expect(yield* db.all("SELECT id FROM delegation_source_test_part")).toHaveLength(1)
        expect(yield* db.all("SELECT id FROM delegation_source_test_receipt")).toHaveLength(1)

        expect(yield* admitWithMessageParts(store, prepared.id)).toEqual({ status: "admitted" })
        expect(yield* admitWithMessageParts(store, prepared.id)).toEqual({ status: "admitted" })
        expect(yield* db.all("SELECT id FROM delegation_source_test_message")).toHaveLength(2)
        expect(yield* db.all("SELECT id FROM delegation_source_test_part")).toHaveLength(2)
        expect(yield* db.all("SELECT id FROM delegation_source_test_receipt")).toHaveLength(2)
        expect((yield* store.getResolution(prepared.id))?.status).toBe("admitted")
        expect(yield* store.pending(root)).toEqual([])

        const sources = yield* store.sources(child)
        expect(sources).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ id: state.childSourceID, state: "finalized" }),
            expect.objectContaining({ id: state.abandonedSourceID, state: "discarded" }),
          ]),
        )
        expect(yield* store.unfinishedSources(child)).toEqual([])
        expect((yield* store.listWork(state.childGenerationID)).every((work) => work.state === "finished")).toBe(true)
        expect(yield* store.unfinished(child)).toEqual([])
        expect(yield* store.incoming(child)).toEqual([])

        const closed = yield* close(store, state.childGenerationID)
        expect(closed.closed).toBe(true)
        expect(closed.blockers).toEqual([])
        const closedRetry = yield* close(store, state.childGenerationID)
        expect(closedRetry.closed).toBe(true)
        expect(closedRetry.blockers).toEqual([])

        yield* store.allowSession(child)
        const replacement = yield* store.register(
          registration("revocation-replacement", root, child, {
            explicitReuse: true,
            mode: "background",
          }),
        )
        expect(replacement.generation.id).not.toBe(state.childGenerationID)

        const staleReply = yield* store
          .capture(
            replacement.generation.id,
            capture(state.childSource, "stale reply", "history-stale-reply", [], "reply"),
          )
          .pipe(Effect.exit)
        expectFailure(staleReply)
        const staleCancellation = yield* store
          .capture(
            replacement.generation.id,
            capture(state.cancellationSource, "stale cancellation", "history-stale-cancellation", [], "cancelled"),
          )
          .pipe(Effect.exit)
        expectFailure(staleCancellation)
        expect(yield* db.all("SELECT id FROM delegation_resolution")).toHaveLength(2)
        expect(yield* store.pending(root)).toEqual([])

        yield* store.finishWork(replacement.workID)
        expect((yield* close(store, replacement.generation.id)).closed).toBe(true)
      }),
    )
  })

  it.effect("rejects assistant output after stop or parent deletion and cleans cancelled root sources without echo", () =>
    Effect.gen(function* () {
      const root = sessionID("post-stop-root")
      const child = sessionID("post-stop-child")
      const rootCleanup = sessionID("post-stop-root-cleanup")
      const { db, store } = yield* setup(
        { id: root },
        { id: child, parentID: root },
        { id: rootCleanup },
      )
      const generation = yield* store.register(
        registration("post-stop-child", root, child, { mode: "background" }),
      )
      const source = assistantSource("post-stop-pre-reserved")
      const reservation = yield* store.reserveSource({
        sessionID: child,
        generationID: generation.generation.id,
        source,
        historyCutoff: "history-post-stop-pre-reserved",
        consumed: [],
      })

      yield* store.revokeDescendants(child)
      const replyAfterStop = yield* store
        .finalizeSource(reservation.source.id, { payload: "late reply", outcome: "reply" })
        .pipe(Effect.exit)
      expectFailure(replyAfterStop)
      const errorAfterStop = yield* store
        .finalizeSource(reservation.source.id, { payload: "late error", outcome: "error" })
        .pipe(Effect.exit)
      expectFailure(errorAfterStop)
      const newAssistantUnderFence = yield* store
        .reserveSource({
          sessionID: child,
          generationID: generation.generation.id,
          source: assistantSource("post-stop-new-assistant"),
          historyCutoff: "history-post-stop-new-assistant",
          consumed: [],
        })
        .pipe(Effect.exit)
      expectFailure(newAssistantUnderFence)

      yield* store.revokeDescendants(root)
      expect((yield* store.get(generation.generation.id))?.state).toBe("revoked")
      const newAssistantAfterParentStop = yield* store
        .reserveSource({
          sessionID: child,
          generationID: generation.generation.id,
          source: assistantSource("post-parent-stop-new-assistant"),
          historyCutoff: "history-post-parent-stop-new-assistant",
          consumed: [],
        })
        .pipe(Effect.exit)
      expectFailure(newAssistantAfterParentStop)
      const cancellationAfterParentStop = yield* store
        .finalizeSource(reservation.source.id, { payload: "late cancellation", outcome: "cancelled" })
        .pipe(Effect.exit)
      expectFailure(cancellationAfterParentStop)

      yield* db.delete(SessionTable).where(eq(SessionTable.id, root)).run()
      const cancellationAfterDelete = yield* store
        .finalizeSource(reservation.source.id, { payload: "late cancellation", outcome: "cancelled" })
        .pipe(Effect.exit)
      expectFailure(cancellationAfterDelete)
      expect(yield* db.all("SELECT id FROM delegation_resolution")).toHaveLength(0)
      expect(yield* db.all("SELECT id FROM delegation_source_test_message")).toHaveLength(0)
      expect(yield* db.all("SELECT id FROM delegation_source_test_part")).toHaveLength(0)
      expect(yield* db.all("SELECT id FROM delegation_source_test_receipt")).toHaveLength(0)
      expect(yield* store.pending(root)).toEqual([])
      expect(yield* store.incoming(root)).toEqual([])
      expect(yield* store.unfinished(child)).toEqual([])
      expect(yield* store.sources(child)).toEqual([
        expect.objectContaining({ id: reservation.source.id, state: "reserved" }),
      ])

      const rootCancellation = yield* store.reserveSource({
        sessionID: rootCleanup,
        source: assistantSource("root-null-generation-cancel"),
        historyCutoff: "history-root-null-generation-cancel",
        consumed: [],
      })
      const rootAbandoned = yield* store.reserveSource({
        sessionID: rootCleanup,
        source: assistantSource("root-null-generation-abandoned"),
        historyCutoff: "history-root-null-generation-abandoned",
        consumed: [],
      })
      const rootFinalized = yield* store.finalizeSource(rootCancellation.source.id, {
        payload: "root cancelled",
        outcome: "cancelled",
      })
      expect(rootFinalized.resolution).toBeUndefined()
      expect(yield* store.finalizeSource(rootCancellation.source.id, {
        payload: "root cancelled",
        outcome: "cancelled",
      })).toEqual(rootFinalized)
      expect(yield* store.sources(rootCleanup)).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: rootCancellation.source.id, state: "finalized" }),
          expect.objectContaining({ id: rootAbandoned.source.id, state: "discarded" }),
        ]),
      )
      expect(yield* store.unfinishedSources(rootCleanup)).toEqual([])
      expect(yield* store.unfinished(rootCleanup)).toEqual([])
      expect(yield* store.pending(rootCleanup)).toEqual([])
      expect(yield* store.incoming(rootCleanup)).toEqual([])
    }),
  )
})
