import { expect } from "bun:test"
import { asc, eq } from "drizzle-orm"
import { Cause, Effect, Exit, Schema } from "effect"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { Database } from "@opencode-ai/core/database/database"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { MessageTable, PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Model } from "@opencode-ai/schema/model"
import { Provider } from "@opencode-ai/schema/provider"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { SessionID } from "@opencode-ai/schema/session-id"
import { SessionDelegation } from "../../src/session/delegation"
import { testEffect } from "../lib/effect"

const it = testEffect(Database.layerFromPath(":memory:"))

type DB = Database.Interface["db"]
type ModelSelection = {
  readonly id: string
  readonly providerID: string
  readonly variant?: string
}
type SessionFixture = {
  readonly id: SessionID
  readonly parentID?: SessionID
  readonly agent?: string | null
  readonly model?: ModelSelection | null
}
type ModelRef = SessionV1.User["model"]

const model = (providerID: string, modelID: string, variant?: string): ModelRef => ({
  providerID: Provider.ID.make(providerID),
  modelID: Model.ID.make(modelID),
  ...(variant === undefined ? {} : { variant }),
})

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
          directory: AbsolutePath.make("/project"),
          title: session.id,
          version: "test",
          agent: session.agent === undefined ? "parent-agent" : session.agent,
          model: session.model === undefined ? null : session.model,
        })),
      )
      .run()
      .pipe(Effect.orDie)

    const core = DelegationStore.make(db)
    return { db, core, delegation: SessionDelegation.make(db, core) }
  })

function sessionID(name: string) {
  return SessionID.make(`ses_session_delegation_${name}`)
}

function origin(name: string): Delegation.Origin {
  return {
    messageID: SessionMessage.ID.make(`msg_session_delegation_${name}`),
    partID: Delegation.OriginPartID.make(`prt_session_delegation_${name}`),
    callID: `call_session_delegation_${name}`,
  }
}

function userInfo(sessionID: SessionID, id: string, created: number, infoModel: ModelRef, agent = "agent") {
  return {
    id: SessionV1.MessageID.make(id),
    sessionID,
    role: "user" as const,
    time: { created },
    agent,
    model: infoModel,
  } satisfies SessionV1.User
}

function userMessage(
  sessionID: SessionID,
  name: string,
  created: number,
  infoModel: ModelRef,
  agent = "agent",
  messageID = `msg_session_delegation_${name}`,
) {
  const info = userInfo(sessionID, messageID, created, infoModel, agent)
  const part = {
    id: SessionV1.PartID.make(`prt_session_delegation_${name}`),
    sessionID,
    messageID: info.id,
    type: "text" as const,
    text: `history-${name}`,
  } satisfies SessionV1.TextPart
  return { info, parts: [part] } satisfies SessionV1.WithParts
}

function assistantMessage(
  sessionID: SessionID,
  id: string,
  parentID: string,
  created: number,
  infoModel: ModelRef,
  options: { readonly agent?: string; readonly summary?: boolean; readonly finish?: string } = {},
) {
  const info = {
    id: SessionV1.MessageID.make(id),
    sessionID,
    role: "assistant" as const,
    time: { created },
    parentID: SessionV1.MessageID.make(parentID),
    modelID: infoModel.modelID,
    providerID: infoModel.providerID,
    ...(infoModel.variant === undefined ? {} : { variant: infoModel.variant }),
    mode: "",
    agent: options.agent ?? "agent",
    path: { cwd: "/project", root: "/project" },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    ...(options.summary === undefined ? {} : { summary: options.summary }),
    ...(options.finish === undefined ? {} : { finish: options.finish }),
  } satisfies SessionV1.Assistant
  return { info, parts: [] } satisfies SessionV1.WithParts
}

function compactionMessage(sessionID: SessionID, name: string, created: number, tailStartID: SessionV1.MessageID) {
  const info = userInfo(sessionID, `msg_session_delegation_${name}`, created, model("history", "history"))
  const part = {
    id: SessionV1.PartID.make(`prt_session_delegation_${name}`),
    sessionID,
    messageID: info.id,
    type: "compaction" as const,
    auto: true,
    tail_start_id: tailStartID,
  } satisfies SessionV1.CompactionPart
  return { info, parts: [part] } satisfies SessionV1.WithParts
}

function messageData(info: SessionV1.Info) {
  const { id: _, sessionID: __, ...data } = info
  return data
}

function insertMessage(db: DB, message: SessionV1.WithParts) {
  return db
    .insert(MessageTable)
    .values({
      id: message.info.id,
      session_id: message.info.sessionID,
      time_created: message.info.time.created,
      data: messageData(message.info),
    })
    .run()
    .pipe(Effect.orDie)
}

function registerResolution(
  core: DelegationStore.Interface,
  name: string,
  parentID: SessionID,
  childID: SessionID,
  registrationOrigin = origin(name),
) {
  return Effect.gen(function* () {
    const registered = yield* core.register({
      requestID: Delegation.RequestID.make(`drq_session_delegation_${name}`),
      generationID: Delegation.ID.make(`dlg_session_delegation_${name}`),
      parentID,
      childID,
      origin: registrationOrigin,
      mode: "background",
      explicitReuse: false,
    })
    yield* core.finishWork(registered.workID)

    const reserved = yield* core.reserveSource({
      sessionID: childID,
      generationID: registered.generation.id,
      source: { kind: "assistant", id: `assistant_session_delegation_${name}` },
      historyCutoff: `history_session_delegation_${name}`,
      consumed: [],
    })
    const finalized = yield* core.finalizeSource(reserved.source.id, {
      payload: `payload-${name}`,
      outcome: "reply",
    })
    if (finalized.resolution === undefined) return yield* Effect.die(`expected a resolution for ${name}`)
    return { generation: registered.generation, resolution: finalized.resolution }
  })
}

function renderResult(resolution: Delegation.Resolution) {
  return [
    "<task>",
    `<task_id>${resolution.childID}</task_id>`,
    "<task_result>",
    resolution.payload,
    "</task_result>",
    "</task>",
  ].join("\n")
}

function recipientContent(resolution: Delegation.Resolution, agent: string, infoModel: ModelRef) {
  const message: Omit<SessionV1.User, "id" | "sessionID"> = {
    role: "user",
    time: { created: resolution.timeCreated },
    agent,
    model: infoModel,
  }
  return {
    message,
    parts: [{ type: "text", synthetic: true, text: renderResult(resolution) } as const],
  } satisfies Delegation.RecipientContent
}

function multipartContent(resolution: Delegation.Resolution, agent: string, infoModel: ModelRef) {
  const message: Omit<SessionV1.User, "id" | "sessionID"> = {
    role: "user",
    time: { created: resolution.timeCreated },
    agent,
    model: infoModel,
  }
  return {
    message,
    parts: [
      { type: "text", synthetic: true, text: renderResult(resolution) } as const,
      { type: "text", synthetic: false, text: `detail-${resolution.childID}` } as const,
      { type: "text", synthetic: false, text: `cutoff-${resolution.historyCutoff}` } as const,
    ],
  } satisfies Delegation.RecipientContent
}

function prepare(
  core: DelegationStore.Interface,
  resolution: Delegation.Resolution,
  content: Delegation.RecipientContent,
) {
  return Effect.gen(function* () {
    const prepared = yield* core.prepare(resolution.id, content)
    if (prepared.envelope === undefined) return yield* Effect.die("expected a prepared envelope")
    return { ...prepared, envelope: prepared.envelope }
  })
}

type PreparedResolution = Effect.Success<ReturnType<typeof prepare>>

function receiveStandalone(db: DB, prepared: PreparedResolution) {
  return db.transaction((tx) => SessionDelegation.receive(tx, prepared), { behavior: "immediate" })
}

function projection(db: DB) {
  return Effect.gen(function* () {
    const messages = yield* db.select().from(MessageTable).orderBy(asc(MessageTable.id)).all()
    const parts = yield* db.select().from(PartTable).orderBy(asc(PartTable.id)).all()
    return { messages, parts }
  })
}

type Projection = Effect.Success<ReturnType<typeof projection>>

function persistPreparedProjection(
  db: DB,
  core: DelegationStore.Interface,
  resolution: Delegation.Resolution,
  agent: string,
  infoModel: ModelRef,
) {
  return Effect.gen(function* () {
    const content = multipartContent(resolution, agent, infoModel)
    const prepared = yield* prepare(core, resolution, content)
    expect(yield* receiveStandalone(db, prepared)).toEqual({ status: "admitted" })
    return { content, prepared }
  })
}

function expectConflict(db: DB, core: DelegationStore.Interface, prepared: PreparedResolution, before: Projection) {
  return Effect.gen(function* () {
    const failed = yield* core.admit(prepared.id, SessionDelegation.receive).pipe(Effect.exit)
    expectFailureCode(failed, "receiver_conflict")
    expect((yield* core.getResolution(prepared.id))?.status).toBe("pending")
    expect(yield* projection(db)).toEqual(before)
  })
}

function resolutionState(resolution: Delegation.Resolution) {
  return {
    status: resolution.status,
    timeCreated: resolution.timeCreated,
    timeAdmitted: resolution.timeAdmitted,
    timeConsumed: resolution.timeConsumed,
    timeResolved: resolution.timeResolved,
  }
}

function storedMessageID(id: string) {
  return SessionV1.MessageID.make(id)
}

function storedPartID(id: string) {
  return SessionV1.PartID.make(id)
}

function syntheticPartData(
  resolution: Delegation.Resolution,
  text = renderResult(resolution),
): Omit<SessionV1.TextPart, "id" | "sessionID" | "messageID"> {
  return {
    type: "text" as const,
    synthetic: true,
    text,
  }
}

function syntheticPartDataWithProvenance(
  resolution: Delegation.Resolution,
  provenance: Delegation.Provenance,
  text = renderResult(resolution),
): Omit<SessionV1.TextPart, "id" | "sessionID" | "messageID"> {
  return {
    ...syntheticPartData(resolution, text),
    metadata: { delegation: provenance },
  }
}

function expectedMultipartData(resolution: Delegation.Resolution, provenance: Delegation.Provenance) {
  return [
    {
      type: "text" as const,
      synthetic: true,
      text: renderResult(resolution),
      metadata: { delegation: provenance },
    },
    { type: "text" as const, synthetic: false, text: `detail-${resolution.childID}` },
    { type: "text" as const, synthetic: false, text: `cutoff-${resolution.historyCutoff}` },
  ]
}

function expectFailureCode<A, E>(exit: Exit.Exit<A, E>, code: string) {
  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toMatchObject({ code })
}

function expectAgentUnavailable(
  db: DB,
  core: DelegationStore.Interface,
  delegation: SessionDelegation.Interface,
  resolution: Delegation.Resolution,
) {
  return Effect.gen(function* () {
    const before = yield* projection(db)
    const failed = yield* delegation.deliver(resolution.parentID).pipe(Effect.exit)
    expectFailureCode(failed, "agent_unavailable")
    const after = yield* core.getResolution(resolution.id)
    expect(after?.status).toBe("pending")
    expect(after?.envelope).toBeUndefined()
    expect(yield* projection(db)).toEqual(before)
  })
}

it.effect("delivers a canonical synthetic user message using the latest parent user model", () =>
  Effect.gen(function* () {
    const parentID = sessionID("latest-user-parent")
    const childID = sessionID("latest-user-child")
    const latestModel = model("latest-provider", "latest-model", "latest-variant")
    const { db, core, delegation } = yield* setup(
      {
        id: parentID,
        model: { providerID: "session-provider", id: "session-model", variant: "wrong" },
      },
      { id: childID, parentID },
    )

    yield* insertMessage(db, userMessage(parentID, "latest-user-old", 10, model("old-provider", "old-model")))
    yield* insertMessage(db, userMessage(parentID, "latest-user-new", 20, latestModel))
    const created = yield* registerResolution(core, "latest-user", parentID, childID)

    expect(yield* delegation.deliver(parentID)).toEqual([parentID])
    const message = yield* db
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.id, storedMessageID(created.resolution.messageID)))
      .get()
    expect(message?.session_id).toBe(parentID)
    expect(message?.data).toMatchObject({
      role: "user",
      agent: "parent-agent",
      model: { providerID: "latest-provider", modelID: "latest-model", variant: "latest-variant" },
    })

    const parts = yield* db
      .select()
      .from(PartTable)
      .where(eq(PartTable.message_id, storedMessageID(created.resolution.messageID)))
      .all()
    expect(parts).toHaveLength(1)
    expect(parts[0]?.data).toMatchObject({
      type: "text",
      synthetic: true,
      text: renderResult(created.resolution),
      metadata: { delegation: expect.objectContaining({ generationID: created.generation.id, childID }) },
    })
  }),
)

it.effect("uses the Task-origin assistant model when the parent has no user message", () =>
  Effect.gen(function* () {
    const parentID = sessionID("task-origin-parent")
    const childID = sessionID("task-origin-child")
    const taskOrigin = origin("task-origin")
    const taskModel = model("task-provider", "task-model", "task-variant")
    const { db, core, delegation } = yield* setup(
      {
        id: parentID,
        model: { providerID: "session-provider", id: "session-model", variant: "wrong" },
      },
      { id: childID, parentID },
    )

    yield* insertMessage(
      db,
      assistantMessage(parentID, taskOrigin.messageID, "msg_task_prompt", 10, taskModel, { finish: "tool-calls" }),
    )
    const created = yield* registerResolution(core, "task-origin", parentID, childID, taskOrigin)

    expect(yield* delegation.deliver(parentID)).toEqual([parentID])
    const message = yield* db
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.id, storedMessageID(created.resolution.messageID)))
      .get()
    expect(message?.data).toMatchObject({
      role: "user",
      model: { providerID: "task-provider", modelID: "task-model", variant: "task-variant" },
    })
  }),
)

it.effect("uses the latest parent user agent when the session agent is null", () =>
  Effect.gen(function* () {
    const parentID = sessionID("latest-user-agent-fallback-parent")
    const childID = sessionID("latest-user-agent-fallback-child")
    const taskOrigin = origin("latest-user-agent-fallback")
    const latestModel = model("latest-agent-provider", "latest-agent-model", "latest-agent-variant")
    const { db, core, delegation } = yield* setup({ id: parentID, agent: null }, { id: childID, parentID })
    yield* insertMessage(
      db,
      assistantMessage(
        parentID,
        taskOrigin.messageID,
        "msg_latest-user-agent-task",
        5,
        model("origin-provider", "origin-model"),
        { agent: "origin-agent" },
      ),
    )
    yield* insertMessage(
      db,
      userMessage(
        parentID,
        "latest-user-agent-old",
        10,
        model("old-agent-provider", "old-agent-model"),
        "older-user-agent",
      ),
    )
    yield* insertMessage(db, userMessage(parentID, "latest-user-agent-new", 20, latestModel, "latest-user-agent"))
    const created = yield* registerResolution(core, "latest-user-agent-fallback", parentID, childID, taskOrigin)

    expect(yield* delegation.deliver(parentID)).toEqual([parentID])
    const message = yield* db
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.id, storedMessageID(created.resolution.messageID)))
      .get()
    expect(message?.data).toMatchObject({ agent: "latest-user-agent", model: latestModel })
  }),
)

it.effect("uses the origin agent when the latest user agent is empty while preserving the latest user model", () =>
  Effect.gen(function* () {
    const parentID = sessionID("latest-empty-origin-agent-parent")
    const childID = sessionID("latest-empty-origin-agent-child")
    const taskOrigin = origin("latest-empty-origin-agent")
    const latestModel = model("latest-empty-provider", "latest-empty-model", "latest-empty-variant")
    const originModel = model("origin-fallback-provider", "origin-fallback-model", "origin-fallback-variant")
    const { db, core, delegation } = yield* setup({ id: parentID, agent: null }, { id: childID, parentID })
    yield* insertMessage(
      db,
      assistantMessage(parentID, taskOrigin.messageID, "msg_latest-empty-origin-task", 50, originModel, {
        agent: "origin-fallback-agent",
      }),
    )
    yield* insertMessage(
      db,
      userMessage(
        parentID,
        "latest-empty-origin-older-user",
        100,
        model("older-provider", "older-model"),
        "older-user-agent",
      ),
    )
    yield* insertMessage(db, userMessage(parentID, "latest-empty-origin-latest-user", 200, latestModel, ""))
    const created = yield* registerResolution(core, "latest-empty-origin-agent", parentID, childID, taskOrigin)

    expect(yield* delegation.deliver(parentID)).toEqual([parentID])
    const message = yield* db
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.id, storedMessageID(created.resolution.messageID)))
      .get()
    expect(message?.data).toMatchObject({ agent: "origin-fallback-agent", model: latestModel })
  }),
)

it.effect("uses a latest parent user agent when the session agent is empty", () =>
  Effect.gen(function* () {
    const parentID = sessionID("empty-session-agent-parent")
    const childID = sessionID("empty-session-agent-child")
    const userModel = model("empty-session-provider", "empty-session-model", "empty-session-variant")
    const { db, core, delegation } = yield* setup({ id: parentID, agent: "" }, { id: childID, parentID })
    yield* insertMessage(db, userMessage(parentID, "empty-session-user", 10, userModel, "empty-session-user-agent"))
    const created = yield* registerResolution(core, "empty-session-agent", parentID, childID)

    expect(yield* delegation.deliver(parentID)).toEqual([parentID])
    const message = yield* db
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.id, storedMessageID(created.resolution.messageID)))
      .get()
    expect(message?.data).toMatchObject({ agent: "empty-session-user-agent", model: userModel })
  }),
)

it.effect("uses the Task-origin assistant agent instead of a newer arbitrary assistant", () =>
  Effect.gen(function* () {
    const parentID = sessionID("origin-agent-fallback-parent")
    const childID = sessionID("origin-agent-fallback-child")
    const taskOrigin = origin("origin-agent-fallback")
    const originModel = model("origin-agent-provider", "origin-agent-model", "origin-agent-variant")
    const { db, core, delegation } = yield* setup({ id: parentID, agent: null }, { id: childID, parentID })
    yield* insertMessage(
      db,
      assistantMessage(parentID, taskOrigin.messageID, "msg_origin-agent-task", 10, originModel, {
        agent: "origin-agent",
      }),
    )
    yield* insertMessage(
      db,
      assistantMessage(
        parentID,
        "msg_origin-agent-newer-assistant",
        "msg_origin-agent-task",
        20,
        model("newer-provider", "newer-model"),
        { agent: "newer-arbitrary-agent" },
      ),
    )
    const created = yield* registerResolution(core, "origin-agent-fallback", parentID, childID, taskOrigin)

    expect(yield* delegation.deliver(parentID)).toEqual([parentID])
    const message = yield* db
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.id, storedMessageID(created.resolution.messageID)))
      .get()
    expect(message?.data).toMatchObject({ agent: "origin-agent", model: originModel })
  }),
)

it.effect("rejects delivery when no recipient agent candidate exists", () =>
  Effect.gen(function* () {
    const parentID = sessionID("no-agent-candidate-parent")
    const childID = sessionID("no-agent-candidate-child")
    const { db, core, delegation } = yield* setup({ id: parentID, agent: null }, { id: childID, parentID })
    const created = yield* registerResolution(core, "no-agent-candidate", parentID, childID)

    yield* expectAgentUnavailable(db, core, delegation, created.resolution)
  }),
)

it.effect("rejects delivery when the latest user agent and Task origin agent are empty", () =>
  Effect.gen(function* () {
    const parentID = sessionID("empty-agent-candidates-parent")
    const childID = sessionID("empty-agent-candidates-child")
    const taskOrigin = origin("empty-agent-candidates")
    const { db, core, delegation } = yield* setup({ id: parentID, agent: null }, { id: childID, parentID })
    yield* insertMessage(
      db,
      assistantMessage(
        parentID,
        taskOrigin.messageID,
        "msg_empty-agent-task",
        5,
        model("empty-origin-provider", "empty-origin-model"),
        { agent: "" },
      ),
    )
    yield* insertMessage(
      db,
      userMessage(parentID, "empty-agent-older-user", 10, model("older-provider", "older-model"), "older-user-agent"),
    )
    yield* insertMessage(
      db,
      userMessage(parentID, "empty-agent-latest-user", 20, model("latest-provider", "latest-model"), ""),
    )
    const created = yield* registerResolution(core, "empty-agent-candidates", parentID, childID, taskOrigin)

    yield* expectAgentUnavailable(db, core, delegation, created.resolution)
  }),
)

it.effect("does not use a Task-origin assistant from another session", () =>
  Effect.gen(function* () {
    const parentID = sessionID("wrong-origin-session-parent")
    const otherParentID = sessionID("wrong-origin-session-other-parent")
    const childID = sessionID("wrong-origin-session-child")
    const taskOrigin = origin("wrong-origin-session")
    const { db, core, delegation } = yield* setup(
      { id: parentID, agent: null },
      { id: otherParentID },
      { id: childID, parentID },
    )
    yield* insertMessage(
      db,
      userMessage(parentID, "wrong-origin-session-latest-user", 200, model("latest-provider", "latest-model"), ""),
    )
    yield* insertMessage(
      db,
      assistantMessage(
        otherParentID,
        taskOrigin.messageID,
        "msg_wrong-origin-session-task",
        10,
        model("wrong-session-provider", "wrong-session-model"),
        { agent: "wrong-session-agent" },
      ),
    )
    const created = yield* registerResolution(core, "wrong-origin-session", parentID, childID, taskOrigin)

    yield* expectAgentUnavailable(db, core, delegation, created.resolution)
  }),
)

it.effect("does not use a Task-origin row with the wrong message role", () =>
  Effect.gen(function* () {
    const parentID = sessionID("wrong-origin-role-parent")
    const childID = sessionID("wrong-origin-role-child")
    const taskOrigin = origin("wrong-origin-role")
    const { db, core, delegation } = yield* setup({ id: parentID, agent: null }, { id: childID, parentID })
    yield* insertMessage(
      db,
      userMessage(
        parentID,
        "wrong-origin-role-task",
        100,
        model("wrong-role-provider", "wrong-role-model"),
        "not-assistant",
        storedMessageID(taskOrigin.messageID),
      ),
    )
    yield* insertMessage(
      db,
      userMessage(parentID, "wrong-origin-role-latest-user", 200, model("latest-provider", "latest-model"), ""),
    )
    const created = yield* registerResolution(core, "wrong-origin-role", parentID, childID, taskOrigin)

    yield* expectAgentUnavailable(db, core, delegation, created.resolution)
  }),
)

it.effect("uses a frozen prepared envelope without rereading mutable recipient metadata", () =>
  Effect.gen(function* () {
    const parentID = sessionID("frozen-envelope-parent")
    const childID = sessionID("frozen-envelope-child")
    const taskOrigin = origin("frozen-envelope")
    const initialFallbackAgent = "frozen-fallback-agent"
    const frozenModel = model("frozen-provider", "frozen-model", "frozen-variant")
    const { db, core, delegation } = yield* setup({ id: parentID, agent: null }, { id: childID, parentID })
    const task = assistantMessage(
      parentID,
      taskOrigin.messageID,
      "msg_frozen-task-prompt",
      10,
      model("origin-provider", "origin-model"),
      { agent: "initial-origin-agent" },
    )
    const latestUser = userMessage(parentID, "frozen-latest-user", 20, frozenModel, initialFallbackAgent)
    yield* insertMessage(db, task)
    yield* insertMessage(db, latestUser)
    const created = yield* registerResolution(core, "frozen-envelope", parentID, childID, taskOrigin)
    const prepared = yield* prepare(
      core,
      created.resolution,
      recipientContent(created.resolution, initialFallbackAgent, frozenModel),
    )

    expect(prepared.envelope.message.data).toMatchObject({ agent: initialFallbackAgent, model: frozenModel })
    expect(Object.isFrozen(prepared.envelope)).toBe(true)
    expect(Object.isFrozen(prepared.envelope.message.data)).toBe(true)
    expect(Object.isFrozen(prepared.envelope.provenance)).toBe(true)

    yield* db
      .update(SessionTable)
      .set({ agent: null, model: { providerID: "changed-provider", id: "changed-model", variant: "changed" } })
      .where(eq(SessionTable.id, parentID))
      .run()
      .pipe(Effect.orDie)
    const changedOriginModel = model("changed-origin-provider", "changed-origin-model", "changed-origin-variant")
    const changedLatestUserData = {
      ...messageData(latestUser.info),
      agent: "changed-user-agent",
      model: model("changed-user-provider", "changed-user-model", "changed-user-variant"),
    }
    const changedOriginData = {
      ...messageData(task.info),
      agent: "changed-origin-agent",
      modelID: changedOriginModel.modelID,
      providerID: changedOriginModel.providerID,
      variant: changedOriginModel.variant,
    }
    yield* db
      .update(MessageTable)
      .set({ data: changedLatestUserData })
      .where(eq(MessageTable.id, latestUser.info.id))
      .run()
      .pipe(Effect.orDie)
    yield* db
      .update(MessageTable)
      .set({ data: changedOriginData })
      .where(eq(MessageTable.id, task.info.id))
      .run()
      .pipe(Effect.orDie)

    expect(yield* delegation.deliver(parentID)).toEqual([parentID])
    const message = yield* db
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.id, storedMessageID(created.resolution.messageID)))
      .get()
    expect(message?.data).toMatchObject({
      agent: initialFallbackAgent,
      model: { providerID: "frozen-provider", modelID: "frozen-model", variant: "frozen-variant" },
    })
  }),
)

it.effect("retries a complete standalone receiver projection before acknowledging the resolution", () =>
  Effect.gen(function* () {
    const parentID = sessionID("standalone-receive-parent")
    const childID = sessionID("standalone-receive-child")
    const infoModel = model("standalone-provider", "standalone-model", "standalone-variant")
    const { db, core } = yield* setup({ id: parentID }, { id: childID, parentID })
    const created = yield* registerResolution(core, "standalone-receive", parentID, childID)
    const content = multipartContent(created.resolution, "standalone-agent", infoModel)
    const prepared = yield* prepare(core, created.resolution, content)

    expect(yield* receiveStandalone(db, prepared)).toEqual({ status: "admitted" })
    expect((yield* core.getResolution(prepared.id))?.status).toBe("pending")

    const before = yield* projection(db)
    expect(before.messages).toHaveLength(1)
    expect(before.messages[0]).toMatchObject({
      id: storedMessageID(prepared.messageID),
      session_id: parentID,
      time_created: created.resolution.timeCreated,
      data: content.message,
    })
    expect(before.parts.map((part) => part.data)).toEqual(
      expectedMultipartData(created.resolution, prepared.envelope.provenance),
    )
    expect(
      before.parts.map((part) => ({
        id: part.id,
        message_id: part.message_id,
        session_id: part.session_id,
        time_created: part.time_created,
      })),
    ).toEqual(
      prepared.envelope.parts.map((part) => ({
        id: storedPartID(part.id),
        message_id: storedMessageID(prepared.messageID),
        session_id: parentID,
        time_created: created.resolution.timeCreated,
      })),
    )

    expect(yield* core.admit(prepared.id, SessionDelegation.receive)).toEqual({ status: "admitted" })
    expect(yield* projection(db)).toEqual(before)
    expect((yield* core.getResolution(prepared.id))?.status).toBe("admitted")
  }),
)

it.effect("repairs a partially persisted message and parts before acknowledging admission", () =>
  Effect.gen(function* () {
    const parentID = sessionID("partial-parent")
    const childID = sessionID("partial-child")
    const { db, core } = yield* setup({ id: parentID }, { id: childID, parentID })
    const created = yield* registerResolution(core, "partial", parentID, childID)
    const content = multipartContent(created.resolution, "partial-agent", model("partial-provider", "partial-model"))
    const prepared = yield* prepare(core, created.resolution, content)
    const partial = prepared.envelope.parts[1]
    if (partial === undefined) return yield* Effect.die("expected a second prepared part")
    const partialData = {
      type: "text" as const,
      synthetic: false,
      text: `detail-${created.resolution.childID}`,
    } satisfies Omit<SessionV1.TextPart, "id" | "sessionID" | "messageID">

    yield* db
      .insert(MessageTable)
      .values({
        id: storedMessageID(created.resolution.messageID),
        session_id: parentID,
        time_created: created.resolution.timeCreated,
        data: content.message,
      })
      .run()
      .pipe(Effect.orDie)
    const partialPart = {
      id: storedPartID(partial.id),
      message_id: storedMessageID(created.resolution.messageID),
      session_id: parentID,
      time_created: created.resolution.timeCreated,
      data: partialData,
    } satisfies typeof PartTable.$inferInsert
    yield* db.insert(PartTable).values(partialPart).run().pipe(Effect.orDie)

    expect(yield* core.admit(prepared.id, SessionDelegation.receive)).toEqual({ status: "admitted" })
    const parts = yield* db
      .select()
      .from(PartTable)
      .where(eq(PartTable.message_id, storedMessageID(created.resolution.messageID)))
      .orderBy(asc(PartTable.id))
      .all()
    expect(parts).toHaveLength(3)
    expect(parts.map((part) => part.id)).toEqual(prepared.envelope.parts.map((part) => storedPartID(part.id)))
    expect(parts[1]?.data).toMatchObject({
      type: "text",
      synthetic: false,
      text: `detail-${created.resolution.childID}`,
    })
  }),
)

it.effect("rejects envelope ownership collisions without making the return visible", () =>
  Effect.gen(function* () {
    const parentID = sessionID("envelope-collision-parent")
    const otherParentID = sessionID("envelope-collision-other-parent")
    const childID = sessionID("envelope-collision-child")
    const { db, core } = yield* setup({ id: parentID }, { id: otherParentID }, { id: childID, parentID })
    const created = yield* registerResolution(core, "envelope-collision", parentID, childID)
    const prepared = yield* prepare(
      core,
      created.resolution,
      recipientContent(created.resolution, "collision-agent", model("collision-provider", "collision-model")),
    )

    const failed = yield* core
      .admit(prepared.id, (tx, resolution) =>
        SessionDelegation.receive(tx, {
          ...resolution,
          envelope: {
            ...resolution.envelope,
            provenance: { ...resolution.envelope.provenance, parentID: otherParentID },
          },
        }),
      )
      .pipe(Effect.exit)
    expectFailureCode(failed, "receiver_conflict")
    expect((yield* core.getResolution(prepared.id))?.status).toBe("pending")
    expect(
      yield* db
        .select()
        .from(MessageTable)
        .where(eq(MessageTable.id, storedMessageID(prepared.messageID)))
        .all(),
    ).toEqual([])
  }),
)

it.effect("rejects persisted message and part ownership collisions", () =>
  Effect.gen(function* () {
    const parentID = sessionID("row-collision-parent")
    const otherParentID = sessionID("row-collision-other-parent")
    const messageChildID = sessionID("row-collision-message-child")
    const partChildID = sessionID("row-collision-part-child")
    const { db, core } = yield* setup(
      { id: parentID },
      { id: otherParentID },
      { id: messageChildID, parentID },
      { id: partChildID, parentID },
    )
    const messageCreated = yield* registerResolution(core, "row-message-collision", parentID, messageChildID)
    const messageContent = recipientContent(
      messageCreated.resolution,
      "message-collision-agent",
      model("message-collision-provider", "message-collision-model"),
    )
    const messagePrepared = yield* prepare(core, messageCreated.resolution, messageContent)
    yield* db
      .insert(MessageTable)
      .values({
        id: storedMessageID(messagePrepared.messageID),
        session_id: otherParentID,
        time_created: messageCreated.resolution.timeCreated,
        data: messageContent.message,
      })
      .run()
      .pipe(Effect.orDie)

    const messageFailed = yield* core.admit(messagePrepared.id, SessionDelegation.receive).pipe(Effect.exit)
    expectFailureCode(messageFailed, "receiver_conflict")
    expect((yield* core.getResolution(messagePrepared.id))?.status).toBe("pending")

    const partCreated = yield* registerResolution(core, "row-part-collision", parentID, partChildID)
    const partContent = recipientContent(
      partCreated.resolution,
      "part-collision-agent",
      model("part-collision-provider", "part-collision-model"),
    )
    const partPrepared = yield* prepare(core, partCreated.resolution, partContent)
    const host = userMessage(otherParentID, "part-collision-host", 1, model("host-provider", "host-model"))
    yield* insertMessage(db, host)
    const collidingPart = partPrepared.envelope.parts[0]
    if (collidingPart === undefined) return yield* Effect.die("expected a colliding part")
    yield* db
      .insert(PartTable)
      .values({
        id: SessionV1.PartID.make(collidingPart.id),
        message_id: host.info.id,
        session_id: otherParentID,
        time_created: partCreated.resolution.timeCreated,
        data: syntheticPartData(partCreated.resolution),
      })
      .run()
      .pipe(Effect.orDie)

    const partFailed = yield* core.admit(partPrepared.id, SessionDelegation.receive).pipe(Effect.exit)
    expectFailureCode(partFailed, "receiver_conflict")
    expect((yield* core.getResolution(partPrepared.id))?.status).toBe("pending")
    expect(
      yield* db
        .select()
        .from(MessageTable)
        .where(eq(MessageTable.id, storedMessageID(partPrepared.messageID)))
        .all(),
    ).toEqual([])
  }),
)

it.effect("rejects same-owner differing message JSON without changing the projection", () =>
  Effect.gen(function* () {
    const parentID = sessionID("message-json-collision-parent")
    const childID = sessionID("message-json-collision-child")
    const { db, core } = yield* setup({ id: parentID }, { id: childID, parentID })
    const created = yield* registerResolution(core, "message-json-collision", parentID, childID)
    const { content, prepared } = yield* persistPreparedProjection(
      db,
      core,
      created.resolution,
      "message-collision-agent",
      model("message-collision-provider", "message-collision-model"),
    )
    const changedMessage = { ...content.message, agent: "changed-agent" }
    yield* db
      .update(MessageTable)
      .set({ data: changedMessage })
      .where(eq(MessageTable.id, storedMessageID(prepared.messageID)))
      .run()
      .pipe(Effect.orDie)
    const before = yield* projection(db)
    yield* expectConflict(db, core, prepared, before)
  }),
)

it.effect("rejects same-owner differing part JSON without changing the projection", () =>
  Effect.gen(function* () {
    const parentID = sessionID("part-json-collision-parent")
    const childID = sessionID("part-json-collision-child")
    const { db, core } = yield* setup({ id: parentID }, { id: childID, parentID })
    const created = yield* registerResolution(core, "part-json-collision", parentID, childID)
    const { prepared } = yield* persistPreparedProjection(
      db,
      core,
      created.resolution,
      "part-json-collision-agent",
      model("part-json-collision-provider", "part-json-collision-model"),
    )
    const first = prepared.envelope.parts[0]
    if (first === undefined) return yield* Effect.die("expected a synthetic prepared part")
    yield* db
      .update(PartTable)
      .set({
        data: syntheticPartDataWithProvenance(created.resolution, prepared.envelope.provenance, "changed-part-text"),
      })
      .where(eq(PartTable.id, storedPartID(first.id)))
      .run()
      .pipe(Effect.orDie)
    const before = yield* projection(db)
    yield* expectConflict(db, core, prepared, before)
  }),
)

it.effect("rejects same-owner differing part provenance without changing the projection", () =>
  Effect.gen(function* () {
    const parentID = sessionID("part-provenance-collision-parent")
    const childID = sessionID("part-provenance-collision-child")
    const { db, core } = yield* setup({ id: parentID }, { id: childID, parentID })
    const created = yield* registerResolution(core, "part-provenance-collision", parentID, childID)
    const { prepared } = yield* persistPreparedProjection(
      db,
      core,
      created.resolution,
      "part-provenance-collision-agent",
      model("part-provenance-collision-provider", "part-provenance-collision-model"),
    )
    const first = prepared.envelope.parts[0]
    if (first === undefined) return yield* Effect.die("expected a synthetic prepared part")
    const changedProvenance = {
      ...prepared.envelope.provenance,
      historyCutoff: "changed-history-cutoff",
    }
    yield* db
      .update(PartTable)
      .set({ data: syntheticPartDataWithProvenance(created.resolution, changedProvenance) })
      .where(eq(PartTable.id, storedPartID(first.id)))
      .run()
      .pipe(Effect.orDie)
    const before = yield* projection(db)
    yield* expectConflict(db, core, prepared, before)
  }),
)

it.effect("rejects an unexpected extra part attached to the expected message", () =>
  Effect.gen(function* () {
    const parentID = sessionID("extra-part-collision-parent")
    const childID = sessionID("extra-part-collision-child")
    const { db, core } = yield* setup({ id: parentID }, { id: childID, parentID })
    const created = yield* registerResolution(core, "extra-part-collision", parentID, childID)
    const { prepared } = yield* persistPreparedProjection(
      db,
      core,
      created.resolution,
      "extra-part-collision-agent",
      model("extra-part-collision-provider", "extra-part-collision-model"),
    )
    yield* db
      .insert(PartTable)
      .values({
        id: storedPartID("prt_session_delegation_extra-part"),
        message_id: storedMessageID(prepared.messageID),
        session_id: parentID,
        time_created: created.resolution.timeCreated,
        data: syntheticPartData(created.resolution, "unexpected-extra-part"),
      })
      .run()
      .pipe(Effect.orDie)
    const before = yield* projection(db)
    yield* expectConflict(db, core, prepared, before)
  }),
)

it.effect("rejects exact canonical part data with only its message owner changed", () =>
  Effect.gen(function* () {
    const parentID = sessionID("message-owner-collision-parent")
    const childID = sessionID("message-owner-collision-child")
    const { db, core } = yield* setup({ id: parentID }, { id: childID, parentID })
    const created = yield* registerResolution(core, "message-owner-collision", parentID, childID)
    const { prepared } = yield* persistPreparedProjection(
      db,
      core,
      created.resolution,
      "message-owner-collision-agent",
      model("message-owner-collision-provider", "message-owner-collision-model"),
    )
    const host = userMessage(parentID, "message-owner-host", 1, model("host-provider", "host-model"))
    yield* insertMessage(db, host)
    const first = prepared.envelope.parts[0]
    if (first === undefined) return yield* Effect.die("expected a synthetic prepared part")
    yield* db
      .update(PartTable)
      .set({ message_id: host.info.id })
      .where(eq(PartTable.id, storedPartID(first.id)))
      .run()
      .pipe(Effect.orDie)
    const before = yield* projection(db)
    yield* expectConflict(db, core, prepared, before)
  }),
)

it.effect("rejects exact canonical part data with only its session owner changed", () =>
  Effect.gen(function* () {
    const parentID = sessionID("session-owner-collision-parent")
    const otherParentID = sessionID("session-owner-collision-other-parent")
    const childID = sessionID("session-owner-collision-child")
    const { db, core } = yield* setup({ id: parentID }, { id: otherParentID }, { id: childID, parentID })
    const created = yield* registerResolution(core, "session-owner-collision", parentID, childID)
    const { prepared } = yield* persistPreparedProjection(
      db,
      core,
      created.resolution,
      "session-owner-collision-agent",
      model("session-owner-collision-provider", "session-owner-collision-model"),
    )
    const first = prepared.envelope.parts[0]
    if (first === undefined) return yield* Effect.die("expected a synthetic prepared part")
    yield* db
      .update(PartTable)
      .set({ session_id: otherParentID })
      .where(eq(PartTable.id, storedPartID(first.id)))
      .run()
      .pipe(Effect.orDie)
    const before = yield* projection(db)
    yield* expectConflict(db, core, prepared, before)
  }),
)

it.effect("rolls back receiver writes when admission fails after receive", () =>
  Effect.gen(function* () {
    const parentID = sessionID("rollback-parent")
    const childID = sessionID("rollback-child")
    const { db, core } = yield* setup({ id: parentID }, { id: childID, parentID })
    const created = yield* registerResolution(core, "rollback", parentID, childID)
    const prepared = yield* prepare(
      core,
      created.resolution,
      multipartContent(created.resolution, "rollback-agent", model("rollback-provider", "rollback-model")),
    )

    const failed = yield* core
      .admit(prepared.id, (tx, resolution) =>
        Effect.gen(function* () {
          yield* SessionDelegation.receive(tx, resolution)
          return yield* new Delegation.AdapterError({
            code: "receiver_rollback",
            message: "receiver failed after persisting the envelope",
          })
        }),
      )
      .pipe(Effect.exit)
    expectFailureCode(failed, "receiver_rollback")
    expect(
      yield* db
        .select()
        .from(MessageTable)
        .where(eq(MessageTable.id, storedMessageID(prepared.messageID)))
        .all(),
    ).toEqual([])
    expect(
      yield* db
        .select()
        .from(PartTable)
        .where(eq(PartTable.message_id, storedMessageID(prepared.messageID)))
        .all(),
    ).toEqual([])
    expect((yield* core.getResolution(prepared.id))?.status).toBe("pending")

    expect(yield* core.admit(prepared.id, SessionDelegation.receive)).toEqual({ status: "admitted" })
    expect(
      yield* db
        .select()
        .from(PartTable)
        .where(eq(PartTable.message_id, storedMessageID(prepared.messageID)))
        .all(),
    ).toHaveLength(3)
  }),
)

it.effect("does not deliver a return revoked with its recipient session", () =>
  Effect.gen(function* () {
    const parentID = sessionID("revoked-parent")
    const childID = sessionID("revoked-child")
    const { db, core, delegation } = yield* setup({ id: parentID }, { id: childID, parentID })
    const created = yield* registerResolution(core, "revoked", parentID, childID)

    yield* core.revokeDescendants(parentID)
    expect((yield* core.getResolution(created.resolution.id))?.status).toBe("revoked")
    expect(yield* delegation.deliver(parentID)).toEqual([])
    expect(yield* db.select().from(MessageTable).where(eq(MessageTable.session_id, parentID)).all()).toEqual([])
  }),
)

it.effect("leaves an unprepared return pending when its recipient session is deleted", () =>
  Effect.gen(function* () {
    const parentID = sessionID("missing-recipient-parent")
    const childID = sessionID("missing-recipient-child")
    const { db, core, delegation } = yield* setup({ id: parentID }, { id: childID, parentID })
    const created = yield* registerResolution(core, "missing-recipient", parentID, childID)

    expect(created.resolution.envelope).toBeUndefined()
    yield* db.delete(SessionTable).where(eq(SessionTable.id, parentID)).run().pipe(Effect.orDie)
    expect(yield* db.select().from(SessionTable).where(eq(SessionTable.id, parentID)).all()).toEqual([])

    expect((yield* core.get(created.generation.id))?.id).toBe(created.generation.id)
    expect((yield* core.getResolution(created.resolution.id))?.status).toBe("pending")
    expect(yield* delegation.deliver(parentID)).toEqual([])

    const resolution = yield* core.getResolution(created.resolution.id)
    expect(resolution?.status).toBe("pending")
    expect(resolution?.envelope).toBeUndefined()
    expect(yield* db.select().from(MessageTable).all()).toEqual([])
    expect(yield* db.select().from(PartTable).all()).toEqual([])
  }),
)

it.effect("appends eligible canonical returns after compacted history in Core incoming order", () =>
  Effect.gen(function* () {
    const parentID = sessionID("history-parent")
    const admittedChildID = sessionID("history-admitted-child")
    const consumedChildID = sessionID("history-consumed-child")
    const pendingChildID = sessionID("history-pending-child")
    const retainedModel = model("history-retained-provider", "history-retained-model")
    const { db, core, delegation } = yield* setup(
      { id: parentID },
      { id: admittedChildID, parentID },
      { id: consumedChildID, parentID },
      { id: pendingChildID, parentID },
    )
    const admitted = yield* registerResolution(core, "history-admitted", parentID, admittedChildID)
    const consumed = yield* registerResolution(core, "history-consumed", parentID, consumedChildID)
    const pending = yield* registerResolution(core, "history-pending", parentID, pendingChildID)
    const admittedPrepared = yield* persistPreparedProjection(
      db,
      core,
      admitted.resolution,
      "history-admitted-agent",
      model("history-admitted-provider", "history-admitted-model", "history-admitted-variant"),
    )
    expect(yield* core.admit(admittedPrepared.prepared.id, SessionDelegation.receive)).toEqual({ status: "admitted" })
    const consumedPrepared = yield* persistPreparedProjection(
      db,
      core,
      consumed.resolution,
      "history-consumed-agent",
      model("history-consumed-provider", "history-consumed-model", "history-consumed-variant"),
    )
    expect(yield* core.admit(consumedPrepared.prepared.id, SessionDelegation.receive)).toEqual({ status: "admitted" })
    yield* core.markConsumed({ parentID, ids: [consumed.resolution.id] })

    const admittedBefore = yield* core.getResolution(admitted.resolution.id)
    const consumedBefore = yield* core.getResolution(consumed.resolution.id)
    const pendingBefore = yield* core.getResolution(pending.resolution.id)
    if (admittedBefore === undefined || consumedBefore === undefined || pendingBefore === undefined) {
      return yield* Effect.die("expected history resolutions")
    }
    expect(admittedBefore.status).toBe("admitted")
    expect(consumedBefore.status).toBe("consumed")
    expect(pendingBefore.status).toBe("pending")

    const eligible = (yield* core.incoming(parentID)).filter(
      (resolution) => resolution.status === "admitted" || resolution.status === "consumed",
    )
    expect(eligible).toHaveLength(2)

    const before = userMessage(parentID, "history-before", 10, retainedModel)
    const compaction = compactionMessage(parentID, "history-compaction", 20, before.info.id)
    const summary = assistantMessage(
      parentID,
      "msg_session_delegation_history-summary",
      compaction.info.id,
      30,
      retainedModel,
      { summary: true, finish: "stop" },
    )
    const staleConsumed = userMessage(
      parentID,
      "history-consumed-stale",
      consumed.resolution.timeCreated,
      model("stale-history-provider", "stale-history-model"),
      "stale-history-agent",
      storedMessageID(consumed.resolution.messageID),
    )
    const tail = userMessage(parentID, "history-tail", 40, retainedModel)
    const compacted = [before, compaction, summary, staleConsumed, tail]
    const stored = yield* projection(db)
    const result = yield* delegation.history(parentID, compacted)

    expect(result.incoming.map((resolution) => resolution.id)).toEqual(eligible.map((resolution) => resolution.id))
    expect(result.incoming.map((resolution) => resolution.id)).not.toContain(pending.resolution.id)
    expect(result.messages.map((message) => message.info.id)).toEqual([
      before.info.id,
      compaction.info.id,
      summary.info.id,
      tail.info.id,
      ...eligible.map((resolution) => storedMessageID(resolution.messageID)),
    ])
    expect(result.messages.map((message) => message.info.id)).not.toContain(pending.resolution.messageID)

    for (const resolution of eligible) {
      const messageRow = stored.messages.find((row) => row.id === storedMessageID(resolution.messageID))
      if (messageRow === undefined) return yield* Effect.die(`expected persisted message ${resolution.messageID}`)
      const actual = result.messages.find((message) => message.info.id === storedMessageID(resolution.messageID))
      if (actual === undefined) return yield* Effect.die(`expected history message ${resolution.messageID}`)
      expect(actual.info as unknown).toEqual(
        Schema.decodeUnknownSync(SessionV1.Info)({
          ...messageRow.data,
          id: storedMessageID(messageRow.id),
          sessionID: messageRow.session_id,
        }),
      )
      const partRows = stored.parts.filter((row) => row.message_id === messageRow.id)
      expect(actual.parts as unknown).toEqual(
        partRows.map((row) =>
          Schema.decodeUnknownSync(SessionV1.Part)({
            ...row.data,
            id: SessionV1.PartID.make(row.id),
            sessionID: row.session_id,
            messageID: SessionV1.MessageID.make(row.message_id),
          }),
        ),
      )
    }
    expect(yield* projection(db)).toEqual(stored)

    const admittedAfter = yield* core.getResolution(admitted.resolution.id)
    const consumedAfter = yield* core.getResolution(consumed.resolution.id)
    const pendingAfter = yield* core.getResolution(pending.resolution.id)
    if (admittedAfter === undefined || consumedAfter === undefined || pendingAfter === undefined) {
      return yield* Effect.die("expected history resolutions after merge")
    }
    expect(resolutionState(admittedAfter)).toEqual(resolutionState(admittedBefore))
    expect(resolutionState(consumedAfter)).toEqual(resolutionState(consumedBefore))
    expect(resolutionState(pendingAfter)).toEqual(resolutionState(pendingBefore))
  }),
)
