import { afterEach, describe, expect } from "bun:test"
import { createHash } from "node:crypto"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Cause, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { Agent } from "../../src/agent/agent"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Config } from "@/config/config"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { Session } from "@/session/session"
import { SessionDelegation } from "@/session/delegation"
import type { SessionPrompt } from "../../src/session/prompt"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"

import { TaskTool, type TaskPromptOps } from "../../src/tool/task"
import type { Context } from "../../src/tool/tool"
import { Truncate } from "@/tool/truncate"
import { ToolRegistry } from "@/tool/registry"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { disposeAllInstances } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"

afterEach(async () => {
  await disposeAllInstances()
})

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

const layer = (flags: Partial<RuntimeFlags.Info> = {}) =>
  LayerNode.compile(
    LayerNode.group([
      Agent.node,
      BackgroundJob.node,
      EventV2Bridge.node,
      Config.node,
      CrossSpawnSpawner.node,
      Session.node,
      SessionProjector.node,
      SessionRunState.node,
      SessionStatus.node,
      Truncate.node,
      ToolRegistry.node,
      Database.node,
      DelegationStore.node,
      SessionDelegation.node,
      RuntimeFlags.node,
      Ripgrep.node,
    ]),
    [[RuntimeFlags.node, RuntimeFlags.layer(flags)]],
  )

const it = testEffect(layer())
const background = testEffect(layer({ experimentalBackgroundSubagents: true }))

function defer<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

const seed = Effect.fn("TaskToolTest.seed")(function* (title = "Pinned", callID = "call-task") {
  const session = yield* Session.Service
  const chat = yield* session.create({ title })
  const user = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID: chat.id,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  const assistant: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    parentID: user.id,
    sessionID: chat.id,
    mode: "build",
    agent: "build",
    cost: 0,
    path: { cwd: "/tmp", root: "/tmp" },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    variant: "xhigh",
    time: { created: Date.now() },
  }
  yield* session.updateMessage(assistant)
  const task = yield* session.updatePart(taskOriginPart(chat.id, assistant.id, callID))
  return { chat, assistant, task, callID }
})

function taskOriginPart(sessionID: SessionID, messageID: MessageID, callID: string): SessionV1.ToolPart {
  return {
    id: PartID.ascending(),
    messageID,
    sessionID,
    type: "tool",
    callID,
    tool: TaskTool.id,
    state: {
      status: "running",
      input: {
        description: "inspect bug",
        prompt: "look into the cache key path",
        subagent_type: "general",
      },
      time: { start: Date.now() },
    },
  }
}

function taskTurn(sessions: Session.Interface, template: SessionV1.Assistant, sessionID: SessionID, callID: string) {
  return Effect.gen(function* () {
    const assistant = yield* sessions.updateMessage({
      ...template,
      id: MessageID.ascending(),
      parentID: MessageID.ascending(),
      sessionID,
    })
    const task = yield* sessions.updatePart(taskOriginPart(sessionID, assistant.id, callID))
    return { assistant, task, callID }
  })
}

function completedTaskPart(part: SessionV1.ToolPart, output: string): SessionV1.ToolPart {
  if (part.state.status !== "running") throw new Error("task origin must be running")
  return {
    ...part,
    state: {
      status: "completed",
      input: part.state.input,
      output,
      title: "Task result",
      metadata: {},
      time: { start: part.state.time.start, end: Date.now() },
    },
  }
}

function taskContext(
  fixture: { chat: { id: SessionID }; assistant: { id: MessageID }; callID: string },
  promptOps: TaskPromptOps,
  extra: Record<string, unknown> = {},
): Context {
  return {
    sessionID: fixture.chat.id,
    messageID: fixture.assistant.id,
    callID: fixture.callID,
    agent: "build",
    abort: new AbortController().signal,
    extra: { promptOps, ...extra },
    messages: [],
    metadata: () => Effect.void,
    ask: () => Effect.void,
  }
}

function originDigest(input: { parentID: SessionID; messageID: MessageID; partID: PartID; callID: string }) {
  return createHash("sha256")
    .update(JSON.stringify([input.parentID, input.messageID, input.partID, input.callID]))
    .digest("hex")
}

function registerOrigin(
  delegation: DelegationStore.Interface,
  input: {
    parentID: SessionID
    childID: SessionID
    part: SessionV1.ToolPart
    mode?: Delegation.Mode
    parentGenerationID?: Delegation.ID
  },
) {
  const digest = originDigest({
    parentID: input.parentID,
    messageID: input.part.messageID,
    partID: input.part.id,
    callID: input.part.callID,
  })
  return delegation.register({
    requestID: Delegation.RequestID.make(`drq_${digest}`),
    generationID: Delegation.ID.make(`dlg_${digest}`),
    parentID: input.parentID,
    childID: input.childID,
    origin: {
      messageID: SessionMessage.ID.make(input.part.messageID),
      partID: Delegation.OriginPartID.make(input.part.id),
      callID: input.part.callID,
    },
    ...(input.mode === undefined ? {} : { mode: input.mode }),
    ...(input.parentGenerationID === undefined ? {} : { parentGenerationID: input.parentGenerationID }),
    explicitReuse: false,
  })
}

function stubOps(opts?: {
  onPrompt?: (
    input: SessionPrompt.PromptInput,
    admission?: { generation: Delegation.Generation; workID: Delegation.WorkID },
  ) => void
  onWake?: (sessionID: SessionID) => void
  resolvePromptParts?: TaskPromptOps["resolvePromptParts"]
  text?: string
  error?: NonNullable<SessionV1.Assistant["error"]>
  toolError?: string
}): TaskPromptOps {
  return {
    cancel: () => Effect.void,
    resolvePromptParts:
      opts?.resolvePromptParts ?? ((template) => Effect.succeed([{ type: "text" as const, text: template }])),
    ...(opts?.onWake ? { wake: (sessionID: SessionID) => Effect.sync(() => opts.onWake?.(sessionID)) } : {}),
    prompt: (input, admission) =>
      Effect.sync(() => {
        opts?.onPrompt?.(input, admission)
        return reply(input, opts?.text ?? "done", opts?.error, opts?.toolError)
      }),
  }
}

function reply(
  input: SessionPrompt.PromptInput,
  text: string,
  error?: NonNullable<SessionV1.Assistant["error"]>,
  toolError?: string,
): SessionV1.WithParts {
  const id = MessageID.ascending()
  return {
    info: {
      id,
      role: "assistant",
      parentID: input.messageID ?? MessageID.ascending(),
      sessionID: input.sessionID,
      mode: input.agent ?? "general",
      agent: input.agent ?? "general",
      cost: 0,
      path: { cwd: "/tmp", root: "/tmp" },
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      modelID: input.model?.modelID ?? ref.modelID,
      providerID: input.model?.providerID ?? ref.providerID,
      time: { created: Date.now() },
      finish: "stop",
      error,
    },
    parts: [
      {
        id: PartID.ascending(),
        messageID: id,
        sessionID: input.sessionID,
        type: "text",
        text,
      },
      ...(toolError
        ? [
            {
              id: PartID.ascending(),
              messageID: id,
              sessionID: input.sessionID,
              type: "tool" as const,
              tool: "read",
              callID: "call-1",
              state: {
                status: "error" as const,
                input: { filePath: "/external" },
                error: toolError,
                time: { start: Date.now(), end: Date.now() },
              },
            },
          ]
        : []),
    ],
  }
}

describe("tool.task", () => {
  it.instance("registers a persisted task origin before the job and passes stable admission IDs", () =>
    Effect.gen(function* () {
      const delegation = yield* DelegationStore.Service
      const fixture = yield* seed("Pinned", "call-stable")
      const tool = yield* TaskTool
      const def = yield* tool.init()
      let seen: SessionPrompt.PromptInput | undefined
      let admission: { generation: Delegation.Generation; workID: Delegation.WorkID } | undefined

      const result = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
        },
        {
          sessionID: fixture.chat.id,
          messageID: fixture.assistant.id,
          callID: fixture.callID,
          agent: "build",
          abort: new AbortController().signal,
          extra: {
            promptOps: stubOps({
              onPrompt: (input, next) => {
                seen = input
                admission = next
              },
            }),
          },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      const digest = originDigest({
        parentID: fixture.chat.id,
        messageID: fixture.assistant.id,
        partID: fixture.task.id,
        callID: fixture.callID,
      })
      const generation = yield* delegation.get(Delegation.ID.make(`dlg_${digest}`))
      expect(generation).toMatchObject({
        id: `dlg_${digest}`,
        parentID: fixture.chat.id,
        origin: {
          messageID: fixture.assistant.id,
          partID: fixture.task.id,
          callID: fixture.callID,
        },
        mode: "foreground",
        state: "active",
      })
      expect(result.metadata.delegationID).toBe(Delegation.ID.make(`dlg_${digest}`))
      expect(seen?.messageID).toBe(MessageID.make(`msg_${digest}`))
      if (generation === undefined) throw new Error("task generation was not persisted")
      if (admission === undefined) throw new Error("task prompt did not receive delegation admission")
      expect(admission.generation.id).toBe(generation.id)
      const work = yield* delegation.listWork(generation.id)
      expect(work).toHaveLength(1)
      expect(work[0]?.id).toBe(admission.workID)
      expect(work[0]?.state).toBe("finished")
    }),
  )

  background.instance(
    "keeps the root, foreground-parent, and background-parent mode matrix",
    () =>
      Effect.gen(function* () {
        const sessions = yield* Session.Service
        const delegation = yield* DelegationStore.Service
        const jobs = yield* BackgroundJob.Service
        const root = yield* seed("Matrix root", "call-root")
        const foregroundParent = yield* sessions.create({ parentID: root.chat.id, title: "Foreground parent" })
        const backgroundParent = yield* sessions.create({ parentID: root.chat.id, title: "Background parent" })
        const foregroundOrigin = yield* sessions.updatePart(
          taskOriginPart(root.chat.id, root.assistant.id, "call-foreground-parent"),
        )
        const backgroundOrigin = yield* sessions.updatePart(
          taskOriginPart(root.chat.id, root.assistant.id, "call-background-parent"),
        )
        const foreground = yield* registerOrigin(delegation, {
          parentID: root.chat.id,
          childID: foregroundParent.id,
          part: foregroundOrigin,
        })
        const background = yield* registerOrigin(delegation, {
          parentID: root.chat.id,
          childID: backgroundParent.id,
          part: backgroundOrigin,
          mode: "background",
        })
        yield* delegation.finishWork(foreground.workID)
        yield* delegation.finishWork(background.workID)

        expect(foreground.generation.mode).toBe("foreground")
        expect(background.generation.mode).toBe("background")

        const foregroundChild = yield* sessions.create({ parentID: foregroundParent.id, title: "Foreground child" })
        const backgroundForegroundChild = yield* sessions.create({
          parentID: backgroundParent.id,
          title: "Background foreground child",
        })
        const backgroundChild = yield* sessions.create({ parentID: backgroundParent.id, title: "Background child" })
        const forbiddenChild = yield* sessions.create({ parentID: foregroundParent.id, title: "Forbidden child" })
        const foregroundTurn = yield* taskTurn(sessions, root.assistant, foregroundParent.id, "call-foreground-child")
        const backgroundForegroundTurn = yield* taskTurn(
          sessions,
          root.assistant,
          backgroundParent.id,
          "call-background-foreground-child",
        )
        const backgroundTurn = yield* taskTurn(sessions, root.assistant, backgroundParent.id, "call-background-child")
        const forbiddenTurn = yield* taskTurn(sessions, root.assistant, foregroundParent.id, "call-forbidden-child")
        const tool = yield* TaskTool
        const def = yield* tool.init()
        const promptOps = stubOps({ text: "matrix result" })

        const execute = (
          parentID: SessionID,
          turn: typeof foregroundTurn,
          params: { description: string; prompt: string; subagent_type: string; background?: boolean },
        ) =>
          def.execute(params, {
            sessionID: parentID,
            messageID: turn.assistant.id,
            callID: turn.callID,
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          })

        const foregroundResult = yield* execute(foregroundParent.id, foregroundTurn, {
          description: "foreground child",
          prompt: "run in foreground",
          subagent_type: "general",
        })
        const backgroundForegroundResult = yield* execute(backgroundParent.id, backgroundForegroundTurn, {
          description: "background parent child",
          prompt: "default to foreground",
          subagent_type: "general",
        })
        const backgroundResult = yield* execute(backgroundParent.id, backgroundTurn, {
          description: "background child",
          prompt: "run in background",
          subagent_type: "general",
          background: true,
        })
        yield* jobs.wait({ id: backgroundResult.metadata.sessionId })
        const forbidden = yield* execute(foregroundParent.id, forbiddenTurn, {
          description: "forbidden child",
          prompt: "must be rejected",
          subagent_type: "general",
          background: true,
        }).pipe(Effect.exit)

        expect((yield* delegation.get(foregroundResult.metadata.delegationID))?.mode).toBe("foreground")
        expect((yield* delegation.get(foregroundResult.metadata.delegationID))?.parentGenerationID).toBe(
          foreground.generation.id,
        )
        expect((yield* delegation.get(backgroundForegroundResult.metadata.delegationID))?.mode).toBe("foreground")
        expect((yield* delegation.get(backgroundForegroundResult.metadata.delegationID))?.parentGenerationID).toBe(
          background.generation.id,
        )
        expect((yield* delegation.get(backgroundResult.metadata.delegationID))?.mode).toBe("background")
        expect(backgroundResult.metadata.background).toBe(true)
        expect(Exit.isFailure(forbidden)).toBe(true)
        expect(yield* delegation.active(forbiddenChild.id)).toBeUndefined()
      }),
    { config: { subagent_depth: 2 } },
  )

  background.instance("retains active mode when omitted and rejects explicit mode conflicts", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const sessions = yield* Session.Service
      const delegation = yield* DelegationStore.Service
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const params = {
        description: "inspect bug",
        prompt: "look into the cache key path",
        subagent_type: "general",
      }

      const backgroundFixture = yield* seed("Background mode", "call-mode-background")
      const backgroundResult = yield* def.execute(
        { ...params, background: true },
        taskContext(backgroundFixture, stubOps({ text: "background" })),
      )
      yield* jobs.wait({ id: backgroundResult.metadata.sessionId })
      const backgroundRetry = yield* taskTurn(
        sessions,
        backgroundFixture.assistant,
        backgroundFixture.chat.id,
        "call-mode-background-retry",
      )
      const retainedBackground = yield* def.execute(
        { ...params, task_id: backgroundResult.metadata.sessionId },
        taskContext(
          { chat: backgroundFixture.chat, assistant: backgroundRetry.assistant, callID: backgroundRetry.callID },
          stubOps({ text: "background retry" }),
        ),
      )
      yield* jobs.wait({ id: retainedBackground.metadata.sessionId })
      expect(retainedBackground.metadata.delegationID).toBe(backgroundResult.metadata.delegationID)
      expect(retainedBackground.metadata.background).toBe(true)
      expect((yield* delegation.active(backgroundResult.metadata.sessionId))?.mode).toBe("background")

      const backgroundConflictTurn = yield* taskTurn(
        sessions,
        backgroundFixture.assistant,
        backgroundFixture.chat.id,
        "call-mode-background-conflict",
      )
      const backgroundConflict = yield* def
        .execute(
          { ...params, background: false, task_id: backgroundResult.metadata.sessionId },
          taskContext(
            {
              chat: backgroundFixture.chat,
              assistant: backgroundConflictTurn.assistant,
              callID: backgroundConflictTurn.callID,
            },
            stubOps(),
          ),
        )
        .pipe(Effect.exit)
      expect(Exit.isFailure(backgroundConflict)).toBe(true)
      if (Exit.isSuccess(backgroundConflict)) throw new Error("expected background mode conflict")
      const backgroundConflictError = Cause.squash(backgroundConflict.cause)
      expect(backgroundConflictError).toBeInstanceOf(Error)
      if (!(backgroundConflictError instanceof Error)) throw new Error("expected background mode conflict error")
      expect(backgroundConflictError.message).toContain("cannot change mode")

      const foregroundFixture = yield* seed("Foreground mode", "call-mode-foreground")
      const foregroundResult = yield* def.execute(
        { ...params, background: false },
        taskContext(foregroundFixture, stubOps({ text: "foreground" })),
      )
      const foregroundRetry = yield* taskTurn(
        sessions,
        foregroundFixture.assistant,
        foregroundFixture.chat.id,
        "call-mode-foreground-retry",
      )
      const retainedForeground = yield* def.execute(
        { ...params, task_id: foregroundResult.metadata.sessionId },
        taskContext(
          { chat: foregroundFixture.chat, assistant: foregroundRetry.assistant, callID: foregroundRetry.callID },
          stubOps({ text: "foreground retry" }),
        ),
      )
      expect(retainedForeground.metadata.background).toBeUndefined()
      expect((yield* delegation.active(foregroundResult.metadata.sessionId))?.mode).toBe("foreground")

      const foregroundConflictTurn = yield* taskTurn(
        sessions,
        foregroundFixture.assistant,
        foregroundFixture.chat.id,
        "call-mode-foreground-conflict",
      )
      const foregroundConflict = yield* def
        .execute(
          { ...params, background: true, task_id: foregroundResult.metadata.sessionId },
          taskContext(
            {
              chat: foregroundFixture.chat,
              assistant: foregroundConflictTurn.assistant,
              callID: foregroundConflictTurn.callID,
            },
            stubOps(),
          ),
        )
        .pipe(Effect.exit)
      expect(Exit.isFailure(foregroundConflict)).toBe(true)
      if (Exit.isSuccess(foregroundConflict)) throw new Error("expected foreground mode conflict")
      const foregroundConflictError = Cause.squash(foregroundConflict.cause)
      expect(foregroundConflictError).toBeInstanceOf(Error)
      if (!(foregroundConflictError instanceof Error)) throw new Error("expected foreground mode conflict error")
      expect(foregroundConflictError.message).toContain("cannot change mode")
    }),
  )

  it.instance("releases a new launch token when metadata fails after registration", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const delegation = yield* DelegationStore.Service
      const jobs = yield* BackgroundJob.Service
      const fixture = yield* seed("Metadata launch failure", "call-metadata-launch-failure")
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const metadataFailure = new Error("metadata sentinel")
      const wakeObserved = yield* Deferred.make<{
        sessionID: SessionID
        generation: Delegation.Generation
        work: readonly Delegation.Work[]
      }>()
      let captured: { title?: string; metadata?: Record<string, unknown> } | undefined
      let prompts = 0
      const promptOps: TaskPromptOps = {
        ...stubOps({ onPrompt: () => prompts++ }),
        wake: (sessionID) =>
          Effect.gen(function* () {
            const generation = yield* delegation.active(sessionID)
            if (generation === undefined) throw new Error("launch generation was not active at wake time")
            const work = yield* delegation.listWork(generation.id)
            yield* Deferred.succeed(wakeObserved, { sessionID, generation, work })
          }).pipe(Effect.orDie),
      }
      const exit = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
          },
          {
            ...taskContext(fixture, promptOps),
            metadata: (input) => {
              captured = input
              return Effect.die(metadataFailure)
            },
          },
        )
        .pipe(Effect.exit)

      const metadata = captured?.metadata
      const childIDValue = metadata?.sessionId
      const delegationIDValue = metadata?.delegationID
      if (typeof childIDValue !== "string") throw new Error("metadata did not publish child session ID")
      if (typeof delegationIDValue !== "string") throw new Error("metadata did not publish delegation ID")
      const childID = SessionID.make(childIDValue)
      const observed = yield* Deferred.await(wakeObserved)
      const generation = yield* delegation.get(Delegation.ID.make(delegationIDValue))
      const work = yield* delegation.listWork(observed.generation.id)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isSuccess(exit)) throw new Error("expected metadata failure")
      expect(Cause.squash(exit.cause)).toBe(metadataFailure)
      expect(prompts).toBe(0)
      expect(observed.sessionID).toBe(childID)
      expect(generation?.id).toBe(observed.generation.id)
      expect(observed.work).toHaveLength(1)
      expect(observed.work[0]?.kind).toBe("launch")
      expect(observed.work[0]?.state).toBe("finished")
      expect(work[0]?.state).toBe("finished")
      expect(yield* jobs.get(childID)).toBeUndefined()
      expect(yield* jobs.list()).toHaveLength(0)
      expect(yield* sessions.get(childID)).toBeDefined()
    }),
  )

  it.instance("rejects before admission when promptOps is missing", () =>
    Effect.gen(function* () {
      const delegation = yield* DelegationStore.Service
      const jobs = yield* BackgroundJob.Service
      const fixture = yield* seed("Missing prompt operations", "call-missing-prompt-ops")
      const tool = yield* TaskTool
      const def = yield* tool.init()
      let prompts = 0
      const promptOps = stubOps({ onPrompt: () => prompts++ })
      const before = yield* delegation.listActive()
      const exit = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
          },
          {
            ...taskContext(fixture, promptOps),
            extra: { promptOps: undefined },
          },
        )
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      expect(prompts).toBe(0)
      expect(yield* delegation.listActive()).toEqual(before)
      expect(yield* jobs.list()).toHaveLength(0)
    }),
  )

  background.instance("finishes only a new update token when active background metadata fails", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const delegation = yield* DelegationStore.Service
      const jobs = yield* BackgroundJob.Service
      const fixture = yield* seed("Background metadata update failure", "call-background-metadata-update")
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const firstStarted = yield* Deferred.make<void>()
      const firstRelease = defer<void>()
      const cancelCalls: SessionID[] = []
      const wakeCalls: SessionID[] = []
      let prompts = 0
      const promptOps: TaskPromptOps = {
        ...stubOps(),
        cancel: (sessionID) => Effect.sync(() => cancelCalls.push(sessionID)),
        wake: (sessionID) => Effect.sync(() => wakeCalls.push(sessionID)),
        prompt: (input) => {
          prompts++
          if (prompts > 1) return Effect.die(new Error("unexpected second prompt"))
          return Effect.gen(function* () {
            yield* Deferred.succeed(firstStarted, undefined)
            yield* Effect.promise(() => firstRelease.promise)
            return reply(input, "original background result")
          })
        },
      }
      const first = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          background: true,
        },
        taskContext(fixture, promptOps),
      )
      yield* Deferred.await(firstStarted)
      const childID = SessionID.make(first.metadata.sessionId)
      const generation = yield* delegation.active(childID)
      if (generation === undefined) throw new Error("background task did not register a generation")
      const original = (yield* delegation.listWork(generation.id)).find((work) => work.state === "active")
      if (original === undefined) throw new Error("background launch token was not active")
      const retryTurn = yield* taskTurn(sessions, fixture.assistant, fixture.chat.id, "call-background-metadata-retry")
      const metadataFailure = new Error("update metadata sentinel")
      const secondExit = yield* def
        .execute(
          {
            description: "extend investigation",
            prompt: "also inspect cancellation",
            subagent_type: "general",
            background: true,
            task_id: first.metadata.sessionId,
          },
          {
            ...taskContext({ chat: fixture.chat, assistant: retryTurn.assistant, callID: retryTurn.callID }, promptOps),
            metadata: () => Effect.die(metadataFailure),
          },
        )
        .pipe(Effect.exit)
      const afterUpdate = yield* delegation.listWork(generation.id)
      const update = afterUpdate.find((work) => work.id !== original.id)

      expect(Exit.isFailure(secondExit)).toBe(true)
      if (Exit.isSuccess(secondExit)) throw new Error("expected update metadata failure")
      expect(Cause.squash(secondExit.cause)).toBe(metadataFailure)
      expect(prompts).toBe(1)
      expect(cancelCalls).toEqual([])
      expect(wakeCalls).toEqual([childID])
      expect(original.state).toBe("active")
      expect(update?.kind).toBe("update")
      expect(update?.state).toBe("finished")
      expect((yield* jobs.get(childID))?.status).toBe("running")

      firstRelease.resolve()
      expect((yield* jobs.wait({ id: childID })).info?.status).toBe("completed")
      expect((yield* delegation.listWork(generation.id)).find((work) => work.id === original.id)?.state).toBe(
        "finished",
      )
    }),
  )

  background.instance("does not release or notify an owner on an exact active retry metadata failure", () =>
    Effect.gen(function* () {
      const delegation = yield* DelegationStore.Service
      const jobs = yield* BackgroundJob.Service
      const fixture = yield* seed("Exact active retry failure", "call-exact-active-retry")
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const firstStarted = yield* Deferred.make<void>()
      const firstRelease = defer<void>()
      const cancelCalls: SessionID[] = []
      const wakeCalls: SessionID[] = []
      let prompts = 0
      const promptOps: TaskPromptOps = {
        ...stubOps(),
        cancel: (sessionID) => Effect.sync(() => cancelCalls.push(sessionID)),
        wake: (sessionID) => Effect.sync(() => wakeCalls.push(sessionID)),
        prompt: (input) => {
          prompts++
          return Effect.gen(function* () {
            yield* Deferred.succeed(firstStarted, undefined)
            yield* Effect.promise(() => firstRelease.promise)
            return reply(input, "original result")
          })
        },
      }
      const params = {
        description: "inspect bug",
        prompt: "look into the cache key path",
        subagent_type: "general",
        background: true,
      }
      const first = yield* def.execute(params, taskContext(fixture, promptOps))
      yield* Deferred.await(firstStarted)
      const childID = SessionID.make(first.metadata.sessionId)
      const generation = yield* delegation.active(childID)
      if (generation === undefined) throw new Error("exact retry did not register a generation")
      const before = yield* delegation.listWork(generation.id)
      if (before.length !== 1) throw new Error("exact retry fixture did not have one launch token")
      const original = before[0]
      if (original === undefined) throw new Error("exact retry launch token was not found")
      const metadataFailure = new Error("exact retry metadata sentinel")
      const retryExit = yield* def
        .execute(params, {
          ...taskContext(fixture, promptOps),
          metadata: () => Effect.die(metadataFailure),
        })
        .pipe(Effect.exit)
      const after = yield* delegation.listWork(generation.id)

      expect(Exit.isFailure(retryExit)).toBe(true)
      if (Exit.isSuccess(retryExit)) throw new Error("expected exact retry metadata failure")
      expect(Cause.squash(retryExit.cause)).toBe(metadataFailure)
      expect(prompts).toBe(1)
      expect(cancelCalls).toEqual([])
      expect(wakeCalls).toEqual([])
      expect(after).toHaveLength(1)
      expect(after[0]?.id).toBe(original.id)
      expect(after[0]?.state).toBe("active")
      expect((yield* jobs.get(childID))?.status).toBe("running")

      firstRelease.resolve()
      expect((yield* jobs.wait({ id: childID })).info?.status).toBe("completed")
      expect((yield* delegation.listWork(generation.id))[0]?.state).toBe("finished")
    }),
  )

  it.instance("interrupts metadata after registration and cancels before waking with a finished token", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const delegation = yield* DelegationStore.Service
      const jobs = yield* BackgroundJob.Service
      const fixture = yield* seed("Interrupted metadata", "call-interrupted-metadata")
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const metadataReady = yield* Deferred.make<void>()
      const cancelled = yield* Deferred.make<{
        sessionID: SessionID
        generation: Delegation.Generation
        work: readonly Delegation.Work[]
      }>()
      const woken = yield* Deferred.make<{
        sessionID: SessionID
        generation: Delegation.Generation
        work: readonly Delegation.Work[]
      }>()
      let captured: { metadata?: Record<string, unknown> } | undefined
      let prompts = 0
      const promptOps: TaskPromptOps = {
        ...stubOps({ onPrompt: () => prompts++ }),
        cancel: (sessionID) =>
          Effect.gen(function* () {
            const generation = yield* delegation.active(sessionID)
            if (generation === undefined) throw new Error("interrupted launch generation was not active at cancel time")
            const work = yield* delegation.listWork(generation.id)
            yield* Deferred.succeed(cancelled, { sessionID, generation, work })
          }).pipe(Effect.orDie),
        wake: (sessionID) =>
          Effect.gen(function* () {
            const generation = yield* delegation.active(sessionID)
            if (generation === undefined) throw new Error("interrupted launch generation was not active at wake time")
            const work = yield* delegation.listWork(generation.id)
            yield* Deferred.succeed(woken, { sessionID, generation, work })
          }).pipe(Effect.orDie),
      }
      const fiber = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
          },
          {
            ...taskContext(fixture, promptOps),
            metadata: (input) =>
              Effect.gen(function* () {
                captured = input
                yield* Deferred.succeed(metadataReady, undefined)
                yield* Effect.never
              }),
          },
        )
        .pipe(Effect.forkChild)

      yield* Deferred.await(metadataReady)
      yield* Fiber.interrupt(fiber)
      const exit = yield* Fiber.await(fiber)
      const metadata = captured?.metadata
      const childIDValue = metadata?.sessionId
      const delegationIDValue = metadata?.delegationID
      if (typeof childIDValue !== "string") throw new Error("metadata did not publish interrupted child session ID")
      if (typeof delegationIDValue !== "string") throw new Error("metadata did not publish interrupted delegation ID")
      const childID = SessionID.make(childIDValue)
      const cancelObservation = yield* Deferred.await(cancelled)
      const wakeObservation = yield* Deferred.await(woken)

      expect(Exit.isFailure(exit)).toBe(true)
      expect(prompts).toBe(0)
      expect(cancelObservation.sessionID).toBe(childID)
      expect(wakeObservation.sessionID).toBe(childID)
      expect(cancelObservation.generation.id).toBe(Delegation.ID.make(delegationIDValue))
      expect(cancelObservation.work).toHaveLength(1)
      expect(cancelObservation.work[0]?.kind).toBe("launch")
      expect(cancelObservation.work[0]?.state).toBe("active")
      expect(wakeObservation.work).toHaveLength(1)
      expect(wakeObservation.work[0]?.id).toBe(cancelObservation.work[0]?.id)
      expect(wakeObservation.work[0]?.state).toBe("finished")
      expect(yield* jobs.list()).toHaveLength(0)
    }),
  )

  it.instance(
    "description sorts subagents by name and is stable across calls",
    () =>
      Effect.gen(function* () {
        const agent = yield* Agent.Service
        const build = yield* agent.get("build")
        const registry = yield* ToolRegistry.Service
        const get = Effect.fnUntraced(function* () {
          const tools = yield* registry.tools({ ...ref, agent: build })
          return tools.find((tool) => tool.id === TaskTool.id)?.description ?? ""
        })
        const first = yield* get()
        const second = yield* get()

        expect(first).toBe(second)

        const alpha = first.indexOf("- alpha: Alpha agent")
        const explore = first.indexOf("- explore:")
        const general = first.indexOf("- general:")
        const zebra = first.indexOf("- zebra: Zebra agent")

        expect(alpha).toBeGreaterThan(-1)
        expect(explore).toBeGreaterThan(alpha)
        expect(general).toBeGreaterThan(explore)
        expect(zebra).toBeGreaterThan(general)
      }),
    {
      config: {
        agent: {
          zebra: {
            description: "Zebra agent",
            mode: "subagent",
          },
          alpha: {
            description: "Alpha agent",
            mode: "subagent",
          },
        },
      },
    },
  )

  it.instance(
    "description hides denied subagents for the caller",
    () =>
      Effect.gen(function* () {
        const agent = yield* Agent.Service
        const build = yield* agent.get("build")
        const registry = yield* ToolRegistry.Service
        const description =
          (yield* registry.tools({ ...ref, agent: build })).find((tool) => tool.id === TaskTool.id)?.description ?? ""

        expect(description).toContain("- alpha: Alpha agent")
        expect(description).not.toContain("- zebra: Zebra agent")
      }),
    {
      config: {
        permission: {
          task: {
            "*": "allow",
            zebra: "deny",
          },
        },
        agent: {
          zebra: {
            description: "Zebra agent",
            mode: "subagent",
          },
          alpha: {
            description: "Alpha agent",
            mode: "subagent",
          },
        },
      },
    },
  )

  it.instance("execute adopts an idle same-parent task session from task_id", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const delegation = yield* DelegationStore.Service
      const { chat, assistant, task } = yield* seed()
      const child = yield* sessions.create({ parentID: chat.id, title: "Existing child" })
      const tool = yield* TaskTool
      const def = yield* tool.init()
      let seen: SessionPrompt.PromptInput | undefined
      const promptOps = stubOps({ text: "resumed", onPrompt: (input) => (seen = input) })

      const result = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          task_id: child.id,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          callID: "call-task",
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      const kids = yield* sessions.children(chat.id)
      expect(kids).toHaveLength(1)
      expect(kids[0]?.id).toBe(child.id)
      expect(result.metadata.sessionId).toBe(child.id)
      expect(result.output).toContain(`<task id="${child.id}" state="completed">`)
      expect(seen?.sessionID).toBe(child.id)
      expect(seen?.variant).toBe("xhigh")
      const generation = yield* delegation.active(child.id)
      expect(generation).toMatchObject({
        parentID: chat.id,
        childID: child.id,
        origin: { messageID: assistant.id, partID: task.id, callID: "call-task" },
        mode: "foreground",
      })
    }),
  )

  it.instance("reconciles an exact finished retry from the persisted task result without prompting again", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const fixture = yield* seed("Persisted result", "call-persisted-result")
      const tool = yield* TaskTool
      const def = yield* tool.init()
      let prompts = 0
      const params = {
        description: "inspect bug",
        prompt: "look into the cache key path",
        subagent_type: "general",
      }
      const first = yield* def.execute(
        params,
        taskContext(fixture, stubOps({ text: "transient output", onPrompt: () => prompts++ })),
      )
      yield* sessions.updatePart(completedTaskPart(fixture.task, "persisted output"))

      const retry = yield* def.execute(
        params,
        taskContext(fixture, stubOps({ text: "must not run", onPrompt: () => prompts++ })),
      )

      expect(prompts).toBe(1)
      expect(retry.output).toBe("persisted output")
      expect(retry.metadata.sessionId).toBe(first.metadata.sessionId)
      expect(retry.metadata.delegationID).toBe(first.metadata.delegationID)
    }),
  )

  it.instance("rejects an exact finished retry when the origin has no persisted result", () =>
    Effect.gen(function* () {
      const delegation = yield* DelegationStore.Service
      const fixture = yield* seed("Missing persisted result", "call-missing-result")
      const tool = yield* TaskTool
      const def = yield* tool.init()
      let prompts = 0
      const params = {
        description: "inspect bug",
        prompt: "look into the cache key path",
        subagent_type: "general",
      }

      const first = yield* def.execute(
        params,
        taskContext(fixture, stubOps({ text: "first output", onPrompt: () => prompts++ })),
      )
      const exit = yield* def
        .execute(params, taskContext(fixture, stubOps({ onPrompt: () => prompts++ })))
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      expect(prompts).toBe(1)
      if (Exit.isSuccess(exit)) throw new Error("expected retry failure")
      const failure = Cause.squash(exit.cause)
      expect(failure).toBeInstanceOf(Error)
      if (!(failure instanceof Error)) throw new Error("expected retry error")
      expect(failure.message).toContain("already finished without a persisted result")
      expect((yield* delegation.listWork(first.metadata.delegationID)).map((work) => work.state)).toEqual(["finished"])
    }),
  )

  it.instance("reuses a closed child only from a fresh retry owner", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const delegation = yield* DelegationStore.Service
      const fixture = yield* seed("Closed generation", "call-closed-generation")
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const params = {
        description: "inspect bug",
        prompt: "look into the cache key path",
        subagent_type: "general",
      }
      const first = yield* def.execute(params, taskContext(fixture, stubOps({ text: "first output" })))
      const previous = yield* delegation.get(first.metadata.delegationID)
      if (previous === undefined) throw new Error("task generation was not persisted")
      const closed = yield* delegation.reconcileAndClose(previous.id, () =>
        Effect.succeed({ quiescent: true, sources: [] }),
      )
      expect(closed.closed).toBe(true)
      expect((yield* delegation.get(previous.id))?.state).toBe("closed")

      const retryTurn = yield* taskTurn(sessions, fixture.assistant, fixture.chat.id, "call-closed-retry")
      const retry = yield* def.execute(
        { ...params, task_id: first.metadata.sessionId },
        taskContext(
          { chat: fixture.chat, assistant: retryTurn.assistant, callID: retryTurn.callID },
          stubOps({ text: "retry output" }),
        ),
      )
      const next = yield* delegation.get(retry.metadata.delegationID)

      expect(retry.metadata.sessionId).toBe(first.metadata.sessionId)
      expect(retry.metadata.delegationID).not.toBe(previous.id)
      expect(next).toMatchObject({ parentID: fixture.chat.id, childID: first.metadata.sessionId, mode: "foreground" })
      expect(next?.parentGenerationID).toBeUndefined()
    }),
  )

  it.instance("execute surfaces child errors with a resumable task_id", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()

      const exit = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            callID: "call-task",
            agent: "build",
            abort: new AbortController().signal,
            extra: {
              promptOps: stubOps({
                text: "",
                error: new SessionV1.APIError({ message: "Network connection lost", isRetryable: false }).toObject(),
              }),
            },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isSuccess(exit)) throw new Error("expected task failure")
      const child = (yield* sessions.children(chat.id))[0]
      expect(child).toBeDefined()
      const failure = Cause.squash(exit.cause)
      expect(failure).toBeInstanceOf(Error)
      if (!(failure instanceof Error)) throw new Error("expected Error defect")
      expect(failure.message).toBe(`Subagent failed (task_id: ${child?.id}): Network connection lost`)
    }),
  )

  it.instance("execute surfaces terminal child tool errors with a resumable task_id", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()

      const exit = yield* def
        .execute(
          {
            description: "inspect external directory",
            prompt: "read the external directory",
            subagent_type: "general",
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            callID: "call-task",
            agent: "build",
            abort: new AbortController().signal,
            extra: {
              promptOps: stubOps({
                text: "I will inspect the directory.",
                toolError: "The user rejected permission to use this specific tool call.",
              }),
            },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isSuccess(exit)) throw new Error("expected task failure")
      const child = (yield* sessions.children(chat.id))[0]
      const failure = Cause.squash(exit.cause)
      expect(failure).toBeInstanceOf(Error)
      if (!(failure instanceof Error)) throw new Error("expected Error defect")
      expect(failure.message).toBe(
        `Subagent failed (task_id: ${child?.id}): The user rejected permission to use this specific tool call.`,
      )
    }),
  )

  it.instance("execute asks by default and skips checks when bypassed", () =>
    Effect.gen(function* () {
      const first = yield* seed("Pinned", "call-permission")
      const second = yield* seed("Pinned", "call-permission-bypass")
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const calls: unknown[] = []
      const promptOps = stubOps()

      const exec = (fixture: typeof first, extra?: Record<string, unknown>) =>
        def.execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
          },
          {
            sessionID: fixture.chat.id,
            messageID: fixture.assistant.id,
            callID: fixture.callID,
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps, ...extra },
            messages: [],
            metadata: () => Effect.void,
            ask: (input) =>
              Effect.sync(() => {
                calls.push(input)
              }),
          },
        )

      yield* exec(first)
      yield* exec(second, { bypassAgentCheck: true })

      expect(calls).toHaveLength(1)
      expect(calls[0]).toEqual({
        permission: "task",
        patterns: ["general"],
        always: ["*"],
        metadata: {
          description: "inspect bug",
          subagent_type: "general",
        },
      })
    }),
  )

  it.instance("execute cancels child session when abort signal fires", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const ready = defer<SessionPrompt.PromptInput>()
      const cancelled = defer<SessionID>()
      const abort = new AbortController()
      const promptOps: TaskPromptOps = {
        cancel: (sessionID) =>
          Effect.sync(() => {
            cancelled.resolve(sessionID)
          }),
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.promise(() => {
            ready.resolve(input)
            return cancelled.promise
          }).pipe(Effect.as(reply(input, "cancelled"))),
      }

      const fiber = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            callID: "call-task",
            agent: "build",
            abort: abort.signal,
            extra: { promptOps },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )
        .pipe(Effect.forkChild)

      const input = yield* Effect.promise(() => ready.promise)
      abort.abort()
      expect(yield* Effect.promise(() => cancelled.promise)).toBe(input.sessionID)

      const exit = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(exit)).toBe(true)
    }),
  )

  it.instance("finishes launch work before waking the child on normal and error runs", () =>
    Effect.gen(function* () {
      const delegation = yield* DelegationStore.Service
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const params = {
        description: "inspect bug",
        prompt: "look into the cache key path",
        subagent_type: "general",
      }

      const wake = (events: SessionID[]) => (sessionID: SessionID) =>
        Effect.gen(function* () {
          const generation = yield* delegation.active(sessionID)
          if (generation === undefined) throw new Error("child generation was not active at wake time")
          const work = yield* delegation.listWork(generation.id)
          expect(work.every((item) => item.state === "finished")).toBe(true)
          events.push(sessionID)
        }).pipe(Effect.orDie)

      const normal = yield* seed("Normal wake", "call-normal-wake")
      const normalWake: SessionID[] = []
      const normalOps: TaskPromptOps = {
        ...stubOps({ text: "normal output" }),
        wake: wake(normalWake),
      }
      const normalResult = yield* def.execute(params, taskContext(normal, normalOps))
      expect(normalWake).toEqual([normalResult.metadata.sessionId])

      const error = yield* seed("Error wake", "call-error-wake")
      const errorWake: SessionID[] = []
      const errorOps: TaskPromptOps = {
        ...stubOps({
          error: new SessionV1.APIError({ message: "child failed", isRetryable: false }).toObject(),
        }),
        wake: wake(errorWake),
      }
      const errorExit = yield* def.execute(params, taskContext(error, errorOps)).pipe(Effect.exit)
      expect(Exit.isFailure(errorExit)).toBe(true)
      expect(errorWake).toHaveLength(1)
    }),
  )

  it.instance("captures resolver failure as terminal input before waking parent then child", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const delegation = yield* DelegationStore.Service
      const fixture = yield* seed("Resolver failure", "call-resolver-failure")
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const wakes: SessionID[] = []
      const promptOps: TaskPromptOps = {
        ...stubOps(),
        resolvePromptParts: () => Effect.die(new Error("pre-prompt failed")),
        wake: (sessionID) => Effect.sync(() => wakes.push(sessionID)),
      }

      const exit = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
          },
          taskContext(fixture, promptOps),
        )
        .pipe(Effect.exit)

      const child = (yield* sessions.children(fixture.chat.id))[0]
      if (child === undefined) throw new Error("resolver failure did not create a child")
      const generation = yield* delegation.active(child.id)
      if (generation === undefined) throw new Error("resolver failure did not register a generation")
      const sources = yield* delegation.sources(child.id)
      const terminal = sources.find((source) => source.source.kind === "terminal")
      const work = yield* delegation.listWork(generation.id)

      expect(Exit.isFailure(exit)).toBe(true)
      expect(wakes).toEqual([fixture.chat.id, child.id])
      expect(work.every((item) => item.state === "finished")).toBe(true)
      expect(terminal).toMatchObject({ state: "finalized", outcome: "error", payload: "pre-prompt failed" })
    }),
  )

  background.instance("settles background resolver failure input and captures its terminal source", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const sessions = yield* Session.Service
      const delegation = yield* DelegationStore.Service
      const fixture = yield* seed("Background resolver failure", "call-background-resolver-failure")
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const wakes: SessionID[] = []
      const promptOps: TaskPromptOps = {
        ...stubOps(),
        resolvePromptParts: () => Effect.die(new Error("background pre-prompt failed")),
        wake: (sessionID) => Effect.sync(() => wakes.push(sessionID)),
      }

      const started = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          background: true,
        },
        taskContext(fixture, promptOps),
      )
      const child = (yield* sessions.children(fixture.chat.id))[0]
      if (child === undefined) throw new Error("background resolver failure did not create a child")
      const waited = yield* jobs.wait({ id: started.metadata.sessionId, timeout: 1_000 })
      const generation = yield* delegation.active(child.id)
      if (generation === undefined) throw new Error("background resolver failure did not register a generation")
      const sources = yield* delegation.sources(child.id)
      const terminal = sources.find((source) => source.source.kind === "terminal")
      const work = yield* delegation.listWork(generation.id)

      expect(started.metadata.background).toBe(true)
      expect(waited.timedOut).toBe(false)
      expect(waited.info?.status).toBe("error")
      expect(wakes).toEqual([fixture.chat.id, child.id])
      expect(work.every((item) => item.state === "finished")).toBe(true)
      expect(terminal).toMatchObject({
        state: "finalized",
        outcome: "error",
        payload: "background pre-prompt failed",
      })
    }),
  )

  it.instance("rejects an explicit task_id that does not exist", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()

      const exit = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
            task_id: "ses_missing",
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            callID: "call-task",
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps: stubOps() },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )
        .pipe(Effect.exit)

      const kids = yield* sessions.children(chat.id)
      expect(Exit.isFailure(exit)).toBe(true)
      expect(kids).toHaveLength(0)
    }),
  )

  it.instance("rejects malformed and cross-parent task IDs before prompting", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const delegation = yield* DelegationStore.Service
      const parent = yield* seed("Parent A", "call-parent-a")
      const other = yield* seed("Parent B", "call-parent-b")
      const foreign = yield* sessions.create({ parentID: other.chat.id, title: "Foreign child" })
      const tool = yield* TaskTool
      const def = yield* tool.init()
      let prompts = 0
      const promptOps = stubOps({ onPrompt: () => prompts++ })
      const params = {
        description: "inspect bug",
        prompt: "look into the cache key path",
        subagent_type: "general",
      }

      const malformed = yield* def
        .execute({ ...params, task_id: "not-a-session" }, taskContext(parent, promptOps))
        .pipe(Effect.exit)
      const crossParent = yield* def
        .execute({ ...params, task_id: foreign.id }, taskContext(parent, promptOps))
        .pipe(Effect.exit)

      expect(Exit.isFailure(malformed)).toBe(true)
      expect(Exit.isFailure(crossParent)).toBe(true)
      expect(prompts).toBe(0)
      expect(yield* sessions.children(parent.chat.id)).toHaveLength(0)
      expect(yield* delegation.active(foreign.id)).toBeUndefined()
    }),
  )

  it.instance("prevents subagents from launching subagents by default", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const child = yield* sessions.create({ parentID: chat.id, title: "child" })
      const nestedAssistant = yield* sessions.updateMessage({
        ...assistant,
        id: MessageID.ascending(),
        parentID: MessageID.ascending(),
        sessionID: child.id,
      })
      yield* sessions.updatePart(taskOriginPart(child.id, nestedAssistant.id, "call-nested"))
      const tool = yield* TaskTool
      const def = yield* tool.init()
      let asked = false

      const exit = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
          },
          {
            sessionID: child.id,
            messageID: nestedAssistant.id,
            callID: "call-nested",
            agent: "general",
            abort: new AbortController().signal,
            extra: { promptOps: stubOps() },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.sync(() => (asked = true)),
          },
        )
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      expect(asked).toBe(false)
      expect(yield* sessions.children(child.id)).toHaveLength(0)
    }),
  )

  it.instance(
    "allows nested subagents up to the configured depth",
    () =>
      Effect.gen(function* () {
        const sessions = yield* Session.Service
        const { chat, assistant } = yield* seed()
        const child = yield* sessions.create({ parentID: chat.id, title: "child" })
        const nestedAssistant = yield* sessions.updateMessage({
          ...assistant,
          id: MessageID.ascending(),
          parentID: MessageID.ascending(),
          sessionID: child.id,
        })
        yield* sessions.updatePart(taskOriginPart(child.id, nestedAssistant.id, "call-nested"))
        const tool = yield* TaskTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
          },
          {
            sessionID: child.id,
            messageID: nestedAssistant.id,
            callID: "call-nested",
            agent: "general",
            abort: new AbortController().signal,
            extra: { promptOps: stubOps() },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        expect((yield* sessions.get(result.metadata.sessionId)).parentID).toBe(child.id)
      }),
    { config: { subagent_depth: 2 } },
  )

  it.instance(
    "execute shapes child permissions for task, todowrite, and primary tools",
    () =>
      Effect.gen(function* () {
        const sessions = yield* Session.Service
        const { chat, assistant } = yield* seed()
        const tool = yield* TaskTool
        const def = yield* tool.init()
        let seen: SessionPrompt.PromptInput | undefined
        const promptOps = stubOps({ onPrompt: (input) => (seen = input) })

        const result = yield* def.execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "reviewer",
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            callID: "call-task",
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        const child = yield* sessions.get(result.metadata.sessionId)
        expect(child.parentID).toBe(chat.id)
        expect(child.agent).toBe("reviewer")
        expect(child.permission).toEqual([
          {
            permission: "todowrite",
            pattern: "*",
            action: "deny",
          },
          {
            permission: "bash",
            pattern: "*",
            action: "deny",
          },
          {
            permission: "read",
            pattern: "*",
            action: "deny",
          },
        ])
        expect(seen?.tools).toBeUndefined()
      }),
    {
      config: {
        agent: {
          reviewer: {
            mode: "subagent",
            permission: {
              task: "allow",
            },
          },
        },
        experimental: {
          primary_tools: ["bash", "read"],
        },
      },
    },
  )

  it.instance("rejects background execution when the experiment is disabled", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()

      const exit = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
            background: true,
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            callID: "call-task",
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps: stubOps() },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
    }),
  )

  it.instance("rejects an omitted background mode when active ownership is background and the flag is off", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const delegation = yield* DelegationStore.Service
      const fixture = yield* seed("Disabled background", "call-disabled-background")
      const child = yield* sessions.create({ parentID: fixture.chat.id, title: "Existing background child" })
      const registration = yield* registerOrigin(delegation, {
        parentID: fixture.chat.id,
        childID: child.id,
        part: fixture.task,
        mode: "background",
      })
      const tool = yield* TaskTool
      const def = yield* tool.init()
      let prompts = 0

      const exit = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
            task_id: child.id,
          },
          taskContext(fixture, stubOps({ onPrompt: () => prompts++ })),
        )
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      expect(prompts).toBe(0)
      expect((yield* delegation.active(child.id))?.id).toBe(registration.generation.id)
      expect((yield* delegation.active(child.id))?.mode).toBe("background")
    }),
  )

  background.instance("waits for every foreground extension and invokes each prompt once", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const delegation = yield* DelegationStore.Service
      const fixture = yield* seed("Foreground extension", "call-foreground-extension")
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const firstStarted = yield* Deferred.make<void>()
      const firstRelease = defer<void>()
      const secondStarted = yield* Deferred.make<void>()
      const secondRelease = defer<void>()
      let prompts = 0
      const promptOps: TaskPromptOps = {
        ...stubOps(),
        prompt: (input) => {
          prompts++
          if (prompts === 1) {
            return Effect.gen(function* () {
              yield* Deferred.succeed(firstStarted, undefined)
              yield* Effect.promise(() => firstRelease.promise)
              return reply(input, "first foreground result")
            })
          }
          return Effect.gen(function* () {
            yield* Deferred.succeed(secondStarted, undefined)
            yield* Effect.promise(() => secondRelease.promise)
            return reply(input, "second foreground result")
          })
        },
      }
      const firstFiber = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
            background: false,
          },
          taskContext(fixture, promptOps),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(firstStarted)

      const child = (yield* sessions.children(fixture.chat.id))[0]
      if (child === undefined) throw new Error("foreground extension did not create a child")
      const retryTurn = yield* taskTurn(sessions, fixture.assistant, fixture.chat.id, "call-foreground-extension-retry")
      const secondFiber = yield* def
        .execute(
          {
            description: "extend investigation",
            prompt: "also inspect cancellation",
            subagent_type: "general",
            task_id: child.id,
          },
          taskContext({ chat: fixture.chat, assistant: retryTurn.assistant, callID: retryTurn.callID }, promptOps),
        )
        .pipe(Effect.forkChild)

      const generation = yield* delegation.active(child.id)
      if (generation === undefined) throw new Error("foreground extension did not register a generation")
      while ((yield* delegation.listWork(generation.id)).length < 2) yield* Effect.yieldNow
      yield* Effect.yieldNow
      firstRelease.resolve()
      yield* Deferred.await(secondStarted)
      expect(firstFiber.pollUnsafe()).toBeUndefined()
      expect(secondFiber.pollUnsafe()).toBeUndefined()

      secondRelease.resolve()
      const firstResult = yield* Fiber.join(firstFiber)
      const secondResult = yield* Fiber.join(secondFiber)
      expect(prompts).toBe(2)
      expect(firstResult.metadata.background).toBeUndefined()
      expect(secondResult.metadata.background).toBeUndefined()
      expect(firstResult.output).toContain(`state="completed"`)
      expect(secondResult.output).toContain(`state="completed"`)
    }),
  )

  background.instance("promotes a running foreground task without restarting it", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const ready = yield* Deferred.make<void>()
      const done = yield* Deferred.make<void>()
      let runs = 0
      const promptSessions: SessionID[] = []
      const metadataCalls: Array<{ title?: string; metadata?: Record<string, unknown> }> = []
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) => {
          return Effect.gen(function* () {
            runs += 1
            promptSessions.push(input.sessionID)
            yield* Deferred.succeed(ready, undefined)
            yield* Deferred.await(done)
            return reply(input, "background done")
          })
        },
      }

      const fiber = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            callID: "call-task",
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps },
            messages: [],
            metadata: (input) => Effect.sync(() => metadataCalls.push(input)),
            ask: () => Effect.void,
          },
        )
        .pipe(Effect.forkChild)

      yield* Deferred.await(ready)
      const job = (yield* jobs.list())[0]
      expect(job).toBeDefined()
      if (!job) throw new Error("task job not found")
      expect(job.metadata?.parentSessionId).toBe(chat.id)
      yield* jobs.promote(job.id)

      const result = yield* Fiber.join(fiber)
      expect(result.metadata.background).toBe(true)
      expect(result.output).toContain(`state="running"`)
      expect((yield* jobs.get(result.metadata.sessionId))?.status).toBe("running")
      expect(runs).toBe(1)
      expect(promptSessions).toEqual([result.metadata.sessionId])
      expect(metadataCalls.at(-1)?.metadata).toMatchObject({
        background: true,
        delegationID: result.metadata.delegationID,
      })

      yield* Deferred.succeed(done, undefined)
      expect((yield* jobs.wait({ id: result.metadata.sessionId })).info?.output).toBe("background done")
      expect(runs).toBe(1)
    }),
  )

  background.instance("execute launches background tasks without waiting for completion", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const delegation = yield* DelegationStore.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const promptStarted = yield* Deferred.make<void>()
      const promptRelease = defer<void>()

      const result = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          background: true,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          callID: "call-task",
          agent: "build",
          abort: new AbortController().signal,
          extra: {
            promptOps: {
              ...stubOps(),
              prompt: (input) =>
                Effect.gen(function* () {
                  yield* Deferred.succeed(promptStarted, undefined)
                  yield* Effect.promise(() => promptRelease.promise)
                  return reply(input, "background result")
                }),
            } satisfies TaskPromptOps,
          },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      yield* Deferred.await(promptStarted)
      const job = yield* jobs.get(result.metadata.sessionId)
      const generation = yield* delegation.active(result.metadata.sessionId)
      if (generation === undefined) throw new Error("background launch did not register a generation")
      const work = yield* delegation.listWork(generation.id)
      expect(result.metadata.background).toBe(true)
      expect(result.output).toContain(`state="running"`)
      expect(job?.status).toBe("running")
      expect(work).toHaveLength(1)
      expect(work[0]?.kind).toBe("launch")
      expect(work[0]?.state).toBe("active")

      promptRelease.resolve()
      expect((yield* jobs.wait({ id: result.metadata.sessionId })).info?.status).toBe("completed")
      expect((yield* delegation.listWork(generation.id))[0]?.state).toBe("finished")
    }),
  )

  background.instance("background task completion waits for running updates", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const first = defer<void>()
      const second = defer<void>()
      const updated = defer<SessionPrompt.PromptInput>()
      let prompts = 0
      const promptSessions: SessionID[] = []
      const promptOps: TaskPromptOps = {
        ...stubOps(),
        prompt: (input) => {
          prompts++
          promptSessions.push(input.sessionID)
          if (prompts === 1) return Effect.promise(() => first.promise).pipe(Effect.as(reply(input, "first done")))
          updated.resolve(input)
          return Effect.promise(() => second.promise).pipe(Effect.as(reply(input, "second done")))
        },
      }
      const context = {
        sessionID: chat.id,
        messageID: assistant.id,
        callID: "call-task",
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }
      const resumeTurn = yield* taskTurn(sessions, assistant, chat.id, "call-task-resume")
      const resumeContext = {
        ...context,
        messageID: resumeTurn.assistant.id,
        callID: resumeTurn.callID,
      }

      const started = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          background: true,
        },
        context,
      )
      const result = yield* def.execute(
        {
          description: "add investigation scope",
          prompt: "also inspect cancellation",
          subagent_type: "general",
          task_id: started.metadata.sessionId,
        },
        resumeContext,
      )

      expect(result.metadata.sessionId).toBe(started.metadata.sessionId)
      expect(result.metadata.background).toBe(true)
      expect(result.output).toContain("Background task updated")
      first.resolve()
      expect((yield* jobs.get(started.metadata.sessionId))?.status).toBe("running")
      expect((yield* Effect.promise(() => updated.promise)).parts).toEqual([
        { type: "text", text: "also inspect cancellation" },
      ])

      second.resolve()
      const waited = yield* jobs.wait({ id: started.metadata.sessionId, timeout: 1_000 })
      expect(waited.info?.status).toBe("completed")
      expect(waited.info?.output).toBe("second done")
      expect(prompts).toBe(2)
      expect(promptSessions).not.toContain(chat.id)
    }),
  )

  background.instance("background tasks complete through the background job service", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()

      const result = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          background: true,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          callID: "call-task",
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps: stubOps({ text: "background done" }) },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      const waited = yield* jobs.wait({ id: result.metadata.sessionId, timeout: 1_000 })
      expect(waited.timedOut).toBe(false)
      expect(waited.info?.status).toBe("completed")
      expect(waited.info?.output).toBe("background done")
    }),
  )

  background.instance("background task completion never prompts the parent session", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const promptSessions: SessionID[] = []
      const wakeSessions: SessionID[] = []

      const result = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          background: true,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          callID: "call-task",
          agent: "build",
          abort: new AbortController().signal,
          extra: {
            promptOps: {
              ...stubOps({ text: "background done", onWake: (sessionID) => wakeSessions.push(sessionID) }),
              prompt: (input) =>
                Effect.sync(() => {
                  promptSessions.push(input.sessionID)
                  return reply(input, "background done")
                }),
            } satisfies TaskPromptOps,
          },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      const waited = yield* jobs.wait({ id: result.metadata.sessionId, timeout: 1_000 })
      expect(waited.timedOut).toBe(false)
      expect(waited.info?.status).toBe("completed")
      expect(promptSessions).toEqual([result.metadata.sessionId])
      expect(wakeSessions).toEqual([result.metadata.sessionId])
    }),
  )

  background.instance("removing the parent session cancels running background tasks", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()

      const result = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          background: true,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          callID: "call-task",
          agent: "build",
          abort: new AbortController().signal,
          extra: {
            promptOps: {
              ...stubOps(),
              prompt: () => Effect.never,
            } satisfies TaskPromptOps,
          },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      yield* sessions.remove(chat.id)
      const waited = yield* jobs.wait({ id: result.metadata.sessionId, timeout: 1_000 })
      expect(waited.timedOut).toBe(false)
      expect(waited.info?.status).toBe("cancelled")
    }),
  )

  background.instance("removing the child task session cancels its running background task", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()

      const result = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          background: true,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          callID: "call-task",
          agent: "build",
          abort: new AbortController().signal,
          extra: {
            promptOps: {
              ...stubOps(),
              prompt: () => Effect.never,
            } satisfies TaskPromptOps,
          },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      yield* sessions.remove(result.metadata.sessionId)
      const waited = yield* jobs.wait({ id: result.metadata.sessionId, timeout: 1_000 })
      expect(waited.timedOut).toBe(false)
      expect(waited.info?.status).toBe("cancelled")
    }),
  )

  background.instance("cancelling the parent run cancels running background tasks", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const runState = yield* SessionRunState.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()

      const result = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          background: true,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          callID: "call-task",
          agent: "build",
          abort: new AbortController().signal,
          extra: {
            promptOps: {
              ...stubOps(),
              prompt: () => Effect.never,
            } satisfies TaskPromptOps,
          },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      yield* runState.cancel(chat.id)
      const waited = yield* jobs.wait({ id: result.metadata.sessionId, timeout: 1_000 })
      expect(waited.timedOut).toBe(false)
      expect(waited.info?.status).toBe("cancelled")
    }),
  )

  it.instance("cancelling a child run cancels its own pre-runner task job", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const runState = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const { chat } = yield* seed()
      const child = yield* sessions.create({ parentID: chat.id, title: "child" })

      yield* jobs.start({
        id: child.id,
        type: "task",
        metadata: { parentSessionId: chat.id, sessionId: child.id },
        run: Effect.never,
      })

      yield* runState.cancel(child.id)

      expect((yield* jobs.get(child.id))?.status).toBe("cancelled")
    }),
  )

  it.instance("cancelling a parent run recursively cancels descendant background tasks", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const runState = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const { chat } = yield* seed()
      const child = yield* sessions.create({ parentID: chat.id, title: "child" })
      const grandchild = yield* sessions.create({ parentID: child.id, title: "grandchild" })

      yield* jobs.start({
        id: child.id,
        type: "task",
        metadata: { parentSessionId: chat.id, sessionId: child.id },
        run: Effect.never,
      })
      yield* jobs.start({
        id: grandchild.id,
        type: "task",
        metadata: { parentSessionId: child.id, sessionId: grandchild.id },
        run: Effect.never,
      })

      yield* runState.cancel(chat.id)

      expect((yield* jobs.get(child.id))?.status).toBe("cancelled")
      expect((yield* jobs.get(grandchild.id))?.status).toBe("cancelled")
    }),
  )
})
