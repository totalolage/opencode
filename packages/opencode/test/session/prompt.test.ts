import { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { DelegationStore } from "@opencode-ai/core/delegation"
import {
  DelegationRegistrationTable,
  DelegationResolutionTable,
  DelegationRevocationTable,
  DelegationSourceTable,
} from "@opencode-ai/core/delegation/sql"
import { EventV2 } from "@opencode-ai/core/event"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { and, eq, inArray } from "drizzle-orm"
import { EventV2Bridge } from "@/event-v2-bridge"
import { GlobalBus, type GlobalEvent } from "@/bus/global"
import { expect } from "bun:test"
import { Cause, Deferred, Duration, Effect, Exit, Fiber, Layer, Option, Schema } from "effect"
import path from "path"
import { fileURLToPath } from "url"
import { NamedError } from "@opencode-ai/core/util/error"
import { Agent as AgentSvc } from "../../src/agent/agent"
import { BackgroundJob } from "@/background/job"
import { Command } from "../../src/command"
import { Config } from "@/config/config"
import { LSP } from "@/lsp/lsp"
import { MCP } from "../../src/mcp"
import { Permission } from "../../src/permission"
import { Plugin } from "../../src/plugin"
import { Provider as ProviderSvc } from "@/provider/provider"
import { Env } from "../../src/env"
import { Git } from "../../src/git"
import { Image } from "../../src/image/image"

import { Question } from "../../src/question"
import { Todo } from "../../src/session/todo"
import { Session } from "@/session/session"
import { MessageTable, PartTable, SessionMessageTable, SessionTable } from "@opencode-ai/core/session/sql"
import { LLM } from "../../src/session/llm"
import { MessageV2 } from "../../src/session/message-v2"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { SessionCompaction } from "../../src/session/compaction"
import { SessionDelegation } from "../../src/session/delegation"
import { SessionSummary } from "../../src/session/summary"
import { Instruction } from "../../src/session/instruction"
import { SessionProcessor } from "../../src/session/processor"
import { SessionPrompt } from "../../src/session/prompt"
import { SessionRevert } from "../../src/session/revert"
import { SessionRunState } from "../../src/session/run-state"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { SessionStatus } from "../../src/session/status"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { Skill } from "../../src/skill"
import { SystemPrompt } from "../../src/session/system"
import { Shell } from "@opencode-ai/core/shell"
import { Snapshot } from "../../src/snapshot"
import { ToolRegistry } from "@/tool/registry"
import { Truncate } from "@/tool/truncate"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { Format } from "../../src/format"
import { TestInstance } from "../fixture/fixture"
import { awaitWithTimeout, pollWithTimeout, testEffect } from "../lib/effect"
import { reply, TestLLMServer } from "../lib/llm-server"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { LocationServiceMap, locationServiceMapLayer } from "@opencode-ai/core/location-services"

const summary = Layer.succeed(
  SessionSummary.Service,
  SessionSummary.Service.of({
    summarize: () => Effect.void,
    diff: () => Effect.succeed([]),
    computeDiff: () => Effect.succeed([]),
  }),
)

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

const decodeDelegationInputMetadata = Schema.decodeUnknownOption(
  Schema.Struct({
    delegationInput: Schema.Struct({ workID: Delegation.WorkID }),
  }),
)

function withSh<A, E, R>(fx: () => Effect.Effect<A, E, R>) {
  return Effect.acquireUseRelease(
    Effect.sync(() => {
      const prev = process.env.SHELL
      process.env.SHELL = "/bin/sh"
      Shell.preferred.reset()
      return prev
    }),
    () => fx(),
    (prev) =>
      Effect.sync(() => {
        if (prev === undefined) delete process.env.SHELL
        else process.env.SHELL = prev
        Shell.preferred.reset()
      }),
  )
}

function toolPart(parts: SessionV1.Part[]) {
  return parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
}

type CompletedToolPart = SessionV1.ToolPart & { state: SessionV1.ToolStateCompleted }
type ErrorToolPart = SessionV1.ToolPart & { state: SessionV1.ToolStateError }

function completedTool(parts: SessionV1.Part[]) {
  const part = toolPart(parts)
  expect(part?.state.status).toBe("completed")
  return part?.state.status === "completed" ? (part as CompletedToolPart) : undefined
}

type TestLLMHit = { body: Record<string, unknown> }

const hasSystemMarker = (marker: string) => (hit: TestLLMHit) => {
  const messages = hit.body.messages
  if (!Array.isArray(messages)) return false
  return messages.some((message) => {
    if (typeof message !== "object" || message === null) return false
    if (!("role" in message) || message.role !== "system") return false
    return "content" in message && typeof message.content === "string" && message.content.includes(marker)
  })
}

const messageContents = (hit: TestLLMHit) => {
  const messages = hit.body.messages
  if (!Array.isArray(messages)) return []
  return messages.flatMap((message) => {
    if (typeof message !== "object" || message === null) return []
    if (!("content" in message) || typeof message.content !== "string") return []
    return [message.content]
  })
}

const taskResult = (childID: SessionID, payload: string) =>
  ["<task>", `<task_id>${childID}</task_id>`, "<task_result>", payload, "</task_result>", "</task>"].join("\n")

const taskError = (childID: SessionID, payload: string) =>
  ["<task>", `<task_id>${childID}</task_id>`, "<task_error>", payload, "</task_error>", "</task>"].join("\n")

const taskCancelled = (childID: SessionID, payload = "Task cancelled") =>
  ["<task>", `<task_id>${childID}</task_id>`, "<task_cancelled>", payload, "</task_cancelled>", "</task>"].join("\n")

const completedTaskResult = (childID: SessionID, payload: string) =>
  [`<task id="${childID}" state="completed">`, "<task_result>", payload, "</task_result>", "</task>"].join("\n")

function waitForSystem<A extends TestLLMHit>(
  llm: {
    readonly hits: Effect.Effect<A[]>
    readonly wait: (count: number) => Effect.Effect<void>
  },
  marker: string,
  count = 1,
) {
  return awaitWithTimeout(
    Effect.gen(function* () {
      while (true) {
        const hits = yield* llm.hits
        const matching = hits.filter(hasSystemMarker(marker))
        const found = matching[count - 1]
        if (found) return found
        yield* llm.wait(hits.length + 1)
      }
    }),
    `timed out waiting for ${marker} provider hit ${count}`,
    "10 seconds",
  )
}

function waitForFinalizedAssistantSource(delegation: DelegationStore.Interface, sessionID: SessionID, payload: string) {
  return awaitWithTimeout(
    Effect.gen(function* () {
      while (true) {
        const source = (yield* delegation.sources(sessionID)).find(
          (source) => source.state === "finalized" && source.source.kind === "assistant" && source.payload === payload,
        )
        if (source) return source
        yield* Effect.yieldNow
      }
    }),
    `timed out waiting for finalized assistant source ${payload}`,
    "10 seconds",
  )
}

function waitForAdmittedResolution(delegation: DelegationStore.Interface, parentID: SessionID, sourceID: string) {
  return awaitWithTimeout(
    Effect.gen(function* () {
      while (true) {
        const resolution = (yield* delegation.incoming(parentID)).find(
          (resolution) => resolution.source.id === sourceID,
        )
        if (resolution?.status === "admitted") return resolution
        yield* Effect.yieldNow
      }
    }),
    `timed out waiting for admitted resolution ${sourceID}`,
    "10 seconds",
  )
}

function waitForAdmittedChildResolution(
  delegation: DelegationStore.Interface,
  parentID: SessionID,
  childID: SessionID,
) {
  return awaitWithTimeout(
    Effect.gen(function* () {
      while (true) {
        const resolution = (yield* delegation.incoming(parentID)).find((resolution) => resolution.childID === childID)
        if (resolution?.status === "admitted") return resolution
        yield* Effect.yieldNow
      }
    }),
    `timed out waiting for admitted child resolution ${childID}`,
    "10 seconds",
  )
}

function waitForNotBusy(runState: SessionRunState.Interface, sessionID: SessionID) {
  return awaitWithTimeout(
    Effect.gen(function* () {
      while (yield* runState.busy(sessionID)) yield* Effect.yieldNow
    }),
    `timed out waiting for session ${sessionID} to become idle`,
    "10 seconds",
  )
}

function decodeDelegationHistoryCutoff(value: string) {
  return Effect.gen(function* () {
    const json = yield* Schema.decodeUnknownEffect(Schema.UnknownFromJsonString)(value).pipe(Effect.orDie)
    return yield* Schema.decodeUnknownEffect(
      Schema.Struct({
        messages: Schema.Array(Schema.String),
        work: Schema.Array(Delegation.WorkID),
      }),
    )(json).pipe(Effect.orDie)
  })
}

function listenForPartText(events: EventV2.Interface, sessionID: SessionID, text: string, messageID?: MessageID) {
  return Effect.gen(function* () {
    const finished = yield* Deferred.make<void>()
    const off = yield* events.listen((event) => {
      if (event.type !== MessageV2.Event.PartUpdated.type) return Effect.void
      const data = event.data as typeof MessageV2.Event.PartUpdated.data.Type
      if (
        data.sessionID === sessionID &&
        (messageID === undefined || data.part.messageID === messageID) &&
        data.part.type === "text" &&
        data.part.text === text
      ) {
        Deferred.doneUnsafe(finished, Effect.void)
      }
      return Effect.void
    })
    yield* Effect.addFinalizer(() => off)
    return finished
  })
}

function seedRecordedBackgroundFailure(
  sessions: Session.Interface,
  core: DelegationStore.Interface,
  adapter: SessionDelegation.Interface,
  rootID: SessionID,
  originMessage: SessionV1.Assistant,
  input?: { readonly childAgent?: string; readonly error?: string },
) {
  return Effect.gen(function* () {
    const childAgent = input?.childAgent ?? "a"
    const error = input?.error ?? "RECORDED_ERROR"
    const child = yield* sessions.create({
      parentID: rootID,
      title: "Recorded background child",
      agent: childAgent,
      model: { providerID: ref.providerID, id: ref.modelID },
    })
    const callID = `recorded-return-${child.id}`
    const originPart = yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: MessageID.make(originMessage.id),
      sessionID: rootID,
      type: "tool",
      callID,
      tool: "task",
      state: {
        status: "running",
        input: {
          description: "recorded return",
          prompt: "recorded return",
          subagent_type: childAgent,
        },
        time: { start: Date.now() },
      },
    })
    if (originPart.type !== "tool") throw new Error("expected persisted task origin part")

    const registered = yield* core.register({
      requestID: Delegation.RequestID.create(),
      generationID: Delegation.ID.create(),
      parentID: rootID,
      childID: child.id,
      origin: {
        messageID: SessionMessage.ID.make(originMessage.id),
        partID: Delegation.OriginPartID.make(originPart.id),
        callID: originPart.callID,
      },
      mode: "background",
      explicitReuse: false,
    })
    yield* adapter.failInput(registered.generation, registered.workID, error)
    return { child, error, originPart, registered }
  })
}

function seedInterruptedRootBoundary(
  mode: "provider" | "tool",
  sessions: Session.Interface,
  core: DelegationStore.Interface,
  rootID: SessionID,
  assistant: SessionV1.Assistant,
) {
  if (mode === "tool") {
    return core
      .startWork({
        id: Delegation.WorkID.make(`dwk_recovery_tool_${rootID}`),
        kind: "tool",
        sessionID: rootID,
      })
      .pipe(Effect.map((work) => ({ mode, work }) as const))
  }

  return Effect.gen(function* () {
    const placeholder = yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "assistant",
      parentID: assistant.parentID,
      sessionID: rootID,
      mode: assistant.mode,
      agent: assistant.agent,
      cost: 0,
      path: assistant.path,
      tokens: assistant.tokens,
      modelID: assistant.modelID,
      providerID: assistant.providerID,
      ...(assistant.variant === undefined ? {} : { variant: assistant.variant }),
      time: { created: Date.now() },
    })
    const reserved = yield* core.reserveSource({
      sessionID: rootID,
      source: { kind: "assistant", id: placeholder.id },
      historyCutoff: "recovery-provider-boundary",
      consumed: [],
    })
    const work = (yield* core.unfinished(rootID)).find((item) => item.id === reserved.source.workID)
    if (work === undefined) throw new Error("expected provider boundary work")
    return { mode, source: reserved.source, work } as const
  })
}

function errorTool(parts: SessionV1.Part[]) {
  const part = toolPart(parts)
  expect(part?.state.status).toBe("error")
  return part?.state.status === "error" ? (part as ErrorToolPart) : undefined
}

function makeMcp(instructions: MCP.ServerInstructions[] = []) {
  return Layer.succeed(
    MCP.Service,
    MCP.Service.of({
      status: () => Effect.succeed({}),
      clients: () => Effect.succeed({}),
      instructions: () => Effect.succeed(instructions),
      tools: () => Effect.succeed({}),
      prompts: () => Effect.succeed({}),
      resources: () => Effect.succeed({}),
      resourceTemplates: () => Effect.succeed({}),
      add: () => Effect.succeed({ status: { status: "disabled" as const } }),
      connect: () => Effect.void,
      disconnect: () => Effect.void,
      getPrompt: () => Effect.succeed(undefined),
      readResource: () => Effect.succeed(undefined),
      startAuth: () => Effect.die("unexpected MCP auth in prompt-effect tests"),
      authenticate: () => Effect.die("unexpected MCP auth in prompt-effect tests"),
      finishAuth: () => Effect.die("unexpected MCP auth in prompt-effect tests"),
      removeAuth: () => Effect.void,
      supportsOAuth: () => Effect.succeed(false),
      hasStoredTokens: () => Effect.succeed(false),
      getAuthStatus: () => Effect.succeed("not_authenticated" as const),
    }),
  )
}

const lsp = Layer.succeed(
  LSP.Service,
  LSP.Service.of({
    init: () => Effect.void,
    status: () => Effect.succeed([]),
    hasClients: () => Effect.succeed(false),
    touchFile: () => Effect.void,
    diagnostics: () => Effect.succeed({}),
    hover: () => Effect.succeed(undefined),
    definition: () => Effect.succeed([]),
    references: () => Effect.succeed([]),
    implementation: () => Effect.succeed([]),
    documentSymbol: () => Effect.succeed([]),
    workspaceSymbol: () => Effect.succeed([]),
    prepareCallHierarchy: () => Effect.succeed([]),
    incomingCalls: () => Effect.succeed([]),
    outgoingCalls: () => Effect.succeed([]),
  }),
)

const processorCreateStarted: Array<() => void> = []
const blockingProcessor = Layer.succeed(
  SessionProcessor.Service,
  SessionProcessor.Service.of({
    create: () => Effect.sync(() => processorCreateStarted.shift()?.()).pipe(Effect.andThen(Effect.never)),
  }),
)

const testLLMServerNode = LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })

const promptRoot = LayerNode.group([
  SessionPrompt.node,
  Session.node,
  SessionProjector.node,
  MessageV2.node,
  Snapshot.node,
  LLM.node,
  Env.node,
  AgentSvc.node,
  Command.node,
  Permission.node,
  Plugin.node,
  Config.node,
  ProviderSvc.node,
  LSP.node,
  MCP.node,
  FSUtil.node,
  BackgroundJob.node,
  SessionStatus.node,
  SessionRunState.node,
  Database.node,
  DelegationStore.node,
  SessionDelegation.node,
  EventV2Bridge.node,
  Question.node,
  Todo.node,
  ToolRegistry.node,
  Skill.node,
  Git.node,
  Ripgrep.node,
  Format.node,
  Truncate.node,
  SessionProcessor.node,
  Image.node,
  SessionCompaction.node,
  SessionRevert.node,
  Instruction.node,
  SystemPrompt.node,
  CrossSpawnSpawner.node,
  RuntimeFlags.node,
])

type PromptFixtureInput = {
  mcpInstructions?: MCP.ServerInstructions[]
  processor?: "blocking"
  background?: boolean
}

function makePrompt(input?: PromptFixtureInput) {
  const replacements = [
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp(input?.mcpInstructions)],
    [
      RuntimeFlags.node,
      RuntimeFlags.layer({
        experimentalEventSystem: true,
        experimentalBackgroundSubagents: input?.background ?? false,
      }),
    ],
  ] as const
  if (input?.processor === "blocking") {
    return LayerNode.compile(promptRoot, [...replacements, [SessionProcessor.node, blockingProcessor]])
  }
  return LayerNode.compile(promptRoot, replacements)
}

function makeHttp(input?: PromptFixtureInput) {
  const root = LayerNode.group([promptRoot, testLLMServerNode])
  const replacements = [
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp(input?.mcpInstructions)],
    [
      RuntimeFlags.node,
      RuntimeFlags.layer({
        experimentalEventSystem: true,
        experimentalBackgroundSubagents: input?.background ?? false,
      }),
    ],
  ] as const
  if (input?.processor === "blocking") {
    return LayerNode.compile(root, [...replacements, [SessionProcessor.node, blockingProcessor]])
  }
  return LayerNode.compile(root, replacements)
}

function makeHttpNoLLMServer(input?: PromptFixtureInput) {
  return makePrompt(input)
}

const it = testEffect(makeHttp())
const background = testEffect(makeHttp({ background: true }))
const noLLMServer = testEffect(makeHttpNoLLMServer())
const raceNoLLMServer = testEffect(makeHttpNoLLMServer({ processor: "blocking" }))
const withMcpInstructions = testEffect(
  makeHttp({
    mcpInstructions: [
      {
        name: "guide-server",
        instructions: "Use lookup before mutate.",
        tools: ["guide-server_lookup"],
      },
    ],
  }),
)
const unix = process.platform !== "win32" ? it.instance : it.instance.skip
const unixNoLLMServer = process.platform !== "win32" ? noLLMServer.instance : noLLMServer.instance.skip

// Config that registers a custom "test" provider with a "test-model" model
// so provider model lookup succeeds inside the loop.
const cfg = {
  provider: {
    test: {
      name: "Test",
      id: "test",
      env: [],
      npm: "@ai-sdk/openai-compatible",
      models: {
        "test-model": {
          id: "test-model",
          name: "Test Model",
          attachment: false,
          reasoning: false,
          temperature: false,
          tool_call: true,
          release_date: "2025-01-01",
          limit: { context: 100000, output: 10000 },
          cost: { input: 0, output: 0 },
          options: {},
        },
      },
      options: {
        apiKey: "test-key",
        baseURL: "http://localhost:1/v1",
      },
    },
  },
}

function providerCfg(url: string) {
  return {
    ...cfg,
    provider: {
      ...cfg.provider,
      test: {
        ...cfg.provider.test,
        options: {
          ...cfg.provider.test.options,
          baseURL: url,
        },
      },
    },
  }
}

const writeText = Effect.fn("test.writeText")(function* (file: string, text: string) {
  const fs = yield* FSUtil.Service
  yield* fs.writeWithDirs(file, text)
})

const writeConfig = Effect.fn("test.writeConfig")(function* (dir: string, config: Partial<ConfigV1.Info>) {
  yield* writeText(
    path.join(dir, "opencode.json"),
    JSON.stringify({ $schema: "https://opencode.ai/config.json", ...config }),
  )
})

const useServerConfig = Effect.fn("test.useServerConfig")(function* (config: (url: string) => Partial<ConfigV1.Info>) {
  const { directory: dir } = yield* TestInstance
  const llm = yield* TestLLMServer
  yield* writeConfig(dir, config(llm.url))
  return { dir, llm }
})

// Wait for a session's runner to enter a busy state. SessionStatus is flipped
// inside Runner.startShell's serialized transition, so cancel can't no-op once
// we observe it.
const waitForBusy = (sessionID: SessionID, duration: Duration.Input = "2 seconds") =>
  pollWithTimeout(
    Effect.gen(function* () {
      const status = yield* SessionStatus.Service
      const s = yield* status.get(sessionID)
      return s.type === "busy" ? (true as const) : undefined
    }),
    `session ${sessionID} never became busy`,
    duration,
  )

const hasBash = Effect.sync(() => Bun.which("bash") !== null)

const deferredAsPromise = <A>(deferred: Deferred.Deferred<A>): PromiseLike<A> => ({
  then: (onfulfilled, onrejected) => {
    Effect.runFork(
      Deferred.await(deferred).pipe(
        Effect.match({
          onFailure: (error) => {
            onrejected?.(error)
          },
          onSuccess: (value) => {
            onfulfilled?.(value)
          },
        }),
      ),
    )
    return deferredAsPromise(deferred) as PromiseLike<never>
  },
})

function defer<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

const succeedVoid = (deferred: Deferred.Deferred<void>) => {
  Effect.runSync(Deferred.succeed(deferred, void 0).pipe(Effect.ignore))
}

const user = Effect.fn("test.user")(function* (sessionID: SessionID, text: string) {
  const session = yield* Session.Service
  const msg = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: msg.id,
    sessionID,
    type: "text",
    text,
  })
  return msg
})

const seed = Effect.fn("test.seed")(function* (sessionID: SessionID, opts?: { finish?: string }) {
  const session = yield* Session.Service
  const msg = yield* user(sessionID, "hello")
  const assistant: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    parentID: msg.id,
    sessionID,
    mode: "build",
    agent: "build",
    cost: 0,
    path: { cwd: "/tmp", root: "/tmp" },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    time: { created: Date.now() },
    ...(opts?.finish ? { finish: opts.finish } : {}),
  }
  yield* session.updateMessage(assistant)
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: assistant.id,
    sessionID,
    type: "text",
    text: "hi there",
  })
  return { user: msg, assistant }
})

const addSubtask = (sessionID: SessionID, messageID: MessageID, model = ref) =>
  Effect.gen(function* () {
    const session = yield* Session.Service
    yield* session.updatePart({
      id: PartID.ascending(),
      messageID,
      sessionID,
      type: "subtask",
      prompt: "look into the cache key path",
      description: "inspect bug",
      agent: "general",
      model,
    })
  })

const boot = Effect.fn("test.boot")(function* (input?: { title?: string }) {
  const config = yield* Config.Service
  const prompt = yield* SessionPrompt.Service
  const run = yield* SessionRunState.Service
  const sessions = yield* Session.Service
  yield* config.get()
  const chat = yield* sessions.create(input ?? { title: "Pinned" })
  return { prompt, run, sessions, chat }
})

// Loop semantics

noLLMServer.instance(
  "loop exits immediately when last assistant has stop finish",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      yield* seed(chat.id, { finish: "stop" })

      const result = yield* prompt.loop({ sessionID: chat.id })
      expect(result.info.role).toBe("assistant")
      if (result.info.role === "assistant") expect(result.info.finish).toBe("stop")
    }),
  { config: cfg },
)

noLLMServer.instance(
  "loop exits for a completed parent turn with nonmonotonic message IDs",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const userID = MessageID.make("msg_z_user")
      const assistantID = MessageID.make("msg_a_assistant")
      yield* sessions.updateMessage({
        id: userID,
        role: "user",
        sessionID: chat.id,
        agent: "build",
        model: ref,
        time: { created: 100 },
      })
      yield* sessions.updateMessage({
        id: assistantID,
        role: "assistant",
        parentID: userID,
        sessionID: chat.id,
        mode: "build",
        agent: "build",
        cost: 0,
        path: { cwd: "/tmp", root: "/tmp" },
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        modelID: ref.modelID,
        providerID: ref.providerID,
        time: { created: 200, completed: 201 },
        finish: "stop",
      })

      const result = yield* prompt.loop({ sessionID: chat.id })

      expect(result.info.id).toBe(assistantID)
    }),
  { config: cfg },
)

it.instance("loop exits without an LLM request for interrupted orphan tool calls", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    const seeded = yield* seed(chat.id, { finish: "stop" })
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: seeded.assistant.id,
      sessionID: chat.id,
      type: "tool",
      callID: "interrupted-call",
      tool: "edit",
      state: {
        status: "error",
        input: {},
        error: "Tool execution aborted",
        metadata: { interrupted: true },
        time: { start: 1, end: 2 },
      },
    })

    const result = yield* prompt.loop({ sessionID: chat.id })
    expect(result.info.id).toBe(seeded.assistant.id)
    expect(yield* llm.hits).toHaveLength(0)
  }),
)

it.instance("loop calls LLM and returns assistant message", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({
      title: "Pinned",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.text("world")

    const result = yield* prompt.loop({ sessionID: chat.id })
    expect(result.info.role).toBe("assistant")
    const parts = result.parts.filter((p) => p.type === "text")
    expect(parts.some((p) => p.type === "text" && p.text === "world")).toBe(true)
    expect(yield* llm.hits).toHaveLength(1)
  }),
)

withMcpInstructions.instance(
  "loop includes MCP instructions in model system context",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* llm.hang
      yield* user(chat.id, "hello")

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "timed out waiting for MCP instruction request", "10 seconds")

      const hits = yield* llm.hits
      const body = JSON.stringify(hits[0]?.body)
      expect(body).toContain('<server name=\\"guide-server\\">')
      expect(body).toContain("Use lookup before mutate.")
      yield* Fiber.interrupt(fiber)
    }),
  15_000,
)

it.instance("legacy prompt emits message events without session.next events", () =>
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({
      title: "Pinned",
      agent: "plan",
      model: { providerID: ProviderV2.ID.make("old"), id: ModelV2.ID.make("old-model") },
    })
    const seen: string[] = []
    const off = yield* events.listen((event) => {
      seen.push(event.type)
      return Effect.void
    })

    const first = yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      model: ref,
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    const second = yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "again" }],
    })
    yield* off

    expect(first.info.role).toBe("user")
    expect(second.info.role).toBe("user")
    if (first.info.role === "user" && second.info.role === "user") {
      expect(first.info.model).toEqual(ref)
      expect(second.info.model).toEqual(ref)
    }
    expect(yield* sessions.get(chat.id)).toMatchObject({
      agent: "build",
      model: { providerID: ref.providerID, id: ref.modelID },
    })
    expect(seen).toContain(Session.Event.Updated.type)
    expect(seen).toContain(MessageV2.Event.Updated.type)
    expect(seen).toContain(MessageV2.Event.PartUpdated.type)
    expect(seen.filter((type) => type.startsWith("session.next."))).toEqual([])
  }),
)

it.instance("loop surfaces content-filter finishes as session errors", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const events = yield* EventV2Bridge.Service
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    const errors: NonNullable<SessionV1.Assistant["error"]>[] = []
    const expected = {
      name: "ContentFilterError",
      data: { message: "The response was blocked by the provider's content filter" },
    } satisfies NonNullable<SessionV1.Assistant["error"]>
    const off = yield* events.listen((event) => {
      if (event.type !== Session.Event.Error.type) return Effect.void
      const data = event.data as typeof Session.Event.Error.data.Type
      if (data.sessionID === chat.id && data.error) errors.push(data.error)
      return Effect.void
    })

    yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.push(reply().text("partial response").contentFilter())

    const result = yield* prompt.loop({ sessionID: chat.id })
    const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: result.info.id })
    yield* off

    expect(yield* llm.hits).toHaveLength(1)
    expect(result.info.role).toBe("assistant")
    expect(stored.info.role).toBe("assistant")
    if (result.info.role === "assistant" && stored.info.role === "assistant") {
      expect(result.info.finish).toBe("content-filter")
      expect(result.info.error).toEqual(expected)
      expect(stored.info.error).toEqual(result.info.error)
      expect(errors).toContainEqual(expected)
    }
    expect(result.parts).toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "text", text: "partial response" })]),
    )
  }),
)

it.instance("loop stops provider overflow instead of auto-compacting when disabled", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig((url) => ({
      ...providerCfg(url),
      compaction: { auto: false },
    }))
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })

    yield* llm.error(413, { error: { message: "request entity too large" } })
    yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })

    const result = yield* prompt.loop({ sessionID: chat.id })
    const messages = yield* sessions.messages({ sessionID: chat.id })

    expect(result.info.role).toBe("assistant")
    if (result.info.role === "assistant") {
      expect(result.info.error?.name).toBe("ContextOverflowError")
      expect(result.info.finish).toBe("error")
    }
    expect(messages.some((message) => message.parts.some((part) => part.type === "compaction"))).toBe(false)
  }),
)

noLLMServer.instance.skip(
  "prompt emits v2 prompted and synthetic events (v2 projector disabled)",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })

      yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
        parts: [
          { type: "text", text: "hello v2" },
          {
            type: "file",
            mime: "text/plain",
            filename: "note.txt",
            url: "data:text/plain;base64,bm90ZSBjb250ZW50",
          },
        ],
      })

      const messages = yield* SessionV2.Service.use((session) => session.messages({ sessionID: chat.id })).pipe(
        Effect.provide(
          LayerNode.compile(SessionV2.node, [
            [SessionExecution.node, SessionExecution.noopLayer],
            [LocationServiceMap.node, locationServiceMapLayer],
          ]),
        ),
      )
      const { db } = yield* Database.Service
      const row = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, chat.id))
        .get()
        .pipe(Effect.orDie)
      expect(messages.find((message) => message.type === "user")).toMatchObject({ type: "user", text: "hello v2" })
      expect(typeof row?.data.time.created).toBe("number")
      expect(messages).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ type: "synthetic", text: expect.stringContaining("Called the Read tool") }),
          expect.objectContaining({ type: "synthetic", text: "note content" }),
        ]),
      )
    }),
  { config: cfg },
)

it.instance("static loop returns assistant text through local provider", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Prompt provider",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })

    yield* llm.text("world")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(result.info.role).toBe("assistant")
    expect(result.parts.some((part) => part.type === "text" && part.text === "world")).toBe(true)
    expect(yield* llm.hits).toHaveLength(1)
    expect(yield* llm.pending).toBe(0)
  }),
)

it.instance("static loop consumes queued replies across turns", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Prompt provider turns",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello one" }],
    })

    yield* llm.text("world one")

    const first = yield* prompt.loop({ sessionID: session.id })
    expect(first.info.role).toBe("assistant")
    expect(first.parts.some((part) => part.type === "text" && part.text === "world one")).toBe(true)

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello two" }],
    })

    yield* llm.text("world two")

    const second = yield* prompt.loop({ sessionID: session.id })
    expect(second.info.role).toBe("assistant")
    expect(second.parts.some((part) => part.type === "text" && part.text === "world two")).toBe(true)

    expect(yield* llm.hits).toHaveLength(2)
    expect(yield* llm.pending).toBe(0)
  }),
)

it.instance("loop continues when finish is tool-calls", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Pinned",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.tool("first", { value: "first" })
    yield* llm.text("second")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(yield* llm.calls).toBe(2)
    expect(result.info.role).toBe("assistant")
    if (result.info.role === "assistant") {
      expect(result.parts.some((part) => part.type === "text" && part.text === "second")).toBe(true)
      expect(result.info.finish).toBe("stop")
    }
  }),
)

it.instance("loop continues when finish is unknown", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Pinned",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.push(reply())
    yield* llm.text("second")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(yield* llm.calls).toBe(2)
    expect(result.info.role).toBe("assistant")
    if (result.info.role === "assistant") {
      expect(result.parts.some((part) => part.type === "text" && part.text === "second")).toBe(true)
      expect(result.info.finish).toBe("stop")
    }
  }),
)

it.instance("glob tool keeps instance context during prompt runs", () =>
  Effect.gen(function* () {
    const { dir, llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Glob context",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    const file = path.join(dir, "probe.txt")
    yield* writeText(file, "probe")

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "find text files" }],
    })
    yield* llm.tool("glob", { pattern: "**/*.txt" })
    yield* llm.text("done")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(result.info.role).toBe("assistant")

    const msgs = yield* MessageV2.filterCompactedEffect(session.id)
    const tool = msgs
      .flatMap((msg) => msg.parts)
      .find(
        (part): part is CompletedToolPart =>
          part.type === "tool" && part.tool === "glob" && part.state.status === "completed",
      )
    if (!tool) return

    expect(tool.state.output).toContain(file)
    expect(tool.state.output).not.toContain("No context found for instance")
    expect(result.parts.some((part) => part.type === "text" && part.text === "done")).toBe(true)
  }),
)

it.instance("loop continues when finish is stop but assistant has tool parts", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Pinned",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.push(reply().tool("first", { value: "first" }).stop())
    yield* llm.text("second")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(yield* llm.calls).toBe(2)
    expect(result.info.role).toBe("assistant")
    if (result.info.role === "assistant") {
      expect(result.parts.some((part) => part.type === "text" && part.text === "second")).toBe(true)
      expect(result.info.finish).toBe("stop")
    }
  }),
)

it.instance("failed subtask preserves metadata on error tool state", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig((url) => ({
      ...providerCfg(url),
      agent: {
        general: {
          model: "test/missing-model",
        },
      },
    }))
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* llm.tool("task", {
      description: "inspect bug",
      prompt: "look into the cache key path",
      subagent_type: "general",
    })
    yield* llm.text("done")
    const msg = yield* user(chat.id, "hello")
    yield* addSubtask(chat.id, msg.id)

    const result = yield* prompt.loop({ sessionID: chat.id })
    expect(result.info.role).toBe("assistant")
    expect(yield* llm.calls).toBe(2)

    const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
    const taskMsg = msgs.find((item) => item.info.role === "assistant" && item.info.agent === "general")
    expect(taskMsg?.info.role).toBe("assistant")
    if (!taskMsg || taskMsg.info.role !== "assistant") return

    const tool = errorTool(taskMsg.parts)
    if (!tool) return

    expect(tool.state.error).toContain("Tool execution failed")
    expect(tool.state.metadata).toBeDefined()
    expect(tool.state.metadata?.sessionId).toBeDefined()
    expect(tool.state.metadata?.model).toEqual({
      providerID: ProviderV2.ID.make("test"),
      modelID: ModelV2.ID.make("missing-model"),
    })
  }),
)

it.instance("subtask child inherits parent session external_directory allow", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({
      title: "Parent",
      permission: [{ permission: "external_directory", pattern: "/tmp/allowed/*", action: "allow" }],
    })
    yield* llm.text("done")
    const msg = yield* user(chat.id, "hello")
    yield* addSubtask(chat.id, msg.id)

    yield* prompt.loop({ sessionID: chat.id })

    const kids = yield* sessions.children(chat.id)
    expect(kids).toHaveLength(1)
    const child = kids[0]!
    const rules = child.permission ?? []
    expect(rules).toEqual(
      expect.arrayContaining([{ permission: "external_directory", pattern: "/tmp/allowed/*", action: "allow" }]),
    )
    expect(Permission.evaluate("external_directory", "/tmp/allowed/file", rules).action).toBe("allow")
    expect(Permission.evaluate("task", "anything", rules).action).toBe("deny")
  }),
)

noLLMServer.instance("prompt tools replace previous prompt tool rules", () =>
  Effect.gen(function* () {
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({ title: "Prompt tools" })

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      tools: { bash: false },
      parts: [{ type: "text", text: "first" }],
    })
    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      tools: { read: true },
      parts: [{ type: "text", text: "second" }],
    })

    const reloaded = yield* sessions.get(session.id)
    expect(reloaded.permission).toEqual([{ permission: "read", pattern: "*", action: "allow" }])
    expect(Permission.evaluate("bash", "anything", reloaded.permission ?? []).action).toBe("ask")
  }),
)

it.instance(
  "running subtask preserves metadata after tool-call transition",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      yield* llm.hang
      const msg = yield* user(chat.id, "hello")
      yield* addSubtask(chat.id, msg.id)

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)

      const tool = yield* pollWithTimeout(
        Effect.gen(function* () {
          const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
          const taskMsg = msgs.find((item) => item.info.role === "assistant" && item.info.agent === "general")
          const tool = taskMsg?.parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
          if (tool?.state.status === "running" && tool.state.metadata?.sessionId) return tool
        }),
        "timed out waiting for running subtask metadata",
      )

      if (tool.state.status !== "running") return
      expect(typeof tool.state.metadata?.sessionId).toBe("string")
      expect(tool.state.title).toBeDefined()
      expect(tool.state.metadata?.model).toBeDefined()

      yield* prompt.cancel(chat.id)
      yield* Fiber.await(fiber)
    }),
  5_000,
)

it.instance(
  "running task tool preserves metadata after tool-call transition",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* llm.tool("task", {
        description: "inspect bug",
        prompt: "look into the cache key path",
        subagent_type: "general",
      })
      yield* llm.hang
      yield* user(chat.id, "hello")

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)

      const tool = yield* pollWithTimeout(
        Effect.gen(function* () {
          const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
          const assistant = msgs.findLast((item) => item.info.role === "assistant" && item.info.agent === "build")
          const tool = assistant?.parts.find(
            (part): part is SessionV1.ToolPart => part.type === "tool" && part.tool === "task",
          )
          if (tool?.state.status === "running" && tool.state.metadata?.sessionId) return tool
        }),
        "timed out waiting for running task metadata",
      )

      if (tool.state.status !== "running") return
      expect(typeof tool.state.metadata?.sessionId).toBe("string")
      expect(tool.state.title).toBe("inspect bug")
      expect(tool.state.metadata?.model).toBeDefined()

      yield* prompt.cancel(chat.id)
      yield* Fiber.await(fiber)
    }),
  10_000,
)

it.instance(
  "loop sets status to busy then idle",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const status = yield* SessionStatus.Service

      yield* llm.hang

      const chat = yield* sessions.create({})
      yield* user(chat.id, "hi")

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* llm.wait(1)
      expect((yield* status.get(chat.id)).type).toBe("busy")
      yield* prompt.cancel(chat.id)
      yield* Fiber.await(fiber)
      expect((yield* status.get(chat.id)).type).toBe("idle")
    }),
  3_000,
)

// Cancel semantics

it.instance("cancel interrupts loop and resolves with an assistant message", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* seed(chat.id)

    yield* llm.hang

    yield* user(chat.id, "more")

    const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
    yield* llm.wait(1)
    yield* waitForBusy(chat.id)
    yield* prompt.cancel(chat.id)
    const exit = yield* Fiber.await(fiber)
    expect(Exit.isSuccess(exit)).toBe(true)
    if (Exit.isSuccess(exit)) {
      expect(exit.value.info.role).toBe("assistant")
    }
  }),
)

it.instance("cancel records MessageAbortedError on interrupted process", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* llm.hang
    yield* user(chat.id, "hello")

    const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
    yield* llm.wait(1)
    yield* waitForBusy(chat.id)
    yield* prompt.cancel(chat.id)
    const exit = yield* Fiber.await(fiber)
    expect(Exit.isSuccess(exit)).toBe(true)
    if (Exit.isSuccess(exit)) {
      const info = exit.value.info
      if (info.role === "assistant") {
        expect(info.error?.name).toBe("MessageAbortedError")
      }
    }
  }),
)

raceNoLLMServer.instance(
  "finalizes assistant when cancelled before processor creation completes",
  () =>
    Effect.gen(function* () {
      processorCreateStarted.length = 0
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          processorCreateStarted.length = 0
        }),
      )

      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Processor creation race" })

      yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
        parts: [{ type: "text", text: "first" }],
      })

      const firstCreate = defer<void>()
      processorCreateStarted.push(firstCreate.resolve)
      const first = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.promise(() => firstCreate.promise)

      yield* prompt.cancel(chat.id)
      const firstExit = yield* Fiber.await(first)
      expect(Exit.isSuccess(firstExit)).toBe(true)

      let messages = yield* sessions.messages({ sessionID: chat.id })
      const firstInterrupted = messages.at(-1)
      expect(firstInterrupted?.info.role).toBe("assistant")
      expect(firstInterrupted?.parts).toHaveLength(0)
      if (firstInterrupted?.info.role === "assistant") {
        expect(firstInterrupted.info.finish).toBeUndefined()
        expect(firstInterrupted.info.time.completed).toBeNumber()
        expect(firstInterrupted.info.error?.name).toBe("MessageAbortedError")
      }

      yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
        parts: [{ type: "text", text: "second" }],
      })

      const secondCreate = defer<void>()
      processorCreateStarted.push(secondCreate.resolve)
      const second = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.promise(() => secondCreate.promise)

      yield* prompt.cancel(chat.id)
      const secondExit = yield* Fiber.await(second)
      expect(Exit.isSuccess(secondExit)).toBe(true)

      messages = yield* sessions.messages({ sessionID: chat.id })
      const poisonMessages = messages.filter(
        (message) =>
          message.info.role === "assistant" &&
          message.parts.length === 0 &&
          !message.info.finish &&
          !message.info.time.completed &&
          !message.info.error,
      )
      expect(poisonMessages).toHaveLength(0)

      const interruptedMessages = messages.filter(
        (message) =>
          message.info.role === "assistant" &&
          message.parts.length === 0 &&
          message.info.time.completed &&
          message.info.error?.name === "MessageAbortedError",
      )
      expect(interruptedMessages).toHaveLength(2)

      const lastUser = messages.at(-2)
      const lastAssistant = messages.at(-1)
      expect(lastUser?.info.role).toBe("user")
      expect(lastAssistant?.info.role).toBe("assistant")
      if (lastUser?.info.role === "user" && lastAssistant?.info.role === "assistant") {
        expect(lastAssistant.info.parentID).toBe(lastUser?.info.id)
      }
    }),
  { config: cfg },
  3_000,
)

noLLMServer.instance(
  "cancel finalizes subtask tool state",
  () =>
    Effect.gen(function* () {
      const ready = yield* Deferred.make<void>()
      const aborted = yield* Deferred.make<void>()
      const registry = yield* ToolRegistry.Service
      const { task } = yield* registry.named()
      const original = task.execute
      task.execute = (_args, ctx) =>
        Effect.callback<never>((_resume) => {
          ctx.abort.addEventListener("abort", () => succeedVoid(aborted), { once: true })
          if (ctx.abort.aborted) succeedVoid(aborted)
          succeedVoid(ready)
          return Effect.sync(() => succeedVoid(aborted))
        })
      yield* Effect.addFinalizer(() => Effect.sync(() => void (task.execute = original)))

      const { prompt, chat } = yield* boot()
      const msg = yield* user(chat.id, "hello")
      yield* addSubtask(chat.id, msg.id)

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(ready), "timed out waiting for task tool to start", "10 seconds")
      yield* prompt.cancel(chat.id)

      const exit = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(exit)).toBe(true)
      yield* awaitWithTimeout(Deferred.await(aborted), "timed out waiting for task tool abort", "10 seconds")

      const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
      const taskMsg = msgs.find((item) => item.info.role === "assistant" && item.info.agent === "general")
      expect(taskMsg?.info.role).toBe("assistant")
      if (!taskMsg || taskMsg.info.role !== "assistant") return

      const tool = toolPart(taskMsg.parts)
      expect(tool?.type).toBe("tool")
      if (!tool) return

      expect(tool.state.status).not.toBe("running")
      expect(taskMsg.info.time.completed).toBeDefined()
      expect(taskMsg.info.finish).toBeDefined()
    }),
  { config: cfg },
  30_000,
)

it.instance(
  "cancel propagates from slash command subtask to child session",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const status = yield* SessionStatus.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      yield* llm.hang
      const msg = yield* user(chat.id, "hello")
      yield* addSubtask(chat.id, msg.id)

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* llm.wait(1)

      const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
      const taskMsg = msgs.find((item) => item.info.role === "assistant" && item.info.agent === "general")
      const tool = taskMsg ? toolPart(taskMsg.parts) : undefined
      const sessionID = tool?.state.status === "running" ? tool.state.metadata?.sessionId : undefined
      expect(typeof sessionID).toBe("string")
      if (typeof sessionID !== "string") throw new Error("missing child session id")
      const childID = SessionID.make(sessionID)
      expect((yield* status.get(childID)).type).toBe("busy")

      yield* prompt.cancel(chat.id)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(exit)).toBe(true)

      expect((yield* status.get(chat.id)).type).toBe("idle")
      expect((yield* status.get(childID)).type).toBe("idle")
    }),
  10_000,
)

it.instance(
  "cancel with queued callers resolves all cleanly",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      yield* llm.hang
      yield* user(chat.id, "hello")

      const a = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* llm.wait(1)
      const b = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.sleep(50)

      yield* prompt.cancel(chat.id)
      const [exitA, exitB] = yield* Effect.all([Fiber.await(a), Fiber.await(b)])
      expect(Exit.isSuccess(exitA)).toBe(true)
      expect(Exit.isSuccess(exitB)).toBe(true)
      if (Exit.isSuccess(exitA) && Exit.isSuccess(exitB)) {
        expect(exitA.value.info.id).toBe(exitB.value.info.id)
      }
    }),
  { git: true },
  10_000,
)

// Queue semantics

noLLMServer.instance("concurrent loop callers get same result", () =>
  Effect.gen(function* () {
    const { prompt, run, chat } = yield* boot()
    yield* seed(chat.id, { finish: "stop" })

    const [a, b] = yield* Effect.all([prompt.loop({ sessionID: chat.id }), prompt.loop({ sessionID: chat.id })], {
      concurrency: "unbounded",
    })

    expect(a.info.id).toBe(b.info.id)
    expect(a.info.role).toBe("assistant")
    yield* run.assertNotBusy(chat.id)
  }),
)

it.instance("concurrent loop callers all receive same error result", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })

    yield* llm.fail("boom")
    yield* user(chat.id, "hello")

    const [a, b] = yield* Effect.all([prompt.loop({ sessionID: chat.id }), prompt.loop({ sessionID: chat.id })], {
      concurrency: "unbounded",
    })
    expect(a.info.id).toBe(b.info.id)
    expect(a.info.role).toBe("assistant")
  }),
)

it.instance("prompt submitted during an active run is included in the next LLM input", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const gate = yield* Deferred.make<void>()
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })

    yield* llm.hold("first", deferredAsPromise(gate))
    yield* llm.text("second")

    const a = yield* prompt
      .prompt({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "first" }],
      })
      .pipe(Effect.forkChild)

    yield* llm.wait(1)
    yield* waitForBusy(chat.id)

    const id = MessageID.ascending()
    const b = yield* prompt
      .prompt({
        sessionID: chat.id,
        messageID: id,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "second" }],
      })
      .pipe(Effect.forkChild)

    yield* pollWithTimeout(
      sessions
        .messages({ sessionID: chat.id })
        .pipe(
          Effect.map((msgs) => (msgs.some((msg) => msg.info.role === "user" && msg.info.id === id) ? true : undefined)),
        ),
      "timed out waiting for second prompt to save",
    )

    yield* Deferred.succeed(gate, void 0)

    const [ea, eb] = yield* Effect.all([Fiber.await(a), Fiber.await(b)])
    expect(Exit.isSuccess(ea)).toBe(true)
    expect(Exit.isSuccess(eb)).toBe(true)
    expect(yield* llm.calls).toBe(2)

    const msgs = yield* sessions.messages({ sessionID: chat.id })
    const assistants = msgs.filter((msg) => msg.info.role === "assistant")
    expect(assistants).toHaveLength(2)
    const last = assistants.at(-1)
    if (!last || last.info.role !== "assistant") throw new Error("expected second assistant")
    expect(last.info.parentID).toBe(id)
    expect(last.parts.some((part) => part.type === "text" && part.text === "second")).toBe(true)

    const inputs = yield* llm.inputs
    expect(inputs).toHaveLength(2)
    const messages = inputs.at(-1)?.messages
    if (!Array.isArray(messages)) throw new Error("expected LLM messages")
    expect(messages.at(-1)).toEqual({ role: "user", content: "second" })
  }),
)

background.instance(
  "recursive background returns publish initial and descendant-informed replies once",
  () =>
    Effect.gen(function* () {
      const rootMarker = "recursive-background-root-marker"
      const aMarker = "recursive-background-a-marker"
      const bMarker = "recursive-background-b-marker"
      const siblingMarker = "recursive-background-sibling-marker"
      const bResult = "B_RESULT_CANONICAL"

      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        subagent_depth: 3,
        permission: {
          task: {
            a: "allow",
            sibling: "allow",
          },
        },
        agent: {
          build: {
            model: "test/test-model",
            prompt: rootMarker,
          },
          a: {
            mode: "subagent",
            model: "test/test-model",
            prompt: aMarker,
            permission: { task: { b: "allow" } },
          },
          b: {
            mode: "subagent",
            model: "test/test-model",
            prompt: bMarker,
          },
          sibling: {
            mode: "subagent",
            model: "test/test-model",
            prompt: siblingMarker,
          },
        },
      }))
      const jobs = yield* BackgroundJob.Service
      const delegation = yield* DelegationStore.Service
      const events = yield* EventV2Bridge.Service
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const root = yield* sessions.create({
        title: "Recursive background chain",
        agent: "build",
        model: { providerID: ref.providerID, id: ref.modelID },
      })
      const aInitialGate = Promise.withResolvers<void>()
      const bGate = Promise.withResolvers<void>()
      const siblingGate = Promise.withResolvers<void>()
      const rootIntermediateGate = Promise.withResolvers<void>()
      const rootInitialGate = Promise.withResolvers<void>()
      const aAfterGate = Promise.withResolvers<void>()
      const rootLaterGate = Promise.withResolvers<void>()

      const cleanup = Effect.gen(function* () {
        yield* prompt.cancel(root.id).pipe(Effect.ignore)
        aInitialGate.resolve()
        bGate.resolve()
        siblingGate.resolve()
        rootIntermediateGate.resolve()
        rootInitialGate.resolve()
        aAfterGate.resolve()
        rootLaterGate.resolve()
      })

      yield* Effect.gen(function* () {
        yield* llm.pushMatch(
          hasSystemMarker(rootMarker),
          reply()
            .tool("task", {
              description: "launch agent a",
              prompt: "Investigate A",
              subagent_type: "a",
              background: true,
            })
            .item(),
          reply()
            .tool("task", {
              description: "launch sibling agent",
              prompt: "Investigate sibling",
              subagent_type: "sibling",
              background: true,
            })
            .item(),
          reply().text("ROOT_INTERMEDIATE").stop().wait(rootIntermediateGate.promise).item(),
          reply().text("ROOT_AFTER_INITIAL").stop().wait(rootInitialGate.promise).item(),
          reply().text("ROOT_AFTER_LATER").stop().wait(rootLaterGate.promise).item(),
        )
        yield* llm.pushMatch(
          hasSystemMarker(aMarker),
          reply()
            .tool("task", {
              description: "launch agent b",
              prompt: "Investigate B",
              subagent_type: "b",
              background: true,
            })
            .item(),
          reply().text("A_INITIAL").stop().wait(aInitialGate.promise).item(),
          reply().text("A_AFTER_B").stop().wait(aAfterGate.promise).item(),
        )
        yield* llm.pushMatch(hasSystemMarker(bMarker), reply().text(bResult).stop().wait(bGate.promise).item())
        yield* llm.pushMatch(
          hasSystemMarker(siblingMarker),
          reply().text("SIBLING_RESULT").stop().wait(siblingGate.promise).item(),
        )

        const rootRun = yield* prompt
          .prompt({
            sessionID: root.id,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "Begin the recursive background chain." }],
          })
          .pipe(Effect.forkChild)

        yield* waitForSystem(llm, rootMarker, 3)
        yield* waitForSystem(llm, aMarker, 2)
        yield* waitForSystem(llm, bMarker)
        yield* waitForSystem(llm, siblingMarker)

        const rootChildren = yield* sessions.children(root.id)
        const a = rootChildren.find((child) => child.agent === "a")
        const sibling = rootChildren.find((child) => child.agent === "sibling")
        if (a === undefined || sibling === undefined) throw new Error("expected A and sibling child sessions")
        const aChildren = yield* sessions.children(a.id)
        const b = aChildren.find((child) => child.agent === "b")
        if (b === undefined) throw new Error("expected B child session")

        const aGeneration = yield* delegation.active(a.id)
        const bGeneration = yield* delegation.active(b.id)
        const siblingGeneration = yield* delegation.active(sibling.id)
        if (aGeneration === undefined || bGeneration === undefined || siblingGeneration === undefined) {
          throw new Error("expected active A, B, and sibling generations")
        }
        expect(aGeneration.parentID).toBe(root.id)
        expect(aGeneration.parentGenerationID).toBeUndefined()
        expect(bGeneration.parentID).toBe(a.id)
        expect(bGeneration.parentGenerationID).toBe(aGeneration.id)
        expect(siblingGeneration.parentID).toBe(root.id)
        expect(siblingGeneration.parentGenerationID).toBeUndefined()

        aInitialGate.resolve()
        const aWaited = yield* jobs.wait({ id: a.id, timeout: 10_000 })
        expect(aWaited.timedOut).toBe(false)
        expect(aWaited.info?.status).toBe("completed")
        expect((yield* jobs.get(b.id))?.status).toBe("running")
        expect((yield* jobs.get(sibling.id))?.status).toBe("running")
        expect((yield* delegation.get(aGeneration.id))?.state).toBe("active")

        rootIntermediateGate.resolve()
        const rootInitialHit = yield* waitForSystem(llm, rootMarker, 4)
        expect(messageContents(rootInitialHit)).toContain(taskResult(a.id, "A_INITIAL"))

        const aSourcesAfterInitial = yield* delegation.sources(a.id)
        const initialSource = aSourcesAfterInitial.find(
          (source) =>
            source.state === "finalized" && source.source.kind === "assistant" && source.payload === "A_INITIAL",
        )
        if (initialSource === undefined) throw new Error("expected finalized A_INITIAL source")
        const rootInitialResolution = (yield* delegation.incoming(root.id)).find(
          (resolution) => resolution.source.id === initialSource.source.id,
        )
        if (rootInitialResolution === undefined) throw new Error("expected A_INITIAL resolution addressed to root")
        expect(rootInitialResolution.parentID).toBe(root.id)
        expect(rootInitialResolution.childID).toBe(a.id)
        expect(rootInitialResolution.status).toBe("consumed")
        expect((yield* delegation.getResolution(rootInitialResolution.id))?.status).toBe("consumed")

        rootInitialGate.resolve()
        bGate.resolve()
        const bWaited = yield* jobs.wait({ id: b.id, timeout: 10_000 })
        expect(bWaited.timedOut).toBe(false)
        expect(bWaited.info?.status).toBe("completed")

        const aAfterHit = yield* waitForSystem(llm, aMarker, 3)
        expect(messageContents(aAfterHit)).toContain(taskResult(b.id, bResult))
        expect(messageContents(aAfterHit).filter((content) => content === taskResult(b.id, bResult))).toHaveLength(1)

        const bSource = (yield* delegation.sources(b.id)).find(
          (source) => source.state === "finalized" && source.source.kind === "assistant" && source.payload === bResult,
        )
        if (bSource === undefined) throw new Error("expected finalized B source")
        const bResolution = (yield* delegation.incoming(a.id)).find(
          (resolution) => resolution.source.id === bSource.source.id,
        )
        if (bResolution === undefined) throw new Error("expected B resolution addressed to A")
        expect(bResolution.parentID).toBe(a.id)
        expect(bResolution.childID).toBe(b.id)

        aAfterGate.resolve()
        const rootLaterHit = yield* waitForSystem(llm, rootMarker, 5)
        expect(messageContents(rootLaterHit)).toContain(taskResult(a.id, "A_AFTER_B"))

        const aMessages = yield* sessions.messages({ sessionID: a.id })
        const aUsers = aMessages.filter((message) => message.info.role === "user")
        const aPrompt = aUsers.find((message) =>
          message.parts.some((part) => part.type === "text" && part.text === "Investigate A"),
        )
        const bIncoming = aUsers.filter((message) =>
          message.parts.some((part) => part.type === "text" && part.text === taskResult(b.id, bResult)),
        )
        const aInitialAssistant = aMessages.find(
          (message) =>
            message.info.role === "assistant" &&
            message.parts.some((part) => part.type === "text" && part.text === "A_INITIAL"),
        )
        const aAfterAssistant = aMessages.find(
          (message) =>
            message.info.role === "assistant" &&
            message.parts.some((part) => part.type === "text" && part.text === "A_AFTER_B"),
        )
        expect(aPrompt).toBeDefined()
        expect(bIncoming).toHaveLength(1)
        expect(aInitialAssistant).toBeDefined()
        expect(aAfterAssistant).toBeDefined()
        if (
          aPrompt === undefined ||
          bIncoming.length !== 1 ||
          aInitialAssistant === undefined ||
          aAfterAssistant === undefined ||
          aInitialAssistant.info.role !== "assistant" ||
          aAfterAssistant.info.role !== "assistant"
        )
          throw new Error("expected A prompt, B result, and A assistant messages")
        expect(aInitialAssistant.info.parentID).toBe(aPrompt.info.id)
        expect(aAfterAssistant.info.parentID).toBe(bIncoming[0]?.info.id)

        const bMessages = yield* sessions.messages({ sessionID: b.id })
        const bPrompt = bMessages.find(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text === "Investigate B"),
        )
        const bAssistant = bMessages.find(
          (message) =>
            message.info.role === "assistant" &&
            message.parts.some((part) => part.type === "text" && part.text === bResult),
        )
        expect(bPrompt).toBeDefined()
        expect(bAssistant).toBeDefined()
        if (bPrompt === undefined || bAssistant === undefined || bAssistant.info.role !== "assistant") {
          throw new Error("expected B prompt and assistant messages")
        }
        expect(bAssistant.info.parentID).toBe(bPrompt.info.id)

        const rootMessagesBeforeLater = yield* sessions.messages({ sessionID: root.id })
        const rootAResults = rootMessagesBeforeLater.filter(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text.includes(`<task_id>${a.id}</task_id>`)),
        )
        expect(
          rootAResults.filter((message) =>
            message.parts.some((part) => part.type === "text" && part.text.includes("A_INITIAL")),
          ),
        ).toHaveLength(1)
        expect(
          rootAResults.filter((message) =>
            message.parts.some((part) => part.type === "text" && part.text.includes("A_AFTER_B")),
          ),
        ).toHaveLength(1)
        expect(
          rootMessagesBeforeLater.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text.includes(`<task_id>${b.id}</task_id>`)),
          ),
        ).toHaveLength(0)

        const rootInitialAssistant = rootMessagesBeforeLater.find(
          (message) =>
            message.info.role === "assistant" &&
            message.parts.some((part) => part.type === "text" && part.text === "ROOT_AFTER_INITIAL"),
        )
        expect(rootInitialAssistant).toBeDefined()
        if (rootInitialAssistant === undefined || rootInitialAssistant.info.role !== "assistant") {
          throw new Error("expected root initial assistant message")
        }
        const rootInitialResult = rootAResults.find((message) =>
          message.parts.some((part) => part.type === "text" && part.text.includes("A_INITIAL")),
        )
        const rootLaterResult = rootAResults.find((message) =>
          message.parts.some((part) => part.type === "text" && part.text.includes("A_AFTER_B")),
        )
        if (rootInitialResult === undefined || rootLaterResult === undefined) {
          throw new Error("expected root A result messages")
        }
        expect(rootInitialAssistant.info.parentID).toBe(rootInitialResult.info.id)

        const finalizedASources = (yield* delegation.sources(a.id)).filter(
          (source) => source.state === "finalized" && source.source.kind === "assistant",
        )
        expect(finalizedASources).toHaveLength(2)
        expect(finalizedASources.map((source) => source.payload)).toEqual(["A_INITIAL", "A_AFTER_B"])
        expect(new Set(finalizedASources.map((source) => source.id)).size).toBe(2)
        expect(finalizedASources.map((source) => source.source.id)).toEqual([
          aInitialAssistant.info.id,
          aAfterAssistant.info.id,
        ])
        expect(finalizedASources.every((source) => source.generationID === aGeneration.id)).toBe(true)
        const laterSource = finalizedASources[1]
        if (laterSource === undefined) throw new Error("expected A_AFTER_B source")

        const rootLaterResolution = (yield* delegation.incoming(root.id)).find(
          (resolution) => resolution.source.id === laterSource.source.id,
        )
        if (rootLaterResolution === undefined) throw new Error("expected later A resolution addressed to root")
        expect(rootLaterResolution.parentID).toBe(root.id)
        expect(rootLaterResolution.childID).toBe(a.id)
        expect(rootLaterResolution.status).toBe("consumed")

        expect(laterSource.consumed).toContain(bResolution.id)
        expect((yield* delegation.getResolution(bResolution.id))?.status).toBe("resolved")
        expect((yield* delegation.getResolution(bResolution.id))?.resolvedSourceID).toBe(laterSource.id)

        const cutoffJson = yield* Schema.decodeUnknownEffect(Schema.UnknownFromJsonString)(
          laterSource.historyCutoff,
        ).pipe(Effect.orDie)
        const cutoff = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ messages: Schema.Array(Schema.String), work: Schema.Array(Schema.String) }),
        )(cutoffJson).pipe(Effect.orDie)
        expect(cutoff.messages).toContain(String(bResolution.messageID))

        const rootResolutions = yield* delegation.incoming(root.id)
        expect(rootResolutions.filter((resolution) => resolution.childID === a.id)).toHaveLength(1)
        expect(rootResolutions[0]?.id).toBe(rootLaterResolution.id)
        expect(rootResolutions.some((resolution) => resolution.childID === b.id)).toBe(false)

        const hits = yield* llm.hits
        const rootHits = hits.filter(hasSystemMarker(rootMarker))
        const aHits = hits.filter(hasSystemMarker(aMarker))
        const bHits = hits.filter(hasSystemMarker(bMarker))
        const siblingHits = hits.filter(hasSystemMarker(siblingMarker))
        expect(hits).toHaveLength(10)
        expect(rootHits).toHaveLength(5)
        expect(aHits).toHaveLength(3)
        expect(bHits).toHaveLength(1)
        expect(siblingHits).toHaveLength(1)
        expect(hits.every((hit) => [rootMarker, aMarker, bMarker, siblingMarker].some(hasSystemMarker))).toBe(true)
        expect(yield* llm.pending).toBe(0)
        expect(yield* llm.misses).toHaveLength(0)
        expect(hits.indexOf(aHits[0]!)).toBeLessThan(hits.indexOf(aHits[1]!))
        expect(hits.indexOf(aHits[1]!)).toBeLessThan(hits.indexOf(aHits[2]!))
        expect(hits.indexOf(aAfterHit)).toBeLessThan(hits.indexOf(rootLaterHit))
        expect((yield* jobs.get(sibling.id))?.status).toBe("running")

        const rootLaterPending = rootMessagesBeforeLater.findLast(
          (message) => message.info.role === "assistant" && message.info.parentID === rootLaterResult.info.id,
        )
        if (rootLaterPending === undefined || rootLaterPending.info.role !== "assistant") {
          throw new Error("expected pending root later assistant message")
        }
        const rootLaterFinished = yield* listenForPartText(
          events,
          root.id,
          "ROOT_AFTER_LATER",
          rootLaterPending.info.id,
        )
        rootLaterGate.resolve()
        yield* awaitWithTimeout(
          Deferred.await(rootLaterFinished),
          "timed out waiting for root later assistant message",
          "10 seconds",
        )
        const rootMessages = yield* sessions.messages({ sessionID: root.id })
        const rootLaterAssistant = rootMessages.find(
          (message) =>
            message.info.role === "assistant" &&
            message.parts.some((part) => part.type === "text" && part.text === "ROOT_AFTER_LATER"),
        )
        expect(rootLaterAssistant).toBeDefined()
        if (rootLaterAssistant === undefined || rootLaterAssistant.info.role !== "assistant") {
          throw new Error("expected root later assistant message")
        }
        expect(rootLaterAssistant.info.parentID).toBe(rootLaterResult.info.id)
        expect((yield* delegation.getResolution(rootInitialResolution.id))?.status).toBe("resolved")
        expect((yield* delegation.getResolution(rootLaterResolution.id))?.status).toBe("consumed")
        const rootRunExit = yield* Fiber.await(rootRun)
        expect(Exit.isSuccess(rootRunExit)).toBe(true)
      }).pipe(Effect.ensuring(cleanup))
    }),
  30_000,
)

background.instance(
  "recursive background return waits for foreground child settlement",
  () =>
    Effect.gen(function* () {
      const rootMarker = "recursive-foreground-root-marker"
      const aMarker = "recursive-foreground-a-marker"
      const bMarker = "recursive-foreground-b-marker"
      const fMarker = "recursive-foreground-f-marker"
      const bResult = "B_RESULT_BEFORE_FOREGROUND"
      const fResult = "F_RESULT_FOREGROUND"
      const aResult = "A_RESULT_AFTER_FOREGROUND_AND_B"

      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        subagent_depth: 3,
        permission: { task: { a: "allow" } },
        agent: {
          build: {
            model: "test/test-model",
            prompt: rootMarker,
          },
          a: {
            mode: "subagent",
            model: "test/test-model",
            prompt: aMarker,
            permission: { task: { b: "allow", f: "allow" } },
          },
          b: {
            mode: "subagent",
            model: "test/test-model",
            prompt: bMarker,
          },
          f: {
            mode: "subagent",
            model: "test/test-model",
            prompt: fMarker,
          },
        },
      }))
      const jobs = yield* BackgroundJob.Service
      const delegation = yield* DelegationStore.Service
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const root = yield* sessions.create({
        title: "Foreground child settlement",
        agent: "build",
        model: { providerID: ref.providerID, id: ref.modelID },
      })
      const bGate = Promise.withResolvers<void>()
      const fGate = Promise.withResolvers<void>()
      const rootBeforeGate = Promise.withResolvers<void>()
      const rootAfterGate = Promise.withResolvers<void>()

      const cleanup = Effect.gen(function* () {
        yield* prompt.cancel(root.id).pipe(Effect.ignore)
        bGate.resolve()
        fGate.resolve()
        rootBeforeGate.resolve()
        rootAfterGate.resolve()
      })

      yield* Effect.gen(function* () {
        yield* llm.pushMatch(
          hasSystemMarker(rootMarker),
          reply()
            .tool("task", {
              description: "launch agent a",
              prompt: "Investigate A",
              subagent_type: "a",
              background: true,
            })
            .item(),
          reply().text("ROOT_WAITING_FOR_A").stop().wait(rootBeforeGate.promise).item(),
          reply().text("ROOT_AFTER_A").stop().wait(rootAfterGate.promise).item(),
        )
        yield* llm.pushMatch(
          hasSystemMarker(aMarker),
          reply()
            .tool("task", {
              description: "launch agent b",
              prompt: "Investigate B",
              subagent_type: "b",
              background: true,
            })
            .item(),
          reply()
            .tool("task", {
              description: "launch foreground agent",
              prompt: "Investigate F",
              subagent_type: "f",
            })
            .item(),
          reply().text(aResult).stop().item(),
        )
        yield* llm.pushMatch(hasSystemMarker(bMarker), reply().text(bResult).stop().wait(bGate.promise).item())
        yield* llm.pushMatch(hasSystemMarker(fMarker), reply().text(fResult).stop().wait(fGate.promise).item())

        const rootRun = yield* prompt
          .prompt({
            sessionID: root.id,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "Begin the foreground settlement chain." }],
          })
          .pipe(Effect.forkChild)

        const rootWaitingHit = yield* waitForSystem(llm, rootMarker, 2)
        yield* waitForSystem(llm, aMarker, 2)
        yield* waitForSystem(llm, bMarker)
        yield* waitForSystem(llm, fMarker)

        const rootChildren = yield* sessions.children(root.id)
        const a = rootChildren.find((child) => child.agent === "a")
        if (a === undefined) throw new Error("expected A child session")
        const aChildren = yield* sessions.children(a.id)
        const b = aChildren.find((child) => child.agent === "b")
        const f = aChildren.find((child) => child.agent === "f")
        if (b === undefined || f === undefined) throw new Error("expected B and F child sessions")

        const fGeneration = yield* delegation.active(f.id)
        if (fGeneration === undefined) throw new Error("expected active foreground generation")
        expect(fGeneration.parentID).toBe(a.id)
        expect(fGeneration.mode).toBe("foreground")
        expect((yield* jobs.get(f.id))?.status).toBe("running")
        expect(messageContents(rootWaitingHit).filter((content) => content === taskResult(a.id, aResult))).toHaveLength(
          0,
        )

        bGate.resolve()
        const bWaited = yield* jobs.wait({ id: b.id, timeout: 10_000 })
        expect(bWaited.timedOut).toBe(false)
        expect(bWaited.info?.status).toBe("completed")
        const bSource = yield* waitForFinalizedAssistantSource(delegation, b.id, bResult)
        const bResolution = yield* waitForAdmittedResolution(delegation, a.id, bSource.source.id)

        const aReserved = (yield* delegation.sources(a.id)).find(
          (source) => source.state === "reserved" && source.source.kind === "assistant",
        )
        if (aReserved === undefined) throw new Error("expected reserved A provider source")
        expect(aReserved.consumed).not.toContain(bResolution.id)
        expect((yield* llm.hits).filter(hasSystemMarker(aMarker))).toHaveLength(2)
        expect((yield* jobs.get(f.id))?.status).toBe("running")
        expect((yield* delegation.incoming(a.id)).filter((resolution) => resolution.childID === f.id)).toHaveLength(0)

        fGate.resolve()
        const fWaited = yield* jobs.wait({ id: f.id, timeout: 10_000 })
        expect(fWaited.timedOut).toBe(false)
        expect(fWaited.info?.status).toBe("completed")

        const aAfterForegroundHit = yield* waitForSystem(llm, aMarker, 3)
        const aAfterForegroundContents = messageContents(aAfterForegroundHit)
        expect(
          aAfterForegroundContents.filter((content) => content === completedTaskResult(f.id, fResult)),
        ).toHaveLength(1)
        expect(aAfterForegroundContents.filter((content) => content === taskResult(b.id, bResult))).toHaveLength(1)
        const aWaited = yield* jobs.wait({ id: a.id, timeout: 10_000 })
        expect(aWaited.timedOut).toBe(false)
        expect(aWaited.info?.status).toBe("completed")

        const aMessages = yield* sessions.messages({ sessionID: a.id })
        const fParts = aMessages
          .flatMap((message) => message.parts)
          .filter(
            (part): part is SessionV1.ToolPart =>
              part.type === "tool" && part.tool === "task" && part.state.input.prompt === "Investigate F",
          )
        expect(fParts).toHaveLength(1)
        const fPart = fParts[0]
        if (fPart === undefined) throw new Error("expected originating F task part")
        expect(fPart.state.status).toBe("completed")
        if (fPart.state.status !== "completed") throw new Error("expected completed F task part")
        expect(fPart.state.output).toBe(completedTaskResult(f.id, fResult))

        const aFinalSources = (yield* delegation.sources(a.id)).filter(
          (source) => source.state === "finalized" && source.source.kind === "assistant" && source.payload === aResult,
        )
        expect(aFinalSources).toHaveLength(1)
        const aFinalSource = aFinalSources[0]
        if (aFinalSource === undefined) throw new Error("expected finalized A reply source")
        expect(aFinalSource.consumed).toEqual([bResolution.id])
        expect((yield* delegation.getResolution(bResolution.id))?.status).toBe("resolved")
        expect((yield* delegation.getResolution(bResolution.id))?.resolvedSourceID).toBe(aFinalSource.id)
        expect((yield* delegation.incoming(a.id)).filter((resolution) => resolution.childID === f.id)).toHaveLength(0)

        rootBeforeGate.resolve()
        const rootAfterAHit = yield* waitForSystem(llm, rootMarker, 3)
        const rootAfterAContents = messageContents(rootAfterAHit)
        expect(rootAfterAContents.filter((content) => content === taskResult(a.id, aResult))).toHaveLength(1)
        expect(rootAfterAContents.filter((content) => content === taskResult(b.id, bResult))).toHaveLength(0)
        expect(rootAfterAContents.filter((content) => content === completedTaskResult(f.id, fResult))).toHaveLength(0)

        const rootMessages = yield* sessions.messages({ sessionID: root.id })
        expect(
          rootMessages.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text === taskResult(a.id, aResult)),
          ),
        ).toHaveLength(1)
        expect(
          rootMessages.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text.includes(`<task_id>${b.id}</task_id>`)),
          ),
        ).toHaveLength(0)
        expect(
          rootMessages.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text.includes(`<task_id>${f.id}</task_id>`)),
          ),
        ).toHaveLength(0)
        expect((yield* delegation.incoming(root.id)).filter((resolution) => resolution.childID === f.id)).toHaveLength(
          0,
        )

        rootAfterGate.resolve()
        const rootRunExit = yield* Fiber.await(rootRun)
        expect(Exit.isSuccess(rootRunExit)).toBe(true)
      }).pipe(Effect.ensuring(cleanup))
    }),
  30_000,
)

background.instance(
  "recursive concurrent background returns coalesce without lost outcomes",
  () =>
    Effect.gen(function* () {
      const rootMarker = "recursive-concurrent-root-marker"
      const aMarker = "recursive-concurrent-a-marker"
      const bMarker = "recursive-concurrent-b-marker"
      const cMarker = "recursive-concurrent-c-marker"
      const bResult = "B_RESULT_CONCURRENT"
      const cResult = "C_RESULT_CONCURRENT"
      const aInitialResult = "A_INITIAL_CONCURRENT"
      const aAfterResult = "A_AFTER_BC_CONCURRENT"

      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        subagent_depth: 3,
        permission: { task: { a: "allow" } },
        agent: {
          build: {
            model: "test/test-model",
            prompt: rootMarker,
          },
          a: {
            mode: "subagent",
            model: "test/test-model",
            prompt: aMarker,
            permission: { task: { b: "allow", c: "allow" } },
          },
          b: {
            mode: "subagent",
            model: "test/test-model",
            prompt: bMarker,
          },
          c: {
            mode: "subagent",
            model: "test/test-model",
            prompt: cMarker,
          },
        },
      }))
      const jobs = yield* BackgroundJob.Service
      const delegation = yield* DelegationStore.Service
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const root = yield* sessions.create({
        title: "Concurrent background returns",
        agent: "build",
        model: { providerID: ref.providerID, id: ref.modelID },
      })
      const bGate = Promise.withResolvers<void>()
      const cGate = Promise.withResolvers<void>()
      const aInitialGate = Promise.withResolvers<void>()
      const rootBeforeGate = Promise.withResolvers<void>()
      const rootAfterGate = Promise.withResolvers<void>()

      const cleanup = Effect.gen(function* () {
        yield* prompt.cancel(root.id).pipe(Effect.ignore)
        bGate.resolve()
        cGate.resolve()
        aInitialGate.resolve()
        rootBeforeGate.resolve()
        rootAfterGate.resolve()
      })

      yield* Effect.gen(function* () {
        yield* llm.pushMatch(
          hasSystemMarker(rootMarker),
          reply()
            .tool("task", {
              description: "launch agent a",
              prompt: "Investigate A",
              subagent_type: "a",
              background: true,
            })
            .item(),
          reply().text("ROOT_WAITING_FOR_A").stop().wait(rootBeforeGate.promise).item(),
          reply().text("ROOT_AFTER_A").stop().wait(rootAfterGate.promise).item(),
        )
        yield* llm.pushMatch(
          hasSystemMarker(aMarker),
          reply()
            .tool("task", {
              description: "launch agent b",
              prompt: "Investigate B",
              subagent_type: "b",
              background: true,
            })
            .item(),
          reply()
            .tool("task", {
              description: "launch agent c",
              prompt: "Investigate C",
              subagent_type: "c",
              background: true,
            })
            .item(),
          reply().text(aInitialResult).stop().wait(aInitialGate.promise).item(),
          reply().text(aAfterResult).stop().item(),
        )
        yield* llm.pushMatch(hasSystemMarker(bMarker), reply().text(bResult).stop().wait(bGate.promise).item())
        yield* llm.pushMatch(hasSystemMarker(cMarker), reply().text(cResult).stop().wait(cGate.promise).item())

        const rootRun = yield* prompt
          .prompt({
            sessionID: root.id,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "Begin the concurrent background chain." }],
          })
          .pipe(Effect.forkChild)

        const rootWaitingHit = yield* waitForSystem(llm, rootMarker, 2)
        yield* waitForSystem(llm, aMarker, 3)
        yield* waitForSystem(llm, bMarker)
        yield* waitForSystem(llm, cMarker)

        const rootChildren = yield* sessions.children(root.id)
        const a = rootChildren.find((child) => child.agent === "a")
        if (a === undefined) throw new Error("expected A child session")
        const aChildren = yield* sessions.children(a.id)
        const b = aChildren.find((child) => child.agent === "b")
        const c = aChildren.find((child) => child.agent === "c")
        if (b === undefined || c === undefined) throw new Error("expected B and C child sessions")
        expect(
          messageContents(rootWaitingHit).filter((content) => content === taskResult(a.id, aInitialResult)),
        ).toHaveLength(0)

        bGate.resolve()
        cGate.resolve()
        const [bWaited, cWaited] = yield* Effect.all([
          jobs.wait({ id: b.id, timeout: 10_000 }),
          jobs.wait({ id: c.id, timeout: 10_000 }),
        ])
        expect(bWaited.timedOut).toBe(false)
        expect(cWaited.timedOut).toBe(false)
        expect(bWaited.info?.status).toBe("completed")
        expect(cWaited.info?.status).toBe("completed")
        const bSource = yield* waitForFinalizedAssistantSource(delegation, b.id, bResult)
        const cSource = yield* waitForFinalizedAssistantSource(delegation, c.id, cResult)
        const bResolution = yield* waitForAdmittedResolution(delegation, a.id, bSource.source.id)
        const cResolution = yield* waitForAdmittedResolution(delegation, a.id, cSource.source.id)
        expect(new Set([bResolution.id, cResolution.id]).size).toBe(2)

        const aReserved = (yield* delegation.sources(a.id)).find(
          (source) => source.state === "reserved" && source.source.kind === "assistant",
        )
        if (aReserved === undefined) throw new Error("expected reserved A initial source")
        expect(aReserved.consumed).not.toContain(bResolution.id)
        expect(aReserved.consumed).not.toContain(cResolution.id)
        expect((yield* llm.hits).filter(hasSystemMarker(aMarker))).toHaveLength(3)

        aInitialGate.resolve()
        const aAfterHit = yield* waitForSystem(llm, aMarker, 4)
        const aAfterContents = messageContents(aAfterHit)
        expect(aAfterContents.filter((content) => content === taskResult(b.id, bResult))).toHaveLength(1)
        expect(aAfterContents.filter((content) => content === taskResult(c.id, cResult))).toHaveLength(1)

        const aWaited = yield* jobs.wait({ id: a.id, timeout: 10_000 })
        expect(aWaited.timedOut).toBe(false)
        expect(aWaited.info?.status).toBe("completed")
        const aMessages = yield* sessions.messages({ sessionID: a.id })
        const aAssistants = aMessages.filter((message) => message.info.role === "assistant")
        expect(
          aAssistants.filter((message) =>
            message.parts.some((part) => part.type === "text" && part.text === aInitialResult),
          ),
        ).toHaveLength(1)
        expect(
          aAssistants.filter((message) =>
            message.parts.some((part) => part.type === "text" && part.text === aAfterResult),
          ),
        ).toHaveLength(1)

        const finalizedASources = (yield* delegation.sources(a.id)).filter(
          (source) => source.state === "finalized" && source.source.kind === "assistant",
        )
        const aInitialSource = finalizedASources.find((source) => source.payload === aInitialResult)
        const aAfterSource = finalizedASources.find((source) => source.payload === aAfterResult)
        if (aInitialSource === undefined || aAfterSource === undefined) {
          throw new Error("expected separate initial and B/C-informed A sources")
        }
        expect(finalizedASources.filter((source) => source.payload === aInitialResult)).toHaveLength(1)
        expect(finalizedASources.filter((source) => source.payload === aAfterResult)).toHaveLength(1)
        expect(aInitialSource.consumed).toEqual([])
        expect(aAfterSource.consumed).toEqual([bResolution.id, cResolution.id])
        expect((yield* delegation.getResolution(bResolution.id))?.status).toBe("resolved")
        expect((yield* delegation.getResolution(cResolution.id))?.status).toBe("resolved")
        expect((yield* delegation.getResolution(bResolution.id))?.resolvedSourceID).toBe(aAfterSource.id)
        expect((yield* delegation.getResolution(cResolution.id))?.resolvedSourceID).toBe(aAfterSource.id)
        expect((yield* delegation.incoming(a.id)).filter((resolution) => resolution.childID === b.id)).toHaveLength(0)
        expect((yield* delegation.incoming(a.id)).filter((resolution) => resolution.childID === c.id)).toHaveLength(0)

        rootBeforeGate.resolve()
        const rootAfterAHit = yield* waitForSystem(llm, rootMarker, 3)
        const rootAfterAContents = messageContents(rootAfterAHit)
        expect(rootAfterAContents.filter((content) => content === taskResult(a.id, aInitialResult))).toHaveLength(1)
        expect(rootAfterAContents.filter((content) => content === taskResult(a.id, aAfterResult))).toHaveLength(1)
        expect(rootAfterAContents.filter((content) => content === taskResult(b.id, bResult))).toHaveLength(0)
        expect(rootAfterAContents.filter((content) => content === taskResult(c.id, cResult))).toHaveLength(0)
        expect(rootAfterAContents.indexOf(taskResult(a.id, aInitialResult))).toBeLessThan(
          rootAfterAContents.indexOf(taskResult(a.id, aAfterResult)),
        )

        const rootMessages = yield* sessions.messages({ sessionID: root.id })
        expect(
          rootMessages.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text === taskResult(a.id, aInitialResult)),
          ),
        ).toHaveLength(1)
        expect(
          rootMessages.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text === taskResult(a.id, aAfterResult)),
          ),
        ).toHaveLength(1)
        expect(
          rootMessages.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text.includes(`<task_id>${b.id}</task_id>`)),
          ),
        ).toHaveLength(0)
        expect(
          rootMessages.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text.includes(`<task_id>${c.id}</task_id>`)),
          ),
        ).toHaveLength(0)

        const rootResolutions = yield* delegation.incoming(root.id)
        expect(rootResolutions.filter((resolution) => resolution.childID === a.id)).toHaveLength(2)
        expect(rootResolutions.filter((resolution) => resolution.childID === b.id)).toHaveLength(0)
        expect(rootResolutions.filter((resolution) => resolution.childID === c.id)).toHaveLength(0)
        const hits = yield* llm.hits
        expect(hits).toHaveLength(9)
        expect(hits.filter(hasSystemMarker(rootMarker))).toHaveLength(3)
        expect(hits.filter(hasSystemMarker(aMarker))).toHaveLength(4)
        expect(hits.filter(hasSystemMarker(bMarker))).toHaveLength(1)
        expect(hits.filter(hasSystemMarker(cMarker))).toHaveLength(1)
        expect(yield* llm.misses).toHaveLength(0)
        expect(yield* llm.pending).toBe(0)

        rootAfterGate.resolve()
        const rootRunExit = yield* Fiber.await(rootRun)
        expect(Exit.isSuccess(rootRunExit)).toBe(true)
      }).pipe(Effect.ensuring(cleanup))
    }),
  30_000,
)

background.instance(
  "recursive root cancellation traverses idle intermediate generations",
  () =>
    Effect.gen(function* () {
      const rootMarker = "recursive-cancel-root-marker"
      const aMarker = "recursive-cancel-a-marker"
      const bMarker = "recursive-cancel-b-marker"
      const aInitialResult = "A_INITIAL_BEFORE_CANCEL"
      const bResult = "B_RESULT_AFTER_CANCEL"

      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        subagent_depth: 3,
        permission: { task: { a: "allow" } },
        agent: {
          build: {
            model: "test/test-model",
            prompt: rootMarker,
          },
          a: {
            mode: "subagent",
            model: "test/test-model",
            prompt: aMarker,
            permission: { task: { b: "allow" } },
          },
          b: {
            mode: "subagent",
            model: "test/test-model",
            prompt: bMarker,
          },
        },
      }))
      const jobs = yield* BackgroundJob.Service
      const delegation = yield* DelegationStore.Service
      const prompt = yield* SessionPrompt.Service
      const runState = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const root = yield* sessions.create({
        title: "Recursive cancellation",
        agent: "build",
        model: { providerID: ref.providerID, id: ref.modelID },
      })
      const bGate = Promise.withResolvers<void>()
      const rootBeforeGate = Promise.withResolvers<void>()

      const cleanup = Effect.gen(function* () {
        yield* prompt.cancel(root.id).pipe(Effect.ignore)
        bGate.resolve()
        rootBeforeGate.resolve()
      })

      yield* Effect.gen(function* () {
        yield* llm.pushMatch(
          hasSystemMarker(rootMarker),
          reply()
            .tool("task", {
              description: "launch agent a",
              prompt: "Investigate A",
              subagent_type: "a",
              background: true,
            })
            .item(),
          reply().text("ROOT_WAITING_FOR_A").stop().wait(rootBeforeGate.promise).item(),
          reply().text("ROOT_INITIAL_DONE").stop().item(),
        )
        yield* llm.pushMatch(
          hasSystemMarker(aMarker),
          reply()
            .tool("task", {
              description: "launch agent b",
              prompt: "Investigate B",
              subagent_type: "b",
              background: true,
            })
            .item(),
          reply().text(aInitialResult).stop().item(),
        )
        yield* llm.pushMatch(hasSystemMarker(bMarker), reply().text(bResult).stop().wait(bGate.promise).item())

        const rootRun = yield* prompt
          .prompt({
            sessionID: root.id,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "Begin the cancellation chain." }],
          })
          .pipe(Effect.forkChild)

        const rootWaitingHit = yield* waitForSystem(llm, rootMarker, 2)
        yield* waitForSystem(llm, aMarker, 2)
        yield* waitForSystem(llm, bMarker)

        const rootChildren = yield* sessions.children(root.id)
        const a = rootChildren.find((child) => child.agent === "a")
        if (a === undefined) throw new Error("expected A child session")
        const aChildren = yield* sessions.children(a.id)
        const b = aChildren.find((child) => child.agent === "b")
        if (b === undefined) throw new Error("expected B child session")
        const aGeneration = yield* delegation.active(a.id)
        const bGeneration = yield* delegation.active(b.id)
        if (aGeneration === undefined || bGeneration === undefined) {
          throw new Error("expected active A and B generations")
        }
        expect(aGeneration.parentID).toBe(root.id)
        expect(bGeneration.parentID).toBe(a.id)
        expect(bGeneration.parentGenerationID).toBe(aGeneration.id)

        const aWaited = yield* jobs.wait({ id: a.id, timeout: 10_000 })
        expect(aWaited.timedOut).toBe(false)
        expect(aWaited.info?.status).toBe("completed")
        const aInitialSource = yield* waitForFinalizedAssistantSource(delegation, a.id, aInitialResult)
        expect((yield* jobs.get(b.id))?.status).toBe("running")
        expect(yield* runState.busy(b.id)).toBe(true)

        rootBeforeGate.resolve()
        const rootInitialHit = yield* waitForSystem(llm, rootMarker, 3)
        expect(
          messageContents(rootWaitingHit).filter((content) => content === taskResult(a.id, aInitialResult)),
        ).toHaveLength(0)
        expect(
          messageContents(rootInitialHit).filter((content) => content === taskResult(a.id, aInitialResult)),
        ).toHaveLength(1)
        const rootRunExit = yield* Fiber.await(rootRun)
        expect(Exit.isSuccess(rootRunExit)).toBe(true)
        yield* waitForNotBusy(runState, root.id)

        const rootMessagesBeforeCancel = yield* sessions.messages({ sessionID: root.id })
        expect(
          rootMessagesBeforeCancel.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text === taskResult(a.id, aInitialResult)),
          ),
        ).toHaveLength(1)
        const bSource = (yield* delegation.sources(b.id)).find(
          (source) => source.state === "reserved" && source.source.kind === "assistant",
        )
        if (bSource === undefined) throw new Error("expected reserved B source before cancellation")
        const callsBeforeCancel = yield* llm.calls
        const rootReturnsBeforeCancel = (yield* delegation.incoming(root.id)).map((resolution) => ({
          id: resolution.id,
          childID: resolution.childID,
          sourceID: resolution.source.id,
          status: resolution.status,
        }))

        yield* prompt.cancel(root.id)

        expect((yield* delegation.get(aGeneration.id))?.state).toBe("revoked")
        expect((yield* delegation.get(bGeneration.id))?.state).toBe("revoked")
        const bWaited = yield* jobs.wait({ id: b.id, timeout: 10_000 })
        expect(bWaited.timedOut).toBe(false)
        expect(bWaited.info?.status).toBe("cancelled")
        yield* waitForNotBusy(runState, a.id)
        yield* waitForNotBusy(runState, b.id)
        yield* waitForNotBusy(runState, root.id)

        const bSourcesAfterCancel = yield* delegation.sources(b.id)
        expect(bSourcesAfterCancel.filter((source) => source.source.kind === "assistant")).toHaveLength(1)
        const bSourceAfterCancel = bSourcesAfterCancel.find((source) => source.id === bSource.id)
        if (bSourceAfterCancel === undefined) throw new Error("expected original B source after cancellation")
        expect(bSourceAfterCancel.state).toBe("reserved")

        const rootMessagesAfterCancel = yield* sessions.messages({ sessionID: root.id })
        expect(rootMessagesAfterCancel).toHaveLength(rootMessagesBeforeCancel.length)
        expect(
          rootMessagesAfterCancel.filter((message) =>
            message.parts.some((part) => part.type === "text" && part.text.includes(`<task_id>${b.id}</task_id>`)),
          ),
        ).toHaveLength(0)
        expect(
          (yield* delegation.incoming(root.id)).map((resolution) => ({
            id: resolution.id,
            childID: resolution.childID,
            sourceID: resolution.source.id,
            status: resolution.status,
          })),
        ).toEqual(rootReturnsBeforeCancel)
        expect(yield* llm.calls).toBe(callsBeforeCancel)

        const bMessagesAfterCancel = yield* sessions.messages({ sessionID: b.id })
        const bAssistantAfterCancel = bMessagesAfterCancel.findLast(
          (message) => message.info.role === "assistant" && String(message.info.id) === String(bSource.source.id),
        )
        if (bAssistantAfterCancel === undefined || bAssistantAfterCancel.info.role !== "assistant") {
          throw new Error("expected persisted registered B assistant after cancellation")
        }
        expect(bAssistantAfterCancel.info.time.completed).toBeDefined()
        expect(bAssistantAfterCancel.info.error?.name).toBe("MessageAbortedError")
        const bCompletedAt = bAssistantAfterCancel.info.time.completed
        if (bCompletedAt === undefined) throw new Error("expected completed B assistant timestamp")
        expect(
          bMessagesAfterCancel
            .flatMap((message) => message.parts)
            .filter((part): part is SessionV1.ToolPart => part.type === "tool")
            .some((part) => part.state.status === "pending" || part.state.status === "running"),
        ).toBe(false)

        const rootMessagesBeforeRecover = yield* sessions.messages({ sessionID: root.id })
        const rootReturnsBeforeRecover = (yield* delegation.incoming(root.id)).map((resolution) => ({
          id: resolution.id,
          childID: resolution.childID,
          sourceID: resolution.source.id,
          status: resolution.status,
        }))
        const callsBeforeRecover = yield* llm.calls
        yield* prompt.recover()

        const bMessagesAfterRecover = yield* sessions.messages({ sessionID: b.id })
        const bAssistantAfterRecover = bMessagesAfterRecover.findLast(
          (message) => message.info.role === "assistant" && String(message.info.id) === String(bSource.source.id),
        )
        if (bAssistantAfterRecover === undefined || bAssistantAfterRecover.info.role !== "assistant") {
          throw new Error("expected persisted registered B assistant after recovery")
        }
        expect(bAssistantAfterRecover.info.id).toBe(bAssistantAfterCancel.info.id)
        expect(bAssistantAfterRecover.info.time.completed).toBe(bCompletedAt)
        expect(bAssistantAfterRecover.info.error?.name).toBe("MessageAbortedError")
        expect(
          bMessagesAfterRecover
            .flatMap((message) => message.parts)
            .filter((part): part is SessionV1.ToolPart => part.type === "tool")
            .some((part) => part.state.status === "pending" || part.state.status === "running"),
        ).toBe(false)
        const rootMessagesAfterRecover = yield* sessions.messages({ sessionID: root.id })
        expect(rootMessagesAfterRecover).toHaveLength(rootMessagesBeforeRecover.length)
        expect(
          rootMessagesAfterRecover.filter((message) =>
            message.parts.some((part) => part.type === "text" && part.text.includes(`<task_id>${b.id}</task_id>`)),
          ),
        ).toHaveLength(0)
        expect(
          (yield* delegation.incoming(root.id)).map((resolution) => ({
            id: resolution.id,
            childID: resolution.childID,
            sourceID: resolution.source.id,
            status: resolution.status,
          })),
        ).toEqual(rootReturnsBeforeRecover)
        expect(yield* llm.calls).toBe(callsBeforeRecover)

        bGate.resolve()
        expect(yield* llm.calls).toBe(callsBeforeCancel)
        expect(yield* llm.misses).toHaveLength(0)
        expect(yield* llm.pending).toBe(0)
        const hits = yield* llm.hits
        expect(hits.filter(hasSystemMarker(rootMarker))).toHaveLength(3)
        expect(hits.filter(hasSystemMarker(aMarker))).toHaveLength(2)
        expect(hits.filter(hasSystemMarker(bMarker))).toHaveLength(1)
        yield* waitForNotBusy(runState, root.id)
        const rootMessagesAfterRelease = yield* sessions.messages({ sessionID: root.id })
        expect(rootMessagesAfterRelease).toHaveLength(rootMessagesBeforeCancel.length)
      }).pipe(Effect.ensuring(cleanup))
    }),
  30_000,
)

background.instance(
  "recursive parent deletion cannot resurrect pending returns",
  () =>
    Effect.gen(function* () {
      const rootMarker = "recursive-delete-root-marker"
      const aMarker = "recursive-delete-a-marker"
      const aResult = "A_RESULT_AFTER_DELETE"

      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        subagent_depth: 3,
        permission: { task: { a: "allow" } },
        agent: {
          build: {
            model: "test/test-model",
            prompt: rootMarker,
          },
          a: {
            mode: "subagent",
            model: "test/test-model",
            prompt: aMarker,
          },
        },
      }))
      const db = yield* Database.Service
      const delegation = yield* DelegationStore.Service
      const jobs = yield* BackgroundJob.Service
      const prompt = yield* SessionPrompt.Service
      const runState = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const root = yield* sessions.create({
        title: "Recursive deletion",
        agent: "build",
        model: { providerID: ref.providerID, id: ref.modelID },
      })
      const childGate = Promise.withResolvers<void>()

      const cleanup = Effect.gen(function* () {
        childGate.resolve()
        yield* prompt.cancel(root.id).pipe(Effect.ignore)
      })

      yield* Effect.gen(function* () {
        yield* llm.pushMatch(
          hasSystemMarker(rootMarker),
          reply()
            .tool("task", {
              description: "launch agent a",
              prompt: "Investigate A",
              subagent_type: "a",
              background: true,
            })
            .item(),
          reply().text("ROOT_INITIAL_DONE").stop().item(),
        )
        yield* llm.pushMatch(hasSystemMarker(aMarker), reply().text(aResult).stop().wait(childGate.promise).item())

        const rootRun = yield* prompt
          .prompt({
            sessionID: root.id,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "Begin the deletion chain." }],
          })
          .pipe(Effect.forkChild)

        yield* waitForSystem(llm, rootMarker, 2)
        yield* waitForSystem(llm, aMarker)

        const rootChild = yield* sessions.children(root.id)
        const a = rootChild.find((child) => child.agent === "a")
        if (a === undefined) throw new Error("expected A child session")
        const aGeneration = yield* delegation.active(a.id)
        if (aGeneration === undefined) throw new Error("expected active A generation")
        const aSource = (yield* delegation.sources(a.id)).find(
          (source) => source.state === "reserved" && source.source.kind === "assistant",
        )
        if (aSource === undefined) throw new Error("expected reserved A source before deletion")
        expect((yield* jobs.get(a.id))?.status).toBe("running")
        expect(yield* runState.busy(a.id)).toBe(true)

        const rootRunExit = yield* Fiber.await(rootRun)
        expect(Exit.isSuccess(rootRunExit)).toBe(true)
        yield* waitForNotBusy(runState, root.id)
        const callsBeforeDelete = yield* llm.calls

        yield* sessions.remove(root.id)

        expect((yield* delegation.get(aGeneration.id))?.state).toBe("revoked")
        const childJob = yield* jobs.wait({ id: a.id, timeout: 10_000 })
        expect(childJob.timedOut).toBe(false)
        expect(childJob.info?.status).toBe("cancelled")
        yield* waitForNotBusy(runState, a.id)
        expect(yield* runState.busy(root.id)).toBe(false)

        const remainingSessions = yield* db.db
          .select({ id: SessionTable.id })
          .from(SessionTable)
          .where(inArray(SessionTable.id, [root.id, a.id]))
          .all()
        expect(remainingSessions).toHaveLength(0)
        const remainingMessages = yield* db.db
          .select({ id: MessageTable.id, sessionID: MessageTable.session_id })
          .from(MessageTable)
          .where(inArray(MessageTable.session_id, [root.id, a.id]))
          .all()
        expect(remainingMessages).toHaveLength(0)
        const aSourcesAfterDelete = yield* delegation.sources(a.id)
        expect(aSourcesAfterDelete.filter((source) => source.source.kind === "assistant")).toHaveLength(1)
        const aSourceAfterDelete = aSourcesAfterDelete.find((source) => source.id === aSource.id)
        if (aSourceAfterDelete === undefined) throw new Error("expected original A source after deletion")
        expect(aSourceAfterDelete.state).toBe("reserved")
        expect(yield* llm.calls).toBe(callsBeforeDelete)

        childGate.resolve()
        yield* Effect.yieldNow
        expect(yield* llm.calls).toBe(callsBeforeDelete)
        expect(yield* llm.misses).toHaveLength(0)
        expect(yield* llm.pending).toBe(0)
        const hits = yield* llm.hits
        expect(hits.filter(hasSystemMarker(rootMarker))).toHaveLength(2)
        expect(hits.filter(hasSystemMarker(aMarker))).toHaveLength(1)
        expect(
          yield* db.db
            .select({ id: SessionTable.id })
            .from(SessionTable)
            .where(inArray(SessionTable.id, [root.id, a.id]))
            .all(),
        ).toHaveLength(0)
        expect(
          yield* db.db
            .select({ id: MessageTable.id })
            .from(MessageTable)
            .where(inArray(MessageTable.session_id, [root.id, a.id]))
            .all(),
        ).toHaveLength(0)
      }).pipe(Effect.ensuring(cleanup))
    }),
  30_000,
)

background.instance(
  "recursive completion admission race preserves unconsumed input",
  () =>
    Effect.gen(function* () {
      const rootMarker = "recursive-race-root-marker"
      const aMarker = "recursive-race-a-marker"
      const bMarker = "recursive-race-b-marker"
      const aInitialResult = "A_INITIAL_RACE"
      const aAfterResult = "A_AFTER_B_RACE"
      const bResult = "B_RESULT_RACE"

      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        subagent_depth: 3,
        permission: { task: { a: "allow" } },
        agent: {
          build: {
            model: "test/test-model",
            prompt: rootMarker,
          },
          a: {
            mode: "subagent",
            model: "test/test-model",
            prompt: aMarker,
            permission: { task: { b: "allow" } },
          },
          b: {
            mode: "subagent",
            model: "test/test-model",
            prompt: bMarker,
          },
        },
      }))
      const events = yield* EventV2Bridge.Service
      const jobs = yield* BackgroundJob.Service
      const delegation = yield* DelegationStore.Service
      const prompt = yield* SessionPrompt.Service
      const runState = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const root = yield* sessions.create({
        title: "Recursive completion race",
        agent: "build",
        model: { providerID: ref.providerID, id: ref.modelID },
      })
      const bGate = Promise.withResolvers<void>()
      const aInitialGate = Promise.withResolvers<void>()
      const rootBeforeGate = Promise.withResolvers<void>()
      const rootAfterGate = Promise.withResolvers<void>()
      const listenerReturned = yield* Deferred.make<void>()

      const cleanup = Effect.gen(function* () {
        bGate.resolve()
        aInitialGate.resolve()
        rootBeforeGate.resolve()
        rootAfterGate.resolve()
        yield* prompt.cancel(root.id).pipe(Effect.ignore)
      })

      yield* Effect.gen(function* () {
        yield* llm.pushMatch(
          hasSystemMarker(rootMarker),
          reply()
            .tool("task", {
              description: "launch agent a",
              prompt: "Investigate A",
              subagent_type: "a",
              background: true,
            })
            .item(),
          reply().text("ROOT_WAITING_FOR_A").stop().wait(rootBeforeGate.promise).item(),
          reply().text("ROOT_AFTER_A").stop().wait(rootAfterGate.promise).item(),
        )
        yield* llm.pushMatch(
          hasSystemMarker(aMarker),
          reply()
            .tool("task", {
              description: "launch agent b",
              prompt: "Investigate B",
              subagent_type: "b",
              background: true,
            })
            .item(),
          reply().text(aInitialResult).stop().wait(aInitialGate.promise).item(),
          reply().text(aAfterResult).stop().item(),
        )
        yield* llm.pushMatch(hasSystemMarker(bMarker), reply().text(bResult).stop().wait(bGate.promise).item())

        const rootRun = yield* prompt
          .prompt({
            sessionID: root.id,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "Begin the completion race chain." }],
          })
          .pipe(Effect.forkChild)

        yield* waitForSystem(llm, rootMarker, 2)
        yield* waitForSystem(llm, aMarker, 2)
        yield* waitForSystem(llm, bMarker)

        const rootChildren = yield* sessions.children(root.id)
        const a = rootChildren.find((child) => child.agent === "a")
        if (a === undefined) throw new Error("expected A child session")
        const aChildren = yield* sessions.children(a.id)
        const b = aChildren.find((child) => child.agent === "b")
        if (b === undefined) throw new Error("expected B child session")
        const aInitialSource = (yield* delegation.sources(a.id)).find(
          (source) => source.state === "reserved" && source.source.kind === "assistant",
        )
        if (aInitialSource === undefined) throw new Error("expected reserved A initial source")
        expect((yield* jobs.get(a.id))?.status).toBe("running")
        expect((yield* jobs.get(b.id))?.status).toBe("running")

        const off = yield* events.listen((event) => {
          if (event.type !== MessageV2.Event.Updated.type) return Effect.void
          const data = event.data as typeof MessageV2.Event.Updated.data.Type
          if (
            data.sessionID !== a.id ||
            data.info.role !== "assistant" ||
            data.info.id !== aInitialSource.source.id ||
            data.info.finish !== "stop" ||
            data.info.time.completed !== undefined
          ) {
            return Effect.void
          }
          bGate.resolve()
          return waitForAdmittedChildResolution(delegation, a.id, b.id).pipe(
            Effect.andThen(
              Effect.sync(() => {
                Deferred.doneUnsafe(listenerReturned, Effect.void)
              }),
            ),
            Effect.orDie,
          )
        })
        yield* Effect.addFinalizer(() => off)

        aInitialGate.resolve()
        yield* awaitWithTimeout(
          Deferred.await(listenerReturned),
          "timed out waiting for the completion listener to admit B before returning",
          "10 seconds",
        )

        const aInitialFinalSource = yield* waitForFinalizedAssistantSource(delegation, a.id, aInitialResult)
        expect(aInitialFinalSource.id).toBe(aInitialSource.id)
        expect(aInitialFinalSource.consumed).toEqual([])
        const bSource = yield* waitForFinalizedAssistantSource(delegation, b.id, bResult)
        const bResolution = yield* waitForAdmittedChildResolution(delegation, a.id, b.id)
        expect(bResolution.source.id).toBe(bSource.source.id)
        expect((yield* delegation.getResolution(bResolution.id))?.status).toBe("admitted")

        const aAfterHit = yield* waitForSystem(llm, aMarker, 3)
        const aAfterContents = messageContents(aAfterHit)
        expect(aAfterContents.filter((content) => content === taskResult(b.id, bResult))).toHaveLength(1)
        const aWaited = yield* jobs.wait({ id: a.id, timeout: 10_000 })
        expect(aWaited.timedOut).toBe(false)
        expect(aWaited.info?.status).toBe("completed")
        const aAfterSource = yield* waitForFinalizedAssistantSource(delegation, a.id, aAfterResult)
        expect(aAfterSource.consumed).toEqual([bResolution.id])
        expect((yield* delegation.getResolution(bResolution.id))?.status).toBe("resolved")
        expect((yield* delegation.getResolution(bResolution.id))?.resolvedSourceID).toBe(aAfterSource.id)
        expect((yield* delegation.incoming(a.id)).filter((resolution) => resolution.childID === b.id)).toHaveLength(0)

        rootBeforeGate.resolve()
        const rootAfterHit = yield* waitForSystem(llm, rootMarker, 3)
        const rootAfterContents = messageContents(rootAfterHit)
        expect(rootAfterContents.filter((content) => content === taskResult(a.id, aInitialResult))).toHaveLength(1)
        expect(rootAfterContents.filter((content) => content === taskResult(a.id, aAfterResult))).toHaveLength(1)
        expect(rootAfterContents.filter((content) => content === taskResult(b.id, bResult))).toHaveLength(0)
        expect(rootAfterContents.indexOf(taskResult(a.id, aInitialResult))).toBeLessThan(
          rootAfterContents.indexOf(taskResult(a.id, aAfterResult)),
        )

        const rootMessages = yield* sessions.messages({ sessionID: root.id })
        expect(
          rootMessages.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text === taskResult(a.id, aInitialResult)),
          ),
        ).toHaveLength(1)
        expect(
          rootMessages.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text === taskResult(a.id, aAfterResult)),
          ),
        ).toHaveLength(1)
        expect(
          rootMessages.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text.includes(`<task_id>${b.id}</task_id>`)),
          ),
        ).toHaveLength(0)

        const rootResolutions = yield* delegation.incoming(root.id)
        expect(rootResolutions.filter((resolution) => resolution.childID === a.id)).toHaveLength(2)
        expect(rootResolutions.filter((resolution) => resolution.childID === b.id)).toHaveLength(0)
        const hits = yield* llm.hits
        expect(hits).toHaveLength(7)
        expect(hits.filter(hasSystemMarker(rootMarker))).toHaveLength(3)
        expect(hits.filter(hasSystemMarker(aMarker))).toHaveLength(3)
        expect(hits.filter(hasSystemMarker(bMarker))).toHaveLength(1)
        expect(yield* llm.misses).toHaveLength(0)
        expect(yield* llm.pending).toBe(0)

        rootAfterGate.resolve()
        const rootRunExit = yield* Fiber.await(rootRun)
        expect(Exit.isSuccess(rootRunExit)).toBe(true)
        yield* waitForNotBusy(runState, root.id)
      }).pipe(Effect.ensuring(cleanup))
    }),
  30_000,
)

background.instance(
  "provider finalization has no completed-message capture gap",
  () =>
    Effect.gen(function* () {
      const rootMarker = "provider-finalization-gap-root-marker"
      const aMarker = "provider-finalization-gap-a-marker"
      const aResult = "A_PROVIDER_FINALIZATION_RESULT"
      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        subagent_depth: 3,
        permission: { task: { a: "allow" } },
        agent: {
          build: {
            model: "test/test-model",
            prompt: rootMarker,
          },
          a: {
            mode: "subagent",
            model: "test/test-model",
            prompt: aMarker,
          },
        },
      }))
      const db = yield* Database.Service
      const events = yield* EventV2Bridge.Service
      const delegation = yield* DelegationStore.Service
      const prompt = yield* SessionPrompt.Service
      const runState = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const root = yield* sessions.create({
        title: "Provider finalization gap",
        agent: "build",
        model: { providerID: ref.providerID, id: ref.modelID },
      })
      const rootGate = Promise.withResolvers<void>()
      const aFinalGate = Promise.withResolvers<void>()
      const uiCompleted = yield* Deferred.make<{
        readonly eventID: string | undefined
        readonly info: SessionV1.Assistant
      }>()
      const observations: Array<{
        readonly completed: number | undefined
        readonly sourceState: Delegation.SourceState | undefined
        readonly outboxLength: number
      }> = []

      const cleanup = Effect.gen(function* () {
        rootGate.resolve()
        aFinalGate.resolve()
        yield* prompt.cancel(root.id).pipe(Effect.ignore)
      })

      yield* Effect.gen(function* () {
        yield* llm.pushMatch(
          hasSystemMarker(rootMarker),
          reply()
            .tool("task", {
              description: "launch provider finalization child",
              prompt: "Run the provider finalization regression.",
              subagent_type: "a",
              background: true,
            })
            .item(),
          reply().text("ROOT_WAITING_FOR_PROVIDER_FINALIZATION").stop().wait(rootGate.promise).item(),
          reply().text("ROOT_AFTER_PROVIDER_FINALIZATION").stop().item(),
        )
        yield* llm.pushMatch(hasSystemMarker(aMarker), reply().text(aResult).stop().wait(aFinalGate.promise).item())

        const rootRun = yield* prompt
          .prompt({
            sessionID: root.id,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "Begin the provider finalization regression." }],
          })
          .pipe(Effect.forkChild)

        yield* waitForSystem(llm, rootMarker, 2)
        yield* waitForSystem(llm, aMarker)
        const child = (yield* sessions.children(root.id)).find((session) => session.agent === "a")
        if (child === undefined) throw new Error("expected provider finalization child session")
        const assistantSource = (yield* delegation.sources(child.id)).find(
          (source) => source.state === "reserved" && source.source.kind === "assistant",
        )
        if (assistantSource === undefined) throw new Error("expected reserved provider assistant source")

        const off = yield* events.listen((event) => {
          if (event.type !== MessageV2.Event.Updated.type) return Effect.void
          const data = event.data as typeof MessageV2.Event.Updated.data.Type
          if (
            data.sessionID !== child.id ||
            data.info.role !== "assistant" ||
            data.info.id !== assistantSource.source.id
          ) {
            return Effect.void
          }
          const info = data.info as SessionV1.Assistant
          return Effect.gen(function* () {
            const source = (yield* delegation.sources(child.id)).find((item) => item.id === assistantSource.id)
            const outbox = (yield* delegation.incoming(root.id)).filter((resolution) => resolution.childID === child.id)
            observations.push({
              completed: info.time.completed,
              sourceState: source?.state,
              outboxLength: outbox.length,
            })
          }).pipe(Effect.orDie)
        })
        yield* Effect.addFinalizer(() => off)

        const onGlobalEvent = (event: GlobalEvent) => {
          const payload = event.payload
          if (payload?.type !== "message.updated") return
          const properties = payload.properties
          const info = properties?.info
          if (properties?.sessionID !== child.id || info?.id !== assistantSource.source.id) return
          const completed = info.time?.completed
          if (typeof completed !== "number") return
          Deferred.doneUnsafe(
            uiCompleted,
            Effect.succeed({
              eventID: typeof payload.id === "string" ? payload.id : undefined,
              info: info as SessionV1.Assistant,
            }),
          )
        }
        GlobalBus.on("event", onGlobalEvent)
        yield* Effect.addFinalizer(() => Effect.sync(() => GlobalBus.off("event", onGlobalEvent)))

        aFinalGate.resolve()
        const completedAnnouncement = yield* awaitWithTimeout(
          Deferred.await(uiCompleted),
          "timed out waiting for the completed provider announcement",
          "10 seconds",
        )
        expect(completedAnnouncement.eventID).toBeDefined()
        expect(completedAnnouncement.info.id).toBe(MessageID.make(assistantSource.source.id))
        expect(completedAnnouncement.info.time.completed).toBeDefined()
        expect(observations.length).toBeGreaterThan(0)
        expect(
          observations.filter(
            (observation) =>
              observation.completed !== undefined &&
              (observation.sourceState === "reserved" || observation.outboxLength === 0),
          ),
        ).toHaveLength(0)
        expect(
          observations.some(
            (observation) =>
              observation.completed === undefined &&
              observation.sourceState === "reserved" &&
              observation.outboxLength === 0,
          ),
        ).toBe(true)

        const sourceRows = yield* db.db
          .select()
          .from(DelegationSourceTable)
          .where(
            and(
              eq(DelegationSourceTable.session_id, child.id),
              eq(DelegationSourceTable.source_kind, "assistant"),
              eq(DelegationSourceTable.source_id, assistantSource.source.id),
            ),
          )
          .all()
        const resolutionRows = yield* db.db
          .select()
          .from(DelegationResolutionTable)
          .where(
            and(
              eq(DelegationResolutionTable.parent_id, root.id),
              eq(DelegationResolutionTable.child_id, child.id),
              eq(DelegationResolutionTable.source_kind, "assistant"),
              eq(DelegationResolutionTable.source_id, assistantSource.source.id),
            ),
          )
          .all()
        const messageRows = yield* db.db
          .select()
          .from(MessageTable)
          .where(
            and(eq(MessageTable.session_id, child.id), eq(MessageTable.id, MessageID.make(assistantSource.source.id))),
          )
          .all()
        const partRows = yield* db.db
          .select()
          .from(PartTable)
          .where(eq(PartTable.message_id, MessageID.make(assistantSource.source.id)))
          .all()

        expect(sourceRows).toHaveLength(1)
        expect(sourceRows[0]?.state).toBe("finalized")
        expect(sourceRows[0]?.outcome).toBe("reply")
        expect(resolutionRows).toHaveLength(1)
        expect(messageRows).toHaveLength(1)
        const messageData = messageRows[0]?.data
        if (messageData === undefined || messageData.role !== "assistant") {
          throw new Error("expected finalized assistant message")
        }
        expect("completed" in messageData.time ? messageData.time.completed : undefined).toBeDefined()
        expect(
          partRows.some((part) => {
            const data = part.data
            return data.type === "text" && "text" in data && data.text === aResult
          }),
        ).toBe(true)

        rootGate.resolve()
        yield* Fiber.await(rootRun)
        yield* waitForNotBusy(runState, root.id)
        expect(yield* llm.misses).toHaveLength(0)
        expect(yield* llm.pending).toBe(0)
      }).pipe(Effect.ensuring(cleanup))
    }),
  30_000,
)

background.instance(
  "recursive recovery admits recorded returns without replaying unfinished work",
  () =>
    Effect.gen(function* () {
      const modes = ["provider", "tool"] as const
      const rootMarker = "recursive-recovery-admit-root-marker"
      const childMarker = "recursive-recovery-admit-child-marker"
      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        subagent_depth: 3,
        permission: { task: { a: "allow" } },
        agent: {
          build: {
            model: "test/test-model",
            prompt: rootMarker,
          },
          a: {
            mode: "subagent",
            model: "test/test-model",
            prompt: childMarker,
          },
        },
      }))
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const core = yield* DelegationStore.Service
      const adapter = yield* SessionDelegation.Service
      const runState = yield* SessionRunState.Service

      yield* Effect.forEach(
        modes,
        (mode) =>
          Effect.gen(function* () {
            const rootResult = `ROOT_INITIAL_${mode}`
            const root = yield* sessions.create({
              title: `Recovery admission ${mode}`,
              agent: "build",
              model: { providerID: ref.providerID, id: ref.modelID },
            })
            const cleanup = Effect.gen(function* () {
              yield* prompt.cancel(root.id).pipe(Effect.ignore)
            })

            yield* Effect.gen(function* () {
              const callsBeforeCase = yield* llm.calls
              yield* llm.pushMatch(hasSystemMarker(rootMarker), reply().text(rootResult).stop().item())
              const initial = yield* prompt.prompt({
                sessionID: root.id,
                agent: "build",
                model: ref,
                parts: [{ type: "text", text: `Complete the ${mode} recovery setup.` }],
              })
              if (initial.info.role !== "assistant") throw new Error("expected completed root assistant")
              expect((yield* llm.hits).filter(hasSystemMarker(rootMarker))).toHaveLength(callsBeforeCase + 1)
              const recorded = yield* seedRecordedBackgroundFailure(sessions, core, adapter, root.id, initial.info)
              const interrupted = yield* seedInterruptedRootBoundary(mode, sessions, core, root.id, initial.info)
              const callsBeforeRecovery = yield* llm.calls
              expect(callsBeforeRecovery).toBe(callsBeforeCase + 1)

              yield* prompt.recover()
              yield* prompt.recover()

              const incoming = yield* core.incoming(root.id)
              expect(incoming).toHaveLength(1)
              const admitted = incoming[0]
              if (admitted === undefined) throw new Error("expected admitted recorded return")
              expect(admitted.status).toBe("admitted")
              expect(admitted.outcome).toBe("error")
              expect(admitted.payload).toBe(recorded.error)
              expect(admitted.envelope).toBeDefined()
              expect(admitted.envelope?.provenance).toEqual({
                generationID: recorded.registered.generation.id,
                parentID: root.id,
                childID: recorded.child.id,
                origin: recorded.registered.generation.origin,
                source: admitted.source,
                historyCutoff: admitted.historyCutoff,
                consumed: [],
              })
              expect(yield* core.pending(root.id)).toHaveLength(0)
              expect(yield* adapter.blocked(root.id)).toBe(true)

              const messages = yield* sessions.messages({ sessionID: root.id })
              const errors = messages.filter((message) =>
                message.parts.some(
                  (part) => part.type === "text" && part.text === taskError(recorded.child.id, recorded.error),
                ),
              )
              expect(errors).toHaveLength(1)
              const errorMessage = errors[0]
              if (errorMessage === undefined) throw new Error("expected persisted recorded error message")
              expect(errorMessage.info.id).toBe(MessageID.make(admitted.messageID))
              expect(errorMessage.info.role).toBe("user")
              expect(errorMessage.parts.filter((part) => part.type === "text" && part.synthetic === true)).toHaveLength(
                1,
              )

              if (interrupted.mode === "provider") {
                const source = interrupted.source
                const currentSource = (yield* core.sources(root.id)).find((item) => item.id === source.id)
                expect(currentSource).toEqual(source)
                const currentWork = (yield* core.unfinished(root.id)).find((item) => item.id === source.workID)
                expect(currentWork).toEqual(interrupted.work)
                expect(currentSource?.state).toBe("reserved")
                expect(currentWork?.state).toBe("active")
              } else {
                const currentWork = (yield* core.unfinished(root.id)).find((item) => item.id === interrupted.work.id)
                expect(currentWork).toEqual(interrupted.work)
                expect(currentWork?.state).toBe("active")
              }
              expect(yield* runState.busy(root.id)).toBe(false)
              expect(yield* llm.calls).toBe(callsBeforeRecovery)
              expect(yield* llm.misses).toHaveLength(0)
            }).pipe(Effect.ensuring(cleanup))
          }),
        { concurrency: 1, discard: true },
      )
    }),
  30_000,
)

background.instance(
  "recursive recovery wakes recorded returns only when safe",
  () =>
    Effect.gen(function* () {
      const rootMarker = "recursive-recovery-wake-root-marker"
      const rootInitialResult = "ROOT_INITIAL_RECOVERY_WAKE"
      const rootRecoveredResult = "ROOT_RECOVERED"
      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        subagent_depth: 3,
        permission: { task: { a: "allow" } },
        agent: {
          build: {
            model: "test/test-model",
            prompt: rootMarker,
          },
          a: {
            mode: "subagent",
            model: "test/test-model",
            prompt: "recursive-recovery-wake-child-marker",
          },
        },
      }))
      const core = yield* DelegationStore.Service
      const adapter = yield* SessionDelegation.Service
      const prompt = yield* SessionPrompt.Service
      const runState = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const root = yield* sessions.create({
        title: "Recovery wake",
        agent: "build",
        model: { providerID: ref.providerID, id: ref.modelID },
      })
      const cleanup = Effect.gen(function* () {
        yield* prompt.cancel(root.id).pipe(Effect.ignore)
      })

      yield* Effect.gen(function* () {
        yield* llm.pushMatch(hasSystemMarker(rootMarker), reply().text(rootInitialResult).stop().item())
        const initial = yield* prompt.prompt({
          sessionID: root.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "Complete the recovery wake setup." }],
        })
        if (initial.info.role !== "assistant") throw new Error("expected completed root assistant")
        const recorded = yield* seedRecordedBackgroundFailure(sessions, core, adapter, root.id, initial.info)
        expect(yield* adapter.blocked(root.id)).toBe(false)
        const pendingBeforeRecovery = yield* core.pending(root.id)
        expect(pendingBeforeRecovery).toHaveLength(1)
        const recordedResolution = pendingBeforeRecovery[0]
        if (recordedResolution === undefined) throw new Error("expected pending recorded return")
        yield* llm.pushMatch(hasSystemMarker(rootMarker), reply().text(rootRecoveredResult).stop().item())

        const callsBeforeRecovery = yield* llm.calls
        yield* prompt.recover()
        const recoveredHit = yield* waitForSystem(llm, rootMarker, 2)
        expect(
          messageContents(recoveredHit).filter((content) => content === taskError(recorded.child.id, recorded.error)),
        ).toHaveLength(1)
        expect(yield* llm.calls).toBe(callsBeforeRecovery + 1)
        const recoveredSource = yield* waitForFinalizedAssistantSource(core, root.id, rootRecoveredResult)
        const resolved = yield* awaitWithTimeout(
          Effect.gen(function* () {
            while (true) {
              const current = yield* core.getResolution(recordedResolution.id)
              if (current?.status === "resolved") return current
              yield* Effect.yieldNow
            }
          }),
          "timed out waiting for the recorded return to resolve",
          "10 seconds",
        )
        expect(resolved.status).toBe("resolved")
        expect(resolved.resolvedSourceID).toBe(recoveredSource.id)
        expect(recoveredSource.consumed).toEqual([recordedResolution.id])
        expect((yield* core.getResolution(recordedResolution.id))?.status).toBe("resolved")
        yield* waitForNotBusy(runState, root.id)

        const messagesBeforeRepeat = yield* sessions.messages({ sessionID: root.id })
        expect(
          messagesBeforeRepeat.filter((message) =>
            message.parts.some(
              (part) => part.type === "text" && part.text === taskError(recorded.child.id, recorded.error),
            ),
          ),
        ).toHaveLength(1)
        const callsAfterRecovery = yield* llm.calls
        yield* prompt.recover()
        expect(yield* llm.calls).toBe(callsAfterRecovery)
        const messagesAfterRepeat = yield* sessions.messages({ sessionID: root.id })
        expect(
          messagesAfterRepeat.filter((message) =>
            message.parts.some(
              (part) => part.type === "text" && part.text === taskError(recorded.child.id, recorded.error),
            ),
          ),
        ).toHaveLength(1)
        expect(
          (yield* core.sources(root.id)).filter(
            (source) =>
              source.state === "finalized" &&
              source.source.kind === "assistant" &&
              source.payload === rootRecoveredResult,
          ),
        ).toHaveLength(1)
        yield* awaitWithTimeout(
          Effect.gen(function* () {
            while ((yield* core.get(recorded.registered.generation.id))?.state !== "closed") yield* Effect.yieldNow
          }),
          "timed out waiting for the recorded child generation to close",
          "10 seconds",
        )
        expect((yield* core.get(recorded.registered.generation.id))?.state).toBe("closed")
        expect(yield* llm.misses).toHaveLength(0)
        expect(yield* llm.pending).toBe(0)
      }).pipe(Effect.ensuring(cleanup))
    }),
  30_000,
)

background.instance(
  "recursive recovery ignores historical unregistered children",
  () =>
    Effect.gen(function* () {
      const rootMarker = "recursive-recovery-historical-root-marker"
      const rootResult = "ROOT_INITIAL_HISTORICAL"
      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        subagent_depth: 3,
        agent: {
          build: {
            model: "test/test-model",
            prompt: rootMarker,
          },
          a: {
            mode: "subagent",
            model: "test/test-model",
            prompt: "recursive-recovery-historical-child-marker",
          },
        },
      }))
      const core = yield* DelegationStore.Service
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const root = yield* sessions.create({
        title: "Historical recovery",
        agent: "build",
        model: { providerID: ref.providerID, id: ref.modelID },
      })
      const cleanup = Effect.gen(function* () {
        yield* prompt.cancel(root.id).pipe(Effect.ignore)
      })

      yield* Effect.gen(function* () {
        yield* llm.pushMatch(hasSystemMarker(rootMarker), reply().text(rootResult).stop().item())
        const initial = yield* prompt.prompt({
          sessionID: root.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "Complete the historical recovery setup." }],
        })
        if (initial.info.role !== "assistant") throw new Error("expected completed root assistant")
        const callsBeforeRecovery = yield* llm.calls
        const child = yield* sessions.create({
          parentID: root.id,
          title: "Historical child",
          agent: "a",
          model: { providerID: ref.providerID, id: ref.modelID },
        })
        const historicalUser = yield* sessions.updateMessage({
          id: MessageID.ascending(),
          role: "user",
          sessionID: child.id,
          agent: "a",
          model: ref,
          time: { created: Date.now() },
        })
        yield* sessions.updatePart({
          id: PartID.ascending(),
          messageID: historicalUser.id,
          sessionID: child.id,
          type: "text",
          text: "Historical child input",
        })
        const historicalAssistant = yield* sessions.updateMessage({
          id: MessageID.ascending(),
          role: "assistant",
          parentID: historicalUser.id,
          sessionID: child.id,
          mode: "a",
          agent: "a",
          cost: 0,
          path: { cwd: "/tmp", root: "/tmp" },
          tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
          modelID: ref.modelID,
          providerID: ref.providerID,
          time: { created: Date.now(), completed: Date.now() },
          finish: "stop",
        })
        yield* sessions.updatePart({
          id: PartID.ascending(),
          messageID: historicalAssistant.id,
          sessionID: child.id,
          type: "text",
          text: "HISTORICAL_CHILD_RESULT",
        })
        const rootMessagesBeforeRecovery = yield* sessions.messages({ sessionID: root.id })
        expect(rootMessagesBeforeRecovery).toHaveLength(2)
        expect(yield* core.active(child.id)).toBeUndefined()
        expect(yield* core.listActive()).toEqual([])
        expect(yield* core.incoming(root.id)).toEqual([])

        yield* prompt.recover()

        expect(yield* llm.calls).toBe(callsBeforeRecovery)
        expect(yield* llm.misses).toHaveLength(0)
        expect(yield* llm.pending).toBe(0)
        expect(yield* core.listActive()).toEqual([])
        expect(yield* core.incoming(root.id)).toEqual([])
        expect(yield* core.active(child.id)).toBeUndefined()
        const rootMessagesAfterRecovery = yield* sessions.messages({ sessionID: root.id })
        expect(rootMessagesAfterRecovery).toHaveLength(rootMessagesBeforeRecovery.length)
        expect(
          rootMessagesAfterRecovery
            .flatMap((message) => message.parts)
            .filter((part) => part.type === "text" && part.synthetic === true && part.text.includes("<task>")),
        ).toHaveLength(0)
        const childMessages = yield* sessions.messages({ sessionID: child.id })
        expect(
          childMessages
            .flatMap((message) => message.parts)
            .filter((part) => part.type === "text" && part.text === "HISTORICAL_CHILD_RESULT"),
        ).toHaveLength(1)
        expect(yield* sessions.children(root.id)).toEqual([expect.objectContaining({ id: child.id })])
      }).pipe(Effect.ensuring(cleanup))
    }),
  30_000,
)

background.instance(
  "recursive delayed return survives newer answered input",
  () =>
    Effect.gen(function* () {
      const rootMarker = "recursive-delayed-newer-root-marker"
      const initialResult = "ROOT_INITIAL_DELAYED_NEWER"
      const newerResult = "ROOT_NEWER_ANSWERED"
      const afterResult = "ROOT_AFTER_DELAY"
      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        subagent_depth: 3,
        permission: { task: { a: "allow" } },
        agent: {
          build: {
            model: "test/test-model",
            prompt: rootMarker,
          },
          a: {
            mode: "subagent",
            model: "test/test-model",
            prompt: "recursive-delayed-newer-child-marker",
          },
        },
      }))
      const core = yield* DelegationStore.Service
      const adapter = yield* SessionDelegation.Service
      const prompt = yield* SessionPrompt.Service
      const runState = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const root = yield* sessions.create({
        title: "Delayed return after newer input",
        agent: "build",
        model: { providerID: ref.providerID, id: ref.modelID },
      })
      const afterGate = Promise.withResolvers<void>()
      const cleanup = Effect.gen(function* () {
        afterGate.resolve()
        yield* prompt.cancel(root.id).pipe(Effect.ignore)
      })

      yield* Effect.gen(function* () {
        yield* llm.pushMatch(
          hasSystemMarker(rootMarker),
          reply().text(initialResult).stop().item(),
          reply().text(newerResult).stop().item(),
        )
        const initial = yield* prompt.prompt({
          sessionID: root.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "Complete the delayed-return setup." }],
        })
        if (initial.info.role !== "assistant") throw new Error("expected completed root assistant")
        const recorded = yield* seedRecordedBackgroundFailure(sessions, core, adapter, root.id, initial.info)
        const pendingBeforeNewer = yield* core.pending(root.id)
        expect(pendingBeforeNewer).toHaveLength(1)
        const delayed = pendingBeforeNewer[0]
        if (delayed === undefined) throw new Error("expected delayed return before newer input")

        const newer = yield* prompt.prompt({
          sessionID: root.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "Answer this newer root input." }],
        })
        expect(newer.info.role).toBe("assistant")
        expect((yield* core.pending(root.id)).map((resolution) => resolution.id)).toEqual([delayed.id])
        const messagesBeforeWake = yield* sessions.messages({ sessionID: root.id })
        expect(
          messagesBeforeWake.filter((message) =>
            message.parts.some(
              (part) => part.type === "text" && part.text === taskError(recorded.child.id, recorded.error),
            ),
          ),
        ).toHaveLength(0)
        const newerUser = messagesBeforeWake.findLast(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text === "Answer this newer root input."),
        )
        if (newerUser === undefined) throw new Error("expected newer root user message")
        expect(newerUser.info.time.created).toBeGreaterThan(delayed.timeCreated)

        yield* llm.pushMatch(
          hasSystemMarker(rootMarker),
          reply().text(afterResult).stop().wait(afterGate.promise).item(),
        )
        const callsBeforeWake = yield* llm.calls
        yield* prompt.wake(root.id)
        const afterHit = yield* waitForSystem(llm, rootMarker, 3)
        expect(
          messageContents(afterHit).filter((content) => content === taskError(recorded.child.id, recorded.error)),
        ).toHaveLength(1)
        expect(yield* llm.calls).toBe(callsBeforeWake + 1)

        const admitted = yield* core.getResolution(delayed.id)
        if (admitted === undefined || admitted.envelope === undefined) {
          throw new Error("expected admitted delayed return envelope")
        }
        expect(admitted.status).toBe("consumed")
        const delayedMessage = (yield* sessions.messages({ sessionID: root.id })).find((message) =>
          message.parts.some(
            (part) => part.type === "text" && part.text === taskError(recorded.child.id, recorded.error),
          ),
        )
        if (delayedMessage === undefined) throw new Error("expected delayed return message after wake")
        expect(delayedMessage.info.time.created).toBe(delayed.timeCreated)
        const envelope = admitted.envelope

        afterGate.resolve()
        const afterSource = yield* waitForFinalizedAssistantSource(core, root.id, afterResult)
        const resolved = yield* awaitWithTimeout(
          Effect.gen(function* () {
            while (true) {
              const current = yield* core.getResolution(delayed.id)
              if (current?.status === "resolved") return current
              yield* Effect.yieldNow
            }
          }),
          "timed out waiting for delayed return resolution",
          "10 seconds",
        )
        expect(resolved.resolvedSourceID).toBe(afterSource.id)
        expect(resolved.envelope).toEqual(envelope)
        expect(resolved.timeCreated).toBe(delayed.timeCreated)
        expect(afterSource.consumed).toEqual([delayed.id])
        yield* waitForNotBusy(runState, root.id)
        expect((yield* core.sources(root.id)).filter((source) => source.payload === afterResult)).toHaveLength(1)
        expect(yield* llm.misses).toHaveLength(0)
        expect(yield* llm.pending).toBe(0)
        expect((yield* llm.hits).filter(hasSystemMarker(rootMarker))).toHaveLength(3)
      }).pipe(Effect.ensuring(cleanup))
    }),
  30_000,
)

background.instance(
  "recursive delayed return survives completed compaction",
  () =>
    Effect.gen(function* () {
      const rootMarker = "recursive-delayed-compaction-root-marker"
      const initialResult = "ROOT_INITIAL_DELAYED_COMPACTION"
      const newerResult = "ROOT_NEWER_BEFORE_COMPACTION"
      const summaryResult = "COMPACTION_SUMMARY_DELAYED"
      const afterResult = "ROOT_AFTER_COMPACTION_DELAY"
      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        compaction: { tail_turns: 1 },
        subagent_depth: 3,
        permission: { task: { a: "allow" } },
        agent: {
          build: {
            model: "test/test-model",
            prompt: rootMarker,
          },
          a: {
            mode: "subagent",
            model: "test/test-model",
            prompt: "recursive-delayed-compaction-child-marker",
          },
        },
      }))
      const events = yield* EventV2Bridge.Service
      const compact = yield* SessionCompaction.Service
      const core = yield* DelegationStore.Service
      const adapter = yield* SessionDelegation.Service
      const prompt = yield* SessionPrompt.Service
      const runState = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const root = yield* sessions.create({
        title: "Delayed return through compaction",
        agent: "build",
        model: { providerID: ref.providerID, id: ref.modelID },
      })
      const afterGate = Promise.withResolvers<void>()
      let compacted = 0
      const cleanup = Effect.gen(function* () {
        afterGate.resolve()
        yield* prompt.cancel(root.id).pipe(Effect.ignore)
      })

      yield* Effect.gen(function* () {
        yield* llm.pushMatch(
          hasSystemMarker(rootMarker),
          reply().text(initialResult).stop().item(),
          reply().text(newerResult).stop().item(),
        )
        const initial = yield* prompt.prompt({
          sessionID: root.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "Complete the delayed compaction setup." }],
        })
        if (initial.info.role !== "assistant") throw new Error("expected completed root assistant")
        const recorded = yield* seedRecordedBackgroundFailure(sessions, core, adapter, root.id, initial.info)
        const pendingBeforeNewer = yield* core.pending(root.id)
        expect(pendingBeforeNewer).toHaveLength(1)
        const delayed = pendingBeforeNewer[0]
        if (delayed === undefined) throw new Error("expected delayed return before newer input")

        const newer = yield* prompt.prompt({
          sessionID: root.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "Answer before compaction." }],
        })
        expect(newer.info.role).toBe("assistant")
        const newerUser = (yield* sessions.messages({ sessionID: root.id })).findLast(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text === "Answer before compaction."),
        )
        if (newerUser === undefined) throw new Error("expected newer user before compaction")
        expect(newerUser.info.time.created).toBeGreaterThan(delayed.timeCreated)
        expect(yield* core.pending(root.id)).toEqual([delayed])

        const off = yield* events.listen((event) => {
          if (event.type !== SessionCompaction.Event.Compacted.type) return Effect.void
          const data = event.data as typeof SessionCompaction.Event.Compacted.data.Type
          if (data.sessionID === root.id) compacted++
          return Effect.void
        })
        yield* Effect.addFinalizer(() => off)
        yield* llm.push(reply().text(summaryResult).stop().item())
        yield* compact.create({
          sessionID: root.id,
          agent: "build",
          model: ref,
          auto: false,
          overflow: false,
        })
        const compactedAssistant = yield* prompt.loop({ sessionID: root.id })
        expect(compactedAssistant.info.role).toBe("assistant")
        if (compactedAssistant.info.role === "assistant") expect(compactedAssistant.info.summary).toBe(true)
        expect(compacted).toBe(1)
        expect(yield* core.pending(root.id)).toEqual([delayed])
        const compactionMessages = yield* sessions.messages({ sessionID: root.id })
        const compactionParts = compactionMessages
          .flatMap((message) => message.parts)
          .filter((part): part is SessionV1.CompactionPart => part.type === "compaction")
        expect(compactionParts).toHaveLength(1)
        expect(compactionParts[0]?.tail_start_id).toBe(newerUser.info.id)
        expect(
          compactionMessages.filter((message) => message.info.role === "assistant" && message.info.summary === true),
        ).toHaveLength(1)

        const delivered = yield* adapter.deliver(root.id)
        expect(delivered).toEqual([root.id])
        const admitted = yield* core.getResolution(delayed.id)
        if (admitted === undefined || admitted.envelope === undefined) {
          throw new Error("expected admitted delayed compaction return")
        }
        expect(admitted.status).toBe("admitted")
        const filtered = yield* MessageV2.filterCompactedEffect(root.id)
        expect(
          filtered
            .flatMap((message) => message.parts)
            .filter((part) => part.type === "text" && part.text === taskError(recorded.child.id, recorded.error)),
        ).toHaveLength(0)
        expect(yield* llm.calls).toBe(3)

        yield* llm.pushMatch(
          hasSystemMarker(rootMarker),
          reply().text(afterResult).stop().wait(afterGate.promise).item(),
        )
        yield* prompt.wake(root.id)
        const afterHit = yield* waitForSystem(llm, rootMarker, 3)
        expect(
          messageContents(afterHit).filter((content) => content === taskError(recorded.child.id, recorded.error)),
        ).toHaveLength(1)
        expect(yield* llm.calls).toBe(4)

        afterGate.resolve()
        const afterSource = yield* waitForFinalizedAssistantSource(core, root.id, afterResult)
        const resolved = yield* awaitWithTimeout(
          Effect.gen(function* () {
            while (true) {
              const current = yield* core.getResolution(delayed.id)
              if (current?.status === "resolved") return current
              yield* Effect.yieldNow
            }
          }),
          "timed out waiting for delayed compaction return resolution",
          "10 seconds",
        )
        expect(resolved.resolvedSourceID).toBe(afterSource.id)
        expect(resolved.envelope).toEqual(admitted.envelope)
        expect(resolved.timeCreated).toBe(delayed.timeCreated)
        expect(afterSource.consumed).toEqual([delayed.id])
        yield* waitForNotBusy(runState, root.id)
        expect(compacted).toBe(1)
        expect(
          (yield* sessions.messages({ sessionID: root.id })).filter(
            (message) => message.info.role === "assistant" && message.info.summary === true,
          ),
        ).toHaveLength(1)
        const hits = yield* llm.hits
        expect(hits.filter(hasSystemMarker(rootMarker))).toHaveLength(3)
        expect(yield* llm.misses).toHaveLength(0)
        expect(yield* llm.pending).toBe(0)
      }).pipe(Effect.ensuring(cleanup))
    }),
  30_000,
)

const delegatedCompactionScenarios = [
  { name: "no-tail", overflow: false, suffix: "NO_TAIL" },
  { name: "overflow-replay", overflow: true, suffix: "OVERFLOW_REPLAY" },
] as const

const runDelegatedCompactionProvenance = (scenario: (typeof delegatedCompactionScenarios)[number]) =>
  Effect.gen(function* () {
    const rootMarker = `recursive-delegated-compaction-${scenario.name}-root-marker`
    const aMarker = `recursive-delegated-compaction-${scenario.name}-a-marker`
    const bMarker = `recursive-delegated-compaction-${scenario.name}-b-marker`
    const summaryResult = `COMPACTION_SUMMARY_${scenario.suffix}`
    const aAfterCompactResult = `A_AFTER_COMPACT_${scenario.suffix}`
    const aAfterLaterResult = `A_AFTER_LATER_${scenario.suffix}`
    const aAfterBResult = `A_AFTER_B_${scenario.suffix}`
    const bResult = `B_RESULT_${scenario.suffix}`
    const rootAfterResult = `ROOT_AFTER_DELEGATED_COMPACTION_${scenario.suffix}`
    const isCompactionRequest = (hit: TestLLMHit) =>
      JSON.stringify(hit.body).includes("Create a new anchored summary from the conversation history")

    const { llm } = yield* useServerConfig((url) => ({
      ...providerCfg(url),
      compaction: { tail_turns: 0 },
      subagent_depth: 3,
      permission: { task: { a: "allow" } },
      agent: {
        build: {
          model: "test/test-model",
          prompt: rootMarker,
        },
        a: {
          mode: "subagent",
          model: "test/test-model",
          prompt: aMarker,
          permission: { task: { b: "allow" } },
        },
        b: {
          mode: "subagent",
          model: "test/test-model",
          prompt: bMarker,
        },
      },
    }))
    const db = yield* Database.Service
    const events = yield* EventV2Bridge.Service
    const jobs = yield* BackgroundJob.Service
    const compact = yield* SessionCompaction.Service
    const core = yield* DelegationStore.Service
    const prompt = yield* SessionPrompt.Service
    const runState = yield* SessionRunState.Service
    const sessions = yield* Session.Service
    const root = yield* sessions.create({
      title: `Delegated compaction provenance ${scenario.name}`,
      agent: "build",
      model: { providerID: ref.providerID, id: ref.modelID },
    })
    const a = yield* sessions.create({
      parentID: root.id,
      title: `Resumable agent A ${scenario.name}`,
      agent: "a",
      model: { providerID: ref.providerID, id: ref.modelID },
      permission: [{ permission: "task", pattern: "b", action: "allow" }],
    })
    const rootGate = Promise.withResolvers<void>()
    const aAfterCompactGate = Promise.withResolvers<void>()
    const aAfterLaterGate = Promise.withResolvers<void>()
    const bGate = Promise.withResolvers<void>()
    const inputReceipt = yield* Deferred.make<{
      readonly messageID: string
      readonly partID: string
      readonly workID: Delegation.WorkID
    }>()
    const compactionCreated = yield* Deferred.make<void>()
    let compactionRequested = false

    const cleanup = Effect.gen(function* () {
      rootGate.resolve()
      aAfterCompactGate.resolve()
      aAfterLaterGate.resolve()
      bGate.resolve()
      yield* prompt.cancel(root.id).pipe(Effect.ignore)
    })

    yield* Effect.gen(function* () {
      yield* llm.pushMatch(isCompactionRequest, reply().text(summaryResult).stop().item())
      yield* llm.pushMatch(
        hasSystemMarker(rootMarker),
        reply()
          .tool("task", {
            description: "resume agent a",
            prompt: "A_REPLAY_INPUT",
            subagent_type: "a",
            task_id: a.id,
            background: true,
          })
          .item(),
        reply().text("ROOT_WAITING_FOR_DELEGATED_COMPACTION").stop().wait(rootGate.promise).item(),
        reply().text(rootAfterResult).stop().item(),
      )
      yield* llm.pushMatch(
        hasSystemMarker(aMarker),
        reply().text("A_HISTORY").stop().item(),
        reply()
          .tool("task", {
            description: "launch agent b",
            prompt: "B_INPUT",
            subagent_type: "b",
            background: true,
          })
          .item(),
        reply().text(aAfterCompactResult).stop().wait(aAfterCompactGate.promise).item(),
        reply().text(aAfterLaterResult).stop().wait(aAfterLaterGate.promise).item(),
        reply().text(aAfterBResult).stop().item(),
      )
      yield* llm.pushMatch(hasSystemMarker(bMarker), reply().text(bResult).stop().wait(bGate.promise).item())

      const history = yield* prompt.prompt({
        sessionID: a.id,
        agent: "a",
        model: ref,
        parts: [{ type: "text", text: "A_HISTORY" }],
      })
      expect(history.info.role).toBe("assistant")
      expect(history.parts.some((part) => part.type === "text" && part.text === "A_HISTORY")).toBe(true)
      expect(yield* core.active(a.id)).toBeUndefined()
      expect(yield* core.unfinished(a.id)).toEqual([])

      const off = yield* events.listen((event) => {
        if (event.type !== MessageV2.Event.PartUpdated.type) return Effect.void
        const data = event.data as typeof MessageV2.Event.PartUpdated.data.Type
        if (
          data.sessionID !== a.id ||
          data.part.type !== "text" ||
          data.part.id !== `prt_input_${data.part.messageID}` ||
          data.part.text !== "" ||
          data.part.synthetic !== true ||
          data.part.ignored !== true
        ) {
          return Effect.void
        }
        const metadata = Option.getOrUndefined(decodeDelegationInputMetadata(data.part.metadata))
        if (metadata === undefined || compactionRequested) return Effect.void
        compactionRequested = true
        Deferred.doneUnsafe(
          inputReceipt,
          Effect.succeed({
            messageID: String(data.part.messageID),
            partID: String(data.part.id),
            workID: metadata.delegationInput.workID,
          }),
        )
        return compact
          .create({
            sessionID: a.id,
            agent: "a",
            model: ref,
            auto: true,
            overflow: scenario.overflow,
          })
          .pipe(Effect.andThen(Effect.sync(() => Deferred.doneUnsafe(compactionCreated, Effect.void))), Effect.orDie)
      })
      yield* Effect.addFinalizer(() => off)

      const rootRun = yield* prompt
        .prompt({
          sessionID: root.id,
          agent: "build",
          model: ref,
          parts: [{ type: "text", text: "Begin delegated compaction provenance." }],
        })
        .pipe(Effect.forkChild)

      yield* waitForSystem(llm, rootMarker, 2)
      const observedInput = yield* awaitWithTimeout(
        Deferred.await(inputReceipt),
        "timed out waiting for the delegated input receipt",
        "10 seconds",
      )
      yield* awaitWithTimeout(
        Deferred.await(compactionCreated),
        "timed out waiting for delegated compaction creation",
        "10 seconds",
      )
      expect(compactionRequested).toBe(true)

      const aGeneration = yield* pollWithTimeout(
        core.active(a.id),
        "timed out waiting for the adopted A generation",
        "10 seconds",
      )
      expect(aGeneration.parentID).toBe(root.id)
      expect(aGeneration.childID).toBe(a.id)
      expect(aGeneration.parentGenerationID).toBeUndefined()
      expect(aGeneration.mode).toBe("background")

      const rootTaskMessage = (yield* sessions.messages({ sessionID: root.id })).findLast(
        (message) =>
          message.info.role === "assistant" &&
          message.parts.some((part) => part.type === "tool" && part.tool === "task"),
      )
      if (rootTaskMessage === undefined || rootTaskMessage.info.role !== "assistant") {
        throw new Error("expected persisted root task origin message")
      }
      const rootTaskPart = rootTaskMessage.parts.find(
        (part): part is SessionV1.ToolPart =>
          part.type === "tool" && part.tool === "task" && part.state.status === "completed",
      )
      if (rootTaskPart === undefined) throw new Error("expected completed root task origin part")
      expect(rootTaskPart.state.input).toMatchObject({ task_id: a.id, background: true })
      expect(String(aGeneration.origin.messageID)).toBe(String(rootTaskMessage.info.id))
      expect(String(aGeneration.origin.partID)).toBe(String(rootTaskPart.id))
      expect(aGeneration.origin.callID).toBe(rootTaskPart.callID)
      expect((yield* sessions.children(root.id)).filter((child) => child.id === a.id)).toHaveLength(1)

      const registrations = yield* db.db
        .select()
        .from(DelegationRegistrationTable)
        .where(eq(DelegationRegistrationTable.generation_id, aGeneration.id))
        .all()
      expect(registrations).toHaveLength(1)
      const registration = registrations[0]
      if (registration === undefined) throw new Error("expected explicit reuse registration")
      expect(registration.request.explicitReuse).toBe(true)
      expect(registration.request.parentID).toBe(root.id)
      expect(registration.request.childID).toBe(a.id)

      const aMessages = yield* sessions.messages({ sessionID: a.id })
      const originalInput = aMessages.find((message) => String(message.info.id) === observedInput.messageID)
      if (originalInput === undefined || originalInput.info.role !== "user") {
        throw new Error("expected original delegated input message")
      }
      const originalReceipt = originalInput.parts.find((part) => String(part.id) === observedInput.partID)
      if (originalReceipt === undefined || originalReceipt.type !== "text") {
        throw new Error("expected original delegated input receipt")
      }
      const originalText = originalInput.parts.find(
        (part): part is SessionV1.TextPart => part.type === "text" && part.text === "A_REPLAY_INPUT",
      )
      if (originalText === undefined) throw new Error("expected original delegated input text")

      const originalWork = yield* pollWithTimeout(
        core.unfinished(a.id).pipe(Effect.map((works) => works.find((work) => work.id === observedInput.workID))),
        "timed out waiting for original delegated work to remain active",
        "10 seconds",
      )
      expect(originalWork.kind).toBe("launch")
      expect(originalWork.state).toBe("active")
      expect(originalWork.id).toBe(observedInput.workID)

      const summaryMessage = yield* pollWithTimeout(
        sessions
          .messages({ sessionID: a.id })
          .pipe(
            Effect.map((messages) =>
              messages.find(
                (message) =>
                  message.info.role === "assistant" &&
                  message.info.summary === true &&
                  message.parts.some((part) => part.type === "text" && part.text === summaryResult),
              ),
            ),
          ),
        "timed out waiting for delegated compaction summary",
        "10 seconds",
      )
      expect(summaryMessage.info.role).toBe("assistant")
      if (summaryMessage.info.role !== "assistant") throw new Error("expected delegated compaction assistant")
      expect(summaryMessage.info.summary).toBe(true)

      const compactionMarker = yield* pollWithTimeout(
        core
          .sources(a.id)
          .pipe(
            Effect.map((sources) =>
              sources.find(
                (source) =>
                  source.state === "discarded" &&
                  source.source.kind === "terminal" &&
                  source.source.id.startsWith("delegation-compaction:"),
              ),
            ),
          ),
        "timed out waiting for discarded compaction marker",
        "10 seconds",
      )
      const markerCutoff = yield* decodeDelegationHistoryCutoff(compactionMarker.historyCutoff)
      expect(markerCutoff.messages).toContain(observedInput.messageID)
      expect(markerCutoff.work).toEqual([originalWork.id])
      expect(compactionMarker.consumed).toEqual([])

      yield* waitForSystem(llm, aMarker, 3)
      yield* waitForSystem(llm, bMarker)
      const b = (yield* sessions.children(a.id)).find((child) => child.agent === "b")
      if (b === undefined) throw new Error("expected B child session")
      const aAfterCompactSource = yield* pollWithTimeout(
        core
          .sources(a.id)
          .pipe(
            Effect.map((sources) =>
              sources.find((source) => source.state === "reserved" && source.source.kind === "assistant"),
            ),
          ),
        "timed out waiting for A_AFTER_COMPACT source reservation",
        "10 seconds",
      )
      const afterCompactCutoff = yield* decodeDelegationHistoryCutoff(aAfterCompactSource.historyCutoff)
      expect(afterCompactCutoff.work).toEqual([originalWork.id])
      expect(afterCompactCutoff.messages).not.toContain(observedInput.messageID)
      expect((yield* core.unfinished(a.id)).find((work) => work.id === originalWork.id)?.state).toBe("active")

      const selected = yield* MessageV2.filterCompactedEffect(a.id)
      const selectedParts = selected.flatMap((message) => message.parts)
      expect(selectedParts.find((part) => String(part.id) === observedInput.partID)).toBeUndefined()
      if (!scenario.overflow) {
        expect(selectedParts.some((part) => part.type === "text" && part.text === "A_REPLAY_INPUT")).toBe(false)
      } else {
        const replayed = selected.find(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text === "A_REPLAY_INPUT"),
        )
        if (replayed === undefined || replayed.info.role !== "user") {
          throw new Error("expected overflow replay user message")
        }
        const replayText = replayed.parts.find(
          (part): part is SessionV1.TextPart => part.type === "text" && part.text === "A_REPLAY_INPUT",
        )
        const replayReceipt = replayed.parts.find(
          (part): part is SessionV1.TextPart =>
            part.type === "text" && part.text === "" && part.synthetic === true && part.ignored === true,
        )
        if (replayText === undefined || replayReceipt === undefined) {
          throw new Error("expected copied overflow replay parts")
        }
        expect(replayed.info.id).not.toBe(originalInput.info.id)
        expect(replayText.id).not.toBe(originalText.id)
        expect(replayReceipt.id).not.toBe(originalReceipt.id)
        expect(replayReceipt.messageID).toBe(replayed.info.id)
        const replayMetadata = Option.getOrUndefined(decodeDelegationInputMetadata(replayReceipt.metadata))
        expect(replayMetadata?.delegationInput.workID).toBe(originalWork.id)
      }

      const later = yield* prompt.prompt({
        sessionID: a.id,
        agent: "a",
        model: ref,
        noReply: true,
        parts: [{ type: "text", text: "A_LATER_INPUT" }],
      })
      if (later.info.role !== "user") throw new Error("expected later A user input")
      const laterReceipt = later.parts.find(
        (part): part is SessionV1.TextPart => part.type === "text" && String(part.id) === `prt_input_${later.info.id}`,
      )
      if (laterReceipt === undefined) throw new Error("expected later A input receipt")
      const laterMetadata = Option.getOrUndefined(decodeDelegationInputMetadata(laterReceipt.metadata))
      if (laterMetadata === undefined) throw new Error("expected later A input work metadata")
      const laterWorkID = laterMetadata.delegationInput.workID
      expect(laterWorkID).not.toBe(originalWork.id)
      const laterWork = yield* pollWithTimeout(
        core.unfinished(a.id).pipe(Effect.map((works) => works.find((work) => work.id === laterWorkID))),
        "timed out waiting for later A input work",
        "10 seconds",
      )
      expect(laterWork.kind).toBe("input")
      expect(laterWork.state).toBe("active")
      expect((yield* core.unfinished(a.id)).find((work) => work.id === originalWork.id)?.state).toBe("active")

      aAfterCompactGate.resolve()
      const aAfterCompactFinal = yield* waitForFinalizedAssistantSource(core, a.id, aAfterCompactResult)
      expect(aAfterCompactFinal.id).toBe(aAfterCompactSource.id)
      expect((yield* core.unfinished(a.id)).find((work) => work.id === originalWork.id)).toBeUndefined()
      expect((yield* core.unfinished(a.id)).find((work) => work.id === laterWorkID)?.state).toBe("active")

      yield* waitForSystem(llm, aMarker, 4)
      const aAfterLaterSource = yield* pollWithTimeout(
        core
          .sources(a.id)
          .pipe(
            Effect.map((sources) =>
              sources.find((source) => source.state === "reserved" && source.source.kind === "assistant"),
            ),
          ),
        "timed out waiting for A_AFTER_LATER source reservation",
        "10 seconds",
      )
      const afterLaterCutoff = yield* decodeDelegationHistoryCutoff(aAfterLaterSource.historyCutoff)
      expect(afterLaterCutoff.work).toEqual([laterWorkID])
      expect(afterLaterCutoff.work).not.toContain(originalWork.id)
      expect((yield* core.unfinished(a.id)).find((work) => work.id === laterWorkID)?.state).toBe("active")

      aAfterLaterGate.resolve()
      const aAfterLaterFinal = yield* waitForFinalizedAssistantSource(core, a.id, aAfterLaterResult)
      expect(aAfterLaterFinal.id).toBe(aAfterLaterSource.id)
      yield* waitForNotBusy(runState, a.id)
      const aJob = yield* jobs.wait({ id: a.id, timeout: 10_000 })
      expect(aJob.timedOut).toBe(false)
      expect(aJob.info?.status).toBe("completed")
      expect((yield* core.unfinished(a.id)).find((work) => work.id === laterWorkID)).toBeUndefined()
      expect((yield* core.unfinished(a.id)).find((work) => work.id === originalWork.id)).toBeUndefined()

      bGate.resolve()
      const bSource = yield* waitForFinalizedAssistantSource(core, b.id, bResult)
      const bResolution = yield* pollWithTimeout(
        db.db
          .select({ id: DelegationResolutionTable.id })
          .from(DelegationResolutionTable)
          .where(
            and(
              eq(DelegationResolutionTable.parent_id, a.id),
              eq(DelegationResolutionTable.source_id, bSource.source.id),
            ),
          )
          .get()
          .pipe(
            Effect.flatMap((row) =>
              row === undefined
                ? Effect.succeed(undefined)
                : core
                    .getResolution(row.id)
                    .pipe(
                      Effect.map((resolution) =>
                        resolution === undefined || resolution.status === "pending" ? undefined : resolution,
                      ),
                    ),
            ),
          ),
        "timed out waiting for B resolution admission",
        "10 seconds",
      )
      expect(bResolution.parentID).toBe(a.id)
      expect(bResolution.childID).toBe(b.id)
      yield* waitForSystem(llm, aMarker, 5)
      const aAfterBSource = yield* waitForFinalizedAssistantSource(core, a.id, aAfterBResult)
      expect(aAfterBSource.consumed).toEqual([bResolution.id])
      const bResolved = yield* pollWithTimeout(
        core
          .getResolution(bResolution.id)
          .pipe(Effect.map((resolution) => (resolution?.status === "resolved" ? resolution : undefined))),
        "timed out waiting for B resolution to resolve",
        "10 seconds",
      )
      expect(bResolved.resolvedSourceID).toBe(aAfterBSource.id)

      const finalizedA = (yield* core.sources(a.id)).filter(
        (source) =>
          source.state === "finalized" && source.source.kind === "assistant" && source.generationID === aGeneration.id,
      )
      expect(finalizedA.map((source) => source.payload)).toEqual([
        aAfterCompactResult,
        aAfterLaterResult,
        aAfterBResult,
      ])
      const rootResolutionRows = yield* pollWithTimeout(
        db.db
          .select({
            sourceID: DelegationResolutionTable.source_id,
            sourceKind: DelegationResolutionTable.source_kind,
            childID: DelegationResolutionTable.child_id,
            status: DelegationResolutionTable.status,
          })
          .from(DelegationResolutionTable)
          .where(eq(DelegationResolutionTable.parent_id, root.id))
          .all()
          .pipe(Effect.map((rows) => (rows.length === 3 ? rows : undefined))),
        "timed out waiting for all A results at root",
        "10 seconds",
      )
      expect(rootResolutionRows.every((row) => row.childID === a.id && row.sourceKind === "assistant")).toBe(true)
      expect(rootResolutionRows.some((row) => row.sourceID === compactionMarker.source.id)).toBe(false)
      expect(rootResolutionRows.map((row) => row.sourceID).toSorted()).toEqual(
        finalizedA.map((source) => source.source.id).toSorted(),
      )
      expect(rootResolutionRows.every((row) => row.status !== "pending")).toBe(true)

      rootGate.resolve()
      const rootAfterHit = yield* waitForSystem(llm, rootMarker, 3)
      const rootAfterContents = messageContents(rootAfterHit)
      expect(rootAfterContents.filter((content) => content === taskResult(a.id, aAfterCompactResult))).toHaveLength(1)
      expect(rootAfterContents.filter((content) => content === taskResult(a.id, aAfterLaterResult))).toHaveLength(1)
      expect(rootAfterContents.filter((content) => content === taskResult(a.id, aAfterBResult))).toHaveLength(1)
      expect(rootAfterContents.filter((content) => content === taskResult(b.id, bResult))).toHaveLength(0)
      const rootRunExit = yield* Fiber.await(rootRun)
      expect(Exit.isSuccess(rootRunExit)).toBe(true)
      yield* waitForNotBusy(runState, root.id)

      const rootMessages = yield* sessions.messages({ sessionID: root.id })
      expect(
        rootMessages.filter(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text === taskResult(a.id, aAfterCompactResult)),
        ),
      ).toHaveLength(1)
      expect(
        rootMessages.filter(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text === taskResult(a.id, aAfterLaterResult)),
        ),
      ).toHaveLength(1)
      expect(
        rootMessages.filter(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text === taskResult(a.id, aAfterBResult)),
        ),
      ).toHaveLength(1)
      expect(
        rootMessages.filter(
          (message) =>
            message.info.role === "user" &&
            message.parts.some((part) => part.type === "text" && part.text === taskResult(b.id, bResult)),
        ),
      ).toHaveLength(0)

      const hits = yield* llm.hits
      expect(hits.filter(hasSystemMarker(rootMarker))).toHaveLength(3)
      expect(hits.filter(hasSystemMarker(aMarker))).toHaveLength(5)
      expect(hits.filter(hasSystemMarker(bMarker))).toHaveLength(1)
      expect(hits.filter(isCompactionRequest)).toHaveLength(1)
      expect(yield* llm.misses).toHaveLength(0)
      expect(yield* llm.pending).toBe(0)
    }).pipe(Effect.ensuring(cleanup))
  })

for (const scenario of delegatedCompactionScenarios) {
  background.instance(
    `recursive delegated compaction provenance (${scenario.name})`,
    () => runDelegatedCompactionProvenance(scenario),
    30_000,
  )
}

background.instance(
  "recursive child-only stop notifies active parent once",
  () =>
    Effect.gen(function* () {
      const rootMarker = "recursive-child-stop-root-marker"
      const aMarker = "recursive-child-stop-a-marker"
      const bMarker = "recursive-child-stop-b-marker"
      const aInitialResult = "A_INITIAL_BEFORE_CHILD_STOP"
      const rootInitialResult = "ROOT_INITIAL_BEFORE_CHILD_STOP"
      const rootCancelAck = "ROOT_CANCEL_ACK"
      const { llm } = yield* useServerConfig((url) => ({
        ...providerCfg(url),
        subagent_depth: 3,
        permission: { task: { a: "allow" } },
        agent: {
          build: {
            model: "test/test-model",
            prompt: rootMarker,
          },
          a: {
            mode: "subagent",
            model: "test/test-model",
            prompt: aMarker,
            permission: { task: { b: "allow" } },
          },
          b: {
            mode: "subagent",
            model: "test/test-model",
            prompt: bMarker,
          },
        },
      }))
      const db = yield* Database.Service
      const jobs = yield* BackgroundJob.Service
      const delegation = yield* DelegationStore.Service
      const prompt = yield* SessionPrompt.Service
      const runState = yield* SessionRunState.Service
      const sessions = yield* Session.Service
      const root = yield* sessions.create({
        title: "Recursive child-only stop",
        agent: "build",
        model: { providerID: ref.providerID, id: ref.modelID },
      })
      const bGate = Promise.withResolvers<void>()
      const rootBeforeGate = Promise.withResolvers<void>()
      const rootAfterCancelGate = Promise.withResolvers<void>()

      const cleanup = Effect.gen(function* () {
        bGate.resolve()
        rootBeforeGate.resolve()
        rootAfterCancelGate.resolve()
        yield* prompt.cancel(root.id).pipe(Effect.ignore)
      })

      yield* Effect.gen(function* () {
        yield* llm.pushMatch(
          hasSystemMarker(rootMarker),
          reply()
            .tool("task", {
              description: "launch agent a",
              prompt: "Investigate A before child-only stop",
              subagent_type: "a",
              background: true,
            })
            .item(),
          reply().text("ROOT_WAITING_FOR_A").stop().wait(rootBeforeGate.promise).item(),
          reply().text(rootInitialResult).stop().item(),
          reply().text(rootCancelAck).stop().wait(rootAfterCancelGate.promise).item(),
        )
        yield* llm.pushMatch(
          hasSystemMarker(aMarker),
          reply()
            .tool("task", {
              description: "launch agent b",
              prompt: "Investigate B before child-only stop",
              subagent_type: "b",
              background: true,
            })
            .item(),
          reply().text(aInitialResult).stop().item(),
        )
        yield* llm.pushMatch(
          hasSystemMarker(bMarker),
          reply().text("B_HELD_BEFORE_CHILD_STOP").stop().wait(bGate.promise).item(),
        )

        const rootRun = yield* prompt
          .prompt({
            sessionID: root.id,
            agent: "build",
            model: ref,
            parts: [{ type: "text", text: "Begin the child-only stop chain." }],
          })
          .pipe(Effect.forkChild)

        yield* waitForSystem(llm, rootMarker, 2)
        yield* waitForSystem(llm, aMarker, 2)
        yield* waitForSystem(llm, bMarker)

        const rootChildren = yield* sessions.children(root.id)
        const a = rootChildren.find((child) => child.agent === "a")
        if (a === undefined) throw new Error("expected A child session")
        const aChildren = yield* sessions.children(a.id)
        const b = aChildren.find((child) => child.agent === "b")
        if (b === undefined) throw new Error("expected B child session")
        const aGeneration = yield* delegation.active(a.id)
        const bGeneration = yield* delegation.active(b.id)
        if (aGeneration === undefined || bGeneration === undefined) {
          throw new Error("expected active A and B generations")
        }
        expect(aGeneration.parentID).toBe(root.id)
        expect(bGeneration.parentID).toBe(a.id)
        expect(bGeneration.parentGenerationID).toBe(aGeneration.id)

        const aWaited = yield* jobs.wait({ id: a.id, timeout: 10_000 })
        expect(aWaited.timedOut).toBe(false)
        expect(aWaited.info?.status).toBe("completed")
        const aInitialSource = yield* waitForFinalizedAssistantSource(delegation, a.id, aInitialResult)
        expect((yield* jobs.get(b.id))?.status).toBe("running")
        expect(yield* runState.busy(a.id)).toBe(false)
        expect(yield* runState.busy(b.id)).toBe(true)

        rootBeforeGate.resolve()
        const rootInitialHit = yield* waitForSystem(llm, rootMarker, 3)
        expect(
          messageContents(rootInitialHit).filter((content) => content === taskResult(a.id, aInitialResult)),
        ).toHaveLength(1)
        const rootRunExit = yield* Fiber.await(rootRun)
        expect(Exit.isSuccess(rootRunExit)).toBe(true)
        yield* waitForNotBusy(runState, root.id)
        const callsBeforeCancel = yield* llm.calls
        const rootMessagesBeforeCancel = yield* sessions.messages({ sessionID: root.id })

        yield* prompt.cancel(a.id)

        expect((yield* delegation.get(aGeneration.id))?.state).toBe("closed")
        expect((yield* delegation.get(bGeneration.id))?.state).toBe("revoked")
        const bWaited = yield* jobs.wait({ id: b.id, timeout: 10_000 })
        expect(bWaited.timedOut).toBe(false)
        expect(bWaited.info?.status).toBe("cancelled")
        yield* waitForNotBusy(runState, a.id)
        yield* waitForNotBusy(runState, b.id)
        expect(yield* runState.busy(root.id)).toBe(true)

        const fences = yield* db.db
          .select({ sessionID: DelegationRevocationTable.session_id, id: DelegationRevocationTable.id })
          .from(DelegationRevocationTable)
          .where(inArray(DelegationRevocationTable.session_id, [root.id, a.id, b.id]))
          .all()
        expect(fences).toHaveLength(2)
        expect(fences.map((fence) => fence.sessionID).sort()).toEqual([a.id, b.id].sort())
        expect(new Set(fences.map((fence) => fence.id)).size).toBe(1)

        const aSources = yield* delegation.sources(a.id)
        const cancellationSources = aSources.filter(
          (source) => source.source.kind === "terminal" && source.payload === "Task cancelled",
        )
        expect(cancellationSources).toHaveLength(1)
        const cancellationSource = cancellationSources[0]
        if (cancellationSource === undefined) throw new Error("expected stable A cancellation source")
        expect(cancellationSource.state).toBe("finalized")
        expect(cancellationSource.outcome).toBe("cancelled")
        expect(aInitialSource.state).toBe("finalized")

        const rootResolution = (yield* delegation.incoming(root.id)).find((resolution) => resolution.childID === a.id)
        if (rootResolution === undefined) throw new Error("expected one A cancellation resolution for root")
        expect(rootResolution.status).toBe("admitted")
        expect(rootResolution.envelope).toBeDefined()

        const cancellationHit = yield* waitForSystem(llm, rootMarker, 4)
        const cancellationContents = messageContents(cancellationHit)
        expect(cancellationContents.filter((content) => content === taskCancelled(a.id))).toHaveLength(1)
        expect(cancellationContents.filter((content) => content === taskCancelled(b.id))).toHaveLength(0)
        expect(cancellationContents.filter((content) => content === "Task cancelled")).toHaveLength(0)
        expect(yield* llm.calls).toBe(callsBeforeCancel + 1)
        expect((yield* delegation.getResolution(rootResolution.id))?.status).toBe("consumed")

        yield* prompt.cancel(a.id)
        expect(yield* llm.calls).toBe(callsBeforeCancel + 1)
        expect((yield* delegation.sources(a.id)).filter((source) => source.source.kind === "terminal")).toHaveLength(1)
        expect(
          (yield* sessions.messages({ sessionID: root.id })).filter((message) =>
            message.parts.some((part) => part.type === "text" && part.text === taskCancelled(a.id)),
          ),
        ).toHaveLength(1)
        expect(yield* runState.busy(root.id)).toBe(true)

        rootAfterCancelGate.resolve()
        const rootCancelSource = yield* waitForFinalizedAssistantSource(delegation, root.id, rootCancelAck)
        expect(rootCancelSource.outcome).toBe("reply")
        yield* waitForNotBusy(runState, root.id)
        const resolution = yield* delegation.getResolution(rootResolution.id)
        expect(resolution?.status).toBe("resolved")
        expect(resolution?.resolvedSourceID).toBe(rootCancelSource.id)
        expect(yield* delegation.incoming(root.id)).toHaveLength(0)
        expect(
          yield* db.db
            .select({ sessionID: DelegationRevocationTable.session_id })
            .from(DelegationRevocationTable)
            .where(eq(DelegationRevocationTable.session_id, root.id))
            .all(),
        ).toHaveLength(0)

        const rootMessagesAfterCancel = yield* sessions.messages({ sessionID: root.id })
        expect(rootMessagesAfterCancel.length).toBeGreaterThan(rootMessagesBeforeCancel.length)
        expect(
          rootMessagesAfterCancel.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text === taskCancelled(a.id)),
          ),
        ).toHaveLength(1)
        expect(
          rootMessagesAfterCancel.filter(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text === taskCancelled(b.id)),
          ),
        ).toHaveLength(0)
        expect(yield* llm.calls).toBe(callsBeforeCancel + 1)
        expect(yield* llm.misses).toHaveLength(0)
        expect(yield* llm.pending).toBe(0)
        const hits = yield* llm.hits
        expect(hits.filter(hasSystemMarker(rootMarker))).toHaveLength(4)
        expect(hits.filter(hasSystemMarker(aMarker))).toHaveLength(2)
        expect(hits.filter(hasSystemMarker(bMarker))).toHaveLength(1)
        expect(yield* runState.busy(root.id)).toBe(false)
      }).pipe(Effect.ensuring(cleanup))
    }),
  30_000,
)

it.instance("assertNotBusy fails with BusyError when loop running", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const run = yield* SessionRunState.Service
    const sessions = yield* Session.Service
    yield* llm.hang

    const chat = yield* sessions.create({})
    yield* user(chat.id, "hi")

    const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
    yield* llm.wait(1)
    yield* waitForBusy(chat.id)

    const exit = yield* run.assertNotBusy(chat.id).pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      expect(Cause.squash(exit.cause)).toBeInstanceOf(Session.BusyError)
      expect(Cause.squash(exit.cause)).toMatchObject({ _tag: "SessionBusyError", sessionID: chat.id })
    }

    yield* prompt.cancel(chat.id)
    yield* Fiber.await(fiber)
  }),
)

noLLMServer.instance("assertNotBusy succeeds when idle", () =>
  Effect.gen(function* () {
    const run = yield* SessionRunState.Service
    const sessions = yield* Session.Service

    const chat = yield* sessions.create({})
    const exit = yield* run.assertNotBusy(chat.id).pipe(Effect.exit)
    expect(Exit.isSuccess(exit)).toBe(true)
  }),
)

// Shell semantics

it.instance("shell rejects with BusyError when loop running", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* llm.hang
    yield* user(chat.id, "hi")

    const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
    yield* llm.wait(1)
    yield* waitForBusy(chat.id)

    const exit = yield* prompt.shell({ sessionID: chat.id, agent: "build", command: "echo hi" }).pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      expect(Cause.squash(exit.cause)).toBeInstanceOf(Session.BusyError)
      expect(Cause.squash(exit.cause)).toMatchObject({ _tag: "SessionBusyError", sessionID: chat.id })
    }

    yield* prompt.cancel(chat.id)
    yield* Fiber.await(fiber)
  }),
)

unixNoLLMServer(
  "shell captures stdout and stderr in completed tool output",
  () =>
    Effect.gen(function* () {
      const { prompt, run, chat } = yield* boot()
      const result = yield* prompt.shell({
        sessionID: chat.id,
        agent: "build",
        command: "printf out && printf err >&2",
      })

      expect(result.info.role).toBe("assistant")
      const tool = completedTool(result.parts)
      if (!tool) return

      expect(tool.state.output).toContain("out")
      expect(tool.state.output).toContain("err")
      expect(tool.state.metadata.output).toContain("out")
      expect(tool.state.metadata.output).toContain("err")
      yield* run.assertNotBusy(chat.id)
    }),
  { config: cfg },
)

unixNoLLMServer(
  "shell completes a fast command on the preferred shell",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const { prompt, run, chat } = yield* boot()
      const result = yield* prompt.shell({
        sessionID: chat.id,
        agent: "build",
        command: "pwd",
      })

      expect(result.info.role).toBe("assistant")
      const tool = completedTool(result.parts)
      if (!tool) return

      expect(tool.state.input.command).toBe("pwd")
      expect(tool.state.output).toContain(dir)
      expect(tool.state.metadata.output).toContain(dir)
      yield* run.assertNotBusy(chat.id)
    }),
  { config: cfg },
)

unixNoLLMServer(
  "shell uses configured shell over env shell",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        if (!(yield* hasBash)) return

        const { prompt, chat } = yield* boot()
        const result = yield* prompt.shell({
          sessionID: chat.id,
          agent: "build",
          command: "[[ 1 -eq 1 ]] && printf configured",
        })

        const tool = completedTool(result.parts)
        if (!tool) return
        expect(tool.state.output).toContain("configured")
      }),
    ),
  { config: { ...cfg, shell: "bash" } },
  30_000,
)

unixNoLLMServer(
  "shell commands can change directory after startup",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { directory: dir } = yield* TestInstance
        const { prompt, run, chat } = yield* boot()
        const parent = path.dirname(dir)
        const result = yield* prompt.shell({
          sessionID: chat.id,
          agent: "build",
          command: "cd .. && pwd",
        })

        expect(result.info.role).toBe("assistant")
        const tool = completedTool(result.parts)
        if (!tool) return

        expect(tool.state.output).toContain(parent)
        expect(tool.state.metadata.output).toContain(parent)
        yield* run.assertNotBusy(chat.id)
      }),
    ),
  { config: cfg },
)

unixNoLLMServer(
  "shell lists files from the project directory",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const { prompt, run, chat } = yield* boot()
      yield* writeText(path.join(dir, "README.md"), "# e2e\n")

      const result = yield* prompt.shell({
        sessionID: chat.id,
        agent: "build",
        command: "command ls",
      })

      expect(result.info.role).toBe("assistant")
      const tool = completedTool(result.parts)
      if (!tool) return

      expect(tool.state.input.command).toBe("command ls")
      expect(tool.state.output).toContain("README.md")
      expect(tool.state.metadata.output).toContain("README.md")
      yield* run.assertNotBusy(chat.id)
    }),
  { config: cfg },
)

unixNoLLMServer(
  "shell captures stderr from a failing command",
  () =>
    Effect.gen(function* () {
      const { prompt, run, chat } = yield* boot()
      const result = yield* prompt.shell({
        sessionID: chat.id,
        agent: "build",
        command: "command -v __nonexistent_cmd_e2e__ || echo 'not found' >&2; exit 1",
      })

      expect(result.info.role).toBe("assistant")
      const tool = completedTool(result.parts)
      if (!tool) return

      expect(tool.state.output).toContain("not found")
      expect(tool.state.metadata.output).toContain("not found")
      yield* run.assertNotBusy(chat.id)
    }),
  { config: cfg },
)

unixNoLLMServer(
  "shell updates running metadata before process exit",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { prompt, chat } = yield* boot()

        const fiber = yield* prompt
          .shell({ sessionID: chat.id, agent: "build", command: "printf first && sleep 0.2 && printf second" })
          .pipe(Effect.forkChild)

        yield* pollWithTimeout(
          Effect.gen(function* () {
            const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
            const taskMsg = msgs.find((item) => item.info.role === "assistant")
            const tool = taskMsg ? toolPart(taskMsg.parts) : undefined
            if (tool?.state.status === "running" && tool.state.metadata?.output.includes("first")) return true
          }),
          "timed out waiting for running shell metadata",
        )

        const exit = yield* Fiber.await(fiber)
        expect(Exit.isSuccess(exit)).toBe(true)
      }),
    ),
  { config: cfg },
  30_000,
)

it.instance(
  "loop waits while shell runs and starts after shell exits",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* llm.text("after-shell")

      const sh = yield* prompt
        .shell({ sessionID: chat.id, agent: "build", command: "sleep 0.2" })
        .pipe(Effect.forkChild)
      yield* waitForBusy(chat.id)

      const loop = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.sleep(50)

      expect(yield* llm.calls).toBe(0)

      yield* Fiber.await(sh)
      const exit = yield* Fiber.await(loop)

      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) {
        expect(exit.value.info.role).toBe("assistant")
        expect(exit.value.parts.some((part) => part.type === "text" && part.text === "after-shell")).toBe(true)
      }
      expect(yield* llm.calls).toBe(1)
    }),
  { git: true },
  10_000,
)

it.instance(
  "shell completion resumes queued loop callers",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* llm.text("done")

      const sh = yield* prompt
        .shell({ sessionID: chat.id, agent: "build", command: "sleep 0.2" })
        .pipe(Effect.forkChild)
      yield* waitForBusy(chat.id)

      const a = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      const b = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.sleep(50)

      expect(yield* llm.calls).toBe(0)

      yield* Fiber.await(sh)
      const [ea, eb] = yield* Effect.all([Fiber.await(a), Fiber.await(b)])

      expect(Exit.isSuccess(ea)).toBe(true)
      expect(Exit.isSuccess(eb)).toBe(true)
      if (Exit.isSuccess(ea) && Exit.isSuccess(eb)) {
        expect(ea.value.info.id).toBe(eb.value.info.id)
        expect(ea.value.info.role).toBe("assistant")
      }
      expect(yield* llm.calls).toBe(1)
    }),
  { git: true },
  10_000,
)

unix(
  "command ! expansion uses configured shell over env shell",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        if (!(yield* hasBash)) return
        const { llm } = yield* useServerConfig((url) => ({
          ...providerCfg(url),
          shell: "bash",
          command: {
            probe: {
              template: "Probe: !`[[ 1 -eq 1 ]] && printf configured`",
            },
          },
        }))

        const { prompt, chat } = yield* boot()
        yield* llm.text("done")

        const result = yield* prompt.command({
          sessionID: chat.id,
          command: "probe",
          arguments: "",
        })

        expect(result.info.role).toBe("assistant")
        const inputs = yield* llm.inputs
        expect(JSON.stringify(inputs.at(-1)?.messages)).toContain("configured")
      }),
    ),
  30_000,
)

unixNoLLMServer(
  "cancel interrupts shell and resolves cleanly",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { prompt, run, chat } = yield* boot()
        const { directory: dir } = yield* TestInstance
        const afs = yield* FSUtil.Service
        const ready = path.join(dir, ".shell-ready")

        const sh = yield* prompt
          .shell({ sessionID: chat.id, agent: "build", command: ": > '.shell-ready'; sleep 30" })
          .pipe(Effect.forkChild)
        yield* pollWithTimeout(
          afs.existsSafe(ready).pipe(Effect.map((exists) => (exists ? (true as const) : undefined))),
          "shell never created readiness marker",
        )

        yield* prompt.cancel(chat.id)

        const status = yield* SessionStatus.Service
        expect((yield* status.get(chat.id)).type).toBe("idle")
        const busy = yield* run.assertNotBusy(chat.id).pipe(Effect.exit)
        expect(Exit.isSuccess(busy)).toBe(true)

        const exit = yield* Fiber.await(sh)
        expect(Exit.isSuccess(exit)).toBe(true)
        if (Exit.isSuccess(exit)) {
          expect(exit.value.info.role).toBe("assistant")
          const tool = completedTool(exit.value.parts)
          if (tool) {
            expect(tool.state.output).toContain("User aborted the command")
          }
        }
      }),
    ),
  { git: true, config: cfg },
  30_000,
)

unixNoLLMServer(
  "concurrent cancellation and deletion await shell finalization",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { prompt, chat } = yield* boot()
        const db = yield* Database.Service
        const events = yield* EventV2Bridge.Service
        const sessions = yield* Session.Service
        const shell = yield* prompt
          .shell({ sessionID: chat.id, agent: "build", command: "printf cancellation-ready; sleep 30" })
          .pipe(Effect.forkChild)
        const shellReady = yield* pollWithTimeout(
          Effect.gen(function* () {
            const messages = yield* sessions.messages({ sessionID: chat.id })
            const assistant = messages.findLast((message) => message.info.role === "assistant")
            const tool = assistant ? toolPart(assistant.parts) : undefined
            if (
              tool?.state.status === "running" &&
              typeof tool.state.metadata?.output === "string" &&
              tool.state.metadata.output.includes("cancellation-ready")
            ) {
              return assistant
            }
          }),
          "timed out waiting for persisted running shell output",
        )
        if (shellReady === undefined || shellReady.info.role !== "assistant") {
          throw new Error("expected running shell assistant")
        }

        const finalizerReached = yield* Deferred.make<void>()
        const finalizerGate = yield* Deferred.make<void>()
        const finalPartSeen = yield* Deferred.make<{ readonly sessionExists: boolean }>()
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            yield* Deferred.succeed(finalizerGate, void 0)
            yield* prompt.cancel(chat.id).pipe(Effect.ignore)
          }),
        )
        const off = yield* events.listen((event) => {
          if (event.type === MessageV2.Event.Updated.type) {
            const data = event.data as typeof MessageV2.Event.Updated.data.Type
            if (
              data.sessionID !== chat.id ||
              data.info.role !== "assistant" ||
              data.info.id !== shellReady.info.id ||
              data.info.time.completed === undefined
            ) {
              return Effect.void
            }
            Deferred.doneUnsafe(finalizerReached, Effect.void)
            return Deferred.await(finalizerGate)
          }
          if (event.type !== MessageV2.Event.PartUpdated.type) return Effect.void
          const data = event.data as typeof MessageV2.Event.PartUpdated.data.Type
          if (
            data.sessionID !== chat.id ||
            data.part.messageID !== shellReady.info.id ||
            data.part.type !== "tool" ||
            data.part.state.status !== "completed"
          ) {
            return Effect.void
          }
          return Effect.gen(function* () {
            const row = yield* db.db
              .select({ id: SessionTable.id })
              .from(SessionTable)
              .where(eq(SessionTable.id, chat.id))
              .get()
            Deferred.doneUnsafe(finalPartSeen, Effect.succeed({ sessionExists: row !== undefined }))
          }).pipe(Effect.orDie)
        })
        yield* Effect.addFinalizer(() => off)

        const firstCancelDone = yield* Deferred.make<void>()
        const firstCancel = yield* prompt
          .cancel(chat.id)
          .pipe(Effect.ensuring(Deferred.succeed(firstCancelDone, void 0)))
          .pipe(Effect.forkChild)
        yield* awaitWithTimeout(
          Deferred.await(finalizerReached),
          "timed out waiting for shell finalizer MessageUpdated",
          "10 seconds",
        )

        const secondCancelDone = yield* Deferred.make<void>()
        const secondCancel = yield* prompt
          .cancel(chat.id)
          .pipe(Effect.ensuring(Deferred.succeed(secondCancelDone, void 0)))
          .pipe(Effect.forkChild)
        const removeDone = yield* Deferred.make<void>()
        const remove = yield* sessions
          .remove(chat.id)
          .pipe(Effect.ensuring(Deferred.succeed(removeDone, void 0)))
          .pipe(Effect.forkChild)
        yield* Effect.yieldNow

        expect(yield* Deferred.isDone(firstCancelDone)).toBe(false)
        expect(yield* Deferred.isDone(secondCancelDone)).toBe(false)
        expect(yield* Deferred.isDone(removeDone)).toBe(false)
        expect(yield* Deferred.isDone(finalPartSeen)).toBe(false)
        const beforeRelease = yield* awaitWithTimeout(
          sessions.messages({ sessionID: chat.id }),
          "timed out reading shell messages before finalization release",
          "5 seconds",
        )
        const beforeAssistant = beforeRelease.findLast((message) => message.info.id === shellReady.info.id)
        expect(beforeAssistant).toBeDefined()
        expect(
          yield* awaitWithTimeout(
            db.db.select({ id: SessionTable.id }).from(SessionTable).where(eq(SessionTable.id, chat.id)).all(),
            "timed out reading session before finalization release",
            "5 seconds",
          ),
        ).toHaveLength(1)

        yield* Deferred.succeed(finalizerGate, void 0)
        const finalPart = yield* awaitWithTimeout(
          Deferred.await(finalPartSeen),
          "timed out waiting for final shell tool part",
          "10 seconds",
        )
        expect(finalPart.sessionExists).toBe(true)

        const [firstExit, secondExit, removeExit, shellExit] = yield* Effect.all([
          Fiber.await(firstCancel),
          Fiber.await(secondCancel),
          Fiber.await(remove),
          Fiber.await(shell),
        ])
        expect(Exit.isSuccess(firstExit)).toBe(true)
        expect(Exit.isSuccess(secondExit)).toBe(true)
        expect(Exit.isSuccess(removeExit)).toBe(true)
        expect(Exit.isSuccess(shellExit)).toBe(true)
        expect(yield* Deferred.isDone(firstCancelDone)).toBe(true)
        expect(yield* Deferred.isDone(secondCancelDone)).toBe(true)
        expect(yield* Deferred.isDone(removeDone)).toBe(true)
        if (Exit.isSuccess(shellExit)) {
          const tool = completedTool(shellExit.value.parts)
          expect(tool).toBeDefined()
          if (tool) expect(tool.state.output).toContain("cancellation-ready")
        }
        expect(
          yield* db.db.select({ id: SessionTable.id }).from(SessionTable).where(eq(SessionTable.id, chat.id)).all(),
        ).toHaveLength(0)
      }),
    ),
  { git: true, config: cfg },
  30_000,
)

unixNoLLMServer(
  "cancel persists aborted shell result when shell ignores TERM",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { prompt, chat } = yield* boot()
        const { directory: dir } = yield* TestInstance
        const afs = yield* FSUtil.Service
        const ready = path.join(dir, ".trap-ready")

        const sh = yield* prompt
          .shell({
            sessionID: chat.id,
            agent: "build",
            // Touch marker AFTER trap installs so the test waits for the actual
            // ignore-TERM state before cancelling; otherwise SIGTERM can arrive
            // before `trap` runs and the escalation path is never exercised.
            command: `trap '' TERM; touch "${ready}"; sleep 30`,
          })
          .pipe(Effect.forkChild)

        yield* Effect.gen(function* () {
          while (!(yield* afs.existsSafe(ready))) {
            yield* Effect.sleep(Duration.millis(10))
          }
        }).pipe(Effect.timeout(Duration.seconds(5)))

        yield* prompt.cancel(chat.id)

        const exit = yield* Fiber.await(sh)
        expect(Exit.isSuccess(exit)).toBe(true)
        if (Exit.isSuccess(exit)) {
          expect(exit.value.info.role).toBe("assistant")
          const tool = completedTool(exit.value.parts)
          if (tool) {
            expect(tool.state.output).toContain("User aborted the command")
          }
        }
      }),
    ),
  { git: true, config: cfg },
  30_000,
)

unix(
  "cancel finalizes interrupted bash tool output through normal truncation",
  () =>
    Effect.gen(function* () {
      const { dir, llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Interrupted bash truncation",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })

      yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
        parts: [{ type: "text", text: "run bash" }],
      })

      yield* llm.tool("bash", {
        command:
          'i=0; while [ "$i" -lt 4000 ]; do printf "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx %05d\\n" "$i"; i=$((i + 1)); done; printf truncation-ready; sleep 30',
        timeout: 30_000,
        workdir: path.resolve(dir),
      })

      const run = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* llm.wait(1)
      yield* pollWithTimeout(
        Effect.gen(function* () {
          const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
          const assistant = msgs.findLast((item) => item.info.role === "assistant")
          const tool = assistant ? toolPart(assistant.parts) : undefined
          if (tool?.state.status === "running" && tool.state.metadata?.output.includes("truncation-ready")) return true
        }),
        "timed out waiting for truncated shell output",
      )
      yield* prompt.cancel(chat.id)

      const exit = yield* Fiber.await(run)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isFailure(exit)) return

      const tool = completedTool(exit.value.parts)
      if (!tool) return

      expect(tool.state.metadata.truncated).toBe(true)
      expect(typeof tool.state.metadata.outputPath).toBe("string")
      expect(tool.state.output).toMatch(/\.\.\.output truncated\.\.\./)
      expect(tool.state.output).toMatch(/Full output saved to:\s+\S+/)
      expect(tool.state.output).not.toContain("Tool execution aborted")
    }),
  { git: true },
  30_000,
)

unixNoLLMServer(
  "cancel interrupts loop queued behind shell",
  () =>
    Effect.gen(function* () {
      const { prompt, chat } = yield* boot()

      const sh = yield* prompt.shell({ sessionID: chat.id, agent: "build", command: "sleep 30" }).pipe(Effect.forkChild)
      yield* waitForBusy(chat.id)

      const loop = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.sleep(50)

      yield* prompt.cancel(chat.id)

      const exit = yield* Fiber.await(loop)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) {
        const tool = completedTool(exit.value.parts)
        expect(tool?.state.output).toContain("User aborted the command")
      }

      yield* Fiber.await(sh)
    }),
  { git: true, config: cfg },
  30_000,
)

unixNoLLMServer(
  "shell rejects when another shell is already running",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { prompt, chat } = yield* boot()

        const a = yield* prompt
          .shell({ sessionID: chat.id, agent: "build", command: "sleep 30" })
          .pipe(Effect.forkChild)
        yield* waitForBusy(chat.id)

        const exit = yield* prompt.shell({ sessionID: chat.id, agent: "build", command: "echo hi" }).pipe(Effect.exit)
        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          expect(Cause.squash(exit.cause)).toBeInstanceOf(Session.BusyError)
        }

        yield* prompt.cancel(chat.id)
        yield* Fiber.await(a)
      }),
    ),
  { git: true, config: cfg },
  30_000,
)

// Abort signal propagation tests for inline tool execution

function hangUntilAborted(tool: { execute: (...args: any[]) => any }) {
  return Effect.gen(function* () {
    const ready = yield* Deferred.make<void>()
    const aborted = yield* Deferred.make<void>()
    const original = tool.execute
    tool.execute = (_args: any, ctx: any) => {
      ctx.abort.addEventListener("abort", () => succeedVoid(aborted), { once: true })
      if (ctx.abort.aborted) succeedVoid(aborted)
      succeedVoid(ready)
      return Effect.callback<never>(() => Effect.sync(() => succeedVoid(aborted)))
    }
    const restore = Effect.addFinalizer(() => Effect.sync(() => void (tool.execute = original)))
    return { ready, aborted, restore }
  })
}

noLLMServer.instance(
  "interrupt propagates abort signal to read tool via file part (text/plain)",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const registry = yield* ToolRegistry.Service
      const { read } = yield* registry.named()
      const { ready, restore } = yield* hangUntilAborted(read)
      yield* restore

      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Abort Test" })

      const testFile = path.join(dir, "test.txt")
      yield* writeText(testFile, "hello world")

      const fiber = yield* prompt
        .prompt({
          sessionID: chat.id,
          agent: "build",
          parts: [
            { type: "text", text: "read this" },
            { type: "file", url: `file://${testFile}`, filename: "test.txt", mime: "text/plain" },
          ],
        })
        .pipe(Effect.forkChild)

      yield* awaitWithTimeout(Deferred.await(ready), "timed out waiting for read tool to start", "10 seconds")
      yield* prompt.cancel(chat.id)
      yield* Fiber.interrupt(fiber)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  { config: cfg },
  30_000,
)

noLLMServer.instance(
  "interrupt propagates abort signal to read tool via file part (directory)",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const registry = yield* ToolRegistry.Service
      const { read } = yield* registry.named()
      const { ready, restore } = yield* hangUntilAborted(read)
      yield* restore

      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Abort Test" })

      const fiber = yield* prompt
        .prompt({
          sessionID: chat.id,
          agent: "build",
          parts: [
            { type: "text", text: "read this" },
            { type: "file", url: `file://${dir}`, filename: "dir", mime: "application/x-directory" },
          ],
        })
        .pipe(Effect.forkChild)

      yield* awaitWithTimeout(Deferred.await(ready), "timed out waiting for read tool to start", "10 seconds")
      yield* prompt.cancel(chat.id)
      yield* Fiber.interrupt(fiber)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  { config: cfg },
  30_000,
)

// Missing file handling

noLLMServer.instance(
  "does not fail the prompt when a file part is missing",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})

      const missing = path.join(dir, "does-not-exist.ts")
      const msg = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        noReply: true,
        parts: [
          { type: "text", text: "please review @does-not-exist.ts" },
          {
            type: "file",
            mime: "text/plain",
            url: `file://${missing}`,
            filename: "does-not-exist.ts",
          },
        ],
      })

      if (msg.info.role !== "user") throw new Error("expected user message")
      const hasFailure = msg.parts.some(
        (part) => part.type === "text" && part.synthetic && part.text.includes("Read tool failed to read"),
      )
      expect(hasFailure).toBe(true)

      yield* sessions.remove(session.id)
    }),
  { config: cfg },
)

noLLMServer.instance(
  "keeps stored part order stable when file resolution is async",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})

      const missing = path.join(dir, "still-missing.ts")
      const msg = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        noReply: true,
        parts: [
          {
            type: "file",
            mime: "text/plain",
            url: `file://${missing}`,
            filename: "still-missing.ts",
          },
          { type: "text", text: "after-file" },
        ],
      })

      if (msg.info.role !== "user") throw new Error("expected user message")

      const stored = yield* MessageV2.get({
        sessionID: session.id,
        messageID: msg.info.id,
      })
      const text = stored.parts.filter((part) => part.type === "text").map((part) => part.text)

      expect(text[0]?.startsWith("Called the Read tool with the following input:")).toBe(true)
      expect(text[1]?.includes("Read tool failed to read")).toBe(true)
      expect(text[2]).toBe("after-file")

      yield* sessions.remove(session.id)
    }),
  { config: cfg },
)

// Special characters in filenames

noLLMServer.instance(
  "handles filenames with # character",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      yield* writeText(path.join(dir, "file#name.txt"), "special content\n")

      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const parts = yield* prompt.resolvePromptParts("Read @file#name.txt")
      const fileParts = parts.filter((part) => part.type === "file")

      expect(fileParts.length).toBe(1)
      expect(fileParts[0].filename).toBe("file#name.txt")
      expect(fileParts[0].url).toContain("%23")

      const decodedPath = fileURLToPath(fileParts[0].url)
      expect(decodedPath).toBe(path.join(dir, "file#name.txt"))

      const message = yield* prompt.prompt({
        sessionID: session.id,
        parts,
        noReply: true,
      })
      const stored = yield* MessageV2.get({ sessionID: session.id, messageID: message.info.id })
      const textParts = stored.parts.filter((part) => part.type === "text")
      const hasContent = textParts.some((part) => part.text.includes("special content"))
      expect(hasContent).toBe(true)

      yield* sessions.remove(session.id)
    }),
  { git: true, config: cfg },
)

// Regression: empty assistant turn loop

it.instance("does not loop empty assistant turns for a simple reply", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({ title: "Prompt regression" })

    yield* llm.text("packages/opencode/src/session/processor.ts")

    const result = yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      parts: [{ type: "text", text: "Where is SessionProcessor?" }],
    })

    expect(result.info.role).toBe("assistant")
    expect(result.parts.some((part) => part.type === "text" && part.text.includes("processor.ts"))).toBe(true)

    const msgs = yield* sessions.messages({ sessionID: session.id })
    expect(msgs.filter((msg) => msg.info.role === "assistant")).toHaveLength(1)
    expect(yield* llm.calls).toBe(1)
  }),
)

it.instance("records aborted errors when prompt is cancelled mid-stream", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({ title: "Prompt cancel regression" })

    yield* llm.hang

    const fiber = yield* prompt
      .prompt({
        sessionID: session.id,
        agent: "build",
        parts: [{ type: "text", text: "Cancel me" }],
      })
      .pipe(Effect.forkChild)

    yield* llm.wait(1)
    yield* waitForBusy(session.id)
    yield* prompt.cancel(session.id)

    const exit = yield* Fiber.await(fiber)
    expect(Exit.isSuccess(exit)).toBe(true)
    if (Exit.isSuccess(exit)) {
      expect(exit.value.info.role).toBe("assistant")
      if (exit.value.info.role === "assistant") {
        expect(exit.value.info.error?.name).toBe("MessageAbortedError")
      }
    }

    const msgs = yield* sessions.messages({ sessionID: session.id })
    const last = msgs.findLast((msg) => msg.info.role === "assistant")
    expect(last?.info.role).toBe("assistant")
    if (last?.info.role === "assistant") {
      expect(last.info.error?.name).toBe("MessageAbortedError")
    }
  }),
)

// Agent variant

noLLMServer.instance(
  "applies agent variant only when using agent model",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})

      const other = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        model: { providerID: ProviderV2.ID.make("opencode"), modelID: ModelV2.ID.make("kimi-k2.5-free") },
        noReply: true,
        parts: [{ type: "text", text: "hello" }],
      })
      if (other.info.role !== "user") throw new Error("expected user message")
      expect(other.info.model.variant).toBeUndefined()

      const match = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        noReply: true,
        parts: [{ type: "text", text: "hello again" }],
      })
      if (match.info.role !== "user") throw new Error("expected user message")
      expect(match.info.model).toEqual({
        providerID: ProviderV2.ID.make("test"),
        modelID: ModelV2.ID.make("test-model"),
        variant: "xhigh",
      })
      expect(match.info.model.variant).toBe("xhigh")

      const override = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        noReply: true,
        variant: "high",
        parts: [{ type: "text", text: "hello third" }],
      })
      if (override.info.role !== "user") throw new Error("expected user message")
      expect(override.info.model.variant).toBe("high")

      yield* sessions.remove(session.id)
    }),
  {
    config: {
      ...cfg,
      provider: {
        ...cfg.provider,
        test: {
          ...cfg.provider.test,
          models: {
            "test-model": {
              ...cfg.provider.test.models["test-model"],
              variants: { xhigh: {}, high: {} },
            },
          },
        },
      },
      agent: {
        build: {
          model: "test/test-model",
          variant: "xhigh",
        },
      },
    },
  },
)

// Agent / command resolution errors

noLLMServer.instance(
  "unknown agent throws typed error",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const exit = yield* prompt
        .prompt({
          sessionID: session.id,
          agent: "nonexistent-agent-xyz",
          noReply: true,
          parts: [{ type: "text", text: "hello" }],
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const err = Cause.squash(exit.cause)
        expect(err).not.toBeInstanceOf(TypeError)
        expect(NamedError.Unknown.isInstance(err)).toBe(true)
        if (NamedError.Unknown.isInstance(err)) {
          expect(err.data.message).toContain('Agent not found: "nonexistent-agent-xyz"')
        }
      }
    }),
  30_000,
)

noLLMServer.instance(
  "unknown agent error includes available agent names",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const exit = yield* prompt
        .prompt({
          sessionID: session.id,
          agent: "nonexistent-agent-xyz",
          noReply: true,
          parts: [{ type: "text", text: "hello" }],
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const err = Cause.squash(exit.cause)
        expect(NamedError.Unknown.isInstance(err)).toBe(true)
        if (NamedError.Unknown.isInstance(err)) {
          expect(err.data.message).toContain("build")
        }
      }
    }),
  30_000,
)

noLLMServer.instance(
  "unknown command throws typed error with available names",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const exit = yield* prompt
        .command({
          sessionID: session.id,
          command: "nonexistent-command-xyz",
          arguments: "",
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const err = Cause.squash(exit.cause)
        expect(err).not.toBeInstanceOf(TypeError)
        expect(NamedError.Unknown.isInstance(err)).toBe(true)
        if (NamedError.Unknown.isInstance(err)) {
          expect(err.data.message).toContain('Command not found: "nonexistent-command-xyz"')
          expect(err.data.message).toContain("init")
        }
      }
    }),
  30_000,
)
