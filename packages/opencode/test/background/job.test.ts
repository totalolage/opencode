import { describe, expect } from "bun:test"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Ref } from "effect"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Session } from "@/session/session"
import { MessageID, PartID } from "@/session/schema"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { testEffect } from "../lib/effect"

const it = testEffect(LayerNode.compile(BackgroundJob.node))

const dbLayer = (flags: Partial<RuntimeFlags.Info> = {}) =>
  LayerNode.compile(
    LayerNode.group([
      BackgroundJob.node,
      EventV2Bridge.node,
      Session.node,
      SessionProjector.node,
      Database.node,
      DelegationStore.node,
      RuntimeFlags.node,
    ]),
    [[RuntimeFlags.node, RuntimeFlags.layer(flags)]],
  )

const dbIt = testEffect(dbLayer())
const backgroundIt = testEffect(dbLayer({ experimentalBackgroundSubagents: true }))

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

type RegisteredTask = {
  readonly parent: Session.Info
  readonly child: Session.Info
  readonly origin: SessionV1.ToolPart
  readonly generation: Delegation.Generation
}

const registerTask = Effect.fn("BackgroundJobTest.registerTask")(function* (input: {
  readonly name: string
  readonly parent?: Session.Info
  readonly parentGenerationID?: Delegation.ID
  readonly mode?: Delegation.Mode
}) {
  const sessions = yield* Session.Service
  const delegation = yield* DelegationStore.Service
  const parent = input.parent ?? (yield* sessions.create({ title: `parent-${input.name}` }))
  const user = yield* sessions.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID: parent.id,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  const assistant = yield* sessions.updateMessage({
    id: MessageID.ascending(),
    role: "assistant",
    parentID: user.id,
    sessionID: parent.id,
    mode: "build",
    agent: "build",
    cost: 0,
    path: { cwd: "/tmp", root: "/tmp" },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    variant: "xhigh",
    time: { created: Date.now() },
  })
  const origin = yield* sessions.updatePart({
    id: PartID.ascending(),
    messageID: assistant.id,
    sessionID: parent.id,
    type: "tool",
    callID: `call-background-job-${input.name}`,
    tool: "task",
    state: {
      status: "running",
      input: {
        description: "inspect bug",
        prompt: "look into the cache key path",
        subagent_type: "general",
      },
      time: { start: Date.now() },
    },
  })
  const child = yield* sessions.create({ parentID: parent.id, title: `child-${input.name}` })
  const registered = yield* delegation.register({
    requestID: Delegation.RequestID.create(),
    generationID: Delegation.ID.create(),
    parentID: parent.id,
    childID: child.id,
    origin: {
      messageID: SessionMessage.ID.make(assistant.id),
      partID: Delegation.OriginPartID.make(origin.id),
      callID: origin.callID,
    },
    ...(input.parentGenerationID === undefined ? {} : { parentGenerationID: input.parentGenerationID }),
    ...(input.mode === undefined ? {} : { mode: input.mode }),
    explicitReuse: false,
  })
  yield* delegation.finishWork(registered.workID)
  return { parent, child, origin, generation: registered.generation } satisfies RegisteredTask
})

function expectPromotionFailure(exit: Exit.Exit<unknown, unknown>, code: string) {
  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toMatchObject({ code })
}

describe("background.job", () => {
  it.instance("tracks started jobs through completion", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const latch = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        type: "test",
        title: "test job",
        run: Deferred.await(latch).pipe(Effect.as("done")),
      })

      expect(job.id.startsWith("job_")).toBe(true)
      expect(job.status).toBe("running")
      expect(job.title).toBe("test job")

      yield* Deferred.succeed(latch, undefined)
      const done = yield* jobs.wait({ id: job.id })

      expect(done.timedOut).toBe(false)
      expect(done.info?.status).toBe("completed")
      expect(done.info?.output).toBe("done")
      expect((yield* jobs.list()).map((item) => item.id)).toEqual([job.id])
    }),
  )

  it.instance("returns a running snapshot when wait times out", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const job = yield* jobs.start({
        type: "test",
        run: Effect.never,
      })

      const result = yield* jobs.wait({ id: job.id, timeout: 1 })

      expect(result.timedOut).toBe(true)
      expect(result.info?.status).toBe("running")
    }),
  )

  it.instance("deduplicates concurrent starts for a running id", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const started = yield* Deferred.make<void>()
      const id = "job_test"
      const [first, second] = yield* Effect.all(
        [
          jobs.start({
            id,
            type: "test",
            run: Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
          }),
          jobs.start({
            id,
            type: "test",
            run: Effect.fail(new Error("duplicate started")),
          }),
        ],
        { concurrency: "unbounded" },
      )

      yield* Deferred.await(started)

      expect(first.id).toBe(id)
      expect(second.id).toBe(id)
      expect(first.status).toBe("running")
      expect(second.status).toBe("running")
      expect((yield* jobs.list()).map((item) => item.id)).toEqual([id])

      yield* jobs.cancel(id)
    }),
  )

  it.instance("waits for extensions before completing a running job", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const first = yield* Deferred.make<void>()
      const second = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        type: "test",
        run: Deferred.await(first).pipe(Effect.as("first")),
      })

      expect(yield* jobs.extend({ id: job.id, run: Deferred.await(second).pipe(Effect.as("second")) })).toBe(true)
      yield* Deferred.succeed(first, undefined)
      expect((yield* jobs.get(job.id))?.status).toBe("running")

      yield* Deferred.succeed(second, undefined)
      const done = yield* jobs.wait({ id: job.id })
      expect(done.info?.status).toBe("completed")
      expect(done.info?.output).toBe("second")
    }),
  )

  it.instance("runs extensions after earlier work completes", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const first = yield* Deferred.make<void>()
      const order: string[] = []
      const job = yield* jobs.start({
        type: "test",
        run: Effect.sync(() => order.push("start")).pipe(Effect.andThen(Deferred.await(first)), Effect.as("first")),
      })

      expect(
        yield* jobs.extend({
          id: job.id,
          run: Effect.sync(() => order.push("extend")).pipe(Effect.as("second")),
        }),
      ).toBe(true)
      yield* Effect.yieldNow
      expect(order).toEqual(["start"])

      yield* Deferred.succeed(first, undefined)
      expect((yield* jobs.wait({ id: job.id })).info?.output).toBe("second")
      expect(order).toEqual(["start", "extend"])
    }),
  )

  it.instance("rejects extensions after a job completes", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const job = yield* jobs.start({ type: "test", run: Effect.succeed("done") })
      yield* jobs.wait({ id: job.id })

      expect(yield* jobs.extend({ id: job.id, run: Effect.succeed("late") })).toBe(false)
      expect((yield* jobs.get(job.id))?.output).toBe("done")
    }),
  )

  it.instance("records failed jobs", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const job = yield* jobs.start({
        type: "test",
        run: Effect.fail(new Error("boom")),
      })

      const result = yield* jobs.wait({ id: job.id })

      expect(result.info?.status).toBe("error")
      expect(result.info?.error).toBe("boom")
    }),
  )

  it.instance("ignores stale settlements after restarting a failed job", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const fail = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const id = "job_test"
      yield* jobs.start({
        id,
        type: "test",
        run: Deferred.await(fail).pipe(Effect.andThen(Effect.fail(new Error("boom")))),
      })
      yield* jobs.extend({
        id,
        run: Effect.never.pipe(
          Effect.ensuring(Deferred.succeed(interrupted, undefined).pipe(Effect.andThen(Deferred.await(release)))),
        ),
      })

      yield* Deferred.succeed(fail, undefined)
      expect((yield* jobs.wait({ id })).info?.status).toBe("error")
      yield* Deferred.await(interrupted)
      yield* jobs.start({ id, type: "test", run: Effect.never })

      yield* Deferred.succeed(release, undefined)
      yield* Effect.yieldNow
      expect((yield* jobs.get(id))?.status).toBe("running")
      yield* jobs.cancel(id)
    }),
  )

  it.instance("can cancel running jobs", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const interrupted = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        type: "test",
        run: Effect.never.pipe(Effect.ensuring(Deferred.succeed(interrupted, undefined))),
      })
      yield* jobs.extend({
        id: job.id,
        run: Effect.never,
      })

      const cancelled = yield* jobs.cancel(job.id)

      expect(cancelled?.status).toBe("cancelled")
      yield* Deferred.await(interrupted).pipe(Effect.timeout("1 second"))
      expect((yield* jobs.get(job.id))?.status).toBe("cancelled")
    }),
  )

  it.instance("promotes running jobs without interrupting them", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const latch = yield* Deferred.make<void>()
      const promoted = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        type: "test",
        metadata: { parentSessionId: "parent" },
        onPromote: Deferred.succeed(promoted, undefined).pipe(Effect.asVoid),
        run: Deferred.await(latch).pipe(Effect.as("done")),
      })

      const info = yield* jobs.promote(job.id)

      expect(info?.status).toBe("running")
      expect(info?.metadata?.background).toBe(true)
      yield* Deferred.await(promoted)
      expect((yield* jobs.get(job.id))?.status).toBe("running")

      yield* Deferred.succeed(latch, undefined)
      expect((yield* jobs.wait({ id: job.id })).info?.output).toBe("done")
    }),
  )

  backgroundIt.instance("rejects a Task promotion when its incoming parent generation is foreground", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const delegation = yield* DelegationStore.Service
      const parent = yield* registerTask({ name: "foreground-parent", mode: "foreground" })
      const task = yield* registerTask({
        name: "foreground-child",
        parent: parent.child,
        parentGenerationID: parent.generation.id,
      })
      const promoted = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        id: task.child.id,
        type: "task",
        metadata: { delegationID: task.generation.id },
        onPromote: Deferred.succeed(promoted, undefined).pipe(Effect.asVoid),
        run: Effect.never,
      })
      const waiter = yield* jobs.waitForPromotion(job.id).pipe(Effect.forkChild)
      yield* Effect.yieldNow

      const exit = yield* jobs.promote(job.id).pipe(Effect.exit)

      expectPromotionFailure(exit, "background_parent_forbidden")
      expect((yield* delegation.get(parent.generation.id))?.mode).toBe("foreground")
      expect((yield* delegation.get(task.generation.id))?.mode).toBe("foreground")
      expect((yield* jobs.get(job.id))?.metadata?.background).toBeUndefined()
      expect(yield* Deferred.isDone(promoted)).toBe(false)
      expect(waiter.pollUnsafe()).toBeUndefined()

      yield* jobs.cancel(job.id)
    }),
  )

  backgroundIt.instance("promotes a registered root Task durably before local promotion", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const delegation = yield* DelegationStore.Service
      const task = yield* registerTask({ name: "root" })
      const callbackCount = yield* Ref.make(0)
      const callbackModes = yield* Ref.make<Delegation.Mode[]>([])
      const job = yield* jobs.start({
        id: task.child.id,
        type: "task",
        metadata: { delegationID: task.generation.id },
        onPromote: Effect.gen(function* () {
          const current = yield* delegation.get(task.generation.id)
          yield* Ref.update(callbackCount, (count) => count + 1)
          if (current) yield* Ref.update(callbackModes, (modes) => [...modes, current.mode])
        }).pipe(Effect.orDie),
        run: Effect.never,
      })
      const waiter = yield* jobs.waitForPromotion(job.id).pipe(Effect.forkChild)

      expect(job.metadata?.delegationID).toBe(task.generation.id)
      expect((yield* delegation.get(task.generation.id))?.mode).toBe("foreground")
      expect((yield* jobs.get(job.id))?.metadata?.background).toBeUndefined()

      const promoted = yield* jobs.promote(job.id)
      const repeated = yield* jobs.promote(job.id)

      expect(promoted?.metadata?.background).toBe(true)
      expect(repeated?.metadata?.background).toBe(true)
      expect((yield* delegation.get(task.generation.id))?.mode).toBe("background")
      expect(yield* Ref.get(callbackCount)).toBe(1)
      expect(yield* Ref.get(callbackModes)).toEqual(["background"])
      expect((yield* Fiber.join(waiter)).metadata?.background).toBe(true)

      yield* jobs.cancel(job.id)
    }),
  )

  backgroundIt.instance("promotes a Task with a background incoming parent", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const delegation = yield* DelegationStore.Service
      const parent = yield* registerTask({ name: "background-parent", mode: "background" })
      const task = yield* registerTask({
        name: "background-child",
        parent: parent.child,
        parentGenerationID: parent.generation.id,
      })
      const callbackCount = yield* Ref.make(0)
      const job = yield* jobs.start({
        id: task.child.id,
        type: "task",
        metadata: { delegationID: task.generation.id },
        onPromote: Ref.update(callbackCount, (count) => count + 1),
        run: Effect.never,
      })

      const promoted = yield* jobs.promote(job.id)

      expect(promoted?.metadata?.background).toBe(true)
      expect((yield* delegation.get(parent.generation.id))?.mode).toBe("background")
      expect((yield* delegation.get(task.generation.id))?.mode).toBe("background")
      expect(yield* Ref.get(callbackCount)).toBe(1)

      yield* jobs.cancel(job.id)
    }),
  )

  backgroundIt.instance("reconciles local promotion when the Task is already durably background", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const delegation = yield* DelegationStore.Service
      const task = yield* registerTask({ name: "already-background", mode: "background" })
      const callbackCount = yield* Ref.make(0)
      const job = yield* jobs.start({
        id: task.child.id,
        type: "task",
        metadata: { delegationID: task.generation.id },
        onPromote: Ref.update(callbackCount, (count) => count + 1),
        run: Effect.never,
      })

      expect((yield* delegation.get(task.generation.id))?.mode).toBe("background")
      expect((yield* jobs.get(job.id))?.metadata?.background).toBeUndefined()

      const promoted = yield* jobs.promote(job.id)

      expect(promoted?.metadata?.background).toBe(true)
      expect((yield* delegation.get(task.generation.id))?.mode).toBe("background")
      expect(yield* Ref.get(callbackCount)).toBe(1)

      yield* jobs.cancel(job.id)
    }),
  )

  dbIt.instance("rejects Task promotion while background subagents are disabled", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const delegation = yield* DelegationStore.Service
      const task = yield* registerTask({ name: "feature-disabled" })
      const promoted = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        id: task.child.id,
        type: "task",
        metadata: { delegationID: task.generation.id },
        onPromote: Deferred.succeed(promoted, undefined).pipe(Effect.asVoid),
        run: Effect.never,
      })
      const waiter = yield* jobs.waitForPromotion(job.id).pipe(Effect.forkChild)
      yield* Effect.yieldNow

      const exit = yield* jobs.promote(job.id).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        expect(Cause.squash(exit.cause)).toMatchObject({
          message: "Background subagents require OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS=true",
        })
      }
      expect((yield* delegation.get(task.generation.id))?.mode).toBe("foreground")
      expect((yield* jobs.get(job.id))?.metadata?.background).toBeUndefined()
      expect(yield* Deferred.isDone(promoted)).toBe(false)
      expect(waiter.pollUnsafe()).toBeUndefined()

      yield* jobs.cancel(job.id)
    }),
  )

  backgroundIt.instance("rejects an unregistered Task promotion without changing local state", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const delegation = yield* DelegationStore.Service
      const delegationID = Delegation.ID.create()
      const promoted = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        id: "job-unregistered-task",
        type: "task",
        metadata: { delegationID },
        onPromote: Deferred.succeed(promoted, undefined).pipe(Effect.asVoid),
        run: Effect.never,
      })

      const exit = yield* jobs.promote(job.id).pipe(Effect.exit)

      expectPromotionFailure(exit, "generation_not_found")
      expect(yield* delegation.get(delegationID)).toBeUndefined()
      expect((yield* jobs.get(job.id))?.metadata?.background).toBeUndefined()
      expect(yield* Deferred.isDone(promoted)).toBe(false)

      yield* jobs.cancel(job.id)
    }),
  )

  it.instance("returns immutable snapshots", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const job = yield* jobs.start({
        type: "test",
        metadata: { value: "initial" },
        run: Effect.succeed("done"),
      })

      if (job.metadata) job.metadata.value = "changed"

      expect((yield* jobs.get(job.id))?.metadata?.value).toBe("initial")
    }),
  )

  it.instance("keeps missing and completed job promotion as no-ops", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service

      expect(yield* jobs.promote("job-missing")).toBeUndefined()

      const job = yield* jobs.start({
        type: "task",
        run: Effect.succeed("done"),
      })
      const completed = yield* jobs.wait({ id: job.id })
      const promoted = yield* jobs.promote(job.id)

      expect(completed.info?.status).toBe("completed")
      expect(promoted).toBeUndefined()
    }),
  )
})
