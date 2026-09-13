import { describe, expect } from "bun:test"
import { Cause, Deferred, Effect, Exit, Fiber, Latch, Ref, Scheduler, Scope } from "effect"
import { Runner } from "@/effect/runner"
import { it } from "../lib/effect"

const waitForState = <A, E>(runner: Runner.Runner<A, E>, tag: Runner.State<A, E>["_tag"]) =>
  Effect.gen(function* () {
    while (runner.state._tag !== tag) yield* Effect.yieldNow
  }).pipe(Effect.timeout("1 second"))

const makeHeldScheduler = (onFirstTask: () => void) => {
  const tasks: Array<() => void> = []
  let held = true
  const flush = () => {
    while (tasks.length > 0) {
      const task = tasks.shift()
      if (task) task()
    }
  }
  const scheduler: Scheduler.Scheduler = {
    executionMode: "async",
    shouldYield: () => false,
    makeDispatcher: () => ({
      scheduleTask(task) {
        if (!held) {
          task()
          return
        }
        held = false
        tasks.push(task)
        onFirstTask()
      },
      flush,
    }),
  }
  return { scheduler, flush }
}

const makePublicationScheduler = (
  onTaskQueued: () => void,
  onShouldYield: (fiber: Fiber.Fiber<unknown, unknown>) => void,
) => {
  const tasks: Array<() => void> = []
  let childQueued = false
  let interruptionRequested = false
  const flush = () => {
    while (tasks.length > 0) {
      const task = tasks.shift()
      if (task) task()
    }
  }
  const scheduler: Scheduler.Scheduler = {
    executionMode: "async",
    shouldYield(fiber) {
      if (childQueued && !interruptionRequested && fiber.pollUnsafe() === undefined) {
        interruptionRequested = true
        onShouldYield(fiber)
      }
      return false
    },
    makeDispatcher: () => ({
      scheduleTask(task) {
        if (!childQueued) {
          childQueued = true
          tasks.push(task)
          onTaskQueued()
          return
        }
        task()
      },
      flush,
    }),
  }
  return { scheduler, flush }
}

const makeGuardedScheduler = (operationBudget: number, taskBudget: number) => {
  const tasks: Array<() => void> = []
  let operations = 0
  let yields = 0
  let droppedTasks = 0
  const flush = () => {
    let flushed = 0
    while (tasks.length > 0 && flushed < taskBudget) {
      const task = tasks.shift()
      if (task) task()
      flushed += 1
    }
  }
  const scheduler: Scheduler.Scheduler = {
    executionMode: "async",
    shouldYield() {
      operations += 1
      if (operations <= operationBudget) return false
      yields += 1
      return true
    },
    makeDispatcher: () => ({
      scheduleTask(task) {
        if (tasks.length >= taskBudget) {
          droppedTasks += 1
          return
        }
        tasks.push(task)
      },
      flush,
    }),
  }
  return {
    scheduler,
    flush,
    pendingTasks: () => tasks.length,
    operations: () => operations,
    yields: () => yields,
    droppedTasks: () => droppedTasks,
  }
}

describe("Runner", () => {
  // --- ensureRunning semantics ---

  it.live(
    "ensureRunning starts work and returns result",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const result = yield* runner.ensureRunning(Effect.succeed("hello"))
      expect(result).toBe("hello")
      expect(runner.state._tag).toBe("Idle")
      expect(runner.busy).toBe(false)
    }),
  )

  it.live(
    "ensureRunning propagates work failures",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string, string>(s)
      const exit = yield* runner.ensureRunning(Effect.fail("boom")).pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      expect(runner.state._tag).toBe("Idle")
    }),
  )

  it.live(
    "concurrent callers share the same run",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const calls = yield* Ref.make(0)
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const work = Effect.gen(function* () {
        yield* Ref.update(calls, (n) => n + 1)
        yield* Deferred.succeed(started, void 0)
        yield* Deferred.await(release)
        return "shared"
      })

      yield* Effect.gen(function* () {
        const first = yield* runner.ensureRunning(work).pipe(Effect.forkChild)
        yield* Deferred.await(started).pipe(Effect.timeout("1 second"))
        const second = yield* runner.ensureRunning(work).pipe(Effect.forkChild)
        yield* Effect.yieldNow
        yield* Deferred.succeed(release, void 0)

        const [a, b] = yield* Effect.all([Fiber.join(first), Fiber.join(second)], {
          concurrency: "unbounded",
        }).pipe(Effect.timeout("1 second"))
        expect(a).toBe("shared")
        expect(b).toBe("shared")
      }).pipe(Effect.ensuring(Deferred.succeed(release, void 0).pipe(Effect.ignore)))

      expect(yield* Ref.get(calls)).toBe(1)
    }),
  )

  it.live(
    "concurrent callers all receive same error",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string, string>(s)
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const work = Effect.gen(function* () {
        yield* Deferred.succeed(started, void 0)
        yield* Deferred.await(release)
        return yield* Effect.fail("boom")
      })

      yield* Effect.gen(function* () {
        const first = yield* runner.ensureRunning(work).pipe(Effect.exit, Effect.forkChild)
        yield* Deferred.await(started).pipe(Effect.timeout("1 second"))
        const second = yield* runner.ensureRunning(work).pipe(Effect.exit, Effect.forkChild)
        yield* Effect.yieldNow
        yield* Deferred.succeed(release, void 0)

        const [a, b] = yield* Effect.all([Fiber.join(first), Fiber.join(second)], {
          concurrency: "unbounded",
        }).pipe(Effect.timeout("1 second"))
        expect(Exit.isFailure(a)).toBe(true)
        expect(Exit.isFailure(b)).toBe(true)
      }).pipe(Effect.ensuring(Deferred.succeed(release, void 0).pipe(Effect.ignore)))
    }),
  )

  it.live(
    "ensureRunning can be called again after previous run completes",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      expect(yield* runner.ensureRunning(Effect.succeed("first"))).toBe("first")
      expect(yield* runner.ensureRunning(Effect.succeed("second"))).toBe("second")
    }),
  )

  it.live(
    "second ensureRunning ignores new work if already running",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const ran = yield* Ref.make<string[]>([])
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()

      const first = Effect.gen(function* () {
        yield* Ref.update(ran, (a) => [...a, "first"])
        yield* Deferred.succeed(started, void 0)
        yield* Deferred.await(release)
        return "first-result"
      })
      const second = Effect.gen(function* () {
        yield* Ref.update(ran, (a) => [...a, "second"])
        return "second-result"
      })

      yield* Effect.gen(function* () {
        const firstRun = yield* runner.ensureRunning(first).pipe(Effect.forkChild)
        yield* Deferred.await(started).pipe(Effect.timeout("1 second"))
        const secondRun = yield* runner.ensureRunning(second).pipe(Effect.forkChild)
        yield* Effect.yieldNow
        yield* Deferred.succeed(release, void 0)

        const [a, b] = yield* Effect.all([Fiber.join(firstRun), Fiber.join(secondRun)], {
          concurrency: "unbounded",
        }).pipe(Effect.timeout("1 second"))
        expect(a).toBe("first-result")
        expect(b).toBe("first-result")
      }).pipe(Effect.ensuring(Deferred.succeed(release, void 0).pipe(Effect.ignore)))

      expect(yield* Ref.get(ran)).toEqual(["first"])
    }),
  )

  // --- wake semantics ---

  it.live(
    "wake acknowledges idle work before its body completes",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const finished = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        yield* runner
          .wake(
            Effect.gen(function* () {
              yield* Deferred.succeed(started, void 0)
              yield* Deferred.await(release)
              yield* Deferred.succeed(finished, void 0)
              return "wake"
            }),
          )
          .pipe(Effect.timeout("250 millis"))
        expect(runner.busy).toBe(true)

        yield* Deferred.await(started).pipe(Effect.timeout("1 second"))
        expect(yield* Deferred.isDone(finished)).toBe(false)

        yield* Deferred.succeed(release, void 0)
        yield* Deferred.await(finished).pipe(Effect.timeout("1 second"))
      }).pipe(Effect.ensuring(Deferred.succeed(release, void 0).pipe(Effect.ignore)))
    }),
  )

  it.live(
    "wake coalesces distinct work behind the current run and keeps ensureRunning on its result",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const active = yield* Ref.make(0)
      const maxActive = yield* Ref.make(0)
      const ran = yield* Ref.make<string[]>([])
      const currentStarted = yield* Deferred.make<void>()
      const currentRelease = yield* Deferred.make<void>()
      const followStarted = yield* Deferred.make<void>()
      const followRelease = yield* Deferred.make<void>()
      const ignored = Effect.gen(function* () {
        yield* Ref.update(ran, (items) => [...items, "ignored"])
        return "ignored"
      })

      const enter = (name: string) =>
        Effect.gen(function* () {
          const count = yield* Ref.modify(active, (value) => [value + 1, value + 1] as const)
          yield* Ref.update(maxActive, (value) => Math.max(value, count))
          yield* Ref.update(ran, (items) => [...items, name])
        })
      const leave = Ref.update(active, (value) => value - 1)

      yield* Effect.gen(function* () {
        const current = yield* runner
          .ensureRunning(
            Effect.gen(function* () {
              yield* enter("current")
              yield* Deferred.succeed(currentStarted, void 0)
              yield* Deferred.await(currentRelease)
              return "current"
            }).pipe(Effect.ensuring(leave)),
          )
          .pipe(Effect.forkChild)
        yield* Deferred.await(currentStarted).pipe(Effect.timeout("1 second"))

        const joined = yield* runner.ensureRunning(ignored).pipe(Effect.forkChild)
        yield* Effect.yieldNow

        yield* runner
          .wake(
            Effect.gen(function* () {
              yield* enter("first-follow-up")
              yield* Deferred.succeed(followStarted, void 0)
              yield* Deferred.await(followRelease)
              return "first-follow-up"
            }).pipe(Effect.ensuring(leave)),
          )
          .pipe(Effect.timeout("250 millis"))
        yield* runner
          .wake(
            Effect.gen(function* () {
              yield* enter("second-follow-up")
              return "second-follow-up"
            }).pipe(Effect.ensuring(leave)),
          )
          .pipe(Effect.timeout("250 millis"))

        yield* Deferred.succeed(currentRelease, void 0)
        expect(yield* Fiber.join(current).pipe(Effect.timeout("1 second"))).toBe("current")
        expect(yield* Fiber.join(joined).pipe(Effect.timeout("1 second"))).toBe("current")

        yield* Deferred.await(followStarted).pipe(Effect.timeout("1 second"))
        expect(yield* Deferred.isDone(followStarted)).toBe(true)
        expect(yield* Ref.get(ran)).toEqual(["current", "first-follow-up"])

        yield* Deferred.succeed(followRelease, void 0)
        yield* waitForState(runner, "Idle")
        expect(yield* Ref.get(ran)).toEqual(["current", "first-follow-up"])
        expect(yield* Ref.get(maxActive)).toBe(1)
      }).pipe(
        Effect.ensuring(
          Effect.all([Deferred.succeed(currentRelease, void 0), Deferred.succeed(followRelease, void 0)], {
            discard: true,
          }).pipe(Effect.ignore),
        ),
      )
    }),
  )

  it.live(
    "wake waits for the current body finalizer before starting its follow-up",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const events = yield* Ref.make<string[]>([])
      const bodyComplete = yield* Deferred.make<void>()
      const finalizerStarted = yield* Deferred.make<void>()
      const finalizerRelease = yield* Deferred.make<void>()
      const followStarted = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        const first = yield* runner
          .ensureRunning(
            Effect.gen(function* () {
              yield* Ref.update(events, (items) => [...items, "body-complete"])
              yield* Deferred.succeed(bodyComplete, void 0)
              return "first"
            }).pipe(
              Effect.ensuring(
                Effect.gen(function* () {
                  yield* Ref.update(events, (items) => [...items, "finalizer-start"])
                  yield* Deferred.succeed(finalizerStarted, void 0)
                  yield* Deferred.await(finalizerRelease)
                  yield* Ref.update(events, (items) => [...items, "finalizer-done"])
                }),
              ),
            ),
          )
          .pipe(Effect.forkChild)

        yield* Deferred.await(bodyComplete).pipe(Effect.timeout("1 second"))
        yield* Deferred.await(finalizerStarted).pipe(Effect.timeout("1 second"))
        expect(runner.busy).toBe(true)

        yield* runner
          .wake(
            Effect.gen(function* () {
              yield* Ref.update(events, (items) => [...items, "follow-start"])
              yield* Deferred.succeed(followStarted, void 0)
              return "follow"
            }),
          )
          .pipe(Effect.timeout("250 millis"))
        expect(yield* Deferred.isDone(followStarted)).toBe(false)
        expect(yield* Ref.get(events)).toEqual(["body-complete", "finalizer-start"])

        yield* Deferred.succeed(finalizerRelease, void 0)
        expect(yield* Fiber.join(first).pipe(Effect.timeout("1 second"))).toBe("first")
        yield* Deferred.await(followStarted).pipe(Effect.timeout("1 second"))
        yield* waitForState(runner, "Idle")
        expect(yield* Ref.get(events)).toEqual(["body-complete", "finalizer-start", "finalizer-done", "follow-start"])
      }).pipe(Effect.ensuring(Deferred.succeed(finalizerRelease, void 0).pipe(Effect.ignore)))
    }),
  )

  it.live(
    "wake during a follow-up queues the next run",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const ran = yield* Ref.make<string[]>([])
      const currentStarted = yield* Deferred.make<void>()
      const currentRelease = yield* Deferred.make<void>()
      const firstStarted = yield* Deferred.make<void>()
      const firstRelease = yield* Deferred.make<void>()
      const secondStarted = yield* Deferred.make<void>()
      const secondRelease = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        const current = yield* runner
          .ensureRunning(
            Effect.gen(function* () {
              yield* Ref.update(ran, (items) => [...items, "current"])
              yield* Deferred.succeed(currentStarted, void 0)
              yield* Deferred.await(currentRelease)
              return "current"
            }),
          )
          .pipe(Effect.forkChild)
        yield* Deferred.await(currentStarted).pipe(Effect.timeout("1 second"))

        yield* runner
          .wake(
            Effect.gen(function* () {
              yield* Ref.update(ran, (items) => [...items, "first-follow-up"])
              yield* Deferred.succeed(firstStarted, void 0)
              yield* Deferred.await(firstRelease)
              return "first-follow-up"
            }),
          )
          .pipe(Effect.timeout("250 millis"))
        yield* Deferred.succeed(currentRelease, void 0)
        expect(yield* Fiber.join(current).pipe(Effect.timeout("1 second"))).toBe("current")
        yield* Deferred.await(firstStarted).pipe(Effect.timeout("1 second"))

        yield* runner
          .wake(
            Effect.gen(function* () {
              yield* Ref.update(ran, (items) => [...items, "second-follow-up"])
              yield* Deferred.succeed(secondStarted, void 0)
              yield* Deferred.await(secondRelease)
              return "second-follow-up"
            }),
          )
          .pipe(Effect.timeout("250 millis"))
        expect(yield* Deferred.isDone(secondStarted)).toBe(false)
        expect(yield* Ref.get(ran)).toEqual(["current", "first-follow-up"])

        yield* Deferred.succeed(firstRelease, void 0)
        yield* Deferred.await(secondStarted).pipe(Effect.timeout("1 second"))
        expect(yield* Ref.get(ran)).toEqual(["current", "first-follow-up", "second-follow-up"])

        yield* Deferred.succeed(secondRelease, void 0)
        yield* waitForState(runner, "Idle")
      }).pipe(
        Effect.ensuring(
          Effect.all(
            [
              Deferred.succeed(currentRelease, void 0),
              Deferred.succeed(firstRelease, void 0),
              Deferred.succeed(secondRelease, void 0),
            ],
            { discard: true },
          ).pipe(Effect.ignore),
        ),
      )
    }),
  )

  it.live(
    "pending wake work runs after the current body fails",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string, string>(s)
      const currentStarted = yield* Deferred.make<void>()
      const failureRelease = yield* Deferred.make<void>()
      const pendingStarted = yield* Deferred.make<void>()
      const ran = yield* Ref.make<string[]>([])

      yield* Effect.gen(function* () {
        const current = yield* runner
          .ensureRunning(
            Effect.gen(function* () {
              yield* Deferred.succeed(currentStarted, void 0)
              yield* Deferred.await(failureRelease)
              return yield* Effect.fail("boom")
            }),
          )
          .pipe(Effect.exit, Effect.forkChild)
        yield* Deferred.await(currentStarted).pipe(Effect.timeout("1 second"))

        yield* runner
          .wake(
            Effect.gen(function* () {
              yield* Ref.update(ran, (items) => [...items, "pending"])
              yield* Deferred.succeed(pendingStarted, void 0)
              return "pending"
            }),
          )
          .pipe(Effect.timeout("250 millis"))
        yield* Deferred.succeed(failureRelease, void 0)

        const exit = yield* Fiber.join(current).pipe(Effect.timeout("1 second"))
        expect(Exit.isFailure(exit)).toBe(true)
        yield* Deferred.await(pendingStarted).pipe(Effect.timeout("1 second"))
        yield* waitForState(runner, "Idle")
        expect(yield* Ref.get(ran)).toEqual(["pending"])
      }).pipe(Effect.ensuring(Deferred.succeed(failureRelease, void 0).pipe(Effect.ignore)))
    }),
  )

  it.live(
    "shell wakes coalesce into one queued run",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const shellStarted = yield* Deferred.make<void>()
      const shellRelease = yield* Deferred.make<void>()
      const runStarted = yield* Deferred.make<void>()
      const runRelease = yield* Deferred.make<void>()
      const secondRunStarted = yield* Deferred.make<void>()
      const ran = yield* Ref.make<string[]>([])

      yield* Effect.gen(function* () {
        const shell = yield* runner
          .startShell(
            Effect.gen(function* () {
              yield* Deferred.succeed(shellStarted, void 0)
              yield* Deferred.await(shellRelease)
              return "shell"
            }),
          )
          .pipe(Effect.forkChild)
        yield* Deferred.await(shellStarted).pipe(Effect.timeout("1 second"))
        yield* waitForState(runner, "Shell")

        yield* runner
          .wake(
            Effect.gen(function* () {
              yield* Ref.update(ran, (items) => [...items, "first"])
              yield* Deferred.succeed(runStarted, void 0)
              yield* Deferred.await(runRelease)
              return "first"
            }),
          )
          .pipe(Effect.timeout("250 millis"))
        expect(runner.state._tag).toBe("ShellThenRun")
        yield* runner
          .wake(
            Effect.gen(function* () {
              yield* Ref.update(ran, (items) => [...items, "second"])
              yield* Deferred.succeed(secondRunStarted, void 0)
              return "second"
            }),
          )
          .pipe(Effect.timeout("250 millis"))
        expect(runner.state._tag).toBe("ShellThenRun")

        yield* Deferred.succeed(shellRelease, void 0)
        expect(yield* Fiber.join(shell).pipe(Effect.timeout("1 second"))).toBe("shell")
        yield* Deferred.await(runStarted).pipe(Effect.timeout("1 second"))
        expect(yield* Deferred.isDone(secondRunStarted)).toBe(false)
        expect(yield* Ref.get(ran)).toEqual(["first"])

        yield* Deferred.succeed(runRelease, void 0)
        yield* waitForState(runner, "Idle")
        expect(yield* Deferred.isDone(secondRunStarted)).toBe(false)
        expect(yield* Ref.get(ran)).toEqual(["first"])
      }).pipe(
        Effect.ensuring(
          Effect.all([Deferred.succeed(shellRelease, void 0), Deferred.succeed(runRelease, void 0)], {
            discard: true,
          }).pipe(Effect.ignore),
        ),
      )
    }),
  )

  // --- scope and fiber lifecycle races ---

  it.live(
    "closed runner scope does not start ensure or wake work",
    Effect.gen(function* () {
      const runScope = yield* Scope.make()
      const ensureStarted = yield* Deferred.make<void>()
      const wakeStarted = yield* Deferred.make<void>()
      const runner = Runner.make<string>(runScope)

      yield* Scope.close(runScope, Exit.void)

      const ensureExit = yield* runner
        .ensureRunning(
          Effect.gen(function* () {
            yield* Deferred.succeed(ensureStarted, void 0)
            return "ensure"
          }),
        )
        .pipe(Effect.exit)
      yield* runner.wake(
        Effect.gen(function* () {
          yield* Deferred.succeed(wakeStarted, void 0)
          return "wake"
        }),
      )

      expect(Exit.isFailure(ensureExit)).toBe(true)
      expect(yield* Deferred.isDone(ensureStarted)).toBe(false)
      expect(yield* Deferred.isDone(wakeStarted)).toBe(false)
      expect(runner.state._tag).toBe("Idle")
      expect(runner.busy).toBe(false)
    }),
  )

  it.live(
    "closing the runner scope waits for finalization and drops pending wake work",
    Effect.gen(function* () {
      const runScope = yield* Scope.make()
      const runner = Runner.make<string>(runScope)
      const started = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      const finalizerStarted = yield* Deferred.make<void>()
      const finalizerRelease = yield* Deferred.make<void>()
      const pendingStarted = yield* Deferred.make<void>()
      const closeDone = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(finalizerRelease, void 0).pipe(Effect.ignore)
          if (runScope.state._tag !== "Closed") yield* Scope.close(runScope, Exit.void).pipe(Effect.ignore)
        }),
      )

      const current = yield* runner
        .ensureRunning(
          Effect.gen(function* () {
            yield* Deferred.succeed(started, void 0)
            yield* Effect.never
            return "current"
          }).pipe(
            Effect.onInterrupt(() => Deferred.succeed(interrupted, void 0)),
            Effect.ensuring(
              Effect.gen(function* () {
                yield* Deferred.succeed(finalizerStarted, void 0)
                yield* Deferred.await(finalizerRelease)
              }),
            ),
          ),
        )
        .pipe(Effect.exit, Effect.forkChild)
      yield* Deferred.await(started).pipe(Effect.timeout("1 second"))

      yield* runner.wake(
        Effect.gen(function* () {
          yield* Deferred.succeed(pendingStarted, void 0)
          return "pending"
        }),
      )
      expect(runner.state._tag).toBe("Running")
      if (runner.state._tag === "Running") expect(runner.state.pending).toBeDefined()

      const closing = yield* Scope.close(runScope, Exit.void).pipe(
        Effect.ensuring(Deferred.succeed(closeDone, void 0).pipe(Effect.ignore)),
        Effect.forkChild,
      )
      yield* Deferred.await(interrupted).pipe(Effect.timeout("1 second"))
      yield* Deferred.await(finalizerStarted).pipe(Effect.timeout("1 second"))
      expect(yield* Deferred.isDone(closeDone)).toBe(false)
      expect(yield* Deferred.isDone(pendingStarted)).toBe(false)
      expect(runner.busy).toBe(true)

      yield* Deferred.succeed(finalizerRelease, void 0)
      expect(Exit.isSuccess(yield* Fiber.await(closing).pipe(Effect.timeout("1 second")))).toBe(true)
      expect(Exit.isFailure(yield* Fiber.join(current).pipe(Effect.timeout("1 second")))).toBe(true)
      expect(yield* Deferred.isDone(closeDone)).toBe(true)
      yield* waitForState(runner, "Idle")
      expect(runner.busy).toBe(false)
    }),
  )

  it.live(
    "interrupting a shell caller before its child starts leaves the runner idle",
    Effect.gen(function* () {
      const runScope = yield* Scope.make()
      const runner = Runner.make<string>(runScope)
      const started = yield* Deferred.make<void>()
      let childQueued = false
      const held = makeHeldScheduler(() => {
        childQueued = true
      })
      const context = yield* Effect.context()
      const caller = Effect.runForkWith(context)(
        runner.startShell(
          Effect.gen(function* () {
            yield* Deferred.succeed(started, void 0)
            yield* Effect.never
            return "shell"
          }),
        ),
        { scheduler: held.scheduler },
      )

      expect(childQueued).toBe(true)
      expect(runner.state._tag).toBe("Shell")
      yield* Fiber.interrupt(caller)
      expect(Exit.isFailure(yield* Fiber.await(caller).pipe(Effect.timeout("1 second")))).toBe(true)
      held.flush()

      yield* waitForState(runner, "Idle")
      expect(yield* Deferred.isDone(started)).toBe(false)
      expect(runner.busy).toBe(false)
      yield* Scope.close(runScope, Exit.void)
    }),
  )

  it.live(
    "an attached child finalizer delays the queued follow-up",
    Effect.gen(function* () {
      const runScope = yield* Scope.make()
      const runner = Runner.make<string>(runScope)
      const childStarted = yield* Deferred.make<void>()
      const parentReturned = yield* Deferred.make<void>()
      const childFinalizerStarted = yield* Deferred.make<void>()
      const childFinalizerRelease = yield* Deferred.make<void>()
      const followStarted = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        const current = yield* runner
          .ensureRunning(
            Effect.gen(function* () {
              yield* Effect.gen(function* () {
                yield* Deferred.succeed(childStarted, void 0)
                yield* Effect.never
              }).pipe(
                Effect.ensuring(
                  Effect.gen(function* () {
                    yield* Deferred.succeed(childFinalizerStarted, void 0)
                    yield* Deferred.await(childFinalizerRelease)
                  }),
                ),
                Effect.forkChild,
              )
              yield* Deferred.await(childStarted)
              yield* Deferred.succeed(parentReturned, void 0)
              return "parent"
            }),
          )
          .pipe(Effect.forkChild)

        yield* Deferred.await(parentReturned).pipe(Effect.timeout("1 second"))
        yield* Deferred.await(childFinalizerStarted).pipe(Effect.timeout("1 second"))
        expect(runner.busy).toBe(true)

        yield* runner.wake(
          Effect.gen(function* () {
            yield* Deferred.succeed(followStarted, void 0)
            return "follow"
          }),
        )
        expect(yield* Deferred.isDone(followStarted)).toBe(false)
        expect(runner.busy).toBe(true)

        yield* Deferred.succeed(childFinalizerRelease, void 0)
        expect(yield* Fiber.join(current).pipe(Effect.timeout("1 second"))).toBe("parent")
        yield* Deferred.await(followStarted).pipe(Effect.timeout("1 second"))
        yield* waitForState(runner, "Idle")
      }).pipe(Effect.ensuring(Deferred.succeed(childFinalizerRelease, void 0).pipe(Effect.ignore)))

      yield* Scope.close(runScope, Exit.void)
    }),
  )

  it.live(
    "an attached shell child finalizer delays the queued follow-up",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const childStarted = yield* Deferred.make<void>()
      const shellBodyReturned = yield* Deferred.make<void>()
      const childFinalizerStarted = yield* Deferred.make<void>()
      const childFinalizerRelease = yield* Deferred.make<void>()
      const followStarted = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        const shell = yield* runner
          .startShell(
            Effect.gen(function* () {
              yield* Effect.gen(function* () {
                yield* Deferred.succeed(childStarted, void 0)
                yield* Effect.never
              }).pipe(
                Effect.ensuring(
                  Effect.gen(function* () {
                    yield* Deferred.succeed(childFinalizerStarted, void 0)
                    yield* Deferred.await(childFinalizerRelease)
                  }),
                ),
                Effect.forkChild,
              )
              yield* Deferred.await(childStarted)
              yield* Deferred.succeed(shellBodyReturned, void 0)
              return "shell"
            }),
          )
          .pipe(Effect.forkChild)

        yield* Deferred.await(shellBodyReturned).pipe(Effect.timeout("1 second"))
        yield* Deferred.await(childFinalizerStarted).pipe(Effect.timeout("1 second"))
        expect(runner.state._tag).toBe("Shell")
        expect(runner.busy).toBe(true)

        yield* runner.wake(
          Effect.gen(function* () {
            yield* Deferred.succeed(followStarted, void 0)
            return "follow"
          }),
        )
        expect(yield* Deferred.isDone(followStarted)).toBe(false)

        yield* Deferred.succeed(childFinalizerRelease, void 0)
        expect(yield* Fiber.join(shell).pipe(Effect.timeout("1 second"))).toBe("shell")
        yield* Deferred.await(followStarted).pipe(Effect.timeout("1 second"))
        yield* waitForState(runner, "Idle")
        expect(runner.busy).toBe(false)
      }).pipe(Effect.ensuring(Deferred.succeed(childFinalizerRelease, void 0).pipe(Effect.ignore)))
    }),
  )

  it.live(
    "interrupting between child fork and publication leaves accepted work cancellable",
    Effect.gen(function* () {
      const runScope = yield* Scope.make()
      const runner = Runner.make<string>(runScope)
      const started = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      const active = yield* Ref.make(0)
      const maxActive = yield* Ref.make(0)
      const events: Array<string> = []
      const context = yield* Effect.context()
      yield* Effect.addFinalizer(() => Scope.close(runScope, Exit.void).pipe(Effect.ignore))

      const enter = Effect.gen(function* () {
        const count = yield* Ref.updateAndGet(active, (value) => value + 1)
        yield* Ref.update(maxActive, (value) => Math.max(value, count))
      })
      const leave = Ref.update(active, (value) => value - 1)
      const currentWork = Effect.gen(function* () {
        yield* enter
        yield* Deferred.succeed(started, void 0)
        yield* Effect.never
        return "current"
      }).pipe(
        Effect.onInterrupt(() => Deferred.succeed(interrupted, void 0)),
        Effect.ensuring(leave),
      )

      const controlled = makePublicationScheduler(
        () => events.push(`child-queued:${runner.state._tag}`),
        (fiber) => {
          events.push(`caller-interrupted:${runner.state._tag}:${fiber.pollUnsafe() === undefined}`)
          fiber.interruptUnsafe()
        },
      )
      const caller = Effect.runForkWith(context)(runner.ensureRunning(currentWork), { scheduler: controlled.scheduler })
      events.push(`caller-returned:${runner.state._tag}`)
      expect(events).toEqual(["child-queued:Idle", "caller-interrupted:Idle:true", "caller-returned:Running"])
      expect(runner.state._tag).toBe("Running")

      expect(Exit.isFailure(yield* Fiber.await(caller).pipe(Effect.timeout("1 second")))).toBe(true)
      controlled.flush()
      yield* Deferred.await(started).pipe(Effect.timeout("1 second"))
      expect(runner.busy).toBe(true)

      yield* runner.cancel
      yield* Deferred.await(interrupted).pipe(Effect.timeout("1 second"))
      expect(runner.state._tag).toBe("Idle")
      expect(runner.busy).toBe(false)

      const replacement = Effect.gen(function* () {
        yield* enter
        yield* leave
        return "replacement"
      })
      expect(yield* runner.ensureRunning(replacement)).toBe("replacement")
      expect(yield* Ref.get(maxActive)).toBe(1)
      yield* waitForState(runner, "Idle")
      expect(runner.busy).toBe(false)
      expect(yield* Ref.get(active)).toBe(0)
      expect(yield* Ref.get(maxActive)).toBe(1)
      yield* Scope.close(runScope, Exit.void)
    }),
  )

  it.live(
    "runner cleanup waits for onIdle before releasing a dependent resource",
    Effect.gen(function* () {
      const owner = yield* Scope.make("sequential")
      const events = yield* Ref.make<string[]>([])
      const resourceReleased = yield* Ref.make(false)
      const callbackUsedAfterRelease = yield* Ref.make(false)
      const callbackUses = yield* Ref.make(0)
      const workDone = yield* Deferred.make<void>()
      const idleStarted = yield* Deferred.make<void>()
      const idleRelease = yield* Deferred.make<void>()
      const resourceReleaseDone = yield* Deferred.make<void>()
      const closeDone = yield* Deferred.make<void>()

      yield* Scope.addFinalizer(
        owner,
        Effect.gen(function* () {
          yield* Ref.set(resourceReleased, true)
          yield* Ref.update(events, (items) => [...items, "resource-release"])
          yield* Deferred.succeed(resourceReleaseDone, void 0)
        }),
      )

      const onIdle = Effect.gen(function* () {
        if (yield* Ref.get(resourceReleased)) yield* Ref.set(callbackUsedAfterRelease, true)
        yield* Ref.update(callbackUses, (value) => value + 1)
        yield* Ref.update(events, (items) => [...items, "idle-start"])
        yield* Deferred.succeed(idleStarted, void 0)
        yield* Deferred.await(idleRelease)
        if (yield* Ref.get(resourceReleased)) yield* Ref.set(callbackUsedAfterRelease, true)
        yield* Ref.update(events, (items) => [...items, "idle-end"])
      })
      const runner = Runner.make<string>(owner, { onIdle })

      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(idleRelease, void 0).pipe(Effect.ignore)
          if (owner.state._tag !== "Closed") yield* Scope.close(owner, Exit.void).pipe(Effect.ignore)
        }),
      )

      const work = yield* runner
        .ensureRunning(
          Effect.gen(function* () {
            yield* Ref.update(events, (items) => [...items, "work-done"])
            yield* Deferred.succeed(workDone, void 0)
            return "done"
          }),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(workDone).pipe(Effect.timeout("1 second"))
      yield* Deferred.await(idleStarted).pipe(Effect.timeout("1 second"))
      expect(yield* Ref.get(events)).toEqual(["work-done", "idle-start"])
      expect(yield* Ref.get(resourceReleased)).toBe(false)

      const closing = yield* Scope.close(owner, Exit.void).pipe(
        Effect.ensuring(
          Effect.gen(function* () {
            yield* Ref.update(events, (items) => [...items, "owner-close-done"])
            yield* Deferred.succeed(closeDone, void 0)
          }),
        ),
        Effect.forkChild,
      )
      yield* Effect.yieldNow
      expect(owner.state._tag).toBe("Closed")
      expect(yield* Deferred.isDone(closeDone)).toBe(false)
      expect(yield* Deferred.isDone(resourceReleaseDone)).toBe(false)
      expect(yield* Ref.get(resourceReleased)).toBe(false)

      yield* Deferred.succeed(idleRelease, void 0)
      expect(Exit.isSuccess(yield* Fiber.await(closing).pipe(Effect.timeout("1 second")))).toBe(true)
      expect(yield* Fiber.join(work).pipe(Effect.timeout("1 second"))).toBe("done")
      expect(yield* Deferred.isDone(closeDone)).toBe(true)
      expect(yield* Deferred.isDone(resourceReleaseDone)).toBe(true)
      expect(yield* Ref.get(resourceReleased)).toBe(true)
      expect(yield* Ref.get(callbackUsedAfterRelease)).toBe(false)
      expect(yield* Ref.get(callbackUses)).toBe(1)
      expect(yield* Ref.get(events)).toEqual([
        "work-done",
        "idle-start",
        "idle-end",
        "resource-release",
        "owner-close-done",
      ])
    }),
  )

  it.live(
    "ensureRunning can close its owner scope before termination signalling completes",
    Effect.gen(function* () {
      const owner = yield* Scope.make("sequential")
      const events: Array<string> = []
      yield* Scope.addFinalizer(
        owner,
        Effect.sync(() => events.push("resource-release")),
      )
      const runner = Runner.make<string>(owner)
      const context = yield* Effect.context()
      const guarded = makeGuardedScheduler(1024, 32)
      const caller = Effect.runForkWith(context)(
        Effect.gen(function* () {
          const result = yield* runner.ensureRunning(
            Effect.sync(() => {
              events.push("work-done")
              return "done"
            }),
          )
          events.push("receiver-result")
          events.push("owner-close-start")
          yield* Scope.close(owner, Exit.void)
          events.push("owner-close-done")
          return result
        }),
        { scheduler: guarded.scheduler },
      )

      guarded.flush()
      expect(guarded.yields()).toBe(0)
      expect(guarded.droppedTasks()).toBe(0)
      expect(guarded.pendingTasks()).toBe(0)
      const exit = caller.pollUnsafe()
      expect(exit).toBeDefined()
      if (exit === undefined) throw new Error("owner close did not complete")
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) expect(exit.value).toBe("done")
      expect(events).toEqual([
        "work-done",
        "receiver-result",
        "owner-close-start",
        "resource-release",
        "owner-close-done",
      ])
    }),
  )

  // --- cancel semantics ---

  it.live(
    "cancel interrupts running work",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const started = yield* Deferred.make<void>()
      const fiber = yield* runner
        .ensureRunning(
          Effect.gen(function* () {
            yield* Deferred.succeed(started, void 0)
            return yield* Effect.never.pipe(Effect.as("never"))
          }),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(started)
      expect(runner.busy).toBe(true)
      expect(runner.state._tag).toBe("Running")

      yield* runner.cancel
      expect(runner.busy).toBe(false)

      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  )

  it.live(
    "cancel on idle is a no-op",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      yield* runner.cancel
      expect(runner.busy).toBe(false)
    }),
  )

  it.live(
    "cancel with onInterrupt resolves callers gracefully",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s, { onInterrupt: Effect.succeed("fallback") })
      const fiber = yield* runner.ensureRunning(Effect.never.pipe(Effect.as("never"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Running")

      yield* runner.cancel

      const exit = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) expect(exit.value).toBe("fallback")
    }),
  )

  it.live(
    "cancel waits for shell finalization before resolving queued onInterrupt callers",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const shellStarted = yield* Deferred.make<void>()
      const finalizerStarted = yield* Deferred.make<void>()
      const finalizerRelease = yield* Deferred.make<void>()
      const finalized = yield* Ref.make(false)
      const cancelDone = yield* Deferred.make<void>()
      const runner = Runner.make<string>(s, {
        onInterrupt: Ref.get(finalized).pipe(Effect.map((value) => (value ? "finalized" : "stale"))),
      })

      yield* Effect.gen(function* () {
        const shell = yield* runner
          .startShell(
            Effect.gen(function* () {
              yield* Deferred.succeed(shellStarted, void 0)
              yield* Effect.never
              return "shell"
            }).pipe(
              Effect.ensuring(
                Effect.gen(function* () {
                  yield* Deferred.succeed(finalizerStarted, void 0)
                  yield* Deferred.await(finalizerRelease)
                  yield* Ref.set(finalized, true)
                }),
              ),
            ),
          )
          .pipe(Effect.exit, Effect.forkChild)
        yield* Deferred.await(shellStarted)
        yield* waitForState(runner, "Shell")

        const queued = yield* runner.ensureRunning(Effect.succeed("queued")).pipe(Effect.forkChild)
        yield* waitForState(runner, "ShellThenRun")

        const cancel = yield* runner.cancel.pipe(
          Effect.ensuring(Deferred.succeed(cancelDone, void 0).pipe(Effect.ignore)),
          Effect.forkChild,
        )
        yield* Deferred.await(finalizerStarted)
        expect(yield* Ref.get(finalized)).toBe(false)
        expect(queued.pollUnsafe()).toBeUndefined()
        expect(yield* Deferred.isDone(cancelDone)).toBe(false)

        yield* Deferred.succeed(finalizerRelease, void 0)
        const cancelExit = yield* Fiber.await(cancel)
        expect(Exit.isSuccess(cancelExit)).toBe(true)
        expect(yield* Deferred.isDone(cancelDone)).toBe(true)
        expect(yield* Fiber.join(queued)).toBe("finalized")
        const shellExit = yield* Fiber.join(shell)
        expect(Exit.isSuccess(shellExit)).toBe(true)
        if (Exit.isSuccess(shellExit)) expect(shellExit.value).toBe("finalized")
        expect(runner.busy).toBe(false)
      }).pipe(Effect.ensuring(Deferred.succeed(finalizerRelease, void 0).pipe(Effect.ignore)))
    }),
  )

  it.live(
    "cancel with queued callers resolves all",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s, { onInterrupt: Effect.succeed("fallback") })

      const a = yield* runner.ensureRunning(Effect.never.pipe(Effect.as("x"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Running")
      const b = yield* runner.ensureRunning(Effect.succeed("y")).pipe(Effect.forkChild)
      yield* Effect.yieldNow

      yield* runner.cancel

      const [exitA, exitB] = yield* Effect.all([Fiber.await(a), Fiber.await(b)])
      expect(Exit.isSuccess(exitA)).toBe(true)
      expect(Exit.isSuccess(exitB)).toBe(true)
      if (Exit.isSuccess(exitA)) expect(exitA.value).toBe("fallback")
      if (Exit.isSuccess(exitB)) expect(exitB.value).toBe("fallback")
    }),
  )

  it.live(
    "work can be started after cancel",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const fiber = yield* runner.ensureRunning(Effect.never.pipe(Effect.as("x"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Running")
      yield* runner.cancel
      yield* Fiber.await(fiber)

      const result = yield* runner.ensureRunning(Effect.succeed("after-cancel"))
      expect(result).toBe("after-cancel")
    }),
  )

  it.live(
    "cancel keeps replacement work behind the interrupted run finalizer",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const hit = yield* Deferred.make<void>()
      const finalizerStarted = yield* Deferred.make<void>()
      const hold = yield* Deferred.make<void>()
      const replacementStarted = yield* Deferred.make<void>()
      const replacementRelease = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        const runner = Runner.make<string>(s)
        const first = Effect.never.pipe(
          Effect.onInterrupt(() => Deferred.succeed(hit, undefined)),
          Effect.ensuring(
            Effect.gen(function* () {
              yield* Deferred.succeed(finalizerStarted, void 0)
              yield* Deferred.await(hold)
            }),
          ),
          Effect.as("first"),
        )

        const a = yield* runner.ensureRunning(first).pipe(Effect.exit, Effect.forkChild)
        yield* waitForState(runner, "Running")

        const stop = yield* runner.cancel.pipe(Effect.forkChild)
        yield* Deferred.await(hit).pipe(Effect.timeout("250 millis"))
        yield* Deferred.await(finalizerStarted).pipe(Effect.timeout("250 millis"))

        const shellExit = yield* runner.startShell(Effect.succeed("not-started")).pipe(Effect.exit)
        expect(Exit.isFailure(shellExit)).toBe(true)
        if (Exit.isFailure(shellExit)) expect(Cause.squash(shellExit.cause)).toBeInstanceOf(Runner.Busy)

        const b = yield* runner
          .ensureRunning(
            Effect.gen(function* () {
              yield* Deferred.succeed(replacementStarted, void 0)
              yield* Deferred.await(replacementRelease)
              return "second"
            }),
          )
          .pipe(Effect.forkChild)
        yield* Effect.yieldNow
        expect(yield* Deferred.isDone(replacementStarted)).toBe(false)
        expect(runner.busy).toBe(true)

        yield* Deferred.succeed(hold, undefined)
        yield* Deferred.await(replacementStarted).pipe(Effect.timeout("1 second"))
        const stopExit = yield* Fiber.await(stop).pipe(Effect.timeout("1 second"))
        expect(Exit.isSuccess(stopExit)).toBe(true)

        expect(runner.busy).toBe(true)
        yield* Deferred.succeed(replacementRelease, void 0)
        expect(yield* Fiber.join(b).pipe(Effect.timeout("1 second"))).toBe("second")
        yield* waitForState(runner, "Idle")
        expect(runner.busy).toBe(false)

        const exit = yield* Fiber.join(a)
        expect(Exit.isFailure(exit)).toBe(true)
      }).pipe(
        Effect.ensuring(
          Effect.all([Deferred.succeed(hold, undefined), Deferred.succeed(replacementRelease, void 0)], {
            discard: true,
          }).pipe(Effect.ignore),
        ),
      )
    }),
  )

  it.live(
    "cancel drops pending wake work and concurrent cancels share the held finalizer",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const started = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      const finalizerStarted = yield* Deferred.make<void>()
      const finalizerRelease = yield* Deferred.make<void>()
      const pendingStarted = yield* Deferred.make<void>()
      const cancelADone = yield* Deferred.make<void>()
      const cancelBDone = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        const current = yield* runner
          .ensureRunning(
            Effect.gen(function* () {
              yield* Deferred.succeed(started, void 0)
              yield* Effect.never
              return "current"
            }).pipe(
              Effect.onInterrupt(() => Deferred.succeed(interrupted, void 0)),
              Effect.ensuring(
                Effect.gen(function* () {
                  yield* Deferred.succeed(finalizerStarted, void 0)
                  yield* Deferred.await(finalizerRelease)
                }),
              ),
              Effect.as("current"),
            ),
          )
          .pipe(Effect.exit, Effect.forkChild)
        yield* Deferred.await(started).pipe(Effect.timeout("1 second"))

        yield* runner.wake(
          Effect.gen(function* () {
            yield* Deferred.succeed(pendingStarted, void 0)
            return "pending"
          }),
        )

        const cancelA = yield* runner.cancel.pipe(
          Effect.ensuring(Deferred.succeed(cancelADone, void 0).pipe(Effect.ignore)),
          Effect.forkChild,
        )
        yield* waitForState(runner, "Stopping")
        const cancelB = yield* runner.cancel.pipe(
          Effect.ensuring(Deferred.succeed(cancelBDone, void 0).pipe(Effect.ignore)),
          Effect.forkChild,
        )

        yield* Deferred.await(interrupted).pipe(Effect.timeout("1 second"))
        yield* Deferred.await(finalizerStarted).pipe(Effect.timeout("1 second"))
        expect(runner.busy).toBe(true)
        expect(yield* Deferred.isDone(cancelADone)).toBe(false)
        expect(yield* Deferred.isDone(cancelBDone)).toBe(false)
        expect(yield* Deferred.isDone(pendingStarted)).toBe(false)

        yield* Deferred.succeed(finalizerRelease, void 0)
        expect(Exit.isSuccess(yield* Fiber.await(cancelA).pipe(Effect.timeout("1 second")))).toBe(true)
        expect(Exit.isSuccess(yield* Fiber.await(cancelB).pipe(Effect.timeout("1 second")))).toBe(true)
        yield* Deferred.await(cancelADone).pipe(Effect.timeout("1 second"))
        yield* Deferred.await(cancelBDone).pipe(Effect.timeout("1 second"))
        expect(yield* Deferred.isDone(pendingStarted)).toBe(false)
        expect(runner.busy).toBe(false)
        expect(Exit.isFailure(yield* Fiber.join(current).pipe(Effect.timeout("1 second")))).toBe(true)
      }).pipe(Effect.ensuring(Deferred.succeed(finalizerRelease, void 0).pipe(Effect.ignore)))
    }),
  )

  it.live(
    "wake during stopping waits for cancellation before scheduling replacement work",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const started = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      const finalizerStarted = yield* Deferred.make<void>()
      const finalizerRelease = yield* Deferred.make<void>()
      const wakeSubmitted = yield* Deferred.make<void>()
      const wakeAcknowledged = yield* Deferred.make<void>()
      const replacementStarted = yield* Deferred.make<void>()
      const replacementRelease = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        const current = yield* runner
          .ensureRunning(
            Effect.gen(function* () {
              yield* Deferred.succeed(started, void 0)
              yield* Effect.never
              return "current"
            }).pipe(
              Effect.onInterrupt(() => Deferred.succeed(interrupted, void 0)),
              Effect.ensuring(
                Effect.gen(function* () {
                  yield* Deferred.succeed(finalizerStarted, void 0)
                  yield* Deferred.await(finalizerRelease)
                }),
              ),
              Effect.as("current"),
            ),
          )
          .pipe(Effect.exit, Effect.forkChild)
        yield* Deferred.await(started).pipe(Effect.timeout("1 second"))

        const stop = yield* runner.cancel.pipe(Effect.forkChild)
        yield* Deferred.await(finalizerStarted).pipe(Effect.timeout("1 second"))

        const wake = yield* Effect.gen(function* () {
          yield* Deferred.succeed(wakeSubmitted, void 0)
          yield* runner.wake(
            Effect.gen(function* () {
              yield* Deferred.succeed(replacementStarted, void 0)
              yield* Deferred.await(replacementRelease)
              return "replacement"
            }),
          )
          yield* Deferred.succeed(wakeAcknowledged, void 0)
        }).pipe(Effect.forkChild)
        yield* Deferred.await(wakeSubmitted).pipe(Effect.timeout("1 second"))
        yield* Effect.yieldNow
        expect(yield* Deferred.isDone(wakeAcknowledged)).toBe(false)
        expect(yield* Deferred.isDone(replacementStarted)).toBe(false)
        expect(runner.busy).toBe(true)

        yield* Deferred.succeed(finalizerRelease, void 0)
        expect(Exit.isSuccess(yield* Fiber.await(stop).pipe(Effect.timeout("1 second")))).toBe(true)
        yield* Deferred.await(wakeAcknowledged).pipe(Effect.timeout("1 second"))
        yield* Deferred.await(replacementStarted).pipe(Effect.timeout("1 second"))
        expect(runner.busy).toBe(true)

        yield* Deferred.succeed(replacementRelease, void 0)
        yield* Fiber.await(wake).pipe(Effect.timeout("1 second"))
        expect(Exit.isFailure(yield* Fiber.join(current).pipe(Effect.timeout("1 second")))).toBe(true)
        yield* waitForState(runner, "Idle")
      }).pipe(
        Effect.ensuring(
          Effect.all([Deferred.succeed(finalizerRelease, void 0), Deferred.succeed(replacementRelease, void 0)], {
            discard: true,
          }).pipe(Effect.ignore),
        ),
      )
    }),
  )

  // --- shell semantics ---

  it.live(
    "shell runs exclusively",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const result = yield* runner.startShell(Effect.succeed("shell-done"))
      expect(result).toBe("shell-done")
      expect(runner.busy).toBe(false)
    }),
  )

  it.live(
    "shell rejects when run is active",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const started = yield* Deferred.make<void>()
      const fiber = yield* runner
        .ensureRunning(
          Effect.gen(function* () {
            yield* Deferred.succeed(started, undefined)
            return yield* Effect.never.pipe(Effect.as("x"))
          }),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(started).pipe(Effect.timeout("250 millis"))
      yield* Effect.gen(function* () {
        while (runner.state._tag !== "Running") yield* Effect.yieldNow
      }).pipe(Effect.timeout("250 millis"))

      const exit = yield* runner.startShell(Effect.succeed("nope")).pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)

      yield* runner.cancel
      yield* Fiber.await(fiber).pipe(Effect.timeout("250 millis"))
    }),
  )

  it.live(
    "shell rejects when another shell is running",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const gate = yield* Deferred.make<void>()

      const sh = yield* runner.startShell(Deferred.await(gate).pipe(Effect.as("first"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Shell")

      const exit = yield* runner.startShell(Effect.succeed("second")).pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(Runner.Busy)

      yield* Deferred.succeed(gate, undefined)
      yield* Fiber.await(sh)
    }),
  )

  it.live(
    "cancel interrupts shell",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const gate = yield* Deferred.make<void>()

      const sh = yield* runner.startShell(Deferred.await(gate).pipe(Effect.as("ignored"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Shell")

      const stop = yield* runner.cancel.pipe(Effect.forkChild)
      const stopExit = yield* Fiber.await(stop).pipe(Effect.timeout("250 millis"))
      expect(Exit.isSuccess(stopExit)).toBe(true)
      expect(runner.busy).toBe(false)

      const shellExit = yield* Fiber.await(sh)
      expect(Exit.isFailure(shellExit)).toBe(true)

      yield* Deferred.succeed(gate, undefined).pipe(Effect.ignore)
    }),
  )

  it.live(
    "cancel does not mask shell defects",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s, { onInterrupt: Effect.succeed("interrupted") })
      const ready = yield* Latch.make()

      const sh = yield* runner
        .startShell(
          Effect.gen(function* () {
            yield* ready.open
            return yield* Effect.never.pipe(Effect.as("ignored"))
          }).pipe(Effect.ensuring(Effect.die("boom"))),
          ready,
        )
        .pipe(Effect.forkChild)
      yield* ready.await.pipe(Effect.timeout("250 millis"))

      yield* runner.cancel
      expect(Exit.isFailure(yield* Fiber.await(sh))).toBe(true)
    }),
  )

  it.live(
    "cancel settles a blocked failing shell before concurrent replacement work",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const ready = yield* Latch.make()
      const failureGate = yield* Deferred.make<void>()
      const shellEntered = yield* Deferred.make<void>()
      const replacementStarted = yield* Deferred.make<void>()
      const replacementRelease = yield* Deferred.make<void>()
      const replacementRuns = yield* Ref.make(0)
      const cancelDone = yield* Deferred.make<void>()

      const shell = yield* runner
        .startShell(
          Effect.gen(function* () {
            yield* Deferred.succeed(shellEntered, void 0)
            yield* Deferred.await(failureGate)
            return yield* Effect.uninterruptible(Effect.die("shell-boom"))
          }),
          ready,
        )
        .pipe(Effect.exit, Effect.forkChild)
      yield* Deferred.await(shellEntered).pipe(Effect.timeout("1 second"))
      yield* waitForState(runner, "Shell")
      const readyWaiter = yield* ready.await.pipe(Effect.forkChild)
      yield* Effect.yieldNow
      expect(readyWaiter.pollUnsafe()).toBeUndefined()
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(failureGate, void 0).pipe(Effect.ignore)
          yield* Deferred.succeed(replacementRelease, void 0).pipe(Effect.ignore)
          yield* Fiber.interrupt(readyWaiter).pipe(Effect.ignore)
        }),
      )

      const cancel = yield* runner.cancel.pipe(
        Effect.ensuring(Deferred.succeed(cancelDone, void 0).pipe(Effect.ignore)),
        Effect.forkChild,
      )
      yield* waitForState(runner, "Stopping")

      const replacement = Effect.gen(function* () {
        yield* Ref.update(replacementRuns, (value) => value + 1)
        yield* Deferred.succeed(replacementStarted, void 0)
        yield* Deferred.await(replacementRelease)
        return "replacement"
      })
      const ensure = yield* runner.ensureRunning(replacement).pipe(Effect.exit, Effect.forkChild)
      const wake = yield* runner.wake(replacement).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      expect(yield* Deferred.isDone(replacementStarted)).toBe(false)
      expect(yield* Deferred.isDone(cancelDone)).toBe(false)
      expect(runner.busy).toBe(true)

      expect(yield* Deferred.isDone(failureGate)).toBe(false)
      yield* Deferred.succeed(failureGate, void 0)
      const cancelExit = yield* Fiber.await(cancel).pipe(Effect.timeout("1 second"))
      const shellExit = yield* Fiber.join(shell).pipe(Effect.timeout("1 second"))
      expect(Exit.isSuccess(cancelExit)).toBe(true)
      expect(Exit.isFailure(shellExit)).toBe(true)
      if (Exit.isFailure(shellExit)) expect(Cause.hasDies(shellExit.cause)).toBe(true)
      expect(readyWaiter.pollUnsafe()).toBeUndefined()

      yield* Deferred.await(replacementStarted).pipe(Effect.timeout("1 second"))
      expect(yield* Deferred.isDone(cancelDone)).toBe(true)
      expect(yield* Ref.get(replacementRuns)).toBeGreaterThanOrEqual(1)
      expect(runner.busy).toBe(true)

      yield* Deferred.succeed(replacementRelease, void 0)
      const ensureExit = yield* Fiber.join(ensure).pipe(Effect.timeout("1 second"))
      expect(Exit.isSuccess(ensureExit)).toBe(true)
      if (Exit.isSuccess(ensureExit)) expect(ensureExit.value).toBe("replacement")
      expect(Exit.isSuccess(yield* Fiber.await(wake).pipe(Effect.timeout("1 second")))).toBe(true)
      yield* waitForState(runner, "Idle")
      expect(runner.busy).toBe(false)
      yield* Fiber.interrupt(readyWaiter)
    }),
  )

  // --- shell→run handoff ---

  it.live(
    "ensureRunning queues behind shell then runs after",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const gate = yield* Deferred.make<void>()

      const sh = yield* runner.startShell(Deferred.await(gate).pipe(Effect.as("shell-result"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Shell")
      expect(runner.state._tag).toBe("Shell")

      const run = yield* runner.ensureRunning(Effect.succeed("run-result")).pipe(Effect.forkChild)
      yield* waitForState(runner, "ShellThenRun")
      expect(runner.state._tag).toBe("ShellThenRun")

      yield* Deferred.succeed(gate, undefined)
      yield* Fiber.await(sh)

      const exit = yield* Fiber.await(run)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) expect(exit.value).toBe("run-result")
      expect(runner.state._tag).toBe("Idle")
    }),
  )

  it.live(
    "multiple ensureRunning callers share the queued run behind shell",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const calls = yield* Ref.make(0)
      const gate = yield* Deferred.make<void>()

      const sh = yield* runner.startShell(Deferred.await(gate).pipe(Effect.as("shell"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Shell")

      const work = Effect.gen(function* () {
        yield* Ref.update(calls, (n) => n + 1)
        return "run"
      })
      const a = yield* runner.ensureRunning(work).pipe(Effect.forkChild)
      const b = yield* runner.ensureRunning(work).pipe(Effect.forkChild)
      yield* waitForState(runner, "ShellThenRun")

      yield* Deferred.succeed(gate, undefined)
      yield* Fiber.await(sh)

      const [exitA, exitB] = yield* Effect.all([Fiber.await(a), Fiber.await(b)])
      expect(Exit.isSuccess(exitA)).toBe(true)
      expect(Exit.isSuccess(exitB)).toBe(true)
      expect(yield* Ref.get(calls)).toBe(1)
    }),
  )

  it.live(
    "cancel during shell_then_run cancels both",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)

      const sh = yield* runner.startShell(Effect.never.pipe(Effect.as("aborted"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Shell")

      const run = yield* runner.ensureRunning(Effect.succeed("y")).pipe(Effect.forkChild)
      yield* waitForState(runner, "ShellThenRun")
      expect(runner.state._tag).toBe("ShellThenRun")

      yield* runner.cancel
      expect(runner.busy).toBe(false)

      yield* Fiber.await(sh)
      const exit = yield* Fiber.await(run)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  )

  // --- lifecycle callbacks ---

  it.live(
    "onIdle fires when returning to idle from running",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const count = yield* Ref.make(0)
      const runner = Runner.make<string>(s, {
        onIdle: Ref.update(count, (n) => n + 1),
      })
      yield* runner.ensureRunning(Effect.succeed("ok"))
      expect(yield* Ref.get(count)).toBe(1)
    }),
  )

  it.live(
    "onIdle fires on cancel",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const count = yield* Ref.make(0)
      const runner = Runner.make<string>(s, {
        onIdle: Ref.update(count, (n) => n + 1),
      })
      const fiber = yield* runner.ensureRunning(Effect.never.pipe(Effect.as("x"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Running")
      yield* runner.cancel
      yield* Fiber.await(fiber)
      expect(yield* Ref.get(count)).toBeGreaterThanOrEqual(1)
    }),
  )

  it.live(
    "onIdle completes before replacement work starts and callbacks do not reenter",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const events = yield* Ref.make<string[]>([])
      const callbackCalls = yield* Ref.make(0)
      const activeCallbacks = yield* Ref.make(0)
      const maxActiveCallbacks = yield* Ref.make(0)
      const idleEntered = yield* Deferred.make<void>()
      const idleRelease = yield* Deferred.make<void>()
      const ran = yield* Ref.make<string[]>([])

      const onIdle = Effect.gen(function* () {
        const call = yield* Ref.updateAndGet(callbackCalls, (value) => value + 1)
        const active = yield* Ref.updateAndGet(activeCallbacks, (value) => value + 1)
        yield* Ref.update(maxActiveCallbacks, (value) => Math.max(value, active))
        yield* Ref.update(events, (items) => [...items, "idle-start"])
        if (call === 1) {
          yield* Deferred.succeed(idleEntered, void 0)
          yield* Deferred.await(idleRelease)
        }
        yield* Ref.update(events, (items) => [...items, "idle-end"])
      }).pipe(Effect.ensuring(Ref.update(activeCallbacks, (value) => value - 1)))
      const runner = Runner.make<string>(s, { onIdle })

      yield* Effect.gen(function* () {
        const first = yield* runner.ensureRunning(Effect.succeed("first")).pipe(Effect.forkChild)
        yield* Deferred.await(idleEntered).pipe(Effect.timeout("1 second"))
        expect(yield* Ref.get(activeCallbacks)).toBe(1)

        const ensure = yield* runner
          .ensureRunning(
            Effect.gen(function* () {
              yield* Ref.update(events, (items) => [...items, "ensure-start"])
              yield* Ref.update(ran, (items) => [...items, "ensure"])
              return "ensure"
            }),
          )
          .pipe(Effect.forkChild)
        const wake = yield* runner
          .wake(
            Effect.gen(function* () {
              yield* Ref.update(events, (items) => [...items, "wake-start"])
              yield* Ref.update(ran, (items) => [...items, "wake"])
              return "wake"
            }),
          )
          .pipe(Effect.forkChild)
        yield* Effect.yieldNow
        expect(yield* Ref.get(ran)).toEqual([])

        yield* Deferred.succeed(idleRelease, void 0)
        expect(yield* Fiber.join(first).pipe(Effect.timeout("1 second"))).toBe("first")
        expect(Exit.isSuccess(yield* Fiber.await(ensure).pipe(Effect.timeout("1 second")))).toBe(true)
        expect(Exit.isSuccess(yield* Fiber.await(wake).pipe(Effect.timeout("1 second")))).toBe(true)
        yield* waitForState(runner, "Idle")

        const finalEvents = yield* Ref.get(events)
        const idleEnd = finalEvents.indexOf("idle-end")
        const bodyEvents = finalEvents.filter((event) => event === "ensure-start" || event === "wake-start")
        expect(bodyEvents.length).toBeGreaterThan(0)
        expect(bodyEvents.every((event) => finalEvents.indexOf(event) > idleEnd)).toBe(true)
        expect(yield* Ref.get(maxActiveCallbacks)).toBe(1)
        expect(yield* Ref.get(callbackCalls)).toBeGreaterThanOrEqual(2)
      }).pipe(Effect.ensuring(Deferred.succeed(idleRelease, void 0).pipe(Effect.ignore)))
    }),
  )

  it.live(
    "onBusy fires when shell starts",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const count = yield* Ref.make(0)
      const runner = Runner.make<string>(s, {
        onBusy: Ref.update(count, (n) => n + 1),
      })
      yield* runner.ensureRunning(Effect.succeed("run"))
      yield* runner.wake(Effect.succeed("wake"))
      yield* waitForState(runner, "Idle")
      expect(yield* Ref.get(count)).toBe(0)
      yield* runner.startShell(Effect.succeed("done"))
      expect(yield* Ref.get(count)).toBe(1)
    }),
  )

  // --- busy flag ---

  it.live(
    "busy is true during run",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const gate = yield* Deferred.make<void>()

      const fiber = yield* runner.ensureRunning(Deferred.await(gate).pipe(Effect.as("ok"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Running")
      expect(runner.busy).toBe(true)

      yield* Deferred.succeed(gate, undefined)
      yield* Fiber.await(fiber)
      expect(runner.busy).toBe(false)
    }),
  )

  it.live(
    "busy is true during shell",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const gate = yield* Deferred.make<void>()

      const fiber = yield* runner.startShell(Deferred.await(gate).pipe(Effect.as("ok"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Shell")
      expect(runner.busy).toBe(true)

      yield* Deferred.succeed(gate, undefined)
      yield* Fiber.await(fiber)
      expect(runner.busy).toBe(false)
    }),
  )
})
