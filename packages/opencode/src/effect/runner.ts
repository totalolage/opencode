import { Cause, Context, Deferred, Effect, Exit, Fiber, Latch, Schema, Scope, SynchronizedRef } from "effect"

export interface Runner<A, E = never> {
  readonly state: State<A, E>
  readonly busy: boolean
  readonly ensureRunning: (work: Effect.Effect<A, E>) => Effect.Effect<A, E>
  readonly wake: (work: Effect.Effect<A, E>) => Effect.Effect<void>
  readonly startShell: (work: Effect.Effect<A, E>, ready?: Latch.Latch) => Effect.Effect<A, E | Busy>
  readonly cancel: Effect.Effect<void>
}

export class Cancelled extends Schema.TaggedErrorClass<Cancelled>()("RunnerCancelled", {}) {}
export class Busy extends Schema.TaggedErrorClass<Busy>()("RunnerBusy", {}) {}

interface RunHandle<A, E> {
  id: number
  done: Deferred.Deferred<A, E | Cancelled>
  terminated: Deferred.Deferred<void>
  fiber: Fiber.Fiber<A, E>
}

interface ShellHandle<A, E> {
  id: number
  cancelled: Deferred.Deferred<void>
  terminated: Deferred.Deferred<void>
  ready?: Latch.Latch
  fiber: Fiber.Fiber<A, E>
}

interface PendingHandle<A, E> {
  id: number
  done: Deferred.Deferred<A, E | Cancelled>
  work: Effect.Effect<A, E>
}

type StopTarget<A, E> =
  | { readonly _tag: "Run"; readonly run: RunHandle<A, E> }
  | { readonly _tag: "Shell"; readonly shell: ShellHandle<A, E> }

interface StopHandle<A, E> {
  id: number
  done: Deferred.Deferred<void>
  target: StopTarget<A, E>
}

export type State<A, E> =
  | { readonly _tag: "Idle" }
  | { readonly _tag: "Running"; readonly run: RunHandle<A, E>; readonly pending?: PendingHandle<A, E> }
  | { readonly _tag: "Shell"; readonly shell: ShellHandle<A, E> }
  | { readonly _tag: "ShellThenRun"; readonly shell: ShellHandle<A, E>; readonly run: PendingHandle<A, E> }
  | { readonly _tag: "Stopping"; readonly stop: StopHandle<A, E> }

/**
 * Lifecycle callbacks run while the runner state transition lock is held. They
 * must not call runner methods.
 */
export const make = <A, E = never>(
  scope: Scope.Scope,
  opts?: {
    onIdle?: Effect.Effect<void>
    onBusy?: Effect.Effect<void>
    onInterrupt?: Effect.Effect<A, E>
  },
): Runner<A, E> => {
  // Keep one sequential child scope for the runner lifetime. Its cleanup
  // obligation outlives individual work fibers and their scope finalizers.
  const lifetimeScope = Scope.forkUnsafe(scope, "sequential")
  const ref = SynchronizedRef.makeUnsafe<State<A, E>>({ _tag: "Idle" })
  const idle = opts?.onIdle ?? Effect.void
  const onBusy = opts?.onBusy ?? Effect.void
  const onInterrupt = opts?.onInterrupt
  let ids = 0
  const obligations = new Set<Deferred.Deferred<void>>()

  const state = () => SynchronizedRef.getUnsafe(ref)
  const next = () => {
    ids += 1
    return ids
  }

  const complete = (done: Deferred.Deferred<A, E | Cancelled>, exit: Exit.Exit<A, E>) =>
    Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
      ? Deferred.fail(done, new Cancelled()).pipe(Effect.asVoid)
      : Deferred.done(done, exit).pipe(Effect.asVoid)

  const awaitDone = (done: Deferred.Deferred<A, E | Cancelled>) =>
    Deferred.await(done).pipe(Effect.catchTag("RunnerCancelled", (e) => onInterrupt ?? Effect.die(e)))

  const observe = <B, F>(
    fiber: Fiber.Fiber<B, F>,
    context: Context.Context<never>,
    completion: (exit: Exit.Exit<B, F>) => Effect.Effect<void>,
  ): void => {
    const dispatch = (exit: Exit.Exit<B, F>) => {
      Effect.runForkWith(context)(Effect.uninterruptible(completion(exit)))
    }

    fiber.addObserver(dispatch)
  }

  const awaitObligations = (): Effect.Effect<void> =>
    Effect.suspend(() => {
      const current = Array.from(obligations)
      if (current.length === 0) return Effect.void
      return Effect.all(
        current.map((obligation) => Deferred.await(obligation)),
        { discard: true },
      ).pipe(Effect.flatMap(() => awaitObligations()))
    })

  function startRun(
    work: Effect.Effect<A, E>,
    done: Deferred.Deferred<A, E | Cancelled>,
  ): Effect.Effect<RunHandle<A, E> | undefined> {
    return Effect.gen(function* () {
      if (lifetimeScope.state._tag === "Closed") {
        yield* Deferred.fail(done, new Cancelled()).pipe(Effect.asVoid)
        return undefined
      }

      const id = next()
      const context = yield* Effect.context()
      const terminated = yield* Deferred.make<void>()
      obligations.add(terminated)
      const fiber = yield* work.pipe(Effect.forkIn(lifetimeScope))
      observe(fiber, context, (exit) =>
        Effect.gen(function* () {
          const completion = yield* Effect.exit(finishRun(id, done, exit))
          obligations.delete(terminated)
          yield* Deferred.succeed(terminated, undefined).pipe(Effect.asVoid)
          if (Exit.isFailure(completion)) yield* completion
        }),
      )
      return { id, done, terminated, fiber } satisfies RunHandle<A, E>
    })
  }

  function finishRun(
    id: number,
    done: Deferred.Deferred<A, E | Cancelled>,
    exit: Exit.Exit<A, E>,
  ): Effect.Effect<void> {
    return SynchronizedRef.modifyEffect(
      ref,
      Effect.fnUntraced(function* (st) {
        if (st._tag === "Running" && st.run.id === id) {
          const pending = st.pending
          if (pending && lifetimeScope.state._tag !== "Closed") {
            const nextRun = yield* startRun(pending.work, pending.done)
            if (nextRun === undefined) {
              const idleExit = yield* Effect.exit(idle)
              return [
                Effect.gen(function* () {
                  yield* complete(done, exit)
                  yield* Deferred.fail(pending.done, new Cancelled()).pipe(Effect.asVoid)
                  yield* idleExit
                }),
                { _tag: "Idle" },
              ] as const
            }
            return [
              Effect.gen(function* () {
                yield* complete(done, exit)
              }),
              { _tag: "Running", run: nextRun },
            ] as const
          }
          if (pending) {
            const idleExit = yield* Effect.exit(idle)
            return [
              Effect.gen(function* () {
                yield* complete(done, exit)
                yield* Deferred.fail(pending.done, new Cancelled()).pipe(Effect.asVoid)
                if (Exit.isFailure(idleExit)) yield* idleExit
              }),
              { _tag: "Idle" },
            ] as const
          }

          const idleExit = yield* Effect.exit(idle)
          return [
            Effect.gen(function* () {
              yield* complete(done, exit)
              yield* idleExit
            }),
            { _tag: "Idle" },
          ] as const
        }

        yield* complete(done, exit)
        return [Exit.succeed(undefined), st] as const
      }),
    ).pipe(Effect.flatten)
  }

  function finishShell(id: number): Effect.Effect<void> {
    return SynchronizedRef.modifyEffect(
      ref,
      Effect.fnUntraced(function* (st) {
        if (st._tag === "Shell" && st.shell.id === id) {
          const idleExit = yield* Effect.exit(idle)
          return [idleExit, { _tag: "Idle" }] as const
        }
        if (st._tag === "ShellThenRun" && st.shell.id === id) {
          if (lifetimeScope.state._tag !== "Closed") {
            const nextRun = yield* startRun(st.run.work, st.run.done)
            if (nextRun !== undefined) return [Effect.void, { _tag: "Running", run: nextRun }] as const
          }

          const idleExit = yield* Effect.exit(idle)
          return [
            Effect.gen(function* () {
              yield* Deferred.fail(st.run.done, new Cancelled()).pipe(Effect.asVoid)
              yield* idleExit
            }),
            { _tag: "Idle" },
          ] as const
        }
        return [Exit.succeed(undefined), st] as const
      }),
    ).pipe(Effect.flatten)
  }

  function finishStopping(id: number): Effect.Effect<void> {
    return SynchronizedRef.modifyEffect(
      ref,
      Effect.fnUntraced(function* (st) {
        if (st._tag !== "Stopping" || st.stop.id !== id) {
          return [Exit.succeed(undefined), st] as const
        }

        const idleExit = yield* Effect.exit(idle)
        return [idleExit, { _tag: "Idle" }] as const
      }),
    ).pipe(Effect.flatten)
  }

  const stopShell = (shell: ShellHandle<A, E>) =>
    Effect.gen(function* () {
      const ready = shell.ready
      if (ready) {
        const first = yield* Effect.race(
          ready.await.pipe(Effect.as("Ready" as const)),
          Fiber.await(shell.fiber).pipe(Effect.as("Terminated" as const)),
        )
        if (first === "Terminated") return
      }
      yield* Deferred.succeed(shell.cancelled, undefined).pipe(Effect.asVoid)
      yield* Fiber.interrupt(shell.fiber)
    })

  const stopCurrent = (stop: StopHandle<A, E>, pending?: PendingHandle<A, E>) =>
    Effect.uninterruptible(
      Effect.gen(function* () {
        const interrupted = yield* Effect.exit(
          stop.target._tag === "Run" ? Fiber.interrupt(stop.target.run.fiber) : stopShell(stop.target.shell),
        )
        const terminated = yield* Effect.exit(
          Deferred.await(stop.target._tag === "Run" ? stop.target.run.terminated : stop.target.shell.terminated),
        )
        const pendingExit = pending
          ? yield* Effect.exit(Deferred.fail(pending.done, new Cancelled()).pipe(Effect.asVoid))
          : Exit.succeed(undefined)
        const finalized = yield* Effect.exit(finishStopping(stop.id))
        yield* Deferred.succeed(stop.done, undefined).pipe(Effect.asVoid)
        if (Exit.isFailure(finalized)) yield* finalized
        if (Exit.isFailure(terminated)) yield* terminated
        if (Exit.isFailure(interrupted)) yield* interrupted
        if (Exit.isFailure(pendingExit)) yield* pendingExit
      }),
    )

  const ensureRunning = (work: Effect.Effect<A, E>): Effect.Effect<A, E> =>
    Effect.suspend(() => {
      const st = state()
      if (st._tag === "Stopping") {
        return Deferred.await(st.stop.done).pipe(Effect.flatMap(() => ensureRunning(work)))
      }

      return Effect.uninterruptible(
        SynchronizedRef.modifyEffect(
          ref,
          Effect.fnUntraced(function* (st) {
            switch (st._tag) {
              case "Running":
              case "ShellThenRun":
                return [awaitDone(st.run.done), st] as const
              case "Shell": {
                if (lifetimeScope.state._tag === "Closed") {
                  const done = yield* Deferred.make<A, E | Cancelled>()
                  yield* Deferred.fail(done, new Cancelled()).pipe(Effect.asVoid)
                  return [awaitDone(done), st] as const
                }
                const run = {
                  id: next(),
                  done: yield* Deferred.make<A, E | Cancelled>(),
                  work,
                } satisfies PendingHandle<A, E>
                return [awaitDone(run.done), { _tag: "ShellThenRun", shell: st.shell, run }] as const
              }
              case "Idle": {
                const done = yield* Deferred.make<A, E | Cancelled>()
                const run = yield* startRun(work, done)
                if (run === undefined) return [awaitDone(done), st] as const
                return [awaitDone(done), { _tag: "Running", run }] as const
              }
              case "Stopping":
                return [Deferred.await(st.stop.done).pipe(Effect.flatMap(() => ensureRunning(work))), st] as const
            }
          }),
        ),
      ).pipe(Effect.flatten)
    })

  const wake = (work: Effect.Effect<A, E>): Effect.Effect<void> =>
    Effect.suspend(() => {
      const st = state()
      if (st._tag === "Stopping") {
        return Deferred.await(st.stop.done).pipe(Effect.flatMap(() => wake(work)))
      }

      return Effect.uninterruptible(
        SynchronizedRef.modifyEffect(
          ref,
          Effect.fnUntraced(function* (st) {
            switch (st._tag) {
              case "Idle": {
                const done = yield* Deferred.make<A, E | Cancelled>()
                const run = yield* startRun(work, done)
                if (run === undefined) return [Effect.void, st] as const
                return [Effect.void, { _tag: "Running", run }] as const
              }
              case "Running": {
                if (lifetimeScope.state._tag === "Closed") return [Effect.void, st] as const
                if (st.pending) return [Effect.void, st] as const
                const pending = {
                  id: next(),
                  done: yield* Deferred.make<A, E | Cancelled>(),
                  work,
                } satisfies PendingHandle<A, E>
                return [Effect.void, { _tag: "Running", run: st.run, pending }] as const
              }
              case "Shell": {
                if (lifetimeScope.state._tag === "Closed") return [Effect.void, st] as const
                const run = {
                  id: next(),
                  done: yield* Deferred.make<A, E | Cancelled>(),
                  work,
                } satisfies PendingHandle<A, E>
                return [Effect.void, { _tag: "ShellThenRun", shell: st.shell, run }] as const
              }
              case "ShellThenRun":
                return [Effect.void, st] as const
              case "Stopping":
                return [Deferred.await(st.stop.done).pipe(Effect.flatMap(() => wake(work))), st] as const
            }
          }),
        ),
      ).pipe(Effect.flatten)
    })

  const startShell = (work: Effect.Effect<A, E>, ready?: Latch.Latch): Effect.Effect<A, E | Busy> =>
    Effect.suspend(() => {
      if (state()._tag === "Stopping" || lifetimeScope.state._tag === "Closed") return Effect.fail(new Busy())
      return Effect.uninterruptible(
        SynchronizedRef.modifyEffect(
          ref,
          Effect.fnUntraced(function* (st) {
            if (st._tag !== "Idle" || lifetimeScope.state._tag === "Closed") {
              const reject: Effect.Effect<A, E | Busy> = Effect.fail(new Busy())
              return [reject, st] as const
            }
            yield* onBusy
            const id = next()
            const cancelled = yield* Deferred.make<void>()
            const terminated = yield* Deferred.make<void>()
            const context = yield* Effect.context()
            obligations.add(terminated)
            const fiber = yield* work.pipe(Effect.forkChild)
            const shell = { id, cancelled, terminated, ready, fiber } satisfies ShellHandle<A, E>
            observe(fiber, context, (exit) =>
              Effect.gen(function* () {
                const completion = yield* Effect.exit(finishShell(id))
                obligations.delete(terminated)
                yield* Deferred.succeed(terminated, undefined).pipe(Effect.asVoid)
                if (Exit.isFailure(completion)) yield* completion
              }),
            )
            return [
              Effect.gen(function* () {
                const exit = yield* Fiber.await(fiber)
                yield* Deferred.await(terminated)
                if (Exit.isSuccess(exit)) return exit.value
                if (
                  Cause.hasInterruptsOnly(exit.cause) ||
                  ((yield* Deferred.isDone(cancelled)) && Cause.hasInterrupts(exit.cause) && !Cause.hasDies(exit.cause))
                ) {
                  if (onInterrupt) return yield* onInterrupt
                  return yield* Effect.die(new Cancelled())
                }
                return yield* Effect.failCause(exit.cause)
              }),
              { _tag: "Shell", shell },
            ] as const
          }),
        ),
      ).pipe(Effect.flatten)
    })

  const cancel = Effect.uninterruptible(
    Effect.suspend(() => {
      return SynchronizedRef.modifyEffect(
        ref,
        Effect.fnUntraced(function* (st) {
          switch (st._tag) {
            case "Idle":
              return [Effect.void, st] as const
            case "Stopping":
              return [Deferred.await(st.stop.done), st] as const
            case "Running": {
              const stop = {
                id: next(),
                done: yield* Deferred.make<void>(),
                target: { _tag: "Run", run: st.run } as const,
              } satisfies StopHandle<A, E>
              return [stopCurrent(stop, st.pending), { _tag: "Stopping", stop }] as const
            }
            case "Shell": {
              const stop = {
                id: next(),
                done: yield* Deferred.make<void>(),
                target: { _tag: "Shell", shell: st.shell } as const,
              } satisfies StopHandle<A, E>
              return [stopCurrent(stop), { _tag: "Stopping", stop }] as const
            }
            case "ShellThenRun": {
              const stop = {
                id: next(),
                done: yield* Deferred.make<void>(),
                target: { _tag: "Shell", shell: st.shell } as const,
              } satisfies StopHandle<A, E>
              return [stopCurrent(stop, st.run), { _tag: "Stopping", stop }] as const
            }
          }
        }),
      ).pipe(Effect.flatten)
    }),
  )

  if (lifetimeScope.state._tag !== "Closed") {
    // Register once, before the runner can accept work and outside its state lock.
    Effect.runSync(Scope.addFinalizerExit(lifetimeScope, () => cancel.pipe(Effect.ensuring(awaitObligations()))))
  }

  return {
    get state() {
      return state()
    },
    get busy() {
      return state()._tag !== "Idle"
    },
    ensureRunning,
    wake,
    startShell,
    cancel,
  }
}

export * as Runner from "./runner"
