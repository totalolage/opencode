import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { InstanceState } from "@/effect/instance-state"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Runner } from "@/effect/runner"
import { BackgroundJob } from "@/background/job"
import { Deferred, Effect, Exit, Latch, Layer, Scope, Context } from "effect"
import { SessionDelegation } from "./delegation"
import { SessionDelegationDelivery } from "./delegation-delivery"
import { SessionDelegationStop } from "./delegation-stop"
import { Session } from "./session"
import { MessageID, SessionID } from "./schema"
import { SessionStatus } from "./status"

type Cancellation = {
  readonly done: Deferred.Deferred<void>
  readonly results: Map<SessionID, Exit.Exit<SessionV1.WithParts>>
}

export interface Interface {
  readonly assertNotBusy: (sessionID: SessionID) => Effect.Effect<void, Session.BusyError>
  readonly busy: (sessionID: SessionID) => Effect.Effect<boolean>
  readonly cancel: (sessionID: SessionID) => Effect.Effect<void>
  readonly cancelFromTask: (sessionID: SessionID) => Effect.Effect<void>
  readonly awaitCancellation: (sessionID: SessionID) => Effect.Effect<void>
  readonly ensureRunning: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<SessionV1.WithParts>,
    advisory?: Effect.Effect<SessionV1.WithParts>,
  ) => Effect.Effect<SessionV1.WithParts>
  readonly wake: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<SessionV1.WithParts>,
  ) => Effect.Effect<void>
  readonly startShell: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<SessionV1.WithParts>,
    ready?: Latch.Latch,
  ) => Effect.Effect<SessionV1.WithParts, Session.BusyError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionRunState") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const background = yield* BackgroundJob.Service
    const status = yield* SessionStatus.Service
    const core = yield* DelegationStore.Service
    const adapter = yield* SessionDelegation.Service
    const delivery = yield* SessionDelegationDelivery.Service
    const stopHelper = yield* SessionDelegationStop.Service

    const state = yield* InstanceState.make(
      Effect.fn("SessionRunState.state")(function* () {
        const scope = yield* Scope.Scope
        const runners = new Map<SessionID, Runner.Runner<SessionV1.WithParts>>()
        const advisory = new Map<SessionID, Effect.Effect<SessionV1.WithParts>>()
        const interrupts = new Map<SessionID, Effect.Effect<SessionV1.WithParts>>()
        const stopping = new Map<SessionID, Cancellation>()
        yield* Effect.addFinalizer(
          Effect.fnUntraced(function* () {
            yield* Effect.forEach(runners.values(), (runner) => runner.cancel, {
              concurrency: "unbounded",
              discard: true,
            })
            runners.clear()
            advisory.clear()
            interrupts.clear()
            stopping.clear()
          }),
        )
        return { runners, advisory, interrupts, stopping, scope }
      }),
    )

    const runner = Effect.fn("SessionRunState.runner")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
    ) {
      const data = yield* InstanceState.get(state)
      const existing = data.runners.get(sessionID)
      if (existing) return existing
      data.interrupts.set(sessionID, onInterrupt)
      const next = Runner.make<SessionV1.WithParts>(data.scope, {
        onIdle: Effect.gen(function* () {
          yield* status.set(sessionID, { type: "idle" })
        }),
        onBusy: status.set(sessionID, { type: "busy" }),
        onInterrupt: Effect.gen(function* () {
          const cancellation = data.stopping.get(sessionID)
          if (cancellation === undefined) return yield* onInterrupt
          yield* Deferred.await(cancellation.done)
          const result = cancellation.results.get(sessionID)
          if (result !== undefined) return yield* result
          return yield* onInterrupt
        }),
      })
      data.runners.set(sessionID, next)
      return next
    })

    const assertNotBusy = Effect.fn("SessionRunState.assertNotBusy")(function* (sessionID: SessionID) {
      const data = yield* InstanceState.get(state)
      const existing = data.runners.get(sessionID)
      if (existing?.busy) yield* busyError(sessionID)
    })

    const busy = Effect.fn("SessionRunState.busy")(function* (sessionID: SessionID) {
      const data = yield* InstanceState.get(state)
      return data.stopping.has(sessionID) || data.runners.get(sessionID)?.busy || false
    })

    const cancel = Effect.fn("SessionRunState.cancel")(function* (sessionID: SessionID) {
      const data = yield* InstanceState.get(state)
      return yield* Effect.suspend(() => {
        const existing = data.stopping.get(sessionID)
        if (existing !== undefined) return Deferred.await(existing.done)

        const cancellation: Cancellation = {
          done: Deferred.makeUnsafe<void>(),
          results: new Map(),
        }
        data.stopping.set(sessionID, cancellation)
        const affected = new Set<SessionID>([sessionID])
        return Effect.uninterruptible(
          Effect.gen(function* () {
            const reserved = yield* core.unfinishedSources(sessionID).pipe(Effect.orDie)
            const stop = yield* core.revokeDescendants(sessionID).pipe(Effect.orDie)
            stop.sessionIDs.forEach((id) => affected.add(id))

            const durableAffected = new Set(affected)
            const jobs = yield* background.list()
            const unregisteredChildren = new Map<SessionID, SessionID[]>()
            for (const job of jobs) {
              if (typeof job.metadata?.delegationID === "string") continue
              const parentID =
                typeof job.metadata?.parentSessionId === "string"
                  ? SessionID.make(job.metadata.parentSessionId)
                  : undefined
              const childID =
                typeof job.metadata?.sessionId === "string" ? SessionID.make(job.metadata.sessionId) : undefined
              if (parentID === undefined || childID === undefined) continue
              const children = unregisteredChildren.get(parentID)
              if (children === undefined) unregisteredChildren.set(parentID, [childID])
              else children.push(childID)
            }

            const pending = Array.from(affected)
            for (const parentID of pending) {
              for (const childID of unregisteredChildren.get(parentID) ?? []) {
                if (affected.has(childID)) continue
                affected.add(childID)
                pending.push(childID)
              }
            }

            const preexisting = new Set<Cancellation>()
            for (const id of affected) {
              const existing = data.stopping.get(id)
              if (existing === undefined) {
                data.stopping.set(id, cancellation)
                continue
              }
              if (existing !== cancellation) preexisting.add(existing)
            }

            const matchingJobs = jobs.filter((job) => {
              if (job.status !== "running") return false
              const jobSessionID =
                typeof job.metadata?.sessionId === "string" ? SessionID.make(job.metadata.sessionId) : undefined
              if (typeof job.metadata?.delegationID === "string") {
                return jobSessionID !== undefined && durableAffected.has(jobSessionID)
              }
              if (affected.has(SessionID.make(job.id))) return true
              if (jobSessionID !== undefined && affected.has(jobSessionID)) return true
              const parentID =
                typeof job.metadata?.parentSessionId === "string"
                  ? SessionID.make(job.metadata.parentSessionId)
                  : undefined
              return parentID !== undefined && affected.has(parentID)
            })

            yield* Effect.all(
              [
                ...Array.from(affected, (id) => {
                  const current = data.runners.get(id)
                  if (current === undefined) return status.set(id, { type: "idle" })
                  return current.cancel
                }),
                ...matchingJobs.map((job) => background.cancel(job.id).pipe(Effect.asVoid)),
              ],
              { concurrency: "unbounded", discard: true },
            )
            yield* Effect.all(
              Array.from(preexisting, (entry) => Deferred.await(entry.done)),
              {
                concurrency: "unbounded",
                discard: true,
              },
            )

            for (const id of affected) {
              const messageIDs = yield* stopHelper.projectRevoked(id).pipe(Effect.orDie)
              for (const messageID of messageIDs) {
                yield* delivery.announce(id, messageID).pipe(Effect.orDie)
              }
            }

            const parentIDs = yield* stopHelper.finish(sessionID, stop, reserved).pipe(Effect.orDie)
            const sources = yield* core.sources(sessionID).pipe(Effect.orDie)
            for (const source of sources) {
              if (
                !reserved.some((reservation) => reservation.id === source.id) ||
                source.state !== "finalized" ||
                source.outcome !== "cancelled" ||
                source.source.kind !== "assistant"
              )
                continue
              yield* delivery.announce(sessionID, MessageID.make(source.source.id)).pipe(Effect.orDie)
            }
            for (const parentID of parentIDs) {
              yield* delivery.deliver(parentID).pipe(Effect.orDie)
              const parentAdvisory = data.advisory.get(parentID)
              const parentRunner = data.runners.get(parentID)
              if (parentAdvisory && parentRunner) yield* parentRunner.wake(parentAdvisory)
            }
            yield* adapter.close(sessionID, true).pipe(Effect.orDie)
            for (const id of affected) {
              if (data.stopping.get(id) !== cancellation) continue
              const fallback = data.interrupts.get(id)
              if (fallback === undefined) continue
              const result = yield* fallback.pipe(Effect.exit)
              if (data.stopping.get(id) === cancellation) cancellation.results.set(id, result)
            }
          }).pipe(
            Effect.onExit((exit) =>
              Effect.gen(function* () {
                yield* Effect.sync(() => {
                  for (const id of affected) {
                    if (data.stopping.get(id) === cancellation) data.stopping.delete(id)
                  }
                })
                yield* Deferred.done(cancellation.done, exit).pipe(Effect.asVoid)
              }),
            ),
          ),
        )
      })
    })

    const cancelFromTask = Effect.fn("SessionRunState.cancelFromTask")(function* (sessionID: SessionID) {
      const data = yield* InstanceState.get(state)
      if (data.stopping.has(sessionID)) return
      yield* cancel(sessionID)
    })

    const awaitCancellation = Effect.fn("SessionRunState.awaitCancellation")(function* (sessionID: SessionID) {
      const data = yield* InstanceState.get(state)
      const cancellation = data.stopping.get(sessionID)
      if (cancellation === undefined) return
      yield* Deferred.await(cancellation.done)
    })

    const ensureRunning = Effect.fn("SessionRunState.ensureRunning")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      work: Effect.Effect<SessionV1.WithParts>,
      advisory?: Effect.Effect<SessionV1.WithParts>,
    ) {
      const data = yield* InstanceState.get(state)
      if (advisory) data.advisory.set(sessionID, advisory)
      yield* awaitCancellation(sessionID)
      const current = yield* runner(sessionID, onInterrupt)
      return yield* current.ensureRunning(work)
    })

    const wake = Effect.fn("SessionRunState.wake")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      work: Effect.Effect<SessionV1.WithParts>,
    ) {
      const data = yield* InstanceState.get(state)
      data.advisory.set(sessionID, work)
      yield* awaitCancellation(sessionID)
      const current = yield* runner(sessionID, onInterrupt)
      yield* current.wake(work)
    })

    const startShell = Effect.fn("SessionRunState.startShell")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      work: Effect.Effect<SessionV1.WithParts>,
      ready?: Latch.Latch,
    ) {
      const data = yield* InstanceState.get(state)
      if (data.stopping.has(sessionID)) return yield* Effect.fail(busyError(sessionID))
      yield* awaitCancellation(sessionID)
      const current = yield* runner(sessionID, onInterrupt)
      return yield* current
        .startShell(work, ready)
        .pipe(Effect.catchTag("RunnerBusy", () => Effect.fail(busyError(sessionID))))
    })

    return Service.of({
      assertNotBusy,
      awaitCancellation,
      busy,
      cancel,
      cancelFromTask,
      ensureRunning,
      wake,
      startShell,
    })
  }),
)

function busyError(sessionID: SessionID) {
  return new Session.BusyError({ sessionID })
}

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [
    BackgroundJob.node,
    SessionStatus.node,
    DelegationStore.node,
    SessionDelegation.node,
    SessionDelegationDelivery.node,
    SessionDelegationStop.node,
  ],
})

export * as SessionRunState from "./run-state"
