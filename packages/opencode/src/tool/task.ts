import * as Tool from "./tool"
import DESCRIPTION from "./task.txt"
import { ToolJsonSchema } from "./json-schema"
import { createHash } from "node:crypto"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { BackgroundJob } from "@/background/job"
import { Session } from "@/session/session"
import { SessionID, MessageID } from "../session/schema"
import { MessageV2 } from "../session/message-v2"
import { SessionDelegation } from "@/session/delegation"
import { Agent } from "../agent/agent"
import { deriveSubagentSessionPermission } from "../agent/subagent-permissions"
import type { SessionPrompt } from "../session/prompt"
import { Config } from "@/config/config"
import { Cause, Effect, Exit, Schema } from "effect"
import { EffectBridge } from "@/effect/bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Database } from "@opencode-ai/core/database/database"

export interface TaskPromptOps {
  cancel(sessionID: SessionID): Effect.Effect<void>
  resolvePromptParts(template: string): Effect.Effect<SessionPrompt.PromptInput["parts"]>
  prompt(
    input: SessionPrompt.PromptInput,
    admission?: { generation: Delegation.Generation; workID: Delegation.WorkID },
  ): Effect.Effect<SessionV1.WithParts>
  wake?(sessionID: SessionID): Effect.Effect<void>
}

const id = "task"
const BACKGROUND_DESCRIPTION = [
  "Background mode: background=true launches the subagent asynchronously and returns immediately.",
  "Foreground is the default; use it when you need the result before continuing.",
  "Use background only for independent work that can run while you continue elsewhere.",
  "You will be notified automatically when it finishes.",
].join(" ")
const BACKGROUND_STARTED = [
  "The task is working in the background. You will be notified automatically when it finishes.",
  "DO NOT sleep, poll for progress, ask the task for status, or duplicate this task's work — avoid working with the same files or topics it is using.",
  "Work on non-overlapping tasks, or briefly tell the user what you launched and end your response.",
].join("\n")
const BACKGROUND_UPDATED = [
  "Additional context sent to the running background task.",
  "The task is still working in the background. You will be notified automatically when it finishes.",
  "DO NOT sleep, poll for progress, ask the task for status, or duplicate this task's work — avoid working with the same files or topics it is using.",
  "Work on non-overlapping tasks, or briefly tell the user what you sent and end your response.",
].join("\n")

const BaseParameterFields = {
  description: Schema.String.annotate({ description: "A short (3-5 words) description of the task" }),
  prompt: Schema.String.annotate({ description: "The task for the agent to perform" }),
  subagent_type: Schema.String.annotate({ description: "The type of specialized agent to use for this task" }),
  task_id: Schema.optional(Schema.String).annotate({
    description:
      "This should only be set if you mean to resume a previous task (you can pass a prior task_id and the task will continue the same subagent session as before instead of creating a fresh one)",
  }),
  command: Schema.optional(Schema.String).annotate({ description: "The command that triggered this task" }),
}

const BaseParameters = Schema.Struct(BaseParameterFields)

export const Parameters = Schema.Struct({
  ...BaseParameterFields,
  background: Schema.optional(Schema.Boolean).annotate({
    description:
      "Run the agent in the background. You will be notified when it completes. DO NOT sleep, poll, or proactively check on its progress",
  }),
})

function renderOutput(input: {
  sessionID: SessionID
  state: "running" | "completed" | "error"
  summary?: string
  text: string
}) {
  const tag = input.state === "error" ? "task_error" : "task_result"
  return [
    `<task id="${input.sessionID}" state="${input.state}">`,
    ...(input.summary ? [`<summary>${input.summary}</summary>`] : []),
    `<${tag}>`,
    input.text,
    `</${tag}>`,
    "</task>",
  ].join("\n")
}

export const TaskTool = Tool.define(
  id,
  Effect.gen(function* () {
    const agent = yield* Agent.Service
    const background = yield* BackgroundJob.Service
    const config = yield* Config.Service
    const sessions = yield* Session.Service
    const flags = yield* RuntimeFlags.Service
    const database = yield* Database.Service
    const delegation = yield* DelegationStore.Service
    const sessionDelegation = yield* SessionDelegation.Service

    const run = Effect.fn("TaskTool.execute")(function* (
      params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context,
    ) {
      const cfg = yield* config.get()
      const requestedMode =
        params.background === undefined
          ? undefined
          : params.background
            ? ("background" as const)
            : ("foreground" as const)

      const parent = yield* sessions.get(ctx.sessionID)
      let current = parent
      let depth = 0
      while (current.parentID) {
        depth++
        current = yield* sessions.get(current.parentID)
      }
      if (depth >= (cfg.subagent_depth ?? 1)) {
        return yield* Effect.fail(
          new Error(
            `Subagent depth limit reached (${cfg.subagent_depth ?? 1}). Increase "subagent_depth" to allow nested subagents.`,
          ),
        )
      }

      if (!ctx.extra?.bypassAgentCheck) {
        yield* ctx.ask({
          permission: id,
          patterns: [params.subagent_type],
          always: ["*"],
          metadata: {
            description: params.description,
            subagent_type: params.subagent_type,
          },
        })
      }

      const next = yield* agent.get(params.subagent_type)
      if (!next) {
        return yield* Effect.fail(new Error(`Unknown agent type: ${params.subagent_type} is not a valid agent type`))
      }

      const msg = yield* MessageV2.get({ sessionID: ctx.sessionID, messageID: ctx.messageID }).pipe(
        Effect.provideService(Database.Service, database),
        Effect.orDie,
      )
      if (msg.info.role !== "assistant")
        return yield* Effect.fail(new Error("Task origin must be an assistant message"))
      if (ctx.callID === undefined || ctx.callID.length === 0) {
        return yield* Effect.fail(new Error("Task tool call is missing a callID"))
      }
      const callID = ctx.callID

      const originParts = msg.parts.filter(
        (part): part is SessionV1.ToolPart =>
          part.type === "tool" &&
          part.tool === id &&
          part.callID === callID &&
          part.messageID === ctx.messageID &&
          part.sessionID === ctx.sessionID,
      )
      if (originParts.length !== 1) {
        return yield* Effect.fail(
          new Error(`Task tool call ${ctx.callID} must have exactly one persisted task origin in ${ctx.messageID}`),
        )
      }
      const originPart = originParts[0]
      if (originPart === undefined) {
        return yield* Effect.fail(new Error(`Task tool call ${ctx.callID} has no persisted origin`))
      }

      const digest = createHash("sha256")
        .update(JSON.stringify([ctx.sessionID, ctx.messageID, originPart.id, callID]))
        .digest("hex")
      const requestID = Delegation.RequestID.make(`drq_${digest}`)
      const generationID = Delegation.ID.make(`dlg_${digest}`)
      const taskID = params.task_id
      const explicitReuse = Boolean(taskID)
      const persistedGeneration = yield* delegation.get(generationID)

      const session =
        taskID !== undefined
          ? yield* sessions.get(SessionID.make(taskID)).pipe(
              Effect.catchCause((cause) => {
                if (Cause.hasInterrupts(cause)) return Effect.failCause(cause)
                return Effect.fail(new Error(`Task session not found: ${taskID}`))
              }),
            )
          : persistedGeneration === undefined
            ? undefined
            : yield* sessions.get(persistedGeneration.childID).pipe(
                Effect.catchCause((cause) => {
                  if (Cause.hasInterrupts(cause)) return Effect.failCause(cause)
                  return Effect.fail(new Error(`Task session not found: ${persistedGeneration.childID}`))
                }),
              )
      if (session !== undefined && session.parentID !== ctx.sessionID) {
        return yield* Effect.fail(
          new Error(`Task session ${session.id} is not a child of parent session ${ctx.sessionID}`),
        )
      }

      if (
        persistedGeneration !== undefined &&
        (persistedGeneration.parentID !== ctx.sessionID ||
          String(persistedGeneration.origin.messageID) !== String(ctx.messageID) ||
          String(persistedGeneration.origin.partID) !== String(originPart.id) ||
          persistedGeneration.origin.callID !== callID)
      ) {
        return yield* Effect.fail(new Error(`Task generation ${generationID} has a conflicting persisted origin`))
      }

      const activeChildGeneration = session === undefined ? undefined : yield* delegation.active(session.id)
      const effectiveBackground =
        requestedMode === "background" ||
        (requestedMode === undefined &&
          (activeChildGeneration?.mode === "background" || persistedGeneration?.mode === "background"))
      if (effectiveBackground && !flags.experimentalBackgroundSubagents) {
        return yield* Effect.fail(
          new Error("Background subagents require OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS=true"),
        )
      }

      const ops = ctx.extra?.promptOps as TaskPromptOps | undefined
      if (!ops) return yield* Effect.fail(new Error("TaskTool requires promptOps in ctx.extra"))

      const childPermission = deriveSubagentSessionPermission({
        parentSessionPermission: parent.permission ?? [],
        subagent: next,
      })
      const childToolDenies = [
        ...(next.permission.some((rule) => rule.permission === "todowrite")
          ? []
          : [{ permission: "todowrite" as const, pattern: "*" as const, action: "deny" as const }]),
        ...(next.permission.some((rule) => rule.permission === id)
          ? []
          : [{ permission: id, pattern: "*" as const, action: "deny" as const }]),
        ...(cfg.experimental?.primary_tools?.map((permission) => ({
          permission,
          pattern: "*" as const,
          action: "deny" as const,
        })) ?? []),
      ]
      const nextSession =
        session ??
        (yield* sessions.create({
          parentID: ctx.sessionID,
          title: params.description + ` (@${next.name} subagent)`,
          agent: next.name,
          permission: [
            ...childPermission,
            ...childToolDenies.filter(
              (deny) =>
                !childPermission.some(
                  (rule) =>
                    rule.permission === deny.permission && rule.pattern === deny.pattern && rule.action === deny.action,
                ),
            ),
          ],
        }))

      const variant = msg.info.variant

      const model = next.model ?? {
        modelID: msg.info.modelID,
        providerID: msg.info.providerID,
      }

      return yield* Effect.acquireUseRelease(
        Effect.uninterruptible(
          Effect.gen(function* () {
            const existingWork = yield* delegation.unfinished(nextSession.id)
            const parentGeneration = yield* delegation.active(ctx.sessionID)
            const registration = yield* delegation.register({
              requestID,
              generationID,
              parentID: ctx.sessionID,
              childID: nextSession.id,
              origin: {
                messageID: SessionMessage.ID.make(ctx.messageID),
                partID: Delegation.OriginPartID.make(originPart.id),
                callID,
              },
              ...(parentGeneration === undefined ? {} : { parentGenerationID: parentGeneration.id }),
              ...(requestedMode === undefined ? {} : { mode: requestedMode }),
              explicitReuse,
            })

            return {
              registration,
              ownsWork:
                registration.workState === "active" && !existingWork.some((work) => work.id === registration.workID),
              transferred: false,
            }
          }),
        ),
        (resource) =>
          Effect.gen(function* () {
            const registration = resource.registration
            const runInBackground = registration.generation.mode === "background"
            const metadata = {
              parentSessionId: ctx.sessionID,
              sessionId: nextSession.id,
              model,
              delegationID: registration.generation.id,
              ...(runInBackground ? { background: true } : {}),
            }

            if (registration.workState === "finished") {
              if (originPart.state.status === "completed") {
                return {
                  title: originPart.state.title,
                  metadata: { ...metadata, ...originPart.state.metadata, delegationID: registration.generation.id },
                  output: originPart.state.output,
                  ...(originPart.state.attachments === undefined
                    ? {}
                    : {
                        attachments: originPart.state.attachments.map(
                          ({ id: _, sessionID: __, messageID: ___, ...part }) => part,
                        ),
                      }),
                }
              }
              if (originPart.state.status === "error") {
                return yield* Effect.fail(
                  new Error(`Subagent failed (task_id: ${nextSession.id}): ${originPart.state.error}`),
                )
              }
              return yield* Effect.fail(
                new Error(
                  `Task request ${requestID} already finished without a persisted result; refusing to run it again`,
                ),
              )
            }

            yield* ctx.metadata({
              title: params.description,
              metadata,
            })

            const runTask = Effect.fn("TaskTool.runTask")(function* () {
              const task = Effect.gen(function* () {
                const parts = yield* ops.resolvePromptParts(params.prompt).pipe(
                  Effect.catchCause((cause) => {
                    if (Cause.hasInterrupts(cause)) return Effect.failCause(cause)
                    return Effect.uninterruptible(
                      Effect.gen(function* () {
                        const error = Cause.squash(cause)
                        yield* sessionDelegation
                          .failInput(
                            registration.generation,
                            registration.workID,
                            error instanceof Error ? error.message : String(error),
                          )
                          .pipe(Effect.ignore)
                        if (ops.wake) yield* ops.wake(ctx.sessionID).pipe(Effect.ignore)
                        return yield* Effect.failCause(cause)
                      }),
                    )
                  }),
                )
                const result = yield* ops.prompt(
                  {
                    messageID: MessageID.make(`msg_${digest}`),
                    sessionID: nextSession.id,
                    model: {
                      modelID: model.modelID,
                      providerID: model.providerID,
                    },
                    variant: next.model ? undefined : variant,
                    agent: next.name,
                    parts,
                  },
                  { generation: registration.generation, workID: registration.workID },
                )
                if (result.info.role === "assistant" && result.info.error) {
                  const message =
                    "message" in result.info.error.data && typeof result.info.error.data.message === "string"
                      ? result.info.error.data.message
                      : result.info.error.name
                  return yield* Effect.fail(new Error(`Subagent failed (task_id: ${nextSession.id}): ${message}`))
                }
                const failed = result.parts.findLast((item) => item.type === "tool" && item.state.status === "error")
                if (failed?.type === "tool" && failed.state.status === "error") {
                  return yield* Effect.fail(
                    new Error(`Subagent failed (task_id: ${nextSession.id}): ${failed.state.error}`),
                  )
                }
                return result.parts.findLast((item) => item.type === "text")?.text ?? ""
              })

              return yield* task.pipe(
                Effect.onExit((exit) =>
                  Effect.uninterruptible(
                    Effect.gen(function* () {
                      if (Exit.hasInterrupts(exit)) yield* ops.cancel(nextSession.id).pipe(Effect.ignore)
                      yield* delegation.finishWork(registration.workID).pipe(Effect.ignore)
                      if (ops.wake) yield* ops.wake(nextSession.id).pipe(Effect.ignore)
                    }),
                  ),
                ),
              )
            })

            const backgroundResult = (summary: "started" | "updated") => ({
              title: params.description,
              metadata: {
                ...metadata,
                background: true,
                jobId: nextSession.id,
              },
              output: renderOutput({
                sessionID: nextSession.id,
                state: "running",
                summary: summary === "started" ? "Background task started" : "Background task updated",
                text: summary === "started" ? BACKGROUND_STARTED : BACKGROUND_UPDATED,
              }),
            })

            const waitForForeground = Effect.fn("TaskTool.waitForForeground")(function* (
              summary: "started" | "updated",
            ) {
              const runCancel = yield* EffectBridge.make()
              const cancel = ops.cancel(nextSession.id)

              function onAbort() {
                runCancel.fork(cancel)
              }

              return yield* Effect.acquireUseRelease(
                Effect.sync(() => {
                  ctx.abort.addEventListener("abort", onAbort)
                }),
                () =>
                  Effect.gen(function* () {
                    const result = yield* Effect.raceFirst(
                      background.wait({ id: nextSession.id }).pipe(Effect.map((waited) => waited.info)),
                      background.waitForPromotion(nextSession.id),
                    )
                    if (result?.metadata?.background === true) return backgroundResult(summary)
                    if (result?.status === "error") return yield* Effect.fail(new Error(result.error ?? "Task failed"))
                    if (result?.status === "cancelled") return yield* Effect.fail(new Error("Task cancelled"))
                    return {
                      title: params.description,
                      metadata,
                      output: renderOutput({
                        sessionID: nextSession.id,
                        state: "completed",
                        text: result?.output ?? "",
                      }),
                    }
                  }),
                (_, exit) =>
                  Effect.gen(function* () {
                    if (Exit.hasInterrupts(exit)) {
                      yield* cancel.pipe(Effect.ignore)
                      yield* background.cancel(nextSession.id).pipe(Effect.ignore)
                    }
                  }).pipe(
                    Effect.ensuring(
                      Effect.sync(() => {
                        ctx.abort.removeEventListener("abort", onAbort)
                      }),
                    ),
                  ),
              )
            })

            const runTaskEffect = Effect.interruptible(runTask())
            const summary = yield* Effect.uninterruptible(
              Effect.gen(function* () {
                if (yield* background.extend({ id: nextSession.id, run: runTaskEffect })) {
                  resource.transferred = true
                  return "updated" as const
                }

                yield* background.start({
                  id: nextSession.id,
                  type: id,
                  title: params.description,
                  metadata,
                  onPromote: ctx.metadata({
                    title: params.description,
                    metadata: { ...metadata, background: true, jobId: nextSession.id },
                  }),
                  run: runTaskEffect,
                })
                resource.transferred = true
                return "started" as const
              }),
            )

            if (runInBackground) return backgroundResult(summary)
            return yield* waitForForeground(summary)
          }),
        (resource, exit) =>
          Effect.uninterruptible(
            Effect.gen(function* () {
              if (!resource.ownsWork || resource.transferred) return
              if (Exit.hasInterrupts(exit)) yield* ops.cancel(nextSession.id).pipe(Effect.ignore)
              yield* delegation.finishWork(resource.registration.workID).pipe(Effect.ignore)
              if (ops.wake) yield* ops.wake(nextSession.id).pipe(Effect.ignore)
            }),
          ),
      )
    })

    return {
      description: flags.experimentalBackgroundSubagents
        ? [DESCRIPTION, BACKGROUND_DESCRIPTION].join("\n\n")
        : DESCRIPTION,
      parameters: Parameters,
      jsonSchema: flags.experimentalBackgroundSubagents ? undefined : ToolJsonSchema.fromSchema(BaseParameters),
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)
