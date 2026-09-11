export * as Delegation from "./delegation"

import { Schema } from "effect"
import { SessionMessage } from "./session-message"
import { SessionID } from "./session-id"
import { ascending } from "./identifier"
import { optional, statics } from "./schema"

export const ID = Schema.String.annotate({ identifier: "Delegation.ID" })
  .check(Schema.isStartsWith("dlg_"))
  .pipe(Schema.brand("Delegation.ID"))
  .pipe(statics((schema) => ({ create: () => schema.make("dlg_" + ascending()) })))
export type ID = typeof ID.Type

export const ResolutionID = Schema.String.annotate({ identifier: "Delegation.ResolutionID" })
  .check(Schema.isStartsWith("res_"))
  .pipe(Schema.brand("Delegation.ResolutionID"))
  .pipe(statics((schema) => ({ create: () => schema.make("res_" + ascending()) })))
export type ResolutionID = typeof ResolutionID.Type

export const WorkID = Schema.String.annotate({ identifier: "Delegation.WorkID" })
  .check(Schema.isStartsWith("dwk_"))
  .pipe(Schema.brand("Delegation.WorkID"))
  .pipe(statics((schema) => ({ create: () => schema.make("dwk_" + ascending()) })))
export type WorkID = typeof WorkID.Type

export const RequestID = Schema.String.annotate({ identifier: "Delegation.RequestID" })
  .check(Schema.isStartsWith("drq_"))
  .pipe(Schema.brand("Delegation.RequestID"))
  .pipe(statics((schema) => ({ create: () => schema.make("drq_" + ascending()) })))
export type RequestID = typeof RequestID.Type

export const SourceID = Schema.String.annotate({ identifier: "Delegation.SourceID" })
  .check(Schema.isStartsWith("dsrc_"))
  .pipe(Schema.brand("Delegation.SourceID"))
  .pipe(statics((schema) => ({ create: () => schema.make("dsrc_" + ascending()) })))
export type SourceID = typeof SourceID.Type

export const RevocationID = Schema.String.annotate({ identifier: "Delegation.RevocationID" })
  .check(Schema.isStartsWith("drvk_"))
  .pipe(Schema.brand("Delegation.RevocationID"))
  .pipe(statics((schema) => ({ create: () => schema.make("drvk_" + ascending()) })))
export type RevocationID = typeof RevocationID.Type

export const OriginPartID = Schema.String.pipe(Schema.brand("Delegation.OriginPartID")).annotate({
  identifier: "Delegation.OriginPartID",
})
export type OriginPartID = typeof OriginPartID.Type

export const Mode = Schema.Literals(["foreground", "background"]).annotate({ identifier: "Delegation.Mode" })
export type Mode = typeof Mode.Type

export const State = Schema.Literals(["active", "closed", "revoked"]).annotate({ identifier: "Delegation.State" })
export type State = typeof State.Type

export const Outcome = Schema.Literals(["reply", "error", "cancelled"]).annotate({
  identifier: "Delegation.Outcome",
})
export type Outcome = typeof Outcome.Type

export const ResolutionStatus = Schema.Literals(["pending", "admitted", "consumed", "resolved", "revoked"]).annotate({
  identifier: "Delegation.ResolutionStatus",
})
export type ResolutionStatus = typeof ResolutionStatus.Type

export const SourceState = Schema.Literals(["reserved", "finalized", "discarded"]).annotate({
  identifier: "Delegation.SourceState",
})
export type SourceState = typeof SourceState.Type

export const WorkKind = Schema.Literals(["launch", "update", "runtime", "provider", "tool", "input"]).annotate({
  identifier: "Delegation.WorkKind",
})
export type WorkKind = typeof WorkKind.Type

export const WorkState = Schema.Literals(["active", "finished"]).annotate({ identifier: "Delegation.WorkState" })
export type WorkState = typeof WorkState.Type

export const Origin = Schema.Struct({
  messageID: SessionMessage.ID,
  partID: OriginPartID,
  callID: Schema.String,
}).annotate({ identifier: "Delegation.Origin" })
export interface Origin extends Schema.Schema.Type<typeof Origin> {}

export const Generation = Schema.Struct({
  id: ID,
  parentID: SessionID,
  childID: SessionID,
  origin: Origin,
  parentGenerationID: ID.pipe(optional),
  mode: Mode,
  state: State,
  timeCreated: Schema.Finite,
  timeClosed: Schema.Finite.pipe(optional),
}).annotate({ identifier: "Delegation.Generation" })
export interface Generation extends Schema.Schema.Type<typeof Generation> {}

export const Source = Schema.Struct({
  kind: Schema.Literals(["assistant", "terminal"]),
  id: Schema.String,
}).annotate({ identifier: "Delegation.Source" })
export interface Source extends Schema.Schema.Type<typeof Source> {}

export const Capture = Schema.Struct({
  source: Source,
  payload: Schema.String,
  outcome: Outcome,
  historyCutoff: Schema.String,
  consumed: Schema.Array(ResolutionID),
}).annotate({ identifier: "Delegation.Capture" })
export interface Capture extends Schema.Schema.Type<typeof Capture> {}

export const SourceRecord = Schema.Struct({
  id: SourceID,
  sessionID: SessionID,
  generationID: ID.pipe(optional),
  source: Source,
  historyCutoff: Schema.String,
  consumed: Schema.Array(ResolutionID),
  workID: WorkID,
  state: SourceState,
  payload: Schema.String.pipe(optional),
  outcome: Outcome.pipe(optional),
  timeCreated: Schema.Finite,
  timeFinalized: Schema.Finite.pipe(optional),
}).annotate({ identifier: "Delegation.SourceRecord" })
export interface SourceRecord extends Schema.Schema.Type<typeof SourceRecord> {}

export const RecipientContent = Schema.Struct({
  message: Schema.Json,
  parts: Schema.Array(Schema.Json),
}).annotate({ identifier: "Delegation.RecipientContent" })
export interface RecipientContent extends Schema.Schema.Type<typeof RecipientContent> {}

export const Provenance = Schema.Struct({
  generationID: ID,
  parentID: SessionID,
  childID: SessionID,
  recipientGenerationID: ID.pipe(optional),
  origin: Origin,
  source: Source,
  historyCutoff: Schema.String,
  consumed: Schema.Array(ResolutionID),
}).annotate({ identifier: "Delegation.Provenance" })
export interface Provenance extends Schema.Schema.Type<typeof Provenance> {}

export const Envelope = Schema.Struct({
  message: Schema.Struct({
    id: SessionMessage.ID,
    data: Schema.Json,
  }),
  parts: Schema.Array(
    Schema.Struct({
      id: OriginPartID,
      data: Schema.Json,
    }),
  ),
  provenance: Provenance,
}).annotate({ identifier: "Delegation.Envelope" })
export interface Envelope extends Schema.Schema.Type<typeof Envelope> {}

export const Resolution = Schema.Struct({
  ...Capture.fields,
  id: ResolutionID,
  generationID: ID,
  parentID: SessionID,
  childID: SessionID,
  recipientGenerationID: ID.pipe(optional),
  messageID: SessionMessage.ID,
  timeCreated: Schema.Finite,
  status: ResolutionStatus,
  timeAdmitted: Schema.Finite.pipe(optional),
  timeConsumed: Schema.Finite.pipe(optional),
  timeResolved: Schema.Finite.pipe(optional),
  resolvedSourceID: SourceID.pipe(optional),
  envelope: Envelope.pipe(optional),
}).annotate({ identifier: "Delegation.Resolution" })
export interface Resolution extends Schema.Schema.Type<typeof Resolution> {}

export const Registration = Schema.Struct({
  requestID: RequestID,
  generationID: ID,
  parentID: SessionID,
  childID: SessionID,
  origin: Origin,
  parentGenerationID: ID.pipe(optional),
  mode: Mode.pipe(optional),
  explicitReuse: Schema.Boolean,
}).annotate({ identifier: "Delegation.Registration" })
export interface Registration extends Schema.Schema.Type<typeof Registration> {}

export const Work = Schema.Struct({
  id: WorkID,
  sessionID: SessionID,
  generationID: ID.pipe(optional),
  state: WorkState,
  kind: WorkKind,
  timeCreated: Schema.Finite,
}).annotate({ identifier: "Delegation.Work" })
export interface Work extends Schema.Schema.Type<typeof Work> {}

export class Error extends Schema.TaggedErrorClass<Error>()("Delegation.Error", {
  code: Schema.String,
  message: Schema.String,
}) {}

export class AdapterError extends Schema.TaggedErrorClass<AdapterError>()("Delegation.AdapterError", {
  code: Schema.String,
  message: Schema.String,
}) {}
