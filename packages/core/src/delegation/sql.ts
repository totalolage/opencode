import { sql } from "drizzle-orm"
import { index, integer, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { SessionID } from "@opencode-ai/schema/session-id"

export const DelegationGenerationTable = sqliteTable(
  "delegation_generation",
  {
    id: text().$type<Delegation.ID>().primaryKey(),
    parent_id: text().$type<SessionID>().notNull(),
    child_id: text().$type<SessionID>().notNull(),
    origin: text({ mode: "json" }).$type<Delegation.Origin>().notNull(),
    parent_generation_id: text().$type<Delegation.ID>(),
    mode: text().$type<Delegation.Mode>().notNull(),
    state: text().$type<Delegation.State>().notNull(),
    time_created: integer().notNull(),
    time_closed: integer(),
  },
  (table) => [
    uniqueIndex("delegation_generation_active_child_idx")
      .on(table.child_id)
      .where(sql`${table.state} = 'active'`),
    index("delegation_generation_parent_idx").on(table.parent_id, table.parent_generation_id),
  ],
)

export const DelegationRegistrationTable = sqliteTable("delegation_registration", {
  request_id: text().$type<Delegation.RequestID>().primaryKey(),
  generation_id: text()
    .$type<Delegation.ID>()
    .notNull()
    .references(() => DelegationGenerationTable.id),
  request: text({ mode: "json" }).$type<Delegation.Registration>().notNull(),
  work_id: text().$type<Delegation.WorkID>().notNull(),
})

export const DelegationWorkTable = sqliteTable(
  "delegation_work",
  {
    id: text().$type<Delegation.WorkID>().primaryKey(),
    session_id: text().$type<SessionID>().notNull(),
    generation_id: text()
      .$type<Delegation.ID>()
      .references(() => DelegationGenerationTable.id),
    kind: text().$type<Delegation.WorkKind>().notNull(),
    state: text().$type<Delegation.WorkState>().notNull(),
    time_created: integer().notNull(),
  },
  (table) => [index("delegation_work_generation_state_idx").on(table.generation_id, table.state)],
)

export const DelegationResolutionTable = sqliteTable(
  "delegation_resolution",
  {
    id: text().$type<Delegation.ResolutionID>().primaryKey(),
    generation_id: text()
      .$type<Delegation.ID>()
      .notNull()
      .references(() => DelegationGenerationTable.id),
    source_kind: text().$type<Delegation.Source["kind"]>().notNull(),
    source_id: text().notNull(),
    payload: text().notNull(),
    outcome: text().$type<Delegation.Outcome>().notNull(),
    history_cutoff: text().notNull(),
    consumed: text({ mode: "json" }).$type<Delegation.ResolutionID[]>().notNull(),
    parent_id: text().$type<SessionID>().notNull(),
    child_id: text().$type<SessionID>().notNull(),
    recipient_generation_id: text().$type<Delegation.ID>(),
    message_id: text().$type<SessionMessage.ID>().notNull(),
    time_created: integer().notNull(),
    status: text().$type<Delegation.ResolutionStatus>().notNull(),
    time_admitted: integer(),
    time_consumed: integer(),
    time_resolved: integer(),
    resolved_source_id: text().$type<Delegation.SourceID>(),
    envelope: text({ mode: "json" }).$type<Delegation.Envelope>(),
  },
  (table) => [
    uniqueIndex("delegation_resolution_generation_source_idx").on(
      table.generation_id,
      table.source_kind,
      table.source_id,
    ),
    uniqueIndex("delegation_resolution_message_idx").on(table.message_id),
    index("delegation_resolution_parent_status_idx").on(table.parent_id, table.status),
    index("delegation_resolution_recipient_status_idx").on(table.recipient_generation_id, table.status),
  ],
)

export const DelegationRevocationTable = sqliteTable("delegation_revocation", {
  session_id: text().$type<SessionID>().primaryKey(),
  id: text().$type<Delegation.RevocationID>().notNull(),
  time_revoked: integer().notNull(),
})

export const DelegationSourceTable = sqliteTable(
  "delegation_source",
  {
    id: text().$type<Delegation.SourceID>().primaryKey(),
    session_id: text().$type<SessionID>().notNull(),
    generation_id: text()
      .$type<Delegation.ID>()
      .references(() => DelegationGenerationTable.id),
    source_kind: text().$type<Delegation.Source["kind"]>().notNull(),
    source_id: text().notNull(),
    history_cutoff: text().notNull(),
    consumed: text({ mode: "json" }).$type<Delegation.ResolutionID[]>().notNull(),
    work_id: text()
      .$type<Delegation.WorkID>()
      .notNull()
      .references(() => DelegationWorkTable.id),
    state: text().$type<Delegation.SourceState>().notNull(),
    payload: text(),
    outcome: text().$type<Delegation.Outcome>(),
    time_created: integer().notNull(),
    time_finalized: integer(),
  },
  (table) => [
    uniqueIndex("delegation_source_generation_source_idx").on(table.generation_id, table.source_kind, table.source_id),
    uniqueIndex("delegation_source_root_identity_idx")
      .on(table.session_id, table.source_kind, table.source_id)
      .where(sql`${table.generation_id} IS NULL`),
    uniqueIndex("delegation_source_work_idx").on(table.work_id),
    index("delegation_source_session_state_idx").on(table.session_id, table.state),
    index("delegation_source_generation_state_idx").on(table.generation_id, table.state),
  ],
)
