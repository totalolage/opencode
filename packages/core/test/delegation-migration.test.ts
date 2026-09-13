import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { sql } from "drizzle-orm"
import { Effect, Exit } from "effect"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { DelegationStore } from "@opencode-ai/core/delegation"
import { Database } from "@opencode-ai/core/database/database"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { migrations } from "@opencode-ai/core/database/migration.gen"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { Delegation } from "@opencode-ai/schema/delegation"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { SessionID } from "@opencode-ai/schema/session-id"
import type { SqlClient } from "effect/unstable/sql/SqlClient"
import { tmpdir } from "./fixture/tmpdir"

const makeDb = EffectDrizzleSqlite.makeWithDefaults()
const delegationMigrationID = "20260910173405_delegation"
const delegationTables = [
  "delegation_generation",
  "delegation_registration",
  "delegation_resolution",
  "delegation_revocation",
  "delegation_source",
  "delegation_work",
]
const delegationIndexes = [
  "delegation_generation_active_child_idx",
  "delegation_generation_parent_idx",
  "delegation_resolution_generation_source_idx",
  "delegation_resolution_message_idx",
  "delegation_resolution_parent_status_idx",
  "delegation_resolution_recipient_status_idx",
  "delegation_source_generation_source_idx",
  "delegation_source_generation_state_idx",
  "delegation_source_root_identity_idx",
  "delegation_source_session_state_idx",
  "delegation_source_work_idx",
  "delegation_work_generation_state_idx",
]

const runSqlite = <A, E>(filename: string, effect: Effect.Effect<A, E, SqlClient>) =>
  Effect.runPromise(
    effect.pipe(Effect.provide(SqliteClient.layer({ filename, disableWAL: true })), Effect.scoped),
  )

const runDatabase = <A, E>(filename: string, effect: Effect.Effect<A, E, Database.Service>) =>
  Effect.runPromise(effect.pipe(Effect.provide(Database.layerFromPath(filename)), Effect.scoped))

function sessionID(name: string) {
  return SessionID.make(`ses_delegation_migration_${name}`)
}

function registration(name: string, parentID: SessionID, childID: SessionID, mode: Delegation.Mode = "background") {
  return {
    requestID: Delegation.RequestID.create(),
    generationID: Delegation.ID.create(),
    parentID,
    childID,
    origin: {
      messageID: SessionMessage.ID.make(`msg_delegation_migration_${name}`),
      partID: Delegation.OriginPartID.make(`part_delegation_migration_${name}`),
      callID: `call_delegation_migration_${name}`,
    },
    mode,
    explicitReuse: false,
  }
}

function content(name: string): Delegation.RecipientContent {
  return {
    message: { kind: "synthetic", text: `delegation migration ${name}` },
    parts: [{ kind: "text", text: `delegation migration ${name}` }],
  }
}

function assistantSource(name: string): Delegation.Source {
  return { kind: "assistant", id: `assistant-delegation-migration-${name}` }
}

function expectFailure<A, E>(exit: Exit.Exit<A, E>) {
  expect(Exit.isFailure(exit)).toBe(true)
}

test("applies the delegation migration incrementally and preserves the durable API across reopen", async () => {
  await using temporary = await tmpdir()
  const filename = `${temporary.path}/delegation-migration.sqlite`
  const parent = sessionID("parent")
  const child = sessionID("child")
  const delegationIndex = migrations.findIndex((migration) => migration.id === delegationMigrationID)

  expect(migrations.filter((migration) => migration.id.includes("delegation")).map((migration) => migration.id)).toEqual([
    delegationMigrationID,
  ])
  expect(delegationIndex).toBeGreaterThan(0)

  await runSqlite(
    filename,
    Effect.gen(function* () {
      const db = yield* makeDb
      yield* DatabaseMigration.applyOnly(db, migrations.slice(0, delegationIndex))

      expect(
        yield* db.all<{ name: string }>(
          sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'delegation_%' ORDER BY name`,
        ),
      ).toEqual([])
      expect(yield* db.get<{ count: number }>(sql`SELECT count(*) AS count FROM migration`)).toEqual({
        count: delegationIndex,
      })

      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/delegation-migration"), sandboxes: [] })
        .run()
      yield* db
        .insert(SessionTable)
        .values([
          {
            id: parent,
            project_id: Project.ID.global,
            parent_id: null,
            slug: parent,
            directory: "/delegation-migration",
            title: "migration parent",
            version: "test",
          },
          {
            id: child,
            project_id: Project.ID.global,
            parent_id: parent,
            slug: child,
            directory: "/delegation-migration",
            title: "migration child",
            version: "test",
          },
        ])
        .run()
    }),
  )

  await runDatabase(
    filename,
    Effect.gen(function* () {
      const database = yield* Database.Service
      const db = database.db
      const store = DelegationStore.make(db)

      expect(
        yield* db.all<{ name: string }>(
          sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'delegation_%' ORDER BY name`,
        ),
      ).toEqual(delegationTables.map((name) => ({ name })))
      expect(
        yield* db.all<{ name: string }>(
          sql`SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'delegation_%' ORDER BY name`,
        ),
      ).toEqual(delegationIndexes.map((name) => ({ name })))
      expect(yield* db.get<{ count: number }>(sql`SELECT count(*) AS count FROM migration`)).toEqual({
        count: migrations.length,
      })
      expect(
        yield* db.get<{ count: number }>(sql`SELECT count(*) AS count FROM migration WHERE id = ${delegationMigrationID}`),
      ).toEqual({ count: 1 })
      expect(yield* db.get(sql`SELECT id, worktree FROM project WHERE id = ${Project.ID.global}`)).toEqual({
        id: Project.ID.global,
        worktree: "/delegation-migration",
      })
      expect(yield* db.get(sql`SELECT id, parent_id, title FROM session WHERE id = ${parent}`)).toEqual({
        id: parent,
        parent_id: null,
        title: "migration parent",
      })
      expect(yield* db.get(sql`SELECT id, parent_id, title FROM session WHERE id = ${child}`)).toEqual({
        id: child,
        parent_id: parent,
        title: "migration child",
      })

      const registered = yield* store.register(registration("child", parent, child))
      const duplicateChild = yield* store
        .register(registration("duplicate-child", parent, child, "foreground"))
        .pipe(Effect.exit)
      expectFailure(duplicateChild)
      yield* store.finishWork(registered.workID)

      const sourceInput = {
        sessionID: child,
        generationID: registered.generation.id,
        source: assistantSource("child"),
        historyCutoff: "history-migration-child",
        consumed: [],
      } as const
      const source = yield* store.reserveSource(sourceInput)
      const sourceRetry = yield* store.reserveSource(sourceInput)
      expect(sourceRetry.created).toBe(false)
      expect(sourceRetry.source.id).toBe(source.source.id)
      expect(
        yield* db.get<{ count: number }>(
          sql`SELECT count(*) AS count FROM delegation_source WHERE id = ${source.source.id}`,
        ),
      ).toEqual({ count: 1 })
      expect(
        yield* db.get(sql`SELECT session_id, generation_id, kind, state FROM delegation_work WHERE id = ${source.source.workID}`),
      ).toEqual({
        session_id: child,
        generation_id: registered.generation.id,
        kind: "provider",
        state: "active",
      })

      const finalized = yield* store.finalizeSource(source.source.id, {
        payload: "migration child reply",
        outcome: "reply",
      })
      if (finalized.resolution === undefined) return yield* Effect.die("expected a delegated resolution")
      const prepared = yield* store.prepare(finalized.resolution.id, content("child"))
      if (prepared.envelope === undefined) return yield* Effect.die("expected a prepared envelope")
      expect(prepared.envelope.provenance.consumed).toEqual([])
      expect(
        yield* db.get<{ envelope_present: number; consumed_count: number }>(sql`
          SELECT envelope IS NOT NULL AS envelope_present, json_array_length(consumed) AS consumed_count
          FROM delegation_resolution
          WHERE id = ${prepared.id}
        `),
      ).toEqual({ envelope_present: 1, consumed_count: 0 })

      yield* db.run(
        sql`CREATE TABLE migration_test_receipt (id TEXT PRIMARY KEY, envelope TEXT NOT NULL)`,
      )
      const admitted = yield* store.admit(prepared.id, (tx, resolution) =>
        Effect.gen(function* () {
          yield* tx.run(
            sql`INSERT INTO migration_test_receipt (id, envelope) VALUES (${resolution.messageID}, ${JSON.stringify(resolution.envelope)})`,
          )
          return { status: "admitted" as const }
        }),
      )
      expect(admitted).toEqual({ status: "admitted" })
      yield* store.markConsumed({ parentID: parent, ids: [prepared.id] })

      const rootSource = yield* store.reserveSource({
        sessionID: parent,
        source: assistantSource("root"),
        historyCutoff: "history-migration-root",
        consumed: [prepared.id],
      })
      expect(
        yield* db.get(sql`SELECT session_id, generation_id, json_array_length(consumed) AS consumed_count, state
          FROM delegation_source WHERE id = ${rootSource.source.id}`),
      ).toEqual({
        session_id: parent,
        generation_id: null,
        consumed_count: 1,
        state: "reserved",
      })
      const rootFinalized = yield* store.finalizeSource(rootSource.source.id, {
        payload: "migration root reply",
        outcome: "reply",
      })
      expect(rootFinalized.resolution).toBeUndefined()
      expect((yield* store.getResolution(prepared.id))?.status).toBe("resolved")
      expect(
        yield* db.get(sql`SELECT session_id, generation_id, json_array_length(consumed) AS consumed_count, state
          FROM delegation_source WHERE id = ${rootSource.source.id}`),
      ).toEqual({
        session_id: parent,
        generation_id: null,
        consumed_count: 1,
        state: "finalized",
      })
      expect(yield* db.get(sql`SELECT count(*) AS count FROM migration_test_receipt`)).toEqual({ count: 1 })
    }),
  )

  await runDatabase(
    filename,
    Effect.gen(function* () {
      const database = yield* Database.Service
      const db = database.db
      expect(yield* db.get<{ count: number }>(sql`SELECT count(*) AS count FROM migration`)).toEqual({
        count: migrations.length,
      })
      expect(
        yield* db.get<{ count: number }>(sql`SELECT count(*) AS count FROM migration WHERE id = ${delegationMigrationID}`),
      ).toEqual({ count: 1 })
      expect(yield* db.get(sql`SELECT id, parent_id FROM session WHERE id = ${child}`)).toEqual({
        id: child,
        parent_id: parent,
      })
    }),
  )
})
