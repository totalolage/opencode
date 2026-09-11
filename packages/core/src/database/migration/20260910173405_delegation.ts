import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260910173405_delegation",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`delegation_generation\` (
          \`id\` text PRIMARY KEY,
          \`parent_id\` text NOT NULL,
          \`child_id\` text NOT NULL,
          \`origin\` text NOT NULL,
          \`parent_generation_id\` text,
          \`mode\` text NOT NULL,
          \`state\` text NOT NULL,
          \`time_created\` integer NOT NULL,
          \`time_closed\` integer
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`delegation_registration\` (
          \`request_id\` text PRIMARY KEY,
          \`generation_id\` text NOT NULL,
          \`request\` text NOT NULL,
          \`work_id\` text NOT NULL,
          CONSTRAINT \`fk_delegation_registration_generation_id_delegation_generation_id_fk\` FOREIGN KEY (\`generation_id\`) REFERENCES \`delegation_generation\`(\`id\`)
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`delegation_resolution\` (
          \`id\` text PRIMARY KEY,
          \`generation_id\` text NOT NULL,
          \`source_kind\` text NOT NULL,
          \`source_id\` text NOT NULL,
          \`payload\` text NOT NULL,
          \`outcome\` text NOT NULL,
          \`history_cutoff\` text NOT NULL,
          \`consumed\` text NOT NULL,
          \`parent_id\` text NOT NULL,
          \`child_id\` text NOT NULL,
          \`recipient_generation_id\` text,
          \`message_id\` text NOT NULL,
          \`time_created\` integer NOT NULL,
          \`status\` text NOT NULL,
          \`time_admitted\` integer,
          \`time_consumed\` integer,
          \`time_resolved\` integer,
          \`resolved_source_id\` text,
          \`envelope\` text,
          CONSTRAINT \`fk_delegation_resolution_generation_id_delegation_generation_id_fk\` FOREIGN KEY (\`generation_id\`) REFERENCES \`delegation_generation\`(\`id\`)
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`delegation_revocation\` (
          \`session_id\` text PRIMARY KEY,
          \`id\` text NOT NULL,
          \`time_revoked\` integer NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`delegation_source\` (
          \`id\` text PRIMARY KEY,
          \`session_id\` text NOT NULL,
          \`generation_id\` text,
          \`source_kind\` text NOT NULL,
          \`source_id\` text NOT NULL,
          \`history_cutoff\` text NOT NULL,
          \`consumed\` text NOT NULL,
          \`work_id\` text NOT NULL,
          \`state\` text NOT NULL,
          \`payload\` text,
          \`outcome\` text,
          \`time_created\` integer NOT NULL,
          \`time_finalized\` integer,
          CONSTRAINT \`fk_delegation_source_generation_id_delegation_generation_id_fk\` FOREIGN KEY (\`generation_id\`) REFERENCES \`delegation_generation\`(\`id\`),
          CONSTRAINT \`fk_delegation_source_work_id_delegation_work_id_fk\` FOREIGN KEY (\`work_id\`) REFERENCES \`delegation_work\`(\`id\`)
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`delegation_work\` (
          \`id\` text PRIMARY KEY,
          \`session_id\` text NOT NULL,
          \`generation_id\` text,
          \`kind\` text NOT NULL,
          \`state\` text NOT NULL,
          \`time_created\` integer NOT NULL,
          CONSTRAINT \`fk_delegation_work_generation_id_delegation_generation_id_fk\` FOREIGN KEY (\`generation_id\`) REFERENCES \`delegation_generation\`(\`id\`)
        );
      `)
      yield* tx.run(
        `CREATE UNIQUE INDEX \`delegation_generation_active_child_idx\` ON \`delegation_generation\` (\`child_id\`) WHERE "delegation_generation"."state" = 'active';`,
      )
      yield* tx.run(
        `CREATE INDEX \`delegation_generation_parent_idx\` ON \`delegation_generation\` (\`parent_id\`,\`parent_generation_id\`);`,
      )
      yield* tx.run(
        `CREATE UNIQUE INDEX \`delegation_resolution_generation_source_idx\` ON \`delegation_resolution\` (\`generation_id\`,\`source_kind\`,\`source_id\`);`,
      )
      yield* tx.run(
        `CREATE UNIQUE INDEX \`delegation_resolution_message_idx\` ON \`delegation_resolution\` (\`message_id\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`delegation_resolution_parent_status_idx\` ON \`delegation_resolution\` (\`parent_id\`,\`status\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`delegation_resolution_recipient_status_idx\` ON \`delegation_resolution\` (\`recipient_generation_id\`,\`status\`);`,
      )
      yield* tx.run(
        `CREATE UNIQUE INDEX \`delegation_source_generation_source_idx\` ON \`delegation_source\` (\`generation_id\`,\`source_kind\`,\`source_id\`);`,
      )
      yield* tx.run(
        `CREATE UNIQUE INDEX \`delegation_source_root_identity_idx\` ON \`delegation_source\` (\`session_id\`,\`source_kind\`,\`source_id\`) WHERE "delegation_source"."generation_id" IS NULL;`,
      )
      yield* tx.run(`CREATE UNIQUE INDEX \`delegation_source_work_idx\` ON \`delegation_source\` (\`work_id\`);`)
      yield* tx.run(
        `CREATE INDEX \`delegation_source_session_state_idx\` ON \`delegation_source\` (\`session_id\`,\`state\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`delegation_source_generation_state_idx\` ON \`delegation_source\` (\`generation_id\`,\`state\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`delegation_work_generation_state_idx\` ON \`delegation_work\` (\`generation_id\`,\`state\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
