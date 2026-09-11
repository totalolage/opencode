# Durable delegation API

`@opencode-ai/schema/delegation` defines the serializable contracts.
`@opencode-ai/core/delegation` exports `DelegationStore`, available through `Service`, `layer`, `node`, or `make(db)`.
Core owns records and transactions. It does not run providers, wake sessions, format Task results, or interrupt jobs.

## Registration and work

`register(request)` returns `{ generation, workID, workState }`. The request contains a stable `requestID`, a proposed `generationID`, parent and child session IDs, the originating Task message, part, and call, an optional parent generation, an optional mode, and `explicitReuse`.

Both sessions must exist. The child's recorded parent must match the invoking parent. Registration never discovers delegations by scanning historical session ancestry.

A new generation cannot adopt a root-like child with unfinished work, a reserved source, an active outgoing delegation, or unresolved incoming returns. Those records must settle first. Active same-parent updates keep their existing ownership and may extend a branch with descendants.

An exact request retry returns its original generation and work token, even after closure. A conflicting request retry fails. Each new request creates an active durable work token. `finishWork(workID)` releases that token idempotently.

An active same-parent explicit reuse keeps the original generation, Task origin, ancestry, and mode. An explicit contradictory mode fails. Closed explicit reuse requires a new generation ID. Cross-parent reuse fails.

New launches default to `foreground`, including launches by background parents. A foreground parent cannot launch a background child. `promote(generationID)` is the only foreground-to-background transition and checks the parent's incoming mode again.

`startWork({ id, generationID, kind })` represents additional work that must block closure. The available kinds are `runtime`, `provider`, `tool`, and `input`. Root work supplies `sessionID` without a generation. Work IDs are stable operation identities. Retrying a finished token does not reactivate it. The adapter records eligible inputs and unfinished provider or tool work before allowing closure.

`get(generationID)`, `active(childID)`, and `listActive()` read durable generations. A child session has at most one active generation. `listWork(generationID)` reads its work tokens, and `unfinished(sessionID)` reads active session work. These queries support recovery without consulting transient jobs. They do not permit automatic provider or tool replay.

## Source reservation and logical finalization

`reserveSource({ sessionID, generationID, source, historyCutoff, consumed })` records source ownership before provider execution. Root sources omit `generationID`. The call atomically binds contributing returns and creates durable provider work. Terminal sources create runtime work instead.

The result is `{ source, created }`. An exact retry returns `created: false`; it is not permission to run a provider again. A conflicting source reservation fails. `sources(sessionID)` and `unfinishedSources(sessionID)` expose the durable records for reconciliation and explicit recovery.

`finalizeSource(sourceID, { payload, outcome })` records logical finalization after all prompt postprocessing. It atomically freezes output, finishes source work, resolves the contributing returns, and captures an asynchronous resolution for a background generation. Foreground and root sources produce no asynchronous return.

An assistant source can finalize with `reply`, `error`, or `cancelled`. Its assistant message identity remains unchanged for every outcome. A terminal source is the fallback for an error or cancellation with no assistant message; it cannot produce a `reply`.

Legacy `time.completed` is not a logical-finalization marker. It persists before structured-output and error postprocessing. The adapter must persist enough evidence to reconcile that later boundary. A crash before Core finalization leaves a reserved source and unfinished work, not a publishable reply.

`discardSource(sourceID)` finishes a source that is not a logical reply, such as an ordinary tool turn. It does not resolve contributing returns. A later reserved source can carry those consumed, unresolved identities forward. Discarded sources cannot be finalized.

Each new source can bind only admitted or consumed returns for its exact session and recipient generation. Finalization cannot overwrite another source's resolution of the same contribution.

## Resolution capture

`capture(generationID, capture)` idempotently captures an already-finalized Core source without notifying the parent. Missing reservations and merely reserved sources fail. `finalizeSource` normally performs capture in its transaction. Only background generations produce asynchronous resolutions. Foreground Task completion remains the runtime's responsibility.

A capture contains:

- `source`: the finalized assistant message identity, including assistant-backed errors and cancellations, or a stable terminal event identity when no assistant exists.
- `payload`: the exact model-visible string.
- `outcome`: `reply`, `error`, or `cancelled`.
- `historyCutoff`: the adapter's stable provider-turn history boundary.
- `consumed`: contributing return IDs bound by the source reservation and resolved by its finalization.

The generation and source determine the resolution ID and recipient message ID. Payload, source, outcome, cutoff, consumed IDs, and routing are immutable. An exact retry returns the existing record. A conflicting retry fails, including after admission or closure.

The adapter proves that the source is finalized, belongs to this generation, is not an ordinary tool turn, and follows the recorded history boundary. Core checks durable return identities but cannot inspect a runtime's provider input.

`pending(parentID?)` lists pending delivery intent. `incoming(parentID)` lists unresolved incoming returns. `getResolution(resolutionID)` reads one record. These methods never schedule work.

## Recipient admission

`prepare(resolutionID, { message, parts })` freezes the complete recipient content before admission. The message and each part contain runtime-neutral JSON data. Core assigns deterministic message and part IDs and adds the immutable Task, generation, source, cutoff, and consumed-return provenance. An exact envelope retry succeeds; different JSON content, IDs, part order, or provenance conflicts. JSON object-key order alone is not a conflict.

`admit(resolutionID, receiver)` calls `receiver(transaction, resolution)` only for an eligible pending return with a prepared envelope. The callback returns either `{ status: "admitted" }` or `{ status: "blocked", reason }`.

The receiver must persist the entire prepared message, every part, and the provenance before reporting admission. It must accept exact retries and reject identity collisions with different content. All model-visible recipient writes use the supplied same-database transaction. External immediately visible commits are not supported by this API. A separate prepared-visibility protocol would require an additional design.

Deterministic IDs alone do not satisfy this contract. The legacy generic prompt path has no exact-content retry check. Its message and part updates are separate durable events followed by projection. A dedicated legacy receiver must reconcile partial message-and-part persistence and reject conflicting content before reporting `admitted`. Core does not treat an event publication or one message-row upsert as proof that the full return is persisted.

Core holds an immediate SQLite transaction across the eligibility check, callback, and acknowledgement. Callback failures roll back recipient writes and acknowledgement together. The previously captured intent and prepared envelope remain pending. A blocked callback also leaves the intent pending. An admitted, consumed, or resolved retry does not call the receiver again. Revoked returns cannot invoke the receiver.

The callback does not start a model turn. The adapter schedules only after commit, under serialized session ownership, and leaves interrupted provider or tool continuations blocked pending explicit recovery.

`markConsumed({ parentID, ids })` atomically records consumption of admitted returns belonging to that parent. `reserveSource` can bind and consume them in one transaction. Admission alone is not consumption, and consumption is not resolution. Only logical source finalization marks contributions `resolved` and records `resolvedSourceID`.

Legacy execution does not currently expose a consumed-return cutoff API. The adapter must establish that boundary under serialized session ownership. Calling `markConsumed` or `reserveSource` alone does not prove that a provider request contained those returns.

## Reconciliation and closure

`reconcileAndClose(generationID, reconciler)` calls `reconciler(transaction, generation)` inside an immediate transaction. The callback returns `{ quiescent, sources }`. Core reconciles logical finalization of those existing source reservations and captures their resolutions before checking closure.

The adapter holds serialized session ownership throughout reconciliation. Its source query must include every logically finalized source for this registered generation, including sources committed before a crash. It excludes historical pre-feature output and ordinary tool turns. Missing pre-provider reservations fail rather than retroactively adopting output.

Legacy assistant finish and completion updates persist separately. The reconciler therefore uses registered generation ownership and a durable history cutoff rather than assuming that legacy source finalization shares Core's transaction. The adapter determines when the source is fully finalized and retries capture by its stable source identity.

The `quiescent` result covers runtime inputs and continuations not represented in Core. It does not override durable blockers. Closure also requires no active work, no active descendants, no pending outgoing returns, and no pending, admitted, or consumed incoming returns addressed to this generation.

Blocked closure commits newly captured sources and leaves the generation active. Successful closure ends return association. Closed or revoked generations do not run the reconciler. There is no separate unchecked close operation.

Registration, capture, admission, work changes, closure, and revocation serialize through database transactions. Late work either blocks closure or encounters a closed generation. An explicit reuse after closure creates a new generation; it cannot retarget an old return.

## Stop, deletion, and explicit resume

`revokeDescendants(sessionID)` persists a session revocation fence and traverses durable delegation ancestry. It revokes active descendant generations and their deliverable returns, including returns addressed to the stopped session. It does not consult transient job status or interrupt historical closed child associations. The returned affected identities let the adapter interrupt runtime work after commit.

The result also includes a durable `revocationID` and stable terminal `cancellationSource` for the no-assistant fallback. An existing assistant reservation keeps its assistant identity instead. Exact stop retries retain the revocation identity. A new stop after an explicit resume receives a new identity.

The stopped session's own incoming generation remains available for cancellation to its still-active parent. Its runtime work is finished, and its child-session fence rejects new assistant reservations and stale reply or error finalization. A pre-reserved assistant may finalize as `cancelled` without changing identity. Cancellation discards other reserved sources in that ownership scope and settles their work so closure can finish. Foreground cancellation still completes through the original Task result.

Contributing returns revoked by child stop remain `revoked`. Fenced cancellation records their settlement source and time without making them deliverable again. Parent-stop and deletion fences still prevent a child from publishing or restarting its parent.

The adapter revokes before interrupting or deleting sessions. Deletion does not erase delegation identity or revocation records. A stopped root has no incoming generation and produces no parent-facing echo.

`allowSession(sessionID)` removes only that session's fence for an explicit resume. It refuses to clear the fence while that session's own incoming generation is still active. It does not reactivate old work, generations, or returns. Recovery reads registered sources, generations, work, and returns only. No API retries unfinished provider or tool execution.

## Verification

The package-local checks are:

```sh
bun test test/delegation-lifecycle.test.ts test/delegation-outbox.test.ts test/delegation-source.test.ts test/delegation-migration.test.ts
bun script/migration.ts --check
bun typecheck
```

The database tests use isolated fixtures. They do not access live user sessions or require a backend restart.

The incremental migration test applies the historical migration chain, seeds pre-delegation session data, and then reopens through normal database initialization. The final delegation migration creates six tables and twelve indexes without an intermediate schema upgrade.
