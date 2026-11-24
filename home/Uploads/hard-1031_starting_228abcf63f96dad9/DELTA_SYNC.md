# Real time incremental delta sync

| Piece | Where |
|---|---|
| Outbox table `sync_delta_queue`, queue + apply queries | `data/src/main/sqldelight/tachiyomi/data/sync_delta_queue.sq` |
| Migration 14 (table + rewritten triggers) | `data/src/main/sqldelight/tachiyomi/migrations/14.sqm` |
| Triggers that also enqueue | `mangas.sq`, `chapters.sq`, `mangas_categories.sq` |
| Repositories (interfaces / impl) | `domain/.../sync/repository`, `data/.../sync` |
| Preferences | `domain/.../sync/service/SyncPreferences.kt` |
| Wire format (kotlinx protobuf) | `app/.../data/sync/model/DeltaSyncPayloads.kt` |
| Engines | `DeltaSyncPushEngine.kt`, `DeltaSyncPullEngine.kt` |
| Jobs / scheduler | `DeltaSyncPushJob.kt`, `DeltaSyncPullJob.kt`, `DeltaSyncManager.kt` |
| Injekt wiring | `DomainModule.kt`, `PreferenceModule.kt`, `App.kt` |

## Flow

**Local edit → push.** A trigger bumps `version` and does `INSERT OR IGNORE INTO sync_delta_queue` (one `PENDING`
entry per record, so repeated edits coalesce). `DeltaSyncManager` notices a new entry id, debounces 2 s and starts
`DeltaSyncPushJob` (plus a 15 min periodic run as a safety net). The job claims ≤ 100 entries
(`PENDING → IN_FLIGHT`) and reads just those rows in one transaction, serialises them as a `DeltaPushRequest`
(protobuf), POSTs it, and only after a 2xx deletes the entries.

| Failure | Result |
|---|---|
| offline / timeout | entries back to `PENDING`, attempt **not** counted, `Result.retry()` (WorkManager backoff) |
| 5xx, 408, 425, 429 | entries back to `PENDING`, attempt counted (8 → `FAILED`), `Result.retry()` |
| 400/413/415/422 or unserialisable row | batch is bisected, only the offending entry is `FAILED` |
| other 4xx / bad URL | request parked, `Result.failure()` |
| process dies mid-upload | leftovers `IN_FLIGHT` are recovered at the start of the next run |
| ack lost after a 2xx | entry is re-sent; receiver must be idempotent (it is, on `version`) |

`FAILED` entries are requeued at app start and when the endpoint preference changes.

**Remote → pull.** `DeltaSyncPullJob` fetches pages from `GET <endpoint>/pull?cursor=…`, each record is applied in
its own transaction by `SyncRecordRepositoryImpl`: write data **and** `is_syncing = 1` in the same `UPDATE` (triggers
silent), write categories, `is_syncing = 0`, drop a now-obsolete pending local entry. The cursor is stored after the
page is applied.

## Endpoint contract (mock)

`<endpoint>` defaults to `http://10.0.2.2:8080/mock-sync/v1` (preference `delta_sync_endpoint`);
`delta_sync_enabled` is the master switch. `python3 tools/mock_sync_server.py [--fail 503 --times 3]` serves it.

* `POST /push` – `application/x-protobuf` `DeltaPushRequest`, header `X-Device-Id`; any 2xx = accepted.
* `GET /pull?cursor=&limit=&deviceId=` – `DeltaPullResponse`, or `204` / empty body when there is nothing new.

## Checks

`python3 tools/verify_delta_sync_sql.py` builds the schema from the `.sq` files and runs the trigger, queue,
`is_syncing` and migration scenarios against SQLite. `DeltaSyncEngineTest` covers the engines with fakes.
