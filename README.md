# ebasketball

## Database migrations

From the repository root, inspect migration state without changing the database:

```sh
python -m ebasketball.migrations status
```

Apply pending migrations by confirming the exact configured database name:

```sh
python -m ebasketball.migrations apply --confirm-database ebasketball_test
```

Replace `ebasketball_test` with the intended database name. Confirmation is
required for every database, including one ending in `_test`. The runner uses
`DB_*` settings through the existing application configuration and never prints
the password or DSN. `status` does not create the ledger.

The project has no pre-existing version ledger, so the runner cannot determine
whether an unledgered database was initialized manually. It reports every
migration as pending and does not infer a baseline from existing tables. Review
that state before applying to an existing database. The existing migrations
are retained unchanged; if they cannot safely be reapplied to a particular
legacy state, establish that database's baseline explicitly before using the
runner, or apply a reviewed forward migration.

## Ingestion

The current H2HGG and AceOdds modules normalize source records but do not fetch
live data. The ingestion command therefore accepts explicit JSON exports as
top-level arrays of raw source records. Each record should use the aliases
supported by `normalize_fixture`; ISO timestamps are recommended. Optional
`odds_observed_at` (or `observed_at` / `odds_timestamp`) is retained on odds
history rows.

Preview collection, validation, and reconciliation without connecting to
PostgreSQL:

```sh
python scripts/run_ingestion.py \
  --input h2hgg=fixtures.json \
  --input aceodds=aceodds.json \
  --dry-run
```

Omit `--dry-run` to persist. The command uses the existing `DB_*` application
configuration and does not run migrations. Each fixture, its provenance, and
its odds are committed in one transaction. It prints JSON counts and explicit
source/record errors; malformed records and unavailable input files produce a
nonzero exit status. Reconciliation ambiguity and exact repeated input records
are reported as skipped. Live collection requires implementing and configuring
adapters that satisfy `SourceAdapter.fetch_records()`; no live endpoint,
authentication, or scraping behavior is currently provided.

The JSON summary counters mean: `fetched` is every raw item yielded, including
malformed and repeated items; `normalized` is successful normalization before
duplicate filtering; `reconciled` is the number of nonduplicate source records
in cross-source groups (ambiguous records are excluded); `inserted` and
`updated` count match upserts, not odds rows, and remain zero during dry runs;
`skipped` counts exact duplicates and ambiguous records; and `failed` counts
individual record normalization, reconciliation, or persistence failures.
Adapter-level failures appear in `source_failures` separately and do not
increment `failed` when no specific record can be attributed to them.

## Historical backfill from exports

The backfill CLI accepts JSON or CSV exports and defaults to a preview. It
filters on inclusive UTC calendar dates; `2026-07-25` through `2026-10-23`
contains 91 dates inclusive. The documented H2HGG marketplace example
fields (`externalId`, `startDate`, participants, team scores, and status) are
mapped for offline imports. This does not establish that live responses have
the same shape.

Preview an export without opening a database connection:

```sh
python scripts/run_backfill.py --source h2hgg --input historical.json
```

To persist, pass `--persist` and a checkpoint file. The checkpoint binds to the
input file's SHA-256 fingerprint and date range, and advances after each
successfully persisted page. Failed pages are retried from the prior cursor;
repository upserts and provenance writes make replay safe. Use the same input
file and checkpoint to resume. JSON may be an array or contain a `games` array;
CSV uses its header row as field names. `--batch-size` bounds records per page.
The output includes fetched, normalized, completed, reconciled, inserted,
updated, skipped, failed, incomplete-record, missing-score, committed-batch,
and provider-failure counts. `reconciled` is the existing pipeline's count of
records assigned to multi-record canonical groups. A single-source backfill
usually reports zero because it has no second source in that run.
`unavailable_dates` means no in-range record was observed on that UTC
date; it cannot distinguish a no-event date from incomplete provider coverage.
Dry runs never write checkpoints or connect to PostgreSQL.

Example execution after reviewing the preview:

```sh
python scripts/run_backfill.py --source h2hgg --input historical.json \
  --persist --checkpoint .backfill-h2hgg.json
```

The adaptive mode tries the largest requested window first, then shorter
windows after truncation, exhausted transient retries, provider failures, or
an incomplete/empty result. Checkpoints are separated by window size:

```sh
python scripts/run_backfill.py --source h2hgg --input historical.json \
  --adaptive --window-sizes 90,60,30
python scripts/run_backfill.py --source h2hgg --input historical.json \
  --adaptive --window-sizes 90,60,30 --persist \
  --checkpoint .backfill-h2hgg.json
```

The first command is a dry run. Persistent mode requires `--checkpoint` and
uses files such as `.backfill-h2hgg_90d.json`, `.backfill-h2hgg_60d.json`, and
`.backfill-h2hgg_30d.json`. Adaptive output records the selected window and
why larger windows were skipped. A final result with no in-range completed,
scored fixture has no selected window and is unsuccessful; it does not
establish historical coverage.

## Incremental ingestion

Incremental ingestion rereads the previous two UTC days through today by
default, including both endpoints and excluding future dates. Overlap lets a
scheduled fixture be updated when a late score or correction appears in the
same source export. Malformed or
undated records stay in the input passed to the normal ingestion validator.
Preview is the default; writes require `--persist`:

```sh
python scripts/run_incremental.py --source h2hgg --input recent.json
python scripts/run_incremental.py --source h2hgg --input recent.json \
  --overlap-days 3 --persist
```

`--overlap-days 0` scans today only. Negative values are rejected. The CLI
reports the normal ingestion JSON summary plus the UTC cutoff and number of
older records filtered out. Exit codes are 0 for success, 1 for record
validation failures, 2 for invalid arguments/input errors, and 3 for ingestion
or persistence failures. It only reads explicit JSON exports; no live fetching
or automatic scheduling is implemented.

The adapters currently do not fetch live data. The linked Parse listing is a
third-party wrapper and explicitly disclaims being an official H2HGG API. It
advertises schedule fields and team-level match statistics, while player
statistics are described as current career aggregates and recent games; it
does not advertise historical per-match player box scores. The listing's
historical retention, pagination limits, and actual response for this requested
period have not been verified. It requires a Parse API key. The published free
tier lists 200 credits per month and 5 requests per minute; its listed match
statistics call costs one credit, making per-match enrichment infeasible for
the advertised daily event volume on that tier. See the [third-party H2HGG
listing](https://parse.bot/marketplace/c2056cb9-2a9f-41dd-b5cd-79ecc9613bd6/h2hggl-com-api)
and [Parse pricing](https://parse.bot/pricing). No live request or import has
been made. Enabling live collection requires an authorized API key, a permitted
test response for the target dates, confirmed retention/pagination/quotas, and
review of the wrapper's terms. Current schema has no player-statistics table,
and no migration is included because supported historical stat fields and
their temporal meaning have not been established.
