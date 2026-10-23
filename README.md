# ebasketball

## Setup

Use Python 3.10 or newer. From the repository root:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
```

This installs the project dependencies plus pytest. No provider credentials are
needed for offline file imports or prediction.

## Database migrations

From the repository root, inspect migration state without changing the database:

```sh
.venv/bin/python -m ebasketball.migrations status
```

Apply pending migrations by confirming the exact configured database name:

```sh
.venv/bin/python -m ebasketball.migrations apply --confirm-database ebasketball_test
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
live data. No official API contract is verified for either provider, so live
collection is disabled. The ingestion command therefore accepts explicit JSON
exports as top-level arrays of raw source records. Each record should use the aliases
supported by `normalize_fixture`; ISO timestamps are recommended. Prediction
and backtesting use completed match scores only. The ingestion path ignores
market fields in exports and writes only match and source provenance data.
Legacy market-observation tables and columns remain in SQL for database
compatibility and are not queried by prediction or backtesting.

Preview collection, validation, and reconciliation without connecting to
PostgreSQL:

```sh
.venv/bin/python scripts/run_ingestion.py \
  --input h2hgg=fixtures.json \
  --input aceodds=aceodds.json
```

The command previews by default, without opening PostgreSQL. Add `--persist` to
write. The command uses the existing `DB_*` application configuration and does
not run migrations. Each fixture and its provenance are committed in one
transaction. It prints JSON counts and explicit
source/record errors; malformed records and unavailable input files produce a
nonzero exit status. Reconciliation ambiguity and exact repeated input records
are reported as skipped. Live collection requires implementing and configuring
adapters that satisfy `SourceAdapter.fetch_records()`; no live endpoint,
authentication, or scraping behavior is currently provided.

The JSON summary counters mean: `fetched` is every raw item yielded, including
malformed and repeated items; `normalized` is successful normalization before
duplicate filtering; `reconciled` is the number of nonduplicate source records
in cross-source groups (ambiguous records are excluded); `inserted` and
`updated` count match upserts and remain zero during dry runs;
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
.venv/bin/python scripts/run_backfill.py --source h2hgg --input historical.json
```

Use a CSV export by passing its path instead:

```sh
.venv/bin/python run_backfill_2.py --source aceodds --input historical.csv
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
The JSON summary also reports completed fixtures by UTC day and source,
unique players and fixture identities, out-of-range records, duplicate fixture
counts, and invalid-record categories. Offline imports are marked `observed`,
never `verified`; `missing_calendar_dates` only means the import contained no
record for those dates and does not prove that matches should have occurred.
Dry runs never write checkpoints or connect to PostgreSQL.

Example execution after reviewing the preview:

```sh
.venv/bin/python scripts/run_backfill.py --source h2hgg --input historical.json \
  --persist --checkpoint .backfill-h2hgg.json
```

The adaptive mode tries the largest requested window first, then shorter
windows after truncation, exhausted transient retries, provider failures, or
an incomplete/empty result. Checkpoints are separated by window size:

```sh
.venv/bin/python scripts/run_backfill.py --source h2hgg --input historical.json \
  --adaptive --window-sizes 90,60,30
.venv/bin/python scripts/run_backfill.py --source h2hgg --input historical.json \
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
.venv/bin/python run_incremental.py --source h2hgg --input recent.json
.venv/bin/python run_incremental.py --source h2hgg --input recent.json \
  --overlap-days 3 --persist
```

`--overlap-days 0` scans today only. Negative values are rejected. The CLI
reports the normal ingestion JSON summary plus the UTC cutoff and number of
older records filtered out. Exit codes are 0 for success, 1 for record
validation failures, 2 for invalid arguments/input errors, and 3 for ingestion
or persistence failures. It reads explicit JSON exports; no live fetching or
automatic scheduling is implemented.

Across ingestion commands, exit code 0 means the run completed, 1 indicates
record validation failures or no usable/completed backfill result, 2 indicates
invalid arguments/configuration/input, and 3 indicates source, reconciliation,
or persistence failure. A completed pagination run does not verify historical
coverage. Prediction returns `insufficient_data` in its JSON result when it
cannot support a winner; that is a valid analysis response.

### Live provider status

Omitting `--input` from the backfill or incremental command requests live
collection. The command currently exits with a provider configuration error
instead of making an undocumented request. This is intentional:

| Provider | Verified capability | Live API status |
| --- | --- | --- |
| H2HGG | Its public page is a schedule. A Parse.bot search listing described `get_schedule` with date and days fields and completed scores, but this is a third-party extraction API, not an H2HGG API. The listing's endpoint URL currently returns 404. | No official API. Parse's terms make the user responsible for source-site permissions. Date retention and current endpoint availability are unverified, so no adapter is enabled. |
| AceOdds | Its public page displays the current streaming schedule and links to matches. Its terms describe website content as owned or licensed and restrict reproduction; they do not document a data API. | No documented historical results API or permission for automated collection. No adapter is enabled. |

[SIS/Genius Sports integration documentation](https://geniussports.atlassian.net/wiki/spaces/BID/pages/5086511298/SIS%2BCompetitive%2BGaming)
also documents an official eBasketball H2H GG event feed, but its published
January 2026 terms prohibit storing data to build a historical archive unless
the agreement expressly permits it. The standard terms therefore do not fit
this project's historical backfill without a separately negotiated written
license.

References: [H2HGG public schedule](https://h2hggl.com/en/ebasketball/schedule),
[AceOdds H2H GG schedule](https://www.aceodds.com/bet365-live-streaming/basketball/ebasketball-h2h-gg-league-4x5mins.html),
the [third-party Parse.bot listing](https://parse.bot/marketplace/c2056cb9-2a9f-41dd-b5cd-79ecc9613bd6/h2hggl-com-api),
[Parse terms](https://parse.bot/terms).
Public schedules alone do not authorize automated collection. No credentials or
undocumented endpoints are assumed. No real provider request has been made.

Until then, use the documented JSON/CSV file imports above. Historical
coverage is observed from supplied records and is not verified for completeness.
The current schema has no player-statistics table; this analysis derives
descriptive measures from completed match scores and does not invent player
statistics.

## Educational match-winner analysis

The prediction module estimates a likely winner from historical completed
matches for educational and analytical study; it does not provide guarantees.
It analyzes completed match results only and currently reports no win
probabilities: there is no calibrated probability model supported by the
verified data.

The transparent baseline compares each team's recent win rates over 5, 10, and
20 matches, overall win rate, head-to-head record, opponent-adjusted win rate
when available, and average scoring differential, in that order. The result
includes points scored/conceded, outcome and scoring variation, opponent form,
sample size, date range, freshness, warnings, a qualitative confidence label,
and an explanation. At least five prior results per team are required by
default. Tied,
invalid, missing-score, and conflicting duplicate records are excluded and
reported. Only fixtures with timestamps strictly before the requested match
are eligible.

Confidence is qualitative: `high` requires at least 20 prior matches for each
team, both teams' latest results within 14 days, and combined scoring-difference
variation below 30 points; `medium` requires at least 10 per team and latest
results within 45 days; other predictions are `low`. This label describes the
available evidence, not a calibrated chance of winning.

Analyze an offline JSON export without connecting to the database:

```sh
.venv/bin/python scripts/run_prediction.py --source h2hgg --input historical.json \
  --team-a PlayerOne --team-b PlayerTwo \
  --match-time 2026-10-23T19:00:00Z
```

Run a chronological backtest on the same export:

```sh
.venv/bin/python scripts/run_prediction.py --source h2hgg --input historical.json --backtest
```

The backtest predicts each fixture using only earlier completed fixtures. It
reports winner accuracy among predictions made, prediction coverage, an
always-home baseline, results by prior-sample-size band and calendar year.
Log-loss and Brier score are unavailable until a probability model is
calibrated and evaluated out of sample. No measured accuracy is published here;
the included tests validate behavior, not predictive quality. Results inherit
the coverage and quality limitations of the supplied records, and current
provider access remains disabled pending documented and permitted interfaces.

### Recording and evaluating predictions

Prediction snapshots are separate from match results. Recording requires an
already stored, real, scheduled fixture and explicit `--persist`; stored
participants and start time must agree with the requested fixture. The default
run ID is stable for the fixture and method version, making a repeated
prediction idempotent. Supply a new `--run-id` UUID to intentionally record a
second snapshot. Snapshots are immutable; result evaluations are appended when
results arrive or scores change.

```sh
# Preview without database access
.venv/bin/python scripts/run_prediction.py --source h2hgg --input history.json \
  --team-a PlayerOne --team-b PlayerTwo --match-time 2026-10-24T19:00:00Z

# Record against an existing scheduled fixture
.venv/bin/python scripts/run_prediction.py --source h2hgg --input history.json \
  --team-a PlayerOne --team-b PlayerTwo --match-time 2026-10-24T19:00:00Z \
  --match-id 123 --persist

# Read-only database reports; reconciliation writes and requires --persist
.venv/bin/python scripts/run_prediction.py --list
.venv/bin/python scripts/run_prediction.py --pending
.venv/bin/python scripts/run_prediction.py --metrics --start-date 2026-01-01
.venv/bin/python scripts/run_prediction.py --reconcile --persist
```

Use `--export report.json` or `--export report.csv` for list and metric output.
Accuracy uses correct plus incorrect predictions as its denominator;
evaluation coverage uses all recorded predictions. Prediction coverage is
reported separately as fixtures with a recorded prediction divided by real
scheduled or completed fixtures in the selected date range. Pending, void, and
unevaluable records are reported separately. Score corrections append a new
evaluation event and update the current result status while preserving earlier
events and the original snapshot. Only completed, non-tied, non-synthetic
results are scored. Database-backed tracking requires migration 004; offline
analysis and chronological backtesting do not.

## Project layout and configuration

- `src/ebasketball/ingestion/` contains shared normalization, reconciliation,
  ingestion, and resumable backfill logic.
- `src/ebasketball/sources/` contains H2HGG/AceOdds record normalizers, JSON
  imports, and an explicit disabled-live-provider guard. It does not fetch live
  data.
- `src/ebasketball/repositories/` contains PostgreSQL persistence code;
  `src/ebasketball/database.py` owns connection and transaction setup.
- `src/ebasketball/prediction.py` calculates historical features and performs
  winner analysis/backtesting. `analytics.py` retains DataFrame summary helpers.
- `scripts/` contains the CLI implementations. Root `run_backfill_2.py` and
  `run_incremental.py` are thin compatibility wrappers. `dashboard/app.py` is
  only a placeholder; no dashboard is implemented.

Offline JSON/CSV preview, incremental filtering, prediction, and backtesting
need no database configuration. Database persistence and migration commands
load the following settings from the environment or a local `.env` file:

| Variable | Required | Default |
| --- | --- | --- |
| `DB_PASSWORD` | Yes, for database access | None |
| `DB_HOST` | No | `localhost` |
| `DB_PORT` | No | `5433` |
| `DB_NAME` | No | `ebasket_db` |
| `DB_USER` | No | `postgres` |
| `DB_CONNECT_TIMEOUT` | No | `10` seconds |

Keep `.env` local and never commit its contents. PostgreSQL integration tests
use the separate `EBASKETBALL_TEST_DSN`; they skip when it is unset. No
provider API key is required for offline work. H2HGG and AceOdds live collection
remain disabled until documented, permitted access is implemented.
