# Codex Project Context --- Indian Index Options Research & Automated Trading System

## 1. Purpose of this file

This document is the project hand-off for Codex. Read it before making
code changes. It captures the goal, current data pipeline, scripts,
research context, and the next planned work so development can continue
without relying on the original ChatGPT conversation.

Treat the existing repository and scripts as the source of truth for
implementation details. This file describes the intended architecture
and known behaviour, but verify the current code before changing it.

## 2. Project goal

I am building a reliable, data-driven research platform for Indian index
options, initially focused on NIFTY and also including BANKNIFTY and
SENSEX where data is available. The long-term goal is to develop,
backtest, evaluate, and eventually automate options-selling strategies.

The system should collect and preserve intraday market data over
multiple years, make the data easy to query, calculate useful derived
features (especially option Greeks and implied volatility), and support
reproducible research. This is a research/data-engineering project
first. Do not jump directly to live order execution or assume a strategy
is profitable without evidence.

Primary objectives:

1.  Collect one-minute historical candles for index options, futures,
    and relevant underlying/index series.
2.  Preserve the collected data in Parquet, which is the authoritative
    research dataset.
3.  Validate data quality and track missing, invalid, failed, or
    duplicate data explicitly.
4.  Make the data queryable locally through DuckDB.
5.  Import additional historical options data from vendor ZIP/CSV/Excel
    files into the research database.
6.  Add implied volatility and option Greeks to the Parquet research
    data, with clearly documented calculation assumptions.
7.  Research options-selling strategies using historical evidence,
    including market regimes, volatility, expiry timing, gaps, and risk.
8.  Build repeatable scripts that can later run on a schedule (for
    example, via GitHub Actions or another scheduled runner) and persist
    outputs outside an ephemeral runner.
9.  Keep the pipeline maintainable, testable, restartable, and safe
    against accidental data loss.

## 3. Working style expected from Codex

-   First inspect the repository, README files, dependency files,
    configuration, and current scripts before proposing or applying
    structural changes.
-   Make small, reviewable changes. Explain the reason and the impact of
    each meaningful change.
-   Do not overwrite or delete collected market data unless I explicitly
    approve it.
-   Treat original downloaded Parquet as raw/authoritative input. Prefer
    writing enriched/derived Parquet to a separate, clearly named
    location or to a deliberate versioned output, rather than silently
    mutating the only copy.
-   Preserve restart/resume and checkpoint behaviour in the downloader.
-   Do not commit credentials, access tokens, `config.json`,
    `session.json`, or other secrets. Check `.gitignore` and avoid
    printing secret values in logs.
-   Add tests for data transformations and edge cases. Run relevant
    tests or validation commands and report exactly what was and was not
    run.
-   Do not invent results or claim that the data is complete merely
    because a script finished. Use explicit validation and audit output.
-   Prefer reproducible calculations, explicit configuration, structured
    logging, and CLI arguments over hidden hard-coded assumptions.
-   Avoid unnecessary rewrites. Maintain compatibility with the existing
    folder layout and database unless a migration is justified and
    explained.
-   Before changing a schema, explain backward compatibility, how old
    Parquet/database data will be handled, and how to rerun safely.
-   Never treat a backtest as proof of future profitability. Include
    realistic transaction costs, slippage, liquidity, execution
    assumptions, and out-of-sample checks where relevant.

## 4. Current scripts and their responsibilities

The following scripts were supplied in the project context. Their exact
filenames in the local working tree may differ slightly (for example, an
uploaded copy may include `(1)`); use the actual repository filenames.

### `01_login.py` --- Zerodha Kite authentication

-   Reads `api_key` and `api_secret` from `config.json`.
-   Creates a Kite Connect login URL and asks the user to paste the
    `request_token` from the redirect URL.
-   Exchanges the request token for a session and saves the access token
    and basic session metadata to `session.json`.
-   Calls the Kite profile endpoint to verify login success.
-   The access token/session may expire and must be handled securely.
    Never commit `config.json` or `session.json`.

### `02_download_instruments.py` --- Instrument master and research universe

-   Authenticates to Kite using `config.json` and `session.json`.
-   Downloads the full Zerodha instrument master.
-   Stores raw instrument data under `data/instruments/raw`.
-   Builds and saves a filtered research instrument universe under
    `data/instruments/research`, including `research_instruments.csv`.
-   The configured universe includes derivative underlyings NIFTY,
    BANKNIFTY, and SENSEX; index series include NIFTY 50, NIFTY BANK,
    INDIA VIX, and SENSEX; derivative instrument types include CE, PE,
    and FUT.
-   The research universe is used by the downloader to map instrument
    tokens to dataset names and contract metadata.

### `03_eod_downloader.py` --- One-minute historical candle downloader

This is the main data-collection script.

-   Uses Kite Connect and the research instrument list.
-   Downloads one-minute historical candles for the selected target
    trading date and instruments.
-   Derivative datasets are mapped to names such as `NIFTY_OPTIONS`,
    `NIFTY_FUTURES`, `BANKNIFTY_OPTIONS`, `BANKNIFTY_FUTURES`,
    `SENSEX_OPTIONS`, and `SENSEX_FUTURES`.
-   Index datasets include `NIFTY_50`, `NIFTY_BANK`, `SENSEX`, and
    `INDIA_VIX`.
-   Normalizes candles and adds instrument metadata such as token,
    exchange, trading symbol, underlying, instrument type, expiry,
    strike, option type, and research dataset.
-   Has API pacing, retries with backoff, checkpointing, batching,
    resume support, and separate tracking of completed instruments,
    no-data responses, invalid tokens, and retryable failures.
-   Checks persisted data so that a rerun can resume rather than blindly
    downloading the same instrument again.
-   Writes Parquet part files grouped by date and dataset. The script
    supports a clean output mode (`--fresh`) that uses
    `data/market_data_clean` rather than the legacy/default
    `data/market_data` output root. It also accepts path/date overrides
    through its CLI; inspect `--help` and the current implementation
    before running.
-   A checkpoint is saved under the target date. Review it and the run
    summary before considering a collection complete.
-   The current uploaded copy contains a configured target date of
    `2026-10-08`. Do not assume this is the desired date for a new run;
    inspect CLI options and current configuration first.
-   Do not change retry/checkpoint logic casually. Partial downloads
    must remain detectable and resumable.

### `04_export_market_data_to_excel.py` --- Excel export and audit

-   Reads Parquet from `data/market_data_clean/<date>/<DATASET>/`.
-   Creates one Excel workbook per dataset, splitting large datasets
    across multiple sheets to respect Excel's row limit.
-   Creates `export_audit.xlsx` with row counts, file counts, unique
    instruments, export details, and warnings.
-   Can create `instrument_status.xlsx` using checkpoint details such as
    no-data, invalid-token, and retryable-failure statuses.
-   Removes timezone metadata only from an export-only copy where needed
    for Excel compatibility.
-   It does not modify or delete the Parquet source files. Parquet
    remains the authoritative data.

### `05_validate_market_data.py` --- Parquet quality checks

-   Validates daily Parquet files under
    `data/market_data_clean/<date>/`.
-   Produces CSV reports and `validation_report.xlsx` under
    `data/validation_reports/<date>/`.
-   Checks for unreadable Parquet files, missing required columns,
    invalid timestamps, duplicate `(instrument_token, date)` rows,
    dataset-folder/schema mismatches, missing or nonnumeric OHLC values,
    invalid OHLC relationships, negative volume/open interest, and
    timestamps outside configured expected market sessions.
-   Produces dataset summaries and per-instrument coverage reports,
    including row counts and first/last timestamps.
-   The script explicitly treats coverage as diagnostic: options can be
    sparse, and a clean report is not proof that all historical market
    data is complete.
-   Session close assumptions are coded in the script. Verify official
    exchange timings and timestamp conventions before changing these
    assumptions.

### `06_import_parquet_to_duckdb.py` --- Parquet to DuckDB

-   Imports daily Parquet datasets into a local DuckDB database, by
    default `data/nifty_research.duckdb`.
-   Can import one date with `--date YYYY-MM-DD` or discover and import
    all available date folders.
-   Creates a table per dataset/instrument folder, using the folder name
    as the table name.
-   Reads nested Parquet files and supports schema union by name.
-   Adds a `source_date` column to identify the source day.
-   Detects instrument and timestamp key columns from candidate names
    and aims to enforce a composite primary key.
-   Warns about duplicate key groups and uses transactions; rerunning a
    source date replaces that date's rows, while conflicts under other
    source dates are handled conservatively.
-   Existing uploaded code has a hard-coded Windows `PROJECT_ROOT`.
    Codex should assess replacing machine-specific paths with
    project-relative defaults and CLI/environment overrides, without
    breaking current workflows.
-   Be careful with schema migration and duplicate handling: back up the
    database before risky changes and test migrations on a copy.

### `07_import_historical_nifty_options.py` --- Historical options data importer

-   Imports historical options data from ZIP archives containing CSVs,
    standalone CSVs, and Excel workbooks.
-   Normalizes common vendor column names and maps fields such as
    ticker/trading symbol, date/time, OHLC, volume, and open interest.
-   Combines date and time into the timestamp key where necessary.
-   Imports into an existing `NIFTY_OPTIONS` table in
    `nifty_research.duckdb` by default.
-   Verifies the table exists and expects a primary key on
    `(tradingsymbol, date)`.
-   Skips invalid symbols/timestamps, deduplicates within incoming data,
    detects keys already present, and uses `ON CONFLICT DO NOTHING` to
    avoid replacing existing database rows.
-   It does not automatically create the destination table; the Parquet
    import pipeline should have created a compatible table first.
-   Check vendor timestamp formats, timezone conventions, symbols,
    expiries, and schema compatibility before importing large archives.

## 5. Data layout and source-of-truth principle

The current intended clean data layout is approximately:

``` text
scanner/
  data/
    instruments/
      raw/
      research/
        research_instruments.csv
    market_data_clean/
      YYYY-MM-DD/
        NIFTY_OPTIONS/
          part_*.parquet
        NIFTY_FUTURES/
          part_*.parquet
        BANKNIFTY_OPTIONS/
        BANKNIFTY_FUTURES/
        NIFTY_50/
        NIFTY_BANK/
        INDIA_VIX/
        checkpoint.json
    validation_reports/
      YYYY-MM-DD/
    excel_exports/
      YYYY-MM-DD/
    nifty_research.duckdb
```

The precise directory tree may vary; verify the actual repository. There
may also be legacy data under `data/market_data`.

Design principle:

-   Raw/downloaded market candles are the durable source layer.
-   Validated/clean data and enriched feature data should have explicit,
    reproducible lineage.
-   Excel is only a convenience export, not the primary store.
-   DuckDB is a local analytical/query layer; it should be rebuildable
    from source Parquet plus documented historical imports.
-   If Greeks are added, make it clear which Parquet dataset contains
    them, how they were calculated, which input data and assumptions
    were used, and how they can be recalculated.

## 6. Important next requirement: add implied volatility and Greeks to Parquet

I explicitly want option Greeks included in the Parquet research files.
Treat this as a core requirement, not an optional future idea.

### Desired outputs

For each option candle/contract observation where valid inputs exist,
work toward storing:

-   `implied_volatility` (IV)
-   `delta`
-   `gamma`
-   `theta`
-   `vega`
-   Optionally `rho`, if useful and the rate assumptions are reliable
-   A calculation status / quality field where calculation is impossible
    or unreliable
-   If practical, calculation metadata such as model/version or
    assumptions identifier

Agree on consistent units and document them. A suggested convention is
IV as decimal or clearly labelled percentage, Greeks in explicitly
documented units, and timestamps/time-to-expiry handled consistently. Do
not mix decimal IV (e.g. `0.20`) and percentage IV (e.g. `20`) without
an explicit column convention.

### Critical calculation details

The downloaded Kite candle schema generally provides option
OHLC/volume/OI and contract metadata, but does not necessarily provide
historical IV/Greeks. Do not assume the API has supplied Greeks. These
values may need to be calculated from option prices and underlying
prices.

Before implementing, inspect the actual Parquet schema and sample
records. Determine how to join each option observation to the correct
underlying price at the same timestamp. For index options, use the
appropriate underlying index/futures series and make that choice
explicit. Avoid using future timestamps or future information when
constructing features.

At minimum, a calculation requires reliable option price, strike,
expiry, option type (CE/PE), valuation timestamp, underlying price, and
assumptions for interest rate and any dividends/carry treatment. IV is
typically solved numerically from an option pricing model; Greeks are
then calculated from the model using the solved IV. Use a model
appropriate for the contracts and market conventions, and document the
model and assumptions rather than treating estimates as
exchange-published truth.

Important edge cases to handle and test:

-   Expired contracts and timestamps at/after expiry
-   Very low or zero option prices, stale quotes, invalid OHLC, and
    no-arbitrage bound violations
-   Missing underlying candles or timestamp mismatches
-   Deep ITM/OTM contracts and numerical solver convergence
-   Expiry-day theta/time-to-expiry handling and market holidays
-   CE versus PE handling
-   Rate/dividend/carry assumptions and model choice
-   Duplicate contract/timestamp rows and daylight/timezone consistency
-   Missing IV/Greeks: preserve the row and record a clear null/status
    reason rather than silently dropping the observation
-   Prevent look-ahead bias in research features

### Recommended implementation approach

1.  Inspect the current schema and choose a representative trading day
    to profile option/underlying timestamp alignment.
2.  Write a design note defining model, input price choice (close versus
    another point), underlying source, risk-free rate assumption,
    time-to-expiry convention, units, output columns, and failure
    statuses.
3.  Build the calculation as a separate, testable module rather than
    mixing numerical logic into the API downloader.
4.  Start with a small sample and independently sanity-check a few
    contracts and timestamps.
5.  Add automated tests for known/controlled cases, solver failures,
    expiry, missing data, and CE/PE parity-related behaviour where
    applicable.
6.  Generate enriched Parquet in a separate output layer initially,
    preserving original source files. Once correctness and performance
    are established, decide whether the main pipeline should write
    enriched data directly or keep raw and derived datasets separate.
7.  Add validation checks for Greek ranges, IV solver failures, null
    rates, schema consistency, and row counts.
8.  Ensure the pipeline can be rerun deterministically without duplicate
    rows or accidental loss of source data.
9.  Make the enriched output queryable in DuckDB and document how to
    rebuild it.

Do not calculate Greeks by simply attaching one current IV value to all
historical candles. Greeks and IV are time-dependent and
contract-dependent. Historical Greeks must be calculated using
information appropriate to each historical observation.

## 7. Strategy research context and current preferences

This section is background for future research, not a command to deploy
any particular trade.

-   I am interested primarily in option-selling strategies on NIFTY,
    with BANKNIFTY and other supported indices as useful extensions.
-   Strategies discussed include Iron Condors, Bull Put Spreads, and
    other defined-risk spreads. I want to understand and test strategies
    rather than blindly follow one.
-   A previously discussed Iron Condor idea used short options around
    delta 0.20 and protective long options farther out of the money.
    Example parameters discussed included a minimum distance from spot
    and a hedge width, but these are hypotheses to test---not universal
    rules.
-   A previously discussed regime idea was to use a wider Iron Condor in
    lower India VIX and a Bull Put Spread in higher India VIX. The
    example threshold previously discussed was IVIX 15.5. Treat this as
    an unvalidated hypothesis and test thresholds out-of-sample rather
    than hard-coding it as truth.
-   I have been researching whether closing weekly positions by Friday
    end-of-day is safer/more robust than holding them over the weekend
    into Monday. Gap risk and losses around market gaps are concerns.
    Test this using historical data with correct expiry calendars and
    realistic execution assumptions.
-   I want to understand how India VIX, implied volatility, futures
    movement, gaps, support/resistance, trend/EMA context, delta, gamma,
    theta, and vega affect option-selling outcomes.
-   Technical features previously considered include support/resistance
    zones and moving averages such as 9/20 EMA or 20/50 EMA. These are
    candidate features, not mandatory strategy rules.
-   I have discussed targeting a return percentage on margin and
    limiting per-trade risk, but capital allocation and stop-loss
    thresholds should remain configurable and be evaluated against
    drawdown and tail risk.
-   Do not assume an entry weekday, expiry weekday, lot size, strike
    interval, market closing time, or contract specification is
    permanent. Indian exchange rules and contract conventions can
    change. Keep these configurable/date-aware and verify them against
    reliable current sources when necessary.
-   Backtests should include brokerage, exchange charges, STT, GST,
    stamp duty, slippage, bid/ask spread, liquidity constraints,
    lot-size changes, expiry changes, and realistic fill assumptions
    where data allows.
-   Avoid survivorship/look-ahead bias and avoid using a bar's final
    close to simulate an order that could only have been placed before
    that close was known.
-   Compare strategies using more than win rate: expectancy, net P&L,
    drawdown, worst day/week, tail loss, risk-adjusted returns, margin
    usage, return on capital, and robustness across years/regimes.

## 8. Historical data and long-term collection plan

I want to accumulate a multi-year options dataset. The one-minute data
can become very large, so storage and query design matter.

-   Parquet is preferred for columnar storage and efficient analytical
    scans.
-   Keep partitioning organized by date and dataset/underlying, while
    avoiding a file-per-tiny-fragment design.
-   DuckDB can query Parquet directly or load selected data locally for
    analysis.
-   Excel should be used only for manageable samples/reports because of
    row limits and memory constraints.
-   For scheduled cloud runs, do not assume files saved on a GitHub
    Actions runner will persist after the job. The workflow must upload
    artifacts to durable storage or commit data intentionally under a
    documented storage policy. Avoid storing large daily data
    indefinitely in Git history without understanding repository limits.
-   Google Drive, Google Cloud Storage, GitHub artifacts, and repository
    storage have different limits and cost models; evaluate current
    quotas/pricing before choosing a long-term archive. Do not promise
    five years of storage is free without calculating expected volume
    and retention.
-   Preserve daily manifests/checkpoints and make failed days easy to
    identify and rerun.

## 9. Data integrity and operational expectations

For every collection/import/enrichment run, aim to report:

-   Requested trading date and source
-   Number of contracts/instruments selected
-   Rows/files read and written
-   Number of instruments with data, explicit no-data results, invalid
    tokens, and retryable failures
-   Duplicate keys detected and how they were handled
-   Missing or invalid timestamps and OHLC values
-   Per-instrument first/last timestamps and candle coverage
-   Enrichment success/failure/null counts for IV and each Greek
-   Output paths and schema/version information

Never silently discard suspicious rows. Prefer an audit warning, an
explicit status, or a separate quarantine report. Do not modify the
original data merely to make validation pass.

## 10. Suggested near-term roadmap

### Phase A --- Understand and stabilize the existing pipeline

1.  Inspect the repository tree, current Git status, dependencies,
    README, and actual versions of scripts.
2.  Confirm how `03_eod_downloader.py` chooses the output root and
    whether the clean Parquet files are the current source of truth.
3.  Review `.gitignore` for credentials, checkpoints, local database
    files, and large generated data.
4.  Verify that scripts work from the project root and identify
    hard-coded machine-specific paths.
5.  Run the validator on a known day and review its reports before
    changing data structures.

### Phase B --- Specify and implement Greeks/IV enrichment

1.  Inspect schemas and real sample data for options, futures, index
    prices, and instrument metadata.
2.  Confirm the correct underlying-price source and timestamp matching
    rules.
3.  Write down the model and calculation conventions.
4.  Implement and test IV/Greek calculation independently.
5.  Produce enriched Parquet in a separate output location first.
6.  Validate results and measure processing time/memory on a
    representative day.
7.  Decide how to integrate enrichment into the scheduled daily
    workflow.

### Phase C --- Analytical research

1.  Create reliable joins between options, underlying/index, futures,
    India VIX, expiry, and instrument metadata.
2.  Create a DuckDB research layer or reproducible SQL/Python analysis
    notebooks/scripts.
3.  Analyze data coverage and gaps before doing strategy backtests.
4.  Build strategy backtests with date-aware contracts and costs.
5.  Compare out-of-sample performance and robustness across multiple
    years and volatility regimes.

### Phase D --- Scheduling and durable storage

1.  Make daily runs configurable and restartable.
2.  Add a scheduler (such as GitHub Actions) only after local
    reliability is established.
3.  Upload Parquet and reports to durable storage; do not rely on an
    ephemeral runner's filesystem.
4.  Add run manifests/alerts so failed or partial data collection is
    visible.
5.  Consider automation of research reports only after the data pipeline
    is dependable.

## 11. First task for Codex

Start by inspecting the actual repository and report back:

1.  Which of the seven scripts exist, and whether their behaviour
    matches this document.
2.  The actual data folder layout, current schemas, and how
    `--fresh`/date arguments work.
3.  Any critical risks or inconsistencies (especially hard-coded paths,
    duplicate handling, checkpoint semantics, timestamp assumptions, and
    secrets in Git).
4.  A concrete, minimal implementation plan for IV/Greeks enrichment,
    including the exact input columns and joins needed based on actual
    data.

Do not immediately rewrite everything. First establish the current
state, then implement the next agreed step with tests and clear
validation.

## 12. Useful commands to begin with

Run from the repository root and adapt filenames/paths to the actual
checkout:

``` powershell
git status --short
git ls-files
python --version
python .\03_eod_downloader.py --help
python .\04_export_market_data_to_excel.py --help
python .\05_validate_market_data.py --help
python .\06_import_parquet_to_duckdb.py --help
python .\07_import_historical_nifty_options.py --help
```

Do not run the downloader against the API until the target date, output
directory, credentials/session, and intended behaviour have been
confirmed.
