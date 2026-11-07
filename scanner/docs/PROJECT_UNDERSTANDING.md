# Project Understanding

**Snapshot reviewed:** 2026-10-20  
**Primary handoff:** [CODEX_PROJECT_CONTEXT.md](CODEX_PROJECT_CONTEXT.md)

## Purpose

This repository is a local data collection and research pipeline for Indian index options, centered on NIFTY and also covering BANKNIFTY and SENSEX. The intended sequence is to collect one-minute candles, retain the original Parquet data, validate and query it locally, and then use it for reproducible options research. Strategy ideas in the handoff are hypotheses to evaluate, not trading rules or evidence of profitability. Live order execution is outside the current pipeline.

Parquet is intended to be the durable market-data source. Excel files are convenience exports, and DuckDB is a local query layer that should be rebuildable from Parquet and documented vendor imports. Any future IV/Greek enrichment should preserve source data and carry explicit calculation assumptions and lineage.

## Code map

| Script | Responsibility and observed behavior |
| --- | --- |
| `01_login.py` | Reads `config.json`, completes the interactive Kite Connect login, writes `session.json`, and checks the profile. |
| `02_download_instruments.py` | Downloads the instrument master, saves raw snapshots, and builds a research universe containing the configured index and derivative instruments. The current universe logic removes expired derivatives using the machine's current date. |
| `03_eod_downloader.py` | Downloads one-minute candles for a target date, normalizes them with instrument metadata, batches Parquet writes, and tracks checkpoint states for resume and failures. Default output is `data/market_data`; `--fresh` selects `data/market_data_clean`, and `--output-root` takes precedence. `--date` overrides the configured target date (`2026-10-08` in this checkout). |
| `04_export_market_data_to_excel.py` | Reads a selected date under the clean-data root by default, writes per-dataset workbooks plus audit/status workbooks, and makes an export-only timestamp copy for Excel compatibility. Default date is also `2026-10-08`. |
| `05_validate_market_data.py` | Checks daily Parquet files for schema, timestamp, duplicate, OHLC, volume/OI, dataset-folder, and session issues. Writes reports under `data/validation_reports`; requires `--date`. Its session boundaries are hard-coded in the script. |
| `06_import_parquet_to_duckdb.py` | Imports one or all date folders into dataset-named tables, adds `source_date`, and enforces a `(tradingsymbol, date)`-style key based on detected columns. Reruns replace rows for the selected source date; duplicate keys are reported and conflicts already present under another source date are skipped. |
| `07_import_historical_nifty_options.py` | Loads ZIP, CSV, or Excel vendor data into an existing `NIFTY_OPTIONS` table, normalizes fields and timestamps, and inserts only new `(tradingsymbol, date)` keys. It requires the destination table and expected primary key to exist. |
| `08_enrich_nifty_options.py` | Enriches selected NIFTY option dates with historical spot/VIX matches and BSM IV/Greek calculations; writes to a separate enriched-data root with explicit assumptions and run summaries. |
| `09_download_historical_nifty_50.py` | Downloads Kite one-minute candles for NIFTY 50 over an inclusive date range, reusing the existing config/session and instrument master. Writes daily Parquet parts under `data/market_data_clean/YYYY-MM-DD/NIFTY_50/` by default, with optional Excel output and a checkpoint for resume. |

The instrument, downloader, export, and validation scripts mostly use paths relative to the current working directory, so the expected launch directory is `scanner`. The two DuckDB importers instead set their defaults from a hard-coded Windows `PROJECT_ROOT`. They have database path overrides, and script 06 also has a data-root override, but their defaults will not travel cleanly to another machine.

## Current checkout snapshot

- All seven scripts described by the handoff are present under `scanner/`.
- `scanner/data/market_data_clean/` contains date folders for 2026-10-06 through 2026-10-19 on ten trading dates. The folders contain dataset directories and daily checkpoints. Visible datasets include NIFTY, BANKNIFTY, and SENSEX options/futures plus index series.
- Local instrument raw/research CSVs, Excel exports, and `nifty_research.duckdb` are present. No `market_data` legacy root or `validation_reports` directory was visible in the current `scanner/data` tree.
- The root README is only a one-line project description. No dependency manifest was found in the files reviewed, which makes environment setup and reproducibility less clear.
- `config.json`, `session.json`, and generated `scanner/data/` content appeared as untracked local files in the checkout. The root `.gitignore` does not exclude these paths or common credential filenames. Keep credentials, session tokens, and generated market data out of commits; add appropriate ignore rules before staging local artifacts.

The context document describes the intended behavior, but it is not a substitute for checking live data and runtime behavior. The current shell did not provide `python`, `py`, or `duckdb`, so I did not execute script help/validation commands or inspect Parquet metadata, sample rows, or database schemas. The schema and coverage points below are based on source code and the handoff and must be confirmed in a working Python environment before implementing enrichment.

## Data and integrity model

The downloader's expected normalized candle fields include `date`, `open`, `high`, `low`, `close`, `volume`, `oi`, `instrument_token`, `exchange`, `tradingsymbol`, `underlying`, `instrument_type`, `expiry`, `strike`, `option_type`, and `research_dataset`. `date` is the candle timestamp. These are code-level expectations; the actual stored Parquet schema still needs profiling.

The validator treats coverage as diagnostic, which is appropriate for sparse option candles. It normalizes timezone-aware values to India wall-clock time for checks and checks duplicate `(instrument_token, date)` rows. Its session map sets derivative data through 15:39 and cash index series through 15:29, but it does not include the SENSEX or SENSEX derivative dataset names. Those assumptions and the timestamp convention need validation before they are used to label data complete or invalid.

The Parquet importer chooses an instrument and timestamp key from candidate columns and uses the pair for the table primary key; it stores `source_date` as an additional column. If an existing table needs migration, the script rebuilds it and keeps one row per key ordered by `source_date`, so duplicate resolution can discard rows. Back up the database and test any migration on a copy. The historical importer expects the same `(tradingsymbol, date)` key, so the two scripts' intended key convention is aligned, though runtime schema compatibility remains to be checked.

## IV and Greeks: inputs and a safe first implementation

The handoff makes historical IV/Greeks a core research requirement. They are not downloaded as authoritative values by the current pipeline and must be estimated separately for each contract observation. A minimum input row needs:

- Option candle timestamp (`date`) and a chosen option price (initially `close`, with its limitations documented).
- Contract `strike`, `expiry`, `option_type` (`CE`/`PE`), and an unambiguous underlying mapping.
- Underlying price at the same exchange-local timestamp, plus explicit rate/carry and time-to-expiry conventions.

The code exposes plausible underlying datasets: map NIFTY options to `NIFTY_50`, BANKNIFTY options to `NIFTY_BANK`, and SENSEX options to `SENSEX` as an initial candidate. Confirm that these series are available, correctly timestamped, and appropriate for pricing before adopting the mapping; futures could be a more suitable underlying in some conventions. Join by exact timestamp after normalizing timezone conventions, and retain options with missing/misaligned underlyings with a status rather than dropping them.

Recommended sequence:

1. Profile real Parquet schemas, sample timestamps, duplicate keys, and option-to-index/futures alignment for a representative day.
2. Write a short design note choosing the price input, model, rate/carry source, expiry-time convention, units, and failure/status codes. Verify contract conventions before selecting the model.
3. Implement the pricer and feature calculation as a separate module; preserve original files and write deterministic enriched Parquet to a separate output root.
4. Keep rows even when a calculation cannot be trusted. Record null values plus a reason such as expired, invalid price, missing underlying, bound violation, or solver failure.
5. Check row counts, solver outcomes, IV/Greek ranges, time alignment, and reproducibility on a small sample before enabling larger runs or adding the enriched data to DuckDB.

Potential output fields are `implied_volatility`, `delta`, `gamma`, `theta`, `vega`, and a calculation status, with model/version and assumptions metadata where practical. Units must be consistent and explicit. Do not reuse a present-day IV for historical candles or use future timestamps to fill missing underlying prices.

## Research principles to retain

The strategy section of the handoff favors evidence-driven research of defined-risk options-selling ideas, with special attention to gap and expiry risk. Candidate signals such as India VIX thresholds, delta targets, EMAs, and support/resistance are hypotheses. Any backtest should use date-aware contracts and realistic costs, liquidity, and fills; avoid look-ahead and survivorship bias; and report drawdown and tail outcomes alongside returns. Contract specifications, exchange schedules, lot sizes, and charges should be configurable and verified for the period being studied.

## Immediate follow-up checks

1. Establish a reproducible Python environment and record dependencies.
2. Inspect Parquet and DuckDB schemas and profile timestamp alignment before making schema or Greek-calculation decisions.
3. Add ignore coverage for secrets, sessions, local databases, and generated data, then verify no sensitive files are staged.
4. Replace machine-specific importer defaults with paths relative to each script or CLI/environment configuration while retaining explicit overrides.
5. Confirm downloader checkpoint/resume behavior and validation assumptions on a copy or read-only sample before changing pipeline behavior.
