# Data Audit Report

**Audit date:** 2026-10-20  
**Scope:** Existing NIFTY, BANKNIFTY, and SENSEX research pipeline, local Parquet data, local DuckDB file, Git exposure, and reproducibility requirements.  
**Project notes read:** [CODEX_PROJECT_CONTEXT.md](CODEX_PROJECT_CONTEXT.md) and [PROJECT_UNDERSTANDING.md](PROJECT_UNDERSTANDING.md).

## Executive summary

- The clean Parquet tree contains ten trading dates from 2026-10-06 through 2026-10-19, ten dataset folders per date, and 922 Parquet part files. Footer metadata reports 6,568,528 rows in total. This is only about two weeks of data, not a multi-year archive.
- The normalized schema is broadly consistent with the downloader's 16 expected fields, but option OHLC columns vary between `DOUBLE` and `INT64` across part files. This should be normalized before relying on schema union or DuckDB imports.
- Parquet timestamp metadata is `INT64` with logical timestamp nanoseconds and `isAdjustedToUTC=true`. Footer statistics map the index datasets to 09:15-15:29 India time and options/futures to 09:15-15:39 for all ten dates. The index series therefore do not cover the last ten derivatives minutes.
- Exact timestamp-set joins, full null counts, and duplicate-key counts could not be computed. The shell has no accessible Python or DuckDB runtime. I parsed Parquet footers read-only; I did not decode row data pages or query the database.
- The DuckDB scripts both expect a primary key on `(tradingsymbol, date)`, so their code assumptions agree. The existing 5.09 GB DuckDB file's actual tables, schemas, row counts, and constraints remain unverified.
- Credentials, session files, generated data, and the database are untracked and not ignored. They are not currently tracked, but a broad `git add -A` could stage them, including the 5.09 GB database.
- **No IV or Greeks code was implemented.** No production script, Parquet file, or database table was changed. This report is the only file added for this task.

## Scope and method

Both project notes were read before the audit. The audit used read-only Git commands, source inspection, file metadata, and a small Node.js footer reader using built-in modules. It parsed the Parquet footer metadata for all 922 `.parquet` files to obtain row counts, physical/logical field types, null-count statistics where present, and timestamp minimum/maximum statistics. It did not modify or copy the data.

The footer reader is not a complete Parquet query engine: it did not decompress data pages or evaluate row values. A command lookup found no accessible `python`, `py`, `duckdb`, or equivalent executable; Node.js and .NET were available, but DuckDB and Parquet Node packages were not. Consequently:

- Footer row counts, schemas, file counts, and min/max timestamp statistics below are **verified from the files' metadata**.
- Timestamp coverage is a **range-level assessment**. Exact minute-by-minute joins and duplicate keys require reading row values and were not verified.
- Footer null statistics are reported where available. Missing-value totals are **incomplete** where the writer omitted statistics, especially for all-null metadata columns.
- The existing DuckDB file was **not opened**. No script help, import, export, validation, login, API, or database command was run.
- No credential values or access tokens were read or printed.

## 1. Parquet schemas, rows, and coverage

### Data coverage and row counts

Observed date folders are 2026-10-06, 07, 08, 09, 12, 13, 14, 15, 16, and 19. Each contains the ten dataset folders listed below. There are 922 Parquet parts totalling 77,380,812 bytes (about 73.8 MiB). In addition, `market_data_clean/2026-10-06.zip` is present at the root of the clean-data directory; it is not a date folder and is not included in these counts. Its purpose was not assessed.

Row counts below are sums of Parquet footer `num_rows`, not row-by-row scans.

| Dataset | Part files | Rows across ten dates |
| --- | ---: | ---: |
| `NIFTY_OPTIONS` | 382 | 3,513,455 |
| `BANKNIFTY_OPTIONS` | 190 | 1,528,992 |
| `SENSEX_OPTIONS` | 280 | 1,476,552 |
| `NIFTY_FUTURES` | 10 | 11,550 |
| `BANKNIFTY_FUTURES` | 10 | 11,550 |
| `SENSEX_FUTURES` | 10 | 11,429 |
| `NIFTY_50` | 10 | 3,750 |
| `NIFTY_BANK` | 10 | 3,750 |
| `SENSEX` | 10 | 3,750 |
| `INDIA_VIX` | 10 | 3,750 |
| **Total** | **922** | **6,568,528** |

Representative daily counts from footer metadata:

| Date | NIFTY options | BANKNIFTY options | SENSEX options | Each cash index/VIX | NIFTY futures | BANKNIFTY futures | SENSEX futures |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-10-06 | 315,562 | 143,026 | 75,081 | 375 | 1,155 | 1,155 | 1,130 |
| 2026-10-14 | 353,126 | 150,091 | 198,050 | 375 | 1,155 | 1,155 | 1,153 |
| 2026-10-19 | 317,958 | 166,075 | 163,189 | 375 | 1,155 | 1,155 | 1,136 |

The underlying index and India VIX counts are exactly 375 per date in the metadata. NIFTY and BANKNIFTY futures have 1,155 rows per date; SENSEX futures vary from 1,123 to 1,155. Futures rows include multiple contracts, so these totals are not the count of one unique futures price per minute.

### Physical and logical schema

All fields are marked OPTIONAL in the Parquet schema. The common schema has the downloader's 16 fields, with these verified Parquet types:

| Column(s) | Parquet type observed |
| --- | --- |
| `date` | `INT64`, logical `TIMESTAMP(NANOS, isAdjustedToUTC=true)` |
| `open`, `high`, `low`, `close` | Usually `DOUBLE`; see type drift below |
| `volume`, `oi`, `instrument_token` | `INT64` |
| `exchange`, `tradingsymbol`, `underlying`, `instrument_type`, `research_dataset` | `BYTE_ARRAY` with UTF8 string annotation |
| `expiry` | `INT32` with logical `DATE` on derivative datasets; nullable `INT32` without a date annotation on cash index/VIX data |
| `strike` | `DOUBLE` |
| `option_type` | UTF8 string on option datasets; nullable `INT32` on non-option datasets |

The Arrow timezone label embedded in the writer metadata, if present, was not decoded. The standard Parquet timestamp annotation confirms UTC-adjusted nanosecond instants, but does not by itself identify a named timezone such as `Asia/Kolkata`.

### Schema drift and missing values

There are 25 option part files where all four OHLC columns are stored as `INT64` instead of `DOUBLE`:

| Dataset | `DOUBLE` OHLC parts / rows | `INT64` OHLC parts / rows |
| --- | ---: | ---: |
| `NIFTY_OPTIONS` | 380 / 3,513,117 | 2 / 338 |
| `BANKNIFTY_OPTIONS` | 183 / 1,528,725 | 7 / 267 |
| `SENSEX_OPTIONS` | 264 / 1,474,368 | 16 / 2,184 |

Other datasets had one physical schema across their part files. This drift likely comes from writing batches whose observed prices are all integral versus batches containing decimal prices; that explanation is an inference from the type split, not a row-value check. Normalize OHLC dtypes explicitly before writing future parts, and test DuckDB/Pandas schema union against the existing mixed files before importing them.

Footer statistics report `null_count=0` for the columns and row groups where the statistic is present, including the option OHLC fields, volume, OI, timestamp, token, and populated contract/string fields. Some all-null or non-applicable fields have no usable statistics: `expiry`/`option_type` in cash index files and `option_type` in futures files are nullable placeholders. The current research-instrument CSV has blank expiry and strike 0 for the four index rows; the downloader assigns `None` to missing expiry and to option type outside CE/PE instruments ([normalization code](../03_eod_downloader.py#L321)). Their full null counts cannot be recovered from the footer metadata alone. No complete null, NaN, infinity, invalid-OHLC, or missing-contract-field scan was possible.

Duplicate keys were **not verified**. The validator checks `(instrument_token, date)` and the Excel exporter checks duplicates for its export copy, but neither was run. The DuckDB importer's key uses `(tradingsymbol, date)`, a different identifier choice from the validator. A full row-level audit should report both key definitions and any conflicting groups rather than treating footer statistics as evidence of uniqueness.

## 2. Option/index/futures timestamp alignment

For all ten dates, footer timestamp statistics show:

| Dataset family | First timestamp | Last timestamp | Rows per date |
| --- | --- | --- | ---: |
| `NIFTY_50`, `NIFTY_BANK`, `SENSEX`, `INDIA_VIX` | 09:15 India time | 15:29 India time | 375 each |
| NIFTY/BANKNIFTY/SENSEX options | 09:15 India time | 15:39 India time | Varies by contracts and active candles |
| NIFTY/BANKNIFTY futures | 09:15 India time | 15:39 India time | 1,155 each |
| SENSEX futures | 09:15 India time | 15:39 India time | 1,123-1,155 |

This confirms a **session-range mismatch**: cash index data ends ten minutes before the options/futures data. It does not prove exact timestamp-set equality for the shared interval. Footer min/max and row counts cannot detect internal gaps, duplicate timestamps, or whether an option has a matching index candle at each minute.

The most plausible initial index mapping is `NIFTY_OPTIONS` -> `NIFTY_50`, `BANKNIFTY_OPTIONS` -> `NIFTY_BANK`, and `SENSEX_OPTIONS` -> `SENSEX`, as also suggested in the project note. These mappings still need exact key-set comparison. Do not forward-fill the 15:29 index value into option observations through 15:39 without an explicitly justified pricing convention and a stale-underlying status.

Futures should be included in the pricing-model assessment because their recorded session extends through 15:39. The current research-instrument CSV lists three futures contracts for each underlying; the NIFTY/BANKNIFTY row totals are consistent with three contracts across a 385-minute session, but the contract identities in historical Parquet rows were not decoded. Futures are not a drop-in replacement for spot: select the correct expiry/tenor, align exact timestamps, and use a model/carry convention consistent with a forward or futures price. Compare a spot-index path with an expiry-matched futures/forward path on the overlapping period; keep rows with missing underlyings flagged instead of silently filling them. This audit does not select a pricing model.

## 3. DuckDB tables and keys

The local `scanner/data/nifty_research.duckdb` file exists and is 5,091,897,344 bytes (about 4.74 GiB). It was not opened because no DuckDB runtime was available; its actual table names, columns, row counts, indexes/constraints, duplicate groups, and compatibility with the current Parquet files are **unknown**.

The two importer scripts agree on the intended key:

- `06_import_parquet_to_duckdb.py` detects an instrument column (the current Parquet schema has `tradingsymbol`) and timestamp (`date`), creates a composite primary key from those two columns, and adds `source_date` as a separate column. It may rebuild an existing table when its key differs; during rebuild it keeps one row per key ordered by `source_date`, which can discard duplicates ([schema creation/migration](../06_import_parquet_to_duckdb.py#L156)).
- `07_import_historical_nifty_options.py` requires an existing `NIFTY_OPTIONS` table and checks for a primary key on `(tradingsymbol, date)` ([key check](../07_import_historical_nifty_options.py#L212)).

Thus the **script-level assumption matches** the project notes and current Parquet column names, but the live database assumption is not verified. Do not run script 06 against the original database as an audit: its schema-repair path can rebuild tables. The next database inspection should use DuckDB's read-only connection mode; if a query tool cannot guarantee read-only behavior, use a verified temporary copy first.

## 4. Seven-script consistency review

All seven scripts exist. The following integration issues are verified from source:

| Area | Finding |
| --- | --- |
| Working directory and paths | Scripts 01-05 use paths relative to the current working directory, so their expected launch directory is `scanner`. The context's example commands say to run from repository root with `python .\03_eod_downloader.py`, but the actual scripts are nested under `scanner/`. Scripts 06 and 07 hard-code this machine's absolute Windows `PROJECT_ROOT`; 06 offers `--data-root` and `--db-path`, and 07 offers `--db-path`. See [06](../06_import_parquet_to_duckdb.py#L32) and [07](../07_import_historical_nifty_options.py#L34). |
| Data roots | Script 03 defaults to `data/market_data`; `--fresh` changes it to `data/market_data_clean`, and `--output-root` overrides both ([arguments](../03_eod_downloader.py#L392)). Scripts 04-06 default to the clean root. The current checkout has clean data and no visible legacy `market_data` folder. |
| Date defaults | Script 03 has `TARGET_DATE = 2026-10-08`; script 04 also defaults to 2026-10-08; script 05 requires a date; script 06 imports all date folders when `--date` is omitted. These defaults do not automatically mean that 2026-10-08 is the intended next run date. |
| Dataset names | Downloader and data folders use `NIFTY_OPTIONS`, `NIFTY_FUTURES`, `BANKNIFTY_OPTIONS`, `BANKNIFTY_FUTURES`, `SENSEX_OPTIONS`, `SENSEX_FUTURES`, `NIFTY_50`, `NIFTY_BANK`, `SENSEX`, and `INDIA_VIX`. This matches the broad dataset naming in the project notes. |
| Validation session map | Script 05 defines session-close entries for NIFTY/BANKNIFTY derivatives and NIFTY 50, NIFTY BANK, and INDIA VIX. It omits `SENSEX`, `SENSEX_OPTIONS`, and `SENSEX_FUTURES`, so those datasets do not receive its out-of-session check ([session map](../05_validate_market_data.py#L19)). |
| Timestamp handling | Script 04 strips timezone metadata without converting the displayed wall time ([export copy](../04_export_market_data_to_excel.py#L81)); script 05 converts aware timestamps to `Asia/Kolkata` before dropping timezone ([validator](../05_validate_market_data.py#L39)); script 07 removes timezone metadata while preserving wall-clock time for vendor imports. This is safe only when each input timestamp's timezone semantics are known and consistent. The Parquet footer gives UTC-adjusted nanosecond timestamps, but the named Arrow timezone was not verified. |
| Validation coverage | Script 05 requires only `date`, `instrument_token`, and OHLC columns. It does not enforce the complete option contract fields needed for pricing. It checks duplicate `(instrument_token, date)` rows and has not been run in this audit. |
| Historical instrument selection | Script 02 filters expired contracts using the machine's current date, then overwrites the current `research_instruments.csv` while also saving a dated snapshot. Reconstructing a historical contract universe must use the appropriate dated instruments, not just today's current file. |
| Schema writes | Script 03 validates non-null finite OHLC values before writing, but does not cast integral OHLC values to a stable float type. The Parquet footer scan confirms mixed int/float option schemas. |

## 5. Git and sensitive/generated files

`git ls-files` contains the scripts and docs but no `scanner/config.json`, `scanner/session.json`, `scanner/data/`, Parquet parts, or DuckDB database. At audit start, `git status --short` showed `scanner/config.json`, `scanner/session.json`, and the whole `scanner/data/` directory as untracked. The root `.gitignore` contains standard Python/build ignores but no ignore rules for credentials, local session state, `scanner/data`, Parquet, Excel exports, or DuckDB. `git check-ignore` returned no matching rule for those sample paths.

This is a **high commit risk**: `config.json` is expected to contain `api_key` and `api_secret`; `session.json` is expected to contain the Kite access token; and `scanner/data/nifty_research.duckdb` alone is about 4.74 GiB. I verified file/path presence and Git state only; I did not inspect or print secret values. A broad `git add -A` could stage the secrets and large generated artifacts. Add and verify ignore rules before staging anything, and keep any intentionally versioned data under an explicit policy.

The pre-existing modification to `scanner/extra/scripts.txt` and other untracked files were left untouched. This audit did not stage or commit files.

## 6. Dependencies and environment reproducibility

No `requirements.txt`, `pyproject.toml`, `Pipfile`, Conda environment, lockfile, Dockerfile, or other dependency manifest was found. There is no pinned Python version or dependency version in the repository. The current shell could not resolve Python or DuckDB, so I could not report installed versions or verify a working environment.

Dependencies implied by the source:

| Package | Needed for |
| --- | --- |
| `pandas` | Instrument, Parquet, Excel, and vendor-file data handling throughout scripts 02-07 and the data viewer |
| `numpy` | Downloader numeric validation in script 03 |
| `kiteconnect` | Zerodha login, instrument download, and candle download in scripts 01-03 |
| `duckdb` | Parquet-to-DuckDB and historical import scripts 06-07, plus the DuckDB viewer |
| `pyarrow` | Pandas Parquet read/write support; `fastparquet` is an alternative engine, but the project should pin and test one engine rather than leave it implicit |
| `openpyxl` | Explicit Excel read/write engine for `.xlsx` files and generated audit/export workbooks |
| `xlrd` | Needed only for historical `.xls` vendor inputs |
| `tzdata` | Recommended on Windows to make `Asia/Kolkata` timezone conversion reproducible when the OS timezone database is unavailable |

The scripts also use Python standard-library modules such as `argparse`, `datetime`, `json`, `logging`, `os`, `pathlib`, `sys`, `time`, `typing`, and `zipfile`. No test/development dependencies are declared. Establish a Python environment, select compatible versions, pin them in a manifest/lock file, and verify Pandas/PyArrow/DuckDB interoperability before treating a machine as reproducible. Exact version pins are outstanding because no runtime was available to test them.

## Outstanding checks

1. Full row-level null/NaN/infinity and OHLC validity counts for representative dates and then all dates.
2. Duplicate counts using `(instrument_token, date)` and `(tradingsymbol, date)`, within and across part files.
3. Exact timestamp-set comparisons for each options/index pair, including the 15:30-15:39 interval, internal gaps, duplicates, and timezone annotations.
4. Futures contract identities/expiries and alignment so a forward/futures price can be compared without mixing expiries.
5. Read-only DuckDB table/column/row-count/constraint inspection and verification that the ten expected dataset tables have the expected key; check whether the existing database has rows not represented in current Parquet.
6. Run script 05 against a representative date after confirming it writes only report files; inspect SENSEX validation coverage before relying on its report.
7. Confirm intended use of the root-level `2026-10-06.zip` archive.

## Prioritized remediation plan

1. **Protect the checkout first:** add ignore rules for `scanner/config.json`, `scanner/session.json`, local database files, exports, checkpoints, and generated `scanner/data/`; verify `git status`/`git check-ignore` without staging or removing local files.
2. **Make the environment reproducible:** choose a supported Python version, declare/pin the packages above, and run scripts from the documented `scanner` working directory. Keep credentials supplied locally or through a secret store.
3. **Finish the read-only data audit:** use PyArrow/DuckDB to scan representative dates 2026-10-06, 2026-10-14, and 2026-10-19, then the full date range. Record per-column nulls, duplicate keys, invalid OHLC, min/max/unique timestamps, and schema variants. Open the existing database read-only or query a verified copy; do not invoke migration/import scripts.
4. **Resolve alignment and schema issues:** quantify exact index matches by option observation, identify options after the cash-index cutoff, evaluate expiry-matched futures/forward prices, normalize OHLC dtypes, and make timestamp timezone behavior consistent. Add SENSEX datasets to validation-session checks.
5. **Only then design enrichment:** document model, price input, underlying selection, expiry-time/rate/carry assumptions, output units, and failure statuses. Preserve raw inputs and make reruns deterministic.

## Safest next step toward historical IV and Greeks

After the missing read-only scans are completed, implement a separate enrichment prototype that reads one representative day's option and underlying Parquet without changing them and writes to a new root such as `data/market_data_enriched/v1/<date>/<dataset>/`. Begin with same-timestamp spot joins over the verified overlap; separately evaluate an expiry-matched futures/forward input for 15:30-15:39 and document the pricing model needed for that input. Keep every option row, writing null IV/Delta/Gamma/Theta/Vega plus a reason when its price, underlying, expiry, or solver result is invalid. Compare output row counts and timestamps with the source, validate known cases, and only expand to all dates after the sample is reproducible. Do not update the DuckDB database or raw Parquet until the separate output and audit are accepted.
