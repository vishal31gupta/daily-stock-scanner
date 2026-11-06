# Read-only row-level data audit

**Audit date:** 2026-10-20  
**Scope:** `scanner/data/market_data_clean/` and `scanner/data/nifty_research.duckdb`  
**Status:** Read-only audit complete. No IV/Greeks were calculated.

## Summary

The audit scanned all 922 current Parquet parts: 6,568,528 rows across ten datasets and ten trading dates from 2026-10-06 through 2026-10-19. Required option and futures metadata is populated; both candidate keys are unique; numeric quality checks found no null OHLC, NaN/infinity, invalid OHLC bounds, nonpositive OHLC prices, or negative volume/OI.

All option and futures timestamps have exact index matches from 09:15 through 15:29 Asia/Kolkata. Every unmatched option/futures row occurs from 15:30 through 15:39, because the corresponding cash index files end at 15:29. The NIFTY and BANKNIFTY futures contain full per-contract minute grids; SENSEX futures have 121 unobserved minutes against a 385-minute per-contract/session grid.

DuckDB opens read-only and contains all ten expected tables. Each table uses a `(tradingsymbol, date)` primary key, matching the database importer’s documented expectation. For current Parquet dates, the database contains only five of the ten dates. Every shared key and compared field value matches; there are no database-only keys for those dates. `NIFTY_OPTIONS` also contains 61,803,688 rows outside the current Parquet date scope, with historical `source_date` values beginning in 2021.

The notable data issues are the 25 option parts whose OHLC fields are physically `INT64` instead of `DOUBLE`, the 15:30–15:39 cash-index coverage gap, sparse option contract timestamps whose cause cannot be determined from these files alone, and the partial DuckDB mirror.

## Method and verification

I read `CODEX_PROJECT_CONTEXT.md`, `PROJECT_UNDERSTANDING.md`, and `DATA_AUDIT_REPORT.md` before the audit. The project’s configured PyCharm SDK resolves to Python 3.9.0 at `C:/Users/91965/VISHAL GUPTA/Code/Python/Langchain/langchain-demo/.venv/Scripts/python.exe`.

| Import | Result |
| --- | --- |
| pandas | Available, 2.2.3 |
| PyArrow | Not available (`ModuleNotFoundError`) |
| DuckDB | Available, 1.4.5 |

No package was installed and the Python environment was not changed. DuckDB’s Parquet reader and footer-schema functions were used instead of PyArrow. The existing database was opened with `read_only=True`; Parquet scans and aggregates ran through an in-memory DuckDB connection. The database stayed at 5,091,897,344 bytes with an unchanged modification time, and no `.wal` file appeared.

All 922 Parquet parts were included. Per-date/per-dataset scans covered 100 partitions and grouped duplicates across all parts in each partition. Timestamp comparisons used exact stored timestamps; `Asia/Kolkata` conversion was used to display and classify exchange-local session times. The Parquet timestamp annotation is UTC-adjusted nanoseconds, but it does not identify a named IANA timezone. The observed minute grid and the project’s timezone convention support the local-time interpretation; the stored annotation alone does not prove the original timezone name.

No login, market download, import, migration, DDL, or DML operation was run. Source Parquet, DuckDB, checkpoints, exports, production scripts, and `.gitignore` were not modified.

## Parquet inventory and schemas

Each dataset is present on all ten trading dates: 2026-10-06, 2026-10-07, 2026-10-08, 2026-10-09, 2026-10-12, 2026-10-13, 2026-10-14, 2026-10-15, 2026-10-16, and 2026-10-19.

| Dataset | Part files | Rows | Observed timestamp coverage (Asia/Kolkata) |
| --- | ---: | ---: | --- |
| `NIFTY_OPTIONS` | 382 | 3,513,455 | 09:15–15:39 on each date |
| `NIFTY_FUTURES` | 10 | 11,550 | 09:15–15:39 on each date |
| `NIFTY_50` | 10 | 3,750 | 09:15–15:29 on each date |
| `BANKNIFTY_OPTIONS` | 190 | 1,528,992 | 09:15–15:39 on each date |
| `BANKNIFTY_FUTURES` | 10 | 11,550 | 09:15–15:39 on each date |
| `NIFTY_BANK` | 10 | 3,750 | 09:15–15:29 on each date |
| `SENSEX_OPTIONS` | 280 | 1,476,552 | 09:15–15:39 on each date |
| `SENSEX_FUTURES` | 10 | 11,429 | 09:15–15:39 overall; some contracts start later |
| `SENSEX` | 10 | 3,750 | 09:15–15:29 on each date |
| `INDIA_VIX` | 10 | 3,750 | 09:15–15:29 on each date |
| **Total** | **922** | **6,568,528** | **10 trading dates** |

The reader-visible Parquet schema is consistent in field names across all datasets. Every field is nullable in the Parquet files.

| Field(s) | DuckDB reader type |
| --- | --- |
| `date` | `TIMESTAMP WITH TIME ZONE` |
| `open`, `high`, `low`, `close` | `DOUBLE` after schema union |
| `volume`, `oi`, `instrument_token` | `BIGINT` |
| `exchange`, `tradingsymbol`, `underlying`, `instrument_type`, `research_dataset` | `VARCHAR` |
| `strike` | `DOUBLE` |
| `expiry`, `option_type` | Dataset-specific types below |

| Dataset family | `expiry` | `option_type` |
| --- | --- | --- |
| Options (`NIFTY_OPTIONS`, `BANKNIFTY_OPTIONS`, `SENSEX_OPTIONS`) | `DATE` | `VARCHAR` |
| Futures (all three futures datasets) | `DATE` | `INTEGER`, physically annotated as `NullType` |
| Cash indices and `INDIA_VIX` | `INTEGER`, physically annotated as `NullType` | `INTEGER`, physically annotated as `NullType` |

In the Parquet footers, `date` is physical `INT64` with a UTC-adjusted nanosecond timestamp annotation; OHLC is generally `DOUBLE`; `volume`, `oi`, and `instrument_token` are `INT64`; strings are UTF-8 `BYTE_ARRAY`; and contract expiries are `INT32` with a `DATE` annotation. The all-null `expiry`/`option_type` placeholders in cash-index/VIX data and `option_type` in futures are represented as Parquet `NullType` fields and read by DuckDB as `INTEGER`.

| Option dataset | Parts with OHLC `DOUBLE` | Parts with OHLC `INT64` |
| --- | ---: | ---: |
| `NIFTY_OPTIONS` | 380 | 2 |
| `BANKNIFTY_OPTIONS` | 183 | 7 |
| `SENSEX_OPTIONS` | 264 | 16 |
| **Total** | **827** | **25** |

In each of the 25 mixed-type files, all four OHLC fields are `INT64`; all four fields are `DOUBLE` in the other option parts. DuckDB widens the mixed fields to `DOUBLE` when reading the dataset with `union_by_name=true`. Future writes should cast OHLC consistently before writing. The existing parts were not rewritten.

## Row-level quality, nulls, and duplicate keys

Across all 6,568,528 rows:

- There are zero nulls in the option datasets’ fields, including the required contract metadata: `instrument_token`, `tradingsymbol`, `underlying`, `instrument_type`, `expiry`, `strike`, and `option_type`. No required string metadata is blank.
- There are zero nulls in the required futures contract fields: `instrument_token`, `tradingsymbol`, `underlying`, `instrument_type`, and `expiry`; none are blank.
- The only nonzero null counts are non-applicable placeholders: `option_type` is null in every futures row (34,529 rows total); `expiry` and `option_type` are null in every cash-index/VIX row (3,750 rows per field per dataset). All other fields have zero nulls.
- Numeric columns have zero NaN and zero positive/negative infinity values.
- OHLC checks found zero incomplete rows, nonfinite prices, `low > high`, open/close outside `[low, high]`, or nonpositive OHLC values. Volume and OI have zero negative values.
- Every row’s local calendar date matches its date-directory partition.

Duplicate-key checks grouped rows across all parts in each date/dataset partition, so duplicates in separate parts were included:

| Key | Duplicate groups | Excess duplicate rows |
| --- | ---: | ---: |
| `(instrument_token, date)` | 0 | 0 |
| `(tradingsymbol, date)` | 0 | 0 |

No null key components were present in the option/futures data. The zero counts therefore cover all valid keys in the scanned data.

## Timestamp alignment and gaps

Each cash index has 3,750 rows: exactly 375 distinct minute timestamps per trading date from 09:15 through 15:29. No index timestamp is duplicated, and the expected 375-minute cash session has zero internal gaps on every date.

| Options dataset | Matching index | Option rows | Unmatched rows | Unmatched share | Unmatched interval |
| --- | --- | ---: | ---: | ---: | --- |
| `NIFTY_OPTIONS` | `NIFTY_50` | 3,513,455 | 100,847 | 2.8703% | All 15:30–15:39 |
| `BANKNIFTY_OPTIONS` | `NIFTY_BANK` | 1,528,992 | 44,422 | 2.9053% | All 15:30–15:39 |
| `SENSEX_OPTIONS` | `SENSEX` | 1,476,552 | 43,233 | 2.9280% | All 15:30–15:39 |

All option rows between 09:15 and 15:29 have an exact matching index timestamp. There are zero unmatched option rows in that shared interval and zero option timestamps outside 09:15–15:39. The exact-match check uses the observed timestamp values; the unmatched rows are due to the ten-minute difference in the source session windows, not an offset between the two timestamp series.

| Futures dataset | Matching index | Futures rows | Unmatched rows | Unmatched share | Unmatched interval |
| --- | --- | ---: | ---: | ---: | --- |
| `NIFTY_FUTURES` | `NIFTY_50` | 11,550 | 300 | 2.5974% | All 15:30–15:39 |
| `BANKNIFTY_FUTURES` | `NIFTY_BANK` | 11,550 | 300 | 2.5974% | All 15:30–15:39 |
| `SENSEX_FUTURES` | `SENSEX` | 11,429 | 300 | 2.6249% | All 15:30–15:39 |

There are 385 distinct derivative timestamps per date, covering 09:15–15:39. All derivative rows from 09:15 through 15:29 have exact matching index timestamps. Index duplicate counts and expected-grid gaps are both zero. The 300 unmatched futures rows per family are the three contracts’ ten late-session minutes across ten dates.

### Option contract timestamp sparsity

For each `(tradingsymbol, trading date)`, I counted minute slots between the first and last observed timestamp that have no row for that contract. This is a measured timestamp gap, not proof that a vendor candle was lost: the files do not establish whether a contract with no trade should have a candle in every minute.

| Options dataset | Contract/date groups | Groups with internal gaps | Missing slots between first/last | Gap segments | Longest consecutive gap |
| --- | ---: | ---: | ---: | ---: | ---: |
| `NIFTY_OPTIONS` | 10,609 | 3,610 | 216,419 | 71,041 | 372 minutes |
| `BANKNIFTY_OPTIONS` | 4,650 | 1,791 | 96,725 | 33,950 | 365 minutes |
| `SENSEX_OPTIONS` | 4,477 | 1,242 | 35,215 | 25,738 | 311 minutes |

The observed gaps are consistent with sparse option-contract activity, but the audit cannot determine whether this is expected no-trade behavior or incomplete source data. Enrichment should calculate only on rows present and retain a status for missing exact-time inputs; it should not synthesize bars from adjacent minutes.

### Futures identities, expiries, and coverage

The current files contain three futures identities per underlying:

| Dataset | Contract symbols and expiries | Rows per contract across the ten dates |
| --- | --- | --- |
| `NIFTY_FUTURES` | `NIFTY26OCTFUT` — 2026-10-27; `NIFTY26NOVFUT` — 2026-11-23; `NIFTY26DECFUT` — 2026-12-29 | 3,850 each |
| `BANKNIFTY_FUTURES` | `BANKNIFTY26OCTFUT` — 2026-10-27; `BANKNIFTY26NOVFUT` — 2026-11-23; `BANKNIFTY26DECFUT` — 2026-12-29 | 3,850 each |
| `SENSEX_FUTURES` | `SENSEX26OCTFUT` — 2026-10-29; `SENSEX26NOVFUT` — 2026-11-26; `SENSEX26DECFUT` — 2026-12-31 | 3,850; 3,800; 3,779 respectively |

Against 385 expected minutes per contract per trading date, NIFTY and BANKNIFTY have zero missing futures timestamps. SENSEX has 11,429 of 11,550 expected points: 121 unobserved minutes over 30 contract/date groups; 14 groups are short of the full grid, with at most 23 missing minutes in one group. Some gaps are at the start of the session, so these figures include leading gaps as well as internal gaps. As with options, the audit cannot tell from Parquet alone whether an unobserved minute is an expected no-trade interval.

Futures are available as a possible pricing input and cover the late 15:30–15:39 interval that lacks cash-index rows. They are not interchangeable with spot: any use requires selecting an expiry-appropriate contract and a model/carry convention consistent with the futures price. SENSEX futures also have the measured timestamp gaps above. The audit does not select a pricing model.

## DuckDB catalog and constraints

The read-only catalog contains the ten expected tables: `NIFTY_OPTIONS`, `BANKNIFTY_OPTIONS`, `SENSEX_OPTIONS`, the three futures tables, `NIFTY_50`, `NIFTY_BANK`, `SENSEX`, and `INDIA_VIX`. Each table has the Parquet columns plus `source_date VARCHAR`. Types otherwise match the dataset-specific Parquet reader schema; all tables use the same candle fields, with options’ `option_type VARCHAR`, indices’/VIX’s integer null placeholders, and futures’ integer null `option_type`.

Every table has a primary key on `(tradingsymbol, date)` and `NOT NULL` constraints on both key columns. This matches the documented DuckDB importer and historical importer assumption. The validator’s `(instrument_token, date)` duplicate check is a separate data-quality key and also has zero duplicates in the current Parquet scan.

| DuckDB table | Rows | `source_date` coverage | Distinct `source_date` values |
| --- | ---: | --- | ---: |
| `NIFTY_OPTIONS` | 63,653,361 | 2021-10-06–2026-10-13 | 559 |
| `BANKNIFTY_OPTIONS` | 753,319 | 2026-10-07–2026-10-13 | 5 |
| `SENSEX_OPTIONS` | 674,450 | 2026-10-07–2026-10-13 | 5 |
| `NIFTY_FUTURES` | 5,775 | 2026-10-07–2026-10-13 | 5 |
| `BANKNIFTY_FUTURES` | 5,775 | 2026-10-07–2026-10-13 | 5 |
| `SENSEX_FUTURES` | 5,701 | 2026-10-07–2026-10-13 | 5 |
| `NIFTY_50` | 1,875 | 2026-10-07–2026-10-13 | 5 |
| `NIFTY_BANK` | 1,875 | 2026-10-07–2026-10-13 | 5 |
| `SENSEX` | 1,875 | 2026-10-07–2026-10-13 | 5 |
| `INDIA_VIX` | 1,875 | 2026-10-07–2026-10-13 | 5 |

All `source_date` values are non-null. The DuckDB database contains 65,105,881 rows total. The high `NIFTY_OPTIONS` count includes data outside the current ten-date Parquet snapshot; its provenance is not established by this audit.

## DuckDB versus current Parquet

The comparison filtered DuckDB rows to the ten local dates represented by the current Parquet folders. It compared distinct `(tradingsymbol, date)` and `(instrument_token, date)` keys, then compared every shared non-key field for matching symbol/date rows. `source_date` has no corresponding Parquet column and was not compared.

| Dataset | Parquet rows | DuckDB rows on these dates | Parquet-only symbol/date keys | DuckDB-only keys | Matched rows with value differences |
| --- | ---: | ---: | ---: | ---: | ---: |
| `NIFTY_OPTIONS` | 3,513,455 | 1,849,673 | 1,663,782 | 0 | 0 |
| `BANKNIFTY_OPTIONS` | 1,528,992 | 753,319 | 775,673 | 0 | 0 |
| `SENSEX_OPTIONS` | 1,476,552 | 674,450 | 802,102 | 0 | 0 |
| `NIFTY_FUTURES` | 11,550 | 5,775 | 5,775 | 0 | 0 |
| `BANKNIFTY_FUTURES` | 11,550 | 5,775 | 5,775 | 0 | 0 |
| `SENSEX_FUTURES` | 11,429 | 5,701 | 5,728 | 0 | 0 |
| `NIFTY_50` | 3,750 | 1,875 | 1,875 | 0 | 0 |
| `NIFTY_BANK` | 3,750 | 1,875 | 1,875 | 0 | 0 |
| `SENSEX` | 3,750 | 1,875 | 1,875 | 0 | 0 |
| `INDIA_VIX` | 3,750 | 1,875 | 1,875 | 0 | 0 |
| **Total** | **6,568,528** | **3,302,193** | **3,266,335** | **0** | **0** |

For all ten tables, the database rows on overlapping current keys match the Parquet values in every compared shared field. Direct grouped counts of DuckDB `CAST(date AS DATE)` confirm that the database contains the five current Parquet dates 2026-10-07, 2026-10-08, 2026-10-09, 2026-10-12, and 2026-10-13; per-date Parquet row counts match DuckDB exactly on those dates. Both candidate key comparisons show zero DuckDB-only keys. The database has no rows for the other five current Parquet dates: 2026-10-06, 2026-10-14, 2026-10-15, 2026-10-16, and 2026-10-19. Thus 3,266,335 current Parquet rows/keys, all from those five absent dates, are not present in DuckDB. The 61,803,688 `NIFTY_OPTIONS` rows outside the ten-date scope may be an intentional historical import, but that provenance should be confirmed before treating the database as a rebuild of the current Parquet tree.

## Limitations and outstanding checks

- PyArrow was unavailable, so no PyArrow import or Arrow-specific schema interpretation was verified. DuckDB read all scanned Parquet parts successfully.
- The Parquet timestamp annotation identifies UTC-adjusted nanoseconds, not a named timezone. Exchange-local times were interpreted as Asia/Kolkata because that produces the observed 09:15 market open and matches the project’s existing convention. A canonical timezone policy should still be documented.
- Contract-level option gaps and SENSEX futures gaps are observed timestamp absences; vendor no-trade behavior and feed completeness cannot be determined without an authoritative source rule or comparison feed.
- The DuckDB key/value comparison does not compare `source_date`, which exists only in DuckDB. The data in the older `NIFTY_OPTIONS` rows was not compared to an external historical archive.
- No pricing-model assumptions, rates/carry inputs, exercise/settlement rules, or IV/Greek outputs were evaluated.

## Prioritized remediation plan

1. **Specify exact-time eligibility and missing-input statuses.** Use the verified index mapping for the shared 09:15–15:29 window. Keep 15:30–15:39 option rows and label the missing cash-index match; decide separately whether an expiry-matched futures price is valid for the selected pricing model. Do not drop rows or carry forward a prior price.
2. **Stabilize the input schema contract.** Cast OHLC to a consistent floating type in future Parquet writes and document the timestamp timezone. Keep readers compatible with the 25 existing integer-OHLC parts; do not rewrite the current source files as part of enrichment.
3. **Reconcile DuckDB on a verified copy.** Confirm whether the five absent Parquet dates and the older NIFTY history are intentional. Before any import or schema action, compare on a copy/backup and reconcile keys and values; do not migrate the current database based solely on this audit.
4. **Define gap semantics.** Establish whether missing option/futures minutes are normal no-trade behavior or a source completeness failure, then encode that policy in validation and enrichment statuses. Pay particular attention to the SENSEX futures 121 missing full-session points.
5. **Run a one-date enrichment pilot only after those decisions.** Read source Parquet without modifying it and write a versioned result to a separate root such as `data/market_data_enriched/v1/<date>/<dataset>/`. Preserve every source option row, include IV/Delta/Gamma/Theta/Vega plus calculation and underlying-match statuses and model/input lineage, and compare output row counts and keys to the source. Keep DuckDB and source files unchanged until the pilot is reviewed.

## Safest next step toward historical IV and Greeks

After approval, build a read-only, one-date prototype that emits a separate enriched Parquet file for a representative date. Start with exact same-timestamp cash-index inputs for 09:15–15:29 and preserve every option row with an explicit status. Keep 15:30–15:39 rows marked as lacking an index match while evaluating expiry-matched futures under a separately chosen futures pricing convention. Finalize price selection, time-to-expiry, rate/carry, and timezone assumptions before calculating IV and Greeks. Do not update raw Parquet or DuckDB during that prototype.

**No IV or Greeks have been implemented in this audit.**
