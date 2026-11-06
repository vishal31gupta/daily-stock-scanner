# NIFTY options enrichment V1

This note describes the separate historical NIFTY options IV and Greeks
pipeline in `08_enrich_nifty_options.py` and
`nifty_options_enrichment.py`. It does not change the source Parquet or the
existing DuckDB research database.

## Inputs and joins

Run commands from the `scanner` directory. By default the program reads:

- `data/market_data_clean/YYYY-MM-DD/NIFTY_OPTIONS/part_*.parquet`
- `data/market_data_clean/YYYY-MM-DD/NIFTY_50/part_*.parquet`
- `data/market_data_clean/YYYY-MM-DD/INDIA_VIX/part_*.parquet`

Options are joined to NIFTY 50 and India VIX by exact `date` timestamp
equality. Inputs are interpreted in Asia/Kolkata (UTC+05:30). The pipeline
does not forward-fill, use the nearest observation, synthesize candles, use
futures, or drop an option row. It checks that each reference series has no
duplicate timestamps before joining, to prevent accidental row multiplication.

The option candle `close` is the reference option price. It is a candle close,
not necessarily an executable price or a bid/ask midpoint. NIFTY cash spot is
the BSM underlying. India VIX is retained as a separate matched feature; it is
not used as the individual option's IV.

## Model and assumptions

The calculation uses European Black–Scholes–Merton with continuous dividend
yield. Calls are `CE`; puts are `PE`. IV is solved with bounded bisection
against the option close, after theoretical lower and upper price-bound checks.
The default solver range is 0.000001 to 5.0 annualized volatility, the absolute
model-price tolerance is 1e-8, and the maximum is 100 iterations. These are
numerical settings, not market assumptions; each can be changed on the CLI.

Every real-data run must provide all five of these values explicitly:

- `--risk-free-rate`: annual continuously compounded decimal rate, such as
  `0.06` for 6%.
- `--dividend-yield`: annual continuous decimal yield.
- `--expiry-time`: contract expiry wall time in Asia/Kolkata.
- `--year-fraction-basis`: `ACT/365` or `ACT/365.25`.
- `--assumptions-as-of-date`: the date on which the supplied rate/yield
  inputs were known. It must be on or before every selected trading date;
  otherwise the run is rejected to prevent backward use of future-known inputs.

The expiry timestamp is the option metadata's `expiry` date combined with the
provided expiry time. Missing expiry is never inferred from a symbol. The time
fraction uses elapsed seconds divided by 365 or 365.25 days. Expired and
zero-time observations are retained with `INVALID_TIME_TO_EXPIRY`.

There is no historical rate or dividend curve in the inputs. The explicit
user-supplied values are therefore recorded in every row and marked
`assumptions_provisional=true` in the output and run report. The CLI does not
provide hidden rate, yield, expiry-time, or day-count defaults.

The as-of date is recorded in each output row and report. The pipeline cannot
independently validate the provenance of a manually entered rate or yield;
the operator must choose inputs genuinely known as of that date. A single
run-level value is applied to every selected observation on or after that
date, so a multi-date run is a constant-assumption scenario, not a historical
rate/dividend curve.

Greek conventions:

- IV: decimal annualized volatility (`0.20` means 20%).
- Delta: option price change per one index point.
- Gamma: Delta change per one index point.
- Vega: option price change per one volatility percentage point (BSM vega
  divided by 100).
- Theta: option price change per calendar day (annual BSM theta divided by
  365).

## Status behavior

`spot_match_status` and `vix_match_status` independently report `MATCHED`,
`MISSING_SPOT`, or `MISSING_VIX`. A missing exact NIFTY spot match prevents
IV/Greek calculation. A missing VIX match does not: if the pricing inputs are
valid, the row has calculated IV/Greeks and `calculation_status=MISSING_VIX`.
The report counts `OK` and `MISSING_VIX` as successfully calculated.

Failed rows remain in the output with null IV/Greeks and a reason. The
calculation status includes `INVALID_OPTION_PRICE`, `MISSING_STRIKE`,
`INVALID_STRIKE`, `MISSING_EXPIRY`, `INVALID_OPTION_TYPE`,
`INVALID_TIME_TO_EXPIRY`, `PRICE_OUT_OF_MODEL_BOUNDS`,
`IV_SOLVER_FAILED`, and `NON_FINITE_RESULT`; `INVALID_SPOT_PRICE` is also
reported when a matched spot close is unusable.

The 2026-11-03 source scan found NIFTY options through 15:39 while NIFTY 50 and
India VIX end at 15:29. Rows at later timestamps therefore remain in the
output with a missing-spot status and no IV/Greeks. No futures substitute is
used in V1.

## Run commands

Preview one date without creating output files:

```powershell
cd scanner
python 08_enrich_nifty_options.py --date 2026-11-03 --dry-run
```

Run one date after reviewing the preview and supplying explicit assumptions:

```powershell
python 08_enrich_nifty_options.py --date 2026-11-03 `
  --risk-free-rate 0.06 --dividend-yield 0.01 `
  --expiry-time 15:40 --year-fraction-basis ACT/365 `
  --assumptions-as-of-date 2026-11-03
```

The example rate and yield are placeholders only; the report marks them
provisional. Change them to the values selected for the research run. A
calendar range can be selected with both `--start-date` and `--end-date`;
only date folders containing NIFTY option Parquet parts are processed. No
date is selected implicitly. Use `--input-root` or `--output-root` to
override paths. The expected working directory is `scanner`.

Output files are written separately under:

```text
data/market_data_enriched/v1/NIFTY_OPTIONS/YYYY-MM-DD/enriched.parquet
data/market_data_enriched/v1/NIFTY_OPTIONS/enrichment_report.json
```

The program refuses to replace an existing output or report unless
`--overwrite` is supplied. It reads Parquet through DuckDB and uses pandas
only for bounded output batches; it does not require PyArrow. It never opens
`nifty_research.duckdb`, reads credentials, logs into Kite, downloads market
data, or imports rows into a database.

## Report and checks

The JSON report includes selected input files and dates, row and file counts,
status and option-type breakdowns, model inputs and solver settings, output
schema, output paths, and checks. Before publishing staged files, the program
checks per-date input/output row counts, compares all original columns as row
multisets in both directions, and verifies one consistent output schema for
all selected dates.

The generated Parquet data is deterministic for the same source rows and
assumptions at the value level. The run report includes its creation time, so
the report file itself is intentionally different on a later run.

## Runtime note

The PyCharm interpreter checked during implementation is Python 3.9.0 with
pandas 2.2.3 and DuckDB 1.4.5. Those imports and versions were available for
the synthetic tests. No packages were installed. The repository's
`requirements.txt` currently proposes newer CPython 3.13 pins; those pins
were not used or runtime-tested for this implementation.

