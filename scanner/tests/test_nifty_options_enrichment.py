import datetime as dt
import importlib.util
import math
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List

import duckdb


SCANNER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCANNER))

from nifty_options_enrichment import (  # noqa: E402
    INDIA_TZ,
    IVSolverError,
    ModelAssumptions,
    bsm_greeks,
    bsm_price,
    enrich_option_row,
    solve_implied_volatility,
    time_to_expiry_years,
)


_SPEC = importlib.util.spec_from_file_location(
    "nifty_options_enrichment_cli", SCANNER / "08_enrich_nifty_options.py"
)
cli = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(cli)

OBSERVATION = dt.datetime(2026, 1, 2, 10, 0, tzinfo=INDIA_TZ)
EXPIRY = dt.date(2026, 2, 2)
ASSUMPTIONS = ModelAssumptions(
    risk_free_rate=0.06,
    dividend_yield=0.01,
    expiry_time=dt.time(15, 40),
    year_fraction_basis="ACT/365",
    assumptions_as_of_date=dt.date(2026, 1, 2),
    sigma_min=1e-5,
    sigma_max=3.0,
    solver_tolerance=1e-9,
)


def option_row(option_type: str = "CE", volatility: float = 0.25) -> Dict[str, Any]:
    years = time_to_expiry_years(
        OBSERVATION, EXPIRY, ASSUMPTIONS.expiry_time,
        ASSUMPTIONS.year_fraction_basis,
    )
    close = bsm_price(option_type, 100.0, 100.0, years, 0.06, 0.01, volatility)
    return {
        "date": OBSERVATION,
        "open": close,
        "high": close + 0.5,
        "low": max(close - 0.5, 0.01),
        "close": close,
        "volume": 10,
        "oi": 100,
        "instrument_token": 123456,
        "exchange": "NFO",
        "tradingsymbol": "NIFTY26FEB10000CE",
        "underlying": "NIFTY",
        "instrument_type": "CE",
        "expiry": EXPIRY,
        "strike": 100.0,
        "option_type": option_type,
        "research_dataset": "NIFTY_OPTIONS",
    }


def write_parquet_dataset(
    root: Path, date_text: str, dataset: str, rows: List[Dict[str, Any]],
    integer_ohlc: bool = False,
) -> Path:
    folder = root / date_text / dataset
    folder.mkdir(parents=True, exist_ok=True)
    output = folder / "part_000001.parquet"
    connection = duckdb.connect(database=":memory:")
    connection.execute("SET TimeZone='Asia/Kolkata'")
    if dataset == "NIFTY_OPTIONS":
        ohlc_type = "BIGINT" if integer_ohlc else "DOUBLE"
        connection.execute(f"""
            CREATE TABLE source (
                date TIMESTAMP WITH TIME ZONE,
                open {ohlc_type}, high {ohlc_type}, low {ohlc_type}, close {ohlc_type},
                volume BIGINT, oi BIGINT, instrument_token BIGINT,
                exchange VARCHAR, tradingsymbol VARCHAR, underlying VARCHAR,
                instrument_type VARCHAR, expiry DATE, strike DOUBLE,
                option_type VARCHAR, research_dataset VARCHAR
            )
        """)
    else:
        connection.execute("""
            CREATE TABLE source (
                date TIMESTAMP WITH TIME ZONE,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                volume BIGINT, oi BIGINT, instrument_token BIGINT,
                exchange VARCHAR, tradingsymbol VARCHAR, underlying VARCHAR,
                instrument_type VARCHAR, expiry INTEGER, strike DOUBLE,
                option_type INTEGER, research_dataset VARCHAR
            )
        """)
    if rows:
        if dataset == "NIFTY_OPTIONS":
            values = [
                tuple(row[name] for name in (
                    "date", "open", "high", "low", "close", "volume", "oi",
                    "instrument_token", "exchange", "tradingsymbol", "underlying",
                    "instrument_type", "expiry", "strike", "option_type",
                    "research_dataset",
                ))
                for row in rows
            ]
        else:
            values = [
                (
                    row["date"], row["open"], row["high"], row["low"], row["close"],
                    row["volume"], row["oi"], row["instrument_token"], row["exchange"],
                    row["tradingsymbol"], row["underlying"], row["instrument_type"],
                    None, row["strike"], None, row["research_dataset"],
                )
                for row in rows
            ]
        connection.executemany("INSERT INTO source VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values)
    connection.execute(
        f"COPY source TO '{output.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    connection.close()
    return output


class IVAndGreeksTests(unittest.TestCase):
    def test_exact_spot_and_vix_timestamps_are_recorded(self):
        row = option_row()
        result = enrich_option_row(
            row, OBSERVATION, 100.0, OBSERVATION, 14.2, ASSUMPTIONS
        )
        self.assertEqual(result.spot_match_status, "MATCHED")
        self.assertEqual(result.vix_match_status, "MATCHED")
        self.assertEqual(result.spot_timestamp, OBSERVATION)
        self.assertEqual(result.vix_timestamp, OBSERVATION)
        self.assertEqual(result.calculation_status, "OK")

    def test_no_nearest_timestamp_or_forward_fill(self):
        row = option_row()
        later = OBSERVATION + dt.timedelta(minutes=1)
        result = enrich_option_row(row, later, 100.0, later, 14.2, ASSUMPTIONS)
        self.assertEqual(result.calculation_status, "MISSING_SPOT")
        self.assertEqual(result.spot_match_status, "MISSING_SPOT")
        self.assertIsNone(result.spot_price)

    def test_missing_spot_blocks_calculation_and_keeps_vix_status(self):
        result = enrich_option_row(
            option_row(), None, None, OBSERVATION, 14.2, ASSUMPTIONS
        )
        self.assertEqual(result.calculation_status, "MISSING_SPOT")
        self.assertEqual(result.vix_match_status, "MATCHED")
        self.assertIsNone(result.implied_volatility)
        self.assertIsNone(result.delta)

    def test_missing_vix_does_not_block_iv_or_greeks(self):
        result = enrich_option_row(
            option_row(), OBSERVATION, 100.0, None, None, ASSUMPTIONS
        )
        self.assertEqual(result.calculation_status, "MISSING_VIX")
        self.assertEqual(result.vix_match_status, "MISSING_VIX")
        self.assertIsNotNone(result.implied_volatility)
        self.assertIsNotNone(result.delta)

    def test_call_iv_is_solved(self):
        result = enrich_option_row(
            option_row("CE", 0.25), OBSERVATION, 100.0, OBSERVATION, 14.2,
            ASSUMPTIONS,
        )
        self.assertAlmostEqual(result.implied_volatility, 0.25, places=7)

    def test_put_iv_is_solved_independently(self):
        result = enrich_option_row(
            option_row("PE", 0.32), OBSERVATION, 100.0, OBSERVATION, 14.2,
            ASSUMPTIONS,
        )
        self.assertAlmostEqual(result.implied_volatility, 0.32, places=7)
        self.assertLess(result.delta, 0)

    def test_greek_units_match_finite_differences(self):
        t = 0.4
        spot, strike, rate, dividend, volatility = 100.0, 102.0, 0.04, 0.01, 0.3
        greeks = bsm_greeks("CE", spot, strike, t, rate, dividend, volatility)
        h = 0.01
        up = bsm_price("CE", spot + h, strike, t, rate, dividend, volatility)
        mid = bsm_price("CE", spot, strike, t, rate, dividend, volatility)
        down = bsm_price("CE", spot - h, strike, t, rate, dividend, volatility)
        self.assertAlmostEqual(greeks["delta"], (up - down) / (2 * h), places=5)
        self.assertAlmostEqual(greeks["gamma"], (up - 2 * mid + down) / (h * h), places=4)
        vol_up = bsm_price("CE", spot, strike, t, rate, dividend, volatility + 0.01)
        vol_down = bsm_price("CE", spot, strike, t, rate, dividend, volatility - 0.01)
        self.assertAlmostEqual(greeks["vega"], (vol_up - vol_down) / 2.0, places=5)
        self.assertTrue(math.isfinite(greeks["theta"]))

    def test_invalid_option_close_is_explicit(self):
        for close in (None, 0.0, float("nan")):
            with self.subTest(close=close):
                row = option_row()
                row["close"] = close
                result = enrich_option_row(
                    row, OBSERVATION, 100.0, OBSERVATION, 14.0, ASSUMPTIONS
                )
                self.assertEqual(result.calculation_status, "INVALID_OPTION_PRICE")

    def test_invalid_strike_is_explicit(self):
        row = option_row()
        row["strike"] = 0
        result = enrich_option_row(row, OBSERVATION, 100, OBSERVATION, 14, ASSUMPTIONS)
        self.assertEqual(result.calculation_status, "INVALID_STRIKE")

    def test_missing_expiry_is_explicit(self):
        row = option_row()
        row["expiry"] = None
        result = enrich_option_row(row, OBSERVATION, 100, OBSERVATION, 14, ASSUMPTIONS)
        self.assertEqual(result.calculation_status, "MISSING_EXPIRY")

    def test_invalid_option_type_is_explicit(self):
        row = option_row()
        row["option_type"] = "XX"
        result = enrich_option_row(row, OBSERVATION, 100, OBSERVATION, 14, ASSUMPTIONS)
        self.assertEqual(result.calculation_status, "INVALID_OPTION_TYPE")

    def test_expired_contract_is_explicit(self):
        row = option_row()
        row["expiry"] = dt.date(2026, 1, 1)
        result = enrich_option_row(row, OBSERVATION, 100, OBSERVATION, 14, ASSUMPTIONS)
        self.assertEqual(result.calculation_status, "INVALID_TIME_TO_EXPIRY")
        self.assertLess(result.time_to_expiry_years, 0)

    def test_theoretical_price_bounds_are_enforced(self):
        row = option_row()
        row["close"] = 200.0
        result = enrich_option_row(row, OBSERVATION, 100, OBSERVATION, 14, ASSUMPTIONS)
        self.assertEqual(result.calculation_status, "PRICE_OUT_OF_MODEL_BOUNDS")

    def test_unbracketed_volatility_is_solver_failure(self):
        row = option_row("CE", 0.5)
        narrow = ModelAssumptions(
            risk_free_rate=0.06, dividend_yield=0.01,
            expiry_time=dt.time(15, 40), year_fraction_basis="ACT/365",
            assumptions_as_of_date=dt.date(2026, 1, 2),
            sigma_min=1e-5, sigma_max=0.1,
        )
        result = enrich_option_row(row, OBSERVATION, 100, OBSERVATION, 14, narrow)
        self.assertEqual(result.calculation_status, "IV_SOLVER_FAILED")

    def test_non_finite_solver_input_is_rejected(self):
        with self.assertRaises(ValueError):
            solve_implied_volatility(
                "CE", float("nan"), 100, 100, 0.25, 0.05, 0.01
            )
        result = enrich_option_row(
            option_row(), OBSERVATION, float("inf"), OBSERVATION, 14, ASSUMPTIONS
        )
        self.assertEqual(result.calculation_status, "INVALID_SPOT_PRICE")

    def test_future_known_assumptions_are_rejected_for_earlier_observations(self):
        future_assumptions = ModelAssumptions(
            risk_free_rate=0.06, dividend_yield=0.01,
            expiry_time=dt.time(15, 40), year_fraction_basis="ACT/365",
            assumptions_as_of_date=dt.date(2026, 1, 3),
        )
        result = enrich_option_row(
            option_row(), OBSERVATION, 100, OBSERVATION, 14, future_assumptions
        )
        self.assertEqual(result.calculation_status, "FUTURE_ASSUMPTION_INPUT")


class ParquetPipelineTests(unittest.TestCase):
    def _fixture(self, root: Path):
        date_text = "2026-01-02"
        first = OBSERVATION.replace(hour=15, minute=29)
        late = OBSERVATION.replace(hour=15, minute=30)
        first_option = option_row()
        first_option["date"] = first
        first_option["oi"] = None
        late_option = option_row()
        late_option["date"] = late
        options = [first_option, late_option]
        write_parquet_dataset(root, date_text, "NIFTY_OPTIONS", options)

        spot = {
            "date": first, "open": 100.0, "high": 100.0, "low": 100.0,
            "close": 100.0, "volume": 0, "oi": 0, "instrument_token": 1,
            "exchange": "NSE", "tradingsymbol": "NIFTY 50",
            "underlying": "NIFTY", "instrument_type": "INDEX", "strike": 0,
            "research_dataset": "NIFTY_50",
        }
        vix = dict(spot)
        vix["close"] = 14.0
        vix["research_dataset"] = "INDIA_VIX"
        write_parquet_dataset(root, date_text, "NIFTY_50", [spot])
        write_parquet_dataset(root, date_text, "INDIA_VIX", [vix])
        return date_text, options

    def test_pipeline_preserves_rows_columns_and_late_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            input_root = base / "input"
            output_root = base / "output"
            date_text, source_rows = self._fixture(input_root)
            report = cli.run_enrichment(
                input_root, output_root, [date_text], ASSUMPTIONS, chunk_size=1
            )
            self.assertEqual(report["total_input_rows"], 2)
            self.assertEqual(report["total_output_rows"], 2)
            self.assertEqual(report["total_input_files"], 3)
            self.assertEqual(report["successfully_calculated_rows"], 1)
            self.assertEqual(report["failed_calculation_rows"], 1)
            self.assertEqual(report["missing_spot_count"], 1)
            self.assertEqual(report["missing_vix_count"], 1)
            self.assertEqual(report["call_count"], 2)
            self.assertTrue(report["checks"]["original_columns_and_values_preserved"])
            self.assertTrue(report["model_configuration"]["assumptions_provisional"])
            output_file = output_root / date_text / "enriched.parquet"
            connection = duckdb.connect(database=":memory:")
            connection.execute("SET TimeZone='Asia/Kolkata'")
            actual = connection.execute(
                f"SELECT * FROM read_parquet('{output_file.as_posix()}') ORDER BY date"
            ).fetchdf()
            self.assertEqual(len(actual), len(source_rows))
            self.assertTrue(set(source_rows[0]).issubset(actual.columns))
            self.assertTrue(bool(actual["oi"].isna().iloc[0]))
            self.assertEqual(actual.loc[0, "spot_match_status"], "MATCHED")
            self.assertEqual(actual.loc[0, "vix_match_status"], "MATCHED")
            self.assertEqual(actual.loc[0, "india_vix"], 14.0)
            self.assertEqual(actual.loc[1, "spot_match_status"], "MISSING_SPOT")
            self.assertEqual(actual.loc[1, "vix_match_status"], "MISSING_VIX")
            self.assertEqual(actual.loc[1, "calculation_status"], "MISSING_SPOT")
            self.assertTrue(math.isnan(actual.loc[1, "implied_volatility"]))
            output_schema = [
                (row[0], row[1])
                for row in connection.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{output_file.as_posix()}')"
                ).fetchall()
            ]
            self.assertEqual(
                dict(output_schema)["implied_volatility"], "DOUBLE"
            )
            self.assertEqual(dict(output_schema)["expiry"], "DATE")
            self.assertTrue((output_root / "enrichment_report.json").is_file())
            connection.close()

    def test_dry_run_does_not_create_output_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            input_root, output_root = base / "input", base / "output"
            date_text, _ = self._fixture(input_root)
            report = cli.build_dry_run_report(input_root, output_root, [date_text])
            self.assertEqual(report["mode"], "dry_run_no_writes")
            self.assertEqual(report["dates"][0]["input_rows"], 2)
            self.assertFalse(output_root.exists())

    def test_reruns_are_logically_deterministic(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            input_root = base / "input"
            date_text, _ = self._fixture(input_root)
            outputs = [base / "out_a", base / "out_b"]
            for output_root in outputs:
                cli.run_enrichment(input_root, output_root, [date_text], ASSUMPTIONS)
            results = []
            for output_root in outputs:
                connection = duckdb.connect(database=":memory:")
                rows = connection.execute(
                    f"SELECT * FROM read_parquet("
                    f"'{(output_root / date_text / 'enriched.parquet').as_posix()}') "
                    "ORDER BY date, instrument_token"
                ).fetchall()
                results.append(rows)
                connection.close()
            self.assertEqual(results[0], results[1])

    def test_integer_and_float_source_parts_produce_one_schema(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            input_root, output_root = base / "input", base / "output"
            first_date, _ = self._fixture(input_root)
            second_date = "2026-01-05"
            second_time = dt.datetime(2026, 1, 5, 10, 0, tzinfo=INDIA_TZ)
            second_option = option_row()
            second_option["date"] = second_time
            second_option["close"] = round(second_option["close"])
            second_option["open"] = round(second_option["open"])
            second_option["high"] = round(second_option["high"])
            second_option["low"] = max(round(second_option["low"]), 1)
            write_parquet_dataset(
                input_root, second_date, "NIFTY_OPTIONS", [second_option],
                integer_ohlc=True,
            )
            index_row = {
                "date": second_time, "open": 100.0, "high": 100.0, "low": 100.0,
                "close": 100.0, "volume": 0, "oi": 0, "instrument_token": 1,
                "exchange": "NSE", "tradingsymbol": "NIFTY 50",
                "underlying": "NIFTY", "instrument_type": "INDEX", "strike": 0,
                "research_dataset": "NIFTY_50",
            }
            vix_row = dict(index_row)
            vix_row["close"] = 14.0
            vix_row["research_dataset"] = "INDIA_VIX"
            write_parquet_dataset(input_root, second_date, "NIFTY_50", [index_row])
            write_parquet_dataset(input_root, second_date, "INDIA_VIX", [vix_row])
            report = cli.run_enrichment(
                input_root, output_root, [first_date, second_date], ASSUMPTIONS
            )
            self.assertTrue(report["checks"]["output_schema_consistent"])
            connection = duckdb.connect(database=":memory:")
            for day in (first_date, second_date):
                output_file = output_root / day / "enriched.parquet"
                schema = dict(
                    (row[0], row[1])
                    for row in connection.execute(
                        f"DESCRIBE SELECT * FROM read_parquet('{output_file.as_posix()}')"
                    ).fetchall()
                )
                self.assertEqual(schema["open"], "DOUBLE")
                self.assertEqual(schema["close"], "DOUBLE")
            connection.close()

    def test_existing_output_requires_explicit_overwrite(self):
        with tempfile.TemporaryDirectory() as temporary:
            output_root = Path(temporary) / "output"
            date_text = "2026-01-02"
            target = output_root / date_text / "enriched.parquet"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"synthetic existing file")
            with self.assertRaises(FileExistsError):
                cli.preflight_output_paths(output_root, [date_text])
            targets = cli.preflight_output_paths(
                output_root, [date_text], overwrite=True
            )
            self.assertIn(target, targets)

    def test_run_rejects_assumptions_dated_after_selected_observation(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            input_root, output_root = base / "input", base / "output"
            date_text, _ = self._fixture(input_root)
            later_assumptions = ModelAssumptions(
                risk_free_rate=0.06, dividend_yield=0.01,
                expiry_time=dt.time(15, 40), year_fraction_basis="ACT/365",
                assumptions_as_of_date=dt.date(2026, 1, 3),
            )
            with self.assertRaises(cli.EnrichmentError):
                cli.run_enrichment(
                    input_root, output_root, [date_text], later_assumptions
                )
            self.assertFalse(output_root.exists())


if __name__ == "__main__":
    unittest.main()
