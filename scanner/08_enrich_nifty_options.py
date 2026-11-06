"""Enrich explicitly selected NIFTY option dates with historical BSM features.

Run from the scanner directory. Example:
    python 08_enrich_nifty_options.py --date 2026-11-03 --dry-run
    python 08_enrich_nifty_options.py --date 2026-11-03 \
      --risk-free-rate 0.06 --dividend-yield 0.01 \
      --expiry-time 15:40 --year-fraction-basis ACT/365 \
      --assumptions-as-of-date 2026-11-02

The real-data command requires explicit assumptions and writes only under the
separate market_data_enriched output root. It never opens the research database.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import math
import os
import re
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import duckdb
import pandas as pd

from nifty_options_enrichment import (
    CALCULATION_VERSION,
    INDIA_TZ,
    NIFTY_LOT_SIZE,
    PRICING_MODEL,
    SUCCESS_STATUSES,
    ModelAssumptions,
    enrich_option_row,
)


DEFAULT_INPUT_ROOT = Path("data/market_data_clean")
DEFAULT_OUTPUT_ROOT = Path("data/market_data_enriched/v1/NIFTY_OPTIONS")
OPTION_DATASET = "NIFTY_OPTIONS"
SPOT_DATASET = "NIFTY_50"
VIX_DATASET = "INDIA_VIX"
REQUIRED_OPTION_COLUMNS = {
    "date", "open", "high", "low", "close", "volume", "oi",
    "instrument_token", "tradingsymbol", "expiry", "strike", "option_type",
}
DERIVED_TYPES = {
    "spot_price": "DOUBLE",
    "india_vix": "DOUBLE",
    "implied_volatility": "DOUBLE",
    "delta": "DOUBLE",
    "gamma": "DOUBLE",
    "vega": "DOUBLE",
    "theta": "DOUBLE",
    "spot_timestamp": "TIMESTAMP WITH TIME ZONE",
    "vix_timestamp": "TIMESTAMP WITH TIME ZONE",
    "time_to_expiry_years": "DOUBLE",
    "assumptions_as_of_date": "DATE",
    "assumption_timing_status": "VARCHAR",
    "scenario_only": "BOOLEAN",
    "risk_free_rate": "DOUBLE",
    "dividend_yield": "DOUBLE",
    "pricing_model": "VARCHAR",
    "calculation_status": "VARCHAR",
    "calculation_reason": "VARCHAR",
    "calculation_version": "VARCHAR",
    "spot_match_status": "VARCHAR",
    "vix_match_status": "VARCHAR",
    "assumptions_provisional": "BOOLEAN",
}
NUMERIC_TYPES = {
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
    "FLOAT", "REAL", "DOUBLE",
}


class EnrichmentError(RuntimeError):
    """Raised when input validation or output safety checks fail."""


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parquet_relation(files: Sequence[Path]) -> str:
    if not files:
        raise EnrichmentError("Cannot read an empty Parquet file list")
    items = ",".join(sql_string(path.resolve().as_posix()) for path in files)
    return f"read_parquet([{items}], union_by_name=true)"


def parquet_files(root: Path, date_text: str, dataset: str) -> List[Path]:
    folder = root / date_text / dataset
    return sorted(path for path in folder.glob("*.parquet") if path.is_file())


def describe_relation(connection: duckdb.DuckDBPyConnection, relation: str) -> List[Tuple[str, str]]:
    return [(row[0], row[1]) for row in connection.execute(
        f"DESCRIBE SELECT * FROM {relation}"
    ).fetchall()]


def _column_map(schema: Sequence[Tuple[str, str]]) -> Dict[str, Tuple[str, str]]:
    return {name.casefold(): (name, kind.upper()) for name, kind in schema}


def _is_numeric_type(kind: str) -> bool:
    normalized = kind.upper()
    return normalized in NUMERIC_TYPES or normalized.startswith("DECIMAL(")


def _normalized_source_schema(schema: Sequence[Tuple[str, str]]) -> List[Tuple[str, str]]:
    normalized = []
    for name, kind in schema:
        if name.casefold() in {"open", "high", "low", "close"}:
            normalized.append((name, "DOUBLE"))
        else:
            normalized.append((name, kind.upper()))
    return normalized


def _expected_output_schema(source_schema: Sequence[Tuple[str, str]]) -> List[Tuple[str, str]]:
    names = {name.casefold() for name, _ in source_schema}
    collisions = names.intersection(DERIVED_TYPES)
    if collisions:
        raise EnrichmentError(
            "Source columns collide with enrichment fields: " + ", ".join(sorted(collisions))
        )
    return _normalized_source_schema(source_schema) + list(DERIVED_TYPES.items())


def _require_schema(
    connection: duckdb.DuckDBPyConnection,
    files: Sequence[Path],
    dataset: str,
    required: Iterable[str],
) -> List[Tuple[str, str]]:
    schema = describe_relation(connection, parquet_relation(files))
    columns = _column_map(schema)
    missing = sorted(set(required).difference(columns))
    if missing:
        raise EnrichmentError(
            f"{dataset} is missing required columns: {', '.join(missing)}"
        )
    return schema


def _timestamp_type(kind: str) -> bool:
    kind = kind.upper()
    return kind in ("TIMESTAMP", "TIMESTAMP_NS", "TIMESTAMP WITH TIME ZONE",
                    "TIMESTAMP_TZ")


def _validate_reference(
    connection: duckdb.DuckDBPyConnection,
    files: Sequence[Path],
    dataset: str,
    option_date_type: str,
) -> Tuple[List[Tuple[str, str]], int, int]:
    if not files:
        return [], 0, 0
    schema = _require_schema(connection, files, dataset, ("date", "close"))
    columns = _column_map(schema)
    date_name, date_type = columns["date"]
    close_name, close_type = columns["close"]
    if not _timestamp_type(date_type) or date_type != option_date_type:
        raise EnrichmentError(
            f"{dataset}.date type {date_type} does not match "
            f"NIFTY_OPTIONS.date type {option_date_type}"
        )
    if not _is_numeric_type(close_type):
        raise EnrichmentError(f"{dataset}.close must be numeric, found {close_type}")
    relation = parquet_relation(files)
    row_count = connection.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]
    duplicate_count = connection.execute(
        f"SELECT count(*) FROM ("
        f"SELECT {quote_identifier(date_name)} FROM {relation} "
        f"GROUP BY {quote_identifier(date_name)} HAVING count(*) > 1)"
    ).fetchone()[0]
    if duplicate_count:
        raise EnrichmentError(
            f"{dataset} has {duplicate_count} duplicate timestamps; "
            "refusing a join that could multiply option rows"
        )
    return schema, row_count, duplicate_count


def _validate_option_schema(
    schema: Sequence[Tuple[str, str]],
) -> Tuple[str, str]:
    columns = _column_map(schema)
    missing = sorted(REQUIRED_OPTION_COLUMNS.difference(columns))
    if missing:
        raise EnrichmentError("NIFTY_OPTIONS is missing: " + ", ".join(missing))
    date_name, date_type = columns["date"]
    close_name, close_type = columns["close"]
    if not _timestamp_type(date_type):
        raise EnrichmentError(f"NIFTY_OPTIONS.date must be a timestamp, found {date_type}")
    if not _is_numeric_type(close_type):
        raise EnrichmentError(f"NIFTY_OPTIONS.close must be numeric, found {close_type}")
    return date_name, date_type


def _date_plan(
    connection: duckdb.DuckDBPyConnection,
    input_root: Path,
    output_root: Path,
    date_text: str,
) -> Dict[str, Any]:
    option_files = parquet_files(input_root, date_text, OPTION_DATASET)
    if not option_files:
        raise EnrichmentError(
            f"No NIFTY_OPTIONS Parquet parts found for {date_text} under {input_root}"
        )
    option_schema = _require_schema(
        connection, option_files, OPTION_DATASET, REQUIRED_OPTION_COLUMNS
    )
    option_date_name, option_date_type = _validate_option_schema(option_schema)
    spot_files = parquet_files(input_root, date_text, SPOT_DATASET)
    vix_files = parquet_files(input_root, date_text, VIX_DATASET)
    spot_schema, spot_rows, _ = _validate_reference(
        connection, spot_files, SPOT_DATASET, option_date_type
    )
    vix_schema, vix_rows, _ = _validate_reference(
        connection, vix_files, VIX_DATASET, option_date_type
    )
    input_rows = connection.execute(
        f"SELECT count(*) FROM {parquet_relation(option_files)}"
    ).fetchone()[0]
    output_dir = output_root / date_text
    return {
        "date": date_text,
        "option_files": option_files,
        "spot_files": spot_files,
        "vix_files": vix_files,
        "option_schema": option_schema,
        "spot_schema": spot_schema,
        "vix_schema": vix_schema,
        "option_date_name": option_date_name,
        "option_date_type": option_date_type,
        "input_rows": input_rows,
        "spot_rows": spot_rows,
        "vix_rows": vix_rows,
        "output_path": output_dir / "enriched.parquet",
        "report_path": output_dir / "enrichment_report.json",
    }


def _select_reference(
    files: Sequence[Path],
    schema: Sequence[Tuple[str, str]],
    option_date_type: str,
    alias: str,
) -> str:
    if files:
        columns = _column_map(schema)
        date_name, _ = columns["date"]
        close_name, _ = columns["close"]
        relation = parquet_relation(files)
        return (
            f"(SELECT {quote_identifier(date_name)} AS date, "
            f"CAST({quote_identifier(close_name)} AS DOUBLE) AS close "
            f"FROM {relation}) AS {alias}"
        )
    return (
        f"(SELECT CAST(NULL AS {option_date_type}) AS date, "
        f"CAST(NULL AS DOUBLE) AS close WHERE FALSE) AS {alias}"
    )


def _joined_query(plan: Dict[str, Any]) -> str:
    source_schema = plan["option_schema"]
    source_columns = []
    for name, _ in source_schema:
        identifier = f"o.{quote_identifier(name)}"
        if name.casefold() in {"open", "high", "low", "close"}:
            source_columns.append(f"CAST({identifier} AS DOUBLE) AS {quote_identifier(name)}")
        else:
            source_columns.append(f"{identifier} AS {quote_identifier(name)}")
    source_columns.extend((
        f"s.date AS {quote_identifier('__spot_timestamp')}",
        f"s.close AS {quote_identifier('__spot_close')}",
        f"v.date AS {quote_identifier('__vix_timestamp')}",
        f"v.close AS {quote_identifier('__vix_close')}",
    ))
    option_relation = parquet_relation(plan["option_files"])
    spot_relation = _select_reference(
        plan["spot_files"], plan["spot_schema"], plan["option_date_type"], "s"
    )
    vix_relation = _select_reference(
        plan["vix_files"], plan["vix_schema"], plan["option_date_type"], "v"
    )
    columns = _column_map(plan["option_schema"])
    order_parts = []
    for key in ("date", "instrument_token", "tradingsymbol"):
        if key in columns:
            order_parts.append(f"o.{quote_identifier(columns[key][0])}")
    order_sql = " ORDER BY " + ", ".join(order_parts) if order_parts else ""
    return (
        "SELECT " + ", ".join(source_columns)
        + f" FROM {option_relation} AS o"
        + f" LEFT JOIN {spot_relation} ON "
        + f"o.{quote_identifier(plan['option_date_name'])} = s.date"
        + f" LEFT JOIN {vix_relation} ON "
        + f"o.{quote_identifier(plan['option_date_name'])} = v.date"
        + order_sql
    )


def _check_roots(input_root: Path, output_root: Path) -> None:
    source = input_root.resolve()
    output = output_root.resolve()
    common = Path(os.path.commonpath((str(source), str(output))))
    if common == source or common == output:
        raise EnrichmentError(
            "Input and output roots must be separate, non-nested directories"
        )


def preflight_output_paths(
    output_root: Path, dates: Sequence[str], overwrite: bool = False,
) -> List[Path]:
    targets = []
    for day in dates:
        date_dir = output_root / day
        targets.extend((date_dir / "enriched.parquet", date_dir / "enrichment_report.json"))
    collisions = [path for path in targets if path.exists()]
    non_files = [path for path in collisions if not path.is_file()]
    if non_files:
        raise EnrichmentError(
            "An output target is not a file: " + ", ".join(str(path) for path in non_files)
        )
    if collisions and not overwrite:
        raise FileExistsError(
            "Enriched output already exists; pass --overwrite to replace: "
            + ", ".join(str(path) for path in collisions)
        )
    return targets


def _output_schema_select(
    source_schema: Sequence[Tuple[str, str]],
) -> List[Tuple[str, str]]:
    return _expected_output_schema(source_schema)


def _cast_expression(name: str, kind: str) -> str:
    return f"CAST({quote_identifier(name)} AS {kind}) AS {quote_identifier(name)}"


def _source_projection(schema: Sequence[Tuple[str, str]]) -> str:
    expressions = []
    for name, kind in _normalized_source_schema(schema):
        expressions.append(_cast_expression(name, kind))
    return ", ".join(expressions)


def _write_chunk(
    connection: duckdb.DuckDBPyConnection,
    records: List[Dict[str, Any]],
    output_schema: Sequence[Tuple[str, str]],
    path: Path,
    sequence: int,
) -> None:
    # Keep Python None distinct from floating NaN. DataFrame inference would
    # otherwise turn nullable numeric source fields into NaN before the SQL
    # cast, changing the original nulls in the derived Parquet.
    frame = pd.DataFrame({
        name: pd.Series([record.get(name) for record in records], dtype=object)
        for name, _ in output_schema
    })
    relation_name = f"enrichment_batch_{sequence}"
    connection.register(relation_name, frame)
    try:
        projection = ", ".join(_cast_expression(name, kind) for name, kind in output_schema)
        connection.execute(
            f"COPY (SELECT {projection} FROM {quote_identifier(relation_name)}) "
            f"TO {sql_string(path.resolve().as_posix())} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        connection.unregister(relation_name)


def _count_original_differences(
    connection: duckdb.DuckDBPyConnection,
    source_files: Sequence[Path],
    output_file: Path,
    source_schema: Sequence[Tuple[str, str]],
) -> Tuple[int, int]:
    names = [name for name, _ in source_schema]
    source_columns = _source_projection(source_schema)
    source = f"SELECT {source_columns} FROM {parquet_relation(source_files)}"
    projected = ", ".join(quote_identifier(name) for name in names)
    output = (
        f"SELECT {projected} FROM "
        f"{parquet_relation((output_file,))}"
    )
    left_only = connection.execute(
        f"SELECT count(*) FROM (({source}) EXCEPT ALL ({output})) AS differences"
    ).fetchone()[0]
    right_only = connection.execute(
        f"SELECT count(*) FROM (({output}) EXCEPT ALL ({source})) AS differences"
    ).fetchone()[0]
    return left_only, right_only


def _schema_as_records(schema: Sequence[Tuple[str, str]]) -> List[Dict[str, str]]:
    return [{"column": name, "type": kind} for name, kind in schema]


def _new_counts() -> Dict[str, Any]:
    return {
        "input_rows": 0,
        "output_rows": 0,
        "successfully_calculated_rows": 0,
        "failed_calculation_rows": 0,
        "historically_timed_rows": 0,
        "scenario_only_rows": 0,
        "missing_spot_count": 0,
        "missing_vix_count": 0,
        "invalid_option_price_count": 0,
        "solver_failure_count": 0,
        "call_count": 0,
        "put_count": 0,
        "calculation_status_counts": {},
        "assumption_timing_status_counts": {},
        "option_type_breakdown": {},
    }


def _record_breakdown(
    counts: Dict[str, Any], result: Dict[str, Any], option_type: Any,
    option_price: Any, spot_timestamp: Any, vix_timestamp: Any,
) -> None:
    status = result["calculation_status"]
    counts["input_rows"] += 1
    counts["output_rows"] += 1
    counts["calculation_status_counts"][status] = (
        counts["calculation_status_counts"].get(status, 0) + 1
    )
    timing_status = result["assumption_timing_status"]
    counts["assumption_timing_status_counts"][timing_status] = (
        counts["assumption_timing_status_counts"].get(timing_status, 0) + 1
    )
    if timing_status == "AS_OF_BEFORE_OBSERVATION":
        counts["historically_timed_rows"] += 1
    if result["scenario_only"]:
        counts["scenario_only_rows"] += 1
    if status in SUCCESS_STATUSES:
        counts["successfully_calculated_rows"] += 1
    else:
        counts["failed_calculation_rows"] += 1
    if spot_timestamp is None:
        counts["missing_spot_count"] += 1
    if vix_timestamp is None:
        counts["missing_vix_count"] += 1
    try:
        numeric_close = float(option_price)
        if not math.isfinite(numeric_close) or numeric_close <= 0:
            counts["invalid_option_price_count"] += 1
    except (TypeError, ValueError, OverflowError):
        counts["invalid_option_price_count"] += 1
    if status == "IV_SOLVER_FAILED":
        counts["solver_failure_count"] += 1
    kind = option_type.strip().upper() if isinstance(option_type, str) else "INVALID"
    if kind not in ("CE", "PE"):
        kind = "INVALID"
    if kind == "CE":
        counts["call_count"] += 1
    elif kind == "PE":
        counts["put_count"] += 1
    breakdown = counts["option_type_breakdown"].setdefault(kind, {
        "rows": 0, "successfully_calculated_rows": 0, "failed_calculation_rows": 0,
    })
    breakdown["rows"] += 1
    if status in SUCCESS_STATUSES:
        breakdown["successfully_calculated_rows"] += 1
    else:
        breakdown["failed_calculation_rows"] += 1


def build_dry_run_report(
    input_root: Path, output_root: Path, dates: Sequence[str],
) -> Dict[str, Any]:
    """Inspect inputs and report paths/counts without creating output files."""
    _check_roots(input_root, output_root)
    connection = duckdb.connect(database=":memory:")
    connection.execute("SET TimeZone='Asia/Kolkata'")
    try:
        plans = [
            _date_plan(connection, input_root, output_root, day) for day in dates
        ]
        _validate_common_schema(plans)
        return {
            "mode": "dry_run_no_writes",
            "selected_dates": list(dates),
            "input_root": str(input_root.resolve()),
            "output_root": str(output_root.resolve()),
            "dates": [
                {
                    "date": plan["date"],
                    "input_files": [str(path.resolve()) for path in plan["option_files"]],
                    "option_input_file_count": len(plan["option_files"]),
                    "input_rows": plan["input_rows"],
                    "spot_files": [str(path.resolve()) for path in plan["spot_files"]],
                    "spot_rows": plan["spot_rows"],
                    "vix_files": [str(path.resolve()) for path in plan["vix_files"]],
                    "vix_rows": plan["vix_rows"],
                    "planned_output_path": str(plan["output_path"].resolve()),
                }
                for plan in plans
            ],
            "planned_report_paths": {
                plan["date"]: str(plan["report_path"].resolve()) for plan in plans
            },
            "output_schema": _schema_as_records(_output_schema_select(plans[0]["option_schema"])),
        }
    finally:
        connection.close()


def _validate_common_schema(plans: Sequence[Dict[str, Any]]) -> List[Tuple[str, str]]:
    if not plans:
        raise EnrichmentError("No dates were selected")
    expected = _output_schema_select(plans[0]["option_schema"])
    for plan in plans[1:]:
        actual = _output_schema_select(plan["option_schema"])
        if actual != expected:
            raise EnrichmentError(
                "NIFTY_OPTIONS schemas differ across selected dates; "
                f"{plan['date']} does not match the first selected date"
            )
    return expected


def run_enrichment(
    input_root: Path,
    output_root: Path,
    dates: Sequence[str],
    assumptions: ModelAssumptions,
    overwrite: bool = False,
    chunk_size: int = 50000,
) -> Dict[str, Any]:
    """Read selected date partitions, write separate enriched Parquet, and audit."""
    assumptions.validate()
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    _check_roots(input_root, output_root)
    if not dates:
        raise EnrichmentError("Select at least one explicit trading date")
    if (
        assumptions.assumption_mode == "historical"
        and assumptions.assumptions_as_of_date > min(
            dt.date.fromisoformat(day) for day in dates
        )
    ):
        raise EnrichmentError(
            "historical mode requires assumptions_as_of_date on or before the "
            "earliest selected date; scenario mode permits later-known inputs"
        )

    connection = duckdb.connect(database=":memory:")
    connection.execute("SET TimeZone='Asia/Kolkata'")
    staged_outputs: Dict[str, Path] = {}
    staged_reports: Dict[str, Path] = {}
    reserved_outputs: List[Path] = []
    try:
        plans = [
            _date_plan(connection, input_root, output_root, day) for day in dates
        ]
        output_schema = _validate_common_schema(plans)
        preflight_output_paths(output_root, dates, overwrite=overwrite)
        output_root.mkdir(parents=True, exist_ok=True)

        date_reports = {}
        source_schemas = {}
        output_schemas = {}
        for plan in plans:
            plan["output_path"].parent.mkdir(parents=True, exist_ok=True)
            stage_path = plan["output_path"].with_name(
                ".enriched.parquet." + uuid.uuid4().hex + ".tmp"
            )
            staged_outputs[plan["date"]] = stage_path
            day_counts = _new_counts()
            chunk_paths = []
            source_schemas[plan["date"]] = _schema_as_records(plan["option_schema"])

            with tempfile.TemporaryDirectory(
                prefix=".nifty-enrichment-", dir=str(plan["output_path"].parent)
            ) as temporary_dir:
                temp_dir = Path(temporary_dir)
                query = _joined_query(plan)
                cursor = connection.execute(query)
                # Keep a separate DuckDB connection for batch serialization:
                # executing COPY on the query connection would replace its
                # active result cursor before all source rows were fetched.
                batch_connection = duckdb.connect(database=":memory:")
                batch_connection.execute("SET TimeZone='Asia/Kolkata'")
                source_names = [name for name, _ in plan["option_schema"]]
                joined_names = [item[0] for item in cursor.description]
                if joined_names[:len(source_names)] != source_names:
                    batch_connection.close()
                    raise EnrichmentError("Joined input columns do not match source schema")
                records = []
                chunk_number = 0
                try:
                    while True:
                        rows = cursor.fetchmany(chunk_size)
                        if not rows:
                            break
                        for row in rows:
                            source_values = row[:len(source_names)]
                            helper = row[len(source_names):]
                            option = {
                                name.casefold(): value
                                for name, value in zip(source_names, source_values)
                            }
                            result = enrich_option_row(
                                option, helper[0], helper[1], helper[2], helper[3], assumptions
                            ).as_dict()
                            output_record = dict(zip(source_names, source_values))
                            output_record.update(result)
                            records.append(output_record)
                            _record_breakdown(
                                day_counts, result, option.get("option_type"),
                                option.get("close"), helper[0], helper[2]
                            )
                        chunk_path = temp_dir / f"chunk_{chunk_number:06d}.parquet"
                        _write_chunk(
                            batch_connection, records, output_schema,
                            chunk_path, chunk_number
                        )
                        chunk_paths.append(chunk_path)
                        records = []
                        chunk_number += 1
                finally:
                    batch_connection.close()

                if not chunk_paths:
                    raise EnrichmentError(
                        f"{plan['date']} contains no NIFTY_OPTIONS rows to enrich"
                    )
                chunks = ",".join(
                    sql_string(path.resolve().as_posix()) for path in chunk_paths
                )
                connection.execute(
                    f"COPY (SELECT * FROM read_parquet([{chunks}], union_by_name=true)) "
                    f"TO {sql_string(stage_path.resolve().as_posix())} "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)"
                )

            output_rows = connection.execute(
                f"SELECT count(*) FROM {parquet_relation((stage_path,))}"
            ).fetchone()[0]
            if output_rows != plan["input_rows"]:
                raise EnrichmentError(
                    f"{plan['date']} input/output row counts differ "
                    f"({plan['input_rows']} vs {output_rows})"
                )
            left_only, right_only = _count_original_differences(
                connection, plan["option_files"], stage_path, plan["option_schema"]
            )
            if left_only or right_only:
                raise EnrichmentError(
                    f"{plan['date']} original columns differ from source: "
                    f"input-only={left_only}, output-only={right_only}"
                )
            measured_schema = describe_relation(
                connection, parquet_relation((stage_path,))
            )
            if measured_schema != output_schema:
                raise EnrichmentError(
                    f"{plan['date']} output schema differs from expected consistent schema"
                )
            output_schemas[plan["date"]] = _schema_as_records(measured_schema)
            day_counts["input_rows"] = plan["input_rows"]
            day_counts["output_rows"] = output_rows
            date_reports[plan["date"]] = {
                "input_files": [str(path.resolve()) for path in plan["option_files"]],
                "option_input_file_count": len(plan["option_files"]),
                "input_rows": plan["input_rows"],
                "spot_files": [str(path.resolve()) for path in plan["spot_files"]],
                "spot_input_rows": plan["spot_rows"],
                "vix_files": [str(path.resolve()) for path in plan["vix_files"]],
                "vix_input_rows": plan["vix_rows"],
                "output_path": str(plan["output_path"].resolve()),
                "output_rows": output_rows,
                "input_only_original_rows": left_only,
                "output_only_original_rows": right_only,
                "counts": day_counts,
            }

        all_counts = _new_counts()
        for daily in date_reports.values():
            counts = daily["counts"]
            for key in (
                "input_rows", "output_rows", "successfully_calculated_rows",
                "failed_calculation_rows", "historically_timed_rows", "scenario_only_rows",
                "missing_spot_count", "missing_vix_count",
                "invalid_option_price_count", "solver_failure_count",
                "call_count", "put_count",
            ):
                all_counts[key] += counts[key]
            for key, value in counts["calculation_status_counts"].items():
                all_counts["calculation_status_counts"][key] = (
                    all_counts["calculation_status_counts"].get(key, 0) + value
                )
            for key, value in counts["assumption_timing_status_counts"].items():
                all_counts["assumption_timing_status_counts"][key] = (
                    all_counts["assumption_timing_status_counts"].get(key, 0) + value
                )
            for key, value in counts["option_type_breakdown"].items():
                target = all_counts["option_type_breakdown"].setdefault(key, {
                    "rows": 0, "successfully_calculated_rows": 0,
                    "failed_calculation_rows": 0,
                })
                for field in target:
                    target[field] += value[field]

        report = {
            "report_version": 1,
            "run_timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "calculation_version": CALCULATION_VERSION,
            "mode": "real_data_enrichment",
            "input_root": str(input_root.resolve()),
            "output_root": str(output_root.resolve()),
            "selected_dates": list(dates),
            "total_input_files": sum(
                item["option_input_file_count"]
                + len(item["spot_files"])
                + len(item["vix_files"])
                for item in date_reports.values()
            ),
            "total_option_input_files": sum(
                item["option_input_file_count"] for item in date_reports.values()
            ),
            "total_input_rows": all_counts["input_rows"],
            "total_output_rows": all_counts["output_rows"],
            "successfully_calculated_rows": all_counts["successfully_calculated_rows"],
            "failed_calculation_rows": all_counts["failed_calculation_rows"],
            "historically_timed_rows": all_counts["historically_timed_rows"],
            "scenario_only_rows": all_counts["scenario_only_rows"],
            "missing_spot_count": all_counts["missing_spot_count"],
            "missing_vix_count": all_counts["missing_vix_count"],
            "invalid_option_price_count": all_counts["invalid_option_price_count"],
            "solver_failure_count": all_counts["solver_failure_count"],
            "call_count": all_counts["call_count"],
            "put_count": all_counts["put_count"],
            "calculation_status_counts": all_counts["calculation_status_counts"],
            "assumption_timing_status_counts": all_counts["assumption_timing_status_counts"],
            "option_type_breakdown": all_counts["option_type_breakdown"],
            "model_configuration": {
                "pricing_model": PRICING_MODEL,
                "option_reference_price": "close",
                "underlying_dataset": SPOT_DATASET,
                "india_vix_dataset": VIX_DATASET,
                "timestamp_join": "exact equality only; no forward fill",
                "timezone": "Asia/Kolkata (fixed UTC+05:30)",
                "expiry_time": assumptions.expiry_time.isoformat(),
                "year_fraction_basis": assumptions.year_fraction_basis,
                "assumptions_as_of_date": assumptions.assumptions_as_of_date.isoformat(),
                "assumption_mode": assumptions.assumption_mode,
                "risk_free_rate": assumptions.risk_free_rate,
                "dividend_yield": assumptions.dividend_yield,
                "assumptions_provisional": assumptions.assumptions_provisional,
                "rate_and_yield_note": (
                    "User-supplied per run; provisional because this pipeline "
                    "does not load historical rate/dividend curves."
                ),
                "sigma_min": assumptions.sigma_min,
                "sigma_max": assumptions.sigma_max,
                "solver_price_tolerance": assumptions.solver_tolerance,
                "solver_max_iterations": assumptions.solver_max_iterations,
                "greek_lot_size": NIFTY_LOT_SIZE,
                "greek_units": {
                    "implied_volatility": "decimal annualized volatility",
                    "delta": "option price change per one spot point",
                    "gamma": "delta change per one spot point",
                    "vega": "option price change per one volatility percentage point for a 65-unit lot",
                    "theta": "option price change per calendar day for a 65-unit lot (annual theta / 365)",
                },
            },
            "date_breakdown": date_reports,
            "output_schema": _schema_as_records(output_schema),
            "checks": {
                "input_output_row_counts_match": all(
                    item["input_rows"] == item["output_rows"]
                    for item in date_reports.values()
                ),
                "original_columns_and_values_preserved": all(
                    item["input_only_original_rows"] == 0
                    and item["output_only_original_rows"] == 0
                    for item in date_reports.values()
                ),
                "output_schema_consistent": len(set(
                    json.dumps(value, sort_keys=True)
                    for value in output_schemas.values()
                )) <= 1,
                "outputs_by_date": {
                    day: str(plan["output_path"].resolve()) for day, plan in
                    ((item["date"], item) for item in plans)
                },
                "report_paths": {
                    day: str(plan["report_path"].resolve()) for day, plan in
                    ((item["date"], item) for item in plans)
                },
            },
        }
        if not all(report["checks"][key] for key in (
            "input_output_row_counts_match",
            "original_columns_and_values_preserved",
            "output_schema_consistent",
        )):
            raise EnrichmentError("One or more output audit checks failed")

        for plan in plans:
            day = plan["date"]
            staged_report = plan["report_path"].with_name(
                ".enrichment_report." + uuid.uuid4().hex + ".tmp"
            )
            staged_reports[day] = staged_report
            date_report = dict(report)
            date_report["selected_dates"] = [day]
            date_report["total_input_files"] = (
                date_reports[day]["option_input_file_count"]
                + len(date_reports[day]["spot_files"])
                + len(date_reports[day]["vix_files"])
            )
            date_report["total_option_input_files"] = date_reports[day]["option_input_file_count"]
            date_report["total_input_rows"] = date_reports[day]["counts"]["input_rows"]
            date_report["total_output_rows"] = date_reports[day]["counts"]["output_rows"]
            for key, value in date_reports[day]["counts"].items():
                if key in date_report:
                    date_report[key] = value
            date_report["date_breakdown"] = {day: date_reports[day]}
            date_report["checks"] = dict(report["checks"])
            date_report["checks"]["outputs_by_date"] = {day: str(plan["output_path"].resolve())}
            date_report["checks"]["report_path"] = str(plan["report_path"].resolve())
            with staged_report.open("w", encoding="utf-8", newline="\n") as stream:
                json.dump(date_report, stream, indent=2, sort_keys=True, allow_nan=False)
                stream.write("\n")

        publication_targets = preflight_output_paths(
            output_root, dates, overwrite=overwrite
        )
        if not overwrite:
            try:
                for target in publication_targets:
                    descriptor = os.open(
                        str(target), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
                    )
                    os.close(descriptor)
                    reserved_outputs.append(target)
            except OSError as exc:
                raise FileExistsError(
                    "Could not reserve every output path without replacement; "
                    "another file may have appeared during the run"
                ) from exc
        for plan in plans:
            os.replace(staged_outputs[plan["date"]], plan["output_path"])
            if plan["output_path"] in reserved_outputs:
                reserved_outputs.remove(plan["output_path"])
            os.replace(staged_reports[plan["date"]], plan["report_path"])
            if plan["report_path"] in reserved_outputs:
                reserved_outputs.remove(plan["report_path"])
        staged_reports.clear()
        return report
    finally:
        for path in staged_outputs.values():
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        for staged_report in staged_reports.values():
            try:
                staged_report.unlink()
            except FileNotFoundError:
                pass
        for path in reserved_outputs:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        connection.close()


def _parse_iso_date(value: str) -> str:
    try:
        parsed = dt.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD")
    return value


def _parse_expiry_time(value: str) -> dt.time:
    try:
        parsed = dt.time.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expiry time must be HH:MM or HH:MM:SS") from exc
    if parsed.tzinfo is not None:
        raise argparse.ArgumentTypeError("expiry time must be an Asia/Kolkata wall time")
    return parsed


def _selected_dates(args: argparse.Namespace, input_root: Path) -> List[str]:
    if args.date:
        found = parquet_files(input_root, args.date, OPTION_DATASET)
        if not found:
            raise EnrichmentError(
                f"No NIFTY_OPTIONS input exists for explicitly requested {args.date}"
            )
        return [args.date]
    if not args.start_date or not args.end_date:
        raise EnrichmentError("Provide --date or both --start-date and --end-date")
    start = dt.date.fromisoformat(args.start_date)
    end = dt.date.fromisoformat(args.end_date)
    if end < start:
        raise EnrichmentError("--end-date must not be earlier than --start-date")
    selected = []
    for folder in input_root.iterdir() if input_root.exists() else ():
        if not folder.is_dir() or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", folder.name):
            continue
        candidate = dt.date.fromisoformat(folder.name)
        if start <= candidate <= end and parquet_files(input_root, folder.name, OPTION_DATASET):
            selected.append(folder.name)
    selected.sort()
    if not selected:
        raise EnrichmentError(
            f"No NIFTY_OPTIONS data found between {args.start_date} and {args.end_date}"
        )
    return selected


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Enrich explicitly selected NIFTY options dates with historical "
            "Black-Scholes-Merton IV and Greeks."
        ),
        epilog=(
            "Run from scanner. A real-data run requires explicit risk-free rate, "
            "dividend yield, expiry time, year-fraction basis and assumptions "
            "as-of date. Example: "
            "python 08_enrich_nifty_options.py --date 2026-11-03 --dry-run"
        ),
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--date", type=_parse_iso_date, help="one explicit trading date")
    selection.add_argument("--start-date", type=_parse_iso_date, help="inclusive range start")
    parser.add_argument("--end-date", type=_parse_iso_date, help="inclusive range end")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--dry-run", action="store_true",
                        help="inspect files, counts and paths without writing output")
    parser.add_argument("--overwrite", action="store_true",
                        help="explicitly allow replacement of existing enriched output")
    parser.add_argument("--risk-free-rate", type=float,
                        help="annual continuously compounded rate as decimal; required for real runs")
    parser.add_argument("--dividend-yield", type=float,
                        help="annual continuous yield as decimal; required for real runs")
    parser.add_argument("--expiry-time", type=_parse_expiry_time,
                        help="contract expiry wall time in Asia/Kolkata; required for real runs")
    parser.add_argument("--assumptions-as-of-date", type=_parse_iso_date,
                        help="date rate/yield inputs were known; must be <= selected dates")
    parser.add_argument(
        "--assumption-mode", choices=("historical", "scenario"), default="historical",
        help=(
            "historical blocks later-known inputs; scenario permits them and "
            "marks affected rows scenario_only"
        ),
    )
    parser.add_argument("--year-fraction-basis", choices=("ACT/365", "ACT/365.25"),
                        help="year-fraction basis; required for real runs")
    parser.add_argument("--sigma-min", type=float, default=1e-6,
                        help="lower IV solver bound (default: 0.000001)")
    parser.add_argument("--sigma-max", type=float, default=5.0,
                        help="upper IV solver bound (default: 5.0)")
    parser.add_argument("--solver-tolerance", type=float, default=1e-8,
                        help="absolute option-price solver tolerance (default: 1e-8)")
    parser.add_argument("--solver-max-iterations", type=int, default=100,
                        help="maximum bisection steps (default: 100)")
    parser.add_argument("--chunk-size", type=int, default=50000,
                        help="number of rows held in memory per batch (default: 50000)")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.date is None and args.start_date is None:
        parser.error("provide --date or --start-date and --end-date")
    if args.start_date is not None and args.end_date is None:
        parser.error("--start-date requires --end-date")
    if args.date is not None and args.end_date is not None:
        parser.error("--end-date is only valid with --start-date")
    try:
        dates = _selected_dates(args, args.input_root)
        if args.dry_run:
            report = build_dry_run_report(args.input_root, args.output_root, dates)
            report["existing_output_targets"] = [
                str(path.resolve())
                for path in preflight_output_paths(
                    args.output_root, dates, overwrite=True
                )
                if path.exists()
            ]
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0
        missing = [
            name for name in (
                "risk-free-rate", "dividend-yield", "expiry-time",
                "year-fraction-basis", "assumptions-as-of-date"
            ) if getattr(args, name.replace("-", "_")) is None
        ]
        if missing:
            parser.error(
                "real-data runs require explicit --" + ", --".join(missing)
            )
        assumptions = ModelAssumptions(
            risk_free_rate=args.risk_free_rate,
            dividend_yield=args.dividend_yield,
            expiry_time=args.expiry_time,
            year_fraction_basis=args.year_fraction_basis,
            assumptions_as_of_date=dt.date.fromisoformat(args.assumptions_as_of_date),
            assumption_mode=args.assumption_mode,
            assumptions_provisional=True,
            sigma_min=args.sigma_min,
            sigma_max=args.sigma_max,
            solver_tolerance=args.solver_tolerance,
            solver_max_iterations=args.solver_max_iterations,
        )
        report = run_enrichment(
            args.input_root, args.output_root, dates, assumptions,
            overwrite=args.overwrite, chunk_size=args.chunk_size
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    except (EnrichmentError, FileExistsError, ValueError, OSError, duckdb.Error) as exc:
        parser.exit(2, f"ERROR: {exc}\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

