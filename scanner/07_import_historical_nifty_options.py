"""
Import historical NIFTY options data into the existing NIFTY_OPTIONS DuckDB table.

Supports:
  - ZIP archives containing daily CSV files
  - CSV files
  - Excel files (.xlsx, .xls)

Example:
  python 07_import_historical_nifty_options.py --file "D:\\data\\OCT_2021.zip"
  python 07_import_historical_nifty_options.py --file "D:\\data\\NIFTY_OPTIONS.xlsx"

Default database:
  scanner/data/nifty_research.duckdb

The source data is expected to have columns similar to:
  Ticker, Date, Time, Open, High, Low, Close, Volume, Open Interest

This script maps Ticker -> tradingsymbol and combines Date + Time into the
existing table's `date` timestamp key. It inserts only new composite keys.
"""

import argparse
import logging
import sys
import zipfile
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd


PROJECT_ROOT = Path(
    r"C:\Users\91965\VISHAL GUPTA\T\repo\daily-stock-scanner\scanner"
)
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "nifty_research.duckdb"
DEFAULT_TABLE = "NIFTY_OPTIONS"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def normalise_column_name(value: str) -> str:
    return (
        str(value).strip().lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
    )


def read_csv_bytes(content: bytes, source_name: str) -> pd.DataFrame:
    """Read vendor CSV bytes, trying common encodings."""
    from io import BytesIO

    for encoding in ("utf-8-sig", "cp1252", "latin1"):
        try:
            return pd.read_csv(BytesIO(content), encoding=encoding, low_memory=False)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode CSV file: {source_name}")


def load_input_file(file_path: Path) -> list[tuple[str, pd.DataFrame]]:
    """Read CSV, Excel, or every CSV in a ZIP archive."""
    if not file_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {file_path}")

    suffix = file_path.suffix.lower()

    if suffix == ".zip":
        results = []
        with zipfile.ZipFile(file_path) as archive:
            csv_names = [
                name for name in archive.namelist()
                if not name.endswith("/") and name.lower().endswith(".csv")
            ]
            if not csv_names:
                raise ValueError(f"No CSV files found inside ZIP archive: {file_path}")
            for name in sorted(csv_names):
                with archive.open(name) as handle:
                    content = handle.read()
                results.append((f"{file_path.name}:{name}", read_csv_bytes(content, name)))
        return results

    if suffix == ".csv":
        return [(str(file_path), pd.read_csv(file_path, low_memory=False))]

    if suffix in (".xlsx", ".xls"):
        # For workbooks with multiple sheets, import every sheet.
        workbook = pd.ExcelFile(file_path)
        return [
            (f"{file_path.name}:{sheet_name}", pd.read_excel(file_path, sheet_name=sheet_name))
            for sheet_name in workbook.sheet_names
        ]

    raise ValueError(
        f"Unsupported input extension '{suffix}'. Use .zip, .csv, .xlsx, or .xls."
    )


def prepare_frame(frame: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """Map the vendor's fields to the existing NIFTY_OPTIONS schema."""
    if frame.empty:
        logger.warning("Empty input skipped: %s", source_name)
        return pd.DataFrame()

    # Normalise source headings for matching.
    original_by_normalised = {
        normalise_column_name(column): column for column in frame.columns
    }

    aliases = {
        "tradingsymbol": ("ticker", "tradingsymbol", "trading_symbol", "symbol"),
        "date_only": ("date", "trading_date"),
        "time_only": ("time", "trading_time"),
        "timestamp": ("timestamp", "datetime", "date_time", "exchange_timestamp"),
        "open": ("open", "o"),
        "high": ("high", "h"),
        "low": ("low", "l"),
        "close": ("close", "ltp", "c"),
        "volume": ("volume", "vol"),
        "open_interest": ("open_interest", "openinterest", "oi"),
    }

    found = {}
    for target, candidates in aliases.items():
        for candidate in candidates:
            if candidate in original_by_normalised:
                found[target] = original_by_normalised[candidate]
                break

    if "tradingsymbol" not in found:
        raise ValueError(
            f"{source_name}: missing Ticker/TradingSymbol column. "
            f"Available columns: {list(frame.columns)}"
        )

    if "timestamp" in found:
        timestamp = pd.to_datetime(frame[found["timestamp"]], errors="coerce", dayfirst=True)
    elif "date_only" in found and "time_only" in found:
        combined = (
            frame[found["date_only"]].astype(str).str.strip()
            + " "
            + frame[found["time_only"]].astype(str).str.strip()
        )
        timestamp = pd.to_datetime(combined, errors="coerce", dayfirst=True)
    elif "date_only" in found:
        timestamp = pd.to_datetime(frame[found["date_only"]], errors="coerce", dayfirst=True)
    else:
        raise ValueError(
            f"{source_name}: no usable timestamp column. Expected Date + Time "
            "or a combined Timestamp/DateTime column."
        )

    result = pd.DataFrame()
    result["tradingsymbol"] = frame[found["tradingsymbol"]].astype("string").str.strip()
    result["date"] = timestamp

    for target in ("open", "high", "low", "close", "volume", "open_interest"):
        if target in found:
            result[target] = pd.to_numeric(frame[found[target]], errors="coerce")

    # Preserve the original date as the source_date audit field if the table has it.
    result["source_date"] = timestamp.dt.strftime("%Y-%m-%d")

    # Remove rows that cannot satisfy the composite primary key.
    invalid_mask = (
        result["tradingsymbol"].isna()
        | result["tradingsymbol"].eq("")
        | result["date"].isna()
    )
    invalid_count = int(invalid_mask.sum())
    if invalid_count:
        logger.warning(
            "%s: skipping %s row(s) with missing/invalid trading symbol or timestamp.",
            source_name, invalid_count,
        )
        result = result.loc[~invalid_mask].copy()

    return result


def get_table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> list[tuple[str, str]]:
    exists = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'main' AND lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchone()[0]
    if not exists:
        raise RuntimeError(
            f"Table {table_name} does not exist in the selected database. "
            "Run your Parquet import script first to create it."
        )
    return [
        (row[1], row[2])
        for row in con.execute(f"PRAGMA table_info({quote_identifier(table_name)})").fetchall()
    ]


def verify_primary_key(
    con: duckdb.DuckDBPyConnection, table_name: str
) -> None:
    constraints = con.execute(
        """
        SELECT constraint_type, constraint_column_names
        FROM duckdb_constraints()
        WHERE lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchall()

    pk_columns = None
    for constraint_type, columns in constraints:
        if constraint_type == "PRIMARY KEY":
            pk_columns = [str(column).lower() for column in columns]
            break

    if pk_columns is None:
        raise RuntimeError(
            f"{table_name} has no PRIMARY KEY. Add a primary key on "
            "(tradingsymbol, date) before importing to guarantee duplicate protection."
        )

    if pk_columns != ["tradingsymbol", "date"] and set(pk_columns) != {"tradingsymbol", "date"}:
        raise RuntimeError(
            f"Unexpected primary key on {table_name}: {pk_columns}. "
            "This importer expects PRIMARY KEY (tradingsymbol, date)."
        )


def import_frames(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    table_columns: list[tuple[str, str]],
    named_frames: Iterable[tuple[str, pd.DataFrame]],
) -> tuple[int, int, int]:
    """Insert all prepared frames, warning about duplicates and skipping conflicts."""
    table_sql = quote_identifier(table_name)
    destination_types = {name.lower(): dtype for name, dtype in table_columns}
    inserted_total = 0
    source_total = 0
    duplicate_total = 0

    for source_name, raw_frame in named_frames:
        prepared = prepare_frame(raw_frame, source_name)
        if prepared.empty:
            continue

        # Only keep columns that exist in the target table. Missing destination
        # columns are omitted from INSERT so DuckDB uses defaults/NULLs.
        selected_columns = [
            column for column in prepared.columns
            if column.lower() in destination_types
        ]

        required = {"tradingsymbol", "date"}
        if not required.issubset({c.lower() for c in selected_columns}):
            raise RuntimeError(
                f"{table_name} must contain tradingsymbol and date columns. "
                f"Found destination columns: {[c for c, _ in table_columns]}"
            )

        # Match the date values to the target schema. DuckDB/Pandas can preserve
        # timezone-aware datetimes from some source files; the existing market-data
        # table uses timezone-free timestamps, so remove timezone metadata while
        # preserving the displayed exchange-local wall-clock time.
        date_type = destination_types["date"].upper()
        parsed_dates = pd.to_datetime(prepared["date"], errors="coerce")
        if "TIMESTAMP" in date_type or date_type == "DATETIME":
            if isinstance(parsed_dates.dtype, pd.DatetimeTZDtype):
                parsed_dates = parsed_dates.dt.tz_localize(None)
            prepared["date"] = parsed_dates
        elif date_type == "DATE":
            if isinstance(parsed_dates.dtype, pd.DatetimeTZDtype):
                parsed_dates = parsed_dates.dt.tz_localize(None)
            prepared["date"] = parsed_dates.dt.date

        # Ensure only one row per key within this file/sheet. Keep the last
        # occurrence deterministically and report the duplicate count.
        before = len(prepared)
        prepared = prepared.drop_duplicates(subset=["tradingsymbol", "date"], keep="last")
        intra_file_duplicates = before - len(prepared)

        # Count keys already present in the destination before inserting.
        con.register("incoming_options_data", prepared[selected_columns])
        source_rows = len(prepared)
        existing_duplicates = con.execute(
            f"""
            SELECT COUNT(*)
            FROM incoming_options_data AS incoming
            INNER JOIN {table_sql} AS existing
              ON incoming.tradingsymbol = existing.tradingsymbol
             AND CAST(incoming.date AS TIMESTAMP) = CAST(existing.date AS TIMESTAMP)
            """
        ).fetchone()[0]

        duplicate_count = intra_file_duplicates + existing_duplicates
        if duplicate_count:
            logger.warning(
                "DUPLICATES in %s: %s duplicate row(s) within source and/or already "
                "present in %s. Existing database rows will be kept.",
                source_name, duplicate_count, table_name,
            )

        # Build explicit destination column list and cast source values to the
        # existing DuckDB column types where needed.
        insert_columns_sql = ", ".join(quote_identifier(c) for c in selected_columns)
        select_expressions = []
        for column in selected_columns:
            dtype = destination_types[column.lower()].upper()
            qcol = quote_identifier(column)
            if column.lower() == "date":
                if "TIMESTAMP" in dtype or dtype == "DATETIME":
                    select_expressions.append(f"CAST({qcol} AS TIMESTAMP) AS {qcol}")
                elif dtype == "DATE":
                    select_expressions.append(f"CAST({qcol} AS DATE) AS {qcol}")
                else:
                    select_expressions.append(f"CAST({qcol} AS VARCHAR) AS {qcol}")
            elif column.lower() == "source_date":
                select_expressions.append(f"CAST({qcol} AS VARCHAR) AS {qcol}")
            else:
                select_expressions.append(f"{qcol} AS {qcol}")

        select_sql = ", ".join(select_expressions)
        con.execute("BEGIN TRANSACTION")
        try:
            result = con.execute(
                f"""
                INSERT INTO {table_sql} ({insert_columns_sql})
                SELECT {select_sql}
                FROM incoming_options_data
                ON CONFLICT DO NOTHING
                """
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.unregister("incoming_options_data")

        # Count newly inserted rows using total table row delta is not safe with
        # concurrent writers; DuckDB is local/single-writer here. Use the cursor
        # rowcount when available, otherwise derive the expected number.
        inserted = max(0, source_rows - existing_duplicates)
        source_total += source_rows
        duplicate_total += duplicate_count
        inserted_total += inserted

        logger.info(
            "%s | source rows: %s | duplicate rows/keys: %s | expected new rows: %s",
            source_name, f"{before:,}", f"{duplicate_count:,}", f"{inserted:,}",
        )

    return source_total, inserted_total, duplicate_total


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Insert historical NIFTY options data into existing DuckDB table."
    )
    parser.add_argument(
        "--file", required=True,
        help="Path to .zip, .csv, .xlsx, or .xls input file.",
    )
    parser.add_argument(
        "--db-path", type=Path, default=DEFAULT_DB_PATH,
        help=f"Path to existing DuckDB database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--table", default=DEFAULT_TABLE,
        help="Destination table (default: NIFTY_OPTIONS).",
    )
    args = parser.parse_args()

    input_path = Path(args.file).expanduser().resolve()
    db_path = args.db_path.expanduser().resolve()
    table_name = args.table.upper()

    if not db_path.is_file():
        logger.error("DuckDB database file does not exist: %s", db_path)
        return 1

    con = None
    try:
        named_frames = load_input_file(input_path)
        logger.info("Input file: %s", input_path)
        logger.info("Source CSV/Excel file(s) or sheet(s): %s", len(named_frames))
        logger.info("DuckDB file: %s", db_path)
        logger.info("Destination table: %s", table_name)

        con = duckdb.connect(str(db_path))
        table_columns = get_table_columns(con, table_name)
        verify_primary_key(con, table_name)

        source_rows, expected_inserted, duplicate_rows = import_frames(
            con, table_name, table_columns, named_frames
        )

        logger.info("Historical import completed.")
        logger.info("Prepared source rows: %s", f"{source_rows:,}")
        logger.info("Duplicate rows/keys reported: %s", f"{duplicate_rows:,}")
        logger.info("Expected newly inserted rows: %s", f"{expected_inserted:,}")
        logger.info("Database: %s", db_path)
        logger.info("Table: %s", table_name)
        return 0

    except Exception:
        logger.exception("Historical import failed.")
        return 1
    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    sys.exit(main())
