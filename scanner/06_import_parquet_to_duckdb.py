"""
Import daily Parquet market data into a local DuckDB database.

Expected source structure:
    scanner/data/market_data_clean/YYYY-MM-DD/INSTRUMENT_NAME/**/*.parquet

Examples of resulting tables:
    NIFTY_OPTIONS, NIFTY_FUTURES, BANKNIFTY_OPTIONS, INDIA_VIX, etc.

Run one date:
    python 06_import_parquet_to_duckdb.py --date 2026-10-07

Run all available dates:
    python 06_import_parquet_to_duckdb.py

Optional path overrides:
    python 06_import_parquet_to_duckdb.py --date 2026-10-07 --db-path "D:/research/market.duckdb"
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import duckdb


# This script is intended to be placed in:
# C:\\Users\\91965\\VISHAL GUPTA\\T\\repo\\daily-stock-scanner\\scanner
PROJECT_ROOT = Path(
    r"C:\Users\91965\VISHAL GUPTA\T\repo\daily-stock-scanner\scanner"
)

DEFAULT_DATA_ROOT = PROJECT_ROOT / "data" / "market_data_clean"
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "nifty_research.duckdb"
SOURCE_DATE_COLUMN = "source_date"

# Recognized source-column names. Matching is case-insensitive.
INSTRUMENT_COLUMN_CANDIDATES = (
    "instrument", "tradingsymbol", "trading_symbol", "symbol", "ticker",
    "instrument_name", "contract_symbol",
)
TIMESTAMP_COLUMN_CANDIDATES = (
    "timestamp", "datetime", "date_time", "date", "time", "exchange_timestamp",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def quote_identifier(identifier: str) -> str:
    """Safely quote a DuckDB table or column name."""
    return '"' + identifier.replace('"', '""') + '"'


def sql_string(value: str) -> str:
    """Safely quote a SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def parquet_reader_sql(files: list[Path]) -> str:
    """Build a DuckDB read_parquet expression for a list of files."""
    paths = ", ".join(sql_string(str(path.resolve())) for path in files)
    return f"read_parquet([{paths}], union_by_name = true)"


def discover_dates(data_root: Path, target_date: str | None) -> list[Path]:
    """Return the requested date directory, or all valid date directories."""
    if not data_root.is_dir():
        raise FileNotFoundError(f"Parquet data root does not exist: {data_root}")

    if target_date:
        try:
            parsed_date = date.fromisoformat(target_date)
        except ValueError as exc:
            raise ValueError("Date must be in YYYY-MM-DD format, e.g. 2026-10-07.") from exc

        # Enforce the exact requested format.
        if parsed_date.isoformat() != target_date:
            raise ValueError("Date must be in YYYY-MM-DD format, e.g. 2026-10-07.")

        date_dir = data_root / target_date
        if not date_dir.is_dir():
            raise FileNotFoundError(f"No folder found for date {target_date}: {date_dir}")
        return [date_dir]

    date_dirs = []
    for path in data_root.iterdir():
        if path.is_dir():
            try:
                if date.fromisoformat(path.name).isoformat() == path.name:
                    date_dirs.append(path)
            except ValueError:
                pass
    return sorted(date_dirs)


def get_parquet_files(dataset_dir: Path) -> list[Path]:
    """Find all Parquet files, including files in nested subfolders."""
    return sorted(
        path for path in dataset_dir.rglob("*")
        if path.is_file() and path.suffix.lower() == ".parquet"
    )


def get_source_schema(
    con: duckdb.DuckDBPyConnection, files: list[Path]
) -> list[tuple[str, str]]:
    """Read the combined source schema."""
    rows = con.execute(
        f"DESCRIBE SELECT * FROM {parquet_reader_sql(files)}"
    ).fetchall()
    columns = [(row[0], row[1]) for row in rows]

    if not columns:
        raise ValueError(f"No columns found in Parquet files: {files}")

    if any(name.lower() == SOURCE_DATE_COLUMN.lower() for name, _ in columns):
        raise ValueError(
            f"Source Parquet already has a '{SOURCE_DATE_COLUMN}' column. "
            "Rename that source column or change SOURCE_DATE_COLUMN."
        )
    return columns


def find_key_columns(
    column_names: list[str],
) -> tuple[str, str]:
    """Find source columns used for the composite primary key."""
    by_lower = {name.lower(): name for name in column_names}
    instrument_col = next(
        (by_lower[name] for name in INSTRUMENT_COLUMN_CANDIDATES if name in by_lower),
        None,
    )
    timestamp_col = next(
        (by_lower[name] for name in TIMESTAMP_COLUMN_CANDIDATES if name in by_lower),
        None,
    )
    if instrument_col is None or timestamp_col is None:
        raise ValueError(
            "Cannot create the required composite key. Detected columns: "
            f"{column_names}. Expected an instrument/contract identifier "
            f"(one of {INSTRUMENT_COLUMN_CANDIDATES}) and timestamp "
            f"(one of {TIMESTAMP_COLUMN_CANDIDATES}). Update the candidate lists "
            "in the script to match your Parquet schema."
        )
    return instrument_col, timestamp_col


def ensure_table_schema(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    source_columns: list[tuple[str, str]],
) -> tuple[str, str]:
    """Create or migrate a table so it has a composite primary key."""
    table_sql = quote_identifier(table_name)
    key_instrument, key_timestamp = find_key_columns(
        [name for name, _ in source_columns]
    )
    exists = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'main' AND lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchone()[0] > 0

    if not exists:
        definitions = [
            f"{quote_identifier(name)} {column_type}"
            for name, column_type in source_columns
        ]
        definitions.append(f"{quote_identifier(SOURCE_DATE_COLUMN)} VARCHAR")
        definitions.append(
            f"PRIMARY KEY ({quote_identifier(key_instrument)}, "
            f"{quote_identifier(key_timestamp)})"
        )
        con.execute(f"CREATE TABLE {table_sql} ({', '.join(definitions)})")
        logger.info(
            "Created table %s with PRIMARY KEY (%s, %s)",
            table_name, key_instrument, key_timestamp,
        )
        return key_instrument, key_timestamp

    info = con.execute(f"PRAGMA table_info({table_sql})").fetchall()
    existing_names = {row[1].lower() for row in info}
    for column_name, column_type in source_columns:
        if column_name.lower() not in existing_names:
            con.execute(
                f"ALTER TABLE {table_sql} ADD COLUMN "
                f"{quote_identifier(column_name)} {column_type}"
            )
            logger.info("Added column '%s' to table %s", column_name, table_name)
            existing_names.add(column_name.lower())

    if SOURCE_DATE_COLUMN.lower() not in existing_names:
        con.execute(
            f"ALTER TABLE {table_sql} "
            f"ADD COLUMN {quote_identifier(SOURCE_DATE_COLUMN)} VARCHAR"
        )

    info = con.execute(f"PRAGMA table_info({table_sql})").fetchall()
    actual_columns = [row[1] for row in info]
    key_instrument, key_timestamp = find_key_columns(actual_columns)
    pk_columns = [row[1] for row in sorted(info, key=lambda r: r[5]) if row[5] > 0]

    if (
        len(pk_columns) == 2
        and {name.lower() for name in pk_columns}
        == {key_instrument.lower(), key_timestamp.lower()}
    ):
        return key_instrument, key_timestamp

    # DuckDB cannot add a primary key in place: rebuild the table.
    duplicate_groups = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT 1 FROM {table_sql}
            GROUP BY {quote_identifier(key_instrument)}, {quote_identifier(key_timestamp)}
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    if duplicate_groups:
        logger.warning(
            "DUPLICATES FOUND in existing table %s: %s duplicate key group(s). "
            "Migration will retain one row per (%s, %s).",
            table_name, duplicate_groups, key_instrument, key_timestamp,
        )

    temp_name = f"__{table_name}_pk_migration"
    temp_sql = quote_identifier(temp_name)
    con.execute(f"DROP TABLE IF EXISTS {temp_sql}")
    definitions = [f"{quote_identifier(row[1])} {row[2]}" for row in info]
    definitions.append(
        f"PRIMARY KEY ({quote_identifier(key_instrument)}, "
        f"{quote_identifier(key_timestamp)})"
    )
    con.execute(f"CREATE TABLE {temp_sql} ({', '.join(definitions)})")

    column_sql = ", ".join(quote_identifier(name) for name in actual_columns)
    con.execute(
        f"""
        INSERT INTO {temp_sql} ({column_sql})
        SELECT {column_sql}
        FROM {table_sql}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY {quote_identifier(key_instrument)}, {quote_identifier(key_timestamp)}
            ORDER BY {quote_identifier(SOURCE_DATE_COLUMN)} DESC NULLS LAST
        ) = 1
        """
    )
    con.execute(f"DROP TABLE {table_sql}")
    con.execute(f"ALTER TABLE {temp_sql} RENAME TO {quote_identifier(table_name)}")
    logger.info(
        "Migrated table %s to composite PRIMARY KEY (%s, %s)",
        table_name, key_instrument, key_timestamp,
    )
    return key_instrument, key_timestamp

def import_dataset(
    con: duckdb.DuckDBPyConnection,
    date_dir: Path,
    dataset_dir: Path,
) -> int:
    """Import one instrument/date; warn about duplicates and enforce the key."""
    table_name = dataset_dir.name.upper()
    table_sql = quote_identifier(table_name)
    source_date = date_dir.name
    files = get_parquet_files(dataset_dir)
    if not files:
        logger.warning("No Parquet files found in %s; skipping.", dataset_dir)
        return 0

    reader = parquet_reader_sql(files)
    source_columns = get_source_schema(con, files)
    key_instrument, key_timestamp = find_key_columns(
        [name for name, _ in source_columns]
    )
    source_column_sql = ", ".join(
        quote_identifier(name) for name, _ in source_columns
    )
    select_sql = (
        f"SELECT {source_column_sql}, "
        f"{sql_string(source_date)}::VARCHAR AS {quote_identifier(SOURCE_DATE_COLUMN)} "
        f"FROM {reader}"
    )
    raw_row_count = con.execute(f"SELECT COUNT(*) FROM {reader}").fetchone()[0]

    duplicate_key_groups, duplicate_extra_rows = con.execute(
        f"""
        SELECT COUNT(*), COALESCE(SUM(row_count - 1), 0)
        FROM (
            SELECT COUNT(*) AS row_count
            FROM {reader}
            GROUP BY {quote_identifier(key_instrument)}, {quote_identifier(key_timestamp)}
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    if duplicate_key_groups:
        logger.warning(
            "DUPLICATE DATA for %s on %s: %s duplicate key group(s), "
            "%s extra row(s), based on (%s, %s). Only one row per key will be retained.",
            table_name, source_date, duplicate_key_groups, duplicate_extra_rows,
            key_instrument, key_timestamp,
        )

    con.execute("BEGIN TRANSACTION")
    try:
        key_instrument, key_timestamp = ensure_table_schema(
            con, table_name, source_columns
        )

        # Rerunning a date replaces its previous rows for that date.
        con.execute(
            f"DELETE FROM {table_sql} "
            f"WHERE {quote_identifier(SOURCE_DATE_COLUMN)} = ?",
            [source_date],
        )

        if raw_row_count:
            deduplicated_select = (
                f"SELECT * FROM ({select_sql}) AS incoming "
                f"QUALIFY ROW_NUMBER() OVER ("
                f"PARTITION BY {quote_identifier(key_instrument)}, "
                f"{quote_identifier(key_timestamp)} ORDER BY "
                f"{quote_identifier(SOURCE_DATE_COLUMN)} DESC) = 1"
            )

            # Detect keys already present under another source date.
            existing_conflicts = con.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT
                        {quote_identifier(key_instrument)},
                        {quote_identifier(key_timestamp)}
                    FROM ({deduplicated_select}) AS incoming
                ) AS incoming_keys
                INNER JOIN {table_sql} AS existing
                  ON incoming_keys.{quote_identifier(key_instrument)}
                     = existing.{quote_identifier(key_instrument)}
                 AND incoming_keys.{quote_identifier(key_timestamp)}
                     = existing.{quote_identifier(key_timestamp)}
                """
            ).fetchone()[0]
            if existing_conflicts:
                logger.warning(
                    "DUPLICATE KEY conflict for %s on %s: %s key(s) already exist "
                    "under another source date. Existing rows will be retained.",
                    table_name, source_date, existing_conflicts,
                )

            con.execute(
                f"INSERT INTO {table_sql} BY NAME {deduplicated_select} "
                "ON CONFLICT DO NOTHING"
            )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    final_date_rows = con.execute(
        f"SELECT COUNT(*) FROM {table_sql} "
        f"WHERE {quote_identifier(SOURCE_DATE_COLUMN)} = ?",
        [source_date],
    ).fetchone()[0]
    logger.info(
        "%s | %-20s | %s source rows, %s rows retained | %s Parquet file(s)",
        source_date, table_name, f"{raw_row_count:,}", f"{final_date_rows:,}", len(files)
    )
    return final_date_rows

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import daily Parquet market data into local DuckDB tables."
    )
    parser.add_argument(
        "--date",
        help="Date to import in YYYY-MM-DD format. Omit to import all date folders.",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help=f"Folder containing date folders (default: {DEFAULT_DATA_ROOT})",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"DuckDB database file (default: {DEFAULT_DB_PATH})",
    )
    args = parser.parse_args()

    data_root = args.data_root.expanduser().resolve()
    db_path = args.db_path.expanduser().resolve()

    try:
        date_dirs = discover_dates(data_root, args.date)
        if not date_dirs:
            logger.error("No YYYY-MM-DD folders found under %s", data_root)
            return 1

        db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Parquet root: %s", data_root)
        logger.info("DuckDB file:  %s", db_path)
        logger.info("Date folders to process: %s", len(date_dirs))

        total_rows = 0
        total_imports = 0
        con = duckdb.connect(str(db_path))
        try:
            for date_dir in date_dirs:
                logger.info("Processing date: %s", date_dir.name)
                dataset_dirs = sorted(path for path in date_dir.iterdir() if path.is_dir())

                if not dataset_dirs:
                    logger.warning("No instrument folders found in %s", date_dir)
                    continue

                # Import every instrument folder automatically, so new instruments
                # do not require edits to a hard-coded allowlist.
                for dataset_dir in dataset_dirs:
                    if not get_parquet_files(dataset_dir):
                        logger.warning("No Parquet files found in %s; skipping.", dataset_dir)
                        continue
                    total_rows += import_dataset(con, date_dir, dataset_dir)
                    total_imports += 1
        finally:
            con.close()

        logger.info("Import completed.")
        logger.info("Instrument/date imports: %s", total_imports)
        logger.info("Rows imported: %s", f"{total_rows:,}")
        logger.info("Database: %s", db_path)
        return 0

    except Exception:
        logger.exception("Import failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
