"""Download NIFTY 50 one-minute historical candles from Kite.

Run from any directory:
  python scanner/09_download_historical_nifty_50.py --start-date 2026-10-01 --end-date 2026-10-13

The default output is:
  scanner/data/market_data_clean/YYYY-MM-DD/NIFTY_50/part_000001.parquet

Authentication reuses scanner/config.json and scanner/session.json created by
01_login.py. Instrument metadata comes from
scanner/data/instruments/research/research_instruments.csv, created by
02_download_instruments.py.

Each Kite request covers one calendar day (well below Kite's 60-day maximum
for minute candles). Existing files are preserved; reruns append only candles
whose timestamps are missing in the selected output format.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from kiteconnect import KiteConnect


PROJECT_DIR = Path(__file__).resolve().parent
CONFIG_FILE = PROJECT_DIR / "config.json"
SESSION_FILE = PROJECT_DIR / "session.json"
INSTRUMENT_FILE = PROJECT_DIR / "data" / "instruments" / "research" / "research_instruments.csv"
DEFAULT_OUTPUT_ROOT = PROJECT_DIR / "data" / "market_data_clean"

DATASET_NAME = "NIFTY_50"
TRADINGSYMBOL = "NIFTY 50"
INTERVAL = "minute"
REQUEST_DELAY_SECONDS = 1.0
MAX_RETRIES = 6
BACKOFF_BASE_SECONDS = 5.0
BACKOFF_MAX_SECONDS = 120.0
EXPECTED_COLUMNS = [
    "date", "open", "high", "low", "close", "volume", "oi",
    "instrument_token", "exchange", "tradingsymbol", "underlying",
    "instrument_type", "expiry", "strike", "option_type", "research_dataset",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True, help="First date, inclusive (YYYY-MM-DD).")
    parser.add_argument("--end-date", required=True, help="Last date, inclusive (YYYY-MM-DD).")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help=f"Daily dataset root (default: {DEFAULT_OUTPUT_ROOT}).",
    )
    parser.add_argument(
        "--instrument-file",
        type=Path,
        default=INSTRUMENT_FILE,
        help=f"Instrument CSV created by 02_download_instruments.py (default: {INSTRUMENT_FILE}).",
    )
    parser.add_argument(
        "--format",
        choices=("parquet", "excel", "both"),
        default="parquet",
        help="Output file format. Parquet matches the existing research pipeline (default).",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=REQUEST_DELAY_SECONDS,
        help=f"Delay between daily API requests in seconds (default: {REQUEST_DELAY_SECONDS}).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=MAX_RETRIES,
        help=f"Maximum attempts per date after a transient API error (default: {MAX_RETRIES}).",
    )
    parser.add_argument(
        "--retry-no-data",
        action="store_true",
        help="Retry dates previously recorded as having no candles.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Fetch dates previously marked saved; existing rows are preserved and deduplicated.",
    )
    return parser.parse_args()


def parse_iso_date(value: str, label: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"{label} must use YYYY-MM-DD format; received {value!r}."
        ) from exc
    if parsed.isoformat() != value:
        raise ValueError(
            f"{label} must use YYYY-MM-DD format; received {value!r}."
        )
    return parsed


def load_credentials() -> tuple[str, str]:
    if not CONFIG_FILE.is_file():
        raise FileNotFoundError(f"Missing {CONFIG_FILE}. Create it with api_key and api_secret.")
    with CONFIG_FILE.open("r", encoding="utf-8") as stream:
        config = json.load(stream)
    api_key = str(config.get("api_key", "")).strip()
    if not api_key or api_key.startswith("YOUR_"):
        raise ValueError("Set a real api_key in config.json.")
    if not config.get("api_secret") or str(config["api_secret"]).startswith("YOUR_"):
        raise ValueError("Set a real api_secret in config.json before running 01_login.py.")
    if not SESSION_FILE.is_file():
        raise FileNotFoundError(f"Missing {SESSION_FILE}. Run 01_login.py first.")
    with SESSION_FILE.open("r", encoding="utf-8") as stream:
        session = json.load(stream)
    access_token = str(session.get("access_token", "")).strip()
    if not access_token:
        raise ValueError("access_token is missing from session.json. Run 01_login.py again.")
    return api_key, access_token


def load_nifty_instrument(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(
            f"Instrument CSV not found: {path}. Run 02_download_instruments.py first."
        )
    instruments = pd.read_csv(path, low_memory=False)
    required = {"instrument_token", "exchange", "tradingsymbol", "segment"}
    missing = required - set(instruments.columns)
    if missing:
        raise ValueError(f"{path} is missing required columns: {sorted(missing)}")

    symbols = instruments["tradingsymbol"].astype("string").str.strip()
    segments = instruments["segment"].astype("string").str.strip().str.upper()
    matches = instruments.loc[symbols.eq(TRADINGSYMBOL) & segments.eq("INDICES")].copy()
    if matches.empty:
        raise ValueError(
            f"Could not find {TRADINGSYMBOL!r} in the INDICES segment of {path}. "
            "Refresh it with 02_download_instruments.py."
        )

    matches["instrument_token"] = pd.to_numeric(matches["instrument_token"], errors="coerce")
    matches = matches.loc[matches["instrument_token"].notna()].copy()
    matches["instrument_token"] = matches["instrument_token"].astype("int64")
    matches = matches.drop_duplicates(subset=["instrument_token"])
    if len(matches) != 1:
        raise ValueError(
            f"Expected one NIFTY 50 index instrument in {path}; found {len(matches)}."
        )

    record = matches.iloc[0].to_dict()
    record["instrument_token"] = int(record["instrument_token"])
    record["tradingsymbol"] = TRADINGSYMBOL
    exchange = record.get("exchange")
    name = record.get("name")
    instrument_type = record.get("instrument_type")
    record["exchange"] = str(exchange).strip() if pd.notna(exchange) and str(exchange).strip() else "NSE"
    record["name"] = str(name).strip() if pd.notna(name) and str(name).strip() else TRADINGSYMBOL
    record["instrument_type"] = str(instrument_type).strip() if pd.notna(instrument_type) else ""
    return record


def create_kite() -> KiteConnect:
    api_key, access_token = load_credentials()
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    try:
        kite.profile()
    except Exception as exc:
        raise RuntimeError(
            "Kite authentication failed. Run 01_login.py to refresh session.json."
        ) from exc
    return kite


def error_text(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def is_auth_error(exc: Exception) -> bool:
    message = str(exc).lower()
    class_name = type(exc).__name__.lower()
    status_code = getattr(exc, "status_code", None)
    return (
        status_code in {401, 403}
        or class_name in {"tokenexception", "authexception"}
        or any(
            marker in message
            for marker in (
                "incorrect api_key", "invalid access_token", "expired token",
                "login required", "unauthorized", "authentication failed",
                "session expired", "token is invalid",
            )
        )
    )


def is_rate_limit_error(exc: Exception) -> bool:
    message = str(exc).lower()
    status_code = getattr(exc, "status_code", None)
    return status_code == 429 or any(
        marker in message for marker in ("too many requests", "429", "rate limit", "request limit")
    )


def fetch_day(kite: KiteConnect, token: int, target_day: date, max_retries: int) -> list[dict[str, Any]]:
    day_text = target_day.isoformat()
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            return kite.historical_data(
                token,
                day_text,
                day_text,
                interval=INTERVAL,
                continuous=False,
                oi=False,
            )
        except Exception as exc:
            last_error = exc
            if is_auth_error(exc):
                raise RuntimeError(
                    "Kite authentication failed during historical download. "
                    "Run 01_login.py to refresh session.json."
                ) from exc
            if attempt >= max_retries:
                break

            if is_rate_limit_error(exc):
                delay = min(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), BACKOFF_MAX_SECONDS)
                print(f"  Rate limited on {day_text}; retry {attempt + 1}/{max_retries} in {delay:.0f}s.")
            else:
                delay = min(2 ** attempt, 30)
                print(
                    f"  Temporary API error on {day_text} ({error_text(exc)}); "
                    f"retry {attempt + 1}/{max_retries} in {delay}s."
                )
            time.sleep(delay)

    raise RuntimeError(
        f"API request failed for {day_text} after {max_retries} attempts: {error_text(last_error)}"
    ) from last_error


def local_exchange_time(values: pd.Series) -> pd.Series:
    """Return exchange-local wall-clock timestamps without timezone metadata."""
    timestamps = pd.to_datetime(values, errors="coerce")
    try:
        if timestamps.dt.tz is not None:
            timestamps = timestamps.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except (AttributeError, TypeError):
        # Kite responses are consistently timezone-aware; keep a safe fallback for
        # older exported files that may contain mixed or naive timestamps.
        pass
    return timestamps


def normalize_candles(
    candles: list[dict[str, Any]], instrument: dict[str, Any], target_day: date
) -> pd.DataFrame:
    if not candles:
        return pd.DataFrame(columns=EXPECTED_COLUMNS)

    frame = pd.DataFrame(candles)
    if "date" not in frame.columns:
        raise ValueError("CANDLE_VALIDATION_ERROR: Kite response has no date field.")

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    if frame["date"].isna().any():
        raise ValueError("CANDLE_VALIDATION_ERROR: Kite response has an invalid timestamp.")

    exchange_times = local_exchange_time(frame["date"])
    if not exchange_times.dt.date.eq(target_day).all():
        raise ValueError(
            f"CANDLE_VALIDATION_ERROR: Kite returned candles outside {target_day.isoformat()}."
        )
    minute = exchange_times.dt.strftime("%H:%M")
    outside_session = (minute < "09:15") | (minute > "15:29")
    if outside_session.any():
        raise ValueError(
            f"CANDLE_VALIDATION_ERROR: {int(outside_session.sum())} candle(s) are outside "
            "the NIFTY 50 session 09:15-15:29."
        )

    for column in ("open", "high", "low", "close"):
        if column not in frame.columns:
            raise ValueError(f"CANDLE_VALIDATION_ERROR: Kite response is missing {column!r}.")
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        prices = frame[column].to_numpy(dtype=float)
        if not np.isfinite(prices).all() or (prices < 0).any():
            raise ValueError(f"CANDLE_VALIDATION_ERROR: {column} contains an invalid price.")

    invalid_ohlc = (
        (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
        | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
    )
    if invalid_ohlc.any():
        raise ValueError(
            f"CANDLE_VALIDATION_ERROR: {int(invalid_ohlc.sum())} candle(s) violate OHLC bounds."
        )

    if "volume" not in frame.columns:
        frame["volume"] = 0
    else:
        frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    if "oi" not in frame.columns:
        frame["oi"] = None
    frame["instrument_token"] = int(instrument["instrument_token"])
    frame["exchange"] = instrument["exchange"]
    frame["tradingsymbol"] = TRADINGSYMBOL
    frame["underlying"] = instrument["name"]
    frame["instrument_type"] = instrument["instrument_type"]
    expiry = instrument.get("expiry")
    frame["expiry"] = None if pd.isna(expiry) else expiry
    strike = instrument.get("strike")
    frame["strike"] = None if pd.isna(strike) else strike
    frame["option_type"] = None
    frame["research_dataset"] = DATASET_NAME
    for column in EXPECTED_COLUMNS:
        if column not in frame.columns:
            frame[column] = None

    frame = frame[EXPECTED_COLUMNS].copy()
    frame = frame.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return frame.reset_index(drop=True)


def normalized_timestamp_keys(values: pd.Series) -> set[pd.Timestamp]:
    timestamps = local_exchange_time(values)
    return {timestamp for timestamp in timestamps.dropna()}


def output_extensions(output_format: str) -> list[str]:
    if output_format == "both":
        return [".parquet", ".xlsx"]
    return [".parquet" if output_format == "parquet" else ".xlsx"]


def load_existing_timestamps(
    folder: Path, extension: str, instrument_token: int, target_day: date
) -> set[pd.Timestamp]:
    existing: set[pd.Timestamp] = set()
    pattern = f"part_*{extension}"
    for path in sorted(folder.glob(pattern)):
        try:
            if extension == ".parquet":
                prior = pd.read_parquet(path, columns=["date", "instrument_token"])
            else:
                prior = pd.read_excel(path, sheet_name=0, usecols=["date", "instrument_token"])
        except Exception as exc:
            raise RuntimeError(
                f"Could not safely inspect existing data file {path}: {error_text(exc)}"
            ) from exc

        tokens = pd.to_numeric(prior["instrument_token"], errors="coerce")
        timestamps = local_exchange_time(prior["date"])
        matches = tokens.eq(instrument_token) & timestamps.dt.date.eq(target_day)
        existing.update(normalized_timestamp_keys(timestamps.loc[matches]))
    return existing


def next_part_path(folder: Path, extension: str) -> Path:
    highest = 0
    for path in folder.glob(f"part_*{extension}"):
        match = re.fullmatch(r"part_(\d+)", path.stem)
        if match:
            highest = max(highest, int(match.group(1)))
    return folder / f"part_{highest + 1:06d}{extension}"


def write_output(frame: pd.DataFrame, folder: Path, extension: str) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = next_part_path(folder, extension)
    temporary = path.with_name(f"{path.stem}.tmp{path.suffix}")
    if extension == ".parquet":
        frame.to_parquet(temporary, index=False, engine="pyarrow")
    else:
        excel_frame = frame.copy()
        excel_frame["date"] = local_exchange_time(excel_frame["date"])
        excel_frame.to_excel(temporary, index=False, engine="openpyxl")
    os.replace(temporary, path)
    return path


def checkpoint_path(output_root: Path) -> Path:
    return output_root / "nifty_50_historical_checkpoint.json"


def load_checkpoint(path: Path, token: int) -> dict[str, Any]:
    if not path.exists():
        return {"instrument_token": token, "dates": {}}
    try:
        with path.open("r", encoding="utf-8") as stream:
            checkpoint = json.load(stream)
    except Exception as exc:
        raise RuntimeError(
            f"Could not safely read {path}. Back it up and inspect it before continuing."
        ) from exc
    stored_token = checkpoint.get("instrument_token")
    if stored_token not in (None, token):
        raise RuntimeError(
            f"{path} records instrument token {stored_token}, but the current NIFTY 50 "
            f"token is {token}. Move the old checkpoint aside before using this instrument master."
        )
    checkpoint["instrument_token"] = token
    checkpoint.setdefault("dates", {})
    return checkpoint


def save_checkpoint(path: Path, checkpoint: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint["updated_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
    temporary = path.with_name(f"{path.stem}.tmp{path.suffix}")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(checkpoint, stream, indent=2, sort_keys=True, default=str)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def checkpoint_matches(
    record: dict[str, Any], formats: list[str], output_folder: Path, retry_no_data: bool, refresh: bool
) -> bool:
    if record.get("formats") != formats:
        return False
    status = record.get("status")
    if status == "saved" and not refresh:
        return all(any(output_folder.glob(f"part_*{extension}")) for extension in formats)
    if status == "no_data" and not retry_no_data:
        return not any(
            any(output_folder.glob(f"part_*{extension}")) for extension in formats
        )
    return False


def date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def run(args: argparse.Namespace) -> int:
    start = parse_iso_date(args.start_date, "--start-date")
    end = parse_iso_date(args.end_date, "--end-date")
    if start > end:
        raise ValueError("--start-date must be on or before --end-date.")
    if args.request_delay < 0:
        raise ValueError("--request-delay must be zero or greater.")
    if args.max_retries < 1:
        raise ValueError("--max-retries must be at least 1.")

    output_root = args.output_root.expanduser().resolve()
    instrument_file = args.instrument_file.expanduser().resolve()
    instrument = load_nifty_instrument(instrument_file)
    token = int(instrument["instrument_token"])
    kite = create_kite()
    formats = output_extensions(args.format)
    cp_file = checkpoint_path(output_root)
    checkpoint = load_checkpoint(cp_file, token)

    print("=" * 72)
    print("KITE NIFTY 50 HISTORICAL ONE-MINUTE DOWNLOADER")
    print("=" * 72)
    print(f"Date range (inclusive): {start} through {end}")
    print(f"Instrument: {TRADINGSYMBOL} | {instrument['exchange']} | token {token}")
    print(f"Format: {args.format}")
    print(f"Output root: {output_root}")
    print(f"Request pacing: {args.request_delay:.1f}s between daily requests")
    print("Existing data files will be preserved; missing timestamps are appended.")

    failures = 0
    no_data_count = 0
    saved_count = 0
    days = list(date_range(start, end))
    try:
        for index, target_day in enumerate(days, start=1):
            day_text = target_day.isoformat()
            folder = output_root / day_text / DATASET_NAME
            previous = checkpoint["dates"].get(day_text, {})
            if checkpoint_matches(
                previous, formats, folder, args.retry_no_data, args.refresh
            ):
                status = previous.get("status", "saved")
                print(f"[{index}/{len(days)}] {day_text}: skipping cached {status} result.")
                if status == "no_data":
                    no_data_count += 1
                else:
                    saved_count += 1
                continue

            print(f"[{index}/{len(days)}] {day_text}: requesting {INTERVAL} candles...")
            try:
                candles = fetch_day(kite, token, target_day, args.max_retries)
                if args.request_delay:
                    time.sleep(args.request_delay)
                frame = normalize_candles(candles, instrument, target_day)
            except RuntimeError as exc:
                checkpoint["dates"][day_text] = {
                    "status": "failed",
                    "formats": formats,
                    "error": error_text(exc),
                }
                save_checkpoint(cp_file, checkpoint)
                if "authentication failed" in str(exc).lower():
                    raise
                failures += 1
                print(f"  FAILED: {exc}")
                continue
            except Exception as exc:
                checkpoint["dates"][day_text] = {
                    "status": "failed",
                    "formats": formats,
                    "error": error_text(exc),
                }
                save_checkpoint(cp_file, checkpoint)
                failures += 1
                print(f"  FAILED: {error_text(exc)}")
                continue

            if frame.empty:
                checkpoint["dates"][day_text] = {
                    "status": "no_data",
                    "formats": formats,
                    "rows": 0,
                }
                save_checkpoint(cp_file, checkpoint)
                no_data_count += 1
                print("  No candles returned (holiday, non-trading day, or unavailable history).")
                continue

            saved_paths = []
            existing_counts: dict[str, int] = {}
            for extension in formats:
                present = load_existing_timestamps(folder, extension, token, target_day)
                existing_counts[extension] = len(present)
                keys = local_exchange_time(frame["date"])
                keep = ~keys.isin(present)
                missing_rows = frame.loc[keep].copy()
                if not missing_rows.empty:
                    path = write_output(missing_rows, folder, extension)
                    saved_paths.append(str(path))
                    print(f"  Saved {len(missing_rows):,} rows: {path}")

            status = "saved"
            checkpoint["dates"][day_text] = {
                "status": status,
                "formats": formats,
                "rows_returned": int(len(frame)),
                "rows_already_present": existing_counts,
                "files_written": saved_paths,
                "saved_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            }
            save_checkpoint(cp_file, checkpoint)
            saved_count += 1
            if not saved_paths:
                print("  All returned timestamps already exist; no duplicate rows written.")

    except KeyboardInterrupt:
        save_checkpoint(cp_file, checkpoint)
        print("\nInterrupted. Saved data and checkpoint; rerun the same command to resume.")
        return 130

    failed_dates = [
        day.isoformat()
        for day in days
        if checkpoint["dates"].get(day.isoformat(), {}).get("status") == "failed"
    ]
    print("=" * 72)
    print(f"Dates processed: {len(days):,}")
    print(f"Dates with data saved or already present: {saved_count:,}")
    print(f"Dates with no candles: {no_data_count:,}")
    print(f"Dates with failures: {len(failed_dates):,}")
    if failed_dates:
        print("Failed dates:")
        for day_text in failed_dates:
            record = checkpoint["dates"][day_text]
            print(f"  {day_text}: {record.get('error', 'No error details recorded.')}")
    print(f"Checkpoint: {cp_file}")
    print("=" * 72)
    return 1 if failures else 0


def main() -> int:
    args = parse_args()
    try:
        return run(args)
    except Exception as exc:
        print(f"ERROR: {error_text(exc)}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
