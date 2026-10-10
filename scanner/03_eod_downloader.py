import argparse
import json
import os
import time
from pathlib import Path
from datetime import datetime, date

import numpy as np
import pandas as pd
from kiteconnect import KiteConnect

try:
    from kiteconnect.exceptions import InputException, TokenException, KiteException
except ImportError:  # Compatibility with versions exposing fewer exception classes
    InputException = TokenException = KiteException = ()

# ============================ CONFIGURATION ============================
CONFIG_FILE = Path("config.json")
SESSION_FILE = Path("session.json")
INSTRUMENT_FILE = Path("data/instruments/research/research_instruments.csv")
DEFAULT_OUTPUT_ROOT = Path("data/market_data")
OUTPUT_ROOT = DEFAULT_OUTPUT_ROOT
TARGET_DATE = "2026-10-08"  # Keep unchanged while resuming this date; None = today
REQUEST_DELAY_SECONDS = 1.0
MAX_RETRIES = 6
BACKOFF_BASE_SECONDS = 5.0
BACKOFF_MAX_SECONDS = 120.0
CHECKPOINT_EVERY = 50
BATCH_INSTRUMENT_LIMIT = 50

DERIVATIVE_DATASETS = {
    ("NIFTY", "OPT"): "NIFTY_OPTIONS",
    ("NIFTY", "FUT"): "NIFTY_FUTURES",
    ("BANKNIFTY", "OPT"): "BANKNIFTY_OPTIONS",
    ("BANKNIFTY", "FUT"): "BANKNIFTY_FUTURES",
    ("SENSEX", "OPT"): "SENSEX_OPTIONS",
    ("SENSEX", "FUT"): "SENSEX_FUTURES",
}
INDEX_DATASETS = {
    "NIFTY 50": "NIFTY_50",
    "NIFTY BANK": "NIFTY_BANK",
    "SENSEX": "SENSEX",
    "INDIA VIX": "INDIA_VIX",
}
EXPECTED_COLUMNS = [
    "date", "open", "high", "low", "close", "volume", "oi",
    "instrument_token", "exchange", "tradingsymbol", "underlying",
    "instrument_type", "expiry", "strike", "option_type", "research_dataset",
]

# ============================ AUTHENTICATION ============================
def load_config():
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Could not find {CONFIG_FILE.resolve()}")
    with CONFIG_FILE.open("r", encoding="utf-8") as f:
        config = json.load(f)
    if not config.get("api_key") or not config.get("api_secret"):
        raise ValueError("api_key or api_secret is missing from config.json")
    if str(config["api_key"]).startswith("YOUR_") or str(config["api_secret"]).startswith("YOUR_"):
        raise ValueError("Replace placeholder API credentials in config.json")
    return config


def load_session():
    if not SESSION_FILE.exists():
        raise FileNotFoundError("session.json not found. Run 01_login.py first.")
    with SESSION_FILE.open("r", encoding="utf-8") as f:
        session = json.load(f)
    access_token = session.get("access_token")
    if not access_token:
        raise ValueError("access_token missing from session.json. Run 01_login.py again.")
    return access_token


def create_kite():
    kite = KiteConnect(api_key=load_config()["api_key"])
    kite.set_access_token(load_session())
    return kite


def get_target_date(date_override=None):
    """Resolve the target date; preserve the configured date unless CLI overrides it."""
    value = date_override if date_override is not None else TARGET_DATE
    if value is None:
        return date.today()
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid target date {value!r}; expected YYYY-MM-DD.") from exc

# ============================ INSTRUMENT UNIVERSE ============================
def get_instruments_for_date(instrument_file, target_date):
    if not instrument_file.exists():
        raise FileNotFoundError(f"Research instrument file not found: {instrument_file.resolve()}")
    df = pd.read_csv(instrument_file)
    required = {"instrument_token", "exchange", "tradingsymbol", "name", "instrument_type", "expiry", "strike", "segment"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"research_instruments.csv is missing columns: {sorted(missing)}")

    df["instrument_token"] = pd.to_numeric(df["instrument_token"], errors="coerce")
    df = df[df["instrument_token"].notna()].copy()
    df["instrument_token"] = df["instrument_token"].astype("int64")
    df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce").dt.date
    df["name"] = df["name"].astype(str).str.strip()
    df["instrument_type"] = df["instrument_type"].astype(str).str.strip().str.upper()
    df["segment"] = df["segment"].astype(str).str.strip().str.upper()

    derivative_underlyings = {"NIFTY", "BANKNIFTY", "SENSEX"}
    index_underlyings = {"NIFTY 50", "NIFTY BANK", "SENSEX", "INDIA VIX"}
    derivative_mask = (
        df["name"].isin(derivative_underlyings)
        & df["instrument_type"].isin(["CE", "PE", "FUT"])
        & (df["expiry"].isna() | (df["expiry"] >= target_date))
    )
    index_mask = df["name"].isin(index_underlyings) & df["segment"].eq("INDICES")
    df = df[derivative_mask | index_mask].copy()

    def classify(row):
        name = str(row["name"]).strip()
        kind = str(row["instrument_type"]).upper()
        segment = str(row["segment"]).upper()
        if segment == "INDICES":
            return INDEX_DATASETS.get(name)
        if name in derivative_underlyings:
            if kind in {"CE", "PE"}:
                return DERIVATIVE_DATASETS.get((name, "OPT"))
            if kind == "FUT":
                return DERIVATIVE_DATASETS.get((name, "FUT"))
        return None

    df["research_dataset"] = df.apply(classify, axis=1)
    df = df[df["research_dataset"].notna()].copy()
    df = df.drop_duplicates(subset=["instrument_token"], keep="last")
    df = df.sort_values(["research_dataset", "expiry", "tradingsymbol"], na_position="last")
    return df.reset_index(drop=True)


def print_dataset_breakdown(df):
    print("\n" + "=" * 72 + "\nDATASET BREAKDOWN\n" + "=" * 72)
    print(df.groupby("research_dataset")["instrument_token"].count().sort_index().to_string())
    print(f"\nTOTAL INSTRUMENTS TO PROCESS: {len(df):,}\n" + "=" * 72)

# ============================ CHECKPOINTS ============================
def checkpoint_path(output_root, target_date):
    folder = output_root / target_date.isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    return folder / "checkpoint.json"


def load_checkpoint(output_root, target_date):
    path = checkpoint_path(output_root, target_date)
    if not path.exists():
        return {
            "target_date": target_date.isoformat(), "completed_tokens": [],
            "failed_tokens": [], "no_data_tokens": [], "invalid_tokens": [],
        }
    try:
        with path.open("r", encoding="utf-8") as f:
            cp = json.load(f)
        if cp.get("target_date") != target_date.isoformat():
            raise ValueError("Checkpoint target date does not match requested date")
        for key in ("completed_tokens", "failed_tokens", "no_data_tokens", "invalid_tokens"):
            cp.setdefault(key, [])
        return cp
    except Exception as exc:
        raise RuntimeError(f"Could not safely read {path}. Back it up and inspect it before continuing.") from exc


def save_checkpoint(output_root, target_date, completed_tokens, failed_tokens, no_data_tokens, invalid_tokens):
    path = checkpoint_path(output_root, target_date)
    # Keep checkpoint statuses mutually consistent. Invalid-token and no-data states
    # are distinct; failed tokens must remain eligible for retry on the next run.
    completed = {int(x) for x in completed_tokens}
    failed = {int(x) for x in failed_tokens}
    no_data = {int(x) for x in no_data_tokens}
    invalid = {int(x) for x in invalid_tokens}
    no_data.difference_update(invalid)
    failed.difference_update(no_data | invalid)
    completed.difference_update(failed | invalid)
    completed.update(no_data)
    payload = {
        "target_date": target_date.isoformat(),
        "completed_tokens": sorted(completed),
        "failed_tokens": sorted(failed),
        "no_data_tokens": sorted(no_data),
        "invalid_tokens": sorted(invalid),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    temp_path = path.with_suffix(".json.tmp")
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp_path, path)

# ============================ EXISTING DATA RECONCILIATION ============================
def get_existing_part_number(folder):
    highest = 0
    if folder.exists():
        for path in folder.glob("part_*.parquet"):
            try:
                highest = max(highest, int(path.stem.split("_")[-1]))
            except (ValueError, IndexError):
                pass
    return highest


def scan_persisted_tokens(target_output, token_to_dataset):
    """Trust rows only when dataset identity is supported by the column or folder."""
    persisted = set()
    warnings = []
    if not target_output.exists():
        return persisted, warnings

    for path in target_output.rglob("*.parquet"):
        try:
            try:
                part = pd.read_parquet(path, columns=["instrument_token", "research_dataset"])
                has_dataset_column = True
            except Exception:
                part = pd.read_parquet(path, columns=["instrument_token"])
                has_dataset_column = False
        except Exception as exc:
            warnings.append(f"Could not inspect {path}: {type(exc).__name__}: {exc}")
            continue

        if "instrument_token" not in part.columns:
            warnings.append(f"No instrument_token column in {path}; not used for resume detection.")
            continue

        tokens = pd.to_numeric(part["instrument_token"], errors="coerce").dropna().astype("int64")
        if has_dataset_column:
            pairs = pd.DataFrame({"token": tokens, "dataset": part.loc[tokens.index, "research_dataset"].astype(str)})
            for row in pairs.itertuples(index=False):
                expected = token_to_dataset.get(int(row.token))
                if expected is None:
                    continue
                if row.dataset == expected:
                    persisted.add(int(row.token))
                else:
                    warnings.append(
                        f"{path.name}: stored dataset {row.dataset!r} does not match expected {expected!r}; token {int(row.token)} will be checked again."
                    )
        else:
            folder_dataset = path.parent.name if path.parent != target_output else None
            if folder_dataset is None:
                # Warn once per root-level file, not once per row.
                warnings.append(f"Root-level file {path.name} has no research_dataset column; preserved but not used for resume detection.")
                continue
            expected_tokens = {int(token) for token in tokens if token_to_dataset.get(int(token)) == folder_dataset}
            persisted.update(expected_tokens)
            mismatches = {int(token) for token in tokens if int(token) in token_to_dataset and token_to_dataset[int(token)] != folder_dataset}
            if mismatches:
                warnings.append(f"{path.name}: {len(mismatches)} token(s) in folder {folder_dataset!r} do not match their expected dataset; not used to skip those tokens.")
    # Keep output bounded if many distinct mismatch messages are present.
    if len(warnings) > 100:
        warnings = warnings[:100] + [f"Additional warnings omitted: {len(warnings) - 100}"]
    return persisted, warnings

# ============================ API ERROR CLASSIFICATION ============================
def error_text(exc):
    return f"{type(exc).__name__}: {exc}"


def is_auth_error(exc):
    message = str(exc).lower()
    class_name = type(exc).__name__.lower()
    status_code = getattr(exc, "status_code", None)
    return (
        status_code in {401, 403}
        or class_name in {"tokenexception", "authexception"}
        or any(phrase in message for phrase in (
            "incorrect api_key", "invalid access_token", "expired token",
            "login required", "unauthorized", "authentication failed",
            "session expired", "token is invalid",
        ))
    )


def is_invalid_instrument_token(exc):
    """Recognize Kite's observed InputException: invalid token without retrying it."""
    message = str(exc).strip().lower()
    class_name = type(exc).__name__.lower()
    return class_name == "inputexception" and "invalid token" in message


def is_rate_limit_error(exc):
    message = str(exc).lower()
    status_code = getattr(exc, "status_code", None)
    return status_code == 429 or any(
        marker in message for marker in ("too many requests", "429", "rate limit", "request limit")
    )


def fetch_historical_data(kite, token, target_date):
    day = target_date.isoformat()
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return kite.historical_data(token, day, day, interval="minute", continuous=False, oi=True)
        except Exception as exc:
            last_error = exc
            if is_auth_error(exc):
                raise RuntimeError("AUTHENTICATION ERROR. Run 01_login.py again to refresh the session.") from exc
            if is_invalid_instrument_token(exc):
                # This is a non-transient instrument/token error, not a session refresh signal.
                raise ValueError(f"INVALID_INSTRUMENT_TOKEN: {error_text(exc)}") from exc
            if attempt >= MAX_RETRIES:
                break
            if is_rate_limit_error(exc):
                wait = min(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), BACKOFF_MAX_SECONDS)
                print(f"    Rate limited (attempt {attempt}/{MAX_RETRIES}); waiting {wait:.0f}s before retry.")
            else:
                wait = min(2 ** attempt, 30)
                print(f"    Temporary API error (attempt {attempt}/{MAX_RETRIES}): {error_text(exc)}. Waiting {wait}s.")
            time.sleep(wait)
    raise RuntimeError(f"API request failed after {MAX_RETRIES} attempts: {error_text(last_error)}") from last_error

# ============================ NORMALIZATION / WRITES ============================
def normalize_candles(candles, instrument, target_date=None):
    """Normalize one API response and reject malformed/out-of-session-date candles."""
    if not candles:
        return pd.DataFrame(columns=EXPECTED_COLUMNS)
    frame = pd.DataFrame(candles)
    if frame.empty:
        return pd.DataFrame(columns=EXPECTED_COLUMNS)
    if "date" not in frame.columns:
        raise ValueError("CANDLE_VALIDATION_ERROR: response has no date column")

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    if frame["date"].isna().any():
        raise ValueError("CANDLE_VALIDATION_ERROR: response contains invalid candle timestamps")
    if target_date is not None:
        candle_dates = frame["date"].dt.date
        if not candle_dates.eq(target_date).all():
            unexpected = sorted({str(x) for x in candle_dates[candle_dates != target_date]})
            raise ValueError(
                f"CANDLE_VALIDATION_ERROR: response contains dates outside {target_date}: {unexpected[:5]}"
            )

    price_columns = ["open", "high", "low", "close"]
    for column in price_columns:
        if column not in frame.columns:
            raise ValueError(f"CANDLE_VALIDATION_ERROR: response is missing {column!r}")
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        values = frame[column].to_numpy(dtype=float)
        if not np.isfinite(values).all():
            raise ValueError(f"CANDLE_VALIDATION_ERROR: {column} contains null/non-finite prices")
        if (values < 0).any():
            raise ValueError(f"CANDLE_VALIDATION_ERROR: {column} contains negative prices")

    bad_ohlc = (
        (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
        | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
    )
    if bad_ohlc.any():
        raise ValueError(f"CANDLE_VALIDATION_ERROR: {int(bad_ohlc.sum())} candle(s) violate OHLC bounds")

    frame["instrument_token"] = int(instrument["instrument_token"])
    frame["exchange"] = str(instrument["exchange"])
    frame["tradingsymbol"] = str(instrument["tradingsymbol"])
    frame["underlying"] = str(instrument["name"])
    frame["instrument_type"] = str(instrument["instrument_type"])
    expiry = instrument.get("expiry")
    frame["expiry"] = None if pd.isna(expiry) else expiry
    frame["strike"] = instrument.get("strike")
    kind = str(instrument["instrument_type"]).upper()
    frame["option_type"] = kind if kind in {"CE", "PE"} else None
    frame["research_dataset"] = str(instrument["research_dataset"])
    for column in EXPECTED_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    return frame[EXPECTED_COLUMNS].copy()


def write_batch(dataset, frames, tokens, output_root, target_date, part_numbers):
    if not frames:
        return set()
    combined = pd.concat(frames, ignore_index=True)
    folder = output_root / target_date.isoformat() / dataset
    folder.mkdir(parents=True, exist_ok=True)
    part_numbers[dataset] = max(part_numbers.get(dataset, 0), get_existing_part_number(folder)) + 1
    path = folder / f"part_{part_numbers[dataset]:06d}.parquet"
    temp_path = path.with_suffix(".parquet.tmp")
    combined.to_parquet(temp_path, index=False)
    os.replace(temp_path, path)
    print(f"    Saved {len(combined):,} rows to {path}")
    return {int(token) for token in tokens}

# ============================ MAIN ============================
def parse_args():
    parser = argparse.ArgumentParser(
        description="Download one-minute historical candles for the configured research universe."
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help=(
            "Use isolated output root data/market_data_clean instead of the existing "
            "data/market_data folder. Existing data is never deleted. Re-running this "
            "command resumes from the clean folder's own checkpoint."
        ),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Optional explicit output root; overrides --fresh and the default root.",
    )
    parser.add_argument(
        "--date",
        dest="target_date",
        default=None,
        help=(
            "Target trading date in YYYY-MM-DD format. If omitted, uses TARGET_DATE "
            "configured at the top of this script (or today when TARGET_DATE=None)."
        ),
    )
    return parser.parse_args()


def main():
    global OUTPUT_ROOT
    run_start_monotonic = time.monotonic()
    run_start_datetime = datetime.now()
    args = parse_args()
    if args.output_root is not None:
        OUTPUT_ROOT = args.output_root
    elif args.fresh:
        OUTPUT_ROOT = Path("data/market_data_clean")
    else:
        OUTPUT_ROOT = DEFAULT_OUTPUT_ROOT

    target_date = get_target_date(args.target_date)
    target_output = OUTPUT_ROOT / target_date.isoformat()
    target_output.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 72)
    print("OPTIONS RESEARCH - RESUMABLE EOD DATA DOWNLOADER")
    print("=" * 72)
    print(f"Target date: {target_date}")
    print(f"Output root: {OUTPUT_ROOT.resolve()}")
    if args.fresh and args.output_root is None:
        print("CLEAN RUN MODE: isolated output folder; existing data/market_data is untouched.")
    print(f"Request pacing: {REQUEST_DELAY_SECONDS:.1f}s between requests")
    print(f"Maximum retries per instrument: {MAX_RETRIES}")

    kite = create_kite()
    try:
        profile = kite.profile()
        print(f"Zerodha authentication successful: {profile.get('user_name', 'user')}")
    except Exception as exc:
        raise RuntimeError("Could not authenticate with Zerodha. Run 01_login.py again.") from exc

    df = get_instruments_for_date(INSTRUMENT_FILE, target_date)
    if df.empty:
        raise RuntimeError("No instruments found for the target date.")
    print(f"Instrument file: {INSTRUMENT_FILE.resolve()}")
    print_dataset_breakdown(df)

    cp = load_checkpoint(OUTPUT_ROOT, target_date)
    completed = {int(x) for x in cp.get("completed_tokens", [])}
    failed_tokens = {int(x) for x in cp.get("failed_tokens", [])}
    no_data = {int(x) for x in cp.get("no_data_tokens", [])}
    invalid_tokens = {int(x) for x in cp.get("invalid_tokens", [])}
    token_to_dataset = {int(row["instrument_token"]): str(row["research_dataset"]) for _, row in df.iterrows()}

    # Drop stale checkpoint entries that are not in today's selected universe.
    universe_tokens = set(token_to_dataset)
    completed.intersection_update(universe_tokens)
    failed_tokens.intersection_update(universe_tokens)
    no_data.intersection_update(universe_tokens)
    invalid_tokens.intersection_update(universe_tokens)

    persisted, warnings = scan_persisted_tokens(target_output, token_to_dataset)
    completed.update(persisted)
    failed_tokens.difference_update(persisted)
    no_data.difference_update(persisted)
    invalid_tokens.difference_update(persisted)
    failed_tokens.difference_update(no_data)
    failed_tokens.difference_update(invalid_tokens)

    print("\n--- RESUME STATUS ---")
    print(f"Legacy/current checkpoint completed tokens: {len(cp.get('completed_tokens', [])):,}")
    print(f"Verified saved-data tokens:                 {len(persisted):,}")
    print(f"Previously recorded no-data tokens:         {len(no_data):,}")
    print(f"Previously recorded invalid tokens:          {len(invalid_tokens):,}")
    print(f"Previously failed/retryable tokens:         {len(failed_tokens):,}")
    for warning in warnings[:10]:
        print(f"WARNING: {warning}")
    if len(warnings) > 10:
        print(f"WARNING: {len(warnings) - 10} additional warning(s) omitted from display.")

    # Existing no-data records are retained for this fixed target date. Invalid tokens
    # are also retained separately and are not sent to the API again automatically.
    pending = df[
        ~df["instrument_token"].isin(persisted)
        & ~df["instrument_token"].isin(no_data)
        & ~df["instrument_token"].isin(invalid_tokens)
    ].copy()
    print(f"Instruments to check/retry: {len(pending):,}")
    print("Existing files will not be deleted or overwritten.")

    datasets = list(df["research_dataset"].drop_duplicates())
    buffers = {name: [] for name in datasets}
    buffer_tokens = {name: [] for name in datasets}
    part_numbers = {name: get_existing_part_number(target_output / name) for name in datasets}
    counts = {"data": 0, "no_data": 0, "failed": 0, "invalid": 0}
    processed = 0
    interrupted = False

    def flush_dataset(dataset):
        if not buffers[dataset]:
            return
        saved = write_batch(dataset, buffers[dataset], buffer_tokens[dataset], OUTPUT_ROOT, target_date, part_numbers)
        persisted.update(saved)
        completed.update(saved)
        failed_tokens.difference_update(saved)
        no_data.difference_update(saved)
        invalid_tokens.difference_update(saved)
        buffers[dataset] = []
        buffer_tokens[dataset] = []

    def flush_all():
        for dataset_name in datasets:
            flush_dataset(dataset_name)

    fatal_error = None
    finalization_error = None

    try:
        for _, instrument in pending.iterrows():
            token = int(instrument["instrument_token"])
            dataset = str(instrument["research_dataset"])
            print(f"[{processed + 1}/{len(pending)}] {dataset:<20} {instrument['tradingsymbol']} (token {token})")
            processed += 1
            try:
                candles = fetch_historical_data(kite, token, target_date)
                if not candles:
                    print("    Successful API response with no candles; recorded separately from failures.")
                    no_data.add(token)
                    completed.add(token)
                    failed_tokens.discard(token)
                    invalid_tokens.discard(token)
                    counts["no_data"] += 1
                else:
                    normalized = normalize_candles(candles, instrument, target_date)
                    if normalized.empty:
                        print("    Successful API response normalized to zero rows; recorded as no data.")
                        no_data.add(token)
                        completed.add(token)
                        failed_tokens.discard(token)
                        invalid_tokens.discard(token)
                        counts["no_data"] += 1
                    else:
                        buffers[dataset].append(normalized)
                        buffer_tokens[dataset].append(token)
                        counts["data"] += 1
                        print(f"    Candles received and validated: {len(normalized):,}")
                        if len(buffers[dataset]) >= BATCH_INSTRUMENT_LIMIT:
                            flush_dataset(dataset)
            except KeyboardInterrupt:
                raise
            except ValueError as exc:
                if "INVALID_INSTRUMENT_TOKEN:" in str(exc):
                    invalid_tokens.add(token)
                    failed_tokens.discard(token)
                    no_data.discard(token)
                    completed.discard(token)
                    counts["invalid"] += 1
                    print(f"    INVALID TOKEN (not retried automatically): {exc}")
                else:
                    counts["failed"] += 1
                    failed_tokens.add(token)
                    completed.discard(token)
                    no_data.discard(token)
                    invalid_tokens.discard(token)
                    print(f"    FAILED (left pending for retry): {exc}")
            except Exception as exc:
                counts["failed"] += 1
                failed_tokens.add(token)
                completed.discard(token)
                no_data.discard(token)
                invalid_tokens.discard(token)
                print(f"    FAILED (left pending for retry): {error_text(exc)}")
                if "AUTHENTICATION ERROR" in str(exc):
                    raise

            if processed % CHECKPOINT_EVERY == 0:
                flush_all()
                save_checkpoint(OUTPUT_ROOT, target_date, completed, failed_tokens, no_data, invalid_tokens)
                print("    Checkpoint saved after flushing buffered data.")
            time.sleep(REQUEST_DELAY_SECONDS)

    except KeyboardInterrupt:
        interrupted = True
        print("\nKeyboard interrupt received. Saving buffered data and checkpoint safely...")
    except Exception as exc:
        fatal_error = exc
        print(f"\nFATAL RUN ERROR: {error_text(exc)}")
    finally:
        try:
            flush_all()
        except Exception as exc:
            finalization_error = exc
            # Any token whose buffer could not be persisted remains retryable.
            for dataset_name in datasets:
                for token in buffer_tokens[dataset_name]:
                    failed_tokens.add(int(token))
                    completed.discard(int(token))
                    no_data.discard(int(token))
                    invalid_tokens.discard(int(token))
            print(f"ERROR while flushing final buffers: {error_text(exc)}")
        try:
            save_checkpoint(OUTPUT_ROOT, target_date, completed, failed_tokens, no_data, invalid_tokens)
            print("Checkpoint saved.")
        except Exception as exc:
            finalization_error = finalization_error or exc
            print(f"ERROR while saving checkpoint: {error_text(exc)}")

    run_end_datetime = datetime.now()
    run_elapsed_seconds = max(0, int(time.monotonic() - run_start_monotonic))
    elapsed_hours, remainder = divmod(run_elapsed_seconds, 3600)
    elapsed_minutes, elapsed_seconds = divmod(remainder, 60)
    elapsed_formatted = (
        f"{elapsed_hours:02d}:{elapsed_minutes:02d}:{elapsed_seconds:02d} "
        f"({run_elapsed_seconds:,} seconds)"
    )

    print("\n" + "=" * 72 + "\nDOWNLOAD RUN SUMMARY\n" + "=" * 72)
    print(f"Run start time:                  {run_start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Run end time:                    {run_end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total run time:                  {elapsed_formatted}")
    print(f"Target date:                     {target_date}")
    print(f"Universe instruments:            {len(df):,}")
    print(f"Verified instruments saved:      {len(persisted):,}")
    print(f"Explicit no-data responses:      {len(no_data):,}")
    print(f"Invalid-token instruments:       {len(invalid_tokens):,}")
    print(f"Failures remaining/retryable:    {len(failed_tokens):,}")
    print(f"Data-bearing responses this run:  {counts['data']:,}")
    print(f"No-data responses this run:      {counts['no_data']:,}")
    print(f"Invalid-token responses this run:{counts['invalid']:>7,}")
    print(f"Failed requests this run:         {counts['failed']:,}")
    print(f"Output folder: {target_output.resolve()}")
    print("=" * 72)
    if interrupted:
        print("Run interrupted by user. Saved data and checkpoint where possible; rerun to resume.")
    elif finalization_error is not None:
        print("Run incomplete: final data flush/checkpoint had an error.")
    elif fatal_error is not None:
        print("Run stopped after a fatal error. Inspect the message above before retrying.")
    elif failed_tokens:
        print("Run incomplete: failed instruments remain retryable in checkpoint.json.")
    elif invalid_tokens:
        print("Run has unresolved invalid-token instruments. Review them before treating collection as complete.")
    elif processed == 0:
        print("No pending instruments were found.")
    else:
        print("Run finished. No retryable failures remain; review no-data counts before treating collection as complete.")

    # Nonzero status is important for scheduled runs: a partial run must not look successful.
    if interrupted or fatal_error is not None or finalization_error is not None or failed_tokens or invalid_tokens:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
