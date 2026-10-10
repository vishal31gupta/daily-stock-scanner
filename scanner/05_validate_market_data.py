"""Validate downloaded intraday Parquet files without modifying source data.

Run from the project root:
  python .\\05_validate_market_data.py --data-root data\\market_data_clean --date 2026-10-08

Creates data/validation_reports/<date>/validation_report.xlsx and CSV files.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd

PRICE_COLS = ["open", "high", "low", "close"]
REQUIRED_COLS = ["date", "instrument_token", *PRICE_COLS]
# Official NSE timings from 2026-08-03: equity derivatives close at 15:40.
# Minute candles are normally timestamped at the start of the minute.
SESSION_CLOSE = {
    "NIFTY_OPTIONS": "15:39",
    "NIFTY_FUTURES": "15:39",
    "BANKNIFTY_OPTIONS": "15:39",
    "BANKNIFTY_FUTURES": "15:39",
    # Cash-market index series retain the regular 15:30 close.
    "NIFTY_50": "15:29",
    "NIFTY_BANK": "15:29",
    "INDIA_VIX": "15:29",
}


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-root", default="data/market_data_clean")
    p.add_argument("--date", required=True, help="Trading date YYYY-MM-DD")
    p.add_argument("--output-root", default="data/validation_reports")
    return p.parse_args()


def localize_for_checks(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    # Preserve wall-clock values: convert aware values to India time, then drop tz.
    try:
        if dt.dt.tz is not None:
            dt = dt.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except (AttributeError, TypeError):
        pass
    return dt


def main():
    args = parse_args()
    trading_date = pd.Timestamp(args.date).date()
    source_root = Path(args.data_root).expanduser().resolve() / args.date
    output_dir = Path(args.output_root).expanduser().resolve() / args.date
    output_dir.mkdir(parents=True, exist_ok=True)

    if not source_root.exists():
        sys.exit(f"ERROR: source folder not found: {source_root}")

    dataset_rows = []
    instrument_frames = []
    issues = []
    parquet_files = sorted(source_root.rglob("*.parquet"))
    if not parquet_files:
        sys.exit(f"ERROR: no Parquet files found under: {source_root}")

    # Read each dataset folder independently so folder/schema mismatches are visible.
    dataset_dirs = sorted(p for p in source_root.iterdir() if p.is_dir())
    for folder in dataset_dirs:
        dataset = folder.name
        files = sorted(folder.rglob("*.parquet"))
        if not files:
            issues.append({"dataset": dataset, "issue": "NO_PARQUET_FILES", "count": 1, "details": str(folder)})
            continue
        frames = []
        for file in files:
            try:
                frame = pd.read_parquet(file)
            except Exception as exc:
                issues.append({"dataset": dataset, "issue": "PARQUET_READ_ERROR", "count": 1, "details": f"{file}: {exc}"})
                continue
            missing = [c for c in REQUIRED_COLS if c not in frame.columns]
            if missing:
                issues.append({"dataset": dataset, "issue": "MISSING_REQUIRED_COLUMNS", "count": 1, "details": f"{file}: {missing}"})
                continue
            frame["_source_file"] = str(file)
            frame["date"] = localize_for_checks(frame["date"])
            frame["_folder_dataset"] = dataset
            frames.append(frame)
        if not frames:
            continue
        df = pd.concat(frames, ignore_index=True)
        row_count = len(df)
        tokens = df["instrument_token"].nunique(dropna=True)
        valid_dates = df["date"].dropna()
        dataset_rows.append({
            "dataset": dataset,
            "parquet_files": len(files),
            "rows": row_count,
            "unique_instruments": tokens,
            "invalid_timestamps": int(df["date"].isna().sum()),
            "first_timestamp": valid_dates.min() if not valid_dates.empty else pd.NaT,
            "last_timestamp": valid_dates.max() if not valid_dates.empty else pd.NaT,
            "duplicate_instrument_timestamp_rows": int(df.duplicated(["instrument_token", "date"], keep=False).sum()),
        })

        if "research_dataset" in df.columns:
            mismatch = df["research_dataset"].notna() & (df["research_dataset"].astype(str) != dataset)
            if mismatch.any():
                issues.append({"dataset": dataset, "issue": "FOLDER_DATASET_MISMATCH", "count": int(mismatch.sum()), "details": "research_dataset differs from folder name"})

        dup_mask = df.duplicated(["instrument_token", "date"], keep=False)
        if dup_mask.any():
            issues.append({"dataset": dataset, "issue": "DUPLICATE_INSTRUMENT_TIMESTAMP", "count": int(dup_mask.sum()), "details": "Rows share instrument_token + date; source files unchanged"})
        if df["date"].isna().any():
            issues.append({"dataset": dataset, "issue": "INVALID_TIMESTAMP", "count": int(df["date"].isna().sum()), "details": "date could not be parsed"})

        numeric = df[PRICE_COLS].apply(pd.to_numeric, errors="coerce")
        missing_prices = numeric.isna().any(axis=1)
        if missing_prices.any():
            issues.append({"dataset": dataset, "issue": "MISSING_OR_NONNUMERIC_OHLC", "count": int(missing_prices.sum()), "details": "At least one OHLC field is missing/non-numeric"})
        bad_ohlc = (numeric["high"] < numeric[["open", "close", "low"]].max(axis=1)) | (numeric["low"] > numeric[["open", "close", "high"]].min(axis=1))
        if bad_ohlc.fillna(False).any():
            issues.append({"dataset": dataset, "issue": "OHLC_INCONSISTENCY", "count": int(bad_ohlc.fillna(False).sum()), "details": "Expected low <= open/close <= high"})
        if "volume" in df.columns:
            vol = pd.to_numeric(df["volume"], errors="coerce")
            if (vol < 0).fillna(False).any():
                issues.append({"dataset": dataset, "issue": "NEGATIVE_VOLUME", "count": int((vol < 0).sum()), "details": "Negative volume values"})
        if "oi" in df.columns:
            oi = pd.to_numeric(df["oi"], errors="coerce")
            if (oi < 0).fillna(False).any():
                issues.append({"dataset": dataset, "issue": "NEGATIVE_OPEN_INTEREST", "count": int((oi < 0).sum()), "details": "Negative open interest values"})

        # Only flag timestamps outside the known session for mapped datasets.
        if dataset in SESSION_CLOSE and df["date"].notna().any():
            minute = df["date"].dt.strftime("%H:%M")
            outside = (minute < "09:15") | (minute > SESSION_CLOSE[dataset])
            if outside.fillna(False).any():
                issues.append({"dataset": dataset, "issue": "OUTSIDE_EXPECTED_SESSION", "count": int(outside.fillna(False).sum()), "details": f"Expected 09:15 through {SESSION_CLOSE[dataset]} inclusive for this dataset"})

        # Per-instrument coverage is diagnostic only; option candles may be sparse.
        grouped = df.groupby("instrument_token", dropna=False)
        for token, g in grouped:
            dts = g["date"].dropna()
            first = dts.min() if not dts.empty else pd.NaT
            last = dts.max() if not dts.empty else pd.NaT
            row = {
                "dataset": dataset,
                "instrument_token": token,
                "tradingsymbol": g["tradingsymbol"].dropna().iloc[0] if "tradingsymbol" in g and g["tradingsymbol"].notna().any() else "",
                "instrument_type": g["instrument_type"].dropna().iloc[0] if "instrument_type" in g and g["instrument_type"].notna().any() else "",
                "expiry": g["expiry"].dropna().iloc[0] if "expiry" in g and g["expiry"].notna().any() else "",
                "rows": len(g),
                "unique_timestamps": int(dts.nunique()),
                "first_timestamp": first,
                "last_timestamp": last,
                "first_time": first.strftime("%H:%M") if pd.notna(first) else "",
                "last_time": last.strftime("%H:%M") if pd.notna(last) else "",
            }
            instrument_frames.append(row)

    summary_df = pd.DataFrame(dataset_rows)
    instrument_df = pd.DataFrame(instrument_frames)
    issues_df = pd.DataFrame(issues, columns=["dataset", "issue", "count", "details"])

    summary_path = output_dir / "dataset_summary.csv"
    instrument_path = output_dir / "instrument_coverage.csv"
    issues_path = output_dir / "validation_issues.csv"
    summary_df.to_csv(summary_path, index=False)
    instrument_df.to_csv(instrument_path, index=False)
    issues_df.to_csv(issues_path, index=False)
    xlsx_path = output_dir / "validation_report.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Dataset Summary", index=False)
        issues_df.to_excel(writer, sheet_name="Validation Issues", index=False)
        instrument_df.to_excel(writer, sheet_name="Instrument Coverage", index=False)

    print(f"Source: {source_root}")
    print(f"Report: {xlsx_path}")
    print(f"Dataset summary: {summary_path}")
    print(f"Instrument coverage: {instrument_path}")
    print(f"Validation issues: {issues_path}")
    print(f"Datasets scanned: {len(summary_df)}; instruments listed: {len(instrument_df)}; issue types/records: {len(issues_df)}")
    if not issues_df.empty:
        print("\nIssue counts:")
        print(issues_df.groupby("issue", dropna=False)["count"].sum().to_string())
    else:
        print("No validation issues detected by these checks.")
    print("Note: this is a data-quality screening report, not proof of historical market-data completeness.")


if __name__ == "__main__":
    main()
