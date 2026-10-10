"""Export a completed Parquet download into Excel-friendly workbooks.

The Parquet files remain the authoritative research dataset. This script creates
one XLSX workbook per dataset, splitting data over sheets to respect Excel limits,
plus an audit workbook. It does not alter or delete Parquet files.
"""

import argparse
import json
from pathlib import Path

import pandas as pd

DEFAULT_DATA_ROOT = Path("data/market_data_clean")
DEFAULT_INSTRUMENT_FILE = Path("data/instruments/research/research_instruments.csv")
EXCEL_MAX_DATA_ROWS_PER_SHEET = 1_000_000  # Excel's hard limit is 1,048,575 data rows + header


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT,
                        help="Download root containing YYYY-MM-DD/<DATASET>/*.parquet")
    parser.add_argument("--date", default="2026-10-08", help="Target date in YYYY-MM-DD format")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="Where to save Excel files (default: data/excel_exports/<date>)")
    parser.add_argument("--max-rows-per-sheet", type=int, default=EXCEL_MAX_DATA_ROWS_PER_SHEET,
                        help="Useful for testing; must be between 1 and Excel's row limit minus header")
    return parser.parse_args()


def read_dataset(folder: Path, dataset_name: str):
    files = sorted(folder.glob("part_*.parquet"))
    frames = []
    warnings = []
    raw_rows = 0

    for path in files:
        try:
            frame = pd.read_parquet(path)
        except Exception as exc:
            warnings.append(f"Could not read {path}: {type(exc).__name__}: {exc}")
            continue
        raw_rows += len(frame)
        if "research_dataset" in frame.columns:
            wrong = frame[frame["research_dataset"].astype(str) != dataset_name]
            if not wrong.empty:
                warnings.append(
                    f"{path.name}: skipped {len(wrong):,} row(s) whose research_dataset "
                    f"does not match folder {dataset_name!r}."
                )
                frame = frame[frame["research_dataset"].astype(str) == dataset_name]
        else:
            warnings.append(f"{path.name}: missing research_dataset column; folder name used as dataset identity.")
            frame["research_dataset"] = dataset_name
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return pd.DataFrame(), len(files), raw_rows, 0, warnings

    combined = pd.concat(frames, ignore_index=True)
    before = len(combined)
    duplicate_rows = 0
    if {"instrument_token", "date"}.issubset(combined.columns):
        # For a candle dataset, one instrument should have at most one row per timestamp.
        duplicate_mask = combined.duplicated(subset=["instrument_token", "date"], keep="last")
        duplicate_rows = int(duplicate_mask.sum())
        if duplicate_rows:
            warnings.append(
                f"{dataset_name}: found {duplicate_rows:,} duplicate instrument/date candle row(s); "
                "kept the last row for the Excel export only. Parquet source files were not changed."
            )
            combined = combined.loc[~duplicate_mask].copy()
    combined = combined.sort_values(
        [column for column in ("instrument_token", "date") if column in combined.columns],
        kind="stable",
    ).reset_index(drop=True)
    return combined, len(files), raw_rows, duplicate_rows, warnings


def make_excel_compatible(frame: pd.DataFrame) -> pd.DataFrame:
    """Return an export-only copy with timezone-aware datetimes made Excel-compatible.

    Timezone metadata is removed while preserving the displayed local wall time.
    The source DataFrame and Parquet files are not modified.
    """
    compatible = frame.copy()
    for column in compatible.columns:
        dtype = compatible[column].dtype
        if isinstance(dtype, pd.DatetimeTZDtype):
            compatible[column] = compatible[column].dt.tz_localize(None)
    return compatible


def write_workbook(frame: pd.DataFrame, path: Path, max_rows_per_sheet: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    # Empty datasets are reported in the audit workbook rather than creating empty XLSX files.
    if frame.empty:
        return 0

    # Excel cannot store timezone-aware datetimes. Strip timezone metadata on an
    # export-only copy; do not convert timestamps or modify the source Parquet.
    excel_frame = make_excel_compatible(frame)

    # Keep sheet names short and predictable. Excel permits at most 1,048,576 rows including header.
    sheet_count = 0
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for start in range(0, len(excel_frame), max_rows_per_sheet):
            sheet_count += 1
            chunk = excel_frame.iloc[start:start + max_rows_per_sheet]
            chunk.to_excel(writer, sheet_name=f"Candles_{sheet_count:03d}", index=False)
    return sheet_count


def export_checkpoint_status(data_root: Path, target_date: str, output_dir: Path, audit_rows):
    checkpoint = data_root / target_date / "checkpoint.json"
    status_path = output_dir / "instrument_status.xlsx"
    # This workbook is generated by this exporter, so rebuild it cleanly on each run.
    if status_path.exists():
        status_path.unlink()
    if not checkpoint.exists():
        return f"Checkpoint not found: {checkpoint}"
    try:
        with checkpoint.open("r", encoding="utf-8") as handle:
            cp = json.load(handle)
    except Exception as exc:
        return f"Could not read checkpoint {checkpoint}: {type(exc).__name__}: {exc}"

    try:
        instruments = pd.read_csv(DEFAULT_INSTRUMENT_FILE)
        if "instrument_token" in instruments.columns:
            instruments["instrument_token"] = pd.to_numeric(instruments["instrument_token"], errors="coerce")
            instruments = instruments[instruments["instrument_token"].notna()].copy()
            instruments["instrument_token"] = instruments["instrument_token"].astype("int64")
        else:
            instruments = pd.DataFrame()
    except Exception:
        instruments = pd.DataFrame()

    for field, status in (("no_data_tokens", "NO_DATA"),
                          ("invalid_tokens", "INVALID_TOKEN"),
                          ("failed_tokens", "FAILED_RETRYABLE")):
        tokens = {int(x) for x in cp.get(field, [])}
        if not tokens:
            continue
        status_frame = pd.DataFrame({"instrument_token": sorted(tokens), "download_status": status})
        if not instruments.empty and "instrument_token" in instruments.columns:
            status_frame = status_frame.merge(instruments, on="instrument_token", how="left")
        audit_rows.append({
            "dataset": "INSTRUMENT_STATUS",
            "parquet_files": 0,
            "raw_rows": 0,
            "export_rows": len(status_frame),
            "unique_instruments": len(tokens),
            "duplicate_rows_removed_for_export": 0,
            "excel_file": str(status_path),
            "notes": f"Checkpoint status: {status}",
        })
        mode = "a" if status_path.exists() else "w"
        with pd.ExcelWriter(status_path, engine="openpyxl", mode=mode) as writer:
            sheet = status[:31]
            status_frame.to_excel(writer, sheet_name=sheet, index=False)
    return None


def main():
    args = parse_args()
    if not (1 <= args.max_rows_per_sheet <= 1_048_575):
        raise ValueError("--max-rows-per-sheet must be between 1 and 1,048,575")

    target_folder = args.data_root / args.date
    if not target_folder.exists():
        raise FileNotFoundError(f"No download folder found: {target_folder.resolve()}")
    output_dir = args.output_dir or Path("data/excel_exports") / args.date
    output_dir.mkdir(parents=True, exist_ok=True)

    audit_rows = []
    all_warnings = []
    dataset_folders = sorted(path for path in target_folder.iterdir() if path.is_dir())
    if not dataset_folders:
        raise RuntimeError(f"No dataset folders found under {target_folder.resolve()}")

    print(f"Source: {target_folder.resolve()}")
    print(f"Excel output: {output_dir.resolve()}")
    print("Parquet files will not be modified.\n")

    for folder in dataset_folders:
        dataset = folder.name
        parquet_files = list(folder.glob("part_*.parquet"))
        if not parquet_files:
            continue
        print(f"Preparing {dataset}: {len(parquet_files)} Parquet file(s)...")
        frame, file_count, raw_rows, duplicates_removed, warnings = read_dataset(folder, dataset)
        all_warnings.extend(warnings)
        xlsx_path = output_dir / f"{dataset}.xlsx"
        sheet_count = write_workbook(frame, xlsx_path, args.max_rows_per_sheet)
        unique_instruments = int(frame["instrument_token"].nunique()) if not frame.empty and "instrument_token" in frame.columns else 0
        notes = f"{sheet_count} sheet(s)" if not frame.empty else "No rows available to export"
        audit_rows.append({
            "dataset": dataset,
            "parquet_files": file_count,
            "raw_rows": raw_rows,
            "export_rows": len(frame),
            "unique_instruments": unique_instruments,
            "duplicate_rows_removed_for_export": duplicates_removed,
            "excel_file": str(xlsx_path) if not frame.empty else "",
            "notes": notes,
        })
        print(f"  Rows: {len(frame):,}; instruments: {unique_instruments:,}; sheets: {sheet_count}")

    status_warning = export_checkpoint_status(args.data_root, args.date, output_dir, audit_rows)
    if status_warning:
        all_warnings.append(status_warning)

    audit_df = pd.DataFrame(audit_rows)
    audit_path = output_dir / "export_audit.xlsx"
    with pd.ExcelWriter(audit_path, engine="openpyxl") as writer:
        audit_df.to_excel(writer, sheet_name="Export Summary", index=False)
        pd.DataFrame({"warning": all_warnings or ["No export warnings."]}).to_excel(
            writer, sheet_name="Warnings", index=False
        )

    print("\nExport complete.")
    print(f"Audit workbook: {audit_path.resolve()}")
    print(f"Instrument status workbook (if checkpoint statuses existed): {(output_dir / 'instrument_status.xlsx').resolve()}")
    print(f"Warnings: {len(all_warnings)}")
    if all_warnings:
        print("Review the Warnings sheet in export_audit.xlsx before using the Excel exports.")


if __name__ == "__main__":
    main()
