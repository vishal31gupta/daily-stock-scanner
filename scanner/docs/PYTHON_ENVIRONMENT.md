# Python Environment and Setup

**Reviewed:** 2026-10-20  
**Status:** Proposed baseline; package imports and pipeline behavior have not been runtime-tested in this checkout.

## Recommended Python version

Use the latest 64-bit CPython 3.13 patch release. Python 3.13 is in its bug-fix support period through October 2029. The selected NumPy, pandas, DuckDB, and PyArrow releases publish CPython 3.13 wheels; Kite Connect publishes a universal Python 3 wheel. These package artifacts establish interpreter/platform availability, not that the full pipeline has been tested against the pins below.

Python is not currently available in the shell used for this audit (`python`, `python3`, and `py` were not found). No Python or package was installed. Install CPython from [python.org](https://www.python.org/downloads/windows/) when approved, and enable the Python launcher or add the interpreter to `PATH`.

## Requirements found in the scripts

| Package | Used by | Purpose |
| --- | --- | --- |
| `kiteconnect` | `01_login.py` through `03_eod_downloader.py` | Kite authentication, instruments, and candle API. |
| `pandas` | `02_download_instruments.py` through `07_import_historical_nifty_options.py` | DataFrame handling, CSV/Excel, and Parquet access. |
| `numpy` | `03_eod_downloader.py` | Numeric validation and array conversion. |
| `pyarrow` | Scripts 03-07 | Explicit Parquet engine for pandas Parquet reads/writes. |
| `duckdb` | `06_import_parquet_to_duckdb.py`, `07_import_historical_nifty_options.py` | Local analytical database access. |
| `openpyxl` | Scripts 04 and 05; `.xlsx` input in script 07 | Excel workbook export and `.xlsx` support. |
| `xlrd` | Script 07, only when reading legacy `.xls` input | pandas engine for `.xls`; it does not read `.xlsx`. |
| `tzdata` | Windows environment | Provides a packaged IANA time-zone database fallback for reproducible timezone conversion. |

Other imports seen in the seven scripts are from Python's standard library. Kite Connect's own dependencies are resolved by pip from its package metadata; this file pins direct project dependencies, not every transitive package.

## Proposed direct dependency pins

`requirements.txt` pins the current stable releases reviewed for CPython 3.13 on 2026-10-20. All entries are **not runtime-tested here** because Python is unavailable. They are proposed package versions, not a claim that a complete install or pipeline run has succeeded. In particular, pandas 3.x and newer DuckDB/PyArrow behavior still needs an import and small, non-destructive compatibility check before operational use.

| Package | Proposed version | Notes |
| --- | --- | --- |
| `numpy` | `2.5.4` | Directly imported by the downloader. |
| `pandas` | `3.0.6` | Used for CSV, Parquet, and Excel processing. |
| `kiteconnect` | `5.2.2` | Current official Kite Connect Python client release reviewed. |
| `duckdb` | `1.5.6` | Used by both database importer scripts. |
| `pyarrow` | `26.0.0` | Chosen as the pandas Parquet engine. |
| `openpyxl` | `3.1.5` | Excel `.xlsx` engine used by the scripts. |
| `xlrd` | `2.0.2` | Required only to support `.xls` vendor workbooks. |
| `tzdata` | `2026.5` | Explicit timezone-data fallback for Windows. |

The pins make the direct dependencies repeatable, while transitive dependencies can still resolve to newer compatible releases. A fully frozen environment would require generating and reviewing a platform-specific lock after the proposed environment can be installed and checked.

## Create the virtual environment

Run commands from the repository's `scanner` directory. The project scripts read `config.json` and `session.json` relative to the current working directory, and most data paths are also relative to it.

```powershell
Set-Location .\scanner
py -3.13 --version
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r ..\requirements.txt
```

The virtual environment is local to `scanner/.venv`; the existing root `.gitignore` ignores virtual environments. If the Python launcher is unavailable after Python is installed, use the installed interpreter's full path in place of `py -3.13`.

Check the environment without calling the Kite API or reading market data:

```powershell
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -c "from importlib.metadata import version; import numpy, pandas, kiteconnect, duckdb, pyarrow, openpyxl, xlrd, tzdata; print({name: version(name) for name in ('numpy','pandas','kiteconnect','duckdb','pyarrow','openpyxl','xlrd','tzdata')})"
```

These are suggested verification commands; none could be run in the current shell. Run the import/version check before running any pipeline script.

## Local configuration and credential handling

Create `scanner/config.json` locally with the two keys `api_key` and `api_secret`. Scripts 01-03 expect this file in the current working directory. Script 01 writes `scanner/session.json`; scripts 02-03 read its `access_token`. These scripts do not currently read environment variables for credentials.

Keep both files local. Do not put credentials in source code, shell commands, notebooks, screenshots, logs, or documentation. Do not display the contents of either file. The Priority 1 rules in the root `.gitignore` ignore `scanner/config.json` and `scanner/session.json`; ignored files remain local and are not automatically removed from Git if they were ever tracked.

## Running the pipeline

Run scripts from `scanner` using the virtual-environment interpreter. The normal collection sequence is login, instrument download, then candle download. The API scripts require a valid local session and make network requests. Review their arguments, dates, and output paths before invoking them.

```powershell
.\.venv\Scripts\python.exe 01_login.py
.\.venv\Scripts\python.exe 02_download_instruments.py
.\.venv\Scripts\python.exe 03_eod_downloader.py --date YYYY-MM-DD --fresh
```

Validation and Excel export also write generated files under `scanner/data/`:

```powershell
.\.venv\Scripts\python.exe 05_validate_market_data.py --date YYYY-MM-DD
.\.venv\Scripts\python.exe 04_export_market_data_to_excel.py --date YYYY-MM-DD
```

Scripts 06 and 07 modify DuckDB. Do not use them as environment checks. Their default database paths are tied to a hard-coded Windows project root in this checkout; script 06 accepts `--data-root` and `--db-path`, while script 07 accepts `--db-path`. On another machine, review those arguments and the target database before running either importer. Back up the database before any import or migration work.

## Sources reviewed

- [Python supported versions](https://devguide.python.org/versions/)
- [NumPy 2.5.4 on PyPI](https://pypi.org/project/numpy/2.5.4/)
- [pandas 3.0.6 on PyPI](https://pypi.org/project/pandas/3.0.6/)
- [Kite Connect 5.2.2 on PyPI](https://pypi.org/project/kiteconnect/5.2.2/) and [official release notes](https://github.com/zerodha/pykiteconnect/releases)
- [DuckDB 1.5.6 on PyPI](https://pypi.org/project/duckdb/1.5.6/) and [Python API documentation](https://duckdb.org/docs/stable/clients/python/overview)
- [PyArrow 26.0.0 on PyPI](https://pypi.org/project/pyarrow/26.0.0/)
- [openpyxl 3.1.5 on PyPI](https://pypi.org/project/openpyxl/3.1.5/)
- [xlrd 2.0.2 on PyPI](https://pypi.org/project/xlrd/2.0.2/)
- [tzdata 2026.5 on PyPI](https://pypi.org/project/tzdata/2026.5/)
