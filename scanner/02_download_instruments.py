import json
from pathlib import Path
from datetime import date

import pandas as pd
from kiteconnect import KiteConnect


# ============================================================
# CONFIGURATION
# ============================================================

CONFIG_FILE = Path("config.json")
SESSION_FILE = Path("session.json")

RAW_DIR = Path("data/instruments/raw")
RESEARCH_DIR = Path("data/instruments/research")

# Exact Zerodha underlying names we want.
DERIVATIVE_UNDERLYINGS = {
    "NIFTY",
    "BANKNIFTY",
    "SENSEX",
}

# Index instruments we want.
INDEX_SYMBOLS = {
    "NIFTY 50",
    "NIFTY BANK",
    "INDIA VIX",
    "SENSEX",
}

# Derivative instrument types we want.
DERIVATIVE_TYPES = {
    "CE",
    "PE",
    "FUT",
}


# ============================================================
# HELPERS
# ============================================================

def load_config():
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            "config.json not found."
        )

    with CONFIG_FILE.open("r", encoding="utf-8") as f:
        config = json.load(f)

    for key in ("api_key", "api_secret"):
        if not config.get(key) or config[key].startswith("YOUR_"):
            raise ValueError(
                f"Set a real {key} in config.json"
            )

    return config


def load_session():
    if not SESSION_FILE.exists():
        raise FileNotFoundError(
            "session.json not found. Run 01_login.py first."
        )

    with SESSION_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def create_kite():
    config = load_config()
    session = load_session()

    kite = KiteConnect(
        api_key=config["api_key"]
    )

    kite.set_access_token(
        session["access_token"]
    )

    return kite


# ============================================================
# DOWNLOAD COMPLETE INSTRUMENT MASTER
# ============================================================

def download_instruments(kite):

    print()
    print("=" * 60)
    print("ZERODHA INSTRUMENT MASTER DOWNLOAD")
    print("=" * 60)
    print()

    print("Downloading complete Zerodha instrument master...")
    print("This may take a little while.")
    print()

    instruments = kite.instruments()

    df = pd.DataFrame(instruments)

    print(
        f"Total instruments received: {len(df):,}"
    )

    return df


# ============================================================
# SAVE RAW INSTRUMENT MASTER
# ============================================================

def save_raw_instruments(df):

    RAW_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    today = date.today().isoformat()

    output_file = (
        RAW_DIR /
        f"instruments_{today}.csv"
    )

    df.to_csv(
        output_file,
        index=False
    )

    print()
    print("Complete instrument master saved:")
    print(output_file.resolve())

    return output_file


# ============================================================
# BUILD RESEARCH UNIVERSE
# ============================================================

def build_research_universe(df):

    df = df.copy()

    # --------------------------------------------------------
    # 1. INDEX DATA
    # --------------------------------------------------------

    indices = df[
        df["tradingsymbol"].isin(INDEX_SYMBOLS)
        &
        df["segment"].eq("INDICES")
    ].copy()

    indices["underlying_group"] = (
        indices["tradingsymbol"]
    )

    # --------------------------------------------------------
    # 2. DERIVATIVES
    # --------------------------------------------------------

    derivatives = df[
        df["instrument_type"].isin(
            DERIVATIVE_TYPES
        )
        &
        df["name"].isin(
            DERIVATIVE_UNDERLYINGS
        )
    ].copy()

    # IMPORTANT:
    # We intentionally use EXACT `name` matching.
    #
    # Do NOT use:
    # startswith("NIFTY")
    #
    # because that would incorrectly include:
    # NIFTYNXT50
    # NIFTYFPI
    # etc.

    derivatives["underlying_group"] = (
        derivatives["name"]
    )

    # --------------------------------------------------------
    # 3. REMOVE EXPIRED CONTRACTS
    # --------------------------------------------------------

    if "expiry" in derivatives.columns:

        derivatives["expiry"] = pd.to_datetime(
            derivatives["expiry"],
            errors="coerce"
        )

        today = pd.Timestamp.today().normalize()

        derivatives = derivatives[
            derivatives["expiry"].notna()
            &
            (derivatives["expiry"] >= today)
        ].copy()

    # --------------------------------------------------------
    # 4. COMBINE
    # --------------------------------------------------------

    research = pd.concat(
        [
            indices,
            derivatives
        ],
        ignore_index=True
    )

    # --------------------------------------------------------
    # 5. SELECT USEFUL COLUMNS
    # --------------------------------------------------------

    preferred_columns = [
        "instrument_token",
        "exchange_token",
        "tradingsymbol",
        "name",
        "last_price",
        "expiry",
        "strike",
        "tick_size",
        "lot_size",
        "instrument_type",
        "segment",
        "exchange",
        "underlying_group",
    ]

    available_columns = [
        col
        for col in preferred_columns
        if col in research.columns
    ]

    research = research[
        available_columns
    ].copy()

    # --------------------------------------------------------
    # 6. SORT
    # --------------------------------------------------------

    sort_columns = [
        col
        for col in [
            "underlying_group",
            "instrument_type",
            "expiry",
            "strike",
            "tradingsymbol",
        ]
        if col in research.columns
    ]

    research = research.sort_values(
        sort_columns,
        na_position="last"
    ).reset_index(drop=True)

    return research


# ============================================================
# SAVE RESEARCH UNIVERSE
# ============================================================

def save_research_universe(research):

    RESEARCH_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    today = date.today().isoformat()

    dated_file = (
        RESEARCH_DIR /
        f"research_instruments_{today}.csv"
    )

    current_file = (
        RESEARCH_DIR /
        "research_instruments.csv"
    )

    research.to_csv(
        dated_file,
        index=False
    )

    research.to_csv(
        current_file,
        index=False
    )

    print()
    print("Research universe saved:")
    print(dated_file.resolve())

    print()
    print("Current research universe:")
    print(current_file.resolve())

    return current_file


# ============================================================
# PRINT SUMMARY
# ============================================================

def print_summary(research):

    print()
    print("=" * 60)
    print("RESEARCH UNIVERSE SUMMARY")
    print("=" * 60)

    print()
    print("By underlying:")

    print(
        research["underlying_group"]
        .value_counts()
        .sort_index()
    )

    print()
    print("By instrument type:")

    print(
        research["instrument_type"]
        .value_counts()
        .sort_index()
    )

    print()
    print("By exchange:")

    print(
        research["exchange"]
        .value_counts()
        .sort_index()
    )

    print()
    print("By segment:")

    print(
        research["segment"]
        .value_counts()
        .sort_index()
    )

    print()
    print(
        f"Total research instruments: "
        f"{len(research):,}"
    )

    print("=" * 60)


# ============================================================
# MAIN
# ============================================================

def main():

    kite = create_kite()

    # Download complete master
    instruments = download_instruments(kite)

    # Save complete raw master
    save_raw_instruments(instruments)

    # Build clean research universe
    research = build_research_universe(
        instruments
    )

    # Save research universe
    save_research_universe(
        research
    )

    # Print summary
    print_summary(
        research
    )

    print()
    print(
        "STEP 2 COMPLETED SUCCESSFULLY."
    )
    print()


if __name__ == "__main__":
    main()