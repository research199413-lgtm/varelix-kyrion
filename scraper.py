import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
from pathlib import Path
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

# =====================================================
# SETTINGS
# =====================================================

URL = "https://dps.psx.com.pk/indices"

LOOP_INTERVAL = 300      # seconds
LOOP_COUNT = 60        # Number of runs

TIMEZONE = ZoneInfo("Asia/Karachi")

# Headers to avoid being blocked
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36"
    )
}

# =====================================================
# OUTPUT FILE (Year / Month / Daily Excel)
# =====================================================

today = datetime.now(TIMEZONE)

OUTPUT_DIR = (
    Path("data")
    / str(today.year)
    / f"{today.month:02d}"
)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / f"{today:%Y-%m-%d}.xlsx"

# =====================================================
# FUNCTION
# =====================================================

def fetch_indices():

    response = requests.get(
        URL,
        headers=HEADERS,
        timeout=30
    )
    response.raise_for_status()

    # -----------------------------------------
    # Extract timestamp
    # -----------------------------------------

    soup = BeautifulSoup(response.text, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    match = re.search(
        r"As of\s+([A-Za-z]{3}\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s+[AP]M)",
        page_text
    )

    last_update = match.group(1) if match else None

    # -----------------------------------------
    # Extract table
    # -----------------------------------------

    tables = pd.read_html(StringIO(response.text))

    indices_df = None

    for df in tables:

        cols = [str(c).strip() for c in df.columns]

        if (
            "Index" in cols
            and "High" in cols
            and "Low" in cols
            and "Current" in cols
        ):
            indices_df = df.copy()
            break

    if indices_df is None:
        raise Exception("Indices table not found")

    # -----------------------------------------
    # Remove Sector Indices row
    # -----------------------------------------

    indices_df = indices_df[
        ~indices_df["Index"].astype(str).str.contains(
            "Sector Indices",
            case=False,
            na=False
        )
    ]

    # -----------------------------------------
    # Clean index names
    # HBLTTI (18-06-2026 18:30:00) -> HBLTTI
    # -----------------------------------------

    indices_df["Index"] = (
        indices_df["Index"]
        .astype(str)
        .str.replace(r"\s*\(.*?\)", "", regex=True)
        .str.strip()
    )

    # -----------------------------------------
    # Add timestamps
    # -----------------------------------------

    indices_df["last_update"] = last_update
    indices_df["scraped_at"] = pd.Timestamp.now(tz=TIMEZONE)

    return indices_df


# =====================================================
# LOOP
# =====================================================

for run_no in range(1, LOOP_COUNT + 1):

    try:

        print(f"\nRun {run_no}/{LOOP_COUNT}")

        new_df = fetch_indices()

        # -------------------------------------
        # Append data to today's Excel file
        # -------------------------------------

        if OUTPUT_FILE.exists():

            old_df = pd.read_excel(
                OUTPUT_FILE,
                engine="openpyxl"
            )

            combined_df = pd.concat(
                [old_df, new_df],
                ignore_index=True
            )

        else:

            combined_df = new_df

        combined_df.to_excel(
            OUTPUT_FILE,
            index=False,
            engine="openpyxl"
        )

        print(f"Saved to: {OUTPUT_FILE}")
        print(f"Added {len(new_df)} rows")
        print(f"Total rows: {len(combined_df)}")
        print(f"Last Update: {new_df['last_update'].iloc[0]}")

    except Exception as e:

        print("ERROR:", e)

    # -----------------------------------------
    # Wait before next run
    # -----------------------------------------

    if run_no < LOOP_COUNT:

        print(f"Sleeping {LOOP_INTERVAL} seconds...")
        time.sleep(LOOP_INTERVAL)

print("\nCompleted.")
