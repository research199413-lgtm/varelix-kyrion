import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
from pathlib import Path
import re
import time

# =====================================================
# SETTINGS
# =====================================================

URL = "https://dps.psx.com.pk/indices"
OUTPUT_FILE = "psx_indices_history.xlsx"

LOOP_INTERVAL = 10   # seconds
LOOP_COUNT = 5       # number of runs

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# =====================================================
# FETCH FUNCTION
# =====================================================

def fetch_indices():

    response = requests.get(URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    # Extract "As of" timestamp
    match = re.search(
        r"As of\s+([A-Za-z]{3}\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s+[AP]M)",
        page_text
    )

    last_update = match.group(1) if match else None

    # Read tables
    tables = pd.read_html(StringIO(response.text))

    indices_df = None

    for df in tables:
        cols = [str(c).strip() for c in df.columns]

        if (
            "Index" in cols and
            "High" in cols and
            "Low" in cols and
            "Current" in cols
        ):
            indices_df = df.copy()
            break

    if indices_df is None:
        raise Exception("Indices table not found")

    # Remove Sector Indices row
    indices_df = indices_df[
        ~indices_df["Index"].astype(str).str.contains(
            "Sector Indices",
            case=False,
            na=False
        )
    ]

    # Clean Index name (remove brackets like dates)
    indices_df["Index"] = (
        indices_df["Index"]
        .astype(str)
        .str.replace(r"\s*\(.*?\)", "", regex=True)
        .str.strip()
    )

    # Standardize column names if needed
    indices_df.rename(columns={
        "% Change": "% Change"
    }, inplace=True)

    # Add timestamps
    indices_df["last_update"] = last_update
    indices_df["scraped_at"] = pd.Timestamp.now()

    return indices_df


# =====================================================
# MAIN LOOP
# =====================================================

for run_no in range(1, LOOP_COUNT + 1):

    try:
        print(f"\nRun {run_no}/{LOOP_COUNT}")

        new_df = fetch_indices()

        output_path = Path(OUTPUT_FILE)

        # Append old data if exists
        if output_path.exists():
            old_df = pd.read_excel(output_path, engine="openpyxl")

            combined_df = pd.concat([old_df, new_df], ignore_index=True)

            # Remove duplicates
            combined_df = combined_df.drop_duplicates(
                subset=["Index", "last_update"],
                keep="last"
            )
        else:
            combined_df = new_df

        # Save
        combined_df.to_excel(
            OUTPUT_FILE,
            index=False,
            engine="openpyxl"
        )

        print(
            f"Added {len(new_df)} rows | "
            f"Total rows: {len(combined_df)}"
        )

    except Exception as e:
        print("ERROR:", e)

    if run_no < LOOP_COUNT:
        print(f"Sleeping {LOOP_INTERVAL} seconds...")
        time.sleep(LOOP_INTERVAL)

print("\nCompleted.")
