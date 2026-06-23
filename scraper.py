import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
import re
import time
import logging
from pathlib import Path

import config


# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# =====================================================
# FETCH DATA
# =====================================================
def fetch_indices():

    response = requests.get(
        config.URL,
        headers=config.HEADERS,
        timeout=30
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    match = re.search(
        r"As of\s+([A-Za-z]{3}\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s+[AP]M)",
        page_text
    )

    last_update = match.group(1) if match else None

    tables = pd.read_html(StringIO(response.text))

    indices_df = None

    for df in tables:
        cols = [str(c).strip() for c in df.columns]

        if {"Index", "High", "Low", "Current"}.issubset(cols):
            indices_df = df.copy()
            break

    if indices_df is None:
        raise ValueError("Indices table not found")

    # remove sector indices
    indices_df = indices_df[
        ~indices_df["Index"].astype(str).str.contains(
            "Sector Indices",
            case=False,
            na=False
        )
    ]

    # clean index name
    indices_df["Index"] = (
        indices_df["Index"]
        .astype(str)
        .str.replace(r"\s*\(.*?\)", "", regex=True)
        .str.strip()
    )

    # timestamps
    indices_df["last_update"] = last_update
    indices_df["scraped_at"] = pd.Timestamp.now()

    return indices_df


# =====================================================
# SAVE DATA
# =====================================================
def save_data(new_df: pd.DataFrame):

    output_path = config.OUTPUT_FILE
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        old_df = pd.read_excel(output_path, engine="openpyxl")

        combined = pd.concat([old_df, new_df], ignore_index=True)

        combined = combined.drop_duplicates(
            subset=["Index", "last_update"],
            keep="last"
        )
    else:
        combined = new_df

    combined.to_excel(output_path, index=False, engine="openpyxl")

    logging.info(
        f"Saved {len(new_df)} rows | Total: {len(combined)}"
    )


# =====================================================
# MAIN RUN LOOP (LOCAL USE ONLY)
# =====================================================
def run_local():

    for i in range(1, config.LOOP_COUNT + 1):

        try:
            logging.info(f"Run {i}/{config.LOOP_COUNT}")

            df = fetch_indices()
            save_data(df)

        except Exception as e:
            logging.error(f"Error: {e}")

        if i < config.LOOP_COUNT:
            logging.info(f"Sleeping {config.LOOP_INTERVAL}s")
            time.sleep(config.LOOP_INTERVAL)

    logging.info("Completed.")


# =====================================================
# ENTRY POINT
# =====================================================
if __name__ == "__main__":
    run_local()