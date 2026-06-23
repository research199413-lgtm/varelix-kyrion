from pathlib import Path

URL = "https://dps.psx.com.pk/indices"

OUTPUT_FILE = Path("data/psx_indices_history.xlsx")

LOOP_INTERVAL = 300   # seconds (only used if running locally)
LOOP_COUNT = 60

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36"
    )
}
