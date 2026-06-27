# =====================================================
# SETTINGS
# =====================================================

URL = "https://dps.psx.com.pk/indices"
LOOP_INTERVAL = 10
LOOP_COUNT = 5
TIMEZONE = ZoneInfo("Asia/Karachi")
DEDUP_COLS = ["Index", "last_update"]  # <-- NEW

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

# =====================================================
# SESSION WITH RETRY
# =====================================================

def create_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    
    return session

# =====================================================
# FUNCTION (with numeric conversion)
# =====================================================

def fetch_indices(session):
    response = session.get(URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    match = re.search(
        r"As of\s+([\w\s,:\dAPM]+?)(?:\s*$|\s*Index)",
        page_text,
        re.IGNORECASE | re.MULTILINE
    )
    last_update = match.group(1).strip() if match else None

    tables = pd.read_html(StringIO(response.text))
    
    indices_df = None
    for df in tables:
        cols = [str(c).strip() for c in df.columns]
        if {"Index", "High", "Low", "Current"}.issubset(set(cols)):
            indices_df = df.copy()
            break

    if indices_df is None:
        raise Exception("Indices table not found")

    # Clean
    indices_df = indices_df[
        ~indices_df["Index"].astype(str).str.contains(
            "Sector Indices", case=False, na=False
        )
    ]

    indices_df["Index"] = (
        indices_df["Index"]
        .astype(str)
        .str.replace(r"\s*\(.*?\)", "", regex=True)
        .str.strip()
    )

    # Convert numeric columns  <-- NEW
    for col in indices_df.columns:
        if col not in ["Index"]:
            indices_df[col] = pd.to_numeric(
                indices_df[col].astype(str).str.replace(",", ""),
                errors="coerce"
            )

    indices_df["last_update"] = last_update
    indices_df["scraped_at"] = pd.Timestamp.now(tz=TIMEZONE)

    return indices_df

# =====================================================
# LOOP (with deduplication)
# =====================================================

session = create_session()

for run_no in range(1, LOOP_COUNT + 1):
    try:
        print(f"\nRun {run_no}/{LOOP_COUNT}")
        new_df = fetch_indices(session)

        if OUTPUT_FILE.exists():
            old_df = pd.read_excel(OUTPUT_FILE, engine="openpyxl")
            combined_df = pd.concat([old_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=DEDUP_COLS, keep="last"  # <-- NEW
            )
        else:
            combined_df = new_df

        combined_df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")

        print(f"✓ Saved: {OUTPUT_FILE}")
        print(f"  Added: {len(new_df)} rows")
        print(f"  Total: {len(combined_df)} rows (after dedup)")
        print(f"  Last Update: {new_df['last_update'].iloc[0]}")

    except Exception as e:
        print(f"✗ ERROR: {e}")

    if run_no < LOOP_COUNT:
        print(f"⏳ Waiting {LOOP_INTERVAL}s...")
        time.sleep(LOOP_INTERVAL)

print("\n✓ Completed.")
