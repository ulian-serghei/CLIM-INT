"""
Download and filter European flight trajectories from the OpenSky Network
Zenodo dataset (DOI: 10.5281/zenodo.3931948).

Downloads monthly flightlist CSV files, filters for European flights,
and samples a target number of flights per month.

Usage:
    python src/data/select_opensky_flights.py
"""

import gzip
import io
import random
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Months to download: (year, month) — 2 per year, spread across 2019-2024
TARGET_MONTHS = [
    (2019, 1), (2019, 7),
    (2020, 4), (2020, 10),
    (2021, 1), (2021, 7),
    (2022, 4), (2022, 10),
    (2023, 1), (2023, 7),
    (2024, 4), (2024, 7),
]

# Target flights to sample per month
FLIGHTS_PER_MONTH = 35  # 35 x 12 = 420 total, within 200-500 range

# Zenodo base URL
ZENODO_BASE = "https://zenodo.org/record/3931948/files"

# European ICAO airport prefixes (first 2 characters of ICAO code)
EUROPEAN_ICAO_PREFIXES = (
    'EG', 'EI', 'EK', 'EN', 'EP', 'ES', 'EY',  # UK, Ireland, Denmark, Norway, Poland, Sweden, Baltics
    'LF', 'LI', 'LK', 'LO', 'LP', 'LS', 'LZ',  # France, Italy, Czech, Austria, Portugal, Switzerland, Slovakia
    'LD', 'LB', 'LH', 'LG', 'LR', 'LT', 'LU',  # Croatia, Bulgaria, Hungary, Greece, Romania, Turkey, Moldova
    'LW', 'ED', 'EB', 'EL', 'EH',               # Macedonia, Germany, Belgium, Luxembourg, Netherlands
    'BK', 'BI', 'LY', 'LJ', 'LQ', 'LA',         # Kosovo, Iceland, Serbia, Slovenia, Bosnia, Albania
)

RAW_DIR = Path('data/raw/opensky')
RAW_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_european(icao_code: str) -> bool:
    """Return True if the ICAO airport code belongs to a European airport."""
    if not isinstance(icao_code, str) or len(icao_code) < 2:
        return False
    return icao_code[:2].upper() in EUROPEAN_ICAO_PREFIXES


def get_zenodo_filename(year: int, month: int) -> str:
    """Return the expected Zenodo filename for a given year/month."""
    # Format: flightlist_YYYYMM01_YYYYMM28/30/31.csv.gz
    # Zenodo files use first and last day of month
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    return f"flightlist_{year}{month:02d}01_{year}{month:02d}{last_day:02d}.csv.gz"


def download_file(url: str, dest: Path) -> bool:
    """Download a file from URL to dest with a progress bar. Returns True on success."""
    try:
        response = requests.get(url, stream=True, timeout=60)
        if response.status_code != 200:
            print(f"  HTTP {response.status_code} — file may not exist for this month")
            return False

        total = int(response.headers.get('content-length', 0))
        with open(dest, 'wb') as f, tqdm(
            desc=dest.name, total=total, unit='B',
            unit_scale=True, unit_divisor=1024
        ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))
        return True
    except Exception as e:
        print(f"  Download error: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_month(year: int, month: int) -> pd.DataFrame:
    """Download, filter, and sample European flights for one month."""
    filename = get_zenodo_filename(year, month)
    local_gz = RAW_DIR / filename
    url = f"{ZENODO_BASE}/{filename}?download=1"

    # Download if not already present
    if not local_gz.exists():
        print(f"\n[{year}-{month:02d}] Downloading {filename}...")
        success = download_file(url, local_gz)
        if not success:
            print(f"  Skipping {year}-{month:02d}")
            return pd.DataFrame()
    else:
        print(f"\n[{year}-{month:02d}] Already downloaded, loading...")

    # Load and filter
    print(f"  Filtering for European flights...")
    with gzip.open(local_gz, 'rb') as f:
        df = pd.read_csv(io.BytesIO(f.read()))

    print(f"  Total flights in file: {len(df):,}")

    # Filter: both origin AND destination must be European
    eu_mask = (
        df['origin'].apply(is_european) &
        df['destination'].apply(is_european)
    )
    df_eu = df[eu_mask].copy()
    print(f"  European flights: {len(df_eu):,}")

    # Drop rows with missing critical fields
    df_eu = df_eu.dropna(subset=['callsign', 'origin', 'destination', 'typecode'])
    print(f"  After dropping missing fields: {len(df_eu):,}")

    if len(df_eu) == 0:
        return pd.DataFrame()

    # Sample up to FLIGHTS_PER_MONTH
    n_sample = min(FLIGHTS_PER_MONTH, len(df_eu))
    df_sample = df_eu.sample(n=n_sample, random_state=42)
    df_sample['source_year'] = year
    df_sample['source_month'] = month
    print(f"  Sampled: {n_sample} flights")

    return df_sample


def main():
    random.seed(42)
    all_flights = []

    for year, month in TARGET_MONTHS:
        df = process_month(year, month)
        if not df.empty:
            all_flights.append(df)

    if not all_flights:
        print("\nNo flights collected. Check your internet connection or Zenodo availability.")
        return

    # Combine all months
    combined = pd.concat(all_flights, ignore_index=True)
    print(f"\n{'='*50}")
    print(f"Total flights collected: {len(combined)}")
    print(f"Years covered: {sorted(combined['source_year'].unique())}")
    print(f"Aircraft types: {combined['typecode'].nunique()} unique types")
    print(f"Routes: {combined['origin'].nunique()} origins, {combined['destination'].nunique()} destinations")

    # Save curated dataset
    output_file = Path('data/raw/opensky/opensky_european_curated.csv')
    combined.to_csv(output_file, index=False)
    print(f"\nSaved curated dataset to: {output_file}")

    # Save summary
    summary = combined.groupby(['source_year', 'source_month']).size().reset_index(name='flight_count')
    summary_file = Path('data/raw/opensky/opensky_sampling_summary.csv')
    summary.to_csv(summary_file, index=False)
    print(f"Saved sampling summary to: {summary_file}")


if __name__ == '__main__':
    main()
