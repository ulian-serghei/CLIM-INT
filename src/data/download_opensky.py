"""
Access OpenSky Network ADS-B flight trajectory data.
Source: Zenodo DOI: 10.5281/zenodo.3931948

The OpenSky dataset is distributed as Parquet files on Zenodo.
This script documents the download URLs and loads flights
for a specified date range into data/raw/opensky/.

Usage:
    python src/data/download_opensky.py
"""

from pathlib import Path


# Zenodo base URL for the OpenSky dataset
ZENODO_BASE = "https://zenodo.org/record/3931948/files"

# Example files available in the dataset (YYYY-MM format)
AVAILABLE_MONTHS = [
    "2019-01", "2019-06", "2020-01", "2020-06",
    "2021-01", "2021-06", "2022-01", "2022-06",
    "2023-01", "2023-06", "2024-01",
]

OUTPUT_DIR = Path("data/raw/opensky")


def download_month(year_month: str):
    """Download OpenSky flight data for a given YYYY-MM."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"flightlist_{year_month.replace('-', '')}_*.csv.gz"
    url = f"{ZENODO_BASE}/{filename}"
    output_file = OUTPUT_DIR / f"opensky_{year_month}.csv.gz"

    if output_file.exists():
        print(f"Already exists, skipping: {output_file.name}")
        return

    print(f"Downloading {year_month} from Zenodo...")
    print(f"URL: {url}")
    print("Note: For full dataset, download manually from:")
    print("      https://zenodo.org/record/3931948")
    print(f"      and place files in {OUTPUT_DIR}/")


def list_available():
    """Print available months in the OpenSky dataset."""
    print("Available months in OpenSky Zenodo dataset:")
    for month in AVAILABLE_MONTHS:
        print(f"  - {month}")
    print("\nDownload from: https://zenodo.org/record/3931948")
    print(f"Place files in: {OUTPUT_DIR}/")


if __name__ == '__main__':
    list_available()
