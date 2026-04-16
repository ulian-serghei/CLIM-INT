"""
Download ERA5 reanalysis data from Copernicus CDS.
Downloads temperature, specific humidity, and wind components
at pressure levels relevant for aviation (150-300 hPa).

Usage:
    python src/data/download_era5.py --year 2022 --month 6
    python src/data/download_era5.py --year 2022 --month 6 --ensemble
"""

import argparse
import cdsapi
from pathlib import Path


# Pressure levels relevant for cruising altitude (FL250-FL410)
PRESSURE_LEVELS = [
    '150', '175', '200', '225', '250', '275', '300'
]

VARIABLES = [
    'temperature',
    'specific_humidity',
    'u_component_of_wind',
    'v_component_of_wind',
    'fraction_of_cloud_cover',
    'specific_cloud_ice_water_content',
]


def download_deterministic(year: int, month: int, output_dir: Path):
    """Download deterministic ERA5 pressure-level data for one month."""
    c = cdsapi.Client()
    output_dir.mkdir(parents=True, exist_ok=True)

    for variable in VARIABLES:
        output_file = output_dir / f"era5_{year}{month:02d}_{variable}.nc"
        if output_file.exists():
            print(f"Already exists, skipping: {output_file.name}")
            continue

        print(f"Downloading: {output_file.name}")
        c.retrieve(
            'reanalysis-era5-pressure-levels',
            {
                'product_type': 'reanalysis',
                'variable': variable,
                'pressure_level': PRESSURE_LEVELS,
                'year': str(year),
                'month': f"{month:02d}",
                'day': [f"{d:02d}" for d in range(1, 32)],
                'time': [f"{h:02d}:00" for h in range(0, 24, 3)],
                'format': 'netcdf',
            },
            str(output_file)
        )
        print(f"Saved: {output_file}")


def download_ensemble(year: int, month: int, output_dir: Path):
    """Download ERA5 ensemble (10-member) data for one month."""
    c = cdsapi.Client()
    output_dir.mkdir(parents=True, exist_ok=True)

    for variable in VARIABLES:
        output_file = output_dir / f"era5_ens_{year}{month:02d}_{variable}.nc"
        if output_file.exists():
            print(f"Already exists, skipping: {output_file.name}")
            continue

        print(f"Downloading ensemble: {output_file.name}")
        c.retrieve(
            'reanalysis-era5-pressure-levels',
            {
                'product_type': 'ensemble_members',
                'variable': variable,
                'pressure_level': PRESSURE_LEVELS,
                'year': str(year),
                'month': f"{month:02d}",
                'day': [f"{d:02d}" for d in range(1, 32)],
                'time': [f"{h:02d}:00" for h in range(0, 24, 3)],
                'format': 'netcdf',
            },
            str(output_file)
        )
        print(f"Saved: {output_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download ERA5 data for CLIM-INT')
    parser.add_argument('--year', type=int, required=True, help='Year (e.g. 2022)')
    parser.add_argument('--month', type=int, required=True, help='Month (1-12)')
    parser.add_argument('--ensemble', action='store_true', help='Download ensemble members')
    args = parser.parse_args()

    if args.ensemble:
        download_ensemble(
            args.year, args.month,
            Path('data/raw/era5_ens')
        )
    else:
        download_deterministic(
            args.year, args.month,
            Path('data/raw/era5')
        )
