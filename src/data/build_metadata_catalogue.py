"""
Build the metadata catalogue for the curated OpenSky European flight dataset.
Must be run after select_opensky_flights.py has produced the curated CSV.

Usage:
    python src/data/build_metadata_catalogue.py
"""

from pathlib import Path

import pandas as pd


INPUT_FILE = Path('data/raw/opensky/opensky_european_curated.csv')
OUTPUT_FILE = Path('data/raw/opensky/metadata_catalogue.csv')


SEASON_MAP = {1: 'Winter', 2: 'Winter', 3: 'Spring', 4: 'Spring',
              5: 'Spring', 6: 'Summer', 7: 'Summer', 8: 'Summer',
              9: 'Autumn', 10: 'Autumn', 11: 'Autumn', 12: 'Winter'}


def classify_aircraft(typecode: str) -> str:
    """Classify aircraft into broad categories by ICAO typecode."""
    if not isinstance(typecode, str):
        return 'Unknown'
    tc = typecode.upper()
    narrowbody = ['A319', 'A320', 'A321', 'B737', 'B738', 'B739',
                  'B732', 'B733', 'B734', 'B735', 'B736', 'B38M', 'A20N', 'A21N']
    widebody = ['A330', 'A332', 'A333', 'A340', 'A350', 'A359', 'A380',
                'B744', 'B747', 'B752', 'B753', 'B762', 'B763', 'B764',
                'B772', 'B773', 'B77W', 'B788', 'B789']
    regional = ['E170', 'E175', 'E190', 'E195', 'CRJ2', 'CRJ7', 'CRJ9',
                'AT72', 'AT75', 'DH8D', 'E75L', 'E75S']
    if tc in narrowbody:
        return 'Narrowbody'
    if tc in widebody:
        return 'Widebody'
    if tc in regional:
        return 'Regional'
    return 'Other'


def main():
    if not INPUT_FILE.exists():
        print(f"Input file not found: {INPUT_FILE}")
        print("Run select_opensky_flights.py first.")
        return

    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df)} flights")

    # Build catalogue
    catalogue = pd.DataFrame()
    catalogue['flight_id'] = [f"OSK_{i:04d}" for i in range(len(df))]
    catalogue['callsign'] = df['callsign'].values
    catalogue['origin'] = df['origin'].values
    catalogue['destination'] = df['destination'].values
    catalogue['typecode'] = df['typecode'].values
    catalogue['aircraft_category'] = df['typecode'].apply(classify_aircraft).values
    catalogue['year'] = df['source_year'].values
    catalogue['month'] = df['source_month'].values
    catalogue['season'] = df['source_month'].map(SEASON_MAP).values
    catalogue['source'] = 'OpenSky'
    catalogue['firstseen'] = df.get('firstseen', pd.NA)
    catalogue['lastseen'] = df.get('lastseen', pd.NA)

    catalogue.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved metadata catalogue to: {OUTPUT_FILE}")

    # Print summary
    print("\n--- Catalogue Summary ---")
    print(f"Total flights:       {len(catalogue)}")
    print(f"Years:               {sorted(catalogue['year'].unique())}")
    print(f"\nBy season:\n{catalogue['season'].value_counts().to_string()}")
    print(f"\nBy aircraft category:\n{catalogue['aircraft_category'].value_counts().to_string()}")
    print(f"\nTop 10 routes:")
    routes = catalogue.groupby(['origin', 'destination']).size().sort_values(ascending=False).head(10)
    print(routes.to_string())


if __name__ == '__main__':
    main()
