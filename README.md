# CLIM-INT

**Machine Learning-based Climate Metrics Integration for operational ATM decision-making**

## Overview
CLIM-INT develops a multi-metric ensemble classifier that learns which of five climate 
metrics (GWP20/50/100, ATR100, RFI) is most predictive for a given flight's route, 
meteorological conditions, and aircraft type.

## Setup

### Prerequisites
- Python 3.10+
- Conda (recommended)

### Installation
conda create -n clim-int python=3.10
conda activate clim-int
pip install -r requirements.txt

## Project Structure
data/
  raw/          # Unprocessed source data (OpenSky, GVCCS)
  processed/    # Cleaned, interpolated data
  synthetic/    # BlueSky-generated trajectories
notebooks/      # Exploratory analysis
src/
  data/         # Ingestion and preprocessing scripts
  features/     # Feature engineering
  models/       # Classifier architecture and training
  evaluation/   # Validation and metrics
tests/          # Unit tests
outputs/        # Results, figures, reports
docs/           # Documentation

## Data Sources
- OpenSky Network — Real ADS-B flight trajectories (DOI: 10.5281/zenodo.3931948)
- BlueSky ATM Simulator — Synthetic trajectories (TU Delft)
- ERA5 Reanalysis — Meteorological data (Copernicus CDS)
- GVCCS — Ground-truth contrail observations (EUROCONTROL)

## License
LGPL-3.0
