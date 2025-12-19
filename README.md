# Sports Betting Analytics and Forecasting System

## Project Overview

This system is designed to ingest extensive football data (NFL and College Football), analyze team and game factors, and forecast game outcomes (point spreads, totals, win probabilities) with comparisons to betting market lines.

## Project Structure

```
.
├── config/                 # Configuration files
├── data/                   # Raw data storage (CSV, JSON)
├── models/                 # Saved model artifacts
├── scripts/                # Executable scripts (CLI entry points)
├── src/                    # Source code modules
│   ├── data/              # Phase 0: Data ingestion modules
│   ├── features/          # Phase 1: Feature engineering
│   ├── models/            # Phase 1: Model training and prediction
│   └── output/            # Phase 1: Output formatting
├── tests/                 # Unit tests
└── requirements.txt       # Python dependencies
```

## Phase 0: Data Ingestion and Storage

Phase 0 establishes the data pipeline to collect and store historical and current football data from various sources.

## Phase 1: Baseline Predictive Models

Phase 1 implements baseline models for predicting game outcomes (spread, total, win probability) with terminal-based output.

## Getting Started

1. Install dependencies: `pip install -r requirements.txt`
2. Set up database: Configure `config/database_config.yaml`
3. Run data ingestion: `python scripts/ingest_data.py --league NFL --historical`
4. Train models: `python scripts/train.py --league NFL`
5. Generate predictions: `python scripts/predict.py --league NFL --week 12`

## Notes

- This is a personal analytics tool, not a commercial betting platform
- All predictions are for research purposes only
- No guarantees on results; betting involves risk

