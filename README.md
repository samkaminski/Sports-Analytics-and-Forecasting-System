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

## How to Run the Project (Current Commands)

### 1. Environment Setup

```bash
# Activate virtual environment (if using venv)
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows

# Set database connection (required)
export DATABASE_URL=postgresql://postgres:password@localhost:5432/sports_betting_db
```
Sets up Python environment and database connection string. DATABASE_URL must be set before running any database operations.

### 2. Database Setup

```bash
python scripts/ingest_data.py init-db
```
Creates all database tables (teams, games, team_stats, team_ratings). Run once before first data ingestion.

### 3. Data Ingestion

```bash
# Ingest single season (all weeks)
python scripts/ingest_data.py ingest --league NFL --season 2023

# Ingest single season with team stats
python scripts/ingest_data.py ingest --league NFL --season 2023 --stats

# Ingest specific week
python scripts/ingest_data.py ingest --league NFL --season 2023 --week 1

# Ingest historical range with stats
python scripts/ingest_data.py ingest --league NFL --historical --start-season 2020 --end-season 2023 --stats

# Ingest current/in-progress season (ignores future games)
python scripts/ingest_data.py ingest --league NFL --season 2025 --stats
```
Ingests NFL game data from nfl-data-py. `--stats` computes team statistics from completed games. Historical ingestion processes multiple seasons. In-progress seasons automatically exclude games after today.

### 4. API

```bash
# Start FastAPI server
python scripts/run_api.py

# Health check (once server is running)
curl http://localhost:8000/health

# Get games for a season
curl "http://localhost:8000/games?league=NFL&season=2023&week=1"

# Get team statistics
curl "http://localhost:8000/stats/teams?league=NFL&season=2023"
```
Starts the FastAPI server on port 8000. Health endpoint verifies server status. Games endpoint returns game data with team names. Team stats endpoint returns aggregated statistics per team.

### 5. Ratings

```bash
# Compute Elo ratings for a season
python scripts/compute_ratings.py --league NFL --season 2023

# Query all team ratings (top 10)
python scripts/query.py ratings --league NFL --season 2023

# Query specific team rating
python scripts/query.py ratings --league NFL --season 2023 --team KC
```
Computes Elo ratings from completed games and stores in team_ratings table. Query command displays ratings table or individual team details with ranking.

### 6. Model Training

```bash
# Train models with walk-forward validation
python scripts/train.py --league NFL --start-season 2020 --end-season 2022

# Train with custom test split (single split, not walk-forward)
python scripts/train.py --league NFL --start-season 2020 --end-season 2022 --test-split 0.2
```
Trains three models (margin, total, win probability) using walk-forward validation by default. Models include calibrated probabilities and prediction intervals. Saves to `models/{league}_{start}_{end}/`.

### 7. Predictions

```bash
# Predict a single game
python scripts/predict.py --league NFL --game-id NFL_2025_17_KC_DEN

# Predict a full week
python scripts/predict.py --league NFL --season 2025 --week 17
```
Generates predictions with calibrated win probabilities and prediction intervals for margin and total. Models must be trained first.

## Notes

- This is a personal analytics tool, not a commercial betting platform
- All predictions are for research purposes only
- No guarantees on results; betting involves risk

