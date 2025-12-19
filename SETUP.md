# Setup Guide

## Prerequisites

1. **Python 3.8+** installed
2. **PostgreSQL** database server running
3. **API Keys** (optional, for data ingestion):
   - CollegeFootballData.com API key (for NCAA data)
   - The Odds API key (for betting odds)
   - OpenWeatherMap API key (for weather data, future use)

## Installation Steps

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Database

Edit `config/database_config.yaml` with your PostgreSQL credentials:

```yaml
database:
  host: localhost
  port: 5432
  name: sports_betting_db
  user: postgres
  password: your_password  # Or set DB_PASSWORD environment variable
```

### 3. Initialize Database Schema

```bash
python scripts/ingest_data.py init-db
```

This creates all necessary tables in PostgreSQL.

### 4. Configure API Keys (Optional)

Create a `.env` file in the project root:

```bash
cp .env.example .env
# Edit .env with your API keys
```

Or set environment variables:
```bash
export CFBD_API_KEY=your_key_here
export THE_ODDS_API_KEY=your_key_here
```

### 5. Ingest Historical Data

**NFL Data:**
```bash
python scripts/ingest_data.py ingest --league NFL --historical --start-season 2018 --end-season 2023
```

**NCAA Data:**
```bash
python scripts/ingest_data.py ingest --league NCAA --historical --start-season 2018 --end-season 2023
```

**Note:** The ingestion scripts currently have placeholder implementations. You'll need to:
- Implement actual API calls in `src/data/nfl_ingestion.py` and `src/data/ncaa_ingestion.py`
- Or use alternative data sources (nflfastR, CFBD API, etc.)

### 6. Compute Team Ratings

Before training models, compute team ratings (Elo/SRS):

```python
from src.data.database import get_db_connection
from src.features.ratings import compute_elo_ratings

db = get_db_connection()
with db.get_session() as session:
    for season in range(2018, 2024):
        compute_elo_ratings(session, 'NFL', season)
```

### 7. Train Models

**NFL Models:**
```bash
python scripts/train.py --league NFL --start-season 2018 --end-season 2023
```

**NCAA Models:**
```bash
python scripts/train.py --league NCAA --start-season 2018 --end-season 2023
```

Models will be saved to `models/nfl/` or `models/ncaa/` directories.

### 8. Generate Predictions

**Predict all upcoming games:**
```bash
python scripts/predict.py predict --league NFL --upcoming
```

**Predict specific week:**
```bash
python scripts/predict.py predict --league NFL --week 12 --season 2024
```

**Detailed prediction for single game:**
```bash
python scripts/predict.py predict --league NFL --game-id NFL_2024_12_KC_BUF --detailed
```

## Project Structure

```
.
├── config/              # Configuration files
│   ├── database_config.yaml
│   ├── data_sources_config.yaml
│   └── model_config.yaml
├── scripts/             # CLI entry points
│   ├── ingest_data.py  # Data ingestion
│   ├── train.py        # Model training
│   └── predict.py      # Generate predictions
├── src/                 # Source code
│   ├── data/           # Phase 0: Data ingestion
│   ├── features/       # Phase 1: Feature engineering
│   ├── models/         # Phase 1: Model training & prediction
│   └── output/        # Phase 1: Terminal formatting
└── models/             # Saved model artifacts (created after training)
```

## Next Steps

1. **Implement Data Ingestion**: Complete the API calls in `nfl_ingestion.py` and `ncaa_ingestion.py`
2. **Test with Real Data**: Verify data ingestion works with actual APIs
3. **Train and Evaluate**: Train models and evaluate performance
4. **Generate Predictions**: Use the prediction CLI to generate forecasts

## Troubleshooting

- **Database Connection Errors**: Check PostgreSQL is running and credentials are correct
- **No Training Data**: Ensure data ingestion completed successfully
- **Model Not Found**: Run `train.py` before using `predict.py`
- **API Rate Limits**: Adjust rate limiting in data ingestion modules

## Notes

- This is Phase 0 and Phase 1 implementation only
- Frontend/API (Phase 3) is not included
- Data ingestion modules need actual API implementations
- Models are baseline (simple linear/logistic regression)

