# Task #4: NFL Team Ratings Computation (Elo) - COMPLETED ✅

## Summary

Task #4 implements Elo rating computation for NFL teams, storing results in a new `team_ratings` table and providing CLI tools for computation and querying.

## Implementation

### 1. Database Schema
- ✅ Added `TeamRating` model in `src/data/database.py`
- ✅ Columns: id, league, season, team_id, team_abbr, team_name, rating, as_of_date, games_count, created_at, updated_at
- ✅ Unique constraint on (league, season, team_id)

### 2. Elo Computation
- ✅ Implemented `compute_elo_ratings()` in `src/features/ratings.py`
- ✅ Uses SQLAlchemy 2.x style (select statements)
- ✅ Processes games chronologically (no data leakage)
- ✅ Uses only completed games (completed=True and scores present)
- ✅ Home advantage: +55 Elo points
- ✅ Handles ties (0.5 outcome)
- ✅ Season reset: all teams start at base_rating (1500.0)

### 3. CLI Tools
- ✅ Created `scripts/compute_ratings.py` for computing ratings
- ✅ Extended `scripts/query.py` with `ratings` command

## Demo Commands

### 1. Initialize Database (if needed)
```bash
python scripts/ingest_data.py init-db
```

### 2. Ingest Games Data (prerequisite)
```bash
python scripts/ingest_data.py ingest --league NFL --season 2023
```

### 3. Compute Elo Ratings
```bash
python scripts/compute_ratings.py --league NFL --season 2023
```

Expected output:
- Shows number of teams updated
- Displays top 5 teams by rating
- Ratings stored in `team_ratings` table

### 4. Query Ratings

**View top teams:**
```bash
python scripts/query.py ratings --league NFL --season 2023
```

**View specific team:**
```bash
python scripts/query.py ratings --league NFL --season 2023 --team KC
```

## Files Modified/Created

### Created:
- `scripts/compute_ratings.py` - CLI for computing ratings

### Modified:
- `src/data/database.py` - Added TeamRating model
- `src/features/ratings.py` - Implemented compute_elo_ratings()
- `scripts/query.py` - Added ratings command
- `src/data/__init__.py` - Exported TeamRating

## Next Tasks (Planning)

- **Task #5**: Single-game feature computation
- **Task #6**: Baseline model training
- **Task #7**: Single-game prediction via CLI

