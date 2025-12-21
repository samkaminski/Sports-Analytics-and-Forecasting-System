"""
NFL Data Ingestion Module
Phase 0: Data Ingestion and Storage

USE: Fetches NFL data from various sources and stores it in the database
WHAT WILL BE BUILT:
  - Functions to fetch game schedules and results
  - Functions to fetch team statistics
  - Functions to fetch play-by-play data (for future phases)
  - Data transformation and cleaning logic
  - Database insertion with duplicate handling

HOW IT WORKS:
  - Connects to NFL data sources (Pro-Football-Reference, nflfastR)
  - Fetches data via API calls or web scraping
  - Transforms raw data into database schema format
  - Uses DatabaseManager to insert data with upsert logic
  - Handles rate limiting and error recovery

FITS IN PROJECT:
  - Phase 0: Populates database with NFL historical and current data
  - Used by feature engineering to query team stats
  - Used by model training to get historical game results
  - Runs on schedule (weekly updates) via cron or scheduler
"""

import os
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, date
import pandas as pd
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select

from .database import DatabaseManager, Team, Game

logger = logging.getLogger(__name__)


class NFLDataIngester:
    """
    Handles all NFL data ingestion from various sources.
    
    This class:
    - Fetches game schedules and results
    - Fetches team statistics
    - Fetches play-by-play data (for future use)
    - Transforms and stores data in database
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize NFL data ingester.
        
        Args:
            db_manager: DatabaseManager instance for database operations
        """
        self.db = db_manager
    
    def fetch_games(self, season: int, week: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch NFL games for a season/week using nfl-data-py.
        
        Args:
            season: NFL season year
            week: Optional week number (None = all weeks)
        
        Returns:
            DataFrame with game data
        """
        try:
            import nfl_data_py as nfl
            
            logger.info(f"Fetching NFL games for season {season}, week {week}")
            
            # Fetch schedule data
            df = nfl.import_schedules([season])
            
            if df.empty:
                return pd.DataFrame()
            
            # Filter by week if specified
            if week is not None:
                df = df[df["week"] == week].copy()
            
            if df.empty:
                return pd.DataFrame()
            
            # Transform to our schema
            games = []
            for _, row in df.iterrows():
                # Create game_id
                game_id = f"NFL_{season}_{row.get('week', 0)}_{row.get('home_team', '')}_{row.get('away_team', '')}"
                
                # Get team IDs (create if needed)
                home_team_id = f"NFL_{row.get('home_team', '')}"
                away_team_id = f"NFL_{row.get('away_team', '')}"
                
                # Parse date
                game_date = None
                if pd.notna(row.get('gameday')):
                    try:
                        game_date = pd.to_datetime(row['gameday']).date()
                    except:
                        pass
                
                # Get scores
                home_score = None
                away_score = None
                completed = False
                if pd.notna(row.get('home_score')):
                    home_score = int(row['home_score'])
                    away_score = int(row.get('away_score', 0))
                    completed = True
                
                games.append({
                    'game_id': game_id,
                    'season': season,
                    'week': int(row.get('week', 0)),
                    'date': game_date,
                    'home_team_id': home_team_id,
                    'away_team_id': away_team_id,
                    'home_team_abbr': row.get('home_team', ''),
                    'away_team_abbr': row.get('away_team', ''),
                    'home_team_name': row.get('home_team_name', ''),
                    'away_team_name': row.get('away_team_name', ''),
                    'home_score': home_score,
                    'away_score': away_score,
                    'completed': completed,
                    'stadium': row.get('stadium', ''),
                    'is_neutral_site': row.get('gametime', '') == '' or 'neutral' in str(row.get('stadium', '')).lower()
                })
            
            return pd.DataFrame(games)
            
        except ImportError:
            logger.error("nfl-data-py package not installed. Install with: pip install nfl-data-py")
            raise
        except Exception as e:
            logger.error(f"Error fetching NFL games: {e}")
            return pd.DataFrame()
    
    def fetch_team_stats(self, season: int, week: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch team statistics for a season/week.
        
        Args:
            season: NFL season year
            week: Optional week number (None = season totals)
        
        Returns:
            DataFrame with team statistics
        """
        logger.info(f"Fetching NFL team stats for season {season}, week {week}")
        
        # TODO: Implement actual data fetching
        # This would fetch from Pro-Football-Reference or nflfastR
        
        return pd.DataFrame()
    
    def _ensure_team(self, session, team_id: str, team_abbr: str, team_name: str):
        """Ensure team exists in database (idempotent)."""
        from sqlalchemy import select
        
        stmt = select(Team).where(Team.team_id == team_id)
        existing = session.scalar(stmt)
        
        if not existing:
            team = Team(
                team_id=team_id,
                name=team_name or team_abbr,
                league='NFL',
                abbreviation=team_abbr,
                created_at=date.today()
            )
            session.add(team)
            return team
        return existing
    
    def ingest_games(self, games_df: pd.DataFrame):
        """
        Insert game data into database (idempotent - no duplicates).
        
        Args:
            games_df: DataFrame with game data
        """
        if games_df.empty:
            logger.warning("No games to ingest")
            return
        
        logger.info(f"Ingesting {len(games_df)} games into database")
        
        with self.db.get_session() as session:
            for _, row in games_df.iterrows():
                try:
                    # Ensure teams exist
                    self._ensure_team(
                        session,
                        row['home_team_id'],
                        row.get('home_team_abbr', ''),
                        row.get('home_team_name', '')
                    )
                    self._ensure_team(
                        session,
                        row['away_team_id'],
                        row.get('away_team_abbr', ''),
                        row.get('away_team_name', '')
                    )
                    
                    # Check if game exists
                    from sqlalchemy import select
                    stmt = select(Game).where(Game.game_id == row['game_id'])
                    existing = session.scalar(stmt)
                    
                    if existing:
                        # Update existing
                        existing.season = row['season']
                        existing.week = row['week']
                        if row.get('date'):
                            existing.date = row['date']
                        existing.home_score = row.get('home_score')
                        existing.away_score = row.get('away_score')
                        existing.completed = row.get('completed', False)
                        existing.stadium = row.get('stadium')
                        existing.is_neutral_site = row.get('is_neutral_site', False)
                        existing.updated_at = date.today()
                    else:
                        # Insert new
                        game = Game(
                            game_id=row['game_id'],
                            season=row['season'],
                            week=row['week'],
                            date=row.get('date') or date.today(),
                            home_team_id=row['home_team_id'],
                            away_team_id=row['away_team_id'],
                            league='NFL',
                            home_score=row.get('home_score'),
                            away_score=row.get('away_score'),
                            completed=row.get('completed', False),
                            stadium=row.get('stadium'),
                            is_neutral_site=row.get('is_neutral_site', False),
                            created_at=date.today(),
                            updated_at=date.today()
                        )
                        session.add(game)
                    
                except Exception as e:
                    logger.error(f"Error ingesting game {row.get('game_id')}: {e}")
                    session.rollback()
                    continue
            
            session.commit()
            logger.info("Games ingestion completed")
    
    def ingest_season(self, season: int, week: Optional[int] = None):
        """
        Ingest NFL games for a season/week.
        
        Args:
            season: NFL season year
            week: Optional week number (None = all weeks)
        """
        logger.info(f"Ingesting NFL games for season {season}, week {week}")
        
        games_df = self.fetch_games(season, week)
        if not games_df.empty:
            self.ingest_games(games_df)
        else:
            logger.warning(f"No games found for season {season}, week {week}")

