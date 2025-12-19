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
import requests
from sqlalchemy.exc import IntegrityError
import time

from .database import DatabaseManager, Team, Game, TeamStats, TeamRating

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
    
    def __init__(self, db_manager: DatabaseManager, config_path: str = "config/data_sources_config.yaml"):
        """
        Initialize NFL data ingester.
        
        Args:
            db_manager: DatabaseManager instance for database operations
            config_path: Path to data sources configuration
        """
        self.db = db_manager
        self.config = self._load_config(config_path)
        self.nfl_config = self.config.get('nfl', {})
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = self.nfl_config.get('pfr', {}).get('rate_limit_seconds', 2)
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load data sources configuration."""
        import yaml
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _rate_limit(self):
        """Enforce rate limiting between API requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()
    
    def fetch_game_schedule(self, season: int, week: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch NFL game schedule for a season/week.
        
        This function will fetch from nflfastR or Pro-Football-Reference.
        For Phase 0, we'll use a simplified approach - in production,
        this would connect to actual APIs or scrape PFR.
        
        Args:
            season: NFL season year
            week: Optional week number (None = all weeks)
        
        Returns:
            DataFrame with game schedule data
        """
        # TODO: Implement actual API calls to nflfastR or PFR
        # For now, this is a placeholder structure
        
        logger.info(f"Fetching NFL schedule for season {season}, week {week}")
        
        # Example structure - replace with actual API calls
        # games = []
        # for week_num in range(1, 18):
        #     url = f"https://api.nfl.com/v1/games?season={season}&week={week_num}"
        #     response = requests.get(url)
        #     games.extend(response.json()['games'])
        
        # Placeholder return
        return pd.DataFrame()
    
    def fetch_game_results(self, season: int, week: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch completed game results.
        
        Args:
            season: NFL season year
            week: Optional week number
        
        Returns:
            DataFrame with game results (scores, etc.)
        """
        logger.info(f"Fetching NFL game results for season {season}, week {week}")
        
        # TODO: Implement actual data fetching
        # This would fetch from nflfastR or PFR
        
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
    
    def ingest_games(self, games_df: pd.DataFrame, upsert: bool = True):
        """
        Insert game data into database.
        
        Args:
            games_df: DataFrame with game data
            upsert: If True, update existing records; if False, skip duplicates
        """
        if games_df.empty:
            logger.warning("No games to ingest")
            return
        
        logger.info(f"Ingesting {len(games_df)} games into database")
        
        with self.db.get_session() as session:
            for _, row in games_df.iterrows():
                try:
                    game = Game(
                        game_id=row['game_id'],
                        season=row['season'],
                        week=row['week'],
                        date=row['date'],
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
                    
                    if upsert:
                        # Update existing or insert new
                        existing = session.query(Game).filter_by(game_id=row['game_id']).first()
                        if existing:
                            # Update existing record
                            for key, value in row.items():
                                if hasattr(existing, key):
                                    setattr(existing, key, value)
                            existing.updated_at = date.today()
                        else:
                            session.add(game)
                    else:
                        session.add(game)
                    
                except IntegrityError as e:
                    if upsert:
                        logger.warning(f"Error upserting game {row.get('game_id')}: {e}")
                    else:
                        logger.debug(f"Game {row.get('game_id')} already exists, skipping")
                    session.rollback()
                    continue
                except Exception as e:
                    logger.error(f"Error ingesting game {row.get('game_id')}: {e}")
                    session.rollback()
                    continue
            
            session.commit()
            logger.info("Games ingestion completed")
    
    def ingest_team_stats(self, stats_df: pd.DataFrame, upsert: bool = True):
        """
        Insert team statistics into database.
        
        Args:
            stats_df: DataFrame with team statistics
            upsert: If True, update existing records
        """
        if stats_df.empty:
            logger.warning("No team stats to ingest")
            return
        
        logger.info(f"Ingesting team stats for {len(stats_df)} team-week combinations")
        
        with self.db.get_session() as session:
            for _, row in stats_df.iterrows():
                try:
                    stats = TeamStats(
                        team_id=row['team_id'],
                        season=row['season'],
                        week=row['week'],
                        league='NFL',
                        points_for=row.get('points_for'),
                        points_against=row.get('points_against'),
                        point_differential=row.get('point_differential'),
                        yards_for=row.get('yards_for'),
                        yards_against=row.get('yards_against'),
                        created_at=date.today()
                    )
                    
                    if upsert:
                        existing = session.query(TeamStats).filter_by(
                            team_id=row['team_id'],
                            season=row['season'],
                            week=row['week']
                        ).first()
                        if existing:
                            for key, value in row.items():
                                if hasattr(existing, key):
                                    setattr(existing, key, value)
                        else:
                            session.add(stats)
                    else:
                        session.add(stats)
                    
                except Exception as e:
                    logger.error(f"Error ingesting team stats: {e}")
                    session.rollback()
                    continue
            
            session.commit()
            logger.info("Team stats ingestion completed")
    
    def ingest_historical_data(self, start_season: int, end_season: int):
        """
        Ingest historical NFL data for multiple seasons.
        
        This is the main function to run during initial setup.
        
        Args:
            start_season: First season to ingest
            end_season: Last season to ingest (inclusive)
        """
        logger.info(f"Starting historical NFL data ingestion: {start_season}-{end_season}")
        
        for season in range(start_season, end_season + 1):
            logger.info(f"Processing season {season}")
            
            # Fetch and ingest games
            games_df = self.fetch_game_results(season)
            if not games_df.empty:
                self.ingest_games(games_df, upsert=True)
            
            # Fetch and ingest team stats
            stats_df = self.fetch_team_stats(season)
            if not stats_df.empty:
                self.ingest_team_stats(stats_df, upsert=True)
            
            # Rate limiting between seasons
            self._rate_limit()
        
        logger.info("Historical NFL data ingestion completed")
    
    def update_current_season(self, season: int, week: Optional[int] = None):
        """
        Update data for current season (weekly updates).
        
        This function is called on a schedule (e.g., weekly) to update
        the database with new game results and stats.
        
        Args:
            season: Current season year
            week: Optional week to update (None = update all recent weeks)
        """
        logger.info(f"Updating NFL data for season {season}, week {week}")
        
        # Fetch latest games
        games_df = self.fetch_game_results(season, week)
        if not games_df.empty:
            self.ingest_games(games_df, upsert=True)
        
        # Fetch latest team stats
        stats_df = self.fetch_team_stats(season, week)
        if not stats_df.empty:
            self.ingest_team_stats(stats_df, upsert=True)
        
        logger.info("Current season update completed")

