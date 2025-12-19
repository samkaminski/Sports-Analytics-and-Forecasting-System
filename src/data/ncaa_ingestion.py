"""
NCAA Data Ingestion Module
Phase 0: Data Ingestion and Storage

USE: Fetches College Football data from various sources and stores it in database
WHAT WILL BE BUILT:
  - Functions to fetch NCAA game schedules and results
  - Functions to fetch team statistics
  - Functions to fetch play-by-play data (for future phases)
  - Data transformation and cleaning logic
  - Database insertion with duplicate handling

HOW IT WORKS:
  - Connects to NCAA data sources (CollegeFootballData.com API, SportsDataVerse)
  - Fetches data via API calls
  - Transforms raw data into database schema format
  - Uses DatabaseManager to insert data with upsert logic
  - Handles rate limiting (CFBD API has rate limits)

FITS IN PROJECT:
  - Phase 0: Populates database with NCAA historical and current data
  - Used by feature engineering to query team stats
  - Used by model training to get historical game results
  - Runs on schedule (weekly updates) via cron or scheduler
  - Note: NCAA has 130+ FBS teams, so data volume is larger than NFL
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


class NCAADataIngester:
    """
    Handles all NCAA Football data ingestion from various sources.
    
    This class:
    - Fetches game schedules and results
    - Fetches team statistics
    - Fetches play-by-play data (for future use)
    - Transforms and stores data in database
    """
    
    def __init__(self, db_manager: DatabaseManager, config_path: str = "config/data_sources_config.yaml"):
        """
        Initialize NCAA data ingester.
        
        Args:
            db_manager: DatabaseManager instance for database operations
            config_path: Path to data sources configuration
        """
        self.db = db_manager
        self.config = self._load_config(config_path)
        self.ncaa_config = self.config.get('ncaa', {})
        self.cfbd_config = self.ncaa_config.get('cfbd', {})
        
        # API key for CFBD
        self.cfbd_api_key = os.getenv(self.cfbd_config.get('api_key_env_var', 'CFBD_API_KEY'))
        self.cfbd_base_url = self.cfbd_config.get('base_url', 'https://api.collegefootballdata.com')
        
        # Rate limiting
        self.last_request_time = 0
        self.rate_limit_per_minute = self.cfbd_config.get('rate_limit_per_minute', 100)
        self.request_times = []  # Track requests for rate limiting
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load data sources configuration."""
        import yaml
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _rate_limit(self):
        """Enforce rate limiting for CFBD API."""
        now = time.time()
        # Remove requests older than 1 minute
        self.request_times = [t for t in self.request_times if now - t < 60]
        
        if len(self.request_times) >= self.rate_limit_per_minute:
            # Wait until we can make another request
            sleep_time = 60 - (now - self.request_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
                # Clean up old requests
                self.request_times = [t for t in self.request_times if now - t < 60]
        
        self.request_times.append(time.time())
    
    def _make_cfbd_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make a request to CFBD API with authentication and rate limiting.
        
        Args:
            endpoint: API endpoint (e.g., '/games')
            params: Query parameters
        
        Returns:
            JSON response data
        """
        self._rate_limit()
        
        url = f"{self.cfbd_base_url}{endpoint}"
        headers = {
            'Authorization': f'Bearer {self.cfbd_api_key}',
            'Accept': 'application/json'
        }
        
        try:
            response = requests.get(url, headers=headers, params=params or {})
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"CFBD API request failed: {e}")
            raise
    
    def fetch_game_schedule(self, season: int, week: Optional[int] = None, team: Optional[str] = None) -> pd.DataFrame:
        """
        Fetch NCAA game schedule for a season/week.
        
        Args:
            season: NCAA season year
            week: Optional week number (None = all weeks)
            team: Optional team name to filter
        
        Returns:
            DataFrame with game schedule data
        """
        logger.info(f"Fetching NCAA schedule for season {season}, week {week}")
        
        params = {'year': season}
        if week:
            params['week'] = week
        if team:
            params['team'] = team
        
        try:
            data = self._make_cfbd_request('/games', params=params)
            
            # Transform CFBD API response to our schema
            games = []
            for game in data:
                games.append({
                    'game_id': f"NCAA_{game['id']}",
                    'season': season,
                    'week': game.get('week', 0),
                    'date': datetime.strptime(game['start_date'], '%Y-%m-%dT%H:%M:%S.%fZ').date(),
                    'home_team_id': f"NCAA_{game['home_team']}",
                    'away_team_id': f"NCAA_{game['away_team']}",
                    'home_score': game.get('home_points'),
                    'away_score': game.get('away_points'),
                    'completed': game.get('completed', False),
                    'stadium': game.get('venue'),
                    'is_neutral_site': game.get('neutral_site', False)
                })
            
            return pd.DataFrame(games)
        except Exception as e:
            logger.error(f"Error fetching NCAA schedule: {e}")
            return pd.DataFrame()
    
    def fetch_team_stats(self, season: int, week: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch team statistics for a season/week.
        
        Args:
            season: NCAA season year
            week: Optional week number (None = season totals)
        
        Returns:
            DataFrame with team statistics
        """
        logger.info(f"Fetching NCAA team stats for season {season}, week {week}")
        
        # TODO: Implement actual CFBD API call for team stats
        # CFBD has endpoints like /stats/team or /stats/game/advanced
        # This is a placeholder structure
        
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
        
        logger.info(f"Ingesting {len(games_df)} NCAA games into database")
        
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
                        league='NCAA',
                        home_score=row.get('home_score'),
                        away_score=row.get('away_score'),
                        completed=row.get('completed', False),
                        stadium=row.get('stadium'),
                        is_neutral_site=row.get('is_neutral_site', False),
                        created_at=date.today(),
                        updated_at=date.today()
                    )
                    
                    if upsert:
                        existing = session.query(Game).filter_by(game_id=row['game_id']).first()
                        if existing:
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
            logger.info("NCAA games ingestion completed")
    
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
        
        logger.info(f"Ingesting NCAA team stats for {len(stats_df)} team-week combinations")
        
        with self.db.get_session() as session:
            for _, row in stats_df.iterrows():
                try:
                    stats = TeamStats(
                        team_id=row['team_id'],
                        season=row['season'],
                        week=row['week'],
                        league='NCAA',
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
            logger.info("NCAA team stats ingestion completed")
    
    def ingest_historical_data(self, start_season: int, end_season: int):
        """
        Ingest historical NCAA data for multiple seasons.
        
        This is the main function to run during initial setup.
        
        Args:
            start_season: First season to ingest
            end_season: Last season to ingest (inclusive)
        """
        logger.info(f"Starting historical NCAA data ingestion: {start_season}-{end_season}")
        
        for season in range(start_season, end_season + 1):
            logger.info(f"Processing season {season}")
            
            # Fetch and ingest games
            games_df = self.fetch_game_schedule(season)
            if not games_df.empty:
                self.ingest_games(games_df, upsert=True)
            
            # Fetch and ingest team stats
            stats_df = self.fetch_team_stats(season)
            if not stats_df.empty:
                self.ingest_team_stats(stats_df, upsert=True)
        
        logger.info("Historical NCAA data ingestion completed")
    
    def update_current_season(self, season: int, week: Optional[int] = None):
        """
        Update data for current season (weekly updates).
        
        Args:
            season: Current season year
            week: Optional week to update (None = update all recent weeks)
        """
        logger.info(f"Updating NCAA data for season {season}, week {week}")
        
        games_df = self.fetch_game_schedule(season, week)
        if not games_df.empty:
            self.ingest_games(games_df, upsert=True)
        
        stats_df = self.fetch_team_stats(season, week)
        if not stats_df.empty:
            self.ingest_team_stats(stats_df, upsert=True)
        
        logger.info("Current season update completed")

