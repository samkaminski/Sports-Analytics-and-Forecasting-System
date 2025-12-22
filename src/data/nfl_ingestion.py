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

from .database import DatabaseManager, Team, Game, TeamStats

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
    
    def fetch_games(self, season: int, week: Optional[int] = None, include_future: bool = False) -> pd.DataFrame:
        """
        Fetch NFL games for a season/week using nfl-data-py.
        
        Args:
            season: NFL season year
            week: Optional week number (None = all weeks)
            include_future: If True, include future games (default: False, filters to games <= today)
        
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
            
            # Filter out future games (only include games before today) unless include_future=True
            if not include_future:
                today = date.today()
                if 'gameday' in df.columns:
                    # Convert gameday to date if it's not already
                    df['gameday_date'] = pd.to_datetime(df['gameday'], errors='coerce').dt.date
                    # Filter to only include games before today (preserves existing behavior)
                    df = df[df['gameday_date'] < today].copy()
                    df = df.drop(columns=['gameday_date'], errors='ignore')
            
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
                
                # Get scores - handle NaN for future/unplayed games
                home_score = None
                away_score = None
                completed = False
                
                # Only mark as completed if both scores are present and not NaN
                home_score_val = row.get('home_score')
                away_score_val = row.get('away_score')
                
                if pd.notna(home_score_val) and pd.notna(away_score_val):
                    home_score = int(home_score_val)
                    away_score = int(away_score_val)
                    completed = True
                else:
                    # Ensure None (not NaN) for unplayed games
                    home_score = None
                    away_score = None
                    completed = False
                
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
    
    def compute_team_stats(self, season: int) -> pd.DataFrame:
        """
        Compute team statistics for a season by aggregating from games table.
        
        Args:
            season: NFL season year
        
        Returns:
            DataFrame with team statistics (empty if no completed games)
        """
        logger.info(f"Computing NFL team stats for season {season} from games table")
        
        with self.db.get_session() as session:
            from sqlalchemy import select
            
            # Query all completed games for this season
            stmt = select(Game).where(
                Game.league == 'NFL',
                Game.season == season,
                Game.completed == True,
                Game.home_score.isnot(None),
                Game.away_score.isnot(None)
            )
            
            games = session.scalars(stmt).all()
            
            if not games:
                logger.warning(f"No completed games yet for season {season}; skipping stats.")
                return pd.DataFrame()
            
            # Aggregate stats per team
            team_stats_dict = {}
            
            for game in games:
                # Process home team
                if game.home_team_id not in team_stats_dict:
                    team_stats_dict[game.home_team_id] = {
                        'team_id': game.home_team_id,
                        'league': 'NFL',
                        'season': season,
                        'team_abbr': game.home_team_id.replace('NFL_', ''),
                        'games_played': 0,
                        'wins': 0,
                        'losses': 0,
                        'points_for': 0,
                        'points_against': 0
                    }
                
                home_stats = team_stats_dict[game.home_team_id]
                home_stats['games_played'] += 1
                home_stats['points_for'] += game.home_score
                home_stats['points_against'] += game.away_score
                if game.home_score > game.away_score:
                    home_stats['wins'] += 1
                elif game.home_score < game.away_score:
                    home_stats['losses'] += 1
                
                # Process away team
                if game.away_team_id not in team_stats_dict:
                    team_stats_dict[game.away_team_id] = {
                        'team_id': game.away_team_id,
                        'league': 'NFL',
                        'season': season,
                        'team_abbr': game.away_team_id.replace('NFL_', ''),
                        'games_played': 0,
                        'wins': 0,
                        'losses': 0,
                        'points_for': 0,
                        'points_against': 0
                    }
                
                away_stats = team_stats_dict[game.away_team_id]
                away_stats['games_played'] += 1
                away_stats['points_for'] += game.away_score
                away_stats['points_against'] += game.home_score
                if game.away_score > game.home_score:
                    away_stats['wins'] += 1
                elif game.away_score < game.home_score:
                    away_stats['losses'] += 1
            
            if not team_stats_dict:
                logger.warning(f"No team stats computed for season {season}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            stats_list = list(team_stats_dict.values())
            logger.info(f"Computed stats for {len(stats_list)} teams")
            
            return pd.DataFrame(stats_list)
    
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
                    
                    # Convert NaN scores to None (handle case where DataFrame still has NaN)
                    home_score_val = row.get('home_score')
                    away_score_val = row.get('away_score')
                    
                    if pd.notna(home_score_val) and pd.notna(away_score_val):
                        home_score = int(home_score_val)
                        away_score = int(away_score_val)
                        completed = True
                    else:
                        home_score = None
                        away_score = None
                        completed = False
                    
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
                        existing.home_score = home_score
                        existing.away_score = away_score
                        existing.completed = completed
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
                            home_score=home_score,
                            away_score=away_score,
                            completed=completed,
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
    
    def ingest_team_stats(self, stats_df: pd.DataFrame):
        """
        Insert team statistics into database (idempotent - no duplicates).
        
        Args:
            stats_df: DataFrame with team statistics
        """
        if stats_df.empty:
            logger.warning("No team stats to ingest")
            return
        
        logger.info(f"Ingesting team stats for {len(stats_df)} teams")
        
        with self.db.get_session() as session:
            for _, row in stats_df.iterrows():
                try:
                    # Check if stats exist
                    from sqlalchemy import select
                    stmt = select(TeamStats).where(
                        TeamStats.team_id == row['team_id'],
                        TeamStats.season == row['season'],
                        TeamStats.league == row['league']
                    )
                    existing = session.scalar(stmt)
                    
                    if existing:
                        # Update existing
                        existing.team_abbr = row['team_abbr']
                        existing.games_played = row.get('games_played')
                        existing.wins = row.get('wins')
                        existing.losses = row.get('losses')
                        existing.points_for = row.get('points_for')
                        existing.points_against = row.get('points_against')
                        existing.updated_at = date.today()
                    else:
                        # Insert new
                        team_stats = TeamStats(
                            team_id=row['team_id'],
                            league=row['league'],
                            season=row['season'],
                            team_abbr=row['team_abbr'],
                            games_played=row.get('games_played'),
                            wins=row.get('wins'),
                            losses=row.get('losses'),
                            points_for=row.get('points_for'),
                            points_against=row.get('points_against'),
                            created_at=date.today(),
                            updated_at=date.today()
                        )
                        session.add(team_stats)
                    
                except Exception as e:
                    logger.error(f"Error ingesting team stats for {row.get('team_id')}: {e}")
                    session.rollback()
                    continue
            
            session.commit()
            logger.info("Team stats ingestion completed")
    
    def ingest_season(self, season: int, week: Optional[int] = None, include_stats: bool = False):
        """
        Ingest NFL games for a season/week.
        
        Args:
            season: NFL season year
            week: Optional week number (None = all weeks)
            include_stats: If True, also ingest team stats for the season
        """
        logger.info(f"Ingesting NFL games for season {season}, week {week}")
        
        games_df = self.fetch_games(season, week)
        if not games_df.empty:
            self.ingest_games(games_df)
        else:
            logger.warning(f"No games found for season {season}, week {week}")
        
        # Ingest team stats if requested (season-level only, not week-specific)
        if include_stats:
            try:
                stats_df = self.compute_team_stats(season)
                if not stats_df.empty:
                    self.ingest_team_stats(stats_df)
                else:
                    logger.warning(f"No team stats computed for season {season}")
            except Exception as e:
                logger.error(f"Failed to compute/ingest team stats for season {season}: {e}")
                raise
    
    def ingest_historical(self, start_season: int, end_season: int, include_stats: bool = False):
        """
        Ingest NFL games for multiple seasons (historical ingestion).
        
        Args:
            start_season: First season year (inclusive)
            end_season: Last season year (inclusive)
            include_stats: If True, also compute and ingest team stats for each season
        """
        logger.info(f"Ingesting historical NFL data: {start_season}-{end_season}")
        
        for season in range(start_season, end_season + 1):
            logger.info(f"Processing season {season}...")
            try:
                # Ingest games for this season (all weeks)
                self.ingest_season(season, week=None, include_stats=False)
                
                # Compute team stats if requested
                if include_stats:
                    try:
                        stats_df = self.compute_team_stats(season)
                        if not stats_df.empty:
                            self.ingest_team_stats(stats_df)
                    except Exception as e:
                        logger.warning(f"Failed to compute team stats for season {season}: {e}")
                        # Continue with next season even if stats fail
                        continue
                
            except Exception as e:
                logger.error(f"Error ingesting season {season}: {e}")
                # Continue with next season even if one fails
                continue
        
        logger.info(f"Historical ingestion completed: {start_season}-{end_season}")
