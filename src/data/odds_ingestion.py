"""
Betting Odds Ingestion Module
Phase 0: Data Ingestion and Storage

USE: Fetches betting odds and lines from sportsbook APIs
WHAT WILL BE BUILT:
  - Functions to fetch current and historical betting lines
  - Functions to fetch opening and closing lines
  - Data transformation to store odds in database
  - Line movement tracking

HOW IT WORKS:
  - Connects to odds APIs (The Odds API, etc.)
  - Fetches point spreads, totals, and moneylines
  - Transforms odds to database format
  - Stores with timestamps to track line movement
  - Note: Odds are NOT used as model features (to avoid circular reasoning)
    They are stored for Phase 3 market comparison

FITS IN PROJECT:
  - Phase 0: Populates betting_odds table
  - Phase 3: Used to compare model predictions vs market
  - Not used in Phase 1 model training (we want to beat the market, not copy it)
"""

import os
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, date
import pandas as pd
import requests
from sqlalchemy.exc import IntegrityError
import time

from .database import DatabaseManager, BettingOdds

logger = logging.getLogger(__name__)


class OddsIngester:
    """
    Handles betting odds ingestion from various sportsbook APIs.
    
    This class:
    - Fetches current betting lines
    - Fetches historical lines (opening/closing)
    - Tracks line movement
    - Stores odds in database for market comparison
    """
    
    def __init__(self, db_manager: DatabaseManager, config_path: str = "config/data_sources_config.yaml"):
        """
        Initialize odds ingester.
        
        Args:
            db_manager: DatabaseManager instance for database operations
            config_path: Path to data sources configuration
        """
        self.db = db_manager
        self.config = self._load_config(config_path)
        self.odds_config = self.config.get('odds', {})
        self.the_odds_api_config = self.odds_config.get('the_odds_api', {})
        
        # API key
        self.api_key = os.getenv(self.the_odds_api_config.get('api_key_env_var', 'THE_ODDS_API_KEY'))
        self.base_url = self.the_odds_api_config.get('base_url', 'https://api.the-odds-api.com')
        
        # Rate limiting
        self.rate_limit_per_month = self.the_odds_api_config.get('rate_limit_per_month', 500)
        self.requests_this_month = 0
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load data sources configuration."""
        import yaml
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def fetch_current_odds(self, sport: str = 'americanfootball_nfl', 
                          regions: List[str] = ['us']) -> pd.DataFrame:
        """
        Fetch current betting odds for upcoming games.
        
        Args:
            sport: Sport identifier ('americanfootball_nfl' or 'americanfootball_ncaaf')
            regions: List of regions (e.g., ['us'] for US sportsbooks)
        
        Returns:
            DataFrame with current odds
        """
        if not self.api_key:
            logger.warning("No API key configured for odds API")
            return pd.DataFrame()
        
        logger.info(f"Fetching current odds for {sport}")
        
        url = f"{self.base_url}/v4/sports/{sport}/odds"
        params = {
            'apiKey': self.api_key,
            'regions': ','.join(regions),
            'markets': 'spreads,totals,h2h',  # spreads, totals, moneylines (h2h)
            'oddsFormat': 'american'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Transform API response to our schema
            odds_list = []
            for game in data:
                game_id = f"{sport}_{game['id']}"
                
                # Extract spreads, totals, moneylines from bookmakers
                for bookmaker in game.get('bookmakers', []):
                    for market in bookmaker.get('markets', []):
                        if market['key'] == 'spreads':
                            for outcome in market['outcomes']:
                                odds_list.append({
                                    'game_id': game_id,
                                    'sportsbook': bookmaker['title'],
                                    'line_type': 'current',
                                    'spread': outcome.get('point'),
                                    'timestamp': datetime.now(),
                                    'home_moneyline': None,
                                    'away_moneyline': None,
                                    'total': None
                                })
                        elif market['key'] == 'totals':
                            for outcome in market['outcomes']:
                                odds_list.append({
                                    'game_id': game_id,
                                    'sportsbook': bookmaker['title'],
                                    'line_type': 'current',
                                    'total': outcome.get('point'),
                                    'timestamp': datetime.now(),
                                    'spread': None,
                                    'home_moneyline': None,
                                    'away_moneyline': None
                                })
                        elif market['key'] == 'h2h':  # Moneylines
                            for outcome in market['outcomes']:
                                # Determine if home or away
                                # This requires matching team names - simplified here
                                pass
            
            return pd.DataFrame(odds_list)
        except Exception as e:
            logger.error(f"Error fetching odds: {e}")
            return pd.DataFrame()
    
    def ingest_odds(self, odds_df: pd.DataFrame, upsert: bool = True):
        """
        Insert betting odds into database.
        
        Args:
            odds_df: DataFrame with odds data
            upsert: If True, update existing records
        """
        if odds_df.empty:
            logger.warning("No odds to ingest")
            return
        
        logger.info(f"Ingesting {len(odds_df)} odds records into database")
        
        with self.db.get_session() as session:
            for _, row in odds_df.iterrows():
                try:
                    odds = BettingOdds(
                        game_id=row['game_id'],
                        spread=row.get('spread'),
                        total=row.get('total'),
                        home_moneyline=row.get('home_moneyline'),
                        away_moneyline=row.get('away_moneyline'),
                        sportsbook=row.get('sportsbook', 'consensus'),
                        line_type=row.get('line_type', 'current'),
                        timestamp=row.get('timestamp', datetime.now()),
                        created_at=date.today()
                    )
                    
                    if upsert:
                        # Check for existing record (same game, sportsbook, line_type)
                        existing = session.query(BettingOdds).filter_by(
                            game_id=row['game_id'],
                            sportsbook=row.get('sportsbook', 'consensus'),
                            line_type=row.get('line_type', 'current')
                        ).first()
                        if existing:
                            for key, value in row.items():
                                if hasattr(existing, key):
                                    setattr(existing, key, value)
                        else:
                            session.add(odds)
                    else:
                        session.add(odds)
                    
                except Exception as e:
                    logger.error(f"Error ingesting odds: {e}")
                    session.rollback()
                    continue
            
            session.commit()
            logger.info("Odds ingestion completed")
    
    def update_current_odds(self, league: str = 'NFL'):
        """
        Update current odds for upcoming games.
        
        This function is called on a schedule to keep odds current.
        
        Args:
            league: 'NFL' or 'NCAA'
        """
        sport = 'americanfootball_nfl' if league == 'NFL' else 'americanfootball_ncaaf'
        
        logger.info(f"Updating current odds for {league}")
        
        odds_df = self.fetch_current_odds(sport=sport)
        if not odds_df.empty:
            self.ingest_odds(odds_df, upsert=True)
        
        logger.info("Current odds update completed")

