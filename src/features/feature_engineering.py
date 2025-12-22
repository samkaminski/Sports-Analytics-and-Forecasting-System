"""
Feature Engineering Module
Phase 1: Feature Engineering

USE: Transforms raw database data into model-ready features
WHAT WILL BE BUILT:
  - Functions to compute features for a game at prediction time
  - Functions to compute rolling averages and statistics
  - Functions to ensure no data leakage (only past data)
  - Feature vector creation for model training

HOW IT WORKS:
  - Takes a game and prediction date/week
  - Queries database for team stats up to that point (no future data)
  - Computes features: rating differences, home field, point differentials
  - Returns feature vector ready for model input
  - Handles missing data gracefully

FITS IN PROJECT:
  - Phase 1: Creates minimal feature set for baseline models
  - Used by model training to create training datasets
  - Used by prediction engine to create features for upcoming games
  - Ensures data integrity (no leakage) for honest backtesting
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import date, datetime
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from ..data.database import Game, TeamStats, TeamRating, Team
from .ratings import get_team_rating

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """
    Main class for feature engineering operations.
    
    This class handles:
    - Computing features for individual games
    - Creating feature matrices for model training
    - Ensuring temporal correctness (no data leakage)
    """
    
    def __init__(self, session: Session, league: str, rating_type: str = 'elo'):
        """
        Initialize feature engineer.
        
        Args:
            session: Database session
            league: 'NFL' or 'NCAA'
            rating_type: 'elo' or 'srs' for team ratings
        """
        self.session = session
        self.league = league
        self.rating_type = rating_type
    
    def compute_game_features(
        self,
        game: Game,
        prediction_week: Optional[int] = None,
        prediction_date: Optional[date] = None
    ) -> Dict[str, float]:
        """
        Compute features for a single game at a specific point in time.
        
        This is the core function - it ensures we only use data available
        at prediction time (no future data leakage).
        
        Args:
            game: Game object to compute features for
            prediction_week: Week number for prediction (if None, uses game.week - 1)
            prediction_date: Date for prediction (alternative to week)
        
        Returns:
            Dictionary of feature names to values
        """
        # Determine cutoff point (no data after this)
        if prediction_week is None:
            prediction_week = game.week - 1 if game.week > 1 else 0
        
        season = game.season
        
        # Get team ratings as of prediction week
        home_rating = get_team_rating(
            self.session,
            game.home_team_id,
            season,
            prediction_week,
            self.league,
            self.rating_type
        )
        away_rating = get_team_rating(
            self.session,
            game.away_team_id,
            season,
            prediction_week,
            self.league,
            self.rating_type
        )
        
        # Rating difference (home - away)
        rating_diff = (home_rating or 0) - (away_rating or 0)
        
        # Home field advantage (binary: 1 if home, 0 if away/neutral)
        home_field = 1.0 if not game.is_neutral_site else 0.0
        
        # Get rolling average point differential
        home_point_diff = self._get_rolling_point_diff(
            game.home_team_id,
            season,
            prediction_week
        )
        away_point_diff = self._get_rolling_point_diff(
            game.away_team_id,
            season,
            prediction_week
        )
        
        # Point differential difference
        point_diff_diff = (home_point_diff or 0) - (away_point_diff or 0)
        
        # League indicator (if using combined model)
        league_indicator = 1.0 if self.league == 'NFL' else 0.0
        
        features = {
            'rating_diff': rating_diff,
            'home_field_advantage': home_field,
            'point_diff_diff': point_diff_diff,
            'league_indicator': league_indicator
        }
        
        return features
    
    def _get_rolling_point_diff(
        self,
        team_id: str,
        season: int,
        week: int,
        window: int = 8
    ) -> Optional[float]:
        """
        Get rolling average point differential for a team.
        
        Args:
            team_id: Team identifier
            season: Season year
            week: Week number (only uses data up to this week)
            window: Number of games to average (default 8)
        
        Returns:
            Average point differential or None if insufficient data
        """
        # Get team stats up to prediction week
        stats = self.session.query(TeamStats).filter(
            TeamStats.team_id == team_id,
            TeamStats.season == season,
            TeamStats.week <= week,
            TeamStats.league == self.league
        ).order_by(TeamStats.week.desc()).limit(window).all()
        
        if not stats:
            return None
        
        # Calculate average point differential
        point_diffs = [s.point_differential for s in stats if s.point_differential is not None]
        
        if not point_diffs:
            return None
        
        return np.mean(point_diffs)
    
    def create_training_features(
        self,
        games: List[Game],
        target_variables: bool = True
    ) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """
        Create feature matrix and target variables for model training.
        
        For each game, computes features using only data available before
        that game (walk-forward approach).
        
        Args:
            games: List of completed games to create features for
            target_variables: If True, also return target variables
        
        Returns:
            Tuple of (features DataFrame, targets DataFrame or None)
        """
        logger.info(f"Creating training features for {len(games)} games")
        
        features_list = []
        targets_list = []
        
        for game in games:
            # Compute features using data up to week before this game
            prediction_week = game.week - 1 if game.week > 1 else 0
            
            features = self.compute_game_features(game, prediction_week=prediction_week)
            features['game_id'] = game.game_id
            features_list.append(features)
            
            if target_variables and game.completed and game.home_score is not None:
                # Create target variables
                home_margin = game.home_score - game.away_score
                total_points = game.home_score + game.away_score
                home_wins = 1 if home_margin > 0 else 0
                
                targets_list.append({
                    'game_id': game.game_id,
                    'home_margin': home_margin,
                    'total_points': total_points,
                    'home_wins': home_wins
                })
        
        features_df = pd.DataFrame(features_list)
        
        if target_variables and targets_list:
            targets_df = pd.DataFrame(targets_list)
        else:
            targets_df = None
        
        logger.info(f"Created features for {len(features_list)} games")
        
        return features_df, targets_df


def compute_game_features(
    session: Session,
    game: Game,
    league: str,
    prediction_week: Optional[int] = None,
    rating_type: str = 'elo'
) -> Dict[str, float]:
    """
    Convenience function to compute features for a game.
    
    Args:
        session: Database session
        game: Game object
        league: 'NFL' or 'NCAA'
        prediction_week: Week for prediction
        rating_type: 'elo' or 'srs'
    
    Returns:
        Dictionary of features
    """
    engineer = FeatureEngineer(session, league, rating_type)
    return engineer.compute_game_features(game, prediction_week=prediction_week)


def compute_game_features_by_id(
    session: Session,
    game_id: str
) -> Dict[str, float]:
    """
    Compute features for a single game by game_id (Task #5).
    
    Ensures no data leakage by using only data available before the game date.
    
    Args:
        session: Database session
        game_id: Game identifier
    
    Returns:
        Dictionary of feature names to values
    
    Raises:
        ValueError: If game_id not found
    """
    from sqlalchemy import select
    
    # Load the game
    stmt = select(Game).where(Game.game_id == game_id)
    game = session.scalar(stmt)
    
    if not game:
        raise ValueError(f"Game not found: {game_id}")
    
    league = game.league
    season = game.season
    game_date = game.date
    
    # Compute Elo ratings on-the-fly using only games before target game (no leakage)
    base_rating = 1500.0
    k_factor = 20.0
    home_advantage_elo = 55.0
    
    # Query all completed games before target game date
    prior_games_stmt = select(Game).where(
        Game.league == league,
        Game.season == season,
        Game.date < game_date,
        Game.completed == True,
        Game.home_score.isnot(None),
        Game.away_score.isnot(None)
    ).order_by(Game.date, Game.week)
    
    prior_games = list(session.scalars(prior_games_stmt).all())
    
    # Initialize ratings dict (all teams start at base_rating)
    ratings = {}
    
    # Process prior games chronologically to build up ratings
    for prior_game in prior_games:
        home_team_id = prior_game.home_team_id
        away_team_id = prior_game.away_team_id
        
        # Initialize teams if first time seeing them
        if home_team_id not in ratings:
            ratings[home_team_id] = base_rating
        if away_team_id not in ratings:
            ratings[away_team_id] = base_rating
        
        # Calculate expected scores (with home advantage)
        home_expected = 1.0 / (1.0 + 10.0 ** ((ratings[away_team_id] - (ratings[home_team_id] + home_advantage_elo)) / 400.0))
        away_expected = 1.0 - home_expected
        
        # Calculate actual outcome (1.0 for win, 0.5 for tie, 0.0 for loss)
        if prior_game.home_score > prior_game.away_score:
            home_actual = 1.0
            away_actual = 0.0
        elif prior_game.home_score < prior_game.away_score:
            home_actual = 0.0
            away_actual = 1.0
        else:  # Tie
            home_actual = 0.5
            away_actual = 0.5
        
        # Update ratings
        home_change = k_factor * (home_actual - home_expected)
        away_change = k_factor * (away_actual - away_expected)
        
        ratings[home_team_id] += home_change
        ratings[away_team_id] += away_change
    
    # Get ratings for target game teams (fallback to base_rating if not seen yet)
    home_rating = ratings.get(game.home_team_id, base_rating)
    away_rating = ratings.get(game.away_team_id, base_rating)
    
    # Compute features
    rating_diff = home_rating - away_rating
    home_field = 1.0  # Always 1.0 for home team (simplified for Task #5)
    
    features = {
        'rating_diff': rating_diff,
        'home_field': home_field,
        'season': float(season),
        'week': float(game.week)
    }
    
    return features

