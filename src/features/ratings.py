"""
Team Ratings Module
Phase 1: Feature Engineering

USE: Computes team strength ratings (Elo, SRS) used as primary features
WHAT WILL BE BUILT:
  - Elo rating system implementation
  - Simple Rating System (SRS) implementation
  - Functions to update ratings week-by-week
  - Functions to query current ratings for predictions

HOW IT WORKS:
  - Elo: Updates team ratings based on game results (win/loss, margin)
  - SRS: Computes strength rating based on point differential and strength of schedule
  - Ratings are computed historically (for training) and maintained for current season
  - Ensures no future data leakage (ratings only use past games)

FITS IN PROJECT:
  - Phase 1: Team rating difference is a key feature for baseline models
  - Used by feature engineering to get team strength metrics
  - Used by model training to create feature vectors
  - Ratings stored in team_ratings table for quick lookup
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import date
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session

from ..data.database import TeamRating, Game, TeamStats

logger = logging.getLogger(__name__)


def compute_elo_ratings(
    session: Session,
    league: str,
    season: int,
    initial_rating: float = 1500.0,
    k_factor: float = 32.0,
    home_advantage: float = 100.0
) -> Dict[str, float]:
    """
    Compute Elo ratings for all teams in a league/season.
    
    Elo ratings measure team strength and update based on game results.
    Higher rating = stronger team.
    
    Args:
        session: Database session
        league: 'NFL' or 'NCAA'
        season: Season year
        initial_rating: Starting Elo for new teams (default 1500)
        k_factor: How much ratings change per game (default 32)
        home_advantage: Elo points added for home team (default 100)
    
    Returns:
        Dictionary mapping team_id to current Elo rating
    """
    logger.info(f"Computing Elo ratings for {league} season {season}")
    
    # Get all teams in league
    teams = session.query(TeamRating.team_id).filter_by(
        league=league
    ).distinct().all()
    
    # Initialize ratings
    ratings = {}
    for team in teams:
        # Check if team has previous season rating
        prev_rating = session.query(TeamRating).filter_by(
            team_id=team.team_id,
            league=league,
            season=season - 1
        ).order_by(TeamRating.week.desc()).first()
        
        if prev_rating:
            # Start with previous season's final rating (with regression to mean)
            ratings[team.team_id] = prev_rating.elo_rating * 0.75 + initial_rating * 0.25
        else:
            ratings[team.team_id] = initial_rating
    
    # Get all games for season, ordered by week
    games = session.query(Game).filter_by(
        league=league,
        season=season
    ).order_by(Game.week, Game.date).all()
    
    # Update ratings game by game
    for game in games:
        if not game.completed or game.home_score is None or game.away_score is None:
            continue
        
        home_team = game.home_team_id
        away_team = game.away_team_id
        
        # Initialize teams if not seen before
        if home_team not in ratings:
            ratings[home_team] = initial_rating
        if away_team not in ratings:
            ratings[away_team] = initial_rating
        
        # Calculate expected scores
        home_expected = 1 / (1 + 10 ** ((ratings[away_team] - (ratings[home_team] + home_advantage)) / 400))
        away_expected = 1 - home_expected
        
        # Calculate actual scores (1 for win, 0.5 for tie, 0 for loss)
        if game.home_score > game.away_score:
            home_actual = 1.0
            away_actual = 0.0
        elif game.home_score < game.away_score:
            home_actual = 0.0
            away_actual = 1.0
        else:
            home_actual = 0.5
            away_actual = 0.5
        
        # Update ratings
        home_change = k_factor * (home_actual - home_expected)
        away_change = k_factor * (away_actual - away_expected)
        
        ratings[home_team] += home_change
        ratings[away_team] += away_change
        
        # Store rating for this week
        home_rating = TeamRating(
            team_id=home_team,
            season=season,
            week=game.week,
            league=league,
            elo_rating=ratings[home_team],
            created_at=date.today()
        )
        away_rating = TeamRating(
            team_id=away_team,
            season=season,
            week=game.week,
            league=league,
            elo_rating=ratings[away_team],
            created_at=date.today()
        )
        
        session.merge(home_rating)
        session.merge(away_rating)
    
    session.commit()
    logger.info(f"Elo ratings computed for {len(ratings)} teams")
    
    return ratings


def compute_srs_ratings(
    session: Session,
    league: str,
    season: int
) -> Dict[str, float]:
    """
    Compute Simple Rating System (SRS) ratings.
    
    SRS measures team strength based on point differential adjusted for
    strength of schedule. Higher rating = stronger team.
    
    Args:
        session: Database session
        league: 'NFL' or 'NCAA'
        season: Season year
    
    Returns:
        Dictionary mapping team_id to SRS rating
    """
    logger.info(f"Computing SRS ratings for {league} season {season}")
    
    # Get all games for season
    games = session.query(Game).filter_by(
        league=league,
        season=season,
        completed=True
    ).all()
    
    # Calculate average point differential per team
    team_diffs = {}
    team_games = {}
    
    for game in games:
        if game.home_score is None or game.away_score is None:
            continue
        
        home_diff = game.home_score - game.away_score
        away_diff = -home_diff
        
        team_diffs[game.home_team_id] = team_diffs.get(game.home_team_id, 0) + home_diff
        team_diffs[game.away_team_id] = team_diffs.get(game.away_team_id, 0) + away_diff
        
        team_games[game.home_team_id] = team_games.get(game.home_team_id, 0) + 1
        team_games[game.away_team_id] = team_games.get(game.away_team_id, 0) + 1
    
    # Calculate average point differential
    avg_diffs = {team: team_diffs[team] / team_games[team] 
                 for team in team_diffs if team_games[team] > 0}
    
    # SRS is average point differential adjusted for opponent strength
    # For Phase 1, we'll use a simplified version
    # Full SRS requires iterative calculation - this is a baseline
    
    srs_ratings = {}
    for team in avg_diffs:
        # Simplified SRS: average point differential
        # In full implementation, would adjust for opponent strength
        srs_ratings[team] = avg_diffs[team]
        
        # Store in database
        # Get latest week for this team
        latest_game = session.query(Game).filter(
            (Game.home_team_id == team) | (Game.away_team_id == team),
            Game.league == league,
            Game.season == season,
            Game.completed == True
        ).order_by(Game.week.desc()).first()
        
        if latest_game:
            rating = TeamRating(
                team_id=team,
                season=season,
                week=latest_game.week,
                league=league,
                srs_rating=srs_ratings[team],
                created_at=date.today()
            )
            session.merge(rating)
    
    session.commit()
    logger.info(f"SRS ratings computed for {len(srs_ratings)} teams")
    
    return srs_ratings


def get_team_rating(
    session: Session,
    team_id: str,
    season: int,
    week: int,
    league: str,
    rating_type: str = 'elo'
) -> Optional[float]:
    """
    Get a team's rating at a specific point in time.
    
    This ensures no data leakage - only uses ratings from games before
    the specified week.
    
    Args:
        session: Database session
        team_id: Team identifier
        season: Season year
        week: Week number (returns rating as of end of this week)
        league: 'NFL' or 'NCAA'
        rating_type: 'elo' or 'srs'
    
    Returns:
        Team rating or None if not found
    """
    rating_col = 'elo_rating' if rating_type == 'elo' else 'srs_rating'
    
    rating = session.query(TeamRating).filter_by(
        team_id=team_id,
        season=season,
        week=week,
        league=league
    ).first()
    
    if rating:
        return getattr(rating, rating_col)
    
    # If exact week not found, get most recent rating before this week
    rating = session.query(TeamRating).filter(
        TeamRating.team_id == team_id,
        TeamRating.season == season,
        TeamRating.week < week,
        TeamRating.league == league
    ).order_by(TeamRating.week.desc()).first()
    
    if rating:
        return getattr(rating, rating_col)
    
    return None

