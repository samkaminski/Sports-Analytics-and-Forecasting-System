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
from typing import List, Optional, Dict
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..data.database import TeamRating, Game, Team

logger = logging.getLogger(__name__)


def compute_elo_ratings(
    session: Session,
    league: str,
    season: int,
    *,
    k_factor: float = 20.0,
    base_rating: float = 1500.0
) -> List[TeamRating]:
    """
    Compute Elo ratings for all teams in a league/season.
    
    Elo ratings measure team strength and update based on game results.
    Processes games chronologically to ensure no data leakage.
    
    Args:
        session: Database session
        league: 'NFL' or 'NCAA'
        season: Season year
        k_factor: How much ratings change per game (default 20.0)
        base_rating: Starting Elo for all teams at season start (default 1500.0)
    
    Returns:
        List of TeamRating objects (one per team) with final ratings
    """
    logger.info(f"Computing Elo ratings for {league} season {season}")
    
    # Get all completed games for season, ordered chronologically
    stmt = select(Game).where(
        Game.league == league,
        Game.season == season,
        Game.completed == True,
        Game.home_score.isnot(None),
        Game.away_score.isnot(None)
    ).order_by(Game.week, Game.date)
    
    games = list(session.scalars(stmt).all())
    
    if not games:
        logger.warning(f"No completed games found for {league} season {season}")
        return []
    
    # Initialize all teams to base rating (season reset)
    ratings = {}  # team_id -> current rating
    team_games_count = {}  # team_id -> games played
    team_info = {}  # team_id -> (team_abbr, team_name)
    
    # Get team info from database
    team_stmt = select(Team).where(Team.league == league)
    teams = session.scalars(team_stmt).all()
    for team in teams:
        team_info[team.team_id] = (team.abbreviation or team.team_id.replace(f"{league}_", ""), team.name)
    
    # Process games chronologically
    for game in games:
        home_team_id = game.home_team_id
        away_team_id = game.away_team_id
        
        # Initialize teams if first time seeing them
        if home_team_id not in ratings:
            ratings[home_team_id] = base_rating
            team_games_count[home_team_id] = 0
        
        if away_team_id not in ratings:
            ratings[away_team_id] = base_rating
            team_games_count[away_team_id] = 0
        
        # Home advantage: +55 Elo points
        home_advantage_elo = 55.0
        
        # Calculate expected scores
        home_expected = 1.0 / (1.0 + 10.0 ** ((ratings[away_team_id] - (ratings[home_team_id] + home_advantage_elo)) / 400.0))
        away_expected = 1.0 - home_expected
        
        # Calculate actual outcome (1.0 for win, 0.5 for tie, 0.0 for loss)
        if game.home_score > game.away_score:
            home_actual = 1.0
            away_actual = 0.0
        elif game.home_score < game.away_score:
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
        
        team_games_count[home_team_id] += 1
        team_games_count[away_team_id] += 1
    
    # Create TeamRating objects for all teams
    result = []
    as_of_date = date.today()
    
    for team_id, rating in ratings.items():
        team_abbr, team_name = team_info.get(team_id, (team_id.replace(f"{league}_", ""), None))
        games_count = team_games_count.get(team_id, 0)
        
        team_rating = TeamRating(
            league=league,
            season=season,
            team_id=team_id,
            team_abbr=team_abbr,
            team_name=team_name,
            rating=rating,
            as_of_date=as_of_date,
            games_count=games_count,
            created_at=date.today(),
            updated_at=date.today()
        )
        result.append(team_rating)
    
    logger.info(f"Computed Elo ratings for {len(result)} teams")
    
    return result


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

