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


def normalize_team_id(team_id: str, league: str) -> str:
    """
    Normalize team ID to canonical format.
    
    Handles both 'NFL_KC' and 'KC' formats, converting to 'KC' (without prefix).
    This ensures consistent dict key lookups regardless of how team IDs are stored.
    
    Args:
        team_id: Team ID in any format (e.g., 'NFL_KC', 'KC')
        league: League name (e.g., 'NFL')
    
    Returns:
        Normalized team ID without league prefix (e.g., 'KC')
    """
    if not team_id:
        return team_id
    
    # Remove league prefix if present (e.g., 'NFL_KC' -> 'KC')
    prefix = f"{league}_"
    if team_id.startswith(prefix):
        return team_id[len(prefix):]
    
    # Already normalized (no prefix)
    return team_id


def compute_elo_ratings(
    session: Session,
    league: str,
    season: int,
    *,
    k_factor: float = 20.0,
    base_rating: float = 1500.0,
    mean_reversion_factor: float = 0.33
) -> List[TeamRating]:
    """
    Compute Elo ratings for all teams in a league/season.
    
    Elo ratings measure team strength and update based on game results.
    Processes games chronologically to ensure no data leakage.
    
    Implements season reset with mean reversion: each team's rating is
    regressed toward the league mean (1500) by mean_reversion_factor (default 33%).
    This prevents stale ratings from previous seasons and accounts for
    offseason roster changes.
    
    Args:
        session: Database session
        league: 'NFL' or 'NCAA'
        season: Season year
        k_factor: How much ratings change per game (default 20.0)
        base_rating: Starting Elo for all teams at season start (default 1500.0)
        mean_reversion_factor: Fraction to regress toward mean (0.33 = 33% toward 1500)
    
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
    
    # Initialize ratings with mean reversion from previous season
    # This ensures no data leakage: ratings start fresh each season
    ratings = {}  # team_id -> current rating
    team_games_count = {}  # team_id -> games played
    team_info = {}  # team_id -> (team_abbr, team_name)
    
    # Get team info from database
    # Normalize team IDs to canonical format (without league prefix) for consistent dict keys
    team_stmt = select(Team).where(Team.league == league)
    teams = session.scalars(team_stmt).all()
    for team in teams:
        normalized_id = normalize_team_id(team.team_id, league)
        if normalized_id:
            team_info[normalized_id] = (team.abbreviation or normalized_id, team.name)
        else:
            logger.warning(f"Could not normalize team_id '{team.team_id}' for league {league}, skipping")
    
    # Apply mean reversion: get previous season's final ratings and regress toward mean
    # This prevents stale ratings and accounts for offseason changes
    if season > 2000:  # Only if we have previous seasons
        prev_season = season - 1
        prev_ratings_stmt = select(TeamRating).where(
            TeamRating.league == league,
            TeamRating.season == prev_season
        )
        # Normalize previous season team IDs for consistent lookup
        prev_ratings = {
            normalize_team_id(r.team_id, league): r.rating 
            for r in session.scalars(prev_ratings_stmt).all()
            if normalize_team_id(r.team_id, league)
        }
        
        for team_id in team_info.keys():
            if team_id in prev_ratings:
                # Mean reversion: new_rating = old_rating * (1 - factor) + base_rating * factor
                prev_rating = prev_ratings[team_id]
                ratings[team_id] = prev_rating * (1 - mean_reversion_factor) + base_rating * mean_reversion_factor
            else:
                # New team or no previous rating: start at base
                ratings[team_id] = base_rating
            # Initialize games count for all teams
            team_games_count[team_id] = 0
    else:
        # First season or no previous data: all teams start at base rating
        for team_id in team_info.keys():
            ratings[team_id] = base_rating
            team_games_count[team_id] = 0
    
    # Process games chronologically
    for game in games:
        # Normalize team IDs from games table to match dict keys
        home_team_id = normalize_team_id(game.home_team_id, league)
        away_team_id = normalize_team_id(game.away_team_id, league)
        
        # Skip if normalization failed
        if not home_team_id or not away_team_id:
            logger.warning(f"Could not normalize team IDs for game {game.game_id} "
                         f"(home: {game.home_team_id}, away: {game.away_team_id}), skipping")
            continue
        
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
    # Note: team_ratings table expects team_id in original format (with league prefix)
    # So we need to convert back from normalized format for storage
    result = []
    as_of_date = date.today()
    
    for normalized_id, rating in ratings.items():
        team_abbr, team_name = team_info.get(normalized_id, (normalized_id, None))
        games_count = team_games_count.get(normalized_id, 0)
        
        # Convert back to full format for storage (team_ratings table expects 'NFL_KC' format)
        # Try to find original team_id from teams table, or reconstruct it
        stored_team_id = f"{league}_{normalized_id}"  # Default: reconstruct with prefix
        
        # Try to find original format from teams table
        team_stmt = select(Team).where(Team.league == league)
        for team in session.scalars(team_stmt).all():
            if normalize_team_id(team.team_id, league) == normalized_id:
                stored_team_id = team.team_id
                break
        
        team_rating = TeamRating(
            league=league,
            season=season,
            team_id=stored_team_id,  # Store in original format
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

