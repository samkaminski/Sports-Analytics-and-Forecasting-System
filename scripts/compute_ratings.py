#!/usr/bin/env python3
"""
Compute Ratings CLI Script
Phase 1: Feature Engineering (Task #4)

USE: Command-line interface to compute Elo ratings for teams
WHAT WILL BE BUILT:
  - CLI script to compute Elo ratings for a league/season
  - Upsert ratings into team_ratings table
  - Display summary of computed ratings

HOW IT WORKS:
  - Parses command-line arguments (league, season)
  - Computes Elo ratings from games table
  - Upserts ratings into database
  - Displays confirmation summary

FITS IN PROJECT:
  - Phase 1 Task #4: Computes team ratings needed for feature engineering
  - Ratings are stored for use in model training and predictions
"""

import sys
import logging
import click
from pathlib import Path
from datetime import date
from sqlalchemy import select

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data.database import get_db_connection, TeamRating
from src.features.ratings import compute_elo_ratings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    '--league',
    type=click.Choice(['NFL', 'NCAA'], case_sensitive=False),
    required=True,
    help='League to compute ratings for'
)
@click.option(
    '--season',
    type=int,
    required=True,
    help='Season year to compute ratings for'
)
@click.option(
    '--k-factor',
    type=float,
    default=20.0,
    help='K-factor for Elo calculation (default: 20.0)'
)
@click.option(
    '--base-rating',
    type=float,
    default=1500.0,
    help='Base Elo rating at season start (default: 1500.0)'
)
def compute(league, season, k_factor, base_rating):
    """
    Compute Elo ratings for teams in a league/season.
    
    Example:
        python scripts/compute_ratings.py --league NFL --season 2023
    """
    league = league.upper()
    
    try:
        db = get_db_connection()
        
        with db.get_session() as session:
            # Compute ratings
            ratings = compute_elo_ratings(
                session,
                league,
                season,
                k_factor=k_factor,
                base_rating=base_rating
            )
            
            if not ratings:
                click.echo(f"No ratings computed (no completed games for {league} season {season})")
                return
            
            # Upsert ratings into database
            click.echo(f"Upserting {len(ratings)} team ratings...")
            for rating in ratings:
                # Check if rating exists
                stmt = select(TeamRating).where(
                    TeamRating.league == league,
                    TeamRating.season == season,
                    TeamRating.team_id == rating.team_id
                )
                existing = session.scalar(stmt)
                
                if existing:
                    # Update existing
                    existing.team_abbr = rating.team_abbr
                    existing.team_name = rating.team_name
                    existing.rating = rating.rating
                    existing.as_of_date = rating.as_of_date
                    existing.games_count = rating.games_count
                    existing.updated_at = date.today()
                else:
                    # Insert new
                    session.add(rating)
            
            session.commit()
            
            # Sort by rating (highest first) for summary
            ratings_sorted = sorted(ratings, key=lambda r: r.rating, reverse=True)
            
            # Display summary
            click.echo("")
            click.echo("=" * 70)
            click.echo(f"Elo Ratings Computed: {league} Season {season}")
            click.echo(f"Teams updated: {len(ratings)}")
            click.echo("")
            click.echo("Top 5 Teams by Rating:")
            click.echo("-" * 70)
            
            for i, rating in enumerate(ratings_sorted[:5], 1):
                click.echo(f"{i}. {rating.team_abbr:6s} ({rating.team_name or 'N/A':30s}): {rating.rating:7.1f} (games: {rating.games_count})")
            
            click.echo("=" * 70)
        
    except Exception as e:
        logger.error(f"Rating computation failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    compute()

