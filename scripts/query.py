#!/usr/bin/env python3
"""
Query CLI Script
Phase 0: Data Ingestion and Storage (Task #2)

USE: Command-line interface to query data directly from database
WHAT WILL BE BUILT:
  - CLI script to query games from database
  - Direct database queries (not via FastAPI)
  - Tabular or JSON output formatting

HOW IT WORKS:
  - Parses command-line arguments
  - Queries database directly using SQLAlchemy
  - Formats and displays results

FITS IN PROJECT:
  - Phase 0 Task #2: Provides CLI query capability for games
  - Allows testing data ingestion without FastAPI
"""

import sys
import logging
import click
import json
from pathlib import Path
from datetime import date
from sqlalchemy import select
from tabulate import tabulate

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data.database import get_db_connection, Game, Team, TeamRating

# Configure logging
logging.basicConfig(
    level=logging.WARNING,  # Less verbose for CLI
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """Sports Betting Analytics - Query CLI"""
    pass


@cli.command()
@click.option(
    '--league',
    type=click.Choice(['NFL', 'NCAA'], case_sensitive=False),
    required=True,
    help='League to query'
)
@click.option(
    '--season',
    type=int,
    required=True,
    help='Season year'
)
@click.option(
    '--week',
    type=int,
    help='Week number (optional)'
)
@click.option(
    '--format',
    type=click.Choice(['table', 'json'], case_sensitive=False),
    default='table',
    help='Output format (default: table)'
)
@click.option(
    '--refresh',
    is_flag=True,
    default=False,
    help='Refresh games from source before displaying (requires --week)'
)
def games(league, season, week, format, refresh):
    """
    Query games from database.
    
    Examples:
        python scripts/query.py games --league NFL --season 2023 --week 1
        python scripts/query.py games --league NFL --season 2023 --format json
        python scripts/query.py games --league NFL --season 2025 --week 16 --refresh
    """
    league = league.upper()
    
    # Handle refresh flag
    if refresh:
        if week is None:
            click.echo("Error: --refresh requires --week to be specified", err=True)
            sys.exit(1)
        
        if league != 'NFL':
            click.echo(f"Error: --refresh is only supported for NFL", err=True)
            sys.exit(1)
        
        try:
            from src.data.nfl_ingestion import NFLDataIngester
            
            click.echo(f"Refreshing games for {league} season {season} week {week}...")
            db = get_db_connection()
            ingester = NFLDataIngester(db)
            
            # Fetch and ingest games (include_future=True to get all games in the week)
            games_df = ingester.fetch_games(season, week, include_future=True)
            if not games_df.empty:
                ingester.ingest_games(games_df)
                click.echo(f"Refreshed {len(games_df)} games.")
            else:
                click.echo("No games found to refresh.")
        except Exception as e:
            logger.error(f"Refresh failed: {e}", exc_info=True)
            click.echo(f"Error during refresh: {e}", err=True)
            sys.exit(1)
    
    try:
        db = get_db_connection()
        
        with db.get_session() as session:
            # Build query
            stmt = select(Game).where(
                Game.league == league,
                Game.season == season
            )
            
            if week is not None:
                stmt = stmt.where(Game.week == week)
            
            stmt = stmt.order_by(Game.week, Game.date)
            
            games = session.scalars(stmt).all()
            
            if not games:
                click.echo(f"No games found for {league} season {season}" + (f" week {week}" if week else ""))
                return
            
            # Get team names
            team_cache = {}
            for game in games:
                if game.home_team_id not in team_cache:
                    home_team = session.scalar(select(Team).where(Team.team_id == game.home_team_id))
                    team_cache[game.home_team_id] = home_team.name if home_team else game.home_team_id
                if game.away_team_id not in team_cache:
                    away_team = session.scalar(select(Team).where(Team.team_id == game.away_team_id))
                    team_cache[game.away_team_id] = away_team.name if away_team else game.away_team_id
            
            if format == 'json':
                # JSON output
                results = []
                for game in games:
                    results.append({
                        'game_id': game.game_id,
                        'season': game.season,
                        'week': game.week,
                        'date': game.date.isoformat() if game.date else None,
                        'home_team': team_cache.get(game.home_team_id, game.home_team_id),
                        'away_team': team_cache.get(game.away_team_id, game.away_team_id),
                        'home_score': game.home_score,
                        'away_score': game.away_score,
                        'completed': game.completed,
                        'stadium': game.stadium
                    })
                click.echo(json.dumps(results, indent=2))
            else:
                # Table output
                table_data = []
                for game in games:
                    home_name = team_cache.get(game.home_team_id, game.home_team_id)
                    away_name = team_cache.get(game.away_team_id, game.away_team_id)
                    
                    # Show score if both scores are present, otherwise show TBD
                    if game.home_score is not None and game.away_score is not None:
                        score_str = f"{game.away_score}-{game.home_score}"
                    else:
                        score_str = "TBD"
                    
                    table_data.append([
                        game.week,
                        game.date.strftime('%Y-%m-%d') if game.date else 'N/A',
                        f"{away_name} @ {home_name}",
                        score_str,
                        game.stadium or 'N/A'
                    ])
                
                headers = ['Week', 'Date', 'Matchup', 'Score', 'Stadium']
                click.echo(tabulate(table_data, headers=headers, tablefmt='grid'))
        
    except Exception as e:
        logger.error(f"Query failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    '--league',
    type=click.Choice(['NFL', 'NCAA'], case_sensitive=False),
    required=True,
    help='League to query'
)
@click.option(
    '--season',
    type=int,
    required=True,
    help='Season year'
)
@click.option(
    '--team',
    type=str,
    help='Team abbreviation (e.g., KC, BUF) - if provided, shows only that team'
)
@click.option(
    '--top',
    type=int,
    default=10,
    help='Number of top teams to show if --team not provided (default: 10)'
)
def ratings(league, season, team, top):
    """
    Query team ratings from database.
    
    Examples:
        python scripts/query.py ratings --league NFL --season 2023
        python scripts/query.py ratings --league NFL --season 2023 --team KC
    """
    league = league.upper()
    
    try:
        db = get_db_connection()
        
        with db.get_session() as session:
            # Build query
            stmt = select(TeamRating).where(
                TeamRating.league == league,
                TeamRating.season == season
            )
            
            if team:
                # Filter by team abbreviation
                team_abbr = team.upper()
                stmt = stmt.where(TeamRating.team_abbr == team_abbr)
            
            stmt = stmt.order_by(TeamRating.rating.desc())
            
            ratings_list = list(session.scalars(stmt).all())
            
            if not ratings_list:
                click.echo(f"No ratings found for {league} season {season}" + (f" team {team}" if team else ""))
                return
            
            # Display results
            if team:
                # Single team view
                rating = ratings_list[0]
                click.echo("=" * 70)
                click.echo(f"Team Rating: {rating.team_abbr} ({rating.team_name or 'N/A'})")
                click.echo(f"League: {league} | Season: {season}")
                click.echo("-" * 70)
                click.echo(f"Elo Rating: {rating.rating:.1f}")
                click.echo(f"Games Played: {rating.games_count}")
                click.echo(f"As of Date: {rating.as_of_date}")
                
                # Find ranking position
                all_ratings_stmt = select(TeamRating).where(
                    TeamRating.league == league,
                    TeamRating.season == season
                ).order_by(TeamRating.rating.desc())
                all_ratings = list(session.scalars(all_ratings_stmt).all())
                rank = next((i + 1 for i, r in enumerate(all_ratings) if r.team_id == rating.team_id), None)
                if rank:
                    click.echo(f"Rank: #{rank} of {len(all_ratings)}")
                click.echo("=" * 70)
            else:
                # Top N teams table
                ratings_to_show = ratings_list[:top]
                table_data = []
                for i, rating in enumerate(ratings_to_show, 1):
                    table_data.append([
                        i,
                        rating.team_abbr,
                        rating.team_name or 'N/A',
                        f"{rating.rating:.1f}",
                        rating.games_count
                    ])
                
                headers = ['Rank', 'Team', 'Name', 'Rating', 'Games']
                click.echo(f"Top {len(ratings_to_show)} Teams by Elo Rating: {league} Season {season}")
                click.echo("")
                click.echo(tabulate(table_data, headers=headers, tablefmt='grid'))
        
    except Exception as e:
        logger.error(f"Query failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()

