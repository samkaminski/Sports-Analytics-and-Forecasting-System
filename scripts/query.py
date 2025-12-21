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

from src.data.database import get_db_connection, Game, Team

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
def games(league, season, week, format):
    """
    Query games from database.
    
    Examples:
        python scripts/query.py games --league NFL --season 2023 --week 1
        python scripts/query.py games --league NFL --season 2023 --format json
    """
    league = league.upper()
    
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
                    
                    score_str = ""
                    if game.completed and game.home_score is not None:
                        score_str = f"{game.away_score}-{game.home_score}"
                    
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


if __name__ == '__main__':
    cli()

