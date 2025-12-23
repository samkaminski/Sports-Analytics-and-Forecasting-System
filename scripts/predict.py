#!/usr/bin/env python3
"""
Prediction CLI Script
Phase 1: Baseline Predictive Models (Task #7)

USE: Command-line interface to generate and display game predictions
WHAT WILL BE BUILT:
  - CLI script to generate predictions for upcoming games
  - Options to predict specific games or full weeks
  - Formatted terminal output with spread, total, favorite, probabilities

HOW IT WORKS:
  - Parses command-line arguments (league, game-id, season, week)
  - Loads trained models from models/{league}_{start}_{end}/
  - Queries database for upcoming games (completed=False, scores NULL)
  - Computes features using prediction mode (as_of_date=today)
  - Generates predictions and displays formatted output

FITS IN PROJECT:
  - Phase 1 Task #7: Main user-facing interface for getting predictions
  - Uses models trained by train.py (Task #6)
  - Uses data ingested in Phase 0
"""

import sys
import os
import logging
import click
from pathlib import Path
from datetime import date
from sqlalchemy import select
from typing import Dict

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from typing import Optional
from src.data.database import get_db_connection, Game, Team
from src.models.predict import load_models, predict_game

# Configure logging
logging.basicConfig(
    level=logging.WARNING,  # Less verbose for CLI
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def find_model_directory(league: str) -> Optional[str]:
    """
    Find the most recent model directory for a league.
    
    Args:
        league: League name (e.g., 'NFL')
    
    Returns:
        Path to model directory or None if not found
    """
    models_base = Path("models")
    if not models_base.exists():
        return None
    
    # Look for directories matching {league}_{start}_{end}
    matching_dirs = []
    for item in models_base.iterdir():
        if item.is_dir() and item.name.startswith(f"{league}_"):
            matching_dirs.append(item)
    
    if not matching_dirs:
        return None
    
    # Return the most recent (by name, which includes season range)
    # Sort by name descending to get latest seasons
    matching_dirs.sort(key=lambda x: x.name, reverse=True)
    return str(matching_dirs[0])


def format_spread(pred: Dict, home_team_name: str, away_team_name: str) -> str:
    """Format spread prediction as string."""
    if pred['spread_team'] == 'home':
        return f"{home_team_name} -{pred['spread_value']:.1f}"
    else:
        return f"{away_team_name} -{pred['spread_value']:.1f}"


@click.command()
@click.option(
    '--league',
    type=click.Choice(['NFL'], case_sensitive=False),
    required=True,
    help='League to predict (currently only NFL supported)'
)
@click.option(
    '--game-id',
    type=str,
    help='Game ID to predict (e.g., NFL_2025_17_KC_DEN)'
)
@click.option(
    '--season',
    type=int,
    help='Season year (required for --week)'
)
@click.option(
    '--week',
    type=int,
    help='Week number to predict (requires --season)'
)
@click.option(
    '--model-dir',
    type=str,
    help='Model directory path (auto-detected if not provided)'
)
def predict(league, game_id, season, week, model_dir):
    """
    Generate predictions for NFL games.
    
    Examples:
        # Predict a single game
        python scripts/predict.py --league NFL --game-id NFL_2025_17_KC_DEN
        
        # Predict a full week
        python scripts/predict.py --league NFL --season 2025 --week 17
    """
    league = league.upper()
    
    try:
        # Find model directory
        if model_dir is None:
            model_dir = find_model_directory(league)
            if model_dir is None:
                click.echo(f"Error: No models found for {league}. Train models first using scripts/train.py", err=True)
                sys.exit(1)
        
        if not os.path.exists(model_dir):
            click.echo(f"Error: Model directory not found: {model_dir}", err=True)
            sys.exit(1)
        
        click.echo(f"Loading models from: {model_dir}")
        models_dict = load_models(model_dir)
        
        # Initialize database connection
        db = get_db_connection()
        
        with db.get_session() as session:
            as_of_date = date.today()
            
            if game_id:
                # Predict single game
                game = session.scalar(select(Game).where(Game.game_id == game_id))
                if not game:
                    click.echo(f"Error: Game not found: {game_id}", err=True)
                    sys.exit(1)
                
                pred = predict_game(session, game, models_dict, as_of_date=as_of_date)
                
                # Get team names
                home_team = session.scalar(select(Team).where(Team.team_id == game.home_team_id))
                away_team = session.scalar(select(Team).where(Team.team_id == game.away_team_id))
                home_name = home_team.name if home_team else game.home_team_id
                away_name = away_team.name if away_team else game.away_team_id
                
                # Display prediction
                click.echo("")
                click.echo("=" * 70)
                click.echo(f"Prediction: {away_name} @ {home_name}")
                click.echo(f"Date: {game.date} | Week {game.week}, {game.season}")
                click.echo("=" * 70)
                click.echo("")
                click.echo(f"Spread:     {format_spread(pred, home_name, away_name)}")
                click.echo(f"Total:      {pred['predicted_total']:.1f}")
                click.echo(f"Favorite:   {home_name if pred['favorite'] == game.home_team_id else away_name}")
                click.echo("")
                click.echo(f"Win Probabilities:")
                click.echo(f"  {home_name}: {pred['p_home']:.1%}")
                click.echo(f"  {away_name}: {pred['p_away']:.1%}")
                click.echo("")
                click.echo("=" * 70)
                
            elif season and week:
                # Predict full week
                stmt = select(Game).where(
                    Game.league == league,
                    Game.season == season,
                    Game.week == week,
                    Game.completed == False,
                    Game.home_score.is_(None),
                    Game.away_score.is_(None)
                ).order_by(Game.date)
                
                games = list(session.scalars(stmt).all())
                
                if not games:
                    click.echo(f"No upcoming games found for {league} Season {season} Week {week}")
                    sys.exit(0)
                
                click.echo("")
                click.echo("=" * 70)
                click.echo(f"{league} Week {week}, {season} Predictions")
                click.echo("=" * 70)
                click.echo("")
                
                predictions = []
                for game in games:
                    try:
                        pred = predict_game(session, game, models_dict, as_of_date=as_of_date)
                        predictions.append((game, pred))
                    except Exception as e:
                        logger.warning(f"Error predicting game {game.game_id}: {e}")
                        continue
                
                # Display predictions in table format
                for game, pred in predictions:
                    home_team = session.scalar(select(Team).where(Team.team_id == game.home_team_id))
                    away_team = session.scalar(select(Team).where(Team.team_id == game.away_team_id))
                    home_name = home_team.name if home_team else game.home_team_id
                    away_name = away_team.name if away_team else game.away_team_id
                    
                    matchup = f"{away_name} @ {home_name}"
                    spread = format_spread(pred, home_name, away_name)
                    favorite = home_name if pred['favorite'] == game.home_team_id else away_name
                    
                    click.echo(f"{matchup:40s} | Spread: {spread:20s} | Total: {pred['predicted_total']:5.1f} | Favorite: {favorite:20s} | P(home): {pred['p_home']:.1%} / P(away): {pred['p_away']:.1%}")
                
                click.echo("")
                click.echo("=" * 70)
                
            else:
                click.echo("Error: Must specify either --game-id or both --season and --week", err=True)
                sys.exit(1)
        
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Prediction failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    predict()
