#!/usr/bin/env python3
"""
Prediction CLI Script
Phase 1: Baseline Predictive Models

USE: Command-line interface to generate and display game predictions
WHAT WILL BE BUILT:
  - CLI script to generate predictions for upcoming games
  - Options to predict specific weeks, dates, or all upcoming games
  - Formatted terminal output (tables or detailed)
  - Integration with trained models and database

HOW IT WORKS:
  - Parses command-line arguments (league, week, date, etc.)
  - Loads trained models from disk
  - Queries database for games to predict
  - Generates predictions using PredictionEngine
  - Formats and displays output using TerminalFormatter

FITS IN PROJECT:
  - Phase 1: Main user-facing interface for getting predictions
  - Uses models trained by train.py
  - Uses data ingested in Phase 0
  - Outputs formatted predictions to terminal (no frontend yet)
"""

import sys
import os
import logging
import click
from pathlib import Path
from datetime import datetime, date

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data.database import get_db_connection
from src.models.predict import PredictionEngine
from src.output.terminal_formatter import TerminalFormatter

# Configure logging
logging.basicConfig(
    level=logging.WARNING,  # Less verbose for CLI
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """Sports Betting Analytics - Prediction CLI"""
    pass


@cli.command()
@click.option(
    '--league',
    type=click.Choice(['NFL', 'NCAA'], case_sensitive=False),
    required=True,
    help='League to predict (NFL or NCAA)'
)
@click.option(
    '--week',
    type=int,
    help='Week number to predict (requires --season)'
)
@click.option(
    '--season',
    type=int,
    help='Season year (defaults to current year)'
)
@click.option(
    '--date',
    type=click.DateTime(formats=['%Y-%m-%d']),
    help='Date to predict games for (alternative to --week)'
)
@click.option(
    '--upcoming',
    is_flag=True,
    help='Predict all upcoming games (not yet completed)'
)
@click.option(
    '--detailed',
    is_flag=True,
    help='Show detailed predictions with key factors'
)
@click.option(
    '--game-id',
    type=str,
    help='Predict a specific game by ID'
)
def predict(league, week, season, date, upcoming, detailed, game_id):
    """
    Generate predictions for NFL or NCAA Football games.
    
    Examples:
        # Predict all upcoming games
        python scripts/predict.py predict --league NFL --upcoming
        
        # Predict a specific week
        python scripts/predict.py predict --league NFL --week 12 --season 2024
        
        # Predict games on a specific date
        python scripts/predict.py predict --league NFL --date 2024-11-24
        
        # Predict a specific game
        python scripts/predict.py predict --league NFL --game-id NFL_2024_12_KC_BUF
    """
    league = league.upper()
    
    if season is None:
        season = datetime.now().year
    
    try:
        # Initialize database connection
        db = get_db_connection()
        
        # Initialize prediction engine
        engine = PredictionEngine(db, league)
        
        # Initialize formatter
        formatter = TerminalFormatter(db)
        
        predictions = []
        
        if game_id:
            # Predict specific game
            from src.data.database import Game
            with db.get_session() as session:
                game = session.query(Game).filter_by(game_id=game_id).first()
                if not game:
                    click.echo(f"Game not found: {game_id}", err=True)
                    sys.exit(1)
                
                pred = engine.predict_game(game)
                pred['game_id'] = game.game_id
                pred['home_team_id'] = game.home_team_id
                pred['away_team_id'] = game.away_team_id
                pred['date'] = game.date
                predictions = [pred]
        
        elif upcoming:
            # Predict all upcoming games
            predictions = engine.predict_upcoming(season)
        
        elif week:
            # Predict specific week
            predictions = engine.predict_week(season, week)
        
        elif date:
            # Predict games on specific date
            from src.data.database import Game
            with db.get_session() as session:
                games = session.query(Game).filter(
                    Game.league == league,
                    Game.date == date.date(),
                    Game.completed == False
                ).all()
                
                for game in games:
                    pred = engine.predict_game(game)
                    pred['game_id'] = game.game_id
                    pred['home_team_id'] = game.home_team_id
                    pred['away_team_id'] = game.away_team_id
                    pred['date'] = game.date
                    pred['week'] = game.week
                    predictions.append(pred)
        
        else:
            click.echo("Error: Must specify --week, --date, --upcoming, or --game-id", err=True)
            sys.exit(1)
        
        if not predictions:
            click.echo("No games found to predict.")
            sys.exit(0)
        
        # Format and display output
        if detailed or game_id:
            # Detailed output for single game or detailed mode
            for pred in predictions:
                output = formatter.format_game_prediction(pred, include_factors=True)
                click.echo(output)
        else:
            # Table output for multiple games
            if week:
                output = formatter.format_week_predictions(predictions, league, season, week)
            else:
                # For upcoming or date-based, use first game's week or create summary
                first_week = predictions[0].get('week', 1) if predictions else 1
                output = formatter.format_week_predictions(predictions, league, season, first_week)
            
            click.echo(output)
        
    except Exception as e:
        logger.error(f"Prediction failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
def list_games():
    """List upcoming games in database."""
    click.echo("List games functionality - to be implemented")
    # TODO: Implement game listing


if __name__ == '__main__':
    cli()

