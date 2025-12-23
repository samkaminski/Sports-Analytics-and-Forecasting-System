#!/usr/bin/env python3
"""
Model Training CLI Script
Phase 1: Baseline Predictive Models (Task #6)

USE: Command-line interface to train models on historical data
WHAT WILL BE BUILT:
  - CLI script to train models for NFL
  - Options to specify training date ranges
  - Model validation and evaluation output
  - Saves trained models to disk for prediction use

HOW IT WORKS:
  - Parses command-line arguments (league, seasons)
  - Loads completed games from database
  - Computes features using training-safe mode (no data leakage)
  - Trains Ridge regression for margin and total, Logistic regression for win probability
  - Outputs training metrics to terminal
  - Saves models to models/{league}_{start}_{end}/ directory

FITS IN PROJECT:
  - Phase 1 Task #6: Trains baseline models that are then used for predictions
  - Run once (or periodically) to create/update models
  - Models are saved and loaded by prediction scripts
"""

import sys
import logging
import click
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data.database import get_db_connection
from src.models.train import train_models, save_models

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    '--league',
    type=click.Choice(['NFL'], case_sensitive=False),
    required=True,
    help='League to train models for (currently only NFL supported)'
)
@click.option(
    '--start-season',
    type=int,
    required=True,
    help='First season to include in training data'
)
@click.option(
    '--end-season',
    type=int,
    required=True,
    help='Last season to include in training data'
)
@click.option(
    '--test-split',
    type=float,
    default=0.2,
    help='Ratio of data to use for testing (default: 0.2)'
)
def train(league, start_season, end_season, test_split):
    """
    Train baseline predictive models for NFL.
    
    This script trains three models:
    - Margin prediction (Ridge regression) - predicts home_score - away_score
    - Total points prediction (Ridge regression) - predicts home_score + away_score
    - Win probability prediction (Logistic regression) - predicts P(home wins)
    
    Models are saved to models/{league}_{start_season}_{end_season}/ and can be used
    by prediction scripts to generate forecasts.
    
    Example usage:
        python scripts/train.py --league NFL --start-season 2020 --end-season 2022
    """
    league = league.upper()
    
    if start_season > end_season:
        click.echo("Error: --start-season must be <= --end-season", err=True)
        sys.exit(1)
    
    click.echo("=" * 70)
    click.echo(f"Training {league} Models")
    click.echo("=" * 70)
    click.echo(f"Training seasons: {start_season}-{end_season}")
    click.echo(f"Test split ratio: {test_split}")
    click.echo("")
    
    try:
        # Initialize database connection
        db = get_db_connection()
        
        # Train models
        with db.get_session() as session:
            click.echo("Loading training data and computing features...")
            results = train_models(
                session,
                league,
                start_season,
                end_season,
                test_split_ratio=test_split
            )
        
        # Save models
        output_dir = f"models/{league}_{start_season}_{end_season}"
        saved_path = save_models(results, output_dir)
        
        # Print results
        click.echo("")
        click.echo("=" * 70)
        click.echo("Training Completed Successfully!")
        click.echo("=" * 70)
        click.echo("")
        click.echo("Model Performance (Test Set):")
        click.echo(f"  Margin MAE:     {results['metrics']['margin_mae']:.2f} points")
        click.echo(f"  Total MAE:      {results['metrics']['total_mae']:.2f} points")
        click.echo(f"  Win Accuracy:   {results['metrics']['win_accuracy']:.2%}")
        click.echo(f"  Win Log Loss:    {results['metrics']['win_log_loss']:.4f}")
        click.echo("")
        click.echo(f"Training samples: {results['train_size']}")
        click.echo(f"Test samples:     {results['test_size']}")
        click.echo("")
        click.echo(f"Models saved to: {saved_path}")
        click.echo("")
        click.echo("Models saved:")
        click.echo(f"  - {saved_path}/margin_model.joblib")
        click.echo(f"  - {saved_path}/total_model.joblib")
        click.echo(f"  - {saved_path}/win_probability_model.joblib")
        click.echo(f"  - {saved_path}/metadata.json")
        click.echo("")
        
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Training failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    train()
