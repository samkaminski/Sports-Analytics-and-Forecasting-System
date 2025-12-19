#!/usr/bin/env python3
"""
Model Training CLI Script
Phase 1: Baseline Predictive Models

USE: Command-line interface to train models on historical data
WHAT WILL BE BUILT:
  - CLI script to train models for NFL or NCAA
  - Options to specify training date ranges
  - Model validation and evaluation output
  - Saves trained models to disk for prediction use

HOW IT WORKS:
  - Parses command-line arguments (league, seasons, etc.)
  - Initializes ModelTrainer
  - Trains models on specified historical data
  - Outputs training metrics to terminal
  - Saves models to configured directory

FITS IN PROJECT:
  - Phase 1: Trains baseline models that are then used for predictions
  - Run once (or periodically) to create/update models
  - Models are saved and loaded by predict.py script
  - Training happens offline; predictions happen on-demand
"""

import sys
import os
import logging
import click
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data.database import get_db_connection
from src.models.train import ModelTrainer

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
    help='League to train models for (NFL or NCAA)'
)
@click.option(
    '--start-season',
    type=int,
    default=2018,
    help='First season to include in training data (default: 2018)'
)
@click.option(
    '--end-season',
    type=int,
    default=2023,
    help='Last season to include in training data (default: 2023)'
)
@click.option(
    '--config',
    type=click.Path(exists=True),
    default='config/model_config.yaml',
    help='Path to model configuration file'
)
def train(league, start_season, end_season, config):
    """
    Train baseline predictive models for NFL or NCAA Football.
    
    This script trains three models:
    - Point spread prediction (Ridge regression)
    - Total points prediction (Ridge regression)
    - Win probability prediction (Logistic regression)
    
    Models are saved to the configured model directory and can be used
    by the predict.py script to generate forecasts.
    
    Example usage:
        python scripts/train.py --league NFL --start-season 2018 --end-season 2023
    """
    league = league.upper()
    
    click.echo(f"Training {league} models...")
    click.echo(f"Training seasons: {start_season}-{end_season}")
    click.echo("")
    
    try:
        # Initialize database connection
        db = get_db_connection()
        
        # Initialize trainer
        trainer = ModelTrainer(db, league, config)
        
        # Train models
        click.echo("Loading training data...")
        results = trainer.train_all_models(start_season, end_season)
        
        click.echo("")
        click.echo("=" * 70)
        click.echo("Training completed successfully!")
        click.echo("=" * 70)
        click.echo(f"Models saved to: {trainer.model_dir}")
        click.echo("")
        click.echo("You can now use predict.py to generate predictions.")
        
    except Exception as e:
        logger.error(f"Training failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    train()

