#!/usr/bin/env python3
"""
Compute Features CLI Script
Phase 1: Feature Engineering (Task #5)

USE: Command-line interface to compute features for a single game
WHAT WILL BE BUILT:
  - CLI script to compute features for a game by game_id
  - Validates no data leakage (only uses data before game date)
  - Displays feature vector in readable format

HOW IT WORKS:
  - Parses game_id argument
  - Loads game from database
  - Computes features using only historical data
  - Displays features as key-value pairs

FITS IN PROJECT:
  - Phase 1 Task #5: Validates feature computation is leakage-safe
  - Features will be used in model training and predictions
"""

import sys
import logging
import click
from pathlib import Path
from datetime import date

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data.database import get_db_connection
from src.features.feature_engineering import compute_game_features_by_id

# Configure logging
logging.basicConfig(
    level=logging.WARNING,  # Less verbose for CLI
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    '--game-id',
    type=str,
    required=True,
    help='Game identifier (e.g., NFL_2023_1_KC_BAL)'
)
@click.option(
    '--as-of',
    type=str,
    default=None,
    help='Date for prediction-time ratings (e.g., "today" or "YYYY-MM-DD"). '
         'If omitted, uses training-safe mode (on-the-fly computation).'
)
def compute(game_id, as_of):
    """
    Compute features for a single game.
    
    Examples:
        # Historical game (training-safe mode)
        python scripts/compute_features.py --game-id NFL_2023_11_BAL_CIN
        
        # Future game (prediction-time mode)
        python scripts/compute_features.py --game-id NFL_2025_17_KC_DEN --as-of today
        python scripts/compute_features.py --game-id NFL_2025_17_KC_DEN --as-of 2025-01-15
    """
    try:
        # Parse as_of_date
        as_of_date = None
        if as_of:
            if as_of.lower() == 'today':
                as_of_date = date.today()
            else:
                try:
                    as_of_date = date.fromisoformat(as_of)
                except ValueError:
                    click.echo(f"Error: Invalid date format '{as_of}'. Use 'today' or 'YYYY-MM-DD'.", err=True)
                    sys.exit(1)
        
        db = get_db_connection()
        
        with db.get_session() as session:
            features = compute_game_features_by_id(session, game_id, as_of_date=as_of_date)
            
            # Display features
            click.echo("=" * 70)
            click.echo(f"Features for Game: {game_id}")
            if as_of_date:
                click.echo(f"Prediction Mode (as_of: {as_of_date})")
            else:
                click.echo("Training Mode (on-the-fly computation)")
            click.echo("=" * 70)
            click.echo("")
            
            for key, value in sorted(features.items()):
                click.echo(f"  {key:20s}: {value:10.2f}")
            
            click.echo("")
            click.echo("=" * 70)
        
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Feature computation failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    compute()

