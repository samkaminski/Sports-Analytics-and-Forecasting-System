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
def compute(game_id):
    """
    Compute features for a single game.
    
    Example:
        python scripts/compute_features.py --game-id NFL_2023_1_KC_BAL
    """
    try:
        db = get_db_connection()
        
        with db.get_session() as session:
            features = compute_game_features_by_id(session, game_id)
            
            # Display features
            click.echo("=" * 70)
            click.echo(f"Features for Game: {game_id}")
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

