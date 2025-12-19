#!/usr/bin/env python3
"""
Data Ingestion CLI Script
Phase 0: Data Ingestion and Storage

USE: Command-line interface to ingest data from various sources
WHAT WILL BE BUILT:
  - CLI script to fetch and store NFL data
  - CLI script to fetch and store NCAA data
  - CLI script to fetch and store betting odds
  - Options for historical bulk ingestion or incremental updates

HOW IT WORKS:
  - Parses command-line arguments (league, data type, date range)
  - Initializes appropriate ingester (NFL, NCAA, or Odds)
  - Fetches data from APIs or scrapes websites
  - Transforms and stores data in PostgreSQL database
  - Handles errors and rate limiting

FITS IN PROJECT:
  - Phase 0: Populates database with data needed for modeling
  - Run once for historical data, then scheduled for weekly updates
  - Provides data foundation for Phase 1 feature engineering and modeling
"""

import sys
import os
import logging
import click
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data.database import get_db_connection, DatabaseManager
from src.data.nfl_ingestion import NFLDataIngester
from src.data.ncaa_ingestion import NCAADataIngester
from src.data.odds_ingestion import OddsIngester

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """Sports Betting Analytics - Data Ingestion CLI"""
    pass


@cli.command()
@click.option(
    '--league',
    type=click.Choice(['NFL', 'NCAA'], case_sensitive=False),
    required=True,
    help='League to ingest data for'
)
@click.option(
    '--historical',
    is_flag=True,
    help='Ingest historical data (multiple seasons)'
)
@click.option(
    '--start-season',
    type=int,
    help='First season to ingest (for historical mode)'
)
@click.option(
    '--end-season',
    type=int,
    help='Last season to ingest (for historical mode)'
)
@click.option(
    '--current',
    is_flag=True,
    help='Update current season data'
)
@click.option(
    '--season',
    type=int,
    help='Season to update (for current mode)'
)
@click.option(
    '--week',
    type=int,
    help='Week to update (optional, for current mode)'
)
def ingest(league, historical, start_season, end_season, current, season, week):
    """
    Ingest game and team statistics data.
    
    Examples:
        # Ingest historical NFL data
        python scripts/ingest_data.py ingest --league NFL --historical --start-season 2018 --end-season 2023
        
        # Update current NFL season
        python scripts/ingest_data.py ingest --league NFL --current --season 2024
        
        # Update specific week
        python scripts/ingest_data.py ingest --league NFL --current --season 2024 --week 12
    """
    league = league.upper()
    
    try:
        db = get_db_connection()
        
        if league == 'NFL':
            ingester = NFLDataIngester(db)
        else:
            ingester = NCAADataIngester(db)
        
        if historical:
            if not start_season or not end_season:
                click.echo("Error: --start-season and --end-season required for historical mode", err=True)
                sys.exit(1)
            
            click.echo(f"Ingesting historical {league} data: {start_season}-{end_season}")
            ingester.ingest_historical_data(start_season, end_season)
        
        elif current:
            if not season:
                season = datetime.now().year
            
            click.echo(f"Updating {league} data for season {season}, week {week or 'all'}")
            ingester.update_current_season(season, week)
        
        else:
            click.echo("Error: Must specify --historical or --current", err=True)
            sys.exit(1)
        
        click.echo("Data ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    '--league',
    type=click.Choice(['NFL', 'NCAA'], case_sensitive=False),
    required=True,
    help='League to fetch odds for'
)
def ingest_odds(league):
    """
    Ingest current betting odds.
    
    Example:
        python scripts/ingest_data.py ingest-odds --league NFL
    """
    league = league.upper()
    
    try:
        db = get_db_connection()
        ingester = OddsIngester(db)
        
        click.echo(f"Fetching current odds for {league}...")
        ingester.update_current_odds(league)
        
        click.echo("Odds ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"Odds ingestion failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
def init_db():
    """
    Initialize database schema (create tables).
    
    Run this once before first data ingestion.
    
    Example:
        python scripts/ingest_data.py init-db
    """
    try:
        db = get_db_connection()
        
        click.echo("Creating database tables...")
        db.create_tables()
        
        click.echo("Database initialization completed!")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()

