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
    type=click.Choice(['NFL'], case_sensitive=False),
    required=True,
    help='League to ingest data for (NFL only for Task #1)'
)
@click.option(
    '--season',
    type=int,
    required=True,
    help='Season year to ingest'
)
@click.option(
    '--week',
    type=int,
    help='Optional week number (ingests all weeks if not specified)'
)
def ingest(league, season, week):
    """
    Ingest NFL game data for a season/week.
    
    Examples:
        # Ingest all weeks for 2023 season
        python scripts/ingest_data.py ingest --league NFL --season 2023
        
        # Ingest specific week
        python scripts/ingest_data.py ingest --league NFL --season 2023 --week 1
    """
    league = league.upper()
    
    try:
        db = get_db_connection()
        
        if league == 'NFL':
            ingester = NFLDataIngester(db)
            ingester.ingest_season(season, week)
        else:
            click.echo("Error: Only NFL supported in Task #1", err=True)
            sys.exit(1)
        
        click.echo("Data ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
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

