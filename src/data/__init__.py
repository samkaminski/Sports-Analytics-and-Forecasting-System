# Phase 0: Data Ingestion Module
# 
# USE: This package contains all data ingestion and storage functionality
# WHAT WILL BE BUILT: Modules to fetch data from various sources (APIs, scraping)
#   and store it in PostgreSQL database
# HOW IT WORKS: Each module handles a specific data source or data type,
#   providing functions to fetch, transform, and load data
# FITS IN PROJECT: Phase 0 foundation - without this, we have no data to model

from .database import DatabaseManager, get_db_connection
from .nfl_ingestion import NFLDataIngester
from .ncaa_ingestion import NCAADataIngester
from .odds_ingestion import OddsIngester

__all__ = [
    'DatabaseManager',
    'get_db_connection',
    'NFLDataIngester',
    'NCAADataIngester',
    'OddsIngester',
]

