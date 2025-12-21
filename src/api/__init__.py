# FastAPI API Module
# 
# USE: This package contains FastAPI endpoints for querying data
# WHAT WILL BE BUILT: REST API endpoints for games, teams, and future predictions
# HOW IT WORKS: FastAPI app with dependency injection for database sessions
# FITS IN PROJECT: Phase 0 - provides queryable API for ingested data

from .main import app
from .dependencies import get_db_session

__all__ = ['app', 'get_db_session']

