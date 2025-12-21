"""
FastAPI Dependencies
Phase 0: Data Ingestion and Storage

USE: Provides database session dependency injection for FastAPI routes
WHAT WILL BE BUILT: Database session dependency using SQLAlchemy 2.x
HOW IT WORKS: FastAPI dependency injection pattern for database access
FITS IN PROJECT: Enables FastAPI routes to access database via dependency injection
"""

from typing import Generator
from sqlalchemy.orm import Session
from src.data.database import DatabaseManager, get_db_connection

_db_manager: DatabaseManager = None


def get_db_manager() -> DatabaseManager:
    """Get or create database manager singleton."""
    global _db_manager
    if _db_manager is None:
        _db_manager = get_db_connection()
    return _db_manager


def get_db_session() -> Generator[Session, None, None]:
    """
    FastAPI dependency for database sessions.
    
    Usage:
        @app.get("/games")
        def get_games(session: Session = Depends(get_db_session)):
            ...
    """
    db = get_db_manager()
    with db.get_session() as session:
        yield session

