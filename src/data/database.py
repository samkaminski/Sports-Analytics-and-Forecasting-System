"""
Database Management Module
Phase 0: Data Ingestion and Storage

USE: Provides database connection management and schema creation utilities
WHAT WILL BE BUILT: 
  - DatabaseManager class for connection pooling and query execution
  - Database schema creation functions (SQLAlchemy 2.x style)
  - Helper functions for common database operations

HOW IT WORKS:
  - Uses SQLAlchemy 2.x for ORM and connection management
  - Provides context managers for safe database connections
  - Handles schema creation and migrations
  - Manages connection pooling for efficient data loading

FITS IN PROJECT:
  - Foundation for all data storage operations
  - Used by all ingestion modules to store fetched data
  - Used by feature engineering to query historical data
  - Used by models to retrieve training data
"""

import os
from contextlib import contextmanager
from typing import Optional, Dict
from datetime import date
from sqlalchemy import create_engine, Index, ForeignKey, UniqueConstraint
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """Base class for all database models (SQLAlchemy 2.x style)."""
    pass


class DatabaseManager:
    """
    Manages database connections and provides high-level database operations.
    
    This class handles:
    - Connection pooling for efficient data access
    - Schema creation and management
    - Transaction management
    - Error handling and logging
    """
    
    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize database manager with DATABASE_URL environment variable.
        
        Args:
            database_url: Optional database URL (defaults to DATABASE_URL env var)
        """
        self.database_url = database_url or os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        
        self.engine = None
        self.SessionLocal = None
        self._initialize_engine()
    
    def _initialize_engine(self):
        """Create SQLAlchemy engine with connection pooling."""
        # Create engine with connection pooling
        self.engine = create_engine(
            self.database_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            echo=False  # Set to True for SQL query logging
        )
        
        # Create session factory
        self.SessionLocal = sessionmaker(bind=self.engine)
    
    @contextmanager
    def get_session(self):
        """
        Context manager for database sessions.
        
        Usage:
            with db_manager.get_session() as session:
                # Use session for queries
                result = session.query(Team).all()
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def create_tables(self):
        """
        Create all database tables defined in the schema.
        
        This should be run once during initial setup.
        """
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def drop_tables(self):
        """Drop all tables (use with caution - for development only)."""
        Base.metadata.drop_all(self.engine)
        logger.warning("All database tables dropped")
    
    def execute_query(self, query: str, params: Optional[Dict] = None):
        """
        Execute a raw SQL query.
        
        Args:
            query: SQL query string
            params: Optional parameters for parameterized queries
        
        Returns:
            Query results
        """
        with self.get_session() as session:
            result = session.execute(query, params or {})
            return result.fetchall()


def get_db_connection(database_url: Optional[str] = None):
    """
    Convenience function to get a database manager instance.
    
    Usage:
        db = get_db_connection()
        with db.get_session() as session:
            # Use session
    """
    return DatabaseManager(database_url)


# Database Schema Definitions (Task #1: Minimal - teams and games only)

class Team(Base):
    """
    Teams table - stores basic team information.
    """
    __tablename__ = 'teams'
    
    team_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    league: Mapped[str]  # 'NFL' or 'NCAA'
    abbreviation: Mapped[Optional[str]] = mapped_column(default=None)
    city: Mapped[Optional[str]] = mapped_column(default=None)
    created_at: Mapped[Optional[date]] = mapped_column(default=None)
    
    __table_args__ = (
        Index('idx_team_league', 'league'),
    )


class Game(Base):
    """
    Games table - stores game results and metadata.
    """
    __tablename__ = 'games'
    
    game_id: Mapped[str] = mapped_column(primary_key=True)
    season: Mapped[int]
    week: Mapped[int]
    date: Mapped[date]
    home_team_id: Mapped[str] = mapped_column(ForeignKey('teams.team_id'))
    away_team_id: Mapped[str] = mapped_column(ForeignKey('teams.team_id'))
    league: Mapped[str]
    
    # Game results (NULL for upcoming games)
    home_score: Mapped[Optional[int]] = mapped_column(default=None)
    away_score: Mapped[Optional[int]] = mapped_column(default=None)
    completed: Mapped[bool] = mapped_column(default=False)
    
    # Metadata
    stadium: Mapped[Optional[str]] = mapped_column(default=None)
    is_neutral_site: Mapped[bool] = mapped_column(default=False)
    
    created_at: Mapped[Optional[date]] = mapped_column(default=None)
    updated_at: Mapped[Optional[date]] = mapped_column(default=None)
    
    __table_args__ = (
        Index('idx_game_season_week', 'season', 'week'),
        Index('idx_game_date', 'date'),
        Index('idx_game_league', 'league'),
    )


class TeamStats(Base):
    """
    Team statistics table - stores aggregated team stats by season.
    
    Used by: Feature engineering (to compute team strength metrics),
             Model training (as features)
    """
    __tablename__ = 'team_stats'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    team_id: Mapped[str] = mapped_column(ForeignKey('teams.team_id'))
    league: Mapped[str]
    season: Mapped[int]
    team_abbr: Mapped[str]
    
    # Basic stats
    games_played: Mapped[Optional[int]] = mapped_column(default=None)
    wins: Mapped[Optional[int]] = mapped_column(default=None)
    losses: Mapped[Optional[int]] = mapped_column(default=None)
    points_for: Mapped[Optional[int]] = mapped_column(default=None)
    points_against: Mapped[Optional[int]] = mapped_column(default=None)
    
    created_at: Mapped[Optional[date]] = mapped_column(default=None)
    updated_at: Mapped[Optional[date]] = mapped_column(default=None)
    
    __table_args__ = (
        Index('idx_team_stats_team_season', 'team_id', 'season'),
        Index('idx_team_stats_season', 'season'),
        Index('idx_team_stats_league_season', 'league', 'season'),
    )


class TeamRating(Base):
    """
    Team ratings table - stores Elo ratings and other rating systems.
    
    Used by: Feature engineering (rating difference is a key Phase 1 feature),
             Model training (as primary feature)
    """
    __tablename__ = 'team_ratings'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    league: Mapped[str]
    season: Mapped[int]
    team_id: Mapped[str] = mapped_column(ForeignKey('teams.team_id'))
    team_abbr: Mapped[str]
    team_name: Mapped[Optional[str]] = mapped_column(default=None)
    rating: Mapped[float]  # Elo rating
    as_of_date: Mapped[date]  # Date this rating is valid as of
    games_count: Mapped[int]  # Number of games used to compute this rating
    created_at: Mapped[Optional[date]] = mapped_column(default=None)
    updated_at: Mapped[Optional[date]] = mapped_column(default=None)
    
    __table_args__ = (
        Index('idx_team_rating_league_season_team', 'league', 'season', 'team_id'),
        Index('idx_team_rating_season', 'season'),
        Index('idx_team_rating_team_id', 'team_id'),
        UniqueConstraint('league', 'season', 'team_id', name='uq_team_rating_league_season_team'),
    )



