"""
Database Management Module
Phase 0: Data Ingestion and Storage

USE: Provides database connection management and schema creation utilities
WHAT WILL BE BUILT: 
  - DatabaseManager class for connection pooling and query execution
  - Database schema creation functions
  - Helper functions for common database operations

HOW IT WORKS:
  - Uses SQLAlchemy for ORM and connection management
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
from typing import Optional, Dict, Any
import yaml
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Date, Boolean, Text, ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class DatabaseManager:
    """
    Manages database connections and provides high-level database operations.
    
    This class handles:
    - Connection pooling for efficient data access
    - Schema creation and management
    - Transaction management
    - Error handling and logging
    """
    
    def __init__(self, config_path: str = "config/database_config.yaml"):
        """
        Initialize database manager with configuration.
        
        Args:
            config_path: Path to database configuration YAML file
        """
        self.config = self._load_config(config_path)
        self.engine = None
        self.SessionLocal = None
        self._initialize_engine()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load database configuration from YAML file."""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config['database']
    
    def _initialize_engine(self):
        """Create SQLAlchemy engine with connection pooling."""
        # Build connection string
        # In production, get password from environment variable
        password = os.getenv('DB_PASSWORD', self.config.get('password', ''))
        
        connection_string = (
            f"postgresql://{self.config['user']}:{password}@"
            f"{self.config['host']}:{self.config['port']}/{self.config['name']}"
        )
        
        # Create engine with connection pooling
        self.engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=self.config.get('pool_size', 5),
            max_overflow=self.config.get('max_overflow', 10),
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


def get_db_connection(config_path: str = "config/database_config.yaml"):
    """
    Convenience function to get a database manager instance.
    
    Usage:
        db = get_db_connection()
        with db.get_session() as session:
            # Use session
    """
    return DatabaseManager(config_path)


# Database Schema Definitions
# These tables store all the data needed for modeling

class Team(Base):
    """
    Teams table - stores basic team information.
    
    Used by: All modules that need to reference teams
    """
    __tablename__ = 'teams'
    
    team_id = Column(String(50), primary_key=True)
    name = Column(String(100), nullable=False)
    league = Column(String(10), nullable=False)  # 'NFL' or 'NCAA'
    abbreviation = Column(String(10))
    city = Column(String(50))
    created_at = Column(Date)
    
    __table_args__ = (
        Index('idx_team_league', 'league'),
    )


class Game(Base):
    """
    Games table - stores game results and metadata.
    
    Used by: Feature engineering (to get historical games),
             Model training (as target variables),
             Prediction (to identify upcoming games)
    """
    __tablename__ = 'games'
    
    game_id = Column(String(50), primary_key=True)
    season = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    home_team_id = Column(String(50), ForeignKey('teams.team_id'), nullable=False)
    away_team_id = Column(String(50), ForeignKey('teams.team_id'), nullable=False)
    league = Column(String(10), nullable=False)
    
    # Game results (NULL for upcoming games)
    home_score = Column(Integer)
    away_score = Column(Integer)
    completed = Column(Boolean, default=False)
    
    # Metadata
    stadium = Column(String(100))
    is_neutral_site = Column(Boolean, default=False)
    
    created_at = Column(Date)
    updated_at = Column(Date)
    
    __table_args__ = (
        Index('idx_game_season_week', 'season', 'week'),
        Index('idx_game_date', 'date'),
        Index('idx_game_league', 'league'),
    )


class TeamStats(Base):
    """
    Team statistics table - stores aggregated team stats by season/week.
    
    Used by: Feature engineering (to compute team strength metrics),
             Model training (as features)
    """
    __tablename__ = 'team_stats'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    team_id = Column(String(50), ForeignKey('teams.team_id'), nullable=False)
    season = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)  # Week of season (0 = preseason)
    league = Column(String(10), nullable=False)
    
    # Offensive stats
    points_for = Column(Float)
    points_against = Column(Float)
    point_differential = Column(Float)
    yards_for = Column(Float)
    yards_against = Column(Float)
    
    # Efficiency metrics (computed in feature engineering)
    offensive_efficiency = Column(Float)
    defensive_efficiency = Column(Float)
    
    # Rolling averages (computed)
    points_for_avg = Column(Float)  # Rolling average
    points_against_avg = Column(Float)
    point_diff_avg = Column(Float)
    
    created_at = Column(Date)
    
    __table_args__ = (
        Index('idx_team_stats_team_season_week', 'team_id', 'season', 'week'),
        Index('idx_team_stats_season_week', 'season', 'week'),
    )


class TeamRating(Base):
    """
    Team ratings table - stores Elo, SRS, or other rating systems.
    
    Used by: Feature engineering (rating difference is a key Phase 1 feature),
             Model training (as primary feature)
    """
    __tablename__ = 'team_ratings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    team_id = Column(String(50), ForeignKey('teams.team_id'), nullable=False)
    season = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    league = Column(String(10), nullable=False)
    
    # Rating systems
    elo_rating = Column(Float)  # Elo rating
    srs_rating = Column(Float)  # Simple Rating System
    
    created_at = Column(Date)
    
    __table_args__ = (
        Index('idx_rating_team_season_week', 'team_id', 'season', 'week'),
        UniqueConstraint('team_id', 'season', 'week', name='uq_rating_team_season_week'),
    )


class BettingOdds(Base):
    """
    Betting odds table - stores sportsbook lines and odds.
    
    Used by: Phase 3 (market comparison), not used in Phase 1 model training
             (to avoid circular reasoning - we want to beat the market, not copy it)
    """
    __tablename__ = 'betting_odds'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(50), ForeignKey('games.game_id'), nullable=False)
    
    # Line information
    spread = Column(Float)  # Point spread (negative = favorite)
    total = Column(Float)  # Over/under total
    home_moneyline = Column(Integer)  # American odds format
    away_moneyline = Column(Integer)
    
    # Line metadata
    sportsbook = Column(String(50))  # Which book (e.g., 'consensus', 'draftkings')
    line_type = Column(String(20))  # 'opening', 'closing', 'current'
    timestamp = Column(Date)
    
    created_at = Column(Date)
    
    __table_args__ = (
        Index('idx_odds_game', 'game_id'),
        Index('idx_odds_timestamp', 'timestamp'),
    )



