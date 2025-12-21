"""
FastAPI Main Application
Phase 0: Data Ingestion and Storage

USE: FastAPI application with endpoints for querying ingested data
WHAT WILL BE BUILT: REST API with /health and /games endpoints
HOW IT WORKS: FastAPI app with SQLAlchemy 2.x database queries
FITS IN PROJECT: Phase 0 - provides queryable API endpoint for games data
"""

from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import select
from typing import List, Optional
from datetime import date
from pydantic import BaseModel

from .dependencies import get_db_session
from src.data.database import Game, Team, TeamStats

app = FastAPI(
    title="Sports Betting Analytics API",
    description="API for querying NFL and NCAA football data",
    version="0.1.0"
)


class GameResponse(BaseModel):
    """Game response model."""
    game_id: str
    season: int
    week: int
    date: date
    home_team_id: str
    away_team_id: str
    home_team_name: Optional[str] = None
    away_team_name: Optional[str] = None
    league: str
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    completed: bool
    stadium: Optional[str] = None
    
    class Config:
        from_attributes = True


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/games", response_model=List[GameResponse])
def get_games(
    league: str = Query(..., description="League (NFL or NCAA)"),
    season: int = Query(..., description="Season year"),
    week: Optional[int] = Query(None, description="Week number (optional)"),
    session: Session = Depends(get_db_session)
):
    """
    Get games for a league, season, and optional week.
    
    Example:
        GET /games?league=NFL&season=2023&week=1
    """
    stmt = select(Game).where(
        Game.league == league.upper(),
        Game.season == season
    )
    
    if week is not None:
        stmt = stmt.where(Game.week == week)
    
    stmt = stmt.order_by(Game.week, Game.date)
    
    games = session.scalars(stmt).all()
    
    # Enrich with team names
    result = []
    for game in games:
        game_dict = {
            "game_id": game.game_id,
            "season": game.season,
            "week": game.week,
            "date": game.date,
            "home_team_id": game.home_team_id,
            "away_team_id": game.away_team_id,
            "league": game.league,
            "home_score": game.home_score,
            "away_score": game.away_score,
            "completed": game.completed,
            "stadium": game.stadium,
        }
        
        # Get team names
        home_team = session.scalar(select(Team).where(Team.team_id == game.home_team_id))
        away_team = session.scalar(select(Team).where(Team.team_id == game.away_team_id))
        
        if home_team:
            game_dict["home_team_name"] = home_team.name
        if away_team:
            game_dict["away_team_name"] = away_team.name
        
        result.append(GameResponse(**game_dict))
    
    return result


class TeamStatsResponse(BaseModel):
    """Team statistics response model."""
    team_id: str
    team_abbr: str
    team_name: Optional[str] = None
    league: str
    season: int
    games_played: Optional[int] = None
    wins: Optional[int] = None
    losses: Optional[int] = None
    points_for: Optional[int] = None
    points_against: Optional[int] = None
    
    class Config:
        from_attributes = True


@app.get("/stats/teams", response_model=List[TeamStatsResponse])
def get_team_stats(
    league: str = Query(..., description="League (NFL or NCAA)"),
    season: int = Query(..., description="Season year"),
    session: Session = Depends(get_db_session)
):
    """
    Get team statistics for a league and season.
    
    Example:
        GET /stats/teams?league=NFL&season=2023
    """
    stmt = select(TeamStats).where(
        TeamStats.league == league.upper(),
        TeamStats.season == season
    ).order_by(TeamStats.team_abbr)
    
    team_stats = session.scalars(stmt).all()
    
    if not team_stats:
        return []
    
    # Enrich with team names
    result = []
    for stats in team_stats:
        team = session.scalar(select(Team).where(Team.team_id == stats.team_id))
        
        stats_dict = {
            "team_id": stats.team_id,
            "team_abbr": stats.team_abbr,
            "league": stats.league,
            "season": stats.season,
            "games_played": stats.games_played,
            "wins": stats.wins,
            "losses": stats.losses,
            "points_for": stats.points_for,
            "points_against": stats.points_against,
        }
        
        if team:
            stats_dict["team_name"] = team.name
        
        result.append(TeamStatsResponse(**stats_dict))
    
    return result

