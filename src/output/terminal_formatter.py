"""
Terminal Output Formatter Module
Phase 1: Baseline Predictive Models

USE: Formats predictions into readable terminal output
WHAT WILL BE BUILT:
  - Functions to format single game predictions
  - Functions to format week predictions (multiple games)
  - Functions to format key factors/explanations
  - Table formatting with proper alignment

HOW IT WORKS:
  - Takes prediction dictionaries from PredictionEngine
  - Formats them as human-readable text
  - Creates tables for multiple games
  - Includes key factors and explanations
  - Handles missing data gracefully

FITS IN PROJECT:
  - Phase 1: Terminal-based output (no frontend yet)
  - Used by CLI scripts to display predictions
  - Provides clear, scannable output for analyst review
  - Future: Can be adapted for API JSON responses or web UI
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import date, datetime
from ..data.database import DatabaseManager, Team

logger = logging.getLogger(__name__)


class TerminalFormatter:
    """
    Formats predictions for terminal display.
    
    This class:
    - Formats single game predictions
    - Formats week predictions (tables)
    - Formats key factors and explanations
    - Handles team name lookups
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize formatter.
        
        Args:
            db_manager: DatabaseManager for team name lookups
        """
        self.db = db_manager
        self.team_cache = {}
    
    def _get_team_name(self, team_id: str) -> str:
        """Get team name from database (with caching)."""
        if team_id in self.team_cache:
            return self.team_cache[team_id]
        
        with self.db.get_session() as session:
            team = session.query(Team).filter_by(team_id=team_id).first()
            if team:
                name = team.name or team_id
            else:
                name = team_id  # Fallback to ID if not found
            
            self.team_cache[team_id] = name
            return name
    
    def format_game_prediction(
        self,
        prediction: Dict[str, Any],
        include_factors: bool = True
    ) -> str:
        """
        Format a single game prediction for terminal output.
        
        Args:
            prediction: Prediction dictionary from PredictionEngine
            include_factors: Whether to include key factors explanation
        
        Returns:
            Formatted string for terminal display
        """
        home_team = self._get_team_name(prediction.get('home_team_id', 'Unknown'))
        away_team = self._get_team_name(prediction.get('away_team_id', 'Unknown'))
        game_date = prediction.get('date', 'Unknown Date')
        
        # Format spread
        spread = prediction.get('spread')
        if spread is not None:
            if spread > 0:
                spread_str = f"{home_team} -{spread:.1f}"
            elif spread < 0:
                spread_str = f"{away_team} -{abs(spread):.1f}"
            else:
                spread_str = "Pick 'em"
        else:
            spread_str = "N/A"
        
        # Format total
        total = prediction.get('total')
        total_str = f"{total:.1f}" if total is not None else "N/A"
        
        # Format win probabilities
        home_prob = prediction.get('home_win_prob')
        away_prob = prediction.get('away_win_prob')
        if home_prob is not None and away_prob is not None:
            prob_str = f"{home_team} {home_prob*100:.1f}% | {away_team} {away_prob*100:.1f}%"
        else:
            prob_str = "N/A"
        
        # Build output
        output = []
        output.append("=" * 70)
        output.append(f"Game: {away_team} @ {home_team}")
        output.append(f"Date: {game_date}")
        output.append("-" * 70)
        output.append("Model Prediction:")
        output.append(f"  Spread: {spread_str}")
        output.append(f"  Total: {total_str} points")
        output.append(f"  Win Probability: {prob_str}")
        
        if include_factors:
            factors = self._format_factors(prediction.get('features', {}))
            if factors:
                output.append("")
                output.append("Key Factors:")
                output.append(factors)
        
        output.append("=" * 70)
        output.append("")
        
        return "\n".join(output)
    
    def _format_factors(self, features: Dict[str, float]) -> str:
        """
        Format key factors from feature values.
        
        Args:
            features: Feature dictionary
        
        Returns:
            Formatted factors string
        """
        factors = []
        
        rating_diff = features.get('rating_diff', 0)
        if rating_diff != 0:
            if rating_diff > 0:
                factors.append(f"  - Home team rating advantage: +{rating_diff:.1f} points")
            else:
                factors.append(f"  - Away team rating advantage: +{abs(rating_diff):.1f} points")
        
        home_field = features.get('home_field_advantage', 0)
        if home_field > 0:
            factors.append(f"  - Home field advantage: +{home_field*2.5:.1f} points (typical)")
        
        point_diff = features.get('point_diff_diff', 0)
        if point_diff != 0:
            if point_diff > 0:
                factors.append(f"  - Home team recent form advantage: +{point_diff:.1f} points")
            else:
                factors.append(f"  - Away team recent form advantage: +{abs(point_diff):.1f} points")
        
        if not factors:
            factors.append("  - Using baseline team ratings and home field")
        
        return "\n".join(factors)
    
    def format_week_predictions(
        self,
        predictions: List[Dict[str, Any]],
        league: str,
        season: int,
        week: int
    ) -> str:
        """
        Format multiple game predictions as a table.
        
        Args:
            predictions: List of prediction dictionaries
            league: League name
            season: Season year
            week: Week number
        
        Returns:
            Formatted table string
        """
        if not predictions:
            return f"No predictions available for {league} Week {week}, Season {season}"
        
        output = []
        output.append("=" * 100)
        output.append(f"{league} Week {week} Predictions - Season {season}")
        output.append("=" * 100)
        output.append("")
        
        # Table header
        header = f"{'Game':<40} {'Spread':<20} {'Total':<12} {'Win Prob':<20}"
        output.append(header)
        output.append("-" * 100)
        
        # Table rows
        for pred in predictions:
            home_team = self._get_team_name(pred.get('home_team_id', 'Unknown'))
            away_team = self._get_team_name(pred.get('away_team_id', 'Unknown'))
            game_str = f"{away_team} @ {home_team}"
            
            # Format spread
            spread = pred.get('spread')
            if spread is not None:
                if spread > 0:
                    spread_str = f"{home_team} -{spread:.1f}"
                else:
                    spread_str = f"{away_team} -{abs(spread):.1f}"
            else:
                spread_str = "N/A"
            
            # Format total
            total = pred.get('total')
            total_str = f"{total:.1f}" if total is not None else "N/A"
            
            # Format win probability
            home_prob = pred.get('home_win_prob')
            if home_prob is not None:
                prob_str = f"{home_team} {home_prob*100:.0f}%"
            else:
                prob_str = "N/A"
            
            row = f"{game_str:<40} {spread_str:<20} {total_str:<12} {prob_str:<20}"
            output.append(row)
        
        output.append("")
        output.append("=" * 100)
        output.append("")
        output.append("Note: Predictions are for research purposes only. No guarantees on results.")
        output.append("")
        
        return "\n".join(output)
    
    def format_detailed_week(
        self,
        predictions: List[Dict[str, Any]],
        league: str,
        season: int,
        week: int
    ) -> str:
        """
        Format week predictions with detailed information for each game.
        
        Args:
            predictions: List of prediction dictionaries
            league: League name
            season: Season year
            week: Week number
        
        Returns:
            Formatted detailed output
        """
        if not predictions:
            return f"No predictions available for {league} Week {week}, Season {season}"
        
        output = []
        output.append("=" * 100)
        output.append(f"{league} Week {week} Detailed Predictions - Season {season}")
        output.append("=" * 100)
        output.append("")
        
        for i, pred in enumerate(predictions, 1):
            output.append(f"Game {i}:")
            output.append(self.format_game_prediction(pred, include_factors=True))
        
        output.append("Note: Predictions are for research purposes only. No guarantees on results.")
        output.append("")
        
        return "\n".join(output)


def format_game_prediction(
    prediction: Dict[str, Any],
    db_manager: DatabaseManager,
    include_factors: bool = True
) -> str:
    """
    Convenience function to format a single game prediction.
    
    Args:
        prediction: Prediction dictionary
        db_manager: DatabaseManager instance
        include_factors: Whether to include factors
    
    Returns:
        Formatted string
    """
    formatter = TerminalFormatter(db_manager)
    return formatter.format_game_prediction(prediction, include_factors)


def format_week_predictions(
    predictions: List[Dict[str, Any]],
    league: str,
    season: int,
    week: int,
    db_manager: DatabaseManager
) -> str:
    """
    Convenience function to format week predictions.
    
    Args:
        predictions: List of prediction dictionaries
        league: League name
        season: Season year
        week: Week number
        db_manager: DatabaseManager instance
    
    Returns:
        Formatted string
    """
    formatter = TerminalFormatter(db_manager)
    return formatter.format_week_predictions(predictions, league, season, week)

