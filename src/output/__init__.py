# Phase 1: Output Formatting Module
# 
# USE: This package formats predictions for terminal display
# WHAT WILL BE BUILT: Functions to format predictions into readable terminal output
# HOW IT WORKS: Takes prediction dictionaries and formats them as text tables
#   with proper spacing, alignment, and readability
# FITS IN PROJECT: Phase 1 - provides user-facing output via terminal

from .terminal_formatter import TerminalFormatter, format_game_prediction, format_week_predictions

__all__ = [
    'TerminalFormatter',
    'format_game_prediction',
    'format_week_predictions',
]

