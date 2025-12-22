# Phase 1: Feature Engineering Module
# 
# USE: This package contains feature engineering functionality for model training
# WHAT WILL BE BUILT: Functions to compute features from raw data for predictions
# HOW IT WORKS: Transforms database data into model-ready features, ensuring
#   no data leakage (only uses data available at prediction time)
# FITS IN PROJECT: Phase 1 - converts raw data into features for baseline models

from .feature_engineering import FeatureEngineer, compute_game_features, compute_game_features_by_id
from .ratings import compute_elo_ratings, compute_srs_ratings

__all__ = [
    'FeatureEngineer',
    'compute_game_features',
    'compute_game_features_by_id',
    'compute_elo_ratings',
    'compute_srs_ratings',
]

