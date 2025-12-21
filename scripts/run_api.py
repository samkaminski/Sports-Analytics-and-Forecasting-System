#!/usr/bin/env python3
"""
FastAPI Server Runner
Phase 0: Data Ingestion and Storage

USE: Command-line script to start FastAPI server
WHAT WILL BE BUILT: Uvicorn server launcher
HOW IT WORKS: Runs uvicorn with FastAPI app
FITS IN PROJECT: Phase 0 - provides API server for querying data
"""

import sys
import uvicorn
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if __name__ == '__main__':
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )

