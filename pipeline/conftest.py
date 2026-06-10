"""Root pytest configuration and shared fixtures for the pipeline test suite."""

import sys
from pathlib import Path

# Ensure shared/ is importable when running pytest from pipeline/ root.
sys.path.insert(0, str(Path(__file__).parent))
