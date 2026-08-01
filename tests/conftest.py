"""Coloca a raiz do repositório no sys.path para que `from src import ...` funcione."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
