import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Bot configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "7219253783:AAGpnSUpvKgvxyP148LA78YkJCtuKzvxVwE")

# Data paths (configurable for different environments)
# Use relative path 'data' if DATA_DIR not set in environment
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DB_PATH = str(DATA_DIR / "mood_tracker.db")

# Ensure data directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")