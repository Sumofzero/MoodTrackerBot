import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Bot configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "7219253783:AAGpnSUpvKgvxyP148LA78YkJCtuKzvxVwE")

# Data paths (configurable for different environments)
# В продакшене на Render используем путь к SSD диску
# В локальной разработке используем относительный путь
if os.getenv("RENDER"):
    # Продакшен на Render - используем SSD диск
    DATA_DIR = Path("/MoodTrackerBot_data")
else:
    # Локальная разработка - используем относительный путь
    DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
    # Создаем директорию только в локальной разработке
    DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = str(DATA_DIR / "mood_tracker.db")

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")