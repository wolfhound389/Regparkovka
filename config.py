import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Telegram Bot Token
    BOT_TOKEN = os.getenv('BOT_TOKEN')

    # Use SQLite
    DATABASE_URL = "sqlite:///parking_bot.db"

    # Moscow timezone
    TIMEZONE = 'Europe/Moscow'

    # Parking settings
    PARKING_SPOTS = 50
    STUCK_TASK_MINUTES = 30

    # Admin user ID
    ADMIN_IDS = list(map(int, os.getenv('ADMIN_IDS', '').split(','))) if os.getenv('ADMIN_IDS') else []

config = Config()
