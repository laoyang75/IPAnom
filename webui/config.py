"""
RB20 v2.5 WebUI 配置
"""
import os
from dotenv import load_dotenv

load_dotenv()

# --- Database ---
DB_HOST = os.getenv("DB_HOST", "192.168.200.217")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "123456")
DB_NAME = os.getenv("DB_NAME", "ip_loc2")
DB_SCHEMA = "rb20_v2_5"

DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DATABASE_URL_SYNC = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- App ---
APP_TITLE = "RB20 v2.5 可视化监控平台"
APP_VERSION = "1.0.0"
DEBUG = os.getenv("DEBUG", "true").lower() == "true"
