from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Streamlit Community Cloud friendly defaults:
# - Prefer a file-backed SQLite database when no DATABASE_URL is provided.
# - Keeps the demo fully self-contained (no Docker/Postgres required).
DB_BACKEND = os.getenv("DB_BACKEND", "sqlite").strip().lower()
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "15432")
DB_NAME = os.getenv("DB_NAME", "portfolio_db")

if not DATABASE_URL:
    if DB_BACKEND == "sqlite":
        db_dir = PROJECT_ROOT / "data"
        db_dir.mkdir(exist_ok=True)
        DATABASE_URL = f"sqlite:///{(db_dir / 'pipeline.db').as_posix()}"
    else:
        DATABASE_URL = (
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)