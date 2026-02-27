"""
SQLAlchemy async engine & session factory.
"""
from typing import Optional, Dict, List, Any
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from config import DATABASE_URL

engine = create_async_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=False,
)

async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db():
    """FastAPI dependency: yields an async DB session."""
    async with async_session() as session:
        yield session


async def fetch_all(query: str, params: Optional[dict] = None) -> List[Dict[str, Any]]:
    """Execute raw SQL and return list of dicts."""
    async with async_session() as session:
        result = await session.execute(text(query), params or {})
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]


async def fetch_one(query: str, params: Optional[dict] = None) -> Optional[Dict[str, Any]]:
    """Execute raw SQL and return single dict or None."""
    rows = await fetch_all(query, params)
    return rows[0] if rows else None
