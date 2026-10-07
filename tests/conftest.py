import os
from collections.abc import AsyncIterator

import asyncpg
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.api.main import create_app
from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory
from app.models import Base

TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL", "postgresql+asyncpg://payments:payments@localhost:5432/payments_test"
)
API_KEY = "test-api-key"


async def _ensure_database(url: str) -> None:
    parsed = make_url(url)
    try:
        conn = await asyncpg.connect(
            user=parsed.username,
            password=parsed.password,
            host=parsed.host,
            port=parsed.port,
            database="postgres",
        )
    except OSError as exc:
        pytest.exit(f"PostgreSQL is not reachable ({exc}). Run `docker compose up -d postgres`.")
    try:
        query = "SELECT 1 FROM pg_database WHERE datname = $1"
        if not await conn.fetchval(query, parsed.database):
            await conn.execute(f'CREATE DATABASE "{parsed.database}"')
    finally:
        await conn.close()


@pytest.fixture(scope="session")
async def engine() -> AsyncIterator[AsyncEngine]:
    await _ensure_database(TEST_DATABASE_URL)
    engine = create_engine(TEST_DATABASE_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture
async def session_factory(engine: AsyncEngine) -> AsyncIterator[async_sessionmaker]:
    yield create_session_factory(engine)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox"))


@pytest.fixture
async def client(session_factory: async_sessionmaker) -> AsyncIterator[AsyncClient]:
    app = create_app()
    app.state.session_factory = session_factory
    app.dependency_overrides[get_settings] = lambda: Settings(api_key=API_KEY)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": API_KEY},
    ) as client:
        yield client
