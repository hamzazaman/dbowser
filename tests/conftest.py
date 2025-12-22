import asyncio
import os
from pathlib import Path
import sys
import time
from typing import AsyncIterator
from urllib.parse import urlparse

if True:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    import asyncpg
    import pytest
    import pytest_asyncio

    from dbowser.config import AppConfig, ConnectionConfig, save_config, save_last_query
    from dbowser.postgres_driver import close_pools


def _database_name_from_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.path:
        return parsed.path.lstrip("/") or "postgres"
    return "postgres"


async def _seed_integration_data(db_url: str) -> None:
    connection = await asyncpg.connect(db_url)
    try:
        await connection.execute(
            """
            CREATE TABLE IF NOT EXISTS public.widgets (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        await connection.execute("TRUNCATE TABLE public.widgets RESTART IDENTITY")
        await connection.executemany(
            "INSERT INTO public.widgets (name, quantity) VALUES ($1, $2)",
            [("alpha", 3), ("beta", 7), ("gamma", 0)],
        )
    finally:
        await connection.close()


@pytest.fixture(scope="session")
def db_url() -> str:
    return os.environ.get(
        "DBOWSER_TEST_DB_URL",
        "postgresql://dbowser:dbowser@localhost:54329/dbowser_test",
    )


@pytest.fixture()
def app_config(tmp_path, monkeypatch, db_url: str) -> AppConfig:
    monkeypatch.setenv("HOME", str(tmp_path))
    config = AppConfig(connections=[ConnectionConfig(name="local", url=db_url)])
    save_config(config)
    save_last_query("SELECT 1 AS one;")
    return config


@pytest.fixture(scope="session")
def database_name(db_url: str) -> str:
    return _database_name_from_url(db_url)


@pytest_asyncio.fixture(autouse=True)
async def _seed_database(db_url: str) -> None:
    await _seed_integration_data(db_url)


@pytest_asyncio.fixture(autouse=True)
async def _close_pools_after_test() -> AsyncIterator[None]:
    yield
    await close_pools()


async def wait_for_db(db_url: str, timeout_seconds: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            connection = await asyncpg.connect(db_url)
            await connection.close()
            return
        except Exception:
            await asyncio.sleep(0.5)
    pytest.skip("Database is not available for integration tests.")
