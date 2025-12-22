import asyncio
import time
from typing import Callable

import pytest
from textual.widgets import ListView

from dbowser.tui import (
    ConnectionListItem,
    DatabaseBrowserApp,
    DatabaseListItem,
    SchemaListItem,
    TableListItem,
)
from conftest import wait_for_db


async def _wait_for(predicate: Callable[[], bool], timeout_seconds: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("Timed out waiting for condition.")


def _resource_list(app: DatabaseBrowserApp) -> ListView:
    return app.query_one("#resource-list", ListView)


@pytest.mark.asyncio
async def test_query_view_runs_query(app_config, db_url: str, database_name: str) -> None:
    await wait_for_db(db_url)
    app = DatabaseBrowserApp(
        app_config,
        initial_connection_name="local",
        initial_database_name=database_name,
        initial_schema_name="public",
    )
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press(":", "q", "u", "e", "r", "y", "enter")
        await pilot.pause()
        await pilot.press("r")
        await pilot.pause()
        assert app._current_view == "query"
        assert app._query_page.columns == ["one"]
        assert app._query_page.rows[0][0] == 1


@pytest.mark.asyncio
async def test_connection_view_lists_connections(app_config, db_url: str) -> None:
    await wait_for_db(db_url)
    app = DatabaseBrowserApp(app_config)
    async with app.run_test() as pilot:
        await pilot.pause()
        await _wait_for(lambda: app._current_view == "connection")
        await _wait_for(lambda: len(_resource_list(app).children) > 0)
        items = [
            child
            for child in _resource_list(app).children
            if isinstance(child, ConnectionListItem)
        ]
        assert any(item.connection_name == "local" for item in items)


@pytest.mark.asyncio
async def test_database_view_after_connection_selection(
    app_config, db_url: str, database_name: str
) -> None:
    await wait_for_db(db_url)
    app = DatabaseBrowserApp(app_config)
    async with app.run_test() as pilot:
        await pilot.pause()
        await _wait_for(lambda: app._current_view == "connection")
        await _wait_for(lambda: len(_resource_list(app).children) > 0)
        await pilot.press("enter")
        await _wait_for(lambda: app._current_view == "database")
        await _wait_for(lambda: len(_resource_list(app).children) > 0)
        items = [
            child
            for child in _resource_list(app).children
            if isinstance(child, DatabaseListItem)
        ]
        assert any(item.database_name == database_name for item in items)


@pytest.mark.asyncio
async def test_schema_view_after_initial_database(
    app_config, db_url: str, database_name: str
) -> None:
    await wait_for_db(db_url)
    app = DatabaseBrowserApp(
        app_config,
        initial_connection_name="local",
        initial_database_name=database_name,
    )
    async with app.run_test() as pilot:
        await pilot.pause()
        await _wait_for(lambda: app._current_view == "schema")
        await _wait_for(lambda: len(_resource_list(app).children) > 0)
        items = [
            child
            for child in _resource_list(app).children
            if isinstance(child, SchemaListItem)
        ]
        assert any(item.schema_name == "public" for item in items)


@pytest.mark.asyncio
async def test_table_view_after_initial_schema(
    app_config, db_url: str, database_name: str
) -> None:
    await wait_for_db(db_url)
    app = DatabaseBrowserApp(
        app_config,
        initial_connection_name="local",
        initial_database_name=database_name,
        initial_schema_name="public",
    )
    async with app.run_test() as pilot:
        await pilot.pause()
        await _wait_for(lambda: app._current_view == "table")
        await _wait_for(lambda: len(_resource_list(app).children) > 0)
        items = [
            child
            for child in _resource_list(app).children
            if isinstance(child, TableListItem)
        ]
        assert any(item.table_name == "widgets" for item in items)


@pytest.mark.asyncio
async def test_rows_view_lists_seed_data(
    app_config, db_url: str, database_name: str
) -> None:
    await wait_for_db(db_url)
    app = DatabaseBrowserApp(
        app_config,
        initial_connection_name="local",
        initial_database_name=database_name,
        initial_schema_name="public",
    )
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("enter")
        await pilot.pause()
        assert app._current_view == "rows"
        assert len(app._rows_page.rows) >= 1
