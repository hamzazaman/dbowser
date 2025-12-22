import asyncio
import time
from typing import Callable

import pytest
from textual.coordinate import Coordinate
from textual.widgets import ListView

from dbowser.tui import (
    ConnectionListItem,
    DatabaseBrowserApp,
    DatabaseListItem,
    SchemaListItem,
    TableListItem,
)
from conftest import LONG_TEXT_VALUE, wait_for_db


async def _wait_for(predicate: Callable[[], bool], timeout_seconds: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("Timed out waiting for condition.")


def _resource_list(app: DatabaseBrowserApp) -> ListView:
    return app.query_one("#resource-list", ListView)


def _cell_detail_view(app: DatabaseBrowserApp):
    try:
        return app.screen.query_one("#cell-detail-text")
    except Exception:
        return None


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
async def test_cell_detail_shows_full_value_and_truncates_in_table(
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
        resource_list = _resource_list(app)
        table_items = [
            child for child in resource_list.children if isinstance(child, TableListItem)
        ]
        long_text_index = next(
            (
                index
                for index, item in enumerate(table_items)
                if item.table_name == "long_texts"
            ),
            None,
        )
        assert long_text_index is not None
        resource_list.index = long_text_index
        await pilot.pause()
        await pilot.press("enter")
        await _wait_for(lambda: app._current_view == "rows")
        await _wait_for(lambda: app._rows_table_view().row_count > 0)
        column_index = app._rows_page.columns.index("note")
        rows_table = app._rows_table_view()
        cell_value = rows_table.get_cell_at(Coordinate(0, column_index))
        cell_text = getattr(cell_value, "plain", str(cell_value))
        max_width = app._max_table_cell_width
        expected = (
            LONG_TEXT_VALUE
            if len(LONG_TEXT_VALUE) <= max_width
            else LONG_TEXT_VALUE[: max_width - 3] + "..."
        )
        assert cell_text == expected
        rows_table.move_cursor(row=0, column=column_index, animate=False)
        await pilot.press("enter")
        await _wait_for(lambda: _cell_detail_view(app) is not None)
        cell_detail = _cell_detail_view(app)
        assert cell_detail is not None
        assert cell_detail.content == LONG_TEXT_VALUE


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
