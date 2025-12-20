import json
from contextlib import asynccontextmanager
import time
import subprocess
import sys
from typing import AsyncIterator, Protocol, Sequence, TypeVar

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.events import Key
from textual.widgets import (
    DataTable,
    Header,
    Input,
    ListItem,
    ListView,
    Static,
)

from dbowser.postgres_driver import (
    ConnectionParameters,
    DatabaseInfo,
    RowPage,
    SchemaInfo,
    TableInfo,
    build_database_connection_parameters,
    list_rows,
    list_schemas,
    list_tables,
)


class DatabaseListItem(ListItem):
    def __init__(self, database_name: str) -> None:
        super().__init__(Static(database_name))
        self.database_name = database_name


class SchemaListItem(ListItem):
    def __init__(self, schema_name: str) -> None:
        super().__init__(Static(schema_name))
        self.schema_name = schema_name


class TableListItem(ListItem):
    def __init__(self, table_name: str, estimated_rows: int) -> None:
        label = f"{table_name}  (~{estimated_rows})"
        super().__init__(Static(label))
        self.table_name = table_name


class _NamedItem(Protocol):
    @property
    def name(self) -> str:
        return ""


NamedItemT = TypeVar("NamedItemT", bound=_NamedItem)


class DatabaseBrowserApp(App):
    DEFAULT_CSS = """
    #top-bar {
        height: 1;
    }

    #selected-status {
        width: 1fr;
    }

    #loading-indicator {
        width: auto;
        content-align: right middle;
    }

    #keybinds-bar {
        height: 1;
    }

    #message-line {
        height: 1;
    }

    #command-input {
        height: 1;
    }

    #resource-list {
        height: 1fr;
    }

    #rows-table {
        height: 1fr;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("j", "cursor_down", "Down"),
        ("k", "cursor_up", "Up"),
        ("h", "cursor_left", "Left"),
        ("l", "cursor_right", "Right"),
        ("y", "yank_cell", "Yank Cell"),
        ("n", "next_page", "Next Page"),
        ("p", "previous_page", "Prev Page"),
        ("G", "cursor_bottom", "Bottom"),
        ("/", "enter_filter_mode", "Filter"),
        (":", "enter_command_mode", "Command"),
        ("escape", "escape", "Back"),
        ("enter", "select_resource", "Select"),
    ]

    def __init__(
        self,
        base_connection_parameters: ConnectionParameters,
        databases: list[DatabaseInfo],
    ) -> None:
        super().__init__()
        if not databases:
            raise ValueError("No databases returned from server.")
        self._base_connection_parameters = base_connection_parameters
        self._databases = databases
        self._schemas: list[SchemaInfo] = []
        self._tables: list[TableInfo] = []
        self._selected_database_name = ""
        self._selected_schema_name = ""
        self._selected_table_name = ""
        self._input_mode = ""
        self._current_view = "database"
        self._view_history: list[str] = []
        self._rows_page_limit = 100
        self._max_table_cell_width = 75
        self._rows_page_offset = 0
        self._page_turn_cooldown_seconds = 0.25
        self._last_page_turn_at = 0.0
        self._last_g_pressed_at = 0.0
        self._gg_timeout_seconds = 0.4
        self._rows_page = RowPage(
            columns=[],
            rows=[],
            limit=self._rows_page_limit,
            offset=self._rows_page_offset,
            has_more=False,
        )
        self._resource_filters: dict[str, str] = {
            "database": "",
            "schema": "",
            "table": "",
            "rows": "",
        }

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical():
            with Horizontal(id="top-bar"):
                yield Static(self._status_text(), id="selected-status")
                yield Static("", id="loading-indicator")
            yield Static("", id="message-line")
            yield KeyBindingBar(id="keybinds-bar")
            yield Input(placeholder="Command", id="command-input")
            yield ListView(id="resource-list")
            yield DataTable(id="rows-table")

    async def on_mount(self) -> None:
        await self._refresh_view()
        self._resource_list_view().focus()
        rows_table = self.query_one("#rows-table", DataTable)
        rows_table.display = False
        command_input = self.query_one("#command-input", Input)
        command_input.display = False
        self._update_keybinds()

    async def action_select_resource(self) -> None:
        if self._input_mode:
            return
        resource_list = self._resource_list_view()
        if self._current_view == "database":
            await self._select_database(resource_list)
        elif self._current_view == "schema":
            await self._select_schema(resource_list)
        elif self._current_view == "table":
            await self._select_table(resource_list)
        elif self._current_view == "rows":
            self._show_cell_detail()

    def action_enter_filter_mode(self) -> None:
        if self._current_view == "rows":
            self._update_message("Filters are not available in rows view.")
            return
        self._enter_input_mode("filter")

    def action_enter_command_mode(self) -> None:
        self._enter_input_mode("command")

    def action_cursor_down(self) -> None:
        if self._input_mode:
            return
        if self._current_view == "rows":
            self._rows_table_view().action_cursor_down()
            return
        self._resource_list_view().action_cursor_down()

    def action_cursor_up(self) -> None:
        if self._input_mode:
            return
        if self._current_view == "rows":
            self._rows_table_view().action_cursor_up()
            return
        self._resource_list_view().action_cursor_up()

    def action_cursor_left(self) -> None:
        if self._input_mode or self._current_view != "rows":
            return
        self._rows_table_view().action_cursor_left()

    def action_cursor_right(self) -> None:
        if self._input_mode or self._current_view != "rows":
            return
        self._rows_table_view().action_cursor_right()

    def action_cursor_bottom(self) -> None:
        if self._input_mode:
            return
        if self._current_view == "rows":
            rows_table = self._rows_table_view()
            if rows_table.row_count == 0:
                return
            rows_table.move_cursor(
                row=rows_table.row_count - 1,
                column=rows_table.cursor_column,
            )
            return
        resource_list = self._resource_list_view()
        item_count = len(resource_list.children)
        if item_count == 0:
            return
        resource_list.index = item_count - 1

    def action_yank_cell(self) -> None:
        if self._input_mode or self._current_view != "rows":
            return
        if not self._rows_page.rows:
            self._update_message("No cell to yank.")
            return
        rows_table = self._rows_table_view()
        coordinate = rows_table.cursor_coordinate
        if coordinate.row >= len(self._rows_page.rows):
            self._update_message("No cell to yank.")
            return
        row = self._rows_page.rows[coordinate.row]
        if coordinate.column >= len(row):
            self._update_message("No cell to yank.")
            return
        cell_value = row[coordinate.column]
        self.copy_text_to_clipboard(self._format_cell_value_full(cell_value))
        self._update_message("Yanked cell to clipboard.")

    async def action_next_page(self) -> None:
        if self._input_mode or self._current_view != "rows":
            return
        if not self._can_turn_page():
            return
        if not self._rows_page.has_more:
            return
        self._rows_page_offset += self._rows_page_limit
        await self._load_rows()
        self._populate_rows_table(self._rows_page)

    async def action_previous_page(self) -> None:
        if self._input_mode or self._current_view != "rows":
            return
        if not self._can_turn_page():
            return
        if self._rows_page_offset == 0:
            return
        self._rows_page_offset = max(0, self._rows_page_offset - self._rows_page_limit)
        await self._load_rows()
        self._populate_rows_table(self._rows_page)

    async def action_escape(self) -> None:
        if self._input_mode:
            self._close_input_mode()
            return
        if await self._clear_active_filter():
            return
        await self._pop_view_history()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "command-input":
            return
        submitted_value = event.value.strip()
        if self._input_mode == "filter":
            await self._apply_filter(submitted_value)
        elif self._input_mode == "command":
            await self._run_command(submitted_value)
        self._close_input_mode()

    async def on_list_view_selected(self, event: ListView.Selected) -> None:
        if event.list_view.id != "resource-list":
            return
        if self._input_mode:
            return
        await self.action_select_resource()

    def on_data_table_cell_selected(self, event: DataTable.CellSelected) -> None:
        if event.data_table.id != "rows-table":
            return
        if self._input_mode or self._current_view != "rows":
            return
        self._show_cell_detail()

    def on_key(self, event: Key) -> None:
        if event.key != "g":
            return
        if self._input_mode:
            return
        now = time.monotonic()
        if now - self._last_g_pressed_at <= self._gg_timeout_seconds:
            self._last_g_pressed_at = 0.0
            self._jump_to_top()
            event.stop()
            return
        self._last_g_pressed_at = now

    def _status_text(self) -> str:
        database_text = self._selected_database_name or "<none>"
        schema_text = self._selected_schema_name or "<none>"
        table_text = self._selected_table_name or "<none>"
        page_text = ""
        if self._current_view == "rows":
            page_number = (self._rows_page_offset // self._rows_page_limit) + 1
            page_text = f" | page: {page_number}"
        filter_text = ""
        active_filter = self._resource_filters.get(self._current_view, "")
        if active_filter and self._current_view != "rows":
            filter_text = f" | filter: {active_filter}"
        return (
            f"View: {self._current_view} | Selected database: "
            f"{database_text} | schema: {schema_text} | table: {table_text}"
            f"{page_text}{filter_text}"
        )

    def _update_status(self) -> None:
        status = self.query_one("#selected-status", Static)
        status.update(self._status_text())

    def _update_message(self, message: str) -> None:
        message_line = self.query_one("#message-line", Static)
        message_line.update(message)

    def _update_keybinds(self) -> None:
        keybinds = self.query_one("#keybinds-bar", KeyBindingBar)
        keybinds.update(self._footer_text())

    def _set_loading(self, is_loading: bool, message: str = "Loading...") -> None:
        loading_indicator = self.query_one("#loading-indicator", Static)
        loading_indicator.update(message if is_loading else "")

    @asynccontextmanager
    async def _loading(self, message: str) -> AsyncIterator[None]:
        self._set_loading(True, message)
        try:
            yield
        finally:
            self._set_loading(False)

    def _resource_list_view(self) -> ListView:
        return self.query_one("#resource-list", ListView)

    def _rows_table_view(self) -> DataTable:
        return self.query_one("#rows-table", DataTable)

    def _jump_to_top(self) -> None:
        if self._current_view == "rows":
            rows_table = self._rows_table_view()
            rows_table.move_cursor(row=0, column=rows_table.cursor_column)
            return
        resource_list = self._resource_list_view()
        if len(resource_list.children) == 0:
            return
        resource_list.index = 0

    async def _select_database(self, resource_list: ListView) -> None:
        if not isinstance(resource_list.highlighted_child, DatabaseListItem):
            return
        self._selected_database_name = resource_list.highlighted_child.database_name
        self._selected_schema_name = ""
        self._selected_table_name = ""
        self._rows_page_offset = 0
        self._update_status()
        await self._load_schemas()
        await self._set_view("schema")

    async def _select_schema(self, resource_list: ListView) -> None:
        if not isinstance(resource_list.highlighted_child, SchemaListItem):
            return
        self._selected_schema_name = resource_list.highlighted_child.schema_name
        self._selected_table_name = ""
        self._rows_page_offset = 0
        self._update_status()
        await self._load_tables()
        await self._set_view("table")

    async def _select_table(self, resource_list: ListView) -> None:
        if not isinstance(resource_list.highlighted_child, TableListItem):
            return
        self._selected_table_name = resource_list.highlighted_child.table_name
        self._rows_page_offset = 0
        self._update_status()
        await self._load_rows()
        await self._set_view("rows")

    async def _load_schemas(self) -> None:
        if not self._selected_database_name:
            self._schemas = []
            return
        selected_parameters = build_database_connection_parameters(
            self._base_connection_parameters,
            self._selected_database_name,
        )
        async with self._loading("Loading schemas..."):
            self._schemas = await list_schemas(selected_parameters)
        self._tables = []

    async def _load_tables(self) -> None:
        if not self._selected_database_name or not self._selected_schema_name:
            self._tables = []
            return
        selected_parameters = build_database_connection_parameters(
            self._base_connection_parameters,
            self._selected_database_name,
        )
        async with self._loading("Loading tables..."):
            self._tables = await list_tables(
                selected_parameters,
                self._selected_schema_name,
            )

    async def _load_rows(self) -> None:
        if (
            not self._selected_database_name
            or not self._selected_schema_name
            or not self._selected_table_name
        ):
            self._rows_page = RowPage(
                columns=[],
                rows=[],
                limit=self._rows_page_limit,
                offset=self._rows_page_offset,
                has_more=False,
            )
            return
        selected_parameters = build_database_connection_parameters(
            self._base_connection_parameters,
            self._selected_database_name,
        )
        async with self._loading("Loading rows..."):
            self._rows_page = await list_rows(
                selected_parameters,
                self._selected_schema_name,
                self._selected_table_name,
                self._rows_page_limit,
                self._rows_page_offset,
            )

    def _enter_input_mode(self, mode: str) -> None:
        if self._input_mode:
            return
        self._input_mode = mode
        command_input = self.query_one("#command-input", Input)
        command_input.placeholder = (
            "Filter" if mode == "filter" else "Command (q to quit)"
        )
        if mode == "filter":
            command_input.value = self._resource_filters.get(self._current_view, "")
        else:
            command_input.value = ""
        command_input.display = True
        command_input.focus()
        self._update_message("FILTER:" if mode == "filter" else "COMMAND:")
        self._update_keybinds()

    def _close_input_mode(self) -> None:
        command_input = self.query_one("#command-input", Input)
        command_input.display = False
        command_input.value = ""
        self._input_mode = ""
        if self._current_view == "rows":
            self._rows_table_view().focus()
        else:
            self._resource_list_view().focus()
        self._update_message("")
        self._update_keybinds()

    async def _apply_filter(self, filter_text: str) -> None:
        self._resource_filters[self._current_view] = filter_text
        self._update_status()
        await self._refresh_view()

    async def _run_command(self, command_text: str) -> None:
        if command_text in {"q", "quit", "exit"}:
            self.exit()
            return
        if not command_text:
            self._update_message("")
            return
        if await self._handle_focus_command(command_text):
            return
        self._update_message(f"Unknown command: {command_text}")

    async def _handle_focus_command(self, command_text: str) -> bool:
        normalized = command_text.strip().lower()
        focus_map = {
            "db": "database",
            "database": "database",
            "databases": "database",
            "schema": "schema",
            "schemas": "schema",
            "table": "table",
            "tables": "table",
            "rows": "rows",
            "data": "rows",
        }
        target_view = focus_map.get(normalized)
        if not target_view:
            return False
        if target_view == "rows" and not self._selected_table_name:
            self._update_message("Select a table first.")
            return True
        await self._set_view(target_view)
        self._update_message(f"Focused {normalized}")
        return True

    async def _refresh_view(self) -> None:
        resource_list = self._resource_list_view()
        resource_list.clear()
        if self._current_view == "database":
            self._show_resource_list()
            filtered = self._filter_items(
                self._databases,
                self._resource_filters["database"],
            )
            for database in filtered:
                resource_list.append(DatabaseListItem(database.name))
            return
        if self._current_view == "schema":
            self._show_resource_list()
            if not self._selected_database_name:
                self._update_message("Select a database first.")
                return
            await self._load_schemas()
            filtered = self._filter_items(
                self._schemas,
                self._resource_filters["schema"],
            )
            for schema in filtered:
                resource_list.append(SchemaListItem(schema.name))
            return
        if self._current_view == "table":
            self._show_resource_list()
            if not self._selected_database_name:
                self._update_message("Select a database first.")
                return
            if not self._selected_schema_name:
                self._update_message("Select a schema first.")
                return
            await self._load_tables()
            filtered = self._filter_items(
                self._tables,
                self._resource_filters["table"],
            )
            for table in filtered:
                resource_list.append(
                    TableListItem(
                        table.name,
                        table.estimated_rows,
                    )
                )
            return
        if self._current_view == "rows":
            self._show_rows_table()
            if not self._selected_database_name or not self._selected_schema_name:
                self._update_message("Select a database and schema first.")
                return
            if not self._selected_table_name:
                self._update_message("Select a table first.")
                return
            await self._load_rows()
            self._populate_rows_table(self._rows_page)

    async def _set_view(self, target_view: str) -> None:
        if target_view == self._current_view:
            return
        self._view_history.append(self._current_view)
        self._current_view = target_view
        self._update_status()
        await self._refresh_view()
        self._update_keybinds()

    async def _pop_view_history(self) -> None:
        if not self._view_history:
            return
        previous_view = self._view_history.pop()
        if previous_view == self._current_view:
            return
        self._current_view = previous_view
        self._update_status()
        await self._refresh_view()
        self._update_keybinds()

    def _show_resource_list(self) -> None:
        resource_list = self._resource_list_view()
        rows_table = self._rows_table_view()
        resource_list.display = True
        rows_table.display = False
        resource_list.focus()

    def _show_rows_table(self) -> None:
        resource_list = self._resource_list_view()
        rows_table = self._rows_table_view()
        resource_list.display = False
        rows_table.display = True
        rows_table.focus()

    def _populate_rows_table(self, row_page: RowPage) -> None:
        rows_table = self._rows_table_view()
        rows_table.clear(columns=True)
        if not row_page.columns:
            return
        formatted_rows = [
            [self._format_cell_value_for_table(value) for value in row]
            for row in row_page.rows
        ]
        column_widths: list[int] = []
        for column_index, column_name in enumerate(row_page.columns):
            max_cell_width = len(column_name)
            for formatted_row in formatted_rows:
                if column_index < len(formatted_row):
                    max_cell_width = max(
                        max_cell_width, len(formatted_row[column_index])
                    )
            column_widths.append(min(max_cell_width, self._max_table_cell_width))
        for column_name, width in zip(row_page.columns, column_widths, strict=False):
            rows_table.add_column(column_name, width=width or 1)
        for formatted_row in formatted_rows:
            rows_table.add_row(*formatted_row)
        self._update_status()

    def _filter_items(
        self,
        items: Sequence[NamedItemT],
        filter_text: str,
    ) -> list[NamedItemT]:
        if not filter_text:
            return list(items)
        return [item for item in items if filter_text.lower() in item.name.lower()]

    async def _clear_active_filter(self) -> bool:
        if self._current_view == "rows":
            return False
        if not self._resource_filters.get(self._current_view, ""):
            return False
        self._resource_filters[self._current_view] = ""
        self._update_message("Filter cleared.")
        self._update_status()
        self._update_keybinds()
        await self._refresh_view()
        return True

    def _footer_text(self) -> str:
        bindings = self._footer_bindings()
        return "  ".join([f"{key}: {label}" for key, label in bindings])

    def _footer_bindings(self) -> list[tuple[str, str]]:
        if self._input_mode == "command":
            return [("enter", "Run"), ("esc", "Cancel")]
        if self._input_mode == "filter":
            return [("enter", "Apply"), ("esc", "Cancel")]

        base = [("q", "Quit"), (":", "Command"), ("esc", "Back")]
        movement = [("j/k", "Move"), ("gg", "Top"), ("G", "Bottom")]

        if self._current_view == "rows":
            return (
                base
                + movement
                + [
                    ("h/l", "Left/Right"),
                    ("enter", "View Cell"),
                    ("y", "Yank"),
                    ("n/p", "Page"),
                ]
            )

        return base + movement + [("/", "Filter"), ("enter", "Select")]

    def _format_cell_value(self, value: object) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=True)
        return "" if value is None else str(value)

    def _format_cell_value_for_table(self, value: object) -> str:
        text = self._format_cell_value(value)
        if len(text) <= self._max_table_cell_width:
            return text
        return text[: self._max_table_cell_width - 3] + "..."

    def _format_cell_value_full(self, value: object) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=True, indent=2)
        return "" if value is None else str(value)

    def copy_text_to_clipboard(self, text: str) -> None:
        self.copy_to_clipboard(text)
        if sys.platform == "darwin":
            subprocess.run(["pbcopy"], input=text, text=True, check=True)

    def _show_cell_detail(self) -> None:
        if not self._rows_page.rows:
            self._update_message("No cell to view.")
            return
        rows_table = self._rows_table_view()
        coordinate = rows_table.cursor_coordinate
        if coordinate.row >= len(self._rows_page.rows):
            self._update_message("No cell to view.")
            return
        row = self._rows_page.rows[coordinate.row]
        if coordinate.column >= len(row):
            self._update_message("No cell to view.")
            return
        cell_value = row[coordinate.column]
        self.push_screen(CellDetailScreen(self._format_cell_value_full(cell_value)))


class CellDetailScreen(ModalScreen[None]):
    BINDINGS = [
        ("q", "dismiss", "Close"),
        ("escape", "dismiss", "Close"),
        ("y", "yank", "Yank Cell"),
    ]

    def __init__(self, cell_text: str) -> None:
        super().__init__()
        self._cell_text = cell_text

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(self._cell_text)

    def action_yank(self) -> None:
        if isinstance(self.app, DatabaseBrowserApp):
            self.app.copy_text_to_clipboard(self._cell_text)


class KeyBindingBar(Static):
    pass

    def _can_turn_page(self) -> bool:
        now = time.monotonic()
        if now - self._last_page_turn_at < self._page_turn_cooldown_seconds:
            return False
        self._last_page_turn_at = now
        return True
