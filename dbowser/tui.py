import json
import subprocess
import sys
from typing import Protocol, Sequence, TypeVar

from textual.app import App, ComposeResult
from textual.containers import Vertical
from textual.widgets import (
    DataTable,
    Footer,
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
    def __init__(self, table_name: str) -> None:
        super().__init__(Static(table_name))
        self.table_name = table_name


class _NamedItem(Protocol):
    @property
    def name(self) -> str:
        return ""


NamedItemT = TypeVar("NamedItemT", bound=_NamedItem)


class DatabaseBrowserApp(App):
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("j", "cursor_down", "Down"),
        ("k", "cursor_up", "Up"),
        ("h", "cursor_left", "Left"),
        ("l", "cursor_right", "Right"),
        ("y", "yank_cell", "Yank Cell"),
        ("n", "next_page", "Next Page"),
        ("p", "previous_page", "Prev Page"),
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
        self._rows_page_offset = 0
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
            yield Static(self._status_text(), id="selected-status")
            yield Static("", id="message-line")
            yield ListView(id="resource-list")
            yield DataTable(id="rows-table")
            yield Input(placeholder="Command", id="command-input")
        yield Footer()

    def on_mount(self) -> None:
        self._refresh_view()
        self._resource_list_view().focus()
        rows_table = self.query_one("#rows-table", DataTable)
        rows_table.display = False
        command_input = self.query_one("#command-input", Input)
        command_input.display = False

    def action_select_resource(self) -> None:
        if self._input_mode:
            return
        resource_list = self._resource_list_view()
        if self._current_view == "database":
            self._select_database(resource_list)
        elif self._current_view == "schema":
            self._select_schema(resource_list)
        elif self._current_view == "table":
            self._select_table(resource_list)

    def action_enter_filter_mode(self) -> None:
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

    def action_next_page(self) -> None:
        if self._input_mode or self._current_view != "rows":
            return
        if not self._rows_page.has_more:
            return
        self._rows_page_offset += self._rows_page_limit
        self._load_rows()
        self._populate_rows_table(self._rows_page)

    def action_previous_page(self) -> None:
        if self._input_mode or self._current_view != "rows":
            return
        if self._rows_page_offset == 0:
            return
        self._rows_page_offset = max(0, self._rows_page_offset - self._rows_page_limit)
        self._load_rows()
        self._populate_rows_table(self._rows_page)

    def action_yank_cell(self) -> None:
        if self._input_mode or self._current_view != "rows":
            return
        if not self._rows_page.rows:
            self._update_message("No cell to yank.")
            return
        rows_table = self._rows_table_view()
        cell_value = rows_table.get_cell_at(rows_table.cursor_coordinate)
        self._copy_to_clipboard(self._format_cell_value(cell_value))
        self._update_message("Yanked cell to clipboard.")

    def _copy_to_clipboard(self, text: str) -> None:
        self.copy_to_clipboard(text)
        if sys.platform == "darwin":
            subprocess.run(["pbcopy"], input=text, text=True, check=True)

    def action_escape(self) -> None:
        if self._input_mode:
            self._close_input_mode()
            return
        self._pop_view_history()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "command-input":
            return
        submitted_value = event.value.strip()
        if self._input_mode == "filter":
            self._apply_filter(submitted_value)
        elif self._input_mode == "command":
            self._run_command(submitted_value)
        self._close_input_mode()

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if event.list_view.id != "resource-list":
            return
        if self._input_mode:
            return
        self.action_select_resource()

    def _status_text(self) -> str:
        database_text = self._selected_database_name or "<none>"
        schema_text = self._selected_schema_name or "<none>"
        table_text = self._selected_table_name or "<none>"
        page_text = ""
        if self._current_view == "rows":
            page_number = (self._rows_page_offset // self._rows_page_limit) + 1
            page_text = f" | page: {page_number}"
        return (
            f"View: {self._current_view} | Selected database: "
            f"{database_text} | schema: {schema_text} | table: {table_text}"
            f"{page_text}"
        )

    def _update_status(self) -> None:
        status = self.query_one("#selected-status", Static)
        status.update(self._status_text())

    def _update_message(self, message: str) -> None:
        message_line = self.query_one("#message-line", Static)
        message_line.update(message)

    def _resource_list_view(self) -> ListView:
        return self.query_one("#resource-list", ListView)

    def _rows_table_view(self) -> DataTable:
        return self.query_one("#rows-table", DataTable)

    def _select_database(self, resource_list: ListView) -> None:
        if not isinstance(resource_list.highlighted_child, DatabaseListItem):
            return
        self._selected_database_name = resource_list.highlighted_child.database_name
        self._selected_schema_name = ""
        self._selected_table_name = ""
        self._rows_page_offset = 0
        self._update_status()
        self._load_schemas()
        self._set_view("schema")

    def _select_schema(self, resource_list: ListView) -> None:
        if not isinstance(resource_list.highlighted_child, SchemaListItem):
            return
        self._selected_schema_name = resource_list.highlighted_child.schema_name
        self._selected_table_name = ""
        self._rows_page_offset = 0
        self._update_status()
        self._load_tables()
        self._set_view("table")

    def _select_table(self, resource_list: ListView) -> None:
        if not isinstance(resource_list.highlighted_child, TableListItem):
            return
        self._selected_table_name = resource_list.highlighted_child.table_name
        self._rows_page_offset = 0
        self._update_status()
        self._load_rows()
        self._set_view("rows")

    def _load_schemas(self) -> None:
        if not self._selected_database_name:
            self._schemas = []
            return
        selected_parameters = build_database_connection_parameters(
            self._base_connection_parameters,
            self._selected_database_name,
        )
        self._schemas = list_schemas(selected_parameters)
        self._tables = []

    def _load_tables(self) -> None:
        if not self._selected_database_name or not self._selected_schema_name:
            self._tables = []
            return
        selected_parameters = build_database_connection_parameters(
            self._base_connection_parameters,
            self._selected_database_name,
        )
        self._tables = list_tables(selected_parameters, self._selected_schema_name)

    def _load_rows(self) -> None:
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
        self._rows_page = list_rows(
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

    def _close_input_mode(self) -> None:
        command_input = self.query_one("#command-input", Input)
        command_input.display = False
        command_input.value = ""
        self._input_mode = ""
        if self._current_view == "rows":
            self._rows_table_view().focus()
        else:
            self._resource_list_view().focus()

    def _apply_filter(self, filter_text: str) -> None:
        self._resource_filters[self._current_view] = filter_text
        self._refresh_view()

    def _run_command(self, command_text: str) -> None:
        if command_text in {"q", "quit", "exit"}:
            self.exit()
            return
        if not command_text:
            self._update_message("")
            return
        if self._handle_focus_command(command_text):
            return
        self._update_message(f"Unknown command: {command_text}")

    def _handle_focus_command(self, command_text: str) -> bool:
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
        self._set_view(target_view)
        self._update_message(f"Focused {normalized}")
        return True

    def _refresh_view(self) -> None:
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
            self._load_schemas()
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
            self._load_tables()
            filtered = self._filter_items(
                self._tables,
                self._resource_filters["table"],
            )
            for table in filtered:
                resource_list.append(TableListItem(table.name))
            return
        if self._current_view == "rows":
            self._show_rows_table()
            if not self._selected_database_name or not self._selected_schema_name:
                self._update_message("Select a database and schema first.")
                return
            if not self._selected_table_name:
                self._update_message("Select a table first.")
                return
            self._load_rows()
            self._populate_rows_table(self._rows_page)

    def _set_view(self, target_view: str) -> None:
        if target_view == self._current_view:
            return
        self._view_history.append(self._current_view)
        self._current_view = target_view
        self._update_status()
        self._refresh_view()

    def _pop_view_history(self) -> None:
        if not self._view_history:
            return
        previous_view = self._view_history.pop()
        if previous_view == self._current_view:
            return
        self._current_view = previous_view
        self._update_status()
        self._refresh_view()

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
        rows_table.add_columns(*row_page.columns)
        for row in row_page.rows:
            rows_table.add_row(*(self._format_cell_value(value) for value in row))
        self._update_status()

    def _filter_items(
        self,
        items: Sequence[NamedItemT],
        filter_text: str,
    ) -> list[NamedItemT]:
        if not filter_text:
            return list(items)
        return [item for item in items if filter_text.lower() in item.name.lower()]

    def _format_cell_value(self, value: object) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=True)
        return "" if value is None else str(value)
