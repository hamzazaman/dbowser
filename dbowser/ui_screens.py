from typing import Protocol, runtime_checkable

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.events import Key
from textual.screen import ModalScreen
from textual.widgets import Header, Input, Static

from dbowser.config import ConnectionConfig


@runtime_checkable
class _AppWithErrorDialog(Protocol):
    _error_dialog_open: bool


@runtime_checkable
class _AppWithClipboard(Protocol):
    def copy_text_to_clipboard(self, text: str) -> None: ...


class KeyBindingBar(Static):
    def __init__(self) -> None:
        super().__init__("", markup=True)


class AddConnectionDialog(ModalScreen[ConnectionConfig | None]):
    BINDINGS = [
        ("escape", "dismiss", "Close"),
    ]

    def compose(self) -> ComposeResult:
        with Vertical(id="add-connection-dialog"):
            yield Static("Add Connection", id="add-connection-title")
            yield Static("Name", id="add-connection-name-label")
            yield Input(placeholder="prod", id="add-connection-name")
            yield Static("URL", id="add-connection-url-label")
            yield Input(
                placeholder="postgresql://user:pass@host:5432/postgres",
                id="add-connection-url",
            )
            yield Static("", id="add-connection-error")

    def on_mount(self) -> None:
        self.focus()
        self.query_one("#add-connection-name", Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "add-connection-name":
            self.query_one("#add-connection-url", Input).focus()
            return
        if event.input.id != "add-connection-url":
            return
        name = self.query_one("#add-connection-name", Input).value.strip()
        url = self.query_one("#add-connection-url", Input).value.strip()
        if not name or not url:
            self.query_one("#add-connection-error", Static).update(
                "Name and URL are required."
            )
            return
        self.dismiss(ConnectionConfig(name=name, url=url))

    def action_cursor_down(self) -> None:
        self.query_one("#add-connection-url", Input).focus()

    def action_cursor_up(self) -> None:
        self.query_one("#add-connection-name", Input).focus()


class ErrorDialog(ModalScreen[None]):
    BINDINGS = [
        ("escape", "dismiss", "Close"),
    ]

    def __init__(self, title: str, message: str) -> None:
        super().__init__()
        self._title = title
        self._message = message

    def compose(self) -> ComposeResult:
        with Vertical(id="error-dialog"):
            yield Static(self._title, id="error-title")
            yield Static(self._message, id="error-message")

    def on_mount(self) -> None:
        self.focus()

    def on_key(self, event: Key) -> None:
        if event.key == "escape":
            self.dismiss()
            event.stop()

    def on_unmount(self) -> None:
        app = self.app
        if isinstance(app, _AppWithErrorDialog):
            app._error_dialog_open = False


class CellDetailScreen(ModalScreen[None]):
    BINDINGS = [
        ("escape", "dismiss", "Close"),
        ("y", "yank", "Yank Cell"),
    ]

    def __init__(self, cell_text: str, status_text: str, view_text: str) -> None:
        super().__init__()
        self._cell_text = cell_text
        self._status_text = status_text
        self._view_text = view_text

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical():
            with Horizontal(id="top-bar"):
                yield Static(self._status_text, id="selected-status")
            keybinds = KeyBindingBar()
            keybinds.id = "keybinds-bar"
            keybinds.update("[bold cyan]y[/] Yank  [bold cyan]esc[/] Back")
            yield keybinds
            with Horizontal(id="view-bar"):
                yield Static("", id="view-bar-left")
                yield Static(self._view_text, id="view-bar-text")
                yield Static("", id="loading-indicator")
            with VerticalScroll():
                yield Static(self._cell_text, id="cell-detail-text")

    def action_yank(self) -> None:
        app = self.app
        if isinstance(app, _AppWithClipboard):
            app.copy_text_to_clipboard(self._cell_text)
