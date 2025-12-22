from dataclasses import dataclass
import json
from pathlib import Path


@dataclass(frozen=True)
class ConnectionConfig:
    name: str
    url: str


@dataclass(frozen=True)
class AppConfig:
    connections: list[ConnectionConfig]


@dataclass(frozen=True)
class LastSelection:
    connection_name: str
    database_name: str
    schema_name: str


def _config_dir() -> Path:
    return Path.home() / ".config" / ".dbowser"


def _config_path() -> Path:
    return _config_dir() / "connections.json"


def _query_path() -> Path:
    return _config_dir() / "query.sql"


def _last_selection_path() -> Path:
    return _config_dir() / "last_selection.json"


def load_config() -> AppConfig:
    config_path = _config_path()
    if not config_path.exists():
        return AppConfig(connections=[])
    data = json.loads(config_path.read_text(encoding="utf-8"))
    connections = [
        ConnectionConfig(name=item["name"], url=item["url"])
        for item in data.get("connections", [])
    ]
    return AppConfig(connections=connections)


def save_config(config: AppConfig) -> None:
    config_dir = _config_dir()
    config_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "connections": [
            {"name": connection.name, "url": connection.url}
            for connection in config.connections
        ],
    }
    _config_path().write_text(json.dumps(payload, indent=2), encoding="utf-8")


def add_connection(config: AppConfig, connection: ConnectionConfig) -> AppConfig:
    if any(existing.name == connection.name for existing in config.connections):
        raise ValueError(f"Connection name already exists: {connection.name}")
    updated_connections = [*config.connections, connection]
    return AppConfig(connections=updated_connections)


def load_last_selection() -> LastSelection:
    selection_path = _last_selection_path()
    if not selection_path.exists():
        return LastSelection(connection_name="", database_name="", schema_name="")
    data = json.loads(selection_path.read_text(encoding="utf-8"))
    return LastSelection(
        connection_name=data.get("connection_name", ""),
        database_name=data.get("database_name", ""),
        schema_name=data.get("schema_name", ""),
    )


def save_last_selection(selection: LastSelection) -> None:
    config_dir = _config_dir()
    config_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "connection_name": selection.connection_name,
        "database_name": selection.database_name,
        "schema_name": selection.schema_name,
    }
    _last_selection_path().write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )


def load_last_query() -> str:
    query_path = _query_path()
    if not query_path.exists():
        return "SELECT 1;"
    return query_path.read_text(encoding="utf-8").strip() or "SELECT 1;"


def save_last_query(query_text: str) -> None:
    config_dir = _config_dir()
    config_dir.mkdir(parents=True, exist_ok=True)
    _query_path().write_text(query_text.strip() or "SELECT 1;", encoding="utf-8")


def query_path() -> Path:
    return _query_path()
