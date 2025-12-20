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


def _config_dir() -> Path:
    return Path.home() / ".config" / ".dbowser"


def _config_path() -> Path:
    return _config_dir() / "connections.json"


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
