import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
import re
from typing import AsyncIterator, Iterable
from urllib.parse import urlparse

import asyncpg
from asyncpg import Connection


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class ConnectionParameters:
    host: str
    port: int
    username: str
    password: str
    database_name: str


@dataclass(frozen=True)
class DatabaseInfo:
    name: str


@dataclass(frozen=True)
class SchemaInfo:
    name: str


@dataclass(frozen=True)
class TableInfo:
    name: str
    estimated_rows: int


@dataclass(frozen=True)
class RowPage:
    columns: list[str]
    rows: list[tuple[object, ...]]
    limit: int
    offset: int
    has_more: bool


def _load_connection_url() -> str:
    return os.environ["DBOWSER_CONN_URL"]


def _parse_connection_parameters(connection_url: str) -> ConnectionParameters:
    parsed_url = urlparse(connection_url)
    database_name = parsed_url.path.lstrip("/") or "postgres"
    if parsed_url.hostname is None:
        raise ValueError("Missing required connection field: host")
    if parsed_url.username is None:
        raise ValueError("Missing required connection field: username")
    if parsed_url.password is None:
        raise ValueError("Missing required connection field: password")

    return ConnectionParameters(
        host=parsed_url.hostname,
        port=parsed_url.port or 5432,
        username=parsed_url.username,
        password=parsed_url.password,
        database_name=database_name,
    )


def _validate_identifier(name: str, label: str) -> None:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {label} identifier: {name}")


@asynccontextmanager
async def _open_connection(
    connection_parameters: ConnectionParameters,
) -> AsyncIterator[Connection]:
    connection = await asyncpg.connect(
        host=connection_parameters.host,
        port=connection_parameters.port,
        user=connection_parameters.username,
        password=connection_parameters.password,
        database=connection_parameters.database_name,
    )
    try:
        await connection.execute("SET default_transaction_read_only = on")
        await connection.execute("SET statement_timeout = '10s'")
        yield connection
    finally:
        await connection.close()


async def _fetch_databases(connection: Connection) -> list[DatabaseInfo]:
    query = """
        SELECT datname
        FROM pg_database
        WHERE datistemplate = false
        ORDER BY datname
    """
    rows = await connection.fetch(query)
    return [DatabaseInfo(name=row["datname"]) for row in rows]


def _prompt_for_database_selection(databases: Iterable[DatabaseInfo]) -> DatabaseInfo:
    database_list = list(databases)
    if not database_list:
        raise ValueError("No databases returned from server.")

    print("Available databases:")
    for index, database in enumerate(database_list, start=1):
        print(f"{index}. {database.name}")

    selection = int(input("Select a database by number: ").strip())
    if selection < 1 or selection > len(database_list):
        raise ValueError("Selected index is out of range.")

    return database_list[selection - 1]


def load_connection_parameters_from_env() -> ConnectionParameters:
    connection_url = _load_connection_url()
    return _parse_connection_parameters(connection_url)


async def list_databases(
    connection_parameters: ConnectionParameters,
) -> list[DatabaseInfo]:
    async with _open_connection(connection_parameters) as connection:
        return await _fetch_databases(connection)


async def list_databases_from_env() -> list[DatabaseInfo]:
    connection_parameters = load_connection_parameters_from_env()
    return await list_databases(connection_parameters)


async def _fetch_schemas(connection: Connection) -> list[SchemaInfo]:
    query = """
        SELECT schema_name
        FROM information_schema.schemata
        ORDER BY schema_name
    """
    rows = await connection.fetch(query)
    return [SchemaInfo(name=row["schema_name"]) for row in rows]


async def list_schemas(
    connection_parameters: ConnectionParameters,
) -> list[SchemaInfo]:
    async with _open_connection(connection_parameters) as connection:
        return await _fetch_schemas(connection)


async def _fetch_tables(connection: Connection, schema_name: str) -> list[TableInfo]:
    query = """
        SELECT
            c.relname AS table_name,
            CASE
                WHEN c.reltuples < 0 THEN 0
                ELSE c.reltuples::bigint
            END AS estimated_rows
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relkind = 'r'
        ORDER BY c.relname
    """
    rows = await connection.fetch(query, schema_name)
    return [
        TableInfo(
            name=row["table_name"],
            estimated_rows=row["estimated_rows"],
        )
        for row in rows
    ]


async def list_tables(
    connection_parameters: ConnectionParameters,
    schema_name: str,
) -> list[TableInfo]:
    async with _open_connection(connection_parameters) as connection:
        return await _fetch_tables(connection, schema_name)


async def list_rows(
    connection_parameters: ConnectionParameters,
    schema_name: str,
    table_name: str,
    limit: int,
    offset: int,
    where_clause: str,
) -> RowPage:
    _validate_identifier(schema_name, "schema")
    _validate_identifier(table_name, "table")
    where_sql = f" WHERE {where_clause}" if where_clause else ""
    query = (
        f'SELECT * FROM "{schema_name}"."{table_name}"{where_sql} LIMIT $1 OFFSET $2'
    )
    async with _open_connection(connection_parameters) as connection:
        statement = await connection.prepare(query)
        columns = [attribute.name for attribute in statement.get_attributes()]
        records = await statement.fetch(limit + 1, offset)
    has_more = len(records) > limit
    trimmed_records = records[:limit]
    rows = [tuple(record) for record in trimmed_records]
    return RowPage(
        columns=columns,
        rows=rows,
        limit=limit,
        offset=offset,
        has_more=has_more,
    )


def build_database_connection_parameters(
    base_parameters: ConnectionParameters,
    database_name: str,
) -> ConnectionParameters:
    return ConnectionParameters(
        host=base_parameters.host,
        port=base_parameters.port,
        username=base_parameters.username,
        password=base_parameters.password,
        database_name=database_name,
    )


async def connect_with_selection() -> None:
    base_parameters = load_connection_parameters_from_env()
    databases = await list_databases(base_parameters)

    selected_database = _prompt_for_database_selection(databases)
    selected_parameters = build_database_connection_parameters(
        base_parameters,
        selected_database.name,
    )

    async with _open_connection(selected_parameters) as connection:
        row = await connection.fetchrow("SELECT current_database() AS name")
    if row is None:
        raise ValueError("Failed to read current database.")
    print(f"Connected to database: {row['name']}")


def connect_with_selection_sync() -> None:
    asyncio.run(connect_with_selection())
