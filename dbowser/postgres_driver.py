from contextlib import asynccontextmanager
from dataclasses import dataclass
import re
from typing import AsyncIterator, Iterable
from urllib.parse import urlparse

import asyncpg
from asyncpg import Connection, Pool
from asyncpg.pool import PoolConnectionProxy


_POOL_MIN_SIZE = 1
_POOL_MAX_SIZE = 4


@dataclass(frozen=True)
class ConnectionParameters:
    host: str
    port: int
    username: str
    password: str
    database_name: str
    restricted_database_name: str


_pools: dict[ConnectionParameters, Pool] = {}


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


def _parse_connection_parameters(connection_url: str) -> ConnectionParameters:
    parsed_url = urlparse(connection_url)
    parsed_path = parsed_url.path.lstrip("/")
    if parsed_path:
        database_name = parsed_path.split("/", 1)[0]
        restricted_database_name = database_name
    else:
        database_name = "postgres"
        restricted_database_name = ""
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
        restricted_database_name=restricted_database_name,
    )


def parse_connection_parameters(connection_url: str) -> ConnectionParameters:
    return _parse_connection_parameters(connection_url)


def _quote_identifier(identifier: str) -> str:
    if not identifier:
        raise ValueError("Identifier cannot be empty.")
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _strip_leading_query_comments(query_text: str) -> str:
    remaining_text = query_text
    while True:
        stripped = remaining_text.lstrip()
        if stripped.startswith("--"):
            newline_index = stripped.find("\n")
            if newline_index == -1:
                return ""
            remaining_text = stripped[newline_index + 1 :]
            continue
        if stripped.startswith("/*"):
            end_index = stripped.find("*/")
            if end_index == -1:
                return ""
            remaining_text = stripped[end_index + 2 :]
            continue
        return stripped


def _strip_trailing_query_comments(query_text: str) -> str:
    remaining_text = query_text.rstrip()
    while True:
        remaining_text = remaining_text.rstrip()
        if remaining_text.endswith("*/"):
            start_index = remaining_text.rfind("/*")
            if start_index == -1:
                break
            remaining_text = remaining_text[:start_index]
            continue
        last_newline_index = remaining_text.rfind("\n")
        last_line = remaining_text[last_newline_index + 1 :]
        if not last_line.lstrip().startswith("--"):
            break
        remaining_text = (
            remaining_text[:last_newline_index] if last_newline_index != -1 else ""
        )
    return remaining_text.rstrip()


def _normalize_query_text(query_text: str) -> str:
    trimmed = query_text.strip()
    if not trimmed:
        return ""
    trimmed = _strip_leading_query_comments(trimmed)
    trimmed = _strip_trailing_query_comments(trimmed)
    trimmed = trimmed.rstrip()
    trimmed = re.sub(r";(?=\s*(--[^\n]*\s*)*$)", "", trimmed)
    while trimmed.endswith(";"):
        trimmed = trimmed[:-1].rstrip()
    return trimmed


async def _init_connection(connection: Connection) -> None:
    await connection.execute("SET default_transaction_read_only = on")
    await connection.execute("SET statement_timeout = '10s'")


async def _get_pool(connection_parameters: ConnectionParameters) -> Pool:
    pool = _pools.get(connection_parameters)
    if pool is not None:
        return pool
    pool = await asyncpg.create_pool(
        host=connection_parameters.host,
        port=connection_parameters.port,
        user=connection_parameters.username,
        password=connection_parameters.password,
        database=connection_parameters.database_name,
        min_size=_POOL_MIN_SIZE,
        max_size=_POOL_MAX_SIZE,
        init=_init_connection,
    )
    _pools[connection_parameters] = pool
    return pool


async def close_pools() -> None:
    pools = list(_pools.values())
    _pools.clear()
    for pool in pools:
        await pool.close()


@asynccontextmanager
async def _acquire_connection(
    connection_parameters: ConnectionParameters,
) -> AsyncIterator[Connection | PoolConnectionProxy]:
    pool = await _get_pool(connection_parameters)
    async with pool.acquire() as connection:
        yield connection


async def _fetch_databases(
    connection: Connection | PoolConnectionProxy,
) -> list[DatabaseInfo]:
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


async def list_databases(
    connection_parameters: ConnectionParameters,
) -> list[DatabaseInfo]:
    if connection_parameters.restricted_database_name:
        return [DatabaseInfo(name=connection_parameters.restricted_database_name)]
    async with _acquire_connection(connection_parameters) as connection:
        return await _fetch_databases(connection)


async def _fetch_schemas(
    connection: Connection | PoolConnectionProxy,
) -> list[SchemaInfo]:
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
    async with _acquire_connection(connection_parameters) as connection:
        return await _fetch_schemas(connection)


async def _fetch_tables(
    connection: Connection | PoolConnectionProxy,
    schema_name: str,
) -> list[TableInfo]:
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
    async with _acquire_connection(connection_parameters) as connection:
        return await _fetch_tables(connection, schema_name)


async def list_rows(
    connection_parameters: ConnectionParameters,
    schema_name: str,
    table_name: str,
    limit: int,
    offset: int,
    where_clause: str,
    order_by_clause: str,
) -> RowPage:
    schema_identifier = _quote_identifier(schema_name)
    table_identifier = _quote_identifier(table_name)
    where_sql = f" WHERE {where_clause}" if where_clause else ""
    order_sql = f" ORDER BY {order_by_clause}" if order_by_clause else ""
    query = (
        f"SELECT * FROM {schema_identifier}.{table_identifier}"
        f"{where_sql}{order_sql} LIMIT $1 OFFSET $2"
    )
    async with _acquire_connection(connection_parameters) as connection:
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


async def run_query(
    connection_parameters: ConnectionParameters,
    query_text: str,
    limit: int,
    offset: int,
) -> RowPage:
    normalized = _normalize_query_text(query_text)
    if not normalized:
        raise ValueError("Query is empty.")
    query = f"SELECT * FROM ({normalized}) AS query_result LIMIT $1 OFFSET $2"
    async with _acquire_connection(connection_parameters) as connection:
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
        restricted_database_name=base_parameters.restricted_database_name,
    )
