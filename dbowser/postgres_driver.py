from dataclasses import dataclass
import os
from typing import Iterable
from urllib.parse import urlparse

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PostgresConnection


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


def _open_connection(connection_parameters: ConnectionParameters) -> PostgresConnection:
    connection = psycopg2.connect(
        host=connection_parameters.host,
        port=connection_parameters.port,
        user=connection_parameters.username,
        password=connection_parameters.password,
        dbname=connection_parameters.database_name,
    )
    connection.set_session(readonly=True, autocommit=True)
    return connection


def _fetch_databases(connection: PostgresConnection) -> list[DatabaseInfo]:
    query = """
        SELECT datname
        FROM pg_database
        WHERE datistemplate = false
        ORDER BY datname
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    return [DatabaseInfo(name=row[0]) for row in rows]


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


def list_databases(connection_parameters: ConnectionParameters) -> list[DatabaseInfo]:
    with _open_connection(connection_parameters) as connection:
        return _fetch_databases(connection)


def list_databases_from_env() -> list[DatabaseInfo]:
    connection_parameters = load_connection_parameters_from_env()
    return list_databases(connection_parameters)


def _fetch_schemas(connection: PostgresConnection) -> list[SchemaInfo]:
    query = """
        SELECT schema_name
        FROM information_schema.schemata
        ORDER BY schema_name
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    return [SchemaInfo(name=row[0]) for row in rows]


def list_schemas(connection_parameters: ConnectionParameters) -> list[SchemaInfo]:
    with _open_connection(connection_parameters) as connection:
        return _fetch_schemas(connection)


def _fetch_tables(connection: PostgresConnection, schema_name: str) -> list[TableInfo]:
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        ORDER BY table_name
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (schema_name,))
        rows = cursor.fetchall()
    return [TableInfo(name=row[0]) for row in rows]


def list_tables(
    connection_parameters: ConnectionParameters,
    schema_name: str,
) -> list[TableInfo]:
    with _open_connection(connection_parameters) as connection:
        return _fetch_tables(connection, schema_name)


def list_rows(
    connection_parameters: ConnectionParameters,
    schema_name: str,
    table_name: str,
    limit: int,
    offset: int,
) -> RowPage:
    query = sql.SQL("SELECT * FROM {}.{} LIMIT %s OFFSET %s").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
    )
    with _open_connection(connection_parameters) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (limit + 1, offset))
            rows = cursor.fetchall()
            columns = [column.name for column in cursor.description]
    has_more = len(rows) > limit
    trimmed_rows = rows[:limit]
    return RowPage(
        columns=columns,
        rows=trimmed_rows,
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


def connect_with_selection() -> None:
    base_parameters = load_connection_parameters_from_env()
    databases = list_databases(base_parameters)

    selected_database = _prompt_for_database_selection(databases)
    selected_parameters = build_database_connection_parameters(
        base_parameters,
        selected_database.name,
    )

    with _open_connection(selected_parameters) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            current_database_row = cursor.fetchone()
            if current_database_row is None:
                raise ValueError("Expected current_database() to return a value.")
            current_database = current_database_row[0]
    print(f"Connected to database: {current_database}")
