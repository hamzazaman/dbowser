import asyncio

from dbowser.postgres_driver import list_databases, load_connection_parameters_from_env
from dbowser.tui import DatabaseBrowserApp


def main() -> None:
    base_parameters = load_connection_parameters_from_env()
    databases = asyncio.run(list_databases(base_parameters))
    app = DatabaseBrowserApp(base_parameters, databases)
    app.run()


if __name__ == "__main__":
    main()
