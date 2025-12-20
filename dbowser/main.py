import argparse

from dbowser.config import (
    add_connection,
    ConnectionConfig,
    load_config,
    save_config,
)
from dbowser.tui import DatabaseBrowserApp


def main() -> None:
    parser = argparse.ArgumentParser(prog="dbowser")
    subparsers = parser.add_subparsers(dest="command")

    add_parser = subparsers.add_parser("add-connection")
    add_parser.add_argument("--name", required=True)
    add_parser.add_argument("--url", required=True)

    parser.add_argument("--conn")
    parser.add_argument("--db")
    parser.add_argument("--schema")

    args = parser.parse_args()

    if args.command == "add-connection":
        config = load_config()
        updated = add_connection(
            config,
            ConnectionConfig(name=args.name, url=args.url),
        )
        save_config(updated)
        print(f"Saved connection: {args.name}")
        return

    config = load_config()
    app = DatabaseBrowserApp(
        config,
        initial_connection_name=args.conn,
        initial_database_name=args.db,
        initial_schema_name=args.schema,
    )
    app.run()


if __name__ == "__main__":
    main()
