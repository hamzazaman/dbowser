# dbowser

Terminal UI for browsing PostgreSQL databases, inspired by DBeaver and k9s. Focused on fast navigation, familiar k9s-style keybindings, and a clean, glanceable layout.

![dbowser demo](assets/dbowser-demo.gif)

## Installation

Installation is recommended via [uv](https://docs.astral.sh/uv/getting-started/installation/)

With uv with the repo cloned:

```bash
uv tool install .
```

With uv directly from GitHub:

```bash
uv tool install git+https://github.com/hamzazaman/dbowser
```

## Usage

Add a connection (saved under `~/.config/.dbowser/connections.json`):

```bash
dbowser add-connection --name prod --url postgresql://user:pass@host:5432/postgres
```

Launch the TUI:

```bash
dbowser
```

Start with a preselected connection/database/schema:

```bash
dbowser --conn prod --db mydb --schema public
```

### Navigation

- `j/k` move, `enter` select
- `:` command mode, `/` filter (not in rows view)
- `w` edit WHERE, `o` edit ORDER BY (rows view)
- `n/p` next/previous page (rows view)
- type a line number, then `G` to jump to that line

### Commands

- `:connection` / `:db` / `:schema` / `:table` / `:rows` focus views
- `:query` open query view
- `:pagesize N` set rows per page
- `:q` quit
