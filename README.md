# Consumed 🎬🎮📚

A personal entertainment tracker MCP server — like Spotify Wrapped, but for movies, games, and books.

Built with [FastMCP](https://gofastmcp.com), deployed on [FastMCP Cloud](https://fastmcp.cloud), and powered by [Turso](https://turso.tech) (SQLite in the cloud). Works seamlessly across Claude Desktop and the Claude Android app.

---

## What it does

Log everything you watch, play, and read — then ask Claude to analyze it. No forms, no dashboards. Just talk to Claude naturally and it handles the rest.

```
"I watched intersteller today, one of the best films i ever watched, easily top 3, rating it a solid 9.5"
"What games have I dropped this year?"
"Generate my wrapped report for this month"
"What's my average rating across all books?"
```

---

## Features

- **3 categories** — Movies/Series, Games, Books
- **Full CRUD** — add, update, delete, search entries
- **Smart filters** — filter by status, genre, or minimum rating
- **Stats** — per-category breakdown by genre, status, average rating
- **Wrapped reports** — Spotify Wrapped-style summaries across all 3 categories for any time period
- **Genre lists** — curated fixed genre lists exposed as MCP resources
- **Cross-device** — works on Claude Desktop and Claude Android app

---

## Stack

| Layer | Tech |
|---|---|
| MCP Framework | FastMCP 3.x |
| Database | Turso (SQLite over HTTP) |
| Deployment | FastMCP Cloud |
| Language | Python 3.12 |
| DB Client | httpx (Turso HTTP API) |

---

## Schema

**movies** — id, title, type (film/series), genre, status (watching/completed/dropped), rating (1-10), date_added, notes

**games** — id, title, genre, status (playing/completed/dropped), rating (1-10), date_added, notes

**books** — id, title, author, genre, status (reading/completed/dropped), rating (1-10), date_added, notes

---

## Tools

| Tool | Description |
|---|---|
| `add_entry` | Add a new movie, game, or book |
| `update_entry` | Update any field of an existing entry |
| `delete_entry` | Delete an entry by id |
| `search_entries` | Search by title (partial match) |
| `get_all` | List all entries with optional filters |
| `get_stats` | Stats for a category over a time period |
| `generate_wrapped` | Full cross-category wrapped report |

**Periods supported:** `all`, `this_year`, `this_month`, `this_quarter`

---

## Resources

| Resource | Description |
|---|---|
| `consumed://genres/movies` | Fixed genre list for movies |
| `consumed://genres/games` | Fixed genre list for games |
| `consumed://genres/books` | Fixed genre list for books |

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/preetdadga/consumed.git
cd consumed
```

### 2. Create a Turso database

Sign up at [turso.tech](https://turso.tech), create a database named `consumed`, and grab your database URL and auth token.

### 3. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env`:
```
TURSO_URL=https://consumed-yourname.aws-ap-south-1.turso.io
TURSO_AUTH_TOKEN=your-token-here
```

### 4. Install dependencies

```bash
pip install -r requirements.txt
```

### 5. Run locally

```bash
python server.py
```

---

## Deployment (FastMCP Cloud)

1. Push repo to GitHub
2. Connect repo on [fastmcp.cloud](https://fastmcp.cloud)
3. Set `TURSO_URL` and `TURSO_AUTH_TOKEN` in environment variables
4. Set entrypoint to `server.py`
5. Deploy — your MCP URL will appear in the dashboard

Add the URL to Claude via **Settings → Connectors → Add Custom Connector**.

---

## Project structure

```
consumed/
├── server.py          # MCP server — all tools, resources, and DB logic
├── requirements.txt   # Python dependencies
├── genres/
│   ├── movies.json    # Movie genre list
│   ├── games.json     # Game genre list
│   └── books.json     # Book genre list
└── .env.example       # Environment variable template
```

---

## Author

**Preet Dadga** — [GitHub](https://github.com/preetdadga) · [LinkedIn](https://linkedin.com/in/preet-dadga)
