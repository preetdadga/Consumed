import json
import os
from contextlib import asynccontextmanager
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import httpx
from dotenv import load_dotenv
from fastmcp import FastMCP

load_dotenv()
TURSO_URL = os.environ["TURSO_URL"]
TURSO_AUTH_TOKEN = os.environ["TURSO_AUTH_TOKEN"]
TURSO_PIPELINE_URL = f"{TURSO_URL}/v2/pipeline"

CATEGORY_SETTINGS = {
    "movies": {
        "table": "movies",
        "status_values": {"watching", "completed", "dropped"},
        "in_progress": {"watching"},
    },
    "games": {
        "table": "games",
        "status_values": {"playing", "completed", "dropped"},
        "in_progress": {"playing"},
    },
    "books": {
        "table": "books",
        "status_values": {"reading", "completed", "dropped"},
        "in_progress": {"reading"},
    },
}


# --- Turso HTTP API helpers ---

def _make_arg(value: Any) -> Dict:
    if value is None:
        return {"type": "null", "value": None}
    if isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    if isinstance(value, float):
        return {"type": "float", "value": value}
    return {"type": "text", "value": str(value)}


async def _execute(sql: str, params: Tuple = ()) -> Dict:
    payload = {
        "requests": [
            {
                "type": "execute",
                "stmt": {
                    "sql": sql,
                    "args": [_make_arg(p) for p in params],
                },
            },
            {"type": "close"},
        ]
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            TURSO_PIPELINE_URL,
            json=payload,
            headers={"Authorization": f"Bearer {TURSO_AUTH_TOKEN}"},
            timeout=10,
        )
        if resp.status_code >= 400:
            print(f"[Turso ERROR] {resp.status_code}: {resp.text}")
            resp.raise_for_status()
        data = resp.json()
        # Turso can return a 200 but still have an error inside the result
        result_wrapper = data["results"][0]
        if result_wrapper.get("type") == "error":
            error_msg = result_wrapper.get("error", {}).get("message", "Unknown Turso error")
            print(f"[Turso RESULT ERROR] {error_msg}")
            raise RuntimeError(error_msg)
        result = result_wrapper["response"]["result"]
        return result


async def _fetch_rows(sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
    result = await _execute(sql, params)
    cols = [c["name"] for c in result["cols"]]
    return [
        {cols[i]: (row[i]["value"] if row[i]["type"] != "null" else None) for i in range(len(cols))}
        for row in result["rows"]
    ]


# --- Normalizers ---

def _get_table(category: str) -> str:
    if category not in CATEGORY_SETTINGS:
        raise ValueError(f"Invalid category: {category}")
    return CATEGORY_SETTINGS[category]["table"]


def _normalize_status(category: str, status: str) -> str:
    s = status.strip().lower()
    if s not in CATEGORY_SETTINGS[category]["status_values"]:
        raise ValueError(f"Invalid status for {category}: {status}")
    return s


def _normalize_genre(genre: str) -> str:
    g = genre.strip()
    if not g:
        raise ValueError("Genre must not be empty")
    return g


def _normalize_type(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    n = value.strip().lower()
    if n not in {"film", "series"}:
        raise ValueError("Movie type must be 'film' or 'series'")
    return n


def _normalize_author(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    a = value.strip()
    if not a:
        raise ValueError("Author must not be empty")
    return a


def _normalize_rating(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    r = float(value)
    if r < 1 or r > 10:
        raise ValueError("Rating must be between 1 and 10")
    return r


def _normalize_date(value: Optional[str]) -> str:
    if not value:
        return date.today().isoformat()
    return value.strip()


def _get_period_start(period: str) -> Optional[str]:
    today = date.today()
    p = period.strip().lower()
    if p == "all":
        return None
    if p == "this_year":
        return date(today.year, 1, 1).isoformat()
    if p == "this_month":
        return date(today.year, today.month, 1).isoformat()
    if p == "this_quarter":
        quarter = (today.month - 1) // 3
        return date(today.year, quarter * 3 + 1, 1).isoformat()
    raise ValueError(f"Invalid period: {period}")


def _period_filter(period: str) -> Tuple[str, Tuple]:
    start = _get_period_start(period)
    if start is None:
        return "", ()
    return "WHERE date_added >= ?", (start,)


def _load_genres(category: str) -> List[str]:
    genre_file = os.path.join(os.path.dirname(__file__), f"genres/{category}.json")
    with open(genre_file, "r") as f:
        return json.load(f)


# --- Lifespan: create tables on startup ---

@asynccontextmanager
async def lifespan(server):
    await _execute("""
        CREATE TABLE IF NOT EXISTS movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            type TEXT NOT NULL,
            genre TEXT NOT NULL,
            status TEXT NOT NULL,
            rating REAL,
            date_added TEXT NOT NULL,
            notes TEXT DEFAULT ''
        )
    """)
    await _execute("""
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            genre TEXT NOT NULL,
            status TEXT NOT NULL,
            rating REAL,
            date_added TEXT NOT NULL,
            notes TEXT DEFAULT ''
        )
    """)
    await _execute("""
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            author TEXT NOT NULL,
            genre TEXT NOT NULL,
            status TEXT NOT NULL,
            rating REAL,
            date_added TEXT NOT NULL,
            notes TEXT DEFAULT ''
        )
    """)
    yield


mcp = FastMCP("Consumed", lifespan=lifespan)


# --- Resources ---

@mcp.resource("consumed://genres/movies")
def genres_movies() -> List[str]:
    return _load_genres("movies")


@mcp.resource("consumed://genres/games")
def genres_games() -> List[str]:
    return _load_genres("games")


@mcp.resource("consumed://genres/books")
def genres_books() -> List[str]:
    return _load_genres("books")


# --- Tools ---

@mcp.tool("add_entry")
async def add_entry(
    category: str,
    title: str,
    genre: str,
    status: str,
    date_added: Optional[str] = None,
    rating: Optional[float] = None,
    notes: str = "",
    type: Optional[str] = None,
    author: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        category = category.strip().lower()
        _get_table(category)
        title_value = title.strip()
        if not title_value:
            raise ValueError("Title must not be empty")

        genre_value = _normalize_genre(genre)
        status_value = _normalize_status(category, status)
        date_value = _normalize_date(date_added)
        rating_value = _normalize_rating(rating)
        notes_value = notes or ""

        if category == "movies":
            type_value = _normalize_type(type)
            if type_value is None:
                raise ValueError("Movie type is required")
            result = await _execute(
                "INSERT INTO movies (title, type, genre, status, rating, date_added, notes) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (title_value, type_value, genre_value, status_value, rating_value, date_value, notes_value),
            )
        elif category == "games":
            result = await _execute(
                "INSERT INTO games (title, genre, status, rating, date_added, notes) VALUES (?, ?, ?, ?, ?, ?)",
                (title_value, genre_value, status_value, rating_value, date_value, notes_value),
            )
        else:
            author_value = _normalize_author(author)
            if author_value is None:
                raise ValueError("Book author is required")
            result = await _execute(
                "INSERT INTO books (title, author, genre, status, rating, date_added, notes) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (title_value, author_value, genre_value, status_value, rating_value, date_value, notes_value),
            )

        return {"status": "success", "id": result.get("last_insert_rowid")}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@mcp.tool("update_entry")
async def update_entry(
    category: str,
    id: int,
    title: Optional[str] = None,
    genre: Optional[str] = None,
    status: Optional[str] = None,
    rating: Optional[float] = None,
    notes: Optional[str] = None,
    type: Optional[str] = None,
    author: Optional[str] = None,
    date_added: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        category = category.strip().lower()
        table = _get_table(category)
        updates: List[str] = []
        values: List[Any] = []

        if title is not None:
            t = title.strip()
            if not t:
                raise ValueError("Title must not be empty")
            updates.append("title = ?")
            values.append(t)
        if genre is not None:
            updates.append("genre = ?")
            values.append(_normalize_genre(genre))
        if status is not None:
            updates.append("status = ?")
            values.append(_normalize_status(category, status))
        if rating is not None:
            updates.append("rating = ?")
            values.append(_normalize_rating(rating))
        if notes is not None:
            updates.append("notes = ?")
            values.append(notes)
        if date_added is not None:
            updates.append("date_added = ?")
            values.append(_normalize_date(date_added))
        if category == "movies" and type is not None:
            updates.append("type = ?")
            values.append(_normalize_type(type))
        if category == "books" and author is not None:
            updates.append("author = ?")
            values.append(_normalize_author(author))

        if not updates:
            return {"status": "success", "message": "No fields to update"}

        values.append(id)
        result = await _execute(
            f"UPDATE {table} SET {', '.join(updates)} WHERE id = ?",
            tuple(values),
        )
        if result.get("affected_row_count", 0) == 0:
            return {"status": "error", "message": "Entry not found"}
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@mcp.tool("delete_entry")
async def delete_entry(category: str, id: int) -> Dict[str, Any]:
    try:
        table = _get_table(category.strip().lower())
        result = await _execute(f"DELETE FROM {table} WHERE id = ?", (id,))
        if result.get("affected_row_count", 0) == 0:
            return {"status": "error", "message": "Entry not found"}
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@mcp.tool("search_entries")
async def search_entries(category: str, query: str) -> Any:
    try:
        table = _get_table(category.strip().lower())
        return await _fetch_rows(
            f"SELECT * FROM {table} WHERE LOWER(title) LIKE ? ORDER BY date_added DESC",
            (f"%{query.strip().lower()}%",),
        )
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@mcp.tool("get_all")
async def get_all(
    category: str,
    status: Optional[str] = None,
    genre: Optional[str] = None,
    min_rating: Optional[float] = None,
) -> Any:
    try:
        cat = category.strip().lower()
        table = _get_table(cat)
        filters: List[str] = []
        values: List[Any] = []

        if status is not None:
            filters.append("status = ?")
            values.append(_normalize_status(cat, status))
        if genre is not None:
            filters.append("genre = ?")
            values.append(_normalize_genre(genre))
        if min_rating is not None:
            filters.append("rating >= ?")
            values.append(_normalize_rating(min_rating))

        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        return await _fetch_rows(
            f"SELECT * FROM {table} {where} ORDER BY date_added DESC",
            tuple(values),
        )
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


async def _stats_for_category(category: str, period: str) -> Dict[str, Any]:
    table = _get_table(category)
    filter_clause, filter_params = _period_filter(period)
    aw = "AND" if filter_clause else "WHERE"

    total_rows = await _fetch_rows(f"SELECT COUNT(*) as cnt FROM {table} {filter_clause}", filter_params)
    total = total_rows[0]["cnt"]

    status_counts = {}
    for s in CATEGORY_SETTINGS[category]["status_values"]:
        rows = await _fetch_rows(
            f"SELECT COUNT(*) as cnt FROM {table} {filter_clause} {aw} status = ?",
            (*filter_params, s),
        )
        status_counts[s] = rows[0]["cnt"]

    avg_rows = await _fetch_rows(
        f"SELECT AVG(rating) as avg FROM {table} {filter_clause} {aw} rating IS NOT NULL",
        filter_params,
    )
    avg_rating = avg_rows[0]["avg"]
    avg_rating = float(avg_rating) if avg_rating is not None else None

    breakdown_rows = await _fetch_rows(
        f"SELECT genre, COUNT(*) as cnt FROM {table} {filter_clause} GROUP BY genre ORDER BY cnt DESC",
        filter_params,
    )
    genre_breakdown = {r["genre"]: r["cnt"] for r in breakdown_rows}

    top_rows = await _fetch_rows(
        f"SELECT * FROM {table} {filter_clause} {aw} rating IS NOT NULL ORDER BY rating DESC, date_added DESC LIMIT 1",
        filter_params,
    )
    top_rated = top_rows[0] if top_rows else None

    in_progress = sum(
        status_counts[s] for s in CATEGORY_SETTINGS[category]["in_progress"] if s in status_counts
    )

    return {
        "total": total,
        "completed": status_counts.get("completed", 0),
        "dropped": status_counts.get("dropped", 0),
        "in_progress": in_progress,
        "average_rating": avg_rating,
        "breakdown_by_genre": genre_breakdown,
        "top_rated_entry": top_rated,
    }


@mcp.tool("get_stats")
async def get_stats(category: str, period: str) -> Any:
    try:
        cat = category.strip().lower()
        if cat not in CATEGORY_SETTINGS:
            raise ValueError(f"Invalid category: {category}")
        return await _stats_for_category(cat, period)
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


async def _most_common_genre(category: str, period: str) -> Optional[str]:
    table = _get_table(category)
    filter_clause, filter_params = _period_filter(period)
    rows = await _fetch_rows(
        f"SELECT genre, COUNT(*) as cnt FROM {table} {filter_clause} GROUP BY genre ORDER BY cnt DESC LIMIT 1",
        filter_params,
    )
    return rows[0]["genre"] if rows else None


async def _overall_avg_rating(period: str) -> Optional[float]:
    filter_clause, filter_params = _period_filter(period)
    aw = "AND" if filter_clause else "WHERE"
    all_ratings = []
    for table in ("movies", "games", "books"):
        rows = await _fetch_rows(
            f"SELECT rating FROM {table} {filter_clause} {aw} rating IS NOT NULL",
            filter_params,
        )
        all_ratings.extend([float(r["rating"]) for r in rows])
    if not all_ratings:
        return None
    return float(sum(all_ratings) / len(all_ratings))


async def _count_completed(category: str, period: str) -> int:
    table = _get_table(category)
    filter_clause, filter_params = _period_filter(period)
    aw = "AND" if filter_clause else "WHERE"
    rows = await _fetch_rows(
        f"SELECT COUNT(*) as cnt FROM {table} {filter_clause} {aw} status = 'completed'",
        filter_params,
    )
    return int(rows[0]["cnt"])


@mcp.tool("generate_wrapped")
async def generate_wrapped(period: str) -> Any:
    try:
        categories = ["movies", "games", "books"]
        category_stats = {}
        total_consumed = 0
        counts: Dict[str, int] = {}

        for category in categories:
            stats = await _stats_for_category(category, period)
            category_stats[category] = {
                "total": stats["total"],
                "completed": stats["completed"],
                "average_rating": stats["average_rating"],
                "top_rated_entry": stats["top_rated_entry"],
                "most_common_genre": await _most_common_genre(category, period),
            }
            counts[category] = stats["total"]
            total_consumed += await _count_completed(category, period)

        most_active = max(counts, key=counts.get) if counts else None
        overall_avg = await _overall_avg_rating(period)

        return {
            "period": period,
            "most_active_category": most_active,
            "overall_average_rating": overall_avg,
            "total_items_consumed": total_consumed,
            "categories": category_stats,
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)