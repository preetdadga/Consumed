import json
import os
from contextlib import asynccontextmanager
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from fastmcp import FastMCP
from libsql_client import Client, create_client

load_dotenv()
TURSO_URL = os.environ["TURSO_URL"]
TURSO_AUTH_TOKEN = os.environ["TURSO_AUTH_TOKEN"]


def _create_async_client() -> Client:
    return create_client(TURSO_URL, auth_token=TURSO_AUTH_TOKEN)


def _row_to_dict(columns: Tuple[str, ...], row: Any) -> Dict[str, Any]:
    return {columns[index]: row[index] for index in range(len(columns))}


def _get_table(category: str) -> str:
    if category not in CATEGORY_SETTINGS:
        raise ValueError(f"Invalid category: {category}")
    return CATEGORY_SETTINGS[category]["table"]


def _normalize_status(category: str, status: str) -> str:
    status_str = status.strip().lower()
    if status_str not in CATEGORY_SETTINGS[category]["status_values"]:
        raise ValueError(f"Invalid status for {category}: {status}")
    return status_str


def _normalize_genre(category: str, genre: str) -> str:
    genre_str = genre.strip()
    if not genre_str:
        raise ValueError("Genre must not be empty")
    return genre_str


def _normalize_type(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized not in {"film", "series"}:
        raise ValueError("Movie type must be 'film' or 'series'")
    return normalized


def _normalize_author(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    author = value.strip()
    if not author:
        raise ValueError("Author must not be empty")
    return author


def _normalize_rating(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    rating = float(value)
    if rating < 1 or rating > 10:
        raise ValueError("Rating must be between 1 and 10")
    return rating


def _normalize_date(date_added: Optional[str]) -> str:
    if not date_added:
        return date.today().isoformat()
    return date_added.strip()


def _get_period_start(period: str) -> Optional[str]:
    today = date.today()
    period = period.strip().lower()
    if period == "all":
        return None
    if period == "this_year":
        return date(today.year, 1, 1).isoformat()
    if period == "this_month":
        return date(today.year, today.month, 1).isoformat()
    if period == "this_quarter":
        quarter = (today.month - 1) // 3
        return date(today.year, quarter * 3 + 1, 1).isoformat()
    raise ValueError(f"Invalid period: {period}")


CATEGORY_SETTINGS = {
    "movies": {
        "table": "movies",
        "status_values": {"watching", "completed", "dropped"},
        "in_progress": {"watching"},
        "extra_columns": ["type"],
    },
    "games": {
        "table": "games",
        "status_values": {"playing", "completed", "dropped"},
        "in_progress": {"playing"},
        "extra_columns": [],
    },
    "books": {
        "table": "books",
        "status_values": {"reading", "completed", "dropped"},
        "in_progress": {"reading"},
        "extra_columns": ["author"],
    },
}


@asynccontextmanager
async def lifespan(server):
    async with _create_async_client() as client:
        await client.execute("""
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
        await client.execute("""
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
        await client.execute("""
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


def _load_genres(category: str) -> List[str]:
    genre_file = os.path.join(os.path.dirname(__file__), f"genres/{category}.json")
    with open(genre_file, "r") as f:
        return json.load(f)


@mcp.resource("consumed://genres/movies")
def genres_movies() -> List[str]:
    return _load_genres("movies")


@mcp.resource("consumed://genres/games")
def genres_games() -> List[str]:
    return _load_genres("games")


@mcp.resource("consumed://genres/books")
def genres_books() -> List[str]:
    return _load_genres("books")


async def _fetch_entries(query: str, params: Tuple[Any, ...]) -> List[Dict[str, Any]]:
    async with _create_async_client() as client:
        result = await client.execute(query, params)
        return [_row_to_dict(result.columns, row) for row in result.rows]


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
        table = _get_table(category)
        title_value = title.strip()
        genre_value = _normalize_genre(category, genre)
        status_value = _normalize_status(category, status)
        date_value = _normalize_date(date_added)
        rating_value = _normalize_rating(rating)
        notes_value = notes or ""

        if not title_value:
            raise ValueError("Title must not be empty")

        if category == "movies":
            type_value = _normalize_type(type)
            if type_value is None:
                raise ValueError("Movie type is required")
            query = (
                "INSERT INTO movies (title, type, genre, status, rating, date_added, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            params = (title_value, type_value, genre_value, status_value, rating_value, date_value, notes_value)
        elif category == "games":
            query = (
                "INSERT INTO games (title, genre, status, rating, date_added, notes) "
                "VALUES (?, ?, ?, ?, ?, ?)"
            )
            params = (title_value, genre_value, status_value, rating_value, date_value, notes_value)
        else:
            author_value = _normalize_author(author)
            if author_value is None:
                raise ValueError("Book author is required")
            query = (
                "INSERT INTO books (title, author, genre, status, rating, date_added, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            params = (title_value, author_value, genre_value, status_value, rating_value, date_value, notes_value)

        async with _create_async_client() as client:
            result = await client.execute(query, params)
            return {"status": "success", "id": result.last_insert_rowid}
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
            title_value = title.strip()
            if not title_value:
                raise ValueError("Title must not be empty")
            updates.append("title = ?")
            values.append(title_value)
        if genre is not None:
            values.append(_normalize_genre(category, genre))
            updates.append("genre = ?")
        if status is not None:
            values.append(_normalize_status(category, status))
            updates.append("status = ?")
        if rating is not None:
            values.append(_normalize_rating(rating))
            updates.append("rating = ?")
        if notes is not None:
            values.append(notes)
            updates.append("notes = ?")
        if date_added is not None:
            values.append(_normalize_date(date_added))
            updates.append("date_added = ?")
        if category == "movies" and type is not None:
            values.append(_normalize_type(type))
            updates.append("type = ?")
        if category == "books" and author is not None:
            values.append(_normalize_author(author))
            updates.append("author = ?")

        if not updates:
            return {"status": "success", "message": "No fields to update"}

        query = f"UPDATE {table} SET {', '.join(updates)} WHERE id = ?"
        values.append(id)

        async with _create_async_client() as client:
            result = await client.execute(query, tuple(values))
            if result.rows_affected == 0:
                return {"status": "error", "message": "Entry not found"}
            return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@mcp.tool("delete_entry")
async def delete_entry(category: str, id: int) -> Dict[str, Any]:
    try:
        table = _get_table(category.strip().lower())
        query = f"DELETE FROM {table} WHERE id = ?"
        async with _create_async_client() as client:
            result = await client.execute(query, (id,))
            if result.rows_affected == 0:
                return {"status": "error", "message": "Entry not found"}
            return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@mcp.tool("search_entries")
async def search_entries(category: str, query: str) -> Any:
    try:
        table = _get_table(category.strip().lower())
        sql = f"SELECT * FROM {table} WHERE LOWER(title) LIKE ? ORDER BY date_added DESC"
        return await _fetch_entries(sql, (f"%{query.strip().lower()}%",))
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
            values.append(_normalize_genre(cat, genre))
        if min_rating is not None:
            values.append(_normalize_rating(min_rating))
            filters.append("rating >= ?")

        where_clause = " AND ".join(filters)
        if where_clause:
            where_clause = "WHERE " + where_clause

        sql = f"SELECT * FROM {table} {where_clause} ORDER BY date_added DESC"
        return await _fetch_entries(sql, tuple(values))
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


async def _get_range_filter(period: str) -> Tuple[str, Tuple[Any, ...]]:
    start = _get_period_start(period)
    if start is None:
        return "", ()
    return "WHERE date_added >= ?", (start,)


async def _get_stats_for_category(category: str, period: str) -> Dict[str, Any]:
    table = _get_table(category)
    filter_clause, filter_params = await _get_range_filter(period)

    async with _create_async_client() as client:
        total_result = await client.execute(
            f"SELECT COUNT(*) FROM {table} {filter_clause}", filter_params
        )
        total = total_result.rows[0][0]

        status_counts = {}
        for status_name in CATEGORY_SETTINGS[category]["status_values"]:
            and_or_where = "AND" if filter_clause else "WHERE"
            result = await client.execute(
                f"SELECT COUNT(*) FROM {table} {filter_clause} {and_or_where} status = ?",
                (*filter_params, status_name),
            )
            status_counts[status_name] = result.rows[0][0]

        and_or_where = "AND" if filter_clause else "WHERE"
        rating_result = await client.execute(
            f"SELECT AVG(rating) FROM {table} {filter_clause} {and_or_where} rating IS NOT NULL",
            filter_params,
        )
        avg_rating = rating_result.rows[0][0]
        avg_rating = float(avg_rating) if avg_rating is not None else None

        breakdown_result = await client.execute(
            f"SELECT genre, COUNT(*) FROM {table} {filter_clause} GROUP BY genre ORDER BY COUNT(*) DESC",
            filter_params,
        )
        genre_breakdown = {row[0]: row[1] for row in breakdown_result.rows}

        top_result = await client.execute(
            f"SELECT * FROM {table} {filter_clause} {and_or_where} rating IS NOT NULL ORDER BY rating DESC, date_added DESC LIMIT 1",
            filter_params,
        )
        top_rated = _row_to_dict(top_result.columns, top_result.rows[0]) if top_result.rows else None

        in_progress_count = sum(
            status_counts[s] for s in CATEGORY_SETTINGS[category]["in_progress"] if s in status_counts
        )

        return {
            "total": total,
            "completed": status_counts.get("completed", 0),
            "dropped": status_counts.get("dropped", 0),
            "in_progress": in_progress_count,
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
        return await _get_stats_for_category(cat, period)
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


async def _get_most_common_genre(category: str, period: str) -> Optional[str]:
    table = _get_table(category)
    filter_clause, filter_params = await _get_range_filter(period)
    async with _create_async_client() as client:
        result = await client.execute(
            f"SELECT genre, COUNT(*) FROM {table} {filter_clause} GROUP BY genre ORDER BY COUNT(*) DESC LIMIT 1",
            filter_params,
        )
        return result.rows[0][0] if result.rows else None


async def _get_overall_average_rating(period: str) -> Optional[float]:
    filter_clause, filter_params = await _get_range_filter(period)
    and_or_where = "AND" if filter_clause else "WHERE"
    async with _create_async_client() as client:
        ratings: List[float] = []
        for table in ("movies", "games", "books"):
            sql = f"SELECT rating FROM {table} {filter_clause} {and_or_where} rating IS NOT NULL"
            result = await client.execute(sql, filter_params)
            ratings.extend([row[0] for row in result.rows])
    if not ratings:
        return None
    return float(sum(ratings) / len(ratings))


async def _count_completed_items(category: str, period: str) -> int:
    table = _get_table(category)
    filter_clause, filter_params = await _get_range_filter(period)
    and_or_where = "AND" if filter_clause else "WHERE"
    sql = f"SELECT COUNT(*) FROM {table} {filter_clause} {and_or_where} status = 'completed'"
    async with _create_async_client() as client:
        result = await client.execute(sql, filter_params)
        return int(result.rows[0][0])


@mcp.tool("generate_wrapped")
async def generate_wrapped(period: str) -> Any:
    try:
        categories = ["movies", "games", "books"]
        category_stats = {}
        total_consumed = 0
        counts = {}

        for category in categories:
            stats = await _get_stats_for_category(category, period)
            category_stats[category] = {
                "total": stats["total"],
                "completed": stats["completed"],
                "average_rating": stats["average_rating"],
                "top_rated_entry": stats["top_rated_entry"],
                "most_common_genre": await _get_most_common_genre(category, period),
            }
            counts[category] = stats["total"]
            total_consumed += await _count_completed_items(category, period)

        most_active = max(counts, key=counts.get) if counts else None
        overall_avg_rating = await _get_overall_average_rating(period)

        return {
            "period": period,
            "most_active_category": most_active,
            "overall_average_rating": overall_avg_rating,
            "total_items_consumed": total_consumed,
            "categories": category_stats,
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)