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
        return {"type": "float", "value": str(value)}  
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
    total = int(total_rows[0]["cnt"])

    status_counts = {}
    for s in CATEGORY_SETTINGS[category]["status_values"]:
        rows = await _fetch_rows(
            f"SELECT COUNT(*) as cnt FROM {table} {filter_clause} {aw} status = ?",
            (*filter_params, s),
        )
        status_counts[s] = int(rows[0]["cnt"])

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
    genre_breakdown = {r["genre"]: int(r["cnt"]) for r in breakdown_rows}

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


@mcp.tool(
    "generate_wrapped",
    description="""Fetches all consumption data for a wrapped report.

IMPORTANT — after calling this tool, you MUST:
1. Study the raw_entries and stats carefully
2. Generate 4-5 deeply personal insights about the user's taste and patterns.
   Insights should feel like observations from someone who studied the user —
   not generic stats. Cross-reference data points. Examples of the right tone:
   - 'You dropped Dark after S2 but still gave it 8.5. You respected it even when it lost you.'
   - 'You gave Nolan 5 chances and he never disappointed you once. That's not a favourite director, that's trust.'
   - 'Every book you picked up, you finished. No drops, no pauses. That's rare.'
   Be specific, use actual titles, ratings, and notes from the data.
3. Then call generate_wrapped_html with the stats AND your generated insights list.
   Do not show the raw data to the user — go straight to generate_wrapped_html."""
)
async def generate_wrapped(period: str) -> Any:
    try:
        categories = ["movies", "games", "books"]
        category_stats = {}
        total_consumed = 0
        counts: Dict[str, int] = {}
        raw_entries: Dict[str, List] = {}

        for category in categories:
            stats = await _stats_for_category(category, period)
            category_stats[category] = {
                "total": stats["total"],
                "completed": stats["completed"],
                "dropped": stats["dropped"],
                "in_progress": stats["in_progress"],
                "average_rating": stats["average_rating"],
                "top_rated_entry": stats["top_rated_entry"],
                "most_common_genre": await _most_common_genre(category, period),
                "breakdown_by_genre": stats["breakdown_by_genre"],
            }
            counts[category] = stats["total"]
            total_consumed += await _count_completed(category, period)

            table = _get_table(category)
            filter_clause, filter_params = _period_filter(period)
            raw_entries[category] = await _fetch_rows(
                f"SELECT * FROM {table} {filter_clause} ORDER BY date_added DESC",
                filter_params,
            )

        most_active = max(counts, key=counts.get) if counts else None
        overall_avg = await _overall_avg_rating(period)

        return {
            "period": period,
            "most_active_category": most_active,
            "overall_average_rating": overall_avg,
            "total_items_consumed": total_consumed,
            "categories": category_stats,
            "raw_entries": raw_entries,
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@mcp.tool(
    "generate_wrapped_html",
    description="Renders the wrapped report as a beautiful HTML page. Call this after generate_wrapped and after generating personal insights. Pass the full stats dict and the insights list you generated."
)
async def generate_wrapped_html(
    period: str,
    stats: Dict[str, Any],
    insights: List[str],
    username: str = "preet dadga",
) -> Dict[str, Any]:
    try:
        cats = stats.get("categories", {})
        movies = cats.get("movies", {})
        games = cats.get("games", {})
        books = cats.get("books", {})

        total_entries = (movies.get("total", 0) or 0) + (games.get("total", 0) or 0) + (books.get("total", 0) or 0)
        total_completed = stats.get("total_items_consumed", 0) or 0
        overall_avg = stats.get("overall_average_rating")
        overall_avg_str = f"{overall_avg:.1f}" if overall_avg else "—"
        most_active = stats.get("most_active_category", "—") or "—"

        def top_title(cat_data):
            t = cat_data.get("top_rated_entry")
            if not t:
                return "—"
            return t.get("title", "—")

        def top_rating(cat_data):
            t = cat_data.get("top_rated_entry")
            if not t or t.get("rating") is None:
                return ""
            return f"{t['rating']}/10"

        def avg_str(cat_data):
            a = cat_data.get("average_rating")
            return f"{a:.1f}" if a else "—"

        def genre_bars(cat_data, color):
            breakdown = cat_data.get("breakdown_by_genre", {})
            if not breakdown:
                return ""
            max_cnt = max(breakdown.values()) if breakdown else 1
            bars = ""
            for genre, cnt in list(breakdown.items())[:4]:
                pct = int((cnt / max_cnt) * 100)
                bars += f"""
                <div style="margin-bottom:10px">
                  <div style="display:flex;justify-content:space-between;font-size:12px;color:#555;margin-bottom:4px">
                    <span style="color:#888">{genre}</span>
                    <span style="color:{color}">{cnt}</span>
                  </div>
                  <div style="height:3px;background:#1e1e1e;border-radius:2px">
                    <div style="width:{pct}%;height:100%;background:{color};border-radius:2px"></div>
                  </div>
                </div>"""
            return bars

        insights_html = ""
        for insight in insights:
            insights_html += f"""
            <div style="padding:1.25rem 0;border-bottom:1px solid #161616">
              <p style="font-size:15px;color:#ccc;line-height:1.6;margin:0">{insight}</p>
            </div>"""

        period_label = {
            "all": "all time",
            "this_year": "this year",
            "this_month": "this month",
            "this_quarter": "this quarter",
        }.get(period.lower(), period)

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Consumed — Wrapped</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{background:#0a0a0a;color:#f0f0f0;font-family:'Inter',sans-serif;min-height:100vh}}
  .page{{max-width:680px;margin:0 auto;padding:2rem 1.5rem 5rem}}
  .header{{text-align:center;padding:4rem 0 2.5rem}}
  .eyebrow{{font-size:10px;letter-spacing:0.25em;text-transform:uppercase;color:#444;margin-bottom:1rem}}
  .logo{{font-size:3.5rem;font-weight:700;letter-spacing:-0.04em;color:#fff;line-height:1}}
  .logo span{{color:#c9a96e}}
  .header-sub{{font-size:13px;color:#444;margin-top:0.75rem;letter-spacing:0.05em}}
  .divider{{height:1px;background:linear-gradient(90deg,transparent,#1e1e1e,transparent);margin:2.5rem 0}}
  .section-label{{font-size:10px;letter-spacing:0.2em;text-transform:uppercase;color:#333;margin-bottom:1rem}}
  .stat-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;background:#151515;border-radius:12px;overflow:hidden;margin-bottom:1px}}
  .stat-cell{{background:#0f0f0f;padding:1.5rem 1rem;text-align:center}}
  .stat-num{{font-size:2.25rem;font-weight:700;letter-spacing:-0.04em;line-height:1}}
  .stat-lbl{{font-size:10px;color:#333;margin-top:6px;text-transform:uppercase;letter-spacing:0.15em}}
  .highlight-grid{{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#151515;border-radius:12px;overflow:hidden}}
  .hcard{{background:#0f0f0f;padding:1.25rem;position:relative;overflow:hidden}}
  .hcard::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px}}
  .hcard-tag{{font-size:9px;letter-spacing:0.15em;text-transform:uppercase;color:#333;margin-bottom:0.5rem}}
  .hcard-title{{font-size:14px;font-weight:600;color:#ddd;line-height:1.3;margin-bottom:0.25rem}}
  .hcard-sub{{font-size:12px;color:#444;line-height:1.4}}
  .wide-card{{background:#0f0f0f;border-radius:12px;padding:1.5rem;position:relative;overflow:hidden}}
  .wide-card::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:#c9a96e}}
  .cat-grid{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:1px;background:#151515;border-radius:12px;overflow:hidden;margin-bottom:1px}}
  .cat-cell{{background:#0f0f0f;padding:1.25rem 1rem}}
  .cat-label{{font-size:9px;letter-spacing:0.18em;text-transform:uppercase;color:#333;margin-bottom:0.75rem}}
  .cat-top{{font-size:13px;font-weight:600;color:#ddd;margin-bottom:0.25rem;line-height:1.3}}
  .cat-rating{{font-size:11px;margin-bottom:0.25rem}}
  .cat-avg{{font-size:11px;color:#444}}
  .insights-section{{background:#0f0f0f;border-radius:12px;overflow:hidden;padding:0 1.5rem}}
  .insights-section .hcard-tag{{padding-top:1.5rem}}
  .insight-item:last-child{{border-bottom:none!important}}
  .currently-list{{background:#151515;border-radius:12px;overflow:hidden}}
  .currently-item{{background:#0f0f0f;padding:1rem 1.25rem;display:flex;align-items:center;gap:1rem;border-bottom:1px solid #151515}}
  .currently-item:last-child{{border-bottom:none}}
  .dot{{width:7px;height:7px;border-radius:50%;flex-shrink:0}}
  .footer{{text-align:center;margin-top:4rem}}
  .footer-text{{font-size:10px;color:#222;letter-spacing:0.15em;text-transform:uppercase}}
  @keyframes fadeUp{{from{{opacity:0;transform:translateY(16px)}}to{{opacity:1;transform:translateY(0)}}}}
  .fade{{opacity:0;animation:fadeUp 0.6s ease forwards}}
  .d1{{animation-delay:0.05s}}.d2{{animation-delay:0.15s}}.d3{{animation-delay:0.25s}}
  .d4{{animation-delay:0.35s}}.d5{{animation-delay:0.45s}}.d6{{animation-delay:0.55s}}
  .d7{{animation-delay:0.65s}}.d8{{animation-delay:0.75s}}.d9{{animation-delay:0.85s}}
</style>
</head>
<body>
<div class="page">

  <div class="header fade d1">
    <div class="eyebrow">your {period_label} in review</div>
    <div class="logo">con<span>sumed</span></div>
    <div class="header-sub">{username} &nbsp;·&nbsp; {period_label} &nbsp;·&nbsp; {total_entries} entries logged</div>
  </div>

  <div class="divider"></div>

  <div class="section-label fade d2">overview</div>
  <div class="stat-grid fade d2">
    <div class="stat-cell">
      <div class="stat-num" style="color:#c9a96e">{movies.get('total', 0)}</div>
      <div class="stat-lbl">films &amp; series</div>
    </div>
    <div class="stat-cell">
      <div class="stat-num" style="color:#6ec9c9">{games.get('total', 0)}</div>
      <div class="stat-lbl">games</div>
    </div>
    <div class="stat-cell">
      <div class="stat-num" style="color:#c96e8a">{books.get('total', 0)}</div>
      <div class="stat-lbl">books</div>
    </div>
  </div>
  <div class="stat-grid fade d3" style="margin-top:1px">
    <div class="stat-cell">
      <div class="stat-num" style="color:#fff">{total_completed}</div>
      <div class="stat-lbl">completed</div>
    </div>
    <div class="stat-cell">
      <div class="stat-num" style="color:#fff">{overall_avg_str}</div>
      <div class="stat-lbl">avg rating</div>
    </div>
    <div class="stat-cell">
      <div class="stat-num" style="color:#fff;font-size:1.5rem">{most_active}</div>
      <div class="stat-lbl">most active</div>
    </div>
  </div>

  <div class="divider"></div>

  <div class="section-label fade d4">top picks</div>
  <div class="cat-grid fade d4">
    <div class="cat-cell">
      <div class="cat-label" style="color:#c9a96e44;color:#555">🎬 film & series</div>
      <div class="cat-top">{top_title(movies)}</div>
      <div class="cat-rating" style="color:#c9a96e">{top_rating(movies)}</div>
      <div class="cat-avg">avg {avg_str(movies)}</div>
    </div>
    <div class="cat-cell">
      <div class="cat-label" style="color:#555">🎮 games</div>
      <div class="cat-top">{top_title(games)}</div>
      <div class="cat-rating" style="color:#6ec9c9">{top_rating(games)}</div>
      <div class="cat-avg">avg {avg_str(games)}</div>
    </div>
    <div class="cat-cell">
      <div class="cat-label" style="color:#555">📚 books</div>
      <div class="cat-top">{top_title(books)}</div>
      <div class="cat-rating" style="color:#c96e8a">{top_rating(books)}</div>
      <div class="cat-avg">avg {avg_str(books)}</div>
    </div>
  </div>

  <div class="divider"></div>

  <div class="section-label fade d5">your taste</div>
  <div class="wide-card fade d5" style="margin-bottom:1px">
    <div style="font-size:11px;color:#444;margin-bottom:1rem;letter-spacing:0.1em;text-transform:uppercase">films &amp; series</div>
    {genre_bars(movies, '#c9a96e')}
  </div>
  <div class="wide-card fade d5" style="margin-bottom:1px;--accent:#6ec9c9">
    <div style="font-size:11px;color:#444;margin-bottom:1rem;letter-spacing:0.1em;text-transform:uppercase">games</div>
    {genre_bars(games, '#6ec9c9')}
  </div>
  <div class="wide-card fade d6" style="--accent:#c96e8a">
    <div style="font-size:11px;color:#444;margin-bottom:1rem;letter-spacing:0.1em;text-transform:uppercase">books</div>
    {genre_bars(books, '#c96e8a')}
  </div>

  <div class="divider"></div>

  <div class="section-label fade d7">what consumed says about you</div>
  <div class="insights-section fade d7">
    <div class="hcard-tag" style="padding-top:1.5rem;padding-bottom:0.5rem;color:#333">observations</div>
    {insights_html}
    <div style="height:1.5rem"></div>
  </div>

  <div class="divider"></div>

  <div class="footer fade d9">
    <div class="footer-text">consumed &nbsp;·&nbsp; {username} &nbsp;·&nbsp; {period_label} &nbsp;·&nbsp; {total_entries} entries</div>
  </div>

</div>
<script>
document.querySelectorAll('.counter').forEach(el => {{
  const target = parseInt(el.dataset.target);
  const dur = 1000;
  const start = performance.now();
  function tick(now) {{
    const p = Math.min((now - start) / dur, 1);
    const e = 1 - Math.pow(1 - p, 3);
    el.textContent = Math.round(e * target);
    if (p < 1) requestAnimationFrame(tick);
  }}
  requestAnimationFrame(tick);
}});
</script>
</body>
</html>"""

        return {"html": html, "filename": f"consumed-wrapped-{period}.html"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)
