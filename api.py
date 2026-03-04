#!/usr/bin/env python3
"""
CampaignWe FastAPI — lightweight REST API over Parquet files.

Reads output/events_anonymized.parquet and output/story_metadata.parquet
using DuckDB in-memory (no persistent .db file needed, no file-locking issues).

Start:
    python api.py                     # default: port 8001
    python api.py --port 9000         # custom port

Endpoints:
    GET /                             # API info + available endpoints
    GET /events                       # all events (filters via query params)
    GET /events/csv                   # same, as CSV download
    GET /stories                      # story metadata
    GET /stats/daily                  # daily aggregates
    GET /stats/actions                # action type breakdown
    GET /stats/stories                # per-story metrics
    GET /health                       # health check

Query parameters for /events:
    date_from, date_to   — filter by session_date (YYYY-MM-DD)
    action_type          — filter by action_type (Read, Like, Submit, …)
    story_id             — filter by story_id
    limit                — max rows (default 10000)
    offset               — pagination offset (default 0)
"""

import argparse
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import duckdb
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

SCRIPT_DIR = Path(__file__).resolve().parent
EVENTS_PARQUET = SCRIPT_DIR / "output" / "events_anonymized.parquet"
STORIES_PARQUET = SCRIPT_DIR / "output" / "story_metadata.parquet"


def _check_parquet(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def _query(sql: str, params: Optional[List] = None) -> List[Dict]:
    """Run a read-only DuckDB query and return list of dicts."""
    con = duckdb.connect()
    try:
        result = con.execute(sql, params or []).fetchdf()
        # Convert timestamps to ISO strings for JSON serialisation
        for col in result.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            result[col] = result[col].astype(str)
        return result.to_dict(orient="records")
    finally:
        con.close()


def _query_csv(sql: str, params: Optional[List] = None) -> str:
    """Run query and return CSV string."""
    con = duckdb.connect()
    try:
        result = con.execute(sql, params or []).fetchdf()
        return result.to_csv(index=False)
    finally:
        con.close()


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: verify parquet files exist."""
    missing = []
    if not _check_parquet(EVENTS_PARQUET):
        missing.append(str(EVENTS_PARQUET))
    if not _check_parquet(STORIES_PARQUET):
        missing.append(str(STORIES_PARQUET))
    if missing:
        print(f"WARNING: Parquet files not found: {', '.join(missing)}")
        print("  Run process_campaignwe.py and fetch_story_metadata.py first.")
    else:
        row_count = _query(f"SELECT count(*) as n FROM read_parquet('{EVENTS_PARQUET}')")[0]["n"]
        print(f"Ready — {row_count:,} events loaded from parquet")
    yield


app = FastAPI(
    title="CampaignWe API",
    description="REST API for CampaignWe click-event analytics",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    events_ok = _check_parquet(EVENTS_PARQUET)
    stories_ok = _check_parquet(STORIES_PARQUET)
    return {
        "api": "CampaignWe",
        "version": "1.0.0",
        "data": {
            "events_parquet": "ok" if events_ok else "missing",
            "stories_parquet": "ok" if stories_ok else "missing",
        },
        "endpoints": [
            "GET /events          — event data (JSON)",
            "GET /events/csv      — event data (CSV download)",
            "GET /stories         — story metadata",
            "GET /stats/daily     — daily aggregates",
            "GET /stats/actions   — action type breakdown",
            "GET /stats/stories   — per-story metrics",
            "GET /health          — health check",
        ],
    }


@app.get("/health")
def health():
    return {"status": "ok", "events": _check_parquet(EVENTS_PARQUET), "stories": _check_parquet(STORIES_PARQUET)}


def _build_events_query(
    date_from: Optional[str],
    date_to: Optional[str],
    action_type: Optional[str],
    story_id: Optional[str],
    limit: int,
    offset: int,
) -> Tuple[str, List]:
    """Build parameterised SQL for events queries."""
    if not _check_parquet(EVENTS_PARQUET):
        raise HTTPException(status_code=503, detail="events_anonymized.parquet not found. Run process_campaignwe.py first.")

    conditions = []
    params = []

    if date_from:
        conditions.append("session_date >= ?::DATE")
        params.append(date_from)
    if date_to:
        conditions.append("session_date <= ?::DATE")
        params.append(date_to)
    if action_type:
        conditions.append("action_type = ?")
        params.append(action_type)
    if story_id:
        conditions.append("CAST(story_id AS VARCHAR) = ?")
        params.append(story_id)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"SELECT * FROM read_parquet('{EVENTS_PARQUET}'){where} ORDER BY session_date DESC, timestamp_cet DESC LIMIT {limit} OFFSET {offset}"
    return sql, params


@app.get("/events")
def get_events(
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    action_type: Optional[str] = Query(None, description="Filter by action_type"),
    story_id: Optional[str] = Query(None, description="Filter by story_id"),
    limit: int = Query(10000, ge=1, le=100000),
    offset: int = Query(0, ge=0),
):
    sql, params = _build_events_query(date_from, date_to, action_type, story_id, limit, offset)
    rows = _query(sql, params)
    return {"count": len(rows), "limit": limit, "offset": offset, "data": rows}


@app.get("/events/csv")
def get_events_csv(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    action_type: Optional[str] = Query(None),
    story_id: Optional[str] = Query(None),
    limit: int = Query(100000, ge=1, le=500000),
    offset: int = Query(0, ge=0),
):
    sql, params = _build_events_query(date_from, date_to, action_type, story_id, limit, offset)
    csv_data = _query_csv(sql, params)
    return StreamingResponse(
        iter([csv_data]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=events.csv"},
    )


@app.get("/stories")
def get_stories():
    if not _check_parquet(STORIES_PARQUET):
        raise HTTPException(status_code=503, detail="story_metadata.parquet not found. Run fetch_story_metadata.py first.")
    rows = _query(f"SELECT * FROM read_parquet('{STORIES_PARQUET}')")
    return {"count": len(rows), "data": rows}


@app.get("/stats/daily")
def stats_daily(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    if not _check_parquet(EVENTS_PARQUET):
        raise HTTPException(status_code=503, detail="events_anonymized.parquet not found.")

    conditions = []
    params = []
    if date_from:
        conditions.append("session_date >= ?::DATE")
        params.append(date_from)
    if date_to:
        conditions.append("session_date <= ?::DATE")
        params.append(date_to)
    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    sql = f"""
        SELECT
            session_date,
            COUNT(*) as total_events,
            COUNT(DISTINCT person_hash) as unique_visitors,
            COUNT(DISTINCT session_key) as sessions,
            COUNT(CASE WHEN action_type = 'Read' THEN 1 END) as views,
            COUNT(CASE WHEN action_type = 'Like' THEN 1 END) as likes,
            COUNT(CASE WHEN action_type = 'Submit' THEN 1 END) as submits
        FROM read_parquet('{EVENTS_PARQUET}'){where}
        GROUP BY session_date
        ORDER BY session_date DESC
    """
    return {"data": _query(sql, params)}


@app.get("/stats/actions")
def stats_actions():
    if not _check_parquet(EVENTS_PARQUET):
        raise HTTPException(status_code=503, detail="events_anonymized.parquet not found.")

    sql = f"""
        SELECT
            action_type,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as pct
        FROM read_parquet('{EVENTS_PARQUET}')
        GROUP BY action_type
        ORDER BY count DESC
    """
    return {"data": _query(sql)}


@app.get("/stats/stories")
def stats_stories():
    if not _check_parquet(EVENTS_PARQUET):
        raise HTTPException(status_code=503, detail="events_anonymized.parquet not found.")

    sql = f"""
        SELECT
            story_id,
            COUNT(*) as total_events,
            COUNT(CASE WHEN action_type = 'Read' THEN 1 END) as views,
            COUNT(DISTINCT person_hash) as unique_visitors,
            COUNT(CASE WHEN action_type = 'Like' THEN 1 END) as likes,
            COUNT(CASE WHEN action_type = 'Open Form' THEN 1 END) as open_forms,
            COUNT(CASE WHEN action_type = 'Submit' THEN 1 END) as submits
        FROM read_parquet('{EVENTS_PARQUET}')
        WHERE story_id IS NOT NULL
        GROUP BY story_id
        ORDER BY views DESC
    """
    return {"data": _query(sql)}


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    parser = argparse.ArgumentParser(description="CampaignWe API")
    parser.add_argument("--port", type=int, default=8001, help="Port (default: 8001)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host (default: 0.0.0.0)")
    args = parser.parse_args()

    print(f"Starting CampaignWe API on http://localhost:{args.port}")
    print(f"Docs: http://localhost:{args.port}/docs")
    uvicorn.run(app, host=args.host, port=args.port)
