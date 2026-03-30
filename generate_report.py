#!/usr/bin/env python3
"""
CampaignWe XLSX Report Generator

Generates a formatted, multi-tab Excel report from the processed Parquet files
to showcase campaign success metrics.

Usage:
    python generate_report.py                          # Default: read from output/, write to output/
    python generate_report.py --output report.xlsx     # Custom output path
    python generate_report.py --date-from 2026-01-01   # Filter by date range
    python generate_report.py --date-to 2026-03-31

Input:
    - output/events_anonymized.parquet
    - output/story_metadata.parquet

Output:
    - output/campaignwe_report.xlsx (8 tabs)
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb
from openpyxl import Workbook
from openpyxl.formatting.rule import ColorScaleRule, DataBarRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side, numbers
from openpyxl.utils import get_column_letter

# ---------------------------------------------------------------------------
# Brand colors
# ---------------------------------------------------------------------------
CORP_RED = "E60000"
BORDEAUX_I = "BD000C"
LAKE_50 = "0C7EC6"
WHITE = "FFFFFF"
BLACK = "000000"
GRAY_I = "CCCABC"
GRAY_IV = "7A7870"
GRAY_VI = "404040"
PASTEL_I = "ECEBE4"
ROW_ALT = "F8F7F2"
BRONZE_I = "B98E2C"
RAG_GREEN = "6F7A1A"
RAG_AMBER = "E4A911"

# ---------------------------------------------------------------------------
# Reusable styles
# ---------------------------------------------------------------------------
HEADER_FONT = Font(bold=True, color=WHITE, size=11)
HEADER_FILL = PatternFill(start_color=GRAY_VI, end_color=GRAY_VI, fill_type="solid")
SECTION_FONT = Font(bold=True, color=GRAY_VI, size=11)
SECTION_FILL = PatternFill(start_color=PASTEL_I, end_color=PASTEL_I, fill_type="solid")
ALT_FILL = PatternFill(start_color=ROW_ALT, end_color=ROW_ALT, fill_type="solid")
TOTAL_FILL = PatternFill(start_color=PASTEL_I, end_color=PASTEL_I, fill_type="solid")
TOTAL_FONT = Font(bold=True, color=BLACK, size=11)
THIN_BORDER = Border(
    left=Side(style="thin", color=GRAY_I),
    right=Side(style="thin", color=GRAY_I),
    top=Side(style="thin", color=GRAY_I),
    bottom=Side(style="thin", color=GRAY_I),
)

NUM_FMT_INT = "#,##0"
NUM_FMT_PCT = "0.0%"
NUM_FMT_RATIO = "0.0"
NUM_FMT_DATE = "YYYY-MM-DD"


def log(msg=""):
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def write_header_row(ws, row, headers, col_start=1):
    """Write a styled header row."""
    for ci, h in enumerate(headers, start=col_start):
        cell = ws.cell(row=row, column=ci, value=h)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = THIN_BORDER


def write_data_rows(ws, start_row, rows, col_start=1, fmt_map=None):
    """Write data rows with alternating row color and number formats.

    fmt_map: dict mapping column-index (0-based within row) to an openpyxl number format string.
    """
    fmt_map = fmt_map or {}
    for ri, row_data in enumerate(rows):
        excel_row = start_row + ri
        fill = ALT_FILL if ri % 2 == 1 else None
        for ci, val in enumerate(row_data):
            cell = ws.cell(row=excel_row, column=col_start + ci, value=val)
            cell.border = THIN_BORDER
            if fill:
                cell.fill = fill
            if ci in fmt_map:
                cell.number_format = fmt_map[ci]
            elif isinstance(val, float):
                cell.number_format = NUM_FMT_RATIO
            elif isinstance(val, int):
                cell.number_format = NUM_FMT_INT


def write_section_header(ws, row, text, col_span=2):
    """Write a section header spanning multiple columns."""
    cell = ws.cell(row=row, column=1, value=text)
    cell.font = SECTION_FONT
    cell.fill = SECTION_FILL
    cell.border = THIN_BORDER
    for ci in range(2, col_span + 1):
        c = ws.cell(row=row, column=ci)
        c.fill = SECTION_FILL
        c.border = THIN_BORDER


def write_kpi_row(ws, row, label, value, col_label=1, col_value=2, fmt=None):
    """Write a KPI label-value pair."""
    lbl = ws.cell(row=row, column=col_label, value=label)
    lbl.font = Font(bold=True, color=GRAY_VI)
    lbl.border = THIN_BORDER
    lbl.alignment = Alignment(indent=1)

    val = ws.cell(row=row, column=col_value, value=value)
    val.border = THIN_BORDER
    val.alignment = Alignment(horizontal="right")
    if fmt:
        val.number_format = fmt
    elif isinstance(value, float):
        val.number_format = NUM_FMT_RATIO
    elif isinstance(value, int):
        val.number_format = NUM_FMT_INT


def write_total_row(ws, row, data, col_start=1, fmt_map=None):
    """Write a bold total row."""
    fmt_map = fmt_map or {}
    for ci, val in enumerate(data):
        cell = ws.cell(row=row, column=col_start + ci, value=val)
        cell.font = TOTAL_FONT
        cell.fill = TOTAL_FILL
        cell.border = THIN_BORDER
        if ci in fmt_map:
            cell.number_format = fmt_map[ci]
        elif isinstance(val, float):
            cell.number_format = NUM_FMT_RATIO
        elif isinstance(val, int):
            cell.number_format = NUM_FMT_INT


def auto_fit_columns(ws, min_width=10, max_width=40):
    """Auto-fit column widths based on content."""
    for col_cells in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col_cells[0].column)
        for cell in col_cells:
            if cell.value is not None:
                max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = min(max(max_len + 2, min_width), max_width)


def finalize_sheet(ws, freeze_row=2):
    """Freeze panes and finalize a sheet."""
    ws.freeze_panes = ws.cell(row=freeze_row, column=1)
    auto_fit_columns(ws)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_data(events_path, metadata_path, date_from=None, date_to=None):
    """Load parquet files into an in-memory DuckDB and return the connection."""
    con = duckdb.connect(":memory:")

    if not events_path.exists():
        log(f"ERROR: Events file not found: {events_path}")
        sys.exit(1)

    con.execute(f"CREATE TABLE events AS SELECT * FROM read_parquet('{events_path}')")
    log(f"  Loaded {con.execute('SELECT COUNT(*) FROM events').fetchone()[0]:,} events")

    # Check available columns
    cols = [r[0] for r in con.execute("DESCRIBE events").fetchall()]

    if metadata_path.exists():
        con.execute(f"CREATE TABLE story_meta AS SELECT * FROM read_parquet('{metadata_path}')")
        log(f"  Loaded {con.execute('SELECT COUNT(*) FROM story_meta').fetchone()[0]:,} stories")
    else:
        log(f"  WARNING: Story metadata not found: {metadata_path}")
        con.execute("CREATE TABLE story_meta (story_id VARCHAR, story_title VARCHAR, status VARCHAR)")

    # Apply date filters
    if date_from:
        con.execute(f"DELETE FROM events WHERE session_date < '{date_from}'")
        log(f"  Filtered: session_date >= {date_from}")
    if date_to:
        con.execute(f"DELETE FROM events WHERE session_date > '{date_to}'")
        log(f"  Filtered: session_date <= {date_to}")

    remaining = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    log(f"  {remaining:,} events after date filters")

    return con, cols


# ---------------------------------------------------------------------------
# Tab builders
# ---------------------------------------------------------------------------
def build_executive_summary(wb, con, cols):
    """Tab 1: Executive Summary — KPIs at a glance."""
    ws = wb.create_sheet("Executive Summary")
    ws.sheet_properties.tabColor = BRONZE_I

    kpis = con.execute("""
        SELECT
            MIN(session_date) as first_date,
            MAX(session_date) as last_date,
            DATEDIFF('day', MIN(session_date), MAX(session_date)) + 1 as duration_days,
            COUNT(*) as total_interactions,
            COUNT(DISTINCT person_hash) as unique_visitors,
            COUNT(DISTINCT session_key) as unique_sessions,
            COUNT(DISTINCT CASE WHEN story_id IS NOT NULL THEN story_id END) as total_stories,
            COUNT(CASE WHEN action_type = 'Read' THEN 1 END) as reads,
            COUNT(CASE WHEN action_type = 'Like' THEN 1 END) as likes,
            COUNT(CASE WHEN action_type = 'Submit' THEN 1 END) as submits,
            COUNT(CASE WHEN action_type = 'Open Form' THEN 1 END) as open_forms,
            COUNT(CASE WHEN action_type = 'Cancel' THEN 1 END) as cancels,
            COUNT(CASE WHEN action_type = 'Delete' THEN 1 END) as deletes,
            COUNT(CASE WHEN action_type = 'Send Invite' THEN 1 END) as invites_sent,
            COUNT(CASE WHEN action_type = 'Open Invite' THEN 1 END) as invites_opened
        FROM events
    """).fetchone()

    first_date, last_date, duration, total, uv, sessions, stories = kpis[:7]
    reads, likes, submits, open_forms, cancels, deletes, invites_sent, invites_opened = kpis[7:]

    # Story counts from metadata
    active_stories = con.execute("""
        SELECT COUNT(DISTINCT story_id) FROM story_meta WHERE status = 'active'
    """).fetchone()[0]
    deleted_stories = con.execute("""
        SELECT COUNT(DISTINCT story_id) FROM story_meta WHERE status = 'deleted'
    """).fetchone()[0]

    # Org coverage
    has_visitor_div = "visitor_division" in cols
    if has_visitor_div:
        org_covered = con.execute("""
            SELECT COUNT(*) FROM events WHERE visitor_division IS NOT NULL
        """).fetchone()[0]
        org_pct = org_covered / total if total > 0 else 0
    else:
        org_pct = None

    # --- Layout ---
    r = 1
    write_section_header(ws, r, "CAMPAIGN OVERVIEW", 2)
    r += 1
    write_kpi_row(ws, r, "Report Period", f"{first_date} to {last_date}"); r += 1
    write_kpi_row(ws, r, "Duration (days)", duration, fmt=NUM_FMT_INT); r += 1
    r += 1

    write_section_header(ws, r, "REACH", 2); r += 1
    write_kpi_row(ws, r, "Total Clicks", total, fmt=NUM_FMT_INT); r += 1
    write_kpi_row(ws, r, "Unique Visitors", uv, fmt=NUM_FMT_INT); r += 1
    write_kpi_row(ws, r, "Unique Sessions", sessions, fmt=NUM_FMT_INT); r += 1
    write_kpi_row(ws, r, "Avg. Clicks / Visitor",
                  round(total / uv, 1) if uv > 0 else 0); r += 1
    write_kpi_row(ws, r, "Avg. Daily Visitors",
                  round(uv / duration, 1) if duration > 0 else 0); r += 1
    if org_pct is not None:
        write_kpi_row(ws, r, "Org Data Coverage", org_pct, fmt=NUM_FMT_PCT); r += 1
    r += 1

    write_section_header(ws, r, "ENGAGEMENT", 2); r += 1
    write_kpi_row(ws, r, "Story Reads", reads, fmt=NUM_FMT_INT); r += 1
    write_kpi_row(ws, r, "Story Likes", likes, fmt=NUM_FMT_INT); r += 1
    write_kpi_row(ws, r, "Like Rate (Likes / Reads)",
                  likes / reads if reads > 0 else 0, fmt=NUM_FMT_PCT); r += 1
    write_kpi_row(ws, r, "Invites Sent", invites_sent, fmt=NUM_FMT_INT); r += 1
    write_kpi_row(ws, r, "Invites Opened", invites_opened, fmt=NUM_FMT_INT); r += 1
    r += 1

    write_section_header(ws, r, "CONTENT", 2); r += 1
    write_kpi_row(ws, r, "Total Stories (all time)", stories, fmt=NUM_FMT_INT); r += 1
    write_kpi_row(ws, r, "Active Stories", active_stories, fmt=NUM_FMT_INT); r += 1
    write_kpi_row(ws, r, "Deleted Stories", deleted_stories, fmt=NUM_FMT_INT); r += 1
    write_kpi_row(ws, r, "Avg. Reads / Active Story",
                  round(reads / active_stories, 1) if active_stories > 0 else 0); r += 1
    write_kpi_row(ws, r, "Avg. Likes / Active Story",
                  round(likes / active_stories, 1) if active_stories > 0 else 0); r += 1
    r += 1

    write_section_header(ws, r, "SUBMISSION FUNNEL", 2); r += 1
    write_kpi_row(ws, r, "Opened Form", open_forms, fmt=NUM_FMT_INT); r += 1
    write_kpi_row(ws, r, "Submitted", submits, fmt=NUM_FMT_INT); r += 1
    submit_rate = submits / open_forms if open_forms > 0 else 0
    write_kpi_row(ws, r, "Submission Rate (Submit / Open Form)", submit_rate, fmt=NUM_FMT_PCT); r += 1
    write_kpi_row(ws, r, "Cancelled", cancels, fmt=NUM_FMT_INT); r += 1
    write_kpi_row(ws, r, "Delete Confirmations (Clicks)", deletes, fmt=NUM_FMT_INT); r += 1

    ws.column_dimensions["A"].width = 38
    ws.column_dimensions["B"].width = 22
    ws.freeze_panes = ws.cell(row=2, column=1)
    log("  Tab 1: Executive Summary")


def build_weekly_trend(wb, con):
    """Tab 2: Weekly Trend — growth and momentum."""
    ws = wb.create_sheet("Weekly Trend")
    ws.sheet_properties.tabColor = GRAY_IV

    rows = con.execute("""
        WITH weekly AS (
            SELECT
                YEARWEEK(session_date) as yw,
                MIN(session_date) as week_start,
                COUNT(*) as clicks,
                COUNT(DISTINCT person_hash) as uv,
                COUNT(CASE WHEN action_type = 'Read' THEN 1 END) as reads,
                COUNT(CASE WHEN action_type = 'Like' THEN 1 END) as likes,
                COUNT(CASE WHEN action_type = 'Submit' THEN 1 END) as submits,
                COUNT(CASE WHEN action_type = 'Open Form' THEN 1 END) as open_forms,
                COUNT(CASE WHEN action_type = 'Cancel' THEN 1 END) as cancels,
                COUNT(CASE WHEN action_type = 'Send Invite' THEN 1 END) as invites_sent,
                COUNT(CASE WHEN action_type = 'Open Invite' THEN 1 END) as invites_opened,
                COUNT(CASE WHEN action_type = 'Delete' THEN 1 END) as deletes,
                COUNT(DISTINCT session_date) as active_days
            FROM events
            GROUP BY YEARWEEK(session_date)
        ),
        first_seen AS (
            SELECT person_hash, MIN(YEARWEEK(session_date)) as first_week
            FROM events
            GROUP BY person_hash
        ),
        new_per_week AS (
            SELECT first_week as yw, COUNT(*) as new_visitors
            FROM first_seen
            GROUP BY first_week
        )
        SELECT
            w.yw,
            w.week_start,
            w.clicks,
            w.uv,
            COALESCE(n.new_visitors, 0) as new_visitors,
            w.uv - COALESCE(n.new_visitors, 0) as returning_visitors,
            w.reads,
            w.likes,
            w.submits,
            w.open_forms,
            w.cancels,
            w.invites_sent,
            w.invites_opened,
            w.deletes,
            CASE WHEN w.reads > 0 THEN ROUND(w.likes * 1.0 / w.reads, 3) ELSE 0 END as like_rate,
            CASE WHEN w.active_days > 0 THEN ROUND(w.uv * 1.0 / w.active_days, 1) ELSE 0 END as avg_daily_uv
        FROM weekly w
        LEFT JOIN new_per_week n ON w.yw = n.yw
        ORDER BY w.yw
    """).fetchall()

    headers = [
        "Week", "Week Start", "Clicks", "Unique Visitors",
        "New Visitors", "Returning Visitors", "Reads", "Likes",
        "Submissions", "Open Form", "Cancel", "Invites Sent",
        "Invites Opened", "Deletes", "Like Rate", "Avg. Daily Visitors"
    ]
    fmt = {
        0: "0", 1: NUM_FMT_DATE, 2: NUM_FMT_INT, 3: NUM_FMT_INT,
        4: NUM_FMT_INT, 5: NUM_FMT_INT, 6: NUM_FMT_INT, 7: NUM_FMT_INT,
        8: NUM_FMT_INT, 9: NUM_FMT_INT, 10: NUM_FMT_INT, 11: NUM_FMT_INT,
        12: NUM_FMT_INT, 13: NUM_FMT_INT, 14: NUM_FMT_PCT, 15: NUM_FMT_RATIO
    }

    write_header_row(ws, 1, headers)
    write_data_rows(ws, 2, rows, fmt_map=fmt)

    # Data bars on Clicks and UV columns
    if rows:
        last_row = len(rows) + 1
        for col_letter in ["C", "D"]:
            ws.conditional_formatting.add(
                f"{col_letter}2:{col_letter}{last_row}",
                DataBarRule(start_type="num", start_value=0, end_type="max",
                            color=LAKE_50, showValue=True)
            )

    finalize_sheet(ws)
    log("  Tab 2: Weekly Trend")


def build_story_performance(wb, con):
    """Tab 3: Story Performance — which stories resonate most."""
    ws = wb.create_sheet("Story Performance")
    ws.sheet_properties.tabColor = GRAY_IV

    rows = con.execute("""
        WITH story_events AS (
            SELECT
                e.story_id,
                COUNT(CASE WHEN e.action_type = 'Read' THEN 1 END) as total_reads,
                COUNT(DISTINCT CASE WHEN e.action_type = 'Read' THEN e.person_hash END) as unique_readers,
                COUNT(CASE WHEN e.action_type = 'Like' THEN 1 END) as likes,
                COUNT(CASE WHEN e.action_type = 'Send Invite' THEN 1 END) as invites,
            FROM events e
            WHERE e.story_id IS NOT NULL
            GROUP BY e.story_id
        )
        SELECT
            se.story_id,
            COALESCE(m.story_title, '(unknown)') as title,
            COALESCE(m.keys, '') as keys,
            COALESCE(m.author_division, '') as author_division,
            COALESCE(m.status, 'unknown') as status,
            m.created,
            se.total_reads,
            se.unique_readers,
            se.likes,
            CASE WHEN se.unique_readers > 0
                THEN ROUND(se.likes * 1.0 / se.unique_readers, 3)
                ELSE 0 END as like_rate,
            se.invites,
            CASE WHEN m.created IS NOT NULL
                THEN DATEDIFF('day', m.created::DATE,
                    COALESCE(m.deleted_date, CURRENT_DATE)) + 1
                ELSE NULL END as lifespan_days,
            CASE WHEN m.created IS NOT NULL
                    AND DATEDIFF('day', m.created::DATE,
                        COALESCE(m.deleted_date, CURRENT_DATE)) + 1 > 0
                THEN ROUND(se.total_reads * 1.0 /
                    (DATEDIFF('day', m.created::DATE,
                        COALESCE(m.deleted_date, CURRENT_DATE)) + 1), 1)
                ELSE NULL END as reads_per_day
        FROM story_events se
        LEFT JOIN story_meta m ON se.story_id = m.story_id
        ORDER BY se.total_reads DESC
    """).fetchall()

    headers = [
        "Story ID", "Title", "Keys", "Author Division", "Status", "Created",
        "Total Reads", "Unique Readers", "Likes", "Like Rate",
        "Invites", "Lifespan (days)", "Reads/Day"
    ]
    fmt = {
        5: NUM_FMT_DATE, 6: NUM_FMT_INT, 7: NUM_FMT_INT, 8: NUM_FMT_INT,
        9: NUM_FMT_PCT, 10: NUM_FMT_INT, 11: NUM_FMT_INT, 12: NUM_FMT_RATIO
    }

    write_header_row(ws, 1, headers)
    write_data_rows(ws, 2, rows, fmt_map=fmt)

    finalize_sheet(ws)
    log("  Tab 3: Story Performance")


def build_key_performance(wb, con):
    """Tab 4: Key Performance — which story keys drive the most engagement."""
    ws = wb.create_sheet("Key Performance")
    ws.sheet_properties.tabColor = GRAY_IV

    # Check if key columns exist in story_meta
    meta_cols = [r[0] for r in con.execute("DESCRIBE story_meta").fetchall()]
    has_split_keys = all(f"story_key{i}" in meta_cols for i in range(1, 4))
    has_keys = "keys" in meta_cols

    if not has_keys and not has_split_keys:
        ws.cell(row=1, column=1, value="No key data available in story metadata")
        log("  Tab 4: Key Performance (skipped — no key data)")
        return

    # Unpivot keys: each story can have up to 3 keys, we need one row per key
    if has_split_keys:
        key_union = """
            SELECT story_id, TRIM(story_key1) as key FROM story_meta WHERE TRIM(COALESCE(story_key1, '')) != ''
            UNION ALL
            SELECT story_id, TRIM(story_key2) FROM story_meta WHERE TRIM(COALESCE(story_key2, '')) != ''
            UNION ALL
            SELECT story_id, TRIM(story_key3) FROM story_meta WHERE TRIM(COALESCE(story_key3, '')) != ''
        """
    else:
        key_union = """
            SELECT story_id, TRIM(UNNEST(string_split(keys, ','))) as key
            FROM story_meta
            WHERE keys IS NOT NULL AND TRIM(keys) != ''
        """

    rows = con.execute(f"""
        WITH story_keys AS ({key_union}),
        key_engagement AS (
            SELECT
                sk.key,
                COUNT(DISTINCT sk.story_id) as stories,
                COUNT(CASE WHEN e.action_type = 'Read' THEN 1 END) as reads,
                COUNT(DISTINCT CASE WHEN e.action_type = 'Read' THEN e.person_hash END) as unique_readers,
                COUNT(CASE WHEN e.action_type = 'Like' THEN 1 END) as likes,
                COUNT(CASE WHEN e.action_type IN ('Read', 'Like') THEN 1 END) as engagements,
                COUNT(DISTINCT CASE WHEN e.action_type IN ('Read', 'Like') THEN e.person_hash END) as engaged_visitors
            FROM story_keys sk
            LEFT JOIN events e ON sk.story_id = e.story_id
                AND e.action_type IN ('Read', 'Like')
            GROUP BY sk.key
        )
        SELECT
            key,
            stories,
            engagements,
            engaged_visitors,
            reads,
            likes,
            CASE WHEN reads > 0 THEN ROUND(likes * 1.0 / reads, 3) ELSE 0 END as like_rate,
            CASE WHEN stories > 0 THEN ROUND(reads * 1.0 / stories, 1) ELSE 0 END as avg_reads_per_story,
            CASE WHEN stories > 0 THEN ROUND(likes * 1.0 / stories, 1) ELSE 0 END as avg_likes_per_story
        FROM key_engagement
        WHERE key IS NOT NULL AND TRIM(key) != ''
        ORDER BY engagements DESC
    """).fetchall()

    headers = [
        "Key", "Stories", "Engagements", "Engaged Visitors", "Reads", "Likes",
        "Like Rate", "Avg. Reads / Story", "Avg. Likes / Story"
    ]
    fmt = {
        1: NUM_FMT_INT, 2: NUM_FMT_INT, 3: NUM_FMT_INT, 4: NUM_FMT_INT,
        5: NUM_FMT_INT, 6: NUM_FMT_PCT, 7: NUM_FMT_RATIO, 8: NUM_FMT_RATIO
    }

    write_header_row(ws, 1, headers)
    write_data_rows(ws, 2, rows, fmt_map=fmt)

    finalize_sheet(ws)
    log("  Tab 4: Key Performance")


def build_division_engagement(wb, con, cols):
    """Tab 4: Division Engagement."""
    ws = wb.create_sheet("Division Engagement")
    ws.sheet_properties.tabColor = GRAY_IV

    if "visitor_division" not in cols:
        ws.cell(row=1, column=1, value="No organizational data available (visitor_division missing)")
        log("  Tab 4: Division Engagement (skipped — no org data)")
        return

    rows = con.execute("""
        SELECT
            COALESCE(visitor_division, '(Unknown)') as division,
            COUNT(DISTINCT person_hash) as engaged_visitors,
            COUNT(*) as engagements,
            COUNT(CASE WHEN action_type = 'Read' THEN 1 END) as reads,
            COUNT(CASE WHEN action_type = 'Like' THEN 1 END) as likes,
            ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT person_hash), 0), 1) as eng_per_visitor,
            CASE WHEN COUNT(CASE WHEN action_type = 'Read' THEN 1 END) > 0
                THEN ROUND(COUNT(CASE WHEN action_type = 'Like' THEN 1 END) * 1.0 /
                    COUNT(CASE WHEN action_type = 'Read' THEN 1 END), 3)
                ELSE 0 END as like_rate
        FROM events
        WHERE action_type IN ('Read', 'Like')
        GROUP BY COALESCE(visitor_division, '(Unknown)')
        ORDER BY engaged_visitors DESC
    """).fetchall()

    total_uv = con.execute("""
        SELECT COUNT(DISTINCT person_hash) FROM events WHERE action_type IN ('Read', 'Like')
    """).fetchone()[0]

    headers = [
        "Division", "Engaged Visitors", "Engagements", "Reads", "Likes",
        "Engagements / Visitor", "Like Rate", "% of Total"
    ]
    fmt = {
        1: NUM_FMT_INT, 2: NUM_FMT_INT, 3: NUM_FMT_INT, 4: NUM_FMT_INT,
        5: NUM_FMT_RATIO, 6: NUM_FMT_PCT, 7: NUM_FMT_PCT
    }

    write_header_row(ws, 1, headers)

    enriched_rows = []
    for row in rows:
        row_list = list(row)
        row_list.append(row[1] / total_uv if total_uv > 0 else 0)
        enriched_rows.append(tuple(row_list))

    write_data_rows(ws, 2, enriched_rows, fmt_map=fmt)

    # Total row
    totals = con.execute("""
        SELECT
            'TOTAL',
            COUNT(DISTINCT person_hash),
            COUNT(*),
            COUNT(CASE WHEN action_type = 'Read' THEN 1 END),
            COUNT(CASE WHEN action_type = 'Like' THEN 1 END),
            ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT person_hash), 0), 1),
            CASE WHEN COUNT(CASE WHEN action_type = 'Read' THEN 1 END) > 0
                THEN ROUND(COUNT(CASE WHEN action_type = 'Like' THEN 1 END) * 1.0 /
                    COUNT(CASE WHEN action_type = 'Read' THEN 1 END), 3)
                ELSE 0 END,
            1.0
        FROM events
        WHERE action_type IN ('Read', 'Like')
    """).fetchone()
    total_row_idx = len(enriched_rows) + 2
    write_total_row(ws, total_row_idx, totals, fmt_map=fmt)

    finalize_sheet(ws)
    log("  Tab 4: Division Engagement")


def build_region_engagement(wb, con, cols):
    """Tab 5: Region Engagement."""
    ws = wb.create_sheet("Region Engagement")
    ws.sheet_properties.tabColor = GRAY_IV

    if "visitor_region" not in cols:
        ws.cell(row=1, column=1, value="No regional data available (visitor_region missing)")
        log("  Tab 5: Region Engagement (skipped — no region data)")
        return

    rows = con.execute("""
        SELECT
            COALESCE(visitor_region, '(Unknown)') as region,
            COUNT(DISTINCT person_hash) as engaged_visitors,
            COUNT(*) as engagements,
            COUNT(CASE WHEN action_type = 'Read' THEN 1 END) as reads,
            COUNT(CASE WHEN action_type = 'Like' THEN 1 END) as likes,
            ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT person_hash), 0), 1) as eng_per_visitor,
            CASE WHEN COUNT(CASE WHEN action_type = 'Read' THEN 1 END) > 0
                THEN ROUND(COUNT(CASE WHEN action_type = 'Like' THEN 1 END) * 1.0 /
                    COUNT(CASE WHEN action_type = 'Read' THEN 1 END), 3)
                ELSE 0 END as like_rate
        FROM events
        WHERE action_type IN ('Read', 'Like')
        GROUP BY COALESCE(visitor_region, '(Unknown)')
        ORDER BY engaged_visitors DESC
    """).fetchall()

    total_uv = con.execute("""
        SELECT COUNT(DISTINCT person_hash) FROM events WHERE action_type IN ('Read', 'Like')
    """).fetchone()[0]

    headers = [
        "Region", "Engaged Visitors", "Engagements", "Reads", "Likes",
        "Engagements / Visitor", "Like Rate", "% of Total"
    ]
    fmt = {
        1: NUM_FMT_INT, 2: NUM_FMT_INT, 3: NUM_FMT_INT, 4: NUM_FMT_INT,
        5: NUM_FMT_RATIO, 6: NUM_FMT_PCT, 7: NUM_FMT_PCT
    }

    write_header_row(ws, 1, headers)

    enriched_rows = []
    for row in rows:
        row_list = list(row)
        row_list.append(row[1] / total_uv if total_uv > 0 else 0)
        enriched_rows.append(tuple(row_list))

    write_data_rows(ws, 2, enriched_rows, fmt_map=fmt)

    # Total row
    totals = con.execute("""
        SELECT
            'TOTAL',
            COUNT(DISTINCT person_hash),
            COUNT(*),
            COUNT(CASE WHEN action_type = 'Read' THEN 1 END),
            COUNT(CASE WHEN action_type = 'Like' THEN 1 END),
            ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT person_hash), 0), 1),
            CASE WHEN COUNT(CASE WHEN action_type = 'Read' THEN 1 END) > 0
                THEN ROUND(COUNT(CASE WHEN action_type = 'Like' THEN 1 END) * 1.0 /
                    COUNT(CASE WHEN action_type = 'Read' THEN 1 END), 3)
                ELSE 0 END,
            1.0
        FROM events
        WHERE action_type IN ('Read', 'Like')
    """).fetchone()
    write_total_row(ws, len(enriched_rows) + 2, totals, fmt_map=fmt)

    finalize_sheet(ws)
    log("  Tab 5: Region Engagement")


def build_hourly_weekday(wb, con):
    """Tab 6: Hourly & Weekday patterns with heatmap."""
    ws = wb.create_sheet("Hourly & Weekday")
    ws.sheet_properties.tabColor = GRAY_IV

    # All three parts filtered to engagement = Read + Like
    ENG_FILTER = "WHERE action_type IN ('Read', 'Like')"

    # Part A: Weekday summary
    r = 1
    write_section_header(ws, r, "WEEKDAY SUMMARY (Engagement: Reads + Likes)", 6); r += 1
    weekday_headers = ["Weekday", "Engagements", "Engaged Visitors", "Reads", "Likes", "Avg. per Day"]
    write_header_row(ws, r, weekday_headers); r += 1

    weekday_rows = con.execute(f"""
        SELECT
            event_weekday,
            COUNT(*) as engagements,
            COUNT(DISTINCT person_hash) as uv,
            COUNT(CASE WHEN action_type = 'Read' THEN 1 END) as reads,
            COUNT(CASE WHEN action_type = 'Like' THEN 1 END) as likes,
            ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT session_date), 0), 1) as avg_per_day
        FROM events
        {ENG_FILTER}
        GROUP BY event_weekday, event_weekday_num
        ORDER BY event_weekday_num
    """).fetchall()

    fmt_wd = {1: NUM_FMT_INT, 2: NUM_FMT_INT, 3: NUM_FMT_INT, 4: NUM_FMT_INT, 5: NUM_FMT_RATIO}
    write_data_rows(ws, r, weekday_rows, fmt_map=fmt_wd)
    r += len(weekday_rows) + 2

    # Part B: Hourly summary
    write_section_header(ws, r, "HOURLY SUMMARY (Engagement, CET)", 4); r += 1
    hourly_headers = ["Hour (CET)", "Engagements", "Engaged Visitors"]
    write_header_row(ws, r, hourly_headers); r += 1

    hourly_rows = con.execute(f"""
        SELECT
            event_hour,
            COUNT(*) as engagements,
            COUNT(DISTINCT person_hash) as uv
        FROM events
        {ENG_FILTER}
        GROUP BY event_hour
        ORDER BY event_hour
    """).fetchall()

    fmt_h = {0: "00", 1: NUM_FMT_INT, 2: NUM_FMT_INT}
    write_data_rows(ws, r, hourly_rows, fmt_map=fmt_h)
    r += len(hourly_rows) + 2

    # Part C: Heatmap matrix (weekday x hour)
    write_section_header(ws, r, "ENGAGEMENT HEATMAP (Reads + Likes by Weekday x Hour CET)", 26); r += 1

    heatmap_headers = [""] + [f"{h:02d}" for h in range(24)]
    write_header_row(ws, r, heatmap_headers); r += 1

    heatmap_data = con.execute(f"""
        SELECT event_weekday, event_weekday_num, event_hour, COUNT(*) as cnt
        FROM events
        {ENG_FILTER}
        GROUP BY event_weekday, event_weekday_num, event_hour
        ORDER BY event_weekday_num, event_hour
    """).fetchall()

    # Build matrix
    weekdays_ordered = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    matrix = {wd: [0] * 24 for wd in weekdays_ordered}
    for wd, _, hour, cnt in heatmap_data:
        if wd in matrix:
            matrix[wd][int(hour)] = cnt

    heatmap_start_row = r
    for wd in weekdays_ordered:
        row_data = [wd] + matrix[wd]
        for ci, val in enumerate(row_data):
            cell = ws.cell(row=r, column=ci + 1, value=val)
            cell.border = THIN_BORDER
            if ci == 0:
                cell.font = Font(bold=True)
            else:
                cell.number_format = NUM_FMT_INT
                cell.alignment = Alignment(horizontal="center")
        r += 1

    # Color scale on heatmap cells
    heatmap_end_row = r - 1
    heatmap_range = f"B{heatmap_start_row}:Y{heatmap_end_row}"
    ws.conditional_formatting.add(
        heatmap_range,
        ColorScaleRule(
            start_type="min", start_color=WHITE,
            mid_type="percentile", mid_value=50, mid_color=RAG_AMBER,
            end_type="max", end_color=CORP_RED
        )
    )

    finalize_sheet(ws, freeze_row=1)
    log("  Tab 6: Hourly & Weekday")


def build_conversion_funnel(wb, con):
    """Tab 7: Conversion Funnel — user journey depth."""
    ws = wb.create_sheet("Conversion Funnel")
    ws.sheet_properties.tabColor = GRAY_IV

    # Funnel: how many unique visitors performed each action type
    funnel = con.execute("""
        SELECT
            COUNT(DISTINCT person_hash) as total_visitors,
            COUNT(DISTINCT CASE WHEN action_type = 'Read' THEN person_hash END) as readers,
            COUNT(DISTINCT CASE WHEN action_type = 'Like' THEN person_hash END) as likers,
            COUNT(DISTINCT CASE WHEN action_type = 'Open Invite' THEN person_hash END) as invite_openers,
            COUNT(DISTINCT CASE WHEN action_type = 'Send Invite' THEN person_hash END) as invite_senders,
            COUNT(DISTINCT CASE WHEN action_type = 'Open Form' THEN person_hash END) as form_openers,
            COUNT(DISTINCT CASE WHEN action_type = 'Submit' THEN person_hash END) as submitters
        FROM events
    """).fetchone()

    total_uv = funnel[0]

    steps = [
        ("Total Visitors", funnel[0]),
        ("Read a Story", funnel[1]),
        ("Liked a Story", funnel[2]),
        ("Opened Invite Dialog", funnel[3]),
        ("Sent an Invite", funnel[4]),
        ("Opened Submission Form", funnel[5]),
        ("Submitted a Story", funnel[6]),
    ]

    r = 1
    write_section_header(ws, r, "ENGAGEMENT FUNNEL", 4); r += 1
    headers = ["Step", "Unique Visitors", "% of Previous Step", "% of Total Visitors"]
    fmt = {1: NUM_FMT_INT, 2: NUM_FMT_PCT, 3: NUM_FMT_PCT}
    write_header_row(ws, r, headers); r += 1

    for i, (label, count) in enumerate(steps):
        prev_count = steps[i - 1][1] if i > 0 else count
        pct_prev = count / prev_count if prev_count > 0 else 0
        pct_total = count / total_uv if total_uv > 0 else 0
        row_data = (label, count, pct_prev, pct_total)
        write_data_rows(ws, r, [row_data], fmt_map=fmt)
        r += 1

    r += 2

    # Multi-action depth
    write_section_header(ws, r, "ENGAGEMENT DEPTH (Distinct Action Types per Visitor)", 3); r += 1
    depth_headers = ["Action Types Used", "Visitors", "% of Total"]
    write_header_row(ws, r, depth_headers); r += 1

    depth_rows = con.execute("""
        WITH visitor_depth AS (
            SELECT person_hash, COUNT(DISTINCT action_type) as n_actions
            FROM events
            GROUP BY person_hash
        )
        SELECT
            CASE
                WHEN n_actions = 1 THEN '1 action type'
                WHEN n_actions = 2 THEN '2 action types'
                WHEN n_actions = 3 THEN '3 action types'
                ELSE '4+ action types'
            END as depth,
            COUNT(*) as visitors,
            ROUND(COUNT(*) * 1.0 / (SELECT COUNT(*) FROM visitor_depth), 3) as pct
        FROM visitor_depth
        GROUP BY CASE
                WHEN n_actions = 1 THEN '1 action type'
                WHEN n_actions = 2 THEN '2 action types'
                WHEN n_actions = 3 THEN '3 action types'
                ELSE '4+ action types'
            END
        ORDER BY depth
    """).fetchall()

    fmt_d = {1: NUM_FMT_INT, 2: NUM_FMT_PCT}
    write_data_rows(ws, r, depth_rows, fmt_map=fmt_d)

    finalize_sheet(ws, freeze_row=1)
    log("  Tab 7: Conversion Funnel")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate CampaignWe XLSX Report")
    parser.add_argument("--output", type=str, default=None,
                        help="Output XLSX path (default: output/campaignwe_report.xlsx)")
    parser.add_argument("--date-from", type=str, default=None,
                        help="Start date filter (YYYY-MM-DD)")
    parser.add_argument("--date-to", type=str, default=None,
                        help="End date filter (YYYY-MM-DD)")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    output_dir = script_dir / "output"
    events_path = output_dir / "events_anonymized.parquet"
    metadata_path = output_dir / "story_metadata.parquet"

    output_path = Path(args.output) if args.output else output_dir / "campaignwe_report.xlsx"

    log("=" * 64)
    log("  CampaignWe XLSX Report Generator")
    log("=" * 64)
    log()
    log("Loading data...")

    con, cols = load_data(events_path, metadata_path, args.date_from, args.date_to)

    log()
    log("Building report tabs...")
    wb = Workbook()
    # Remove the default sheet
    wb.remove(wb.active)

    build_executive_summary(wb, con, cols)
    build_weekly_trend(wb, con)
    build_story_performance(wb, con)
    build_key_performance(wb, con)
    build_division_engagement(wb, con, cols)
    build_region_engagement(wb, con, cols)
    build_hourly_weekday(wb, con)
    build_conversion_funnel(wb, con)

    log()
    log(f"Saving to {output_path}...")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(str(output_path))

    size_kb = output_path.stat().st_size / 1024
    log(f"  Done! ({size_kb:.1f} KB)")
    log()

    con.close()


if __name__ == "__main__":
    main()
