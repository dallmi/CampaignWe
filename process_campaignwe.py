#!/usr/bin/env python3
"""
CampaignWe Click Event Processing Script

This script processes click_event data extracted via KQL from Application Insights
for the example.aspx page. It creates/updates a DuckDB database, joins with
HR data from hr_history.parquet via GPN, and exports Parquet files for reporting.

Usage:
    python process_campaignwe.py                    # Auto-detect latest file in data/
    python process_campaignwe.py data/export.xlsx   # Process specific file
    python process_campaignwe.py --full-refresh     # Delete DB and reprocess all files

Input folder: data/
    Place your KQL export files here with date suffix _YYYY_MM_DD, e.g.:
    - campaign_export_2026_02_25.xlsx
    - campaign_export_2026_02_25.csv

    The file with the most recent date in the filename will be processed.

Output:
    - data/campaignwe.db                (DuckDB database)
    - output/events_raw.parquet         (all event-level data with HR fields)
    - output/events_daily.parquet       (aggregated by day)
    - output/events_story.parquet       (story engagement by day, division, region)

Primary Key: timestamp + user_id + session_id + name
    On conflict, the latest file's data takes precedence.
"""

import sys
import os
import re
import glob
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime


def log(message):
    """Print timestamped log message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def extract_date_from_filename(filepath):
    """
    Extract date from filename with format _YYYY_MM_DD.
    Returns a date object or None if not found.
    """
    filename = Path(filepath).stem
    match = re.search(r'_(\d{4})_(\d{2})_(\d{2})$', filename)
    if match:
        try:
            year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
            return datetime(year, month, day).date()
        except ValueError:
            return None
    return None


def find_latest_input_file(input_dir):
    """
    Find the latest input file in the input directory based on date in filename.
    Expects format: filename_YYYY_MM_DD.xlsx or filename_YYYY_MM_DD.csv
    """
    patterns = ['*.xlsx', '*.xls', '*.csv']
    all_files = []

    for pattern in patterns:
        all_files.extend(glob.glob(str(input_dir / pattern)))

    if not all_files:
        return None

    files_with_dates = []
    for f in all_files:
        file_date = extract_date_from_filename(f)
        if file_date:
            files_with_dates.append((Path(f), file_date))

    if not files_with_dates:
        log("  Warning: No files with _YYYY_MM_DD suffix found, using modification time")
        all_files.sort(key=os.path.getmtime, reverse=True)
        return Path(all_files[0])

    files_with_dates.sort(key=lambda x: x[1], reverse=True)
    return files_with_dates[0][0]


def get_all_input_files(input_dir):
    """Get all input files sorted by date in filename (oldest first for processing order)."""
    patterns = ['*.xlsx', '*.xls', '*.csv']
    all_files = []

    for pattern in patterns:
        all_files.extend(glob.glob(str(input_dir / pattern)))

    files_with_dates = []
    files_without_dates = []

    for f in all_files:
        file_date = extract_date_from_filename(f)
        if file_date:
            files_with_dates.append((Path(f), file_date))
        else:
            files_without_dates.append(Path(f))

    files_with_dates.sort(key=lambda x: x[1])

    result = [f for f, _ in files_with_dates]
    files_without_dates.sort(key=os.path.getmtime)
    result.extend(files_without_dates)

    return result


def load_file_to_temp_table(con, input_path, temp_table='temp_import'):
    """Load a CSV or Excel file into a temporary table."""
    con.execute(f"DROP TABLE IF EXISTS {temp_table}")

    if input_path.suffix.lower() in ['.xlsx', '.xls']:
        # First pass: read only column names
        df_cols = pd.read_excel(input_path, nrows=0)
        all_cols = df_cols.columns.tolist()
        timestamp_cols = [col for col in all_cols if 'timestamp' in col.lower()]

        # Read Excel with timestamp columns as strings to preserve precision
        if timestamp_cols:
            dtype_dict = {col: str for col in timestamp_cols}
            df = pd.read_excel(input_path, dtype=dtype_dict)
            log(f"  Reading timestamp columns as strings: {timestamp_cols}")
        else:
            df = pd.read_excel(input_path)

        con.register('excel_df', df)
        con.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM excel_df")
        con.unregister('excel_df')
    else:
        con.execute(f"""
            CREATE TABLE {temp_table} AS
            SELECT * FROM read_csv('{input_path}', auto_detect=true)
        """)

    # Normalize column names
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    col_names = schema['column_name'].tolist()

    rename_map = {
        'user_Id': 'user_id',
        'session_Id': 'session_id',
        'timestamp [UTC]': 'timestamp'
    }
    for old_name, new_name in rename_map.items():
        if old_name in col_names:
            con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{old_name}" TO {new_name}')

    # Convert date formats (German dd.MM.yyyy and App Insights dd/MM/yyyy)
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    varchar_cols = schema[schema['column_type'] == 'VARCHAR']['column_name'].tolist()

    for col in varchar_cols:
        sample = con.execute(f'SELECT "{col}" FROM {temp_table} WHERE "{col}" IS NOT NULL LIMIT 1').df()
        if len(sample) > 0:
            val = str(sample.iloc[0, 0])
            fmt = None

            if re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}\.\d+$', val):
                fmt = '%d/%m/%Y %H:%M:%S.%f'
                frac_part = val.split('.')[-1]
                if len(frac_part) > 6:
                    fmt = 'TRUNCATE_FRAC'
            elif re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$', val):
                fmt = '%d/%m/%Y %H:%M:%S'
            elif re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$', val):
                fmt = '%d/%m/%Y %H:%M'
            elif re.match(r'^\d{2}/\d{2}/\d{4}$', val):
                fmt = '%d/%m/%Y'
            elif re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$', val):
                fmt = '%d.%m.%Y %H:%M:%S'
            elif re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}$', val):
                fmt = '%d.%m.%Y %H:%M'
            elif re.match(r'^\d{2}\.\d{2}\.\d{4}$', val):
                fmt = '%d.%m.%Y'
            elif re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$', val):
                fmt = '%Y-%m-%d %H:%M:%S.%f'
            elif re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', val):
                fmt = '%Y-%m-%d %H:%M:%S'
            elif re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', val):
                fmt = 'ISO'

            if fmt == 'TRUNCATE_FRAC':
                try:
                    con.execute(f'ALTER TABLE {temp_table} ADD COLUMN "{col}_temp" TIMESTAMP')
                    con.execute(f'''
                        UPDATE {temp_table} SET "{col}_temp" = strptime(
                            CASE
                                WHEN "{col}" LIKE '%.%'
                                THEN SUBSTRING("{col}", 1, POSITION('.' IN "{col}") + 6)
                                ELSE "{col}"
                            END,
                            '%d/%m/%Y %H:%M:%S.%f'
                        )
                    ''')
                    con.execute(f'ALTER TABLE {temp_table} DROP COLUMN "{col}"')
                    con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{col}_temp" TO "{col}"')
                except Exception as e:
                    log(f"  WARNING: Failed to convert '{col}' with truncation: {e}")
            elif fmt == 'ISO':
                try:
                    con.execute(f'ALTER TABLE {temp_table} ADD COLUMN "{col}_temp" TIMESTAMP')
                    con.execute(f'UPDATE {temp_table} SET "{col}_temp" = CAST("{col}" AS TIMESTAMP)')
                    con.execute(f'ALTER TABLE {temp_table} DROP COLUMN "{col}"')
                    con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{col}_temp" TO "{col}"')
                except Exception:
                    pass
            elif fmt:
                try:
                    con.execute(f'ALTER TABLE {temp_table} ADD COLUMN "{col}_temp" TIMESTAMP')
                    con.execute(f'UPDATE {temp_table} SET "{col}_temp" = strptime("{col}", \'{fmt}\')')
                    con.execute(f'ALTER TABLE {temp_table} DROP COLUMN "{col}"')
                    con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{col}_temp" TO "{col}"')
                except Exception:
                    pass

    # Fallback: Try to convert any remaining VARCHAR timestamp column using CAST
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    for _, row in schema.iterrows():
        col = row['column_name']
        col_type = row['column_type']
        if col.lower() == 'timestamp' and col_type == 'VARCHAR':
            try:
                con.execute(f'ALTER TABLE {temp_table} ADD COLUMN "{col}_temp" TIMESTAMP')
                con.execute(f'UPDATE {temp_table} SET "{col}_temp" = TRY_CAST("{col}" AS TIMESTAMP)')
                con.execute(f'ALTER TABLE {temp_table} DROP COLUMN "{col}"')
                con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{col}_temp" TO "{col}"')
                log(f"  Converted '{col}' to TIMESTAMP using TRY_CAST")
            except Exception as e:
                log(f"  WARNING: Could not convert '{col}' to TIMESTAMP: {e}")

    # Check for timestamp precision
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    timestamp_cols = [col for col in schema['column_name'].tolist()
                      if 'timestamp' in col.lower()]

    for col in timestamp_cols:
        try:
            result = con.execute(f"""
                SELECT COUNT(*) as cnt
                FROM {temp_table}
                WHERE EXTRACT(microsecond FROM "{col}") != 0
            """).df()
            has_microseconds = result['cnt'][0] > 0

            if not has_microseconds:
                log(f"  WARNING: Column '{col}' has no microsecond precision!")
                log(f"           For precise timing, export from App Insights as CSV (not Excel).")
        except Exception:
            pass

    row_count = con.execute(f"SELECT COUNT(*) as n FROM {temp_table}").df()['n'][0]
    return row_count


def upsert_data(con, temp_table='temp_import'):
    """
    Upsert data from temp table into main events_raw table.
    Primary key: timestamp + user_id + session_id + name
    """
    tables = con.execute("SHOW TABLES").df()
    table_exists = 'events_raw' in tables['name'].values if len(tables) > 0 else False

    if not table_exists:
        con.execute(f"ALTER TABLE {temp_table} RENAME TO events_raw")
        log("  Created new events_raw table")
        return

    before_count = con.execute("SELECT COUNT(*) as n FROM events_raw").df()['n'][0]

    con.execute(f"""
        DELETE FROM events_raw
        WHERE EXISTS (
            SELECT 1 FROM {temp_table} t
            WHERE events_raw.timestamp = t.timestamp
              AND events_raw.user_id = t.user_id
              AND events_raw.session_id = t.session_id
              AND events_raw.name = t.name
        )
    """)

    deleted_count = before_count - con.execute("SELECT COUNT(*) as n FROM events_raw").df()['n'][0]

    con.execute(f"""
        INSERT INTO events_raw
        SELECT * FROM {temp_table}
    """)

    after_count = con.execute("SELECT COUNT(*) as n FROM events_raw").df()['n'][0]
    new_rows = after_count - before_count + deleted_count

    if deleted_count > 0:
        log(f"  Updated {deleted_count:,} existing rows, added {new_rows - deleted_count:,} new rows")
    else:
        log(f"  Added {new_rows:,} new rows")

    con.execute(f"DROP TABLE IF EXISTS {temp_table}")


def load_hr_history(con, hr_parquet_path):
    """
    Load hr_history.parquet into DuckDB for GPN-based joining.
    Returns True if loaded successfully, False otherwise.
    """
    if not hr_parquet_path.exists():
        log(f"  WARNING: HR history file not found: {hr_parquet_path}")
        log(f"           Run process_hr_history.py in SearchAnalytics first.")
        return False

    con.execute("DROP TABLE IF EXISTS hr_history")
    con.execute(f"""
        CREATE TABLE hr_history AS
        SELECT * FROM read_parquet('{hr_parquet_path}')
    """)

    row_count = con.execute("SELECT COUNT(*) FROM hr_history").fetchone()[0]
    gpn_count = con.execute("SELECT COUNT(DISTINCT gpn) FROM hr_history").fetchone()[0]
    snapshot_count = con.execute(
        "SELECT COUNT(DISTINCT (snapshot_year, snapshot_month)) FROM hr_history"
    ).fetchone()[0]

    log(f"  Loaded hr_history: {row_count:,} rows, {gpn_count:,} GPNs, {snapshot_count} snapshot(s)")
    return True


def add_calculated_columns(con, has_hr_history=False):
    """Add all calculated columns to events_raw and create final events table."""
    log("Adding calculated columns...")

    con.execute("DROP TABLE IF EXISTS events")

    # Set timezone to UTC so DuckDB interprets naive timestamps as UTC
    con.execute("SET TIMEZONE='UTC'")

    # Get column list
    schema = con.execute("DESCRIBE events_raw").df()
    col_names = schema['column_name'].tolist()

    has_user_id = 'user_id' in col_names
    has_session_id = 'session_id' in col_names
    has_timestamp = 'timestamp' in col_names

    # --- Dynamic column resolution ---
    # GPN field (for HR join) - top-level Email/GPN come from App Insights export,
    # CP_GPN/CP_Email come from CustomProps flattening
    gpn_candidates = [c for c in ['CP_GPN', 'CP_gpn', 'GPN', 'gpn'] if c in col_names]
    # Cast to VARCHAR and strip trailing .0 from Excel float conversion (e.g. "12345678.0" → "12345678")
    if gpn_candidates:
        gpn_expr = f"REGEXP_REPLACE(CAST(COALESCE({', '.join(gpn_candidates)}) AS VARCHAR), '\\.0$', '')"
    else:
        gpn_expr = 'NULL'

    # Email field - top-level Email column exists in this dataset
    email_candidates = [c for c in ['Email', 'email', 'CP_Email', 'CP_email'] if c in col_names]
    email_expr = f"COALESCE({', '.join(email_candidates)})" if email_candidates else 'NULL'

    # Log resolution
    log(f"  GPN column resolved from: [{', '.join(gpn_candidates) if gpn_candidates else 'none found'}]")
    log(f"  Email column resolved from: [{', '.join(email_candidates) if email_candidates else 'none found'}]")

    # HR join expression: match on GPN + event month/year = snapshot month/year
    # Fallback: closest preceding snapshot, then closest following snapshot
    if has_hr_history and gpn_candidates:
        # Discover available HR columns
        hr_schema = con.execute("DESCRIBE hr_history").df()
        hr_cols = hr_schema['column_name'].tolist()

        # Common HR columns to bring in (if they exist in hr_history)
        hr_field_map = {
            'gcrs_division_desc': 'hr_division',
            'ou_code': 'hr_ou_code',
            'work_location_country': 'hr_country',
            'work_location_region': 'hr_region',
            'job_title': 'hr_job_title',
            'job_family': 'hr_job_family',
            'management_level': 'hr_management_level',
            'cost_center': 'hr_cost_center',
        }

        available_hr_fields = {src: alias for src, alias in hr_field_map.items() if src in hr_cols}
        log(f"  HR fields available: {list(available_hr_fields.keys())}")

        # Build HR subquery with time-aware join
        hr_select_parts = [f'h.{src} as {alias}' for src, alias in available_hr_fields.items()]
        hr_select_sql = ', '.join(hr_select_parts) if hr_select_parts else 'NULL as hr_placeholder'

        # Diagnostic: show sample GPNs from both sides
        try:
            event_gpn_sample = con.execute(f"""
                SELECT DISTINCT {gpn_expr} as gpn FROM events_raw
                WHERE {gpn_expr} IS NOT NULL AND TRIM({gpn_expr}) != ''
                LIMIT 5
            """).df()
            hr_gpn_sample = con.execute("""
                SELECT DISTINCT CAST(gpn AS VARCHAR) as gpn FROM hr_history
                LIMIT 5
            """).df()
            log(f"  Sample event GPNs: {event_gpn_sample['gpn'].tolist()}")
            log(f"  Sample HR GPNs:    {hr_gpn_sample['gpn'].tolist()}")
        except Exception:
            pass

        hr_join_sql = f"""
            LEFT JOIN LATERAL (
                SELECT {hr_select_sql}
                FROM hr_history h
                WHERE CAST(h.gpn AS VARCHAR) = {gpn_expr}
                  AND (h.snapshot_year * 100 + h.snapshot_month) <= (YEAR(r.timestamp) * 100 + MONTH(r.timestamp))
                ORDER BY h.snapshot_year DESC, h.snapshot_month DESC
                LIMIT 1
            ) hr_exact ON true
        """

        # Fallback: if no preceding snapshot, try closest following
        hr_fallback_sql = f"""
            LEFT JOIN LATERAL (
                SELECT {hr_select_sql}
                FROM hr_history h
                WHERE CAST(h.gpn AS VARCHAR) = {gpn_expr}
                  AND (h.snapshot_year * 100 + h.snapshot_month) > (YEAR(r.timestamp) * 100 + MONTH(r.timestamp))
                ORDER BY h.snapshot_year ASC, h.snapshot_month ASC
                LIMIT 1
            ) hr_fallback ON true
        """

        # Build COALESCE for each HR field (exact match first, fallback second)
        hr_coalesce_parts = []
        for src, alias in available_hr_fields.items():
            hr_coalesce_parts.append(f"COALESCE(hr_exact.{alias}, hr_fallback.{alias}) as {alias}")
        hr_coalesce_sql = ', '.join(hr_coalesce_parts) if hr_coalesce_parts else ''
    else:
        hr_join_sql = ''
        hr_fallback_sql = ''
        hr_coalesce_sql = ''
        available_hr_fields = {}

    # Build the main query with all calculated columns
    hr_select = f",\n            {hr_coalesce_sql}" if hr_coalesce_sql else ''

    # Resolve CP_Link_label column name
    link_label_candidates = [c for c in ['CP_Link_label', 'CP_link_label', 'Link_label'] if c in col_names]
    link_label_col = link_label_candidates[0] if link_label_candidates else None
    if link_label_col:
        log(f"  Link label column: {link_label_col}")
        story_sql = f"""
            -- Story parsing from {link_label_col}
            -- Only extract story_id from "story of NNN" pattern (not pagination numbers etc.)
            NULLIF(regexp_extract(r."{link_label_col}", 'story of (\\d+)', 1), '') as story_id,
            CASE
                WHEN r."{link_label_col}" ILIKE 'Read story%' OR r."{link_label_col}" ILIKE '%Show More%' THEN 'Read'
                WHEN r."{link_label_col}" ILIKE 'hide story%' OR r."{link_label_col}" ILIKE '%Show Less%' THEN 'Hide'
                WHEN r."{link_label_col}" ILIKE 'View Prompt%' THEN 'View Prompt'
                WHEN r."{link_label_col}" ILIKE '%like%' THEN 'Like'
                WHEN r."{link_label_col}" ILIKE '%share%' THEN 'Share'
                WHEN regexp_full_match(TRIM(r."{link_label_col}"), '\\d+') THEN 'Pagination'
                WHEN r."{link_label_col}" IS NULL OR TRIM(r."{link_label_col}") = '' THEN NULL
                ELSE 'Other'
            END as action_type,"""
    else:
        log("  WARNING: No Link_label column found — story parsing skipped")
        story_sql = """
            NULL::VARCHAR as story_id,
            NULL::VARCHAR as action_type,"""

    con.execute(f"""
        CREATE TABLE events AS
        SELECT
            r.*,
            -- GPN and email extracted for reference
            {gpn_expr} as gpn,
            {email_expr} as email,
            {story_sql}
            -- Timestamp as string for reporting (UTC)
            STRFTIME(r.timestamp, '%Y-%m-%d %H:%M:%S.%g') as timestamp_str,
            -- CET timestamp (convert UTC to Europe/Berlin)
            ((r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::TIMESTAMP as timestamp_cet,
            STRFTIME((r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin', '%Y-%m-%d %H:%M:%S.%g') as timestamp_cet_str,
            -- Session columns (CET-based)
            DATE_TRUNC('day', (r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::DATE as session_date,
            COALESCE(CAST(DATE_TRUNC('day', (r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::DATE AS VARCHAR), '') || '_' ||
                COALESCE(r.user_id, '') || '_' ||
                COALESCE(r.session_id, '') as session_key,
            -- Time extraction (CET-based)
            EXTRACT(HOUR FROM (r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::INTEGER as event_hour,
            DAYNAME((r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin') as event_weekday,
            ISODOW((r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin') as event_weekday_num,
            -- Event ordering (populated via window functions in next step)
            NULL::INTEGER as event_order,
            NULL::VARCHAR as prev_event,
            NULL::TIMESTAMP as prev_timestamp,
            NULL::BIGINT as ms_since_prev_event,
            NULL::DOUBLE as sec_since_prev_event,
            NULL::VARCHAR as time_since_prev_bucket
            {hr_select}
        FROM events_raw r
        {hr_join_sql}
        {hr_fallback_sql}
    """)

    # Now update the window function columns
    con.execute("""
        CREATE OR REPLACE TABLE events AS
        SELECT
            e.* EXCLUDE (event_order, prev_event, prev_timestamp, ms_since_prev_event, sec_since_prev_event, time_since_prev_bucket),
            ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY timestamp) as event_order,
            LAG(name) OVER (PARTITION BY session_key ORDER BY timestamp) as prev_event,
            LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp) as prev_timestamp,
            DATEDIFF('millisecond',
                LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp),
                timestamp
            ) as ms_since_prev_event,
            ROUND(
                DATEDIFF('millisecond',
                    LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp),
                    timestamp
                ) / 1000.0,
            3) as sec_since_prev_event,
            CASE
                WHEN LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp) IS NULL THEN 'First Event'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 500 THEN '< 0.5s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 1000 THEN '0.5-1s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 2000 THEN '1-2s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 5000 THEN '2-5s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 10000 THEN '5-10s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 30000 THEN '10-30s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 60000 THEN '30-60s'
                ELSE '> 60s'
            END as time_since_prev_bucket
        FROM events e
    """)

    row_count = con.execute("SELECT COUNT(*) as n FROM events").df()['n'][0]
    log(f"  Calculated columns added for {row_count:,} rows")

    # Verify CET timezone conversion
    cet_sample = con.execute("""
        SELECT
            timestamp as utc_timestamp,
            timestamp_cet as cet_timestamp,
            EXTRACT(HOUR FROM timestamp) as utc_hour,
            event_hour as cet_hour,
            session_date
        FROM events
        ORDER BY timestamp
        LIMIT 3
    """).df()

    if len(cet_sample) > 0:
        log("  CET timezone conversion verification:")
        for _, row in cet_sample.iterrows():
            utc_ts = str(row['utc_timestamp'])[:23]
            cet_ts = str(row['cet_timestamp'])[:23]
            log(f"    UTC: {utc_ts} (hour {int(row['utc_hour']):02d}) -> CET: {cet_ts} (hour {int(row['cet_hour']):02d}) | session_date: {row['session_date']}")


def export_parquet_files(con, output_dir):
    """Export all Parquet files for reporting."""
    log("Exporting Parquet files...")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Raw data export
    raw_file = output_dir / 'events_raw.parquet'
    if raw_file.exists():
        raw_file.unlink()
    con.execute(f"COPY events TO '{raw_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    raw_count = con.execute(f"SELECT COUNT(*) as n FROM read_parquet('{raw_file}')").df()['n'][0]
    raw_size = os.path.getsize(raw_file) / (1024 * 1024)
    log(f"  events_raw.parquet ({raw_count:,} rows, {raw_size:.1f} MB)")

    # Daily aggregation
    daily_file = output_dir / 'events_daily.parquet'
    if daily_file.exists():
        daily_file.unlink()

    # Check which HR columns exist in events table
    events_schema = con.execute("DESCRIBE events").df()
    events_cols = events_schema['column_name'].tolist()

    hr_daily_cols = []
    if 'hr_division' in events_cols:
        hr_daily_cols.append("COUNT(DISTINCT hr_division) as unique_divisions")
    if 'hr_country' in events_cols:
        hr_daily_cols.append("COUNT(DISTINCT hr_country) as unique_countries")
    if 'hr_region' in events_cols:
        hr_daily_cols.append("COUNT(DISTINCT hr_region) as unique_regions")

    hr_daily_sql = ',\n                ' + ',\n                '.join(hr_daily_cols) if hr_daily_cols else ''

    con.execute(f"""
        CREATE OR REPLACE TABLE events_daily AS
            SELECT
                session_date as date,
                COUNT(*) as total_events,
                COUNT(DISTINCT session_key) as unique_sessions,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT gpn) as unique_gpns,
                -- Temporal patterns
                DAYNAME(session_date) as day_of_week,
                ISODOW(session_date) as day_of_week_num,
                -- Hour distribution (CET-based)
                COUNT(CASE WHEN event_hour >= 3 AND event_hour < 9 THEN 1 END) as events_night,
                COUNT(CASE WHEN event_hour >= 9 AND event_hour < 16 THEN 1 END) as events_morning,
                COUNT(CASE WHEN event_hour >= 16 AND event_hour < 22 THEN 1 END) as events_afternoon,
                COUNT(CASE WHEN event_hour >= 22 OR event_hour < 3 THEN 1 END) as events_evening
                {hr_daily_sql}
            FROM events
            GROUP BY 1
            ORDER BY 1
    """)
    con.execute(f"COPY events_daily TO '{daily_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    daily_count = con.execute("SELECT COUNT(*) as n FROM events_daily").df()['n'][0]
    log(f"  events_daily.parquet ({daily_count} days)")

    # Story-level aggregation (engagement per story with HR dimensions)
    if 'story_id' in events_cols and 'action_type' in events_cols:
        story_file = output_dir / 'events_story.parquet'
        if story_file.exists():
            story_file.unlink()

        hr_story_cols = []
        if 'hr_division' in events_cols:
            hr_story_cols.append("hr_division")
        if 'hr_region' in events_cols:
            hr_story_cols.append("hr_region")

        hr_story_group = ', ' + ', '.join(hr_story_cols) if hr_story_cols else ''
        hr_story_select = hr_story_group

        con.execute(f"""
            CREATE OR REPLACE TABLE events_story AS
                SELECT
                    story_id,
                    session_date as date
                    {hr_story_select},
                    COUNT(*) as total_events,
                    COUNT(DISTINCT gpn) as unique_users,
                    COUNT(DISTINCT session_key) as unique_sessions,
                    COUNT(CASE WHEN action_type = 'Read' THEN 1 END) as reads,
                    COUNT(CASE WHEN action_type = 'Hide' THEN 1 END) as hides,
                    COUNT(CASE WHEN action_type = 'Like' THEN 1 END) as likes,
                    COUNT(CASE WHEN action_type = 'Share' THEN 1 END) as shares,
                    COUNT(CASE WHEN action_type = 'View Prompt' THEN 1 END) as view_prompts,
                    COUNT(CASE WHEN action_type = 'Other' THEN 1 END) as other_actions
                FROM events
                WHERE story_id IS NOT NULL AND story_id != ''
                GROUP BY story_id, session_date {hr_story_group}
                ORDER BY story_id, session_date {hr_story_group}
        """)
        con.execute(f"COPY events_story TO '{story_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
        story_count = con.execute("SELECT COUNT(*) as n FROM events_story").df()['n'][0]
        log(f"  events_story.parquet ({story_count:,} rows)")


def print_summary(con, output_dir=None):
    """Print comprehensive processing summary."""
    log("")
    log("=" * 64)
    log("  PROCESSING SUMMARY")
    log("=" * 64)

    # --- DuckDB tables ---
    log("\n  DATABASE TABLES")
    log("  " + "-" * 60)
    tables = con.execute("SHOW TABLES").df()['name'].tolist()
    for table in sorted(tables):
        if table.startswith('temp'):
            continue
        row_count = con.execute(f"SELECT COUNT(*) as n FROM {table}").df()['n'][0]
        col_count = len(con.execute(f"DESCRIBE {table}").df())
        log(f"    {table:<30s} {row_count:>10,} rows  ({col_count} columns)")

    # --- Parquet files ---
    if output_dir:
        parquet_files = sorted(Path(output_dir).glob('*.parquet'))
        if parquet_files:
            log("\n  PARQUET FILES EXPORTED")
            log("  " + "-" * 60)
            for pf in parquet_files:
                size_mb = os.path.getsize(pf) / (1024 * 1024)
                log(f"    {pf.name:<40s} ({size_mb:.1f} MB)")

    # --- Date range & volume ---
    overview = con.execute("""
        SELECT
            MIN(session_date) as first_date,
            MAX(session_date) as last_date,
            COUNT(DISTINCT session_date) as days,
            COUNT(*) as total_events,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(DISTINCT session_key) as unique_sessions,
            COUNT(DISTINCT gpn) as unique_gpns
        FROM events
    """).df().iloc[0]

    log("\n  DATA OVERVIEW")
    log("  " + "-" * 60)
    if overview['first_date'] is not None:
        log(f"    Date range:        {overview['first_date']} to {overview['last_date']} ({int(overview['days'])} days)")
    log(f"    Total events:      {int(overview['total_events']):,}")
    log(f"    Unique users:      {int(overview['unique_users']):,}")
    log(f"    Unique sessions:   {int(overview['unique_sessions']):,}")
    log(f"    Unique GPNs:       {int(overview['unique_gpns']):,}")

    # --- HR join coverage ---
    events_cols = con.execute("DESCRIBE events").df()['column_name'].tolist()
    if 'hr_division' in events_cols:
        hr_coverage = con.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(hr_division) as with_hr_data,
                COUNT(gpn) as with_gpn
            FROM events
        """).df().iloc[0]

        total = int(hr_coverage['total'])
        with_hr = int(hr_coverage['with_hr_data'])
        with_gpn = int(hr_coverage['with_gpn'])

        log("\n  HR JOIN COVERAGE")
        log("  " + "-" * 60)
        log(f"    Events with GPN:       {with_gpn:>8,} / {total:,}  ({100.0 * with_gpn / total if total > 0 else 0:.1f}%)")
        log(f"    Events with HR data:   {with_hr:>8,} / {total:,}  ({100.0 * with_hr / total if total > 0 else 0:.1f}%)")

        if 'hr_division' in events_cols:
            divisions = con.execute("""
                SELECT hr_division, COUNT(*) as cnt
                FROM events
                WHERE hr_division IS NOT NULL
                GROUP BY hr_division
                ORDER BY cnt DESC
                LIMIT 10
            """).df()
            if len(divisions) > 0:
                log("\n    Top divisions:")
                for _, row in divisions.iterrows():
                    log(f"      {str(row['hr_division']):<40s} {int(row['cnt']):>8,}")

    # --- Field coverage ---
    log("\n  FIELD COVERAGE (non-null values)")
    log("  " + "-" * 60)
    total = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    check_fields = ['gpn', 'email', 'session_id', 'user_id', 'story_id', 'action_type']
    # Add any HR fields
    for col in events_cols:
        if col.startswith('hr_'):
            check_fields.append(col)
    # Add any CP_ fields
    cp_fields = [c for c in events_cols if c.startswith('CP_')]
    check_fields.extend(cp_fields[:15])  # Show first 15 CP fields

    for field in check_fields:
        if field in events_cols:
            val = con.execute(f'SELECT COUNT("{field}") FROM events').fetchone()[0]
            pct = 100.0 * val / total if total > 0 else 0
            bar = "#" * int(pct / 5) if pct > 0 else ""
            log(f"    {field:<35s} {val:>8,} / {total:,}  ({pct:5.1f}%)  {bar}")

    # --- Event name breakdown ---
    events_df = con.execute("""
        SELECT name, COUNT(*) as cnt,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
        FROM events
        GROUP BY name
        ORDER BY cnt DESC
    """).df()

    log("\n  EVENT TYPES")
    log("  " + "-" * 60)
    for _, row in events_df.iterrows():
        log(f"    {row['name']:<35s} {int(row['cnt']):>8,}  ({row['pct']:.1f}%)")

    # --- Action type breakdown ---
    if 'action_type' in events_cols:
        action_df = con.execute("""
            SELECT COALESCE(action_type, '(null)') as action_type, COUNT(*) as cnt,
                   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
            FROM events
            GROUP BY 1
            ORDER BY cnt DESC
        """).df()

        log("\n  ACTION TYPES (from CP_Link_label)")
        log("  " + "-" * 60)
        for _, row in action_df.iterrows():
            log(f"    {row['action_type']:<35s} {int(row['cnt']):>8,}  ({row['pct']:.1f}%)")

        # Show sample "Other" labels for refinement
        other_count = con.execute("SELECT COUNT(*) FROM events WHERE action_type = 'Other'").fetchone()[0]
        if other_count > 0:
            # Find the link label column in events
            ll_col = next((c for c in ['CP_Link_label', 'CP_link_label'] if c in events_cols), None)
            if ll_col:
                other_samples = con.execute(f"""
                    SELECT "{ll_col}" as label, COUNT(*) as cnt
                    FROM events
                    WHERE action_type = 'Other'
                    GROUP BY 1
                    ORDER BY cnt DESC
                    LIMIT 10
                """).df()
                if len(other_samples) > 0:
                    log(f"\n    Sample 'Other' labels ({other_count:,} total):")
                    for _, row in other_samples.iterrows():
                        label_preview = str(row['label'])[:60]
                        log(f"      {label_preview:<60s} {int(row['cnt']):>6,}")

    # --- Link type breakdown ---
    link_type_col = next((c for c in ['CP_Link_Type', 'CP_link_type', 'CP_LinkType'] if c in events_cols), None)
    if link_type_col:
        lt_df = con.execute(f"""
            SELECT COALESCE("{link_type_col}", '(blank)') as link_type, COUNT(*) as cnt,
                   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
            FROM events
            GROUP BY 1
            ORDER BY cnt DESC
        """).df()

        log("\n  LINK TYPES (CP_Link_Type)")
        log("  " + "-" * 60)
        for _, row in lt_df.iterrows():
            log(f"    {row['link_type']:<35s} {int(row['cnt']):>8,}  ({row['pct']:.1f}%)")

    # --- Story engagement ---
    if 'story_id' in events_cols and 'action_type' in events_cols:
        story_stats = con.execute("""
            SELECT
                COUNT(DISTINCT story_id) as unique_stories,
                COUNT(CASE WHEN action_type = 'Read' THEN 1 END) as reads,
                COUNT(CASE WHEN action_type = 'Hide' THEN 1 END) as hides,
                COUNT(CASE WHEN action_type = 'Like' THEN 1 END) as likes,
                COUNT(CASE WHEN action_type = 'Share' THEN 1 END) as shares,
                COUNT(CASE WHEN action_type = 'View Prompt' THEN 1 END) as view_prompts,
                COUNT(CASE WHEN action_type = 'Other' THEN 1 END) as other_actions
            FROM events
            WHERE story_id IS NOT NULL AND story_id != ''
        """).df().iloc[0]

        log("\n  STORY ENGAGEMENT")
        log("  " + "-" * 60)
        log(f"    Unique stories:    {int(story_stats['unique_stories']):,}")
        log(f"    Reads:             {int(story_stats['reads']):,}")
        log(f"    Hides:             {int(story_stats['hides']):,}")
        log(f"    Likes:             {int(story_stats['likes']):,}")
        log(f"    Shares:            {int(story_stats['shares']):,}")
        log(f"    View Prompts:      {int(story_stats['view_prompts']):,}")
        if int(story_stats['other_actions']) > 0:
            log(f"    Other:             {int(story_stats['other_actions']):,}")

        # Top stories by reads
        top_stories = con.execute("""
            SELECT
                story_id,
                COUNT(CASE WHEN action_type = 'Read' THEN 1 END) as reads,
                COUNT(DISTINCT gpn) as unique_readers,
                COUNT(CASE WHEN action_type = 'Like' THEN 1 END) as likes,
                COUNT(CASE WHEN action_type = 'Share' THEN 1 END) as shares
            FROM events
            WHERE story_id IS NOT NULL AND story_id != ''
            GROUP BY story_id
            ORDER BY reads DESC
            LIMIT 10
        """).df()

        if len(top_stories) > 0:
            log("\n    Top stories by reads:")
            log(f"    {'Story ID':<12s} {'Reads':>8s} {'Readers':>8s} {'Likes':>8s} {'Shares':>8s}")
            for _, row in top_stories.iterrows():
                log(f"    {str(row['story_id']):<12s} {int(row['reads']):>8,} {int(row['unique_readers']):>8,} {int(row['likes']):>8,} {int(row['shares']):>8,}")

    log("\n" + "=" * 64)


def process_campaignwe(input_file=None, full_refresh=False):
    """
    Main processing function.

    Args:
        input_file: Specific file to process, or None to auto-detect
        full_refresh: If True, delete DB and reprocess all files
    """
    script_dir = Path(__file__).parent
    data_dir = script_dir / 'data'
    output_dir = script_dir / 'output'
    db_path = data_dir / 'campaignwe.db'

    # HR history parquet from SearchAnalytics
    hr_parquet_path = script_dir.parent / 'SearchAnalytics' / 'output' / 'hr_history.parquet'

    # Create directories
    data_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    log("=" * 60)
    log("CAMPAIGNWE CLICK EVENT PROCESSING")
    log("=" * 60)

    # Handle full refresh
    if full_refresh:
        if db_path.exists():
            db_path.unlink()
            log("Full refresh: deleted existing database")

        files_to_process = get_all_input_files(data_dir)
        if not files_to_process:
            log(f"ERROR: No input files found in {data_dir}")
            log("Place your KQL export files (xlsx/csv) in the data/ folder")
            sys.exit(1)
        log(f"Full refresh: processing {len(files_to_process)} files")
    elif input_file:
        files_to_process = [Path(input_file)]
        if not files_to_process[0].exists():
            log(f"ERROR: File not found: {input_file}")
            sys.exit(1)
    else:
        latest_file = find_latest_input_file(data_dir)
        if not latest_file:
            log(f"ERROR: No input files found in {data_dir}")
            log("Place your KQL export files (xlsx/csv) in the data/ folder")
            log("Supported formats: .xlsx, .xls, .csv")
            log("\nFilename format: campaign_export_YYYY_MM_DD.xlsx")
            log("Example filenames:")
            log("  campaign_export_2026_02_25.xlsx")
            log("  campaign_export_2026_02_25.csv")
            sys.exit(1)
        files_to_process = [latest_file]
        log(f"Auto-detected latest file: {latest_file.name}")

    # Connect to DuckDB
    con = duckdb.connect(str(db_path))

    # Process each file
    for input_path in files_to_process:
        log(f"\nProcessing: {input_path.name}")

        row_count = load_file_to_temp_table(con, input_path)
        log(f"  Loaded {row_count:,} rows")

        upsert_data(con)

    # Load HR history for GPN-based join
    has_hr_history = load_hr_history(con, hr_parquet_path)

    # Add calculated columns (with HR join if available)
    add_calculated_columns(con, has_hr_history=has_hr_history)

    # Export Parquet files
    export_parquet_files(con, output_dir)

    # Print summary
    print_summary(con, output_dir)

    log(f"\nDatabase: {db_path}")
    log(f"Parquet files: {output_dir}")

    con.close()
    log("\nDone!")


if __name__ == "__main__":
    full_refresh = '--full-refresh' in sys.argv

    input_file = None
    for arg in sys.argv[1:]:
        if not arg.startswith('--'):
            input_file = arg
            break

    if len(sys.argv) == 1:
        print(__doc__)
        print("\nNo arguments provided - auto-detecting latest file in data/\n")

    process_campaignwe(input_file=input_file, full_refresh=full_refresh)
