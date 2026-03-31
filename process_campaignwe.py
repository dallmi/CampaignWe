#!/usr/bin/env python3
"""
CampaignWe Click Event Processing Script

This script processes click_event data extracted via KQL from Application Insights
for the example.aspx page. It creates/updates a DuckDB database, joins with
HR data from hr_history.parquet via GPN, and exports Parquet files for reporting.

Usage:
    python process_campaignwe.py                                # Process only new/changed files (delta)
    python process_campaignwe.py input/export.xlsx              # Force-process a specific file
    python process_campaignwe.py --full-refresh                 # Delete DB and reprocess all files
    python process_campaignwe.py --delete-input                 # Delete each input file after processing
    python process_campaignwe.py --full-refresh --delete-input  # Combine flags

Input folder: input/
    Place your KQL export files here with date suffix _YYYY_MM_DD, e.g.:
    - campaign_export_2026_02_25.xlsx
    - campaign_export_2026_02_25.csv

    Only new or modified files are processed (tracked via SHA-256 hash).
    Overlapping time ranges are handled via upsert on the primary key.

Output:
    - data/campaignwe.db                (DuckDB database; events table contains person_hash, no plain GPN/email)
    - output/events_anonymized.parquet  (GPN hashed as person_hash, email dropped; safe for reporting)

PII handling:
    Input files are deleted after successful processing.
    GPN is hashed (SHA-256 -> person_hash) and email is dropped before any data is
    written to disk, so no plain PII is ever stored in output files or the database.

Primary Key: timestamp + user_id + session_id + name
    On conflict, the latest file's data takes precedence.

Action Type Classification (from CP_Link_label, case-insensitive):
    - Open Form   — "%Share your story%"   (user opened the story submission form)
    - Submit      — "%Submit%"             (user submitted a story)
    - Cancel      — "%Cancel%"             (user cancelled/closed the submission form)
    - Delete      — "^\\d+Yes$"            (user confirmed story deletion, e.g. "56Yes")
    - Read        — "%Read%"               (user opened/expanded a story)
    - Like        — "%like%"               (user liked content)
    - Other       — anything else          (excluded from dashboard)

    "Other" groups clicks with no analytical value: closing a story after reading
    (close), editing form fields (edit), browsing/pagination (See more stories,
    pure digit clicks), and events with no label (NULL).
"""

import sys
import os
import re
import glob
import hashlib
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


def compute_file_hash(filepath):
    """SHA-256 hash of file contents for change detection."""
    h = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def ensure_manifest_table(con):
    """Create processed_files manifest table if it doesn't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            filename     TEXT PRIMARY KEY,
            file_hash    TEXT,
            row_count    INTEGER,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            date_suffix  DATE
        )
    """)


def get_unprocessed_files(con, input_dir):
    """
    Return list of (filepath, hash, reason) for files that are new or changed.
    Compares SHA-256 hashes against the processed_files manifest in DuckDB.
    """
    ensure_manifest_table(con)
    all_files = get_all_input_files(input_dir)

    to_process = []
    skipped = []

    for filepath in all_files:
        file_hash = compute_file_hash(filepath)
        filename = filepath.name

        existing = con.execute(
            "SELECT file_hash FROM processed_files WHERE filename = ?",
            [filename]
        ).fetchone()

        if existing is None:
            to_process.append((filepath, file_hash, 'new'))
        elif existing[0] != file_hash:
            to_process.append((filepath, file_hash, 'changed'))
        else:
            skipped.append(filename)

    if skipped:
        log(f"  Skipping {len(skipped)} already-processed file(s): {', '.join(skipped)}")
    if to_process:
        log(f"  Found {len(to_process)} file(s) to process")

    return to_process


def record_processed_file(con, filepath, file_hash, row_count):
    """Record a successfully processed file in the manifest."""
    filename = filepath.name
    date_suffix = extract_date_from_filename(filepath)
    # Use INSERT OR REPLACE to update existing entries (e.g. changed files)
    con.execute("""
        DELETE FROM processed_files WHERE filename = ?
    """, [filename])
    con.execute("""
        INSERT INTO processed_files (filename, file_hash, row_count, processed_at, date_suffix)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)
    """, [filename, file_hash, int(row_count), date_suffix])


def load_file_to_temp_table(con, input_path, temp_table='temp_import'):
    """Load a CSV or Excel file into a temporary table."""
    con.execute(f"DROP TABLE IF EXISTS {temp_table}")

    if input_path.suffix.lower() in ['.xlsx', '.xls']:
        # First pass: read only column names
        df_cols = pd.read_excel(input_path, nrows=0)
        all_cols = df_cols.columns.tolist()
        timestamp_cols = [col for col in all_cols if 'timestamp' in col.lower()]
        # GPN columns must be read as string to preserve leading zeros
        gpn_cols = [col for col in all_cols if col.lower() in ('cp_gpn', 'gpn')]

        # Read Excel with specific columns forced to string type
        dtype_dict = {}
        if timestamp_cols:
            dtype_dict.update({col: str for col in timestamp_cols})
            log(f"  Reading timestamp columns as strings: {timestamp_cols}")
        if gpn_cols:
            dtype_dict.update({col: str for col in gpn_cols})
            log(f"  Reading GPN columns as strings: {gpn_cols}")

        if dtype_dict:
            df = pd.read_excel(input_path, dtype=dtype_dict)
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


def load_story_titles(con, story_titles_path):
    """
    Load story_titles.parquet into DuckDB for story_id -> story_text lookup.
    Returns True if loaded successfully, False otherwise.
    """
    if not story_titles_path.exists():
        log(f"  INFO: Story titles file not found: {story_titles_path}")
        log(f"        Run fetch_story_metadata.py to pull metadata from SharePoint.")
        return False

    con.execute("DROP TABLE IF EXISTS story_titles")
    con.execute(f"""
        CREATE TABLE story_titles AS
        SELECT * FROM read_parquet('{story_titles_path}')
    """)

    # Ensure story_id is VARCHAR to match events.story_id (extracted via regex)
    st_type = con.execute("SELECT typeof(story_id) FROM story_titles LIMIT 1").fetchone()
    if st_type and st_type[0] != 'VARCHAR':
        log(f"  Casting story_titles.story_id from {st_type[0]} to VARCHAR")
        con.execute("ALTER TABLE story_titles ALTER story_id TYPE VARCHAR")

    row_count = con.execute("SELECT COUNT(*) FROM story_titles").fetchone()[0]
    log(f"  Loaded story_titles: {row_count} stories")

    # Diagnostic: show story_id values for debugging joins
    sample = con.execute("""
        SELECT story_id, story_title FROM story_titles ORDER BY story_id
    """).fetchall()
    for sid, title in sample:
        log(f"    story_titles: id={sid!r} title={title!r}")

    return True


def correct_deleted_dates_from_events(con, story_metadata_path):
    """
    Correct deleted_date in story_metadata.parquet using Delete events from App Insights.

    When a user deletes their story, App Insights logs a Delete event with an exact
    timestamp. This is more precise than the metadata comparison (which only detects
    the deletion on the next fetch_story_metadata.py run). This function:
    1. Finds Delete events in the events table
    2. Extracts the earliest delete timestamp per story_id
    3. Updates story_metadata.parquet with the exact date and the person who deleted
    4. Updates the in-memory events table to reflect the corrected deleted_date
    """
    evt_cols = [r[0] for r in con.execute("DESCRIBE events").fetchall()]
    if 'action_type' not in evt_cols:
        return

    # Find Delete events with their timestamps and person_hash
    delete_events = con.execute("""
        SELECT
            story_id,
            MIN(CAST(timestamp AS DATE)) as delete_date,
            FIRST(person_hash) as deleted_by
        FROM events
        WHERE action_type = 'Delete'
          AND story_id IS NOT NULL
        GROUP BY story_id
    """).df()

    if delete_events.empty:
        return

    log(f"\n  Found {len(delete_events)} Delete event(s) in App Insights:")
    for _, row in delete_events.iterrows():
        log(f"    story_id={row['story_id']} deleted on {row['delete_date']} by {str(row['deleted_by'])[:16]}…")

    # Read and update story_metadata.parquet
    if not story_metadata_path.exists():
        return

    meta = pd.read_parquet(story_metadata_path)
    meta["story_id"] = meta["story_id"].astype(str).str.strip()

    if "status" not in meta.columns:
        meta["status"] = "active"
    if "deleted_date" not in meta.columns:
        meta["deleted_date"] = pd.NaT
    if "deleted_by" not in meta.columns:
        meta["deleted_by"] = None

    updated = 0
    for _, evt in delete_events.iterrows():
        sid = str(evt["story_id"])
        mask = meta["story_id"] == sid
        if not mask.any():
            continue

        current_status = meta.loc[mask, "status"].iloc[0]
        current_date = meta.loc[mask, "deleted_date"].iloc[0]
        new_date = evt["delete_date"]

        # Update if: not yet marked as deleted, or our date is more precise (earlier)
        # Normalize both dates to datetime.date for comparison
        if hasattr(new_date, 'date'):
            new_date = new_date.date()
        if hasattr(current_date, 'date'):
            current_date_cmp = current_date.date()
        elif isinstance(current_date, str):
            current_date_cmp = pd.Timestamp(current_date).date()
        else:
            current_date_cmp = current_date
        if current_status != "deleted" or pd.isna(current_date) or new_date < current_date_cmp:
            meta.loc[mask, "status"] = "deleted"
            meta.loc[mask, "deleted_date"] = new_date
            meta.loc[mask, "deleted_by"] = evt["deleted_by"]
            updated += 1
            if current_status == "deleted" and not pd.isna(current_date):
                log(f"    Corrected story {sid}: {current_date} → {new_date} (from App Insights)")
            else:
                log(f"    Marked story {sid} as deleted: {new_date} (from App Insights)")

    if updated > 0:
        meta["deleted_date"] = pd.to_datetime(meta["deleted_date"], errors="coerce").dt.date
        meta.to_parquet(story_metadata_path, index=False)
        log(f"  Updated {updated} story deletion(s) in {story_metadata_path.name}")

        # Refresh the in-memory events table with corrected dates
        for _, evt in delete_events.iterrows():
            sid = str(evt["story_id"])
            if 'story_deleted_date' in evt_cols:
                con.execute(f"""
                    UPDATE events SET story_deleted_date = '{evt['delete_date']}'
                    WHERE story_id = '{sid}'
                """)
            if 'story_status' in evt_cols:
                con.execute(f"""
                    UPDATE events SET story_status = 'deleted'
                    WHERE story_id = '{sid}'
                """)


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
    # Cast to VARCHAR, strip trailing .0 from Excel float conversion, then zero-pad to 8 digits
    # e.g. "01234567.0" → "1234567.0" → "1234567" → "01234567"
    if gpn_candidates:
        gpn_expr = f"LPAD(REGEXP_REPLACE(CAST(COALESCE({', '.join(gpn_candidates)}) AS VARCHAR), '\\.0$', ''), 8, '0')"
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
            'gcrs_unit_desc': 'hr_unit',
            'gcrs_area_desc': 'hr_area',
            'gcrs_sector_desc': 'hr_sector',
            'gcrs_segment_desc': 'hr_segment',
            'gcrs_function_desc': 'hr_function',
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
            -- Format: "<story_id><Action>" e.g. "15Like", "15Read full story"
            NULLIF(regexp_extract(r."{link_label_col}", '^(\\d+)', 1), '') as story_id,
            CASE
                WHEN r."{link_label_col}" ILIKE '%Share your story%' THEN 'Open Form'
                WHEN r."{link_label_col}" ILIKE '%Submit%' THEN 'Submit'
                WHEN r."{link_label_col}" ILIKE '%Send Invite%' THEN 'Send Invite'
                WHEN r."{link_label_col}" ILIKE '%Invite your colleagues%' THEN 'Open Invite'
                WHEN r."{link_label_col}" ILIKE '%Cancel%' THEN 'Cancel'
                WHEN regexp_matches(r."{link_label_col}", '^\\d+Yes$') THEN 'Delete'
                WHEN r."{link_label_col}" ILIKE '%Read%' THEN 'Read'
                WHEN r."{link_label_col}" ILIKE '%like%' THEN 'Like'
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
            -- CET timestamp (convert UTC to Europe/Berlin)
            ((r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::TIMESTAMP as timestamp_cet,
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


def anonymize_events_table(con):
    """Replace plain GPN/email in the events table with hashed/dropped values.

    Called immediately after add_calculated_columns (HR join complete) so that
    PII is never written to any output file or persisted in the database.
    - gpn     -> person_hash  (SHA-256)
    - CP_GPN  -> Person_Hash  (SHA-256)
    - email   -> dropped
    - CP_Email-> dropped
    """
    log("Anonymizing events table (hash GPN, drop email)...")
    schema = con.execute("DESCRIBE events").df()
    all_cols = schema['column_name'].tolist()

    hash_columns = {'gpn', 'CP_GPN'}
    drop_columns = {
        'email', 'CP_Email',
        # Unused CP_* columns — not referenced in PowerBI or dashboard
        'CP_ContentType', 'CP_FileName_Label', 'CP_Filetype_Label',
        'CP_FileType_Label', 'CP_Link_ancestors', 'CP_Link_Type',
        'CP_Link_type', 'CP_NewsCategory',
        'CP_pageId', 'CP_PageStatus', 'CP_PageURL',
        'CP_TargetOrganisation', 'CP_TargetRegion',
    }

    cols_to_hash = [c for c in all_cols if c in hash_columns]
    cols_to_drop = [c for c in all_cols if c in drop_columns]

    select_parts = []
    renamed_cols = []
    for c in all_cols:
        if c in drop_columns:
            continue
        if c in hash_columns:
            alias = c.replace('gpn', 'person_hash').replace('GPN', 'Person_Hash')
            select_parts.append(f"sha256(CAST({c} AS VARCHAR))::VARCHAR AS {alias}")
        elif c.startswith('hr_'):
            alias = 'visitor_' + c[3:]
            select_parts.append(f'"{c}" AS {alias}')
            renamed_cols.append(f"{c} -> {alias}")
        else:
            select_parts.append(c)

    select_sql = ', '.join(select_parts)
    con.execute(f"CREATE OR REPLACE TABLE events AS SELECT {select_sql} FROM events")

    changes = []
    if cols_to_hash:
        changes.append(f"hashed: {', '.join(cols_to_hash)}")
    if cols_to_drop:
        changes.append(f"dropped: {', '.join(cols_to_drop)}")
    if renamed_cols:
        changes.append(f"renamed: {', '.join(renamed_cols)}")
    log(f"  {'; '.join(changes) if changes else 'no PII columns found'}")


def export_parquet_files(con, output_dir):
    """Export all Parquet files for reporting."""
    log("Exporting Parquet files...")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Anonymized export — only events with a known story (matched in story_metadata)
    anonymized_file = output_dir / 'events_anonymized.parquet'
    if anonymized_file.exists():
        anonymized_file.unlink()

    # Check if story_title column exists (set by story metadata join)
    evt_cols = [r[0] for r in con.execute("DESCRIBE events").fetchall()]
    has_story_title = 'story_title' in evt_cols

    # Check if story_deleted_date column exists (set by soft-delete metadata)
    has_deleted_date = 'story_deleted_date' in evt_cols

    if has_story_title:
        # Keep: events with known story metadata OR non-story actions (invite, form, cancel)
        # For deleted stories: only include events up to the deleted_date
        # Exclude: "Other" action type and story events without metadata
        total_before = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]

        deleted_filter = ""
        if has_deleted_date:
            deleted_filter = "AND (story_deleted_date IS NULL OR CAST(timestamp AS DATE) <= story_deleted_date)"

        con.execute(f"""
            COPY (
                SELECT * FROM events
                WHERE action_type != 'Other'
                  {deleted_filter}
                  AND (
                    (story_id IS NOT NULL AND story_title IS NOT NULL)
                    OR action_type IN ('Open Form', 'Submit', 'Cancel', 'Send Invite', 'Open Invite', 'Delete')
                  )
            )
            TO '{anonymized_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        row_count = con.execute(f"SELECT COUNT(*) as n FROM read_parquet('{anonymized_file}')").df()['n'][0]
        excluded = total_before - row_count
        log(f"  Filtered: {row_count:,} rows kept, {excluded:,} excluded (Other + unmatched + post-delete)")

        # Export all excluded events to XLSX for transparency
        has_deleted = 'story_deleted_date' in evt_cols
        ll_col = next((c for c in ['CP_Link_label', 'CP_link_label'] if c in evt_cols), None)

        # Build exclusion reason for every event
        reason_case = """
            CASE
                WHEN action_type = 'Other' THEN 'Other'
                WHEN story_id IS NOT NULL AND story_title IS NULL THEN 'No story title'
                WHEN story_deleted_date IS NOT NULL
                     AND CAST(timestamp AS DATE) > story_deleted_date THEN 'Post-delete'
                WHEN story_id IS NULL
                     AND action_type NOT IN ('Open Form','Submit','Cancel','Send Invite','Open Invite','Delete')
                     THEN 'No story ID'
                ELSE NULL
            END
        """ if has_deleted else """
            CASE
                WHEN action_type = 'Other' THEN 'Other'
                WHEN story_id IS NOT NULL AND story_title IS NULL THEN 'No story title'
                WHEN story_id IS NULL
                     AND action_type NOT IN ('Open Form','Submit','Cancel','Send Invite','Open Invite','Delete')
                     THEN 'No story ID'
                ELSE NULL
            END
        """

        # Available metadata columns to include
        meta_cols = [c for c in ['story_text', 'story_keys', 'story_deleted_date', 'story_title']
                     if c in evt_cols]
        meta_select = ', '.join(f'CAST(MAX({c}) AS VARCHAR) as {c}' for c in meta_cols)
        meta_select_prefix = ', ' + meta_select if meta_select else ''
        ll_select = f', COALESCE("{ll_col}", \'(NULL)\') as link_label' if ll_col else ''
        ll_group = f', "{ll_col}"' if ll_col else ''
        ll_alias_group = ', link_label' if ll_col else ''

        # Summary sheet: one row per exclusion_reason
        summary_df = con.execute(f"""
            WITH excluded AS (
                SELECT *, {reason_case} as exclusion_reason
                FROM events
            )
            SELECT
                exclusion_reason,
                COUNT(*) as event_count,
                COUNT(DISTINCT story_id) as unique_stories,
                COUNT(DISTINCT person_hash) as unique_users
            FROM excluded
            WHERE exclusion_reason IS NOT NULL
            GROUP BY exclusion_reason
            ORDER BY event_count DESC
        """).df()

        # Detail sheet: per story_id and action_type
        detail_df = con.execute(f"""
            WITH excluded AS (
                SELECT *, {reason_case} as exclusion_reason
                FROM events
            )
            SELECT
                exclusion_reason,
                COALESCE(story_id, '(none)') as story_id,
                action_type,
                COUNT(*) as event_count,
                COUNT(DISTINCT person_hash) as unique_users,
                CAST(MIN(timestamp) AS VARCHAR) as first_event,
                CAST(MAX(timestamp) AS VARCHAR) as last_event
            FROM excluded
            WHERE exclusion_reason IS NOT NULL
            GROUP BY exclusion_reason, story_id, action_type
            ORDER BY exclusion_reason, event_count DESC
        """).df()

        # Enrich detail rows with available metadata per story_id
        if meta_cols:
            meta_df = con.execute(f"""
                SELECT COALESCE(story_id, '(none)') as story_id
                    {meta_select_prefix}
                FROM events
                WHERE story_id IS NOT NULL
                GROUP BY story_id
            """).df()
            detail_df = detail_df.merge(meta_df, on='story_id', how='left')

        # Other labels sheet: show original CP_Link_label values for "Other" events
        other_labels_df = None
        if ll_col:
            other_labels_df = con.execute(f"""
                SELECT
                    COALESCE("{ll_col}", '(NULL)') as link_label,
                    COUNT(*) as event_count,
                    COUNT(DISTINCT person_hash) as unique_users
                FROM events
                WHERE action_type = 'Other'
                GROUP BY "{ll_col}"
                ORDER BY event_count DESC
            """).df()

        if len(summary_df) > 0:
            excluded_xlsx = output_dir / 'excluded_events.xlsx'
            with pd.ExcelWriter(excluded_xlsx, engine='openpyxl') as writer:
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                detail_df.to_excel(writer, sheet_name='Detail', index=False)
                if other_labels_df is not None and len(other_labels_df) > 0:
                    other_labels_df.to_excel(writer, sheet_name='Other Labels', index=False)
            total_excluded = summary_df['event_count'].sum()
            log(f"  excluded_events.xlsx ({total_excluded:,} events across {len(summary_df)} categories)")
    else:
        con.execute(f"COPY events TO '{anonymized_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
        row_count = con.execute(f"SELECT COUNT(*) as n FROM read_parquet('{anonymized_file}')").df()['n'][0]

    size_mb = os.path.getsize(anonymized_file) / (1024 * 1024)
    log(f"  events_anonymized.parquet ({row_count:,} rows, {size_mb:.1f} MB)")


def print_summary(con, output_dir=None):
    """Print comprehensive processing summary."""
    log("")
    log("=" * 64)
    log("  PROCESSING SUMMARY")
    log("=" * 64)

    # --- Processed files manifest ---
    tables = con.execute("SHOW TABLES").df()['name'].tolist()
    if 'processed_files' in tables:
        manifest = con.execute("""
            SELECT filename, row_count, processed_at, date_suffix
            FROM processed_files
            ORDER BY date_suffix, filename
        """).df()
        if len(manifest) > 0:
            log("\n  PROCESSED FILES")
            log("  " + "-" * 60)
            for _, row in manifest.iterrows():
                ts = str(row['processed_at'])[:19] if row['processed_at'] else '?'
                rows = f"{int(row['row_count']):,}" if row['row_count'] else '?'
                log(f"    {row['filename']:<45s} {rows:>8s} rows  (at {ts})")

    # --- DuckDB tables ---
    log("\n  DATABASE TABLES")
    log("  " + "-" * 60)
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
            COUNT(DISTINCT person_hash) as unique_gpns
        FROM events
    """).df().iloc[0]

    log("\n  DATA OVERVIEW")
    log("  " + "-" * 60)
    if overview['first_date'] is not None:
        log(f"    Date range:        {overview['first_date']} to {overview['last_date']} ({int(overview['days'])} days)")
    log(f"    Total events:      {int(overview['total_events']):,}")
    log(f"    Unique users:      {int(overview['unique_users']):,}")
    log(f"    Unique sessions:   {int(overview['unique_sessions']):,}")
    log(f"    Unique persons:    {int(overview['unique_gpns']):,}")

    # --- HR join coverage ---
    events_cols = con.execute("DESCRIBE events").df()['column_name'].tolist()
    if 'visitor_division' in events_cols:
        hr_coverage = con.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(visitor_division) as with_hr_data,
                COUNT(person_hash) as with_gpn
            FROM events
        """).df().iloc[0]

        total = int(hr_coverage['total'])
        with_hr = int(hr_coverage['with_hr_data'])
        with_gpn = int(hr_coverage['with_gpn'])

        log("\n  HR JOIN COVERAGE")
        log("  " + "-" * 60)
        log(f"    Events with person hash:{with_gpn:>7,} / {total:,}  ({100.0 * with_gpn / total if total > 0 else 0:.1f}%)")
        log(f"    Events with HR data:   {with_hr:>8,} / {total:,}  ({100.0 * with_hr / total if total > 0 else 0:.1f}%)")

        divisions = con.execute("""
            SELECT visitor_division, COUNT(*) as cnt
            FROM events
            WHERE visitor_division IS NOT NULL
            GROUP BY visitor_division
            ORDER BY cnt DESC
            LIMIT 10
        """).df()
        if len(divisions) > 0:
            log("\n    Top divisions:")
            for _, row in divisions.iterrows():
                log(f"      {str(row['visitor_division']):<40s} {int(row['cnt']):>8,}")

        # Show unmatched persons (have person_hash but no HR data)
        if with_gpn > with_hr:
            unmatched = con.execute("""
                SELECT person_hash, COUNT(*) as cnt
                FROM events
                WHERE person_hash IS NOT NULL AND visitor_division IS NULL
                GROUP BY person_hash
                ORDER BY cnt DESC
                LIMIT 15
            """).df()
            if len(unmatched) > 0:
                log(f"\n    Unmatched persons ({with_gpn - with_hr:,} events from {len(unmatched)} shown, may be more):")
                for _, row in unmatched.iterrows():
                    log(f"      {row['person_hash'][:16]}… ({int(row['cnt']):,} events)")

    # --- Field coverage ---
    log("\n  FIELD COVERAGE (non-null values)")
    log("  " + "-" * 60)
    total = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    check_fields = ['person_hash', 'session_id', 'user_id', 'story_id', 'action_type']
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

        # Show exclusion breakdown so terminal totals reconcile with report
        if 'story_title' in events_cols:
            total = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]

            no_title_count = con.execute("""
                SELECT COUNT(*) FROM events
                WHERE action_type != 'Other'
                  AND story_id IS NOT NULL
                  AND story_title IS NULL
            """).fetchone()[0]

            post_delete_count = 0
            no_story_id_count = 0
            if 'story_deleted_date' in events_cols:
                post_delete_count = con.execute("""
                    SELECT COUNT(*) FROM events
                    WHERE action_type != 'Other'
                      AND story_id IS NOT NULL
                      AND story_title IS NOT NULL
                      AND story_deleted_date IS NOT NULL
                      AND CAST(timestamp AS DATE) > story_deleted_date
                """).fetchone()[0]

            no_story_id_count = con.execute("""
                SELECT COUNT(*) FROM events
                WHERE action_type != 'Other'
                  AND story_id IS NULL
                  AND action_type NOT IN ('Open Form', 'Submit', 'Cancel', 'Send Invite', 'Open Invite', 'Delete')
            """).fetchone()[0]

            other_count_for_summary = con.execute("SELECT COUNT(*) FROM events WHERE action_type = 'Other'").fetchone()[0]
            total_excluded = other_count_for_summary + no_title_count + post_delete_count + no_story_id_count
            reported = total - total_excluded

            log("\n  EXCLUSION BREAKDOWN")
            log("  " + "-" * 60)
            if no_title_count > 0:
                pct = 100.0 * no_title_count / total
                log(f"    {'No story title':<35s} {no_title_count:>8,}  ({pct:.1f}%)")
            if post_delete_count > 0:
                pct = 100.0 * post_delete_count / total
                log(f"    {'Post-delete':<35s} {post_delete_count:>8,}  ({pct:.1f}%)")
            if no_story_id_count > 0:
                pct = 100.0 * no_story_id_count / total
                log(f"    {'No story ID':<35s} {no_story_id_count:>8,}  ({pct:.1f}%)")
            log(f"    {'Other':<35s} {other_count_for_summary:>8,}  ({100.0 * other_count_for_summary / total:.1f}%)")
            log("  " + "-" * 60)
            log(f"    {'Total excluded':<35s} {total_excluded:>8,}")
            log(f"    {'Reported clicks':<35s} {reported:>8,}  (= {total:,} - {total_excluded:,})")

        # Show sample "Other" labels for refinement
        other_count = con.execute("SELECT COUNT(*) FROM events WHERE action_type = 'Other'").fetchone()[0]
        if other_count > 0:
            # Find the link label column in events
            ll_col = next((c for c in ['CP_Link_label', 'CP_link_label'] if c in events_cols), None)
            if ll_col:
                other_samples = con.execute(f"""
                    SELECT COALESCE("{ll_col}", '(NULL)') as label, COUNT(*) as cnt
                    FROM events
                    WHERE action_type = 'Other'
                    GROUP BY 1
                    ORDER BY cnt DESC
                """).df()
                if len(other_samples) > 0:
                    log(f"\n    All 'Other' labels ({other_count:,} events, {len(other_samples)} distinct):")
                    for _, row in other_samples.iterrows():
                        label_preview = str(row['label'])[:60]
                        log(f"      {label_preview:<60s} {int(row['cnt']):>6,}")

    # --- Story engagement ---
    if 'story_id' in events_cols and 'action_type' in events_cols:
        story_stats = con.execute("""
            SELECT
                COUNT(DISTINCT story_id) as unique_stories,
                COUNT(CASE WHEN action_type = 'Read' THEN 1 END) as reads,
                COUNT(CASE WHEN action_type = 'Like' THEN 1 END) as likes,
                COUNT(CASE WHEN action_type = 'Open Form' THEN 1 END) as open_forms,
                COUNT(CASE WHEN action_type = 'Submit' THEN 1 END) as submits,
                COUNT(CASE WHEN action_type = 'Cancel' THEN 1 END) as cancels
            FROM events
            WHERE story_id IS NOT NULL AND story_id != ''
        """).df().iloc[0]

        log("\n  STORY ENGAGEMENT")
        log("  " + "-" * 60)
        log(f"    Unique stories:    {int(story_stats['unique_stories']):,}")
        log(f"    Reads:             {int(story_stats['reads']):,}")
        log(f"    Likes:             {int(story_stats['likes']):,}")

        # Top stories by reads
        has_title = 'story_title' in events_cols
        title_select = ", MAX(story_title) as story_title" if has_title else ""
        top_stories = con.execute(f"""
            SELECT
                story_id
                {title_select},
                COUNT(CASE WHEN action_type = 'Read' THEN 1 END) as reads,
                COUNT(DISTINCT person_hash) as unique_readers,
                COUNT(CASE WHEN action_type = 'Like' THEN 1 END) as likes
            FROM events
            WHERE story_id IS NOT NULL AND story_id != ''
            GROUP BY story_id
            ORDER BY reads DESC
            LIMIT 10
        """).df()

        # Build story_id -> label lookup: story_title -> author_email -> story_id
        story_labels = {}
        try:
            st_cols = [r[0] for r in con.execute("DESCRIBE story_titles").fetchall()]
            label_cols = []
            if 'story_title' in st_cols:
                label_cols.append("story_title")
            if 'author_email' in st_cols:
                label_cols.append("author_email")
            if label_cols:
                meta = con.execute(f"SELECT story_id, {', '.join(label_cols)} FROM story_titles").df()
                for _, m in meta.iterrows():
                    label = None
                    if 'story_title' in label_cols and m.get('story_title'):
                        label = str(m['story_title'])
                    elif 'author_email' in label_cols and m.get('author_email'):
                        label = str(m['author_email'])
                    if label:
                        story_labels[str(m['story_id'])] = label[:28]
        except Exception:
            pass  # story_titles table may not exist

        if len(top_stories) > 0:
            log("\n    Top stories by reads:")
            if story_labels:
                log(f"    {'Story ID':<12s} {'Label':<30s} {'Reads':>8s} {'Readers':>8s} {'Likes':>8s}")
                for _, row in top_stories.iterrows():
                    label = story_labels.get(str(row['story_id']), '')
                    log(f"    {str(row['story_id']):<12s} {label:<30s} {int(row['reads']):>8,} {int(row['unique_readers']):>8,} {int(row['likes']):>8,}")
            else:
                log(f"    {'Story ID':<12s} {'Reads':>8s} {'Readers':>8s} {'Likes':>8s}")
                for _, row in top_stories.iterrows():
                    log(f"    {str(row['story_id']):<12s} {int(row['reads']):>8,} {int(row['unique_readers']):>8,} {int(row['likes']):>8,}")

    # --- Deleted stories overview ---
    # Read from parquet directly (story_titles table may already be dropped or stale)
    story_metadata_path = Path(output_dir) / 'story_metadata.parquet' if output_dir else None
    if story_metadata_path and story_metadata_path.exists():
        meta_df = pd.read_parquet(story_metadata_path)
        if 'status' in meta_df.columns and 'deleted_date' in meta_df.columns:
            deleted_stories = meta_df[meta_df['status'] == 'deleted'].sort_values(
                'deleted_date', ascending=False)

            if len(deleted_stories) > 0:
                has_deleted_by = 'deleted_by' in deleted_stories.columns
                log(f"\n  DELETED STORIES ({len(deleted_stories)} total)")
                log("  " + "-" * 60)
                header = f"    {'Story ID':<10s} {'Title/Author':<28s} {'Created':<12s} {'Deleted':<12s} {'Source':<10s} {'Events':>8s}"
                log(header)
                for _, row in deleted_stories.iterrows():
                    sid = str(row['story_id'])
                    label = str(row.get('story_title') or row.get('author_email') or '')[:26]
                    created = str(row.get('created') or '')[:10]
                    deleted = str(row.get('deleted_date') or '')[:10]
                    source = "AppInsight" if (has_deleted_by and pd.notna(row.get('deleted_by'))) else "Metadata"
                    # Count events for this deleted story
                    evt_count = 0
                    if 'story_id' in events_cols:
                        evt_count = con.execute(f"""
                            SELECT COUNT(*) FROM events
                            WHERE story_id = '{sid}'
                        """).fetchone()[0]
                    log(f"    {sid:<10s} {label:<28s} {created:<12s} {deleted:<12s} {source:<10s} {evt_count:>8,}")

    log("\n" + "=" * 64)


def process_campaignwe(input_file=None, full_refresh=False, delete_input=False):
    """
    Main processing function.

    Args:
        input_file:   Specific file to process, or None to auto-detect
        full_refresh: If True, delete DB and reprocess all files
        delete_input: If True, delete each input file after successful processing
    """
    script_dir = Path(__file__).parent
    input_dir = script_dir / 'input'
    data_dir = script_dir / 'data'
    output_dir = script_dir / 'output'
    db_path = data_dir / 'campaignwe.db'

    # HR history parquet from SearchAnalytics
    hr_parquet_path = script_dir.parent / 'SearchAnalytics' / 'output' / 'hr_history.parquet'

    # Story titles from SharePoint
    story_titles_path = output_dir / 'story_metadata.parquet'

    # Create directories
    input_dir.mkdir(parents=True, exist_ok=True)
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

        files_to_process = get_all_input_files(input_dir)
        if not files_to_process:
            log(f"ERROR: No input files found in {input_dir}")
            log("Place your KQL export files (xlsx/csv) in the input/ folder")
            sys.exit(1)
        log(f"Full refresh: processing {len(files_to_process)} files")

        # Connect to DuckDB (fresh DB after deletion)
        con = duckdb.connect(str(db_path))
        ensure_manifest_table(con)

        for input_path in files_to_process:
            log(f"\nProcessing: {input_path.name}")
            file_hash = compute_file_hash(input_path)
            row_count = load_file_to_temp_table(con, input_path)
            log(f"  Loaded {row_count:,} rows")
            upsert_data(con)
            record_processed_file(con, input_path, file_hash, row_count)
            if delete_input:
                input_path.unlink()
                log(f"  Deleted input file: {input_path.name}")

    elif input_file:
        # Force-process a specific file (bypass delta check)
        input_path = Path(input_file)
        if not input_path.exists():
            log(f"ERROR: File not found: {input_file}")
            sys.exit(1)

        con = duckdb.connect(str(db_path))
        ensure_manifest_table(con)

        log(f"\nForce-processing: {input_path.name}")
        file_hash = compute_file_hash(input_path)
        row_count = load_file_to_temp_table(con, input_path)
        log(f"  Loaded {row_count:,} rows")
        upsert_data(con)
        record_processed_file(con, input_path, file_hash, row_count)
        input_path.unlink()
        log(f"  Deleted input file: {input_path.name}")

    else:
        # Default: delta mode — only process new or changed files
        all_files = get_all_input_files(input_dir)
        if not all_files:
            log(f"ERROR: No input files found in {input_dir}")
            log("Place your KQL export files (xlsx/csv) in the input/ folder")
            log("Supported formats: .xlsx, .xls, .csv")
            log("\nFilename format: campaign_export_YYYY_MM_DD.xlsx")
            log("Example filenames:")
            log("  campaign_export_2026_02_25.xlsx")
            log("  campaign_export_2026_02_25.csv")
            sys.exit(1)

        con = duckdb.connect(str(db_path))
        unprocessed = get_unprocessed_files(con, input_dir)

        if not unprocessed:
            log("All files already processed. Nothing new to do.")
            log("Use --full-refresh to reprocess everything.")
            con.close()
            return

        for input_path, file_hash, reason in unprocessed:
            log(f"\nProcessing ({reason}): {input_path.name}")
            row_count = load_file_to_temp_table(con, input_path)
            log(f"  Loaded {row_count:,} rows")
            upsert_data(con)
            record_processed_file(con, input_path, file_hash, row_count)
            if delete_input:
                input_path.unlink()
                log(f"  Deleted input file: {input_path.name}")

    # # TEST DATA — uncomment to inject sample story events for flow validation
    # # Funnel shape: View Prompt (20) > Read (14) > Like (6) > Share (4) > Hide (2)
    # log("\n  Injecting test story events for flow validation...")
    # con.execute("""
    #     INSERT INTO events_raw (timestamp, user_id, session_id, name, CP_GPN, CP_Link_label)
    #     VALUES
    #         -- Story 123 (most popular): 8 VP, 6 Read, 3 Like, 2 Share, 1 Hide = 20 events
    #         ('2026-02-24 09:10:00', 'test-user-1', 'test-sess-01', 'click', '00294573', 'View Prompt story of 123'),
    #         ('2026-02-24 09:11:00', 'test-user-1', 'test-sess-01', 'click', '00294573', 'Read story of 123'),
    #         ('2026-02-24 09:12:00', 'test-user-1', 'test-sess-01', 'click', '00294573', 'Like story of 123'),
    #         ('2026-02-24 10:30:00', 'test-user-2', 'test-sess-02', 'click', '43397977', 'View Prompt story of 123'),
    #         ('2026-02-24 10:31:00', 'test-user-2', 'test-sess-02', 'click', '43397977', 'Read story of 123'),
    #         ('2026-02-24 10:32:00', 'test-user-2', 'test-sess-02', 'click', '43397977', 'Share story of 123'),
    #         ('2026-02-25 08:00:00', 'test-user-3', 'test-sess-03', 'click', '43272388', 'View Prompt story of 123'),
    #         ('2026-02-25 08:01:00', 'test-user-3', 'test-sess-03', 'click', '43272388', 'Read story of 123'),
    #         ('2026-02-25 08:02:00', 'test-user-3', 'test-sess-03', 'click', '43272388', 'Like story of 123'),
    #         ('2026-02-25 14:20:00', 'test-user-4', 'test-sess-04', 'click', '00287943', 'View Prompt story of 123'),
    #         ('2026-02-25 14:21:00', 'test-user-4', 'test-sess-04', 'click', '00287943', 'Read story of 123'),
    #         ('2026-02-25 14:22:00', 'test-user-4', 'test-sess-04', 'click', '00287943', 'Like story of 123'),
    #         ('2026-02-26 11:00:00', 'test-user-1', 'test-sess-05', 'click', '00294573', 'View Prompt story of 123'),
    #         ('2026-02-26 11:01:00', 'test-user-1', 'test-sess-05', 'click', '00294573', 'Read story of 123'),
    #         ('2026-02-26 11:02:00', 'test-user-1', 'test-sess-05', 'click', '00294573', 'Share story of 123'),
    #         ('2026-02-26 16:45:00', 'test-user-3', 'test-sess-06', 'click', '43272388', 'View Prompt story of 123'),
    #         ('2026-02-26 16:46:00', 'test-user-3', 'test-sess-06', 'click', '43272388', 'Read story of 123'),
    #         ('2026-02-26 16:47:00', 'test-user-3', 'test-sess-06', 'click', '43272388', 'hide story of 123'),
    #         ('2026-02-24 17:00:00', 'test-user-4', 'test-sess-17', 'click', '00287943', 'View Prompt story of 123'),
    #         ('2026-02-25 17:00:00', 'test-user-2', 'test-sess-18', 'click', '43397977', 'View Prompt story of 123'),
    #         -- Story 456 (medium): 7 VP, 5 Read, 2 Like, 1 Share, 1 Hide = 16 events
    #         ('2026-02-24 11:00:00', 'test-user-2', 'test-sess-07', 'click', '43397977', 'View Prompt story of 456'),
    #         ('2026-02-24 11:01:00', 'test-user-2', 'test-sess-07', 'click', '43397977', 'Read story of 456'),
    #         ('2026-02-24 11:02:00', 'test-user-2', 'test-sess-07', 'click', '43397977', 'Like story of 456'),
    #         ('2026-02-24 13:30:00', 'test-user-4', 'test-sess-08', 'click', '00287943', 'View Prompt story of 456'),
    #         ('2026-02-24 13:31:00', 'test-user-4', 'test-sess-08', 'click', '00287943', 'Read story of 456'),
    #         ('2026-02-24 13:32:00', 'test-user-4', 'test-sess-08', 'click', '00287943', 'Share story of 456'),
    #         ('2026-02-25 09:15:00', 'test-user-1', 'test-sess-09', 'click', '00294573', 'View Prompt story of 456'),
    #         ('2026-02-25 09:16:00', 'test-user-1', 'test-sess-09', 'click', '00294573', 'Read story of 456'),
    #         ('2026-02-25 09:17:00', 'test-user-1', 'test-sess-09', 'click', '00294573', 'Like story of 456'),
    #         ('2026-02-26 10:00:00', 'test-user-3', 'test-sess-10', 'click', '43272388', 'View Prompt story of 456'),
    #         ('2026-02-26 10:01:00', 'test-user-3', 'test-sess-10', 'click', '43272388', 'Read story of 456'),
    #         ('2026-02-26 15:30:00', 'test-user-2', 'test-sess-11', 'click', '43397977', 'View Prompt story of 456'),
    #         ('2026-02-26 15:31:00', 'test-user-2', 'test-sess-11', 'click', '43397977', 'Read story of 456'),
    #         ('2026-02-26 15:32:00', 'test-user-2', 'test-sess-11', 'click', '43397977', 'hide story of 456'),
    #         ('2026-02-25 16:00:00', 'test-user-3', 'test-sess-19', 'click', '43272388', 'View Prompt story of 456'),
    #         ('2026-02-26 17:00:00', 'test-user-4', 'test-sess-20', 'click', '00287943', 'View Prompt story of 456'),
    #         -- Story 789 (niche): 5 VP, 3 Read, 1 Like, 1 Share, 0 Hide = 10 events
    #         ('2026-02-24 14:00:00', 'test-user-3', 'test-sess-12', 'click', '43272388', 'View Prompt story of 789'),
    #         ('2026-02-24 14:01:00', 'test-user-3', 'test-sess-12', 'click', '43272388', 'Read story of 789'),
    #         ('2026-02-24 14:02:00', 'test-user-3', 'test-sess-12', 'click', '43272388', 'Share story of 789'),
    #         ('2026-02-24 15:45:00', 'test-user-1', 'test-sess-13', 'click', '00294573', 'View Prompt story of 789'),
    #         ('2026-02-24 15:46:00', 'test-user-1', 'test-sess-13', 'click', '00294573', 'Read story of 789'),
    #         ('2026-02-24 15:47:00', 'test-user-1', 'test-sess-13', 'click', '00294573', 'Like story of 789'),
    #         ('2026-02-25 10:30:00', 'test-user-4', 'test-sess-14', 'click', '00287943', 'View Prompt story of 789'),
    #         ('2026-02-25 10:31:00', 'test-user-4', 'test-sess-14', 'click', '00287943', 'Read story of 789'),
    #         ('2026-02-26 09:00:00', 'test-user-2', 'test-sess-15', 'click', '43397977', 'View Prompt story of 789'),
    #         ('2026-02-26 13:00:00', 'test-user-4', 'test-sess-16', 'click', '00287943', 'View Prompt story of 789');
    # """)
    # log("  Added 46 test events for stories 123, 456, 789")

    # Load HR history for GPN-based join
    has_hr_history = load_hr_history(con, hr_parquet_path)

    # Add calculated columns (with HR join if available)
    add_calculated_columns(con, has_hr_history=has_hr_history)

    # Anonymize: hash GPN -> person_hash, drop email (HR join already complete)
    anonymize_events_table(con)

    # Load story metadata for story_id -> story_text/story_title + keys lookup
    has_story_titles = load_story_titles(con, story_titles_path)
    if has_story_titles:
        # Check which columns are available in story_titles
        st_cols = [r[0] for r in con.execute("DESCRIBE story_titles").fetchall()]
        has_keys = 'keys' in st_cols

        # Map story_text (full story body)
        if 'story_text' in st_cols:
            con.execute("""
                ALTER TABLE events ADD COLUMN IF NOT EXISTS story_text VARCHAR;
                UPDATE events SET story_text = st.story_text
                FROM story_titles st WHERE events.story_id = st.story_id;
            """)
        # Map story_title (short title — added by dev team when available)
        if 'story_title' in st_cols:
            con.execute("""
                ALTER TABLE events ADD COLUMN IF NOT EXISTS story_title VARCHAR;
                UPDATE events SET story_title = st.story_title
                FROM story_titles st WHERE events.story_id = st.story_id;
            """)
        if has_keys:
            con.execute("""
                ALTER TABLE events ADD COLUMN IF NOT EXISTS story_keys VARCHAR;
                UPDATE events SET story_keys = st.keys
                FROM story_titles st WHERE events.story_id = st.story_id;
            """)
            # Split comma-separated keys into story_key1, story_key2, story_key3
            has_key_cols = 'story_key1' in st_cols
            if has_key_cols:
                for k in ['story_key1', 'story_key2', 'story_key3']:
                    con.execute(f"""
                        ALTER TABLE events ADD COLUMN IF NOT EXISTS {k} VARCHAR;
                        UPDATE events SET {k} = st.{k}
                        FROM story_titles st WHERE events.story_id = st.story_id;
                    """)
        # Map story status and deleted_date (soft-delete support)
        if 'status' in st_cols:
            con.execute("""
                ALTER TABLE events ADD COLUMN IF NOT EXISTS story_status VARCHAR;
                UPDATE events SET story_status = st.status
                FROM story_titles st WHERE events.story_id = st.story_id;
            """)
        if 'deleted_date' in st_cols:
            con.execute("""
                ALTER TABLE events ADD COLUMN IF NOT EXISTS story_deleted_date DATE;
                UPDATE events SET story_deleted_date = st.deleted_date
                FROM story_titles st WHERE events.story_id = st.story_id;
            """)

        has_st_text = 'story_text' in st_cols
        has_st_title = 'story_title' in st_cols
        if has_st_text and has_st_title:
            match_where = "story_text IS NOT NULL OR story_title IS NOT NULL"
        elif has_st_text:
            match_where = "story_text IS NOT NULL"
        elif has_st_title:
            match_where = "story_title IS NOT NULL"
        else:
            match_where = None
        matched = con.execute(f"""
            SELECT COUNT(DISTINCT story_id) FROM events
            WHERE story_id IS NOT NULL AND ({match_where})
        """).fetchone()[0] if match_where else 0
        total = con.execute("""
            SELECT COUNT(DISTINCT story_id) FROM events
            WHERE story_id IS NOT NULL
        """).fetchone()[0]
        log(f"  Matched {matched}/{total} story IDs to metadata" +
            (" (with keys)" if has_keys else ""))

        # Diagnostic: show which event story_ids matched/missed
        diag = con.execute("""
            SELECT DISTINCT e.story_id, e.story_title,
                   CASE WHEN e.story_title IS NOT NULL THEN 'OK' ELSE 'MISS' END as status
            FROM events e
            WHERE e.story_id IS NOT NULL
            ORDER BY e.story_id
        """).fetchall()
        for sid, title, status in diag:
            log(f"    events join: id={sid!r} title={title!r} [{status}]")

    # Correct deleted_date in story_metadata using Delete events from App Insights
    if has_story_titles:
        correct_deleted_dates_from_events(con, story_titles_path)

    # Export Parquet files
    export_parquet_files(con, output_dir)

    # Print summary
    print_summary(con, output_dir)

    # Cleanup: drop tables that are re-created each run to reduce DB size
    # hr_history is re-loaded from parquet each run and only needed during the join
    # events_raw is superseded by the enriched 'events' table
    log("\nCleaning up intermediate tables...")
    db_size_before = os.path.getsize(db_path) / (1024 * 1024)
    con.execute("DROP TABLE IF EXISTS hr_history")
    con.execute("DROP TABLE IF EXISTS story_titles")
    con.execute("DROP TABLE IF EXISTS events_raw")
    con.execute("VACUUM")
    con.execute("CHECKPOINT")
    db_size_after = os.path.getsize(db_path) / (1024 * 1024)
    log(f"  Dropped intermediate tables (hr_history, story_titles, events_raw), vacuumed database")
    log(f"  Database size: {db_size_before:.1f} MB -> {db_size_after:.1f} MB")

    log(f"\nDatabase: {db_path}")
    log(f"Parquet files: {output_dir}")

    con.close()
    log("\nDone!")


if __name__ == "__main__":
    full_refresh = '--full-refresh' in sys.argv
    delete_input = '--delete-input' in sys.argv

    input_file = None
    for arg in sys.argv[1:]:
        if not arg.startswith('--'):
            input_file = arg
            break

    if len(sys.argv) == 1:
        print(__doc__)
        print("\nNo arguments provided - processing new/changed files (delta mode)\n")

    process_campaignwe(input_file=input_file, full_refresh=full_refresh, delete_input=delete_input)
