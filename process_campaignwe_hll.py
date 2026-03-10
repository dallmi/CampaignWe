#!/usr/bin/env python3
"""
CampaignWe HyperLogLog Sketch Pipeline

Alternative anonymization pipeline that pre-aggregates click event data into
HyperLogLog sketches at processing time. No individual token (GPN, email,
person_hash) is ever written to disk — only dimension columns, event counts,
and serialized HLL sketches.

Purpose: compare UV accuracy of HLL-based true anonymization against the
SHA-256 pseudonymization approach in process_campaignwe.py.

Usage:
    python process_campaignwe_hll.py             # process input/ → events_hll.parquet
    python process_campaignwe_hll.py --compare   # also compare against events_anonymized.parquet

Input:
    input/*.xlsx / *.csv   — same KQL export files as the main pipeline
    ../SearchAnalytics/output/hr_history.parquet

Output:
    output/events_hll.parquet
        Columns: session_date, story_id, action_type,
                 visitor_division, visitor_unit, visitor_area, visitor_sector, visitor_region,
                 event_count, uv_sketch (BLOB)
        No GPN, no email, no person_hash.

    output/events_hll_uv.parquet
        Single-dimension UV aggregates for the HTML comparison dashboard.

    output/events_hll_powerbi.parquet
        Pre-computed UV for all 32 combinations of:
            month × story_id × action_type × visitor_division × visitor_region
        Power BI fallback — no person_hash required.

Pre-aggregation grain:
    session_date × story_id × action_type ×
    visitor_division × visitor_unit × visitor_area × visitor_sector × visitor_region

HLL parameters: datasketch.HyperLogLog(p=12) — ~1.6% std error, 4096 registers.

NOTE: Run this script BEFORE process_campaignwe.py, which deletes input files
      after processing.
"""

import sys
import os
import re
import glob
import hashlib
import pickle
import itertools
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from datasketch import HyperLogLog


# ---------------------------------------------------------------------------
# Utilities (identical to process_campaignwe.py)
# ---------------------------------------------------------------------------

def log(message):
    """Print timestamped log message."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def extract_date_from_filename(filepath):
    """Extract date from filename with format _YYYY_MM_DD."""
    filename = Path(filepath).stem
    match = re.search(r'_(\d{4})_(\d{2})_(\d{2})$', filename)
    if match:
        try:
            year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
            return datetime(year, month, day).date()
        except ValueError:
            return None
    return None


def compute_file_hash(filepath):
    """SHA-256 hash of file contents for change detection."""
    h = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def get_all_input_files(input_dir):
    """Get all input files sorted by date in filename (oldest first)."""
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
    """Load a CSV or Excel file into a temporary DuckDB table."""
    con.execute(f"DROP TABLE IF EXISTS {temp_table}")

    if input_path.suffix.lower() in ['.xlsx', '.xls']:
        df_cols = pd.read_excel(input_path, nrows=0)
        all_cols = df_cols.columns.tolist()
        timestamp_cols = [col for col in all_cols if 'timestamp' in col.lower()]
        gpn_cols = [col for col in all_cols if col.lower() in ('cp_gpn', 'gpn')]

        dtype_dict = {}
        if timestamp_cols:
            dtype_dict.update({col: str for col in timestamp_cols})
            log(f"  Reading timestamp columns as strings: {timestamp_cols}")
        if gpn_cols:
            dtype_dict.update({col: str for col in gpn_cols})
            log(f"  Reading GPN columns as strings: {gpn_cols}")

        df = pd.read_excel(input_path, dtype=dtype_dict) if dtype_dict else pd.read_excel(input_path)
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
    rename_map = {'user_Id': 'user_id', 'session_Id': 'session_id', 'timestamp [UTC]': 'timestamp'}
    for old_name, new_name in rename_map.items():
        if old_name in col_names:
            con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{old_name}" TO {new_name}')

    # Convert timestamp-like VARCHAR columns
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    varchar_cols = schema[schema['column_type'] == 'VARCHAR']['column_name'].tolist()

    for col in varchar_cols:
        sample = con.execute(f'SELECT "{col}" FROM {temp_table} WHERE "{col}" IS NOT NULL LIMIT 1').df()
        if len(sample) == 0:
            continue
        val = str(sample.iloc[0, 0])
        fmt = None

        if re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}\.\d+$', val):
            frac_part = val.split('.')[-1]
            fmt = 'TRUNCATE_FRAC' if len(frac_part) > 6 else '%d/%m/%Y %H:%M:%S.%f'
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
                        CASE WHEN "{col}" LIKE '%.%'
                             THEN SUBSTRING("{col}", 1, POSITION('.' IN "{col}") + 6)
                             ELSE "{col}" END,
                        '%d/%m/%Y %H:%M:%S.%f')
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

    # Fallback TRY_CAST for remaining timestamp VARCHAR
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    for _, row in schema.iterrows():
        col, col_type = row['column_name'], row['column_type']
        if col.lower() == 'timestamp' and col_type == 'VARCHAR':
            try:
                con.execute(f'ALTER TABLE {temp_table} ADD COLUMN "{col}_temp" TIMESTAMP')
                con.execute(f'UPDATE {temp_table} SET "{col}_temp" = TRY_CAST("{col}" AS TIMESTAMP)')
                con.execute(f'ALTER TABLE {temp_table} DROP COLUMN "{col}"')
                con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{col}_temp" TO "{col}"')
                log(f"  Converted '{col}' to TIMESTAMP using TRY_CAST")
            except Exception as e:
                log(f"  WARNING: Could not convert '{col}' to TIMESTAMP: {e}")

    return con.execute(f"SELECT COUNT(*) as n FROM {temp_table}").df()['n'][0]


def upsert_data(con, temp_table='temp_import'):
    """Upsert from temp table into events_raw. PK: timestamp + user_id + session_id + name."""
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
              AND events_raw.user_id    = t.user_id
              AND events_raw.session_id = t.session_id
              AND events_raw.name       = t.name
        )
    """)
    deleted_count = before_count - con.execute("SELECT COUNT(*) as n FROM events_raw").df()['n'][0]
    con.execute(f"INSERT INTO events_raw SELECT * FROM {temp_table}")
    after_count = con.execute("SELECT COUNT(*) as n FROM events_raw").df()['n'][0]
    new_rows = after_count - before_count + deleted_count

    if deleted_count > 0:
        log(f"  Updated {deleted_count:,} existing rows, added {new_rows - deleted_count:,} new rows")
    else:
        log(f"  Added {new_rows:,} new rows")
    con.execute(f"DROP TABLE IF EXISTS {temp_table}")


def load_hr_history(con, hr_parquet_path):
    """Load hr_history.parquet for GPN-based joining. Returns True if successful."""
    if not hr_parquet_path.exists():
        log(f"  WARNING: HR history file not found: {hr_parquet_path}")
        return False

    con.execute("DROP TABLE IF EXISTS hr_history")
    con.execute(f"CREATE TABLE hr_history AS SELECT * FROM read_parquet('{hr_parquet_path}')")

    row_count  = con.execute("SELECT COUNT(*) FROM hr_history").fetchone()[0]
    gpn_count  = con.execute("SELECT COUNT(DISTINCT gpn) FROM hr_history").fetchone()[0]
    snap_count = con.execute(
        "SELECT COUNT(DISTINCT (snapshot_year, snapshot_month)) FROM hr_history"
    ).fetchone()[0]
    log(f"  Loaded hr_history: {row_count:,} rows, {gpn_count:,} GPNs, {snap_count} snapshot(s)")
    return True


# ---------------------------------------------------------------------------
# HLL-specific functions
# ---------------------------------------------------------------------------

# HR dimensions included in the pre-aggregation grain.
# Dropped (high-cardinality, low-value for UV): hr_segment, hr_function, hr_ou_code,
# hr_country, hr_management_level, hr_job_family, hr_job_title, hr_cost_center.
HLL_HR_FIELD_MAP = {
    'gcrs_division_desc': 'visitor_division',
    'gcrs_unit_desc':     'visitor_unit',
    'gcrs_area_desc':     'visitor_area',
    'gcrs_sector_desc':   'visitor_sector',
    'work_location_region': 'visitor_region',
}

GRAIN_DIMS = ['session_date', 'story_id', 'action_type',
              'visitor_division', 'visitor_unit', 'visitor_area', 'visitor_sector', 'visitor_region']


def build_events_table(con, has_hr_history=False):
    """Build the enriched events table used for aggregation.

    Adapted from add_calculated_columns() in process_campaignwe.py:
    - Keeps: gpn (for HLL input), story_id, action_type, session_date, HR dims
    - Drops: window functions, timestamp strings, session_key, weekday columns
    """
    log("Building events table for HLL aggregation...")
    con.execute("DROP TABLE IF EXISTS events")
    con.execute("SET TIMEZONE='UTC'")

    schema    = con.execute("DESCRIBE events_raw").df()
    col_names = schema['column_name'].tolist()

    # GPN expression
    gpn_candidates = [c for c in ['CP_GPN', 'CP_gpn', 'GPN', 'gpn'] if c in col_names]
    if gpn_candidates:
        gpn_expr = (f"LPAD(REGEXP_REPLACE(CAST(COALESCE({', '.join(gpn_candidates)}) AS VARCHAR),"
                    f" '\\.0$', ''), 8, '0')")
    else:
        gpn_expr = 'NULL'
    log(f"  GPN resolved from: [{', '.join(gpn_candidates) if gpn_candidates else 'none'}]")

    # HR join
    if has_hr_history and gpn_candidates:
        hr_cols   = con.execute("DESCRIBE hr_history").df()['column_name'].tolist()
        avail_hr  = {src: alias for src, alias in HLL_HR_FIELD_MAP.items() if src in hr_cols}
        log(f"  HR fields available for grain: {list(avail_hr.values())}")

        hr_select_parts = [f'h.{src} as {alias}' for src, alias in avail_hr.items()]
        hr_select_sql   = ', '.join(hr_select_parts) if hr_select_parts else 'NULL as hr_placeholder'

        hr_join_sql = f"""
            LEFT JOIN LATERAL (
                SELECT {hr_select_sql}
                FROM hr_history h
                WHERE CAST(h.gpn AS VARCHAR) = {gpn_expr}
                  AND (h.snapshot_year * 100 + h.snapshot_month)
                      <= (YEAR(r.timestamp) * 100 + MONTH(r.timestamp))
                ORDER BY h.snapshot_year DESC, h.snapshot_month DESC
                LIMIT 1
            ) hr_exact ON true
        """
        hr_fallback_sql = f"""
            LEFT JOIN LATERAL (
                SELECT {hr_select_sql}
                FROM hr_history h
                WHERE CAST(h.gpn AS VARCHAR) = {gpn_expr}
                  AND (h.snapshot_year * 100 + h.snapshot_month)
                      > (YEAR(r.timestamp) * 100 + MONTH(r.timestamp))
                ORDER BY h.snapshot_year ASC, h.snapshot_month ASC
                LIMIT 1
            ) hr_fallback ON true
        """
        hr_coalesce_parts = [
            f"COALESCE(hr_exact.{alias}, hr_fallback.{alias}) as {alias}"
            for src, alias in avail_hr.items()
        ]
        hr_select = ',\n            ' + ', '.join(hr_coalesce_parts) if hr_coalesce_parts else ''
    else:
        hr_join_sql = hr_fallback_sql = hr_select = ''
        avail_hr = {}
        # Emit NULL columns so GRAIN_DIMS are always present
        null_hr = ', '.join(
            f"NULL::VARCHAR as {alias}" for alias in HLL_HR_FIELD_MAP.values()
        )
        hr_select = f',\n            {null_hr}'

    # Story / action_type SQL
    link_label_candidates = [c for c in ['CP_Link_label', 'CP_link_label', 'Link_label'] if c in col_names]
    link_label_col = link_label_candidates[0] if link_label_candidates else None
    if link_label_col:
        story_sql = f"""
            NULLIF(regexp_extract(r."{link_label_col}", '^(\\d+)', 1), '') as story_id,
            CASE
                WHEN r."{link_label_col}" ILIKE '%Share your story%' THEN 'Open Form'
                WHEN r."{link_label_col}" ILIKE '%Submit%'           THEN 'Submit'
                WHEN r."{link_label_col}" ILIKE '%Send Invite%'      THEN 'Send Invite'
                WHEN r."{link_label_col}" ILIKE '%Invite your colleagues%' THEN 'Open Invite'
                WHEN r."{link_label_col}" ILIKE '%Cancel%'           THEN 'Cancel'
                WHEN r."{link_label_col}" ILIKE '%Read%'             THEN 'Read'
                WHEN r."{link_label_col}" ILIKE '%like%'             THEN 'Like'
                ELSE 'Other'
            END as action_type,"""
    else:
        log("  WARNING: No Link_label column — story parsing skipped")
        story_sql = "NULL::VARCHAR as story_id, NULL::VARCHAR as action_type,"

    con.execute(f"""
        CREATE TABLE events AS
        SELECT
            {gpn_expr} as gpn,
            {story_sql}
            DATE_TRUNC('day', (r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::DATE
                as session_date
            {hr_select}
        FROM events_raw r
        {hr_join_sql}
        {hr_fallback_sql}
    """)

    row_count = con.execute("SELECT COUNT(*) as n FROM events").df()['n'][0]
    log(f"  events table built: {row_count:,} rows")


def aggregate_to_hll(con):
    """Group events by grain dimensions and compute one HLL sketch per cell.

    Returns a pandas DataFrame with GRAIN_DIMS + event_count + uv_sketch (bytes).
    GPN values are consumed here and never written to disk.
    """
    log("Aggregating to HLL sketches...")

    # Pull all groups at once — LIST(gpn) gives us the raw GPN values per cell
    grain_cols = ', '.join(GRAIN_DIMS)
    df = con.execute(f"""
        SELECT
            {grain_cols},
            COUNT(*)      AS event_count,
            LIST(gpn)     AS gpns
        FROM events
        GROUP BY {grain_cols}
    """).df()

    sketches = []
    for gpns in df['gpns']:
        hll = HyperLogLog(p=12)
        for gpn in (gpns if gpns is not None else []):
            if gpn is not None:
                hll.update(str(gpn).encode('utf-8'))
        sketches.append(pickle.dumps(hll))

    df['uv_sketch'] = sketches
    df = df.drop(columns=['gpns'])

    total_cells  = len(df)
    nonempty     = (df['uv_sketch'].notna()).sum()
    log(f"  {total_cells:,} cells ({nonempty:,} with sketches)")
    return df


def export_hll_parquet(df, output_dir):
    """Write the pre-aggregated HLL DataFrame to events_hll.parquet."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / 'events_hll.parquet'

    # Convert to pyarrow table — uv_sketch bytes column becomes LARGE_BINARY
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(out_path), compression='snappy')

    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    log(f"  events_hll.parquet: {len(df):,} rows, {size_mb:.1f} MB")
    log(f"  Columns: {', '.join(df.columns.tolist())}")


# ---------------------------------------------------------------------------
# UV aggregates export (browser-consumable, no sketch blobs)
# ---------------------------------------------------------------------------

def export_uv_aggregates(df, output_dir):
    """Pre-compute HLL UV estimates at key dimensions and export for the browser.

    Produces events_hll_uv.parquet with columns:
        dimension  VARCHAR  — 'visitor_division', 'visitor_unit', 'visitor_region', 'story_id',
                               'action_type', 'month', 'overall'
        value      VARCHAR  — the dimension value
        event_count INTEGER — exact event count in that group
        hll_uv     INTEGER  — HLL UV estimate (sketch-merged)

    The browser dashboard loads this alongside events_anonymized.parquet and
    computes exact UV via COUNT(DISTINCT person_hash) for comparison.
    """
    log("Computing UV aggregates for dashboard...")
    records = []

    # Per-dimension aggregations
    for dim in ['visitor_division', 'visitor_unit', 'visitor_region', 'story_id', 'action_type']:
        if dim not in df.columns:
            continue
        for val, grp in df.groupby(dim, dropna=False):
            uv  = _merge_sketches(grp['uv_sketch'])
            evt = int(grp['event_count'].sum())
            records.append({'dimension': dim,
                            'value':     str(val) if pd.notna(val) else None,
                            'event_count': evt,
                            'hll_uv':    uv})

    # Monthly trend — UV active per month (not cumulative)
    df2 = df.copy()
    df2['_month'] = pd.to_datetime(df2['session_date']).dt.to_period('M').astype(str)
    for val, grp in df2.groupby('_month', dropna=False):
        uv  = _merge_sketches(grp['uv_sketch'])
        evt = int(grp['event_count'].sum())
        records.append({'dimension': 'month', 'value': str(val),
                        'event_count': evt, 'hll_uv': uv})

    # Overall total
    records.append({'dimension': 'overall', 'value': 'total',
                    'event_count': int(df['event_count'].sum()),
                    'hll_uv': _merge_sketches(df['uv_sketch'])})

    result = pd.DataFrame(records)
    out_path = output_dir / 'events_hll_uv.parquet'
    result.to_parquet(str(out_path), index=False)
    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    log(f"  events_hll_uv.parquet: {len(result):,} rows, {size_mb:.2f} MB")
    return result


# ---------------------------------------------------------------------------
# Power BI fallback: all dimension-combination UV aggregates
# ---------------------------------------------------------------------------

# Dimensions included in the power-set pre-computation.
# Reduced HR set (visitor_division + visitor_region only) keeps the combination count
# manageable (2^5 = 32) while covering every Power BI slicer combination.
_POWERBI_DIMS = ['month', 'story_id', 'action_type', 'visitor_division', 'visitor_region']


def export_powerbi_aggregates(df, output_dir):
    """Pre-compute HLL UV for every combination of the 5 Power BI dimensions.

    Produces events_hll_powerbi.parquet — a fallback dataset for Power BI that
    requires no person_hash column.  Each row carries pre-computed integer UV
    estimates for one specific grouping of:
        month, story_id, action_type, visitor_division, visitor_region

    Schema:
        month        VARCHAR  — "YYYY-MM" or NULL (dimension not in this grouping)
        story_id     VARCHAR  — or NULL
        action_type  VARCHAR  — or NULL
        visitor_division  VARCHAR  — or NULL
        visitor_region    VARCHAR  — or NULL
        event_count  INTEGER  — exact event total for this cell
        hll_uv       INTEGER  — HLL UV estimate (merged sketches)
        grouping     VARCHAR  — comma-separated list of active dimensions

    NULL in a dimension column means "aggregated across all values of that
    dimension" (not "data had no value").  Use the `grouping` column in DAX
    to identify which dimensions are active for a given row.

    Power BI usage pattern:
        UV Measure = CALCULATE(
            SUMX(
                FILTER(events_hll_powerbi,
                    events_hll_powerbi[grouping] = <active-dim-combo>),
                events_hll_powerbi[hll_uv]
            )
        )
    """
    log("Computing Power BI UV aggregates (all 32 dimension combinations)...")

    df2 = df.copy()
    df2['month'] = pd.to_datetime(df2['session_date']).dt.to_period('M').astype(str)

    records = []
    total_combos = 0

    # Iterate over every subset of _POWERBI_DIMS (2^5 = 32 subsets)
    for r in range(len(_POWERBI_DIMS) + 1):
        for combo in itertools.combinations(_POWERBI_DIMS, r):
            combo = list(combo)
            total_combos += 1

            if not combo:
                # r=0 — overall total
                uv  = _merge_sketches(df2['uv_sketch'])
                evt = int(df2['event_count'].sum())
                row = {d: None for d in _POWERBI_DIMS}
                row.update({'event_count': evt, 'hll_uv': uv, 'grouping': '(overall)'})
                records.append(row)
                continue

            for keys, grp in df2.groupby(combo, dropna=False):
                if len(combo) == 1:
                    keys = (keys,)
                uv  = _merge_sketches(grp['uv_sketch'])
                evt = int(grp['event_count'].sum())
                row = {d: None for d in _POWERBI_DIMS}
                for dim, val in zip(combo, keys):
                    row[dim] = str(val) if pd.notna(val) else None
                row['event_count'] = evt
                row['hll_uv']      = uv
                row['grouping']    = ','.join(combo)
                records.append(row)

    result   = pd.DataFrame(records)
    out_path = output_dir / 'events_hll_powerbi.parquet'
    result.to_parquet(str(out_path), index=False)
    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    log(f"  events_hll_powerbi.parquet: {len(result):,} rows across {total_combos} "
        f"groupings, {size_mb:.2f} MB")
    return result


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

def _merge_sketches(sketch_series):
    """Merge an iterable of pickle-serialised HyperLogLog sketches."""
    merged = HyperLogLog(p=12)
    for blob in sketch_series:
        if blob is not None:
            merged.merge(pickle.loads(blob))
    return int(merged.count())


def compare_pipelines(output_dir):
    """Compare UV counts from events_anonymized.parquet vs events_hll.parquet."""
    anon_path = output_dir / 'events_anonymized.parquet'
    hll_path  = output_dir / 'events_hll.parquet'

    if not anon_path.exists():
        log("  SKIP: events_anonymized.parquet not found — run process_campaignwe.py first")
        return
    if not hll_path.exists():
        log("  SKIP: events_hll.parquet not found")
        return

    log("\nLoading parquet files for comparison...")
    con  = duckdb.connect(':memory:')
    anon = con.execute(f"SELECT * FROM read_parquet('{anon_path}')").df()
    hll  = pd.read_parquet(str(hll_path))

    queries = [
        ("Overall UV",              None,                         None),
        ("UV by action_type",       'action_type',                None),
        ("UV by visitor_division",       'visitor_division',                None),
        ("UV by month",             'month',                      None),
        ("UV by story_id (top 10)", 'story_id',                   10),
        ("UV by visitor_division × action_type", ['visitor_division', 'action_type'], None),
    ]

    log("")
    log("=" * 80)
    log("  COMPARISON: exact COUNT(DISTINCT person_hash) vs HLL sketch estimate")
    log("=" * 80)
    hdr = f"  {'Query':<40s} {'Exact':>8} {'HLL est':>8} {'Delta':>7} {'Err%':>6}"
    log(hdr)
    log("  " + "-" * 78)

    for label, group_by, limit in queries:
        # --- Exact (from events_anonymized.parquet) ---
        if group_by is None:
            exact_df = pd.DataFrame([{'exact_uv': anon['person_hash'].nunique()}])
            exact_df['_key'] = ''
        elif group_by == 'month':
            anon['_month'] = pd.to_datetime(anon['session_date']).dt.to_period('M').astype(str)
            exact_df = (anon.groupby('_month')['person_hash']
                            .nunique().reset_index()
                            .rename(columns={'_month': '_key', 'person_hash': 'exact_uv'}))
        elif isinstance(group_by, list):
            cols = [c for c in group_by if c in anon.columns]
            exact_df = (anon.groupby(cols, dropna=False)['person_hash']
                            .nunique().reset_index()
                            .rename(columns={'person_hash': 'exact_uv'}))
            exact_df['_key'] = exact_df[cols].astype(str).agg('|'.join, axis=1)
        else:
            if group_by not in anon.columns:
                log(f"  {label:<40s} (column '{group_by}' not in anonymized parquet, skipped)")
                continue
            exact_df = (anon.groupby(group_by, dropna=False)['person_hash']
                            .nunique().reset_index()
                            .rename(columns={group_by: '_key', 'person_hash': 'exact_uv'}))

        # --- HLL estimate (from events_hll.parquet) ---
        if group_by is None:
            hll_uv = _merge_sketches(hll['uv_sketch'])
            hll_df = pd.DataFrame([{'_key': '', 'hll_uv': hll_uv}])
        elif group_by == 'month':
            hll['_month'] = pd.to_datetime(hll['session_date']).dt.to_period('M').astype(str)
            hll_df = (hll.groupby('_month', dropna=False)['uv_sketch']
                         .apply(_merge_sketches).reset_index()
                         .rename(columns={'_month': '_key', 'uv_sketch': 'hll_uv'}))
        elif isinstance(group_by, list):
            cols = [c for c in group_by if c in hll.columns]
            if not cols:
                log(f"  {label:<40s} (columns not in HLL parquet, skipped)")
                continue
            hll_df = (hll.groupby(cols, dropna=False)['uv_sketch']
                         .apply(_merge_sketches).reset_index()
                         .rename(columns={'uv_sketch': 'hll_uv'}))
            hll_df['_key'] = hll_df[cols].astype(str).agg('|'.join, axis=1)
        else:
            if group_by not in hll.columns:
                log(f"  {label:<40s} (column '{group_by}' not in HLL parquet, skipped)")
                continue
            hll_df = (hll.groupby(group_by, dropna=False)['uv_sketch']
                         .apply(_merge_sketches).reset_index()
                         .rename(columns={group_by: '_key', 'uv_sketch': 'hll_uv'}))

        # --- Merge & report ---
        merged = exact_df.merge(hll_df, on='_key', how='outer').fillna(0)
        if limit:
            merged = merged.nlargest(limit, 'exact_uv')

        if group_by is None or isinstance(group_by, str) and group_by not in ['month']:
            # Single-row or summary
            if len(merged) == 1:
                exact_uv = int(merged['exact_uv'].iloc[0])
                hll_uv   = int(merged['hll_uv'].iloc[0])
                delta    = abs(exact_uv - hll_uv)
                pct      = delta / exact_uv * 100 if exact_uv > 0 else 0
                log(f"  {label:<40s} {exact_uv:>8,} {hll_uv:>8,} {delta:>7,} {pct:>5.1f}%")
            else:
                # Multi-row: show aggregate stats
                exact_total = int(merged['exact_uv'].sum())
                hll_total   = int(merged['hll_uv'].sum())
                deltas      = (merged['exact_uv'] - merged['hll_uv']).abs()
                avg_err     = (deltas / merged['exact_uv'].replace(0, 1) * 100).mean()
                log(f"  {label:<40s} {'(per row avg err: ' + f'{avg_err:.1f}%)':<18s}"
                    f"  rows={len(merged)}")
                for _, row in merged.head(min(limit or 5, 5)).iterrows():
                    key   = str(row['_key'])[:28]
                    ex    = int(row['exact_uv'])
                    hl    = int(row['hll_uv'])
                    d     = abs(ex - hl)
                    p     = d / ex * 100 if ex > 0 else 0
                    log(f"    {key:<38s} {ex:>8,} {hl:>8,} {d:>7,} {p:>5.1f}%")
        else:
            deltas  = (merged['exact_uv'] - merged['hll_uv']).abs()
            avg_err = (deltas / merged['exact_uv'].replace(0, 1) * 100).mean()
            log(f"  {label:<40s} {'avg err: ' + f'{avg_err:.1f}%':>20s}  rows={len(merged)}")

    log("=" * 80)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_campaignwe_hll(run_compare=False):
    script_dir = Path(__file__).parent
    input_dir  = script_dir / 'input'
    output_dir = script_dir / 'output'
    hr_parquet_path = script_dir.parent / 'SearchAnalytics' / 'output' / 'hr_history.parquet'

    log("=" * 64)
    log("  CampaignWe HyperLogLog Pipeline")
    log("=" * 64)

    # Locate input files
    all_files = get_all_input_files(input_dir)
    if not all_files:
        log(f"ERROR: No input files found in {input_dir}")
        log("Place your KQL export files (xlsx/csv) in the input/ folder")
        sys.exit(1)
    log(f"\nFound {len(all_files)} input file(s)")

    # In-memory DuckDB — nothing written to disk until final parquet export
    con = duckdb.connect(':memory:')

    # Load all input files with deduplication
    for input_path in all_files:
        log(f"\nLoading: {input_path.name}")
        row_count = load_file_to_temp_table(con, input_path)
        log(f"  Loaded {row_count:,} rows")
        upsert_data(con)

    total = con.execute("SELECT COUNT(*) FROM events_raw").fetchone()[0]
    log(f"\nTotal deduplicated events: {total:,}")

    # HR join
    log("\nLoading HR history...")
    has_hr_history = load_hr_history(con, hr_parquet_path)

    # Build enriched events table
    log("")
    build_events_table(con, has_hr_history=has_hr_history)

    # Filter to known stories only (must match story_metadata.parquet)
    story_meta_path = output_dir / 'story_metadata.parquet'
    if story_meta_path.exists():
        con.execute(f"""
            CREATE TABLE story_titles AS
            SELECT * FROM read_parquet('{story_meta_path}')
        """)
        st_type = con.execute("SELECT typeof(story_id) FROM story_titles LIMIT 1").fetchone()
        if st_type and st_type[0] != 'VARCHAR':
            con.execute("ALTER TABLE story_titles ALTER story_id TYPE VARCHAR")
        before = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        # Keep: events with known story metadata OR non-story actions (invite, form, cancel)
        # Exclude: "Other" action type and story events without metadata
        con.execute("""
            DELETE FROM events
            WHERE action_type = 'Other'
               OR (
                   action_type NOT IN ('Open Form', 'Submit', 'Cancel', 'Send Invite', 'Open Invite')
                   AND (story_id IS NULL OR story_id NOT IN (SELECT story_id FROM story_titles))
               )
        """)
        after = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        log(f"  Filtered: {after:,} rows kept, {before - after:,} excluded (Other + unmatched stories)")
        con.execute("DROP TABLE story_titles")
    else:
        log(f"  WARNING: {story_meta_path} not found — no story filter applied")

    # Aggregate to HLL sketches — GPN discarded after this point
    log("")
    df = aggregate_to_hll(con)
    con.close()

    # Export
    log("\nExporting...")
    export_hll_parquet(df, output_dir)
    export_uv_aggregates(df, output_dir)
    export_powerbi_aggregates(df, output_dir)

    log(f"\nDone. Output: {output_dir / 'events_hll.parquet'}")

    # Optional comparison
    if run_compare:
        log("")
        compare_pipelines(output_dir)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_compare = '--compare' in sys.argv
    process_campaignwe_hll(run_compare=run_compare)
