"""
Convert story list CSV (from Power Automate) to Parquet lookup table.

The CSV is automatically synced by a Power Automate flow from a SharePoint list
into a OneDrive folder. This script reads it, marks approval status, and saves
story_metadata.parquet with ID, title, author metadata, and approval columns.

Approval: Stories with Status#Id == 1 are marked as approved. All other stories
are marked as pending (approved=False, status='pending'). Only approved stories
are used for event matching in process_campaignwe.py; pending stories are counted
but excluded from reports.

Soft-delete: Stories that were present in a previous run but are no longer in the
SharePoint list are marked with a deleted_date (date of disappearance) and kept in
the output. This preserves historical analytics data up to the deletion date.

If the main file has no story_title column but does have an Email column,
the script looks for a separate "Title*.csv" or "Title*.xlsx" file in the
same folder. It joins on Email to enrich the data with display names/titles.

HR enrichment: If author_email is available and hr_history.parquet exists, the
script enriches stories with author_country, author_business_sector, author_area,
and author_unit from the latest HR snapshot.

Author Division and Author Region come directly from SharePoint lookup columns,
not from the HR enrichment.

Input priority:
  1. OneDrive sync folder: <OneDrive>/Projekte/CampaignWe/input/We Are *.csv
  2. Local fallback: input/ folder (any .xlsx or .csv)

Usage:
    python fetch_story_metadata.py              # convert and save to output/story_metadata.parquet
    python fetch_story_metadata.py --preview    # read and print without saving

Prerequisites:
    pip install pandas pyarrow openpyxl
"""

import datetime
import json
import os
import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
LOCAL_INPUT_DIR = SCRIPT_DIR / "input"
OUTPUT_PATH = SCRIPT_DIR / "output" / "story_metadata.parquet"
HR_HISTORY_PATH = SCRIPT_DIR.parent / "SearchAnalytics" / "output" / "hr_history.parquet"

# Relative path inside OneDrive to the Power Automate output folder
ONEDRIVE_INPUT_DIR = Path("Projekte") / "CampaignWe" / "input"
ONEDRIVE_FILE_PATTERN = "We Are *.csv"

# Column mapping: our_name -> SharePoint column name(s) to look for (case-insensitive)
COLUMN_MAP = {
    "story_id": ["ID", "StoryID", "Story ID", "storyid", "story_id"],
    "story_text": ["Story", "story_text"],
}

# Additional columns to include in the output (optional — won't fail if missing)
EXTRA_COLUMNS = {
    "story_title": ["Story Title", "StoryTitle", "Titel", "story_title"],
    "status_id": ["Status#Id", "StatusId", "Status_Id", "status#id"],
    "keys": ["*Keys"],  # suffix match — the only column ending in "Keys"
    "author_email": ["Email", "E-Mail", "email"],
    "author_division": ["Division", "division"],
    "author_region": ["Region", "region"],
    "author_department": ["Department", "department"],
    "author_job_title": ["JobTitle", "Job Title", "jobtitle", "job_title"],
    "created": ["Created", "created"],
    "modified": ["Modified", "modified"],
}

# Title lookup file: a separate CSV/XLSX with ID and Title columns
# placed in the same folder as the main story list. Joined on ID
# when the main file doesn't contain a story_title column directly.
TITLE_FILE_PATTERN = "Title*"
TITLE_ID_CANDIDATES = ["ID", "Id", "id"]
TITLE_NAME_CANDIDATES = ["Title", "Titel", "Name", "DisplayName", "Display Name"]

# Only include rows where this column equals the given value
FILTER_COLUMN = "status_id"  # references our_name in EXTRA_COLUMNS
FILTER_VALUE = 1


def find_onedrive_root():
    """Auto-detect the corporate OneDrive sync folder."""
    if sys.platform == "win32":
        # Windows: look in user home for "OneDrive - *" folders
        home = Path.home()
        candidates = sorted(home.glob("OneDrive - *"), key=lambda p: p.name)
        if candidates:
            return candidates[0]
    else:
        # macOS / Linux: look in ~/Library/CloudStorage/OneDrive-*
        cloud = Path.home() / "Library" / "CloudStorage"
        if cloud.exists():
            candidates = sorted(cloud.glob("OneDrive-*"), key=lambda p: p.name)
            # Prefer corporate over personal
            corp = [c for c in candidates if "person" not in c.name.lower()
                    and "persönlich" not in c.name.lower()]
            if corp:
                return corp[0]
            if candidates:
                return candidates[0]

    # Also check ONEDRIVE environment variable (set by OneDrive on Windows)
    env_path = os.environ.get("OneDriveCommercial") or os.environ.get("OneDrive")
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p

    return None


def find_input_file():
    """Find the story CSV: first check OneDrive, then fall back to local input/."""
    # 1. Try OneDrive path
    onedrive_root = find_onedrive_root()
    onedrive_dir = onedrive_root / ONEDRIVE_INPUT_DIR if onedrive_root else None
    if onedrive_dir and onedrive_dir.exists():
        matches = list(onedrive_dir.glob(ONEDRIVE_FILE_PATTERN))
        matches = [f for f in matches if not f.name.startswith("~$")]
        if matches:
            newest = max(matches, key=lambda f: f.stat().st_mtime)
            print(f"  OneDrive root: {onedrive_root}")
            return newest
        else:
            print(f"  OneDrive folder found at {onedrive_dir} but no '{ONEDRIVE_FILE_PATTERN}' files")

    # 2. Fall back to local input/ folder
    LOCAL_INPUT_DIR.mkdir(parents=True, exist_ok=True)

    candidates = list(LOCAL_INPUT_DIR.glob("*.xlsx")) + list(LOCAL_INPUT_DIR.glob("*.csv"))
    candidates = [f for f in candidates if not f.name.startswith("~$")]

    if not candidates:
        print(f"ERROR: Could not find story data.")
        print(f"  Checked OneDrive: {onedrive_dir or '(not found)'} for '{ONEDRIVE_FILE_PATTERN}'")
        print(f"  Checked local:    {LOCAL_INPUT_DIR}/ (no .xlsx or .csv files)")
        print(f"\n  Ensure the Power Automate flow is syncing to OneDrive,")
        print(f"  or manually place a file in {LOCAL_INPUT_DIR}/")
        sys.exit(1)

    newest = max(candidates, key=lambda f: f.stat().st_mtime)
    print(f"  (Using local fallback: {LOCAL_INPUT_DIR}/)")
    return newest


def read_file(path):
    """Read an Excel or CSV file into a DataFrame, auto-detecting the delimiter."""
    if path.suffix.lower() == ".csv":
        import csv
        with open(path, "r", newline="", encoding="utf-8-sig") as f:
            sample = f.read(8192)
        sniffer = csv.Sniffer()
        try:
            dialect = sniffer.sniff(sample, delimiters=",;\t|")
            sep = dialect.delimiter
        except csv.Error:
            sep = ","
        print(f"  CSV delimiter: {repr(sep)}")
        return pd.read_csv(path, sep=sep)
    else:
        return pd.read_excel(path)


def resolve_column(df, candidates):
    """Find the first matching column name from a list of candidates.

    Supports exact match (case-insensitive) and wildcard patterns:
      "*Keys"  — matches any column ending with "Keys"
      "Prefix*" — matches any column starting with "Prefix"
    """
    for name in candidates:
        for col in df.columns:
            col_stripped = col.strip()
            if name.startswith("*"):
                if col_stripped.lower().endswith(name[1:].lower()):
                    return col
            elif name.endswith("*"):
                if col_stripped.lower().startswith(name[:-1].lower()):
                    return col
            else:
                if col_stripped.lower() == name.lower():
                    return col
    return None


def load_title_lookup(input_dir):
    """Load a Title lookup file from the same folder as the main story file.

    Returns a DataFrame with columns [id, title] or None if not found.
    """
    candidates = []
    for ext in ("*.csv", "*.xlsx"):
        candidates.extend(input_dir.glob(TITLE_FILE_PATTERN + ext[1:]))
    candidates = [f for f in candidates if not f.name.startswith("~$")]

    if not candidates:
        return None

    newest = max(candidates, key=lambda f: f.stat().st_mtime)
    print(f"  Title lookup file: {newest.name}")

    title_df = read_file(newest)

    id_col = resolve_column(title_df, TITLE_ID_CANDIDATES)
    title_col = resolve_column(title_df, TITLE_NAME_CANDIDATES)

    if id_col is None or title_col is None:
        print(f"  Warning: Title file found but missing ID or Title column")
        print(f"           Columns: {list(title_df.columns)}")
        return None

    print(f"  Title file columns: {list(title_df.columns)}")
    print(f"  Using: ID='{id_col}', Title='{title_col}'")

    result = title_df[[id_col, title_col]].copy()
    result.columns = ["id", "title"]
    result["id"] = result["id"].astype(str).str.strip()
    result = result.dropna(subset=["title"])
    result = result.drop_duplicates(subset=["id"], keep="first")
    print(f"  Title lookup: {len(result)} entries")
    print(f"  Title IDs: {result['id'].tolist()}")
    return result


def main():
    preview = "--preview" in sys.argv

    print("Looking for input file...")
    input_file = find_input_file()
    print(f"  Found: {input_file}")

    print("Reading file...")
    df = read_file(input_file)
    print(f"  Read {len(df)} rows, columns: {list(df.columns)}")

    # Map required columns
    mapped = {}
    for our_name, candidates in COLUMN_MAP.items():
        col = resolve_column(df, candidates)
        if col is None:
            print(f"ERROR: Could not find column for '{our_name}'.")
            print(f"       Expected one of: {candidates}")
            print(f"       Found columns: {list(df.columns)}")
            sys.exit(1)
        mapped[our_name] = col

    # Map extra columns (optional — warn but don't fail if missing)
    extra_mapped = {}
    for our_name, candidates in EXTRA_COLUMNS.items():
        col = resolve_column(df, candidates)
        if col is not None:
            extra_mapped[our_name] = col
        else:
            print(f"  Warning: optional column '{our_name}' not found (looked for {candidates})")

    print(f"\n  Required columns mapped: {mapped}")
    print(f"  Extra columns mapped: {extra_mapped}")
    print(f"  story_title found in main file: {'story_title' in extra_mapped}")

    # If story_title is missing, try joining from Title lookup file on ID
    if "story_title" not in extra_mapped:
        print("\n  story_title not in main file — looking for Title lookup file...")
        title_lookup = load_title_lookup(input_file.parent)
        if title_lookup is not None:
            id_col = mapped["story_id"]
            df["_join_id"] = df[id_col].astype(str).str.strip()
            df = df.merge(title_lookup, left_on="_join_id", right_on="id", how="left")
            df.drop(columns=["_join_id", "id"], inplace=True)
            extra_mapped["story_title"] = "title"
            matched = df["title"].notna().sum()
            print(f"  Joined titles: {matched}/{len(df)} rows matched", flush=True)
            print(f"\n  {'ID':<8} {'Title':<60} {'Match'}", flush=True)
            print(f"  {'─'*8} {'─'*60} {'─'*5}", flush=True)
            id_col_name = mapped["story_id"]
            for _, row in df.iterrows():
                sid = str(row[id_col_name]).strip()
                title = row.get("title", None)
                status = "OK" if pd.notna(title) else "MISS"
                title_str = str(title)[:58] if pd.notna(title) else "(no match)"
                print(f"  {sid:<8} {title_str:<60} {status}", flush=True)
        else:
            print(f"  No Title lookup file found in {input_file.parent}/")

    # Enrich with HR data from hr_history.parquet (e_mail_address -> country, sector, area, unit)
    HR_ENRICH_FIELDS = {
        "work_location_country": "author_country",
        "gcrs_sector_desc": "author_business_sector",
        "gcrs_area_desc": "author_area",
        "gcrs_unit_desc": "author_unit",
    }

    if "author_email" in extra_mapped and HR_HISTORY_PATH.exists():
        print("\n  Enriching with HR data from hr_history.parquet...")
        try:
            # Read only the columns we need (skip any that don't exist in the file)
            import pyarrow.parquet as pq
            available_cols = set(pq.read_schema(HR_HISTORY_PATH).names)
            hr_src_cols = [c for c in HR_ENRICH_FIELDS if c in available_cols]
            missing_hr_cols = [c for c in HR_ENRICH_FIELDS if c not in available_cols]
            if missing_hr_cols:
                print(f"  Warning: HR columns not in file: {missing_hr_cols}")

            hr = pd.read_parquet(HR_HISTORY_PATH,
                                 columns=["e_mail_address", "snapshot_year", "snapshot_month"] + hr_src_cols)
            hr = hr.dropna(subset=["e_mail_address"])
            # Keep only the latest snapshot to avoid duplicates
            latest = hr.nlargest(1, ["snapshot_year", "snapshot_month"])[["snapshot_year", "snapshot_month"]].iloc[0]
            hr = hr[(hr["snapshot_year"] == latest["snapshot_year"]) &
                    (hr["snapshot_month"] == latest["snapshot_month"])]
            hr["e_mail_address"] = hr["e_mail_address"].astype(str).str.strip().str.lower()

            # Build lookup: email + all available HR fields
            lookup_cols = ["e_mail_address"] + hr_src_cols
            hr_lookup = hr[lookup_cols].drop_duplicates(subset=["e_mail_address"], keep="first")
            rename_map = {"e_mail_address": "_hr_email"}
            rename_map.update({src: HR_ENRICH_FIELDS[src] for src in hr_src_cols})
            hr_lookup = hr_lookup.rename(columns=rename_map)
            print(f"  Using HR snapshot {int(latest['snapshot_year'])}-{int(latest['snapshot_month']):02d}")
            print(f"  HR fields enriched: {[HR_ENRICH_FIELDS[c] for c in hr_src_cols]}")

            email_col = extra_mapped["author_email"]
            df["_join_email"] = df[email_col].astype(str).str.strip().str.lower()
            df = df.merge(hr_lookup, left_on="_join_email", right_on="_hr_email", how="left")
            df.drop(columns=["_join_email", "_hr_email"], inplace=True)

            # Register enriched columns in extra_mapped
            for src in hr_src_cols:
                alias = HR_ENRICH_FIELDS[src]
                extra_mapped[alias] = alias

            # Diagnostic output
            matched = df[HR_ENRICH_FIELDS[hr_src_cols[0]]].notna().sum() if hr_src_cols else 0
            print(f"  HR enrichment: {matched}/{len(df)} rows matched", flush=True)
            enriched_aliases = [HR_ENRICH_FIELDS[c] for c in hr_src_cols]
            header_fields = " ".join(f"{a:<20}" for a in enriched_aliases)
            print(f"\n  {'ID':<8} {'Email':<40} {header_fields} {'Match'}", flush=True)
            print(f"  {'─'*8} {'─'*40} {' '.join('─'*20 for _ in enriched_aliases)} {'─'*5}", flush=True)
            id_col_name = mapped["story_id"]
            email_col_name = extra_mapped.get("author_email", None)
            for _, row in df.iterrows():
                sid = str(row[id_col_name]).strip()
                email = str(row[email_col_name])[:38] if email_col_name else ""
                vals = []
                any_matched = False
                for alias in enriched_aliases:
                    v = row.get(alias, None)
                    if pd.notna(v):
                        vals.append(f"{str(v)[:18]:<20}")
                        any_matched = True
                    else:
                        vals.append(f"{'(no match)':<20}")
                status = "OK" if any_matched else "MISS"
                print(f"  {sid:<8} {email:<40} {' '.join(vals)} {status}", flush=True)
        except Exception as e:
            print(f"  Warning: Could not enrich HR data: {e}")
    elif not HR_HISTORY_PATH.exists():
        print(f"  Info: {HR_HISTORY_PATH} not found, skipping HR enrichment")

    # Build result with required + extra columns
    src_cols = [mapped["story_id"], mapped["story_text"]]
    dst_names = ["story_id", "story_text"]
    for our_name, src_col in extra_mapped.items():
        src_cols.append(src_col)
        dst_names.append(our_name)

    result = df[src_cols].copy()
    result.columns = dst_names

    # Parse SharePoint JSON lookup columns
    # These come as JSON like [{"Id":2,"Value":"Connectivity"}, ...] (array)
    # or {"Id":2,"Value":"APAC"} (single object)
    # Extract the "Value" field(s) into a comma-separated string
    SP_LOOKUP_COLUMNS = ["keys", "author_division", "author_region"]

    def parse_sp_lookup(val):
        if pd.isna(val) or val == "":
            return ""
        try:
            parsed = json.loads(val) if isinstance(val, str) else val
            if isinstance(parsed, list):
                return ", ".join(item["Value"] for item in parsed if "Value" in item)
            elif isinstance(parsed, dict) and "Value" in parsed:
                return parsed["Value"]
        except (json.JSONDecodeError, TypeError, KeyError):
            pass
        return str(val)

    for col_name in SP_LOOKUP_COLUMNS:
        if col_name in result.columns:
            result[col_name] = result[col_name].apply(parse_sp_lookup)

    # Mark approval status (status_id == 1 = approved, anything else = pending)
    if FILTER_COLUMN in result.columns:
        result[FILTER_COLUMN] = pd.to_numeric(result[FILTER_COLUMN], errors="coerce")
        approved = (result[FILTER_COLUMN] == FILTER_VALUE).sum()
        pending = len(result) - approved
        result["approved"] = (result[FILTER_COLUMN] == FILTER_VALUE)
        print(f"  Approval status: {approved} approved, {pending} pending/unapproved")
    else:
        result["approved"] = True
        print(f"  Warning: filter column '{FILTER_COLUMN}' not available, marking all as approved")

    # Split comma-separated keys into story_key1, story_key2, story_key3
    if "keys" in result.columns:
        split_keys = result["keys"].fillna("").str.split(",", expand=True, n=2)
        for i in range(3):
            col = f"story_key{i+1}"
            result[col] = split_keys[i].str.strip() if i in split_keys.columns else ""

    # Convert date columns to proper date types
    for date_col in ["created", "modified"]:
        if date_col in result.columns:
            result[date_col] = pd.to_datetime(result[date_col], errors="coerce").dt.date

    # Clean up
    result["story_id"] = result["story_id"].astype(str).str.strip()
    result = result.dropna(subset=["story_id"])
    result = result[result["story_id"] != ""]

    print(f"  Mapped {len(result)} stories")

    # Add status column — only approved stories are "active"
    result["status"] = result["approved"].map({True: "active", False: "pending"})
    result["deleted_date"] = pd.NaT
    result["deleted_by"] = None

    if preview or result.empty:
        print("\n--- Story Titles ---")
        print(result.to_string(index=False))
        if result.empty:
            print("  (no items found)")
        return

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Soft-delete: merge with existing parquet to preserve deleted stories
    if not OUTPUT_PATH.exists():
        print(f"\n  WARNING: No existing {OUTPUT_PATH.name} found.")
        print(f"           Deleted story history will not be preserved.")
        print(f"           If this is a first run, this is expected.")

    if OUTPUT_PATH.exists():
        existing = pd.read_parquet(OUTPUT_PATH)
        existing["story_id"] = existing["story_id"].astype(str).str.strip()

        # Ensure columns exist in existing data (backward compat with old parquet)
        if "status" not in existing.columns:
            existing["status"] = "active"
        if "approved" not in existing.columns:
            existing["approved"] = existing["status"].isin(["active", "deleted"])
        if "deleted_date" not in existing.columns:
            existing["deleted_date"] = pd.NaT
        if "deleted_by" not in existing.columns:
            existing["deleted_by"] = None

        active_ids = set(result["story_id"].tolist())
        existing_ids = set(existing["story_id"].tolist())

        # Stories that were in existing but are NOT in current fetch → newly deleted
        newly_deleted_ids = existing_ids - active_ids
        # Stories that were already marked deleted previously (preserve their corrected dates)
        previously_deleted = existing[existing["status"] == "deleted"].copy()

        if newly_deleted_ids:
            today = datetime.date.today()
            newly_deleted = existing[
                (existing["story_id"].isin(newly_deleted_ids)) &
                (existing["status"] != "deleted")  # don't re-mark already deleted
            ].copy()
            newly_deleted["status"] = "deleted"
            newly_deleted["deleted_date"] = today  # approximate — corrected later by process_campaignwe.py
            print(f"  Soft-deleted {len(newly_deleted)} stories (no longer in SharePoint): "
                  f"IDs {sorted(newly_deleted_ids)}")

            # Combine: current active + newly deleted + previously deleted
            result = pd.concat([result, newly_deleted, previously_deleted],
                               ignore_index=True)
        elif len(previously_deleted) > 0:
            # No new deletions, but carry forward previously deleted stories
            result = pd.concat([result, previously_deleted], ignore_index=True)
            print(f"  Carrying forward {len(previously_deleted)} previously deleted stories")

        # Deduplicate by story_id (active takes precedence if a deleted story reappears)
        result = result.sort_values("status", ascending=True)  # "active" before "deleted"
        result = result.drop_duplicates(subset=["story_id"], keep="first")

    # Convert deleted_date to proper date type
    result["deleted_date"] = pd.to_datetime(result["deleted_date"], errors="coerce").dt.date

    result.to_parquet(OUTPUT_PATH, index=False)
    active_count = (result["status"] == "active").sum()
    deleted_count = (result["status"] == "deleted").sum()
    pending_count = (result["status"] == "pending").sum()
    print(f"\nSaved {len(result)} stories to {OUTPUT_PATH} "
          f"({active_count} active, {deleted_count} deleted, {pending_count} pending)")


if __name__ == "__main__":
    main()
