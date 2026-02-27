"""
Convert story list CSV (from Power Automate) to Parquet lookup table.

The CSV is automatically synced by a Power Automate flow from a SharePoint list
into a OneDrive folder. This script reads it, filters active stories (Status#Id = 1),
and saves story_metadata.parquet with ID, title, and metadata columns.

Input priority:
  1. OneDrive sync folder: <OneDrive>/Projekte/CampaignWe/input/We Are *.csv
  2. Local fallback: input/ folder (any .xlsx or .csv)

Usage:
    python fetch_story_titles.py              # convert and save to output/story_metadata.parquet
    python fetch_story_titles.py --preview    # read and print without saving

Prerequisites:
    pip install pandas pyarrow openpyxl
"""

import json
import os
import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
LOCAL_INPUT_DIR = SCRIPT_DIR / "input"
OUTPUT_PATH = SCRIPT_DIR / "output" / "story_metadata.parquet"

# Relative path inside OneDrive to the Power Automate output folder
ONEDRIVE_INPUT_DIR = Path("Projekte") / "CampaignWe" / "input"
ONEDRIVE_FILE_PATTERN = "We Are *.csv"

# Column mapping: our_name -> SharePoint column name(s) to look for (case-insensitive)
COLUMN_MAP = {
    "story_id": ["ID", "StoryID", "Story ID", "storyid", "story_id"],
    "story_title": ["Story", "Title", "Story Title", "title", "story_title"],
}

# Additional columns to include in the output (optional — won't fail if missing)
EXTRA_COLUMNS = {
    "status_id": ["Status#Id", "StatusId", "Status_Id", "status#id"],
    "keys": ["*Keys"],  # suffix match — the only column ending in "Keys"
    "email": ["Email", "E-Mail", "email"],
    "division": ["Division", "division"],
    "region": ["Region", "region"],
    "department": ["Department", "department"],
    "job_title": ["JobTitle", "Job Title", "jobtitle", "job_title"],
    "created": ["Created", "created"],
    "modified": ["Modified", "modified"],
}

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

    # Build result with required + extra columns
    src_cols = [mapped["story_id"], mapped["story_title"]]
    dst_names = ["story_id", "story_title"]
    for our_name, src_col in extra_mapped.items():
        src_cols.append(src_col)
        dst_names.append(our_name)

    result = df[src_cols].copy()
    result.columns = dst_names

    # Parse SharePoint JSON lookup columns
    # These come as JSON like [{"Id":2,"Value":"Connectivity"}, ...] (array)
    # or {"Id":2,"Value":"APAC"} (single object)
    # Extract the "Value" field(s) into a comma-separated string
    SP_LOOKUP_COLUMNS = ["keys", "division", "region"]

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

    # Filter by status (only active stories)
    if FILTER_COLUMN in result.columns:
        before = len(result)
        result[FILTER_COLUMN] = pd.to_numeric(result[FILTER_COLUMN], errors="coerce")
        result = result[result[FILTER_COLUMN] == FILTER_VALUE]
        print(f"  Filtered {FILTER_COLUMN} == {FILTER_VALUE}: {before} -> {len(result)} rows")
    else:
        print(f"  Warning: filter column '{FILTER_COLUMN}' not available, skipping filter")

    # Clean up
    result["story_id"] = result["story_id"].astype(str).str.strip()
    result = result.dropna(subset=["story_id"])
    result = result[result["story_id"] != ""]

    print(f"  Mapped {len(result)} stories")

    if preview or result.empty:
        print("\n--- Story Titles ---")
        print(result.to_string(index=False))
        if result.empty:
            print("  (no items found)")
        return

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nSaved {len(result)} stories to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
