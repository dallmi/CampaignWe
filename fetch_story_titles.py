"""
Convert story titles CSV (from Power Automate) to Parquet lookup table.

The CSV is automatically synced by a Power Automate flow from a SharePoint list
into a OneDrive folder. This script reads it and saves story_titles.parquet.

Input priority:
  1. OneDrive sync folder: <OneDrive>/Projekte/CPLAN/input/story.csv
  2. Local fallback: input/ folder (any .xlsx or .csv)

Usage:
    python fetch_story_titles.py              # convert and save to output/story_titles.parquet
    python fetch_story_titles.py --preview    # read and print without saving

Prerequisites:
    pip install pandas pyarrow openpyxl
"""

import os
import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
LOCAL_INPUT_DIR = SCRIPT_DIR / "input"
OUTPUT_PATH = SCRIPT_DIR / "output" / "story_titles.parquet"

# Relative path inside OneDrive to the Power Automate output
ONEDRIVE_SUBPATH = Path("Projekte") / "CampaignWe" / "input" / "story.csv"

# Column mapping: our_name -> SharePoint column name(s) to look for
COLUMN_MAP = {
    "story_id": ["StoryID", "Story ID", "storyid", "story_id", "ID"],
    "story_title": ["Title", "Story Title", "title", "story_title"],
}


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
    if onedrive_root:
        onedrive_file = onedrive_root / ONEDRIVE_SUBPATH
        if onedrive_file.exists():
            print(f"  OneDrive root: {onedrive_root}")
            return onedrive_file
        else:
            print(f"  OneDrive found at {onedrive_root} but story.csv not at expected path:")
            print(f"    {onedrive_file}")

    # 2. Fall back to local input/ folder
    LOCAL_INPUT_DIR.mkdir(parents=True, exist_ok=True)

    candidates = list(LOCAL_INPUT_DIR.glob("*.xlsx")) + list(LOCAL_INPUT_DIR.glob("*.csv"))
    candidates = [f for f in candidates if not f.name.startswith("~$")]

    if not candidates:
        print(f"ERROR: Could not find story data.")
        print(f"  Checked OneDrive: {onedrive_root / ONEDRIVE_SUBPATH if onedrive_root else '(not found)'}")
        print(f"  Checked local:    {LOCAL_INPUT_DIR}/ (no .xlsx or .csv files)")
        print(f"\n  Ensure the Power Automate flow is syncing story.csv to OneDrive,")
        print(f"  or manually place a file in {LOCAL_INPUT_DIR}/")
        sys.exit(1)

    newest = max(candidates, key=lambda f: f.stat().st_mtime)
    print(f"  (Using local fallback: {LOCAL_INPUT_DIR}/)")
    return newest


def read_file(path):
    """Read an Excel or CSV file into a DataFrame."""
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    else:
        return pd.read_excel(path)


def resolve_column(df, candidates):
    """Find the first matching column name from a list of candidates."""
    for name in candidates:
        for col in df.columns:
            if col.strip().lower() == name.lower():
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

    # Map columns
    mapped = {}
    for our_name, candidates in COLUMN_MAP.items():
        col = resolve_column(df, candidates)
        if col is None:
            print(f"ERROR: Could not find column for '{our_name}'.")
            print(f"       Expected one of: {candidates}")
            print(f"       Found columns: {list(df.columns)}")
            sys.exit(1)
        mapped[our_name] = col

    result = df[[mapped["story_id"], mapped["story_title"]]].copy()
    result.columns = ["story_id", "story_title"]

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
    print(result.head(10).to_string(index=False))
    if len(result) > 10:
        print(f"  ... and {len(result) - 10} more")


if __name__ == "__main__":
    main()
