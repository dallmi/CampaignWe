"""
Convert a SharePoint list export (Excel/CSV) to Parquet lookup table.

Steps:
  1. Export the SharePoint list via "Export to Excel" in your browser
  2. Place the .xlsx or .csv file in the input/ folder
  3. Run this script — it reads the newest file and saves story_titles.parquet

Usage:
    python fetch_story_titles.py              # convert and save to output/story_titles.parquet
    python fetch_story_titles.py --preview    # read and print without saving

Prerequisites:
    pip install pandas pyarrow openpyxl
"""

import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR / "input"
OUTPUT_PATH = SCRIPT_DIR / "output" / "story_titles.parquet"

# Column mapping: our_name -> SharePoint column name(s) to look for
COLUMN_MAP = {
    "story_id": ["StoryID", "Story ID", "storyid", "story_id"],
    "story_title": ["Title", "Story Title", "title", "story_title"],
}


def find_input_file():
    """Find the newest .xlsx or .csv file in the input/ folder."""
    INPUT_DIR.mkdir(parents=True, exist_ok=True)

    candidates = list(INPUT_DIR.glob("*.xlsx")) + list(INPUT_DIR.glob("*.csv"))
    # Exclude Excel temp files
    candidates = [f for f in candidates if not f.name.startswith("~$")]

    if not candidates:
        print(f"ERROR: No .xlsx or .csv files found in {INPUT_DIR}/")
        print(f"       Export the SharePoint list and place the file there.")
        sys.exit(1)

    newest = max(candidates, key=lambda f: f.stat().st_mtime)
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
    print(f"  Found: {input_file.name}")

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
