"""
Fetch story titles from a SharePoint list and save as Parquet lookup table.

Uses device-code flow: the script shows a code, you sign in via browser
with your corporate AD credentials (+ Authenticator), and the token flows back.
Tokens are cached locally so you only re-authenticate when the session expires.

Usage:
    python fetch_story_titles.py              # fetch and save to output/story_titles.parquet
    python fetch_story_titles.py --preview    # fetch and print without saving

Prerequisites:
    pip install msal requests pandas pyarrow
"""

import json
import sys
from pathlib import Path

import msal
import pandas as pd
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "config" / "sharepoint.json"
OUTPUT_PATH = SCRIPT_DIR / "output" / "story_titles.parquet"
TOKEN_CACHE_PATH = SCRIPT_DIR / "config" / ".token_cache.bin"

# Microsoft Office public client ID — a first-party app that is allowed
# in virtually all corporate tenants (unlike the Azure CLI client ID).
WELL_KNOWN_CLIENT_ID = "d3590ed6-52b3-4102-aeff-aad2292ab01c"

SCOPES = ["https://graph.microsoft.com/Sites.Read.All"]


def load_config():
    if not CONFIG_PATH.exists():
        print(f"ERROR: Config file not found at {CONFIG_PATH}")
        sys.exit(1)

    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    for key in ("site_url", "list_name"):
        if not cfg.get(key) or "YOUR_" in cfg[key]:
            print(f"ERROR: Please set '{key}' in {CONFIG_PATH}")
            sys.exit(1)

    auth = cfg.get("auth", {})
    if not auth.get("tenant_id") or "YOUR_" in auth["tenant_id"]:
        print(f"ERROR: Please set 'auth.tenant_id' in {CONFIG_PATH}")
        sys.exit(1)

    return cfg


def build_msal_app(tenant_id):
    """Build a public client MSAL app with persistent token cache."""
    cache = msal.SerializableTokenCache()
    if TOKEN_CACHE_PATH.exists():
        cache.deserialize(TOKEN_CACHE_PATH.read_text())

    app = msal.PublicClientApplication(
        WELL_KNOWN_CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        token_cache=cache,
    )
    return app, cache


def save_cache(cache):
    """Persist the token cache if it changed."""
    if cache.has_state_changed:
        TOKEN_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        TOKEN_CACHE_PATH.write_text(cache.serialize())


def get_access_token(cfg):
    """Acquire token via device-code flow with cached refresh tokens."""
    tenant_id = cfg["auth"]["tenant_id"]
    app, cache = build_msal_app(tenant_id)

    # Try cached accounts first (silent token refresh)
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
        if result and "access_token" in result:
            print("  Using cached credentials (no sign-in needed)")
            save_cache(cache)
            return result["access_token"]

    # No cached token — initiate device-code flow
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        print(f"ERROR: Could not initiate device flow: {flow.get('error_description')}")
        sys.exit(1)

    print()
    print(flow["message"])  # "To sign in, use a web browser to open ... and enter the code ..."
    print()

    result = app.acquire_token_by_device_flow(flow)

    if "access_token" not in result:
        print("ERROR: Authentication failed.")
        print(f"  Error: {result.get('error')}")
        print(f"  Description: {result.get('error_description')}")
        sys.exit(1)

    save_cache(cache)
    return result["access_token"]


def get_sharepoint_site_id(token, site_url):
    """Resolve a SharePoint site URL to a Graph site ID."""
    from urllib.parse import urlparse
    parsed = urlparse(site_url.rstrip("/"))
    hostname = parsed.hostname
    site_path = parsed.path.rstrip("/")

    url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    return resp.json()["id"]


def fetch_list_items(token, site_id, list_name, columns):
    """Fetch all items from a SharePoint list via Microsoft Graph, handling pagination."""
    headers = {"Authorization": f"Bearer {token}", "Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly"}

    sp_cols = list(columns.values())
    select = ",".join(sp_cols)
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_name}/items?$expand=fields($select={select})&$top=500"

    all_items = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        all_items.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    return all_items


def items_to_dataframe(items, columns):
    """Convert Graph API list items to a DataFrame with our column names."""
    rows = []
    for item in items:
        fields = item.get("fields", {})
        row = {}
        for our_name, sp_name in columns.items():
            row[our_name] = fields.get(sp_name)
        rows.append(row)

    df = pd.DataFrame(rows)

    if "story_id" in df.columns:
        df["story_id"] = df["story_id"].astype(str).str.strip()

    return df


def main():
    preview = "--preview" in sys.argv

    print("Loading config...")
    cfg = load_config()
    columns = cfg.get("columns", {"story_id": "StoryID", "story_title": "Title"})

    print("Authenticating...")
    token = get_access_token(cfg)

    print(f"Resolving site: {cfg['site_url']}")
    site_id = get_sharepoint_site_id(token, cfg["site_url"])

    print(f"Fetching list: {cfg['list_name']}")
    items = fetch_list_items(token, site_id, cfg["list_name"], columns)
    print(f"  Retrieved {len(items)} items")

    df = items_to_dataframe(items, columns)

    if preview or df.empty:
        print("\n--- Story Titles ---")
        print(df.to_string(index=False))
        if df.empty:
            print("  (no items found)")
        return

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nSaved {len(df)} stories to {OUTPUT_PATH}")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
