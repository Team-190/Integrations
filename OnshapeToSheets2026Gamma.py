#!/usr/bin/env python3
"""
Sync an Onshape Assembly BOM to a Google Sheet (overwrite worksheet with filtered BOM).

Designed for GitHub Actions:
- Reads all secrets from environment variables (no files committed).
- Writes Google service account JSON from env (no cred.json needed).
- Fetches BOM from Onshape, filters rows, normalizes fields, uploads to Google Sheets.

ENV VARS REQUIRED:
  ONSHAPE_ACCESS_KEY
  ONSHAPE_SECRET_KEY
  ONSHAPE_BASE_URL              (optional, default: https://frc190.onshape.com)
  ONSHAPE_DOCUMENT_ID
  ONSHAPE_WORKSPACE_ID
  ONSHAPE_ASSEMBLY_ID

  GOOGLE_SERVICE_ACCOUNT_JSON   (full JSON string)
  GOOGLE_SHEET_NAME             (e.g. "2025GammaBOM")
  GOOGLE_WORKSHEET_NAME         (e.g. "Sheet1")

OPTIONAL FILTERING / OUTPUT:
  PARTNUMBER_PREFIX             (default: P-25)
  INCLUDE_COLUMNS               (comma-separated, default below)
"""

import os
import sys
import json
import time
import hmac
import base64
import hashlib
from urllib.parse import urlparse, urlencode

import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials


DEFAULT_COLUMNS = [
    "name",
    "description",
    "vendor",
    "partNumber",
    "material",
    "quantity",
    "revision",
    "manufacturingmethod",
]


def require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


def create_onshape_headers(method: str, url: str, query_string: str = "", body: str = "") -> dict:
    """
    Generate signed headers for Onshape API.

    NOTE: This matches the user's currently-working approach (nonce/date as ms timestamp).
    If you later want the "official" HTTP Date signing scheme, this function is where to change it.
    """
    access_key = require_env("ONSHAPE_ACCESS_KEY")
    secret_key = require_env("ONSHAPE_SECRET_KEY")

    current_time_ms = str(int(time.time() * 1000))  # used as nonce + date in this scheme
    url_parts = urlparse(url)
    request_path = url_parts.path
    if query_string:
        request_path += "?" + query_string

    prehash_string = (method + "\n" + current_time_ms + "\n" + request_path + "\n" + body).lower()
    signature = hmac.new(
        secret_key.encode("utf-8"),
        prehash_string.encode("utf-8"),
        hashlib.sha256
    ).digest()
    signature_b64 = base64.b64encode(signature).decode("utf-8")

    return {
        "Authorization": f"On {access_key}:HmacSHA256:{signature_b64}",
        "On-Nonce": current_time_ms,
        "Date": current_time_ms,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def fetch_bom() -> dict:
    base_url = os.environ.get("ONSHAPE_BASE_URL", "https://frc190.onshape.com").rstrip("/")
    did = require_env("ONSHAPE_DOCUMENT_ID")
    wid = require_env("ONSHAPE_WORKSPACE_ID")
    eid = require_env("ONSHAPE_ASSEMBLY_ID")

    endpoint = f"{base_url}/api/assemblies/d/{did}/w/{wid}/e/{eid}/bom"

    params = {
        "indented": "false",
        "multiLevel": "false",
        "generateIfAbsent": "false",
        "includeItemMicroversions": "false",
        "includeTopLevelAssemblyRow": "false",
        "thumbnail": "false",
    }
    query_string = urlencode(params)
    url = f"{endpoint}?{query_string}"

    headers = create_onshape_headers("GET", endpoint, query_string=query_string, body="")
    resp = requests.get(url, headers=headers, timeout=60)

    if resp.status_code != 200:
        # Show useful debugging info in GitHub Actions logs
        print(f"[ERROR] Onshape BOM fetch failed: {resp.status_code}")
        try:
            print(resp.json())
        except Exception:
            print(resp.text[:2000])
        raise RuntimeError(f"Onshape request failed with HTTP {resp.status_code}")

    return resp.json()


def extract_items(bom_json: dict) -> list[dict]:
    """
    Attempt to pull the BOM line items from common Onshape BOM response structures.
    Your original code used: data['bomTable']['items'].
    """
    if isinstance(bom_json, dict):
        if "bomTable" in bom_json and isinstance(bom_json["bomTable"], dict):
            items = bom_json["bomTable"].get("items")
            if isinstance(items, list):
                return items

        # Fallback guesses (in case schema differs)
        for k in ("items", "rows", "bomItems", "bomRows"):
            v = bom_json.get(k)
            if isinstance(v, list):
                return v

    raise RuntimeError("Could not locate BOM items array in response JSON (unexpected schema).")


def normalize_items(items: list[dict], part_prefix: str) -> list[dict]:
    """
    - Filter by partNumber prefix
    - Normalize material: if material is object with id, replace with that id
    """
    filtered = []
    for it in items:
        pn = it.get("partNumber")
        if not pn or not isinstance(pn, str):
            continue
        if part_prefix and not pn.startswith(part_prefix):
            continue

        it2 = dict(it)  # shallow copy

        mat = it2.get("material")
        if isinstance(mat, dict) and "id" in mat:
            it2["material"] = mat["id"]

        filtered.append(it2)

    return filtered


def to_dataframe(items: list[dict], columns: list[str]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(items)

    # Ensure all expected columns exist
    for c in columns:
        if c not in df.columns:
            df[c] = ""

    # Order columns
    df = df.loc[:, columns]

    # Replace NaN with empty string for Sheets
    df = df.fillna("")
    return df


def write_to_google_sheet(df: pd.DataFrame) -> None:
    sheet_name = require_env("GOOGLE_SHEET_NAME")
    worksheet_name = require_env("GOOGLE_WORKSHEET_NAME")
    sa_json_str = require_env("GOOGLE_SERVICE_ACCOUNT_JSON")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    sa_dict = json.loads(sa_json_str)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_dict, scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name)
    ws = sheet.worksheet(worksheet_name)

    # Prepare values (header + rows)
    values = [df.columns.values.tolist()] + df.values.tolist()

    # Clear & update in one shot
    ws.clear()
    ws.update(values)

    print(f"[OK] Wrote {max(len(values) - 1, 0)} rows to Google Sheet '{sheet_name}' / '{worksheet_name}'")


def main() -> int:
    part_prefix = os.environ.get("PARTNUMBER_PREFIX", "P-25").strip()
    cols_env = os.environ.get("INCLUDE_COLUMNS", "")
    columns = [c.strip() for c in cols_env.split(",") if c.strip()] if cols_env else DEFAULT_COLUMNS

    print("[INFO] Fetching BOM from Onshape...")
    bom = fetch_bom()

    print("[INFO] Extracting BOM items...")
    items = extract_items(bom)

    print(f"[INFO] Normalizing + filtering items (prefix='{part_prefix}')...")
    filtered = normalize_items(items, part_prefix=part_prefix)
    print(f"[INFO] Items after filter: {len(filtered)}")

    df = to_dataframe(filtered, columns=columns)

    # Log a small preview
    if len(df) > 0:
        print("[INFO] Preview (first 5 rows):")
        print(df.head(5).to_string(index=False))
    else:
        print("[WARN] No rows after filtering; sheet will be overwritten with just headers.")

    print("[INFO] Writing to Google Sheets...")
    write_to_google_sheet(df)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"[FATAL] {e}")
        raise
