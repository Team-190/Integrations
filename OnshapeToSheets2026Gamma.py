#!/usr/bin/env python3
"""
Onshape BOM -> Google Sheets (filters by partNumber prefixes)

Fixes:
- Google Sheets cannot accept dict/list values. This script stringifies any dict/list cells.
- itemSource sometimes arrives as a dict; we convert it to itemSourceHref if present.

ENV (.env example):

ONSHAPE_ACCESS_KEY=...
ONSHAPE_SECRET_KEY=...
ONSHAPE_DOC_URL=https://frc190.onshape.com/documents/<did>/w/<wid>/e/<eid>

GOOGLE_SERVICE_ACCOUNT_FILE=cred.json
GOOGLE_SHEET_NAME=2026GammaBOM
GOOGLE_WORKSHEET_NAME=Sheet1

PARTNUMBER_PREFIXES=P-190A-26,WCP-,912,TTB-,SDS
"""

import os
import re
import json
import base64
import hmac
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse, urlencode

import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


DEFAULT_COLUMNS = [
    "key",
    "item",
    "quantity",
    "partNumber",
    "name",
    "description",
    "material",
    "manufacturingmethod",
    "vendor",
    "revision",
    "state",
    "category",
    "frcbombommaterial",
    "frcbompreprocess",
    "frcbomprocess1",
    "frcbomprocess2",
    # Instead of raw itemSource (can be dict), we write a safe string column:
    "itemSource",
]


@dataclass
class OnshapeTarget:
    base_url: str
    did: str
    wvm_type: str
    wvm_id: str
    eid: str


def require_env(name: str) -> str:
    v = os.environ.get(name)
    if v is None or v == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


def parse_onshape_doc_url(doc_url: str) -> OnshapeTarget:
    u = urlparse(doc_url.strip())
    if not u.scheme or not u.netloc:
        raise ValueError("Please provide a full URL starting with https://...")

    base_url = f"{u.scheme}://{u.netloc}"
    path = u.path

    m = re.search(r"/documents/([a-fA-F0-9]+)/([wvm])/([a-fA-F0-9]+)/e/([a-fA-F0-9]+)", path)
    if not m:
        raise ValueError(
            "Could not parse URL.\n"
            "Expected: https://<domain>/documents/<did>/w|v|m/<id>/e/<eid>"
        )

    did, wvm_type, wvm_id, eid = m.group(1), m.group(2), m.group(3), m.group(4)
    return OnshapeTarget(base_url=base_url, did=did, wvm_type=wvm_type, wvm_id=wvm_id, eid=eid)


def _rfc1123_gmt_now() -> str:
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def _nonce_hex(nbytes: int = 16) -> str:
    return os.urandom(nbytes).hex()


def onshape_headers_official(method: str, full_url: str, content_type: str = "application/json") -> dict:
    access_key = require_env("ONSHAPE_ACCESS_KEY")
    secret_key = require_env("ONSHAPE_SECRET_KEY")

    u = urlparse(full_url)
    path = u.path
    query = u.query or ""

    date = _rfc1123_gmt_now()
    nonce = _nonce_hex(16)

    string_to_sign = (
        f"{method}\n"
        f"{nonce}\n"
        f"{date}\n"
        f"{content_type}\n"
        f"{path}\n"
        f"{query}\n"
    ).lower()

    sig = hmac.new(secret_key.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha256).digest()
    sig_b64 = base64.b64encode(sig).decode("utf-8")

    return {
        "Authorization": f"On {access_key}:HmacSHA256:{sig_b64}",
        "Date": date,
        "On-Nonce": nonce,
        "Content-Type": content_type,
        "Accept": "application/json",
    }


def test_doc_access(target: OnshapeTarget) -> None:
    url = f"{target.base_url}/api/documents/{target.did}"
    headers = onshape_headers_official("GET", url)
    r = requests.get(url, headers=headers, timeout=30)
    print(f"[INFO] Doc access test: {r.status_code}")
    if r.status_code != 200:
        try:
            print(r.json())
        except Exception:
            print(r.text[:2000])
        raise RuntimeError("Doc access test failed. Fix permissions/domain.")


def fetch_bom(target: OnshapeTarget) -> dict:
    endpoint = f"{target.base_url}/api/assemblies/d/{target.did}/{target.wvm_type}/{target.wvm_id}/e/{target.eid}/bom"
    params = {
        "indented": "false",
        "multiLevel": "false",
        "generateIfAbsent": "false",
        "includeItemMicroversions": "false",
        "includeTopLevelAssemblyRow": "false",
        "thumbnail": "false",
    }
    url = f"{endpoint}?{urlencode(params)}"
    headers = onshape_headers_official("GET", url)
    r = requests.get(url, headers=headers, timeout=60)

    if r.status_code != 200:
        print(f"[ERROR] BOM fetch failed: {r.status_code}")
        try:
            print(r.json())
        except Exception:
            print(r.text[:2000])
        raise RuntimeError(f"Onshape BOM fetch failed with HTTP {r.status_code}")

    return r.json()


def extract_items(bom_json: dict) -> list[dict]:
    if isinstance(bom_json, dict) and "bomTable" in bom_json and isinstance(bom_json["bomTable"], dict):
        items = bom_json["bomTable"].get("items")
        if isinstance(items, list):
            return items
    for k in ("items", "rows", "bomItems", "bomRows"):
        v = bom_json.get(k) if isinstance(bom_json, dict) else None
        if isinstance(v, list):
            return v
    raise RuntimeError("Unexpected BOM schema: could not find items array.")


def normalize_and_filter(items: list[dict], part_prefixes: list[str]) -> list[dict]:
    prefixes = [p.strip() for p in part_prefixes if p and p.strip()]
    out: list[dict] = []

    for it in items:
        pn = (it.get("partNumber") or "").strip()
        if prefixes and not any(pn.startswith(p) for p in prefixes):
            continue

        it2 = dict(it)

        # normalize material dict to id if present
        mat = it2.get("material")
        if isinstance(mat, dict) and "id" in mat:
            it2["material"] = mat["id"]

        # itemSource sometimes is dict -> keep a useful href if possible
        src = it2.get("itemSource")
        if isinstance(src, dict):
            it2["itemSource"] = src.get("viewHref") or json.dumps(src, ensure_ascii=False)
        elif isinstance(src, list):
            it2["itemSource"] = json.dumps(src, ensure_ascii=False)

        # stable key
        nm = (it2.get("name") or "").strip()
        it2["key"] = pn if pn else nm

        out.append(it2)

    return out


def to_dataframe(items: list[dict], columns: list[str]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(items)

    for c in columns:
        if c not in df.columns:
            df[c] = ""

    df = df.loc[:, columns].fillna("")
    return df


def stringify_complex_cells(df: pd.DataFrame) -> pd.DataFrame:
    """
    Google Sheets rejects dict/list cells. Convert any dict/list values into JSON strings.
    """
    def coerce(v):
        if isinstance(v, (dict, list)):
            return json.dumps(v, ensure_ascii=False)
        return v

    for col in df.columns:
        df[col] = df[col].map(coerce)

    # Ensure everything is a plain Python scalar or string
    return df.astype(str)


def load_service_account_json() -> dict:
    sa_file = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE") or "").strip()
    if sa_file:
        with open(sa_file, "r", encoding="utf-8") as f:
            return json.load(f)

    sa_inline = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if sa_inline:
        return json.loads(sa_inline)

    raise RuntimeError("Set GOOGLE_SERVICE_ACCOUNT_FILE (local) or GOOGLE_SERVICE_ACCOUNT_JSON (Actions).")


def write_to_google_sheet(df: pd.DataFrame) -> None:
    sheet_name = require_env("GOOGLE_SHEET_NAME")
    worksheet_name = require_env("GOOGLE_WORKSHEET_NAME")

    sa_json = load_service_account_json()

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_json, scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name)
    ws = sheet.worksheet(worksheet_name)

    values = [df.columns.values.tolist()] + df.values.tolist()

    ws.clear()
    ws.update(values)

    print(f"[OK] Wrote {max(len(values)-1, 0)} rows to '{sheet_name}' / '{worksheet_name}'")


def main() -> int:
    if load_dotenv is not None and os.path.exists(".env"):
        load_dotenv(".env")

    doc_url = (os.environ.get("ONSHAPE_DOC_URL") or "").strip()
    if not doc_url:
        doc_url = input("Paste Onshape document URL (assembly tab URL): ").strip()

    prefixes_env = (os.environ.get("PARTNUMBER_PREFIXES") or "").strip()
    part_prefixes = [p.strip() for p in prefixes_env.split(",") if p.strip()] if prefixes_env else []

    columns_env = (os.environ.get("INCLUDE_COLUMNS") or "").strip()
    columns = [c.strip() for c in columns_env.split(",") if c.strip()] if columns_env else DEFAULT_COLUMNS

    target = parse_onshape_doc_url(doc_url)
    print("[INFO] Parsed target:")
    print(f"  did:      {target.did}")
    print(f"  {target.wvm_type} id:   {target.wvm_id}")
    print(f"  eid:      {target.eid}")

    print("[INFO] Testing document access...")
    test_doc_access(target)

    print("[INFO] Fetching BOM...")
    bom = fetch_bom(target)

    print("[INFO] Extracting BOM items...")
    items = extract_items(bom)
    print(f"[INFO] Total BOM rows returned: {len(items)}")

    print(f"[INFO] Filtering by partNumber prefixes: {part_prefixes if part_prefixes else '(none -> keep all)'}")
    filtered = normalize_and_filter(items, part_prefixes=part_prefixes)
    print(f"[INFO] Rows after filter: {len(filtered)}")

    df = to_dataframe(filtered, columns)

    # IMPORTANT: make Sheets-safe
    df = stringify_complex_cells(df)

    if not df.empty:
        print("[INFO] Preview (first 10 rows):")
        print(df.head(10).to_string(index=False))
    else:
        print("[WARN] No rows after filtering; sheet will be overwritten with headers only.")

    print("[INFO] Writing to Google Sheets...")
    write_to_google_sheet(df)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"[FATAL] {e}")
        raise
