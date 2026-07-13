#!/usr/bin/env python3
"""
BEAM Movie Downloader — GitHub Actions Pipeline
================================================
Reads Google Sheet rows → downloads movie → detects audio languages via MediaInfo →
renames file → uploads to Vidara → calls BEAM Worker upsert API → writes URL back to sheet.

Sheet columns (Movies tab):
  A: Filename
  B: Status           (DONE, NOT_FOUND)
  C: TMDB_ID
  D: TMDB_NAME
  E: YEAR
  F: Quality          (1080p, 720p, 480p, 2160p, 360p)
  G: Download_Link
  H: DOWNLOAD_STATUS  (blank=pending, Done, Failed)
  I: FINAL_LINK       (Vidara URL)
  J: Duplicate_Check  (DUPLICATE or blank)
  K: Error

Setup:
  GitHub Secrets needed:
    GOOGLE_SHEETS_JSON  — service account JSON (string)
    SPREADSHEET_ID      — Google Sheet ID
    VIDARA_API_KEY      — Vidara API key
    BEAM_ADMIN_EMAIL    — beam-worker admin email
    BEAM_ADMIN_PASSWORD — beam-worker admin password
"""

import os
import re
import json
import shutil
import requests
import time
import subprocess
from pathlib import Path
import gspread
from google.oauth2.service_account import Credentials
from pymediainfo import MediaInfo
from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor

# ============================================================================
# CONFIGURATION
# ============================================================================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Vidara
VIDARA_API_KEY = os.environ.get("VIDARA_API_KEY", "").strip()
if not VIDARA_API_KEY:
    VIDARA_API_KEY = "de57ed8e0bd00f3c0c18db283f5377caf14ad141ffb74ee49f83cb5ed13ab9dc"

# Google Sheets
RAW_SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
SPREADSHEET_ID = RAW_SPREADSHEET_ID.replace("'", "").replace('"', '').strip()

# BEAM Worker
BEAM_WORKER_URL = "https://beamplay.beam-api.workers.dev"
ADMIN_EMAIL = os.environ.get("BEAM_ADMIN_EMAIL", "chanducharan2030@gmail.com")
ADMIN_PASSWORD = os.environ.get("BEAM_ADMIN_PASSWORD", "Chandu2030")

# Folders
OUTPUT_FOLDER = "./media/movies"
TEMP_FOLDER = "./temp_downloads"
SHEET_INDEX = 0  # First tab

MAX_CONCURRENT = 3  # sequential for now, can be threaded later

for p in [OUTPUT_FOLDER, TEMP_FOLDER]:
    os.makedirs(p, exist_ok=True)

# Language mapping (MediaInfo codes → display names)
LANG_MAP = {
    "as": "Assamese", "te": "Telugu", "hi": "Hindi", "ta": "Tamil", "ml": "Malayalam",
    "kn": "Kannada", "bn": "Bengali", "pa": "Punjabi", "gu": "Gujarati", "mr": "Marathi",
    "or": "Oriya", "en": "English", "ja": "Japanese", "ko": "Korean", "es": "Spanish",
    "fr": "French", "de": "German", "ru": "Russian", "zh": "Chinese", "it": "Italian",
    "pt": "Portuguese", "ar": "Arabic", "tr": "Turkish",
}

# ============================================================================
# GOOGLE SHEETS AUTH
# ============================================================================
print("=" * 60)
print("BEAM MOVIE DOWNLOADER — STARTING")
print("=" * 60)

try:
    raw_json_str = os.environ.get("GOOGLE_SHEETS_JSON")
    if not raw_json_str:
        raise ValueError("GOOGLE_SHEETS_JSON is missing.")
    creds_dict = json.loads(raw_json_str)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    print("[OK] Connected to Google Sheets API")
except Exception as auth_err:
    print(f"[CRITICAL] Auth failed: {auth_err}")
    raise

sheet = gc.open_by_key(SPREADSHEET_ID).get_worksheet(SHEET_INDEX)
all_rows = sheet.get_all_records()
headers = sheet.row_values(1)
# Strip whitespace from headers (Google Sheets sometimes has leading/trailing spaces)
headers = [h.strip() if isinstance(h, str) else h for h in headers]

# Column indices (1-based for gspread)
try:
    tmdb_id_col = headers.index("TMDB_ID") + 1         # C
    tmdb_name_col = headers.index("TMDB_NAME") + 1     # D
    year_col = headers.index("YEAR") + 1               # E
    quality_col = headers.index("Quality") + 1         # F
    dl_link_col = headers.index("Download_Link") + 1   # G
    status_col = headers.index("DOWNLOAD_STATUS") + 1  # H
    final_link_col = headers.index("FINAL_LINK") + 1   # I
    dup_col = headers.index("Duplicate_Check") + 1     # J
    error_col = headers.index("Error") + 1             # K
except ValueError as e:
    raise Exception(f"Missing column header: {e}. Found headers: {headers}")

# ============================================================================
# HELPERS
# ============================================================================

def parse_media_languages(file_path):
    """Detect audio languages from file using MediaInfo."""
    try:
        media = MediaInfo.parse(str(file_path))
        langs = []
        for track in media.tracks:
            if track.track_type == "Audio":
                code = track.language if track.language else "en"
                mapped = LANG_MAP.get(code.lower(), "English")
                langs.append(mapped)
        return list(dict.fromkeys(langs)) or ["English"]
    except Exception:
        return ["English"]

def build_filename(tmdb_name, year, quality, languages):
    """Build clean filename: Title (Year) Quality Lang1 + Lang2 (no extension — Vidara works without it)"""
    short_langs = [l[:3] for l in languages]
    if year:
        name = f"{tmdb_name} ({year}) {quality} {' + '.join(short_langs)}"
    else:
        name = f"{tmdb_name} {quality} {' + '.join(short_langs)}"
    return name

def fetch_vidara_upload_server():
    """Get active Vidara upload server."""
    try:
        res = requests.get("https://api.vidara.so/v1/upload/server", params={"api_key": VIDARA_API_KEY}, timeout=30)
        res.raise_for_status()
        data = res.json()
        return data.get("result", {}).get("upload_server") or data.get("upload_server") or "https://api.vidara.so/v1/upload/server"
    except Exception as e:
        print(f"[WARN] Vidara server fetch failed: {e}")
        return "https://api.vidara.so/v1/upload/server"

def upload_to_vidara(file_path, custom_name):
    """Upload file to Vidara, return filecode/URL."""
    upload_server = fetch_vidara_upload_server()
    print(f"   Uploading to Vidara: {custom_name} ({round(os.path.getsize(file_path) / 1048576, 1)} MB)")

    encoder = MultipartEncoder(fields={
        "api_key": VIDARA_API_KEY,
        "file": (custom_name, open(file_path, "rb"), "video/mp4")
    })
    monitor = MultipartEncoderMonitor(encoder)
    response = requests.post(upload_server, data=monitor, headers={"Content-Type": monitor.content_type}, timeout=None)
    encoder.fields["file"][1].close()

    if response.status_code == 200:
        data = response.json()
        final_url = data.get("filecode") or data.get("url") or data.get("result", {}).get("url")
        return final_url
    else:
        raise Exception(f"Vidara upload failed: {response.status_code} {response.text[:200]}")

def beam_login():
    """Login to BEAM worker, return JWT token."""
    res = requests.post(f"{BEAM_WORKER_URL}/auth/login", json={
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD
    }, timeout=30)
    res.raise_for_status()
    return res.json()["token"]

def beam_upsert(jwt, tmdb_id, quality, languages, url):
    """Call BEAM worker upsert endpoint."""
    res = requests.post(f"{BEAM_WORKER_URL}/admin/vidara/upsert", json={
        "content_type": "movie",
        "tmdb_id": int(tmdb_id),
        "url": url,
        "quality": quality,
        "audio_languages": languages
    }, headers={"Authorization": f"Bearer {jwt}"}, timeout=30)
    res.raise_for_status()
    return res.json()

def download_file(url, dest_path):
    """Download file using aria2c, fall back to requests streaming."""
    # Try aria2c first
    cmd = [
        "aria2c", "-x", "8", "-s", "8", "-k", "5M",
        "--file-allocation=none", "--summary-interval=0", "--retry-wait=10",
        "--max-tries=8", "--timeout=45", "--auto-file-renaming=false",
        "-d", os.path.dirname(dest_path), "-o", os.path.basename(dest_path), url
    ]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0 and os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024 * 1024:
        return True

    # Fallback: requests streaming
    print("   [WARN] aria2c failed, trying direct stream...")
    try:
        if os.path.exists(dest_path):
            os.remove(dest_path)
        headers = {"User-Agent": "Mozilla/5.0"}
        with requests.get(url, headers=headers, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024 * 1024
    except Exception as e:
        print(f"   [ERROR] Direct stream failed: {e}")
        return False

# ============================================================================
# MAIN PIPELINE
# ============================================================================
print(f"\nProcessing {len(all_rows)} rows...")

jwt = beam_login()
print("[OK] Logged into BEAM worker\n")

processed = 0
failed = 0

for idx, row in enumerate(all_rows):
    row_idx = idx + 2  # sheet rows are 1-indexed, +1 for header

    # Skip if already done or duplicate
    dl_status = str(row.get("DOWNLOAD_STATUS", "")).strip()
    dup_check = str(row.get("Duplicate_Check", "")).strip().upper()
    if dl_status.lower() == "done" or dup_check == "DUPLICATE":
        continue

    download_link = str(row.get("Download_Link", "")).strip()
    tmdb_id = str(row.get("TMDB_ID", "")).strip()
    tmdb_name = str(row.get("TMDB_NAME", "")).strip()
    year = str(row.get("YEAR", "")).strip()
    quality = str(row.get("Quality", "")).strip()

    if not download_link or not tmdb_id or not quality:
        continue

    print(f"\n{'='*60}")
    print(f"Row {row_idx}: {tmdb_name} ({year}) — {quality}")
    print(f"{'='*60}")

    try:
        # 1. Download
        original_name = os.path.basename(download_link.split('?')[0]) or f"movie_{row_idx}.mkv"
        temp_path = os.path.join(TEMP_FOLDER, original_name)
        print(f"   Downloading from: {download_link[:80]}...")

        if not download_file(download_link, temp_path):
            raise Exception("Download failed (both aria2c and streaming)")

        # 2. Detect audio languages
        languages = parse_media_languages(temp_path)
        print(f"   Detected languages: {languages}")

        # 3. Rename
        clean_name = build_filename(tmdb_name, year, quality, languages)
        final_path = os.path.join(OUTPUT_FOLDER, clean_name)
        shutil.move(temp_path, final_path)
        print(f"   Renamed to: {clean_name}")

        # 4. Upload to Vidara
        vidara_url = upload_to_vidara(final_path, clean_name)
        if not vidara_url:
            raise Exception("Vidara upload returned no URL")
        print(f"   Vidara URL: {vidara_url}")

        # 5. Call BEAM worker upsert
        result = beam_upsert(jwt, tmdb_id, quality, languages, vidara_url)
        print(f"   DB: {result.get('action', 'unknown')} (link_id: {result.get('id')})")

        # 6. Write back to sheet
        sheet.update_cell(row_idx, final_link_col, vidara_url)
        sheet.update_cell(row_idx, status_col, "Done")
        sheet.update_cell(row_idx, error_col, "")

        processed += 1

        # Cleanup
        if os.path.exists(final_path):
            os.remove(final_path)

    except Exception as e:
        print(f"   [ERROR] {e}")
        sheet.update_cell(row_idx, status_col, "Failed")
        sheet.update_cell(row_idx, error_col, str(e)[:500])
        failed += 1

        # Cleanup temp
        if os.path.exists(temp_path):
            try: os.remove(temp_path)
            except: pass

print(f"\n{'='*60}")
print(f"COMPLETE — {processed} processed, {failed} failed")
print(f"{'='*60}")
