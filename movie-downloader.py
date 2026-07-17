#!/usr/bin/env python3
"""
BEAM Movie Downloader — GitHub Actions Pipeline (Single-Row Multi-Quality Workflow)
==================================================================================
Reads Queue sheet rows → downloads movie variants → detects languages via MediaInfo →
renames file → uploads to Vidara → calls BEAM Worker upsert API → updates quality statuses →
archives completed variants.
"""

import os
import re
import json
import shutil
import requests
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

for p in [OUTPUT_FOLDER, TEMP_FOLDER]:
    os.makedirs(p, exist_ok=True)

# Language mapping
LANG_MAP = {
    "as": "Assamese", "te": "Telugu", "hi": "Hindi", "ta": "Tamil", "ml": "Malayalam",
    "kn": "Kannada", "bn": "Bengali", "pa": "Punjabi", "gu": "Gujarati", "mr": "Marathi",
    "or": "Oriya", "en": "English", "ja": "Japanese", "ko": "Korean", "es": "Spanish",
    "fr": "French", "de": "German", "ru": "Russian", "zh": "Chinese", "it": "Italian",
    "pt": "Portuguese", "ar": "Arabic", "tr": "Turkish",
}

# ============================================================================
# HELPERS
# ============================================================================

def parse_media_languages(file_path):
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

def clean_string_for_vidara(text):
    if not text:
        return ""
    text = text.replace(".", "")
    text = text.replace("/", "-")
    text = re.sub(r'[:*?"<>|]', "", text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def build_filename(tmdb_name, year, quality, languages):
    clean_title = clean_string_for_vidara(tmdb_name)
    short_langs = [l[:3] for l in languages]
    if year:
        name = f"{clean_title} ({year}) {quality} {' + '.join(short_langs)}.mkv"
    else:
        name = f"{clean_title} {quality} {' + '.join(short_langs)}.mkv"
    return name

def fetch_vidara_upload_server():
    try:
        res = requests.get("https://api.vidara.so/v1/upload/server", params={"api_key": VIDARA_API_KEY}, timeout=30)
        res.raise_for_status()
        data = res.json()
        return data.get("result", {}).get("upload_server") or data.get("upload_server") or "https://api.vidara.so/v1/upload/server"
    except Exception as e:
        print(f"[WARN] Vidara server fetch failed: {e}")
        return "https://api.vidara.so/v1/upload/server"

def upload_to_vidara(file_path, custom_name):
    upload_server = fetch_vidara_upload_server()
    print(f"    Uploading to Vidara: {custom_name} ({round(os.path.getsize(file_path) / 1048576, 1)} MB)")

    encoder = MultipartEncoder(fields={
        "api_key": VIDARA_API_KEY,
        "file": (custom_name, open(file_path, "rb"), "video/x-matroska")
    })
    monitor = MultipartEncoderMonitor(encoder)
    response = requests.post(upload_server, data=monitor, headers={"Content-Type": monitor.content_type}, timeout=None)
    encoder.fields["file"][1].close()

    if response.status_code == 200:
        data = response.json()
        filecode = data.get("filecode") or data.get("url") or data.get("result", {}).get("url") or data.get("result", {}).get("filecode")
        return filecode
    else:
        raise Exception(f"Vidara upload failed: {response.status_code} {response.text[:200]}")

def beam_login():
    res = requests.post(f"{BEAM_WORKER_URL}/auth/login", json={
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD
    }, timeout=30)
    res.raise_for_status()
    return res.json()["token"]

def beam_upsert(jwt, tmdb_id, quality, languages, url):
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
    cmd = [
        "aria2c", "-x", "8", "-s", "8", "-k", "5M",
        "--file-allocation=none", "--summary-interval=0", "--retry-wait=10",
        "--max-tries=8", "--timeout=45", "--auto-file-renaming=false",
        "-d", os.path.dirname(dest_path), "-o", os.path.basename(dest_path), url
    ]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0 and os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024 * 1024:
        return True

    print("    [WARN] aria2c failed, trying direct stream...")
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
        print(f"    [ERROR] Direct stream failed: {e}")
        return False

# ============================================================================
# MAIN MULTI-QUALITY PIPELINE
# ============================================================================
print("=" * 60)
print("BEAM MOVIE DOWNLOADER — STARTING MULTI-QUALITY QUEUE RUN")
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

spreadsheet = gc.open_by_key(SPREADSHEET_ID)
try:
    queue_sheet = spreadsheet.worksheet("Queue")
    archive_sheet = spreadsheet.worksheet("Archive")
    print("[OK] Loaded worksheets: Queue & Archive")
except Exception:
    queue_sheet = spreadsheet.get_worksheet(0)
    archive_sheet = spreadsheet.get_worksheet(1)
    print(f"[WARN] Specific named tabs not found. Falling back to indices 0 and 1.")

raw_values = queue_sheet.get_all_values()
if not raw_values:
    raise Exception("The Queue worksheet is completely empty!")

headers = [h.strip() for h in raw_values[0]]

# Map indices dynamically based on actual header values
try:
    filename_col = headers.index("Filename") + 1
    status_col = headers.index("Status") + 1
    tmdb_id_col = headers.index("TMDB_ID") + 1
    tmdb_name_col = headers.index("TMDB_NAME") + 1
    year_col = headers.index("YEAR") + 1
    
    link_1080_col = headers.index("Link_1080p") + 1
    link_720_col = headers.index("Link_720p") + 1
    link_480_col = headers.index("Link_480p") + 1
    
    status_1080_col = headers.index("DOWNLOAD_STATUS_1080p") + 1
    status_720_col = headers.index("DOWNLOAD_STATUS_720p") + 1
    status_480_col = headers.index("DOWNLOAD_STATUS_480p") + 1
    
    dup_col = headers.index("Duplicate_Check") + 1
    error_col = headers.index("Error") + 1
except ValueError as e:
    raise Exception(f"Missing column header configuration: {e}. Available headers: {headers}")

all_rows = []
for row_cells in raw_values[1:]:
    padded_row = row_cells + [""] * (len(headers) - len(row_cells))
    row_dict = {headers[i]: padded_row[i] for i in range(len(headers)) if headers[i] != ""}
    all_rows.append(row_dict)

print(f"\nProcessing {len(all_rows)} rows...")

try:
    jwt = beam_login()
    print("[OK] Logged into BEAM worker\n")
except Exception as login_err:
    print(f"[CRITICAL] BEAM Engine authentication failed: {login_err}")
    raise

# Iterate backwards to maintain correct structural deletions safely
for idx in range(len(all_rows) - 1, -1, -1):
    row = all_rows[idx]
    row_idx = idx + 2

    tmdb_id = str(row.get("TMDB_ID", "")).strip()
    tmdb_name = str(row.get("TMDB_NAME", "")).strip()
    year = str(row.get("YEAR", "")).strip()

    if not tmdb_id:
        continue

    dup_check = str(row.get("Duplicate_Check", "")).strip().upper()
    if dup_check == "DUPLICATE":
        print(f"Skipping Row {row_idx}: Marked as DUPLICATE.")
        continue

    links = {
        "1080p": str(row.get("Link_1080p", "")).strip(),
        "720p": str(row.get("Link_720p", "")).strip(),
        "480p": str(row.get("Link_480p", "")).strip()
    }

    statuses = {
        "1080p": str(row.get("DOWNLOAD_STATUS_1080p", "")).strip().lower(),
        "720p": str(row.get("DOWNLOAD_STATUS_720p", "")).strip().lower(),
        "480p": str(row.get("DOWNLOAD_STATUS_480p", "")).strip().lower()
    }

    active_variants = {}
    for q, link in links.items():
        if link and statuses[q] != "done":
            active_variants[q] = link

    if not active_variants:
        continue

    print(f"\n{'='*60}")
    print(f"Row {row_idx}: {tmdb_name} ({year}) — Processing qualities: {list(active_variants.keys())}")
    print(f"{'='*60}")

    errors = []
    status_col_map = {
        "1080p": status_1080_col,
        "720p": status_720_col,
        "480p": status_480_col
    }

    newly_finished = []

    for quality, download_link in active_variants.items():
        print(f"\n -> Starting {quality}...")
        temp_path = os.path.join(TEMP_FOLDER, f"movie_{row_idx}_{quality}.mkv")

        try:
            if not download_file(download_link, temp_path):
                raise Exception("Download stage execution failed.")

            languages = parse_media_languages(temp_path)
            print(f"    Detected languages: {languages}")

            clean_name = build_filename(tmdb_name, year, quality, languages)
            final_path = os.path.join(OUTPUT_FOLDER, clean_name)
            shutil.move(temp_path, final_path)
            print(f"    Renamed to: {clean_name}")

            vidara_url = upload_to_vidara(final_path, clean_name)
            if not vidara_url:
                raise Exception("Vidara upload execution returned no valid code/URL")
            print(f"    Vidara File Identifier/URL: {vidara_url}")

            result = beam_upsert(jwt, tmdb_id, quality, languages, vidara_url)
            print(f"    DB Upsert status: {result.get('action', 'unknown')} (link_id: {result.get('id')})")

            queue_sheet.update_cell(row_idx, status_col_map[quality], "Done")
            statuses[quality] = "done"
            newly_finished.append(quality)

            if os.path.exists(final_path):
                os.remove(final_path)

        except Exception as variant_error:
            print(f"    [ERROR ON {quality}]: {variant_error}")
            queue_sheet.update_cell(row_idx, status_col_map[quality], "Failed")
            statuses[quality] = "failed"
            errors.append(f"{quality}: {str(variant_error)}")

            if os.path.exists(temp_path):
                try: os.remove(temp_path)
                except: pass

    # Error handling reflection logic
    if errors:
        queue_sheet.update_cell(row_idx, error_col, " | ".join(errors)[:500])
    else:
        queue_sheet.update_cell(row_idx, error_col, "")

    # Calculate overall completeness metrics
    total_links_count = sum(1 for link in links.values() if link)
    done_links_count = sum(1 for q, link in links.items() if link and statuses[q] == "done")

    # Scenario A: All valid inputs are verified as "Done"
    if total_links_count > 0 and done_links_count == total_links_count:
        print(f"\nRow {row_idx} fully completed ({done_links_count}/{total_links_count}). Archiving entry...")
        
        archive_row = [
            row.get("Filename", ""),
            row.get("Status", ""),
            tmdb_id,
            tmdb_name,
            year,
            links["1080p"],
            links["720p"],
            links["480p"],
            "Done" if links["1080p"] else "",
            "Done" if links["720p"] else "",
            "Done" if links["480p"] else "",
            row.get("Duplicate_Check", ""),
            ""
        ]
        
        archive_sheet.append_row(archive_row, value_input_option="USER_ENTERED")
        queue_sheet.delete_rows(row_idx)
        print(f"[OK] Row {row_idx} fully processed and dropped from active operational Queue.")

    # Scenario B: Partial updates completed
    elif len(newly_finished) > 0:
        print(f"\nRow {row_idx} has logged partial success. Writing completed variants to Archive...")
        
        archive_row = [
            row.get("Filename", ""),
            row.get("Status", ""),
            tmdb_id,
            tmdb_name,
            year,
            links["1080p"] if statuses["1080p"] == "done" else "",
            links["720p"] if statuses["720p"] == "done" else "",
            links["480p"] if statuses["480p"] == "done" else "",
            "Done" if statuses["1080p"] == "done" else "",
            "Done" if statuses["720p"] == "done" else "",
            "Done" if statuses["480p"] == "done" else "",
            row.get("Duplicate_Check", ""),
            "Partial processing completed"
        ]
        
        archive_sheet.append_row(archive_row, value_input_option="USER_ENTERED")
        print(f"[OK] Logged updates successfully for variants: {newly_finished}.")

# Global cleanups
try:
    shutil.rmtree(OUTPUT_FOLDER)
    shutil.rmtree(TEMP_FOLDER)
except:
    pass

print(f"\n{'='*60}\nPIPELINE SEQUENCE COMPLETE\n{'='*60}")
