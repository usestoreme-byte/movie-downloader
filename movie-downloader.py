#!/usr/bin/env python3
"""
BEAM Movie Downloader — GitHub Actions Pipeline (v2: per-link, per-language sequential)
=========================================================================================
For every row, for every quality (1080p/720p/480p):
    - Cell contains one or more links (one per line).
    - For each link, in order:
        - Download the file.
        - Detect audio tracks (MediaInfo) — one track = one language.
        - Detect subtitle tracks in that same file (normalized, unknown -> English).
        - For each NEW audio language (not already done for this quality):
            - Remux (stream copy, NO re-encoding) that single audio + all subtitles
              from THIS source file into one output .mkv.
            - Upload to Vidara.
            - beam_upsert() into BEAM Worker DB.
            - Delete the output file immediately.
        - Delete the original downloaded file once all its languages are processed.
    - "Already done" languages are tracked ONLY in memory, and ONLY for the current
      quality. The set is thrown away before moving to the next quality.
    - Sheet writes happen only at checkpoints:
        quality start   -> DOWNLOAD_STATUS_xxxx = Running          (1 write)
        quality success -> DOWNLOAD_STATUS_xxxx = Done, ERROR_xxxx cleared   (1 write)
        quality failure -> DOWNLOAD_STATUS_xxxx = Failed, ERROR_xxxx = details (1 write)
      No language-level or link-level sheet writes. No batch requests.
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

VIDARA_API_KEY = os.environ.get("VIDARA_API_KEY", "").strip()

RAW_SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
SPREADSHEET_ID = RAW_SPREADSHEET_ID.replace("'", "").replace('"', '').strip()

BEAM_WORKER_URL = "https://beamplay.beam-api.workers.dev"
ADMIN_EMAIL = os.environ.get("BEAM_ADMIN_EMAIL", "")
ADMIN_PASSWORD = os.environ.get("BEAM_ADMIN_PASSWORD", "")

OUTPUT_FOLDER = "./media/movies"
TEMP_FOLDER = "./temp_downloads"

for p in [OUTPUT_FOLDER, TEMP_FOLDER]:
    os.makedirs(p, exist_ok=True)

LANG_MAP = {
    "as": "Assamese", "te": "Telugu", "hi": "Hindi", "ta": "Tamil", "ml": "Malayalam",
    "kn": "Kannada", "bn": "Bengali", "pa": "Punjabi", "gu": "Gujarati", "mr": "Marathi",
    "or": "Oriya", "en": "English", "ja": "Japanese", "ko": "Korean", "es": "Spanish",
    "fr": "French", "de": "German", "ru": "Russian", "zh": "Chinese", "it": "Italian",
    "pt": "Portuguese", "ar": "Arabic", "tr": "Turkish",
}

UNKNOWN_TOKENS = {"", "und", "unknown", "unk", "n/a", "none"}

# 2-letter -> 3-letter ISO 639-2 codes, used to re-tag output stream metadata
# so Vidara/players show the correct language instead of "Unknown".
ISO2_TO_ISO3 = {
    "as": "asm", "te": "tel", "hi": "hin", "ta": "tam", "ml": "mal",
    "kn": "kan", "bn": "ben", "pa": "pan", "gu": "guj", "mr": "mar",
    "or": "ori", "en": "eng", "ja": "jpn", "ko": "kor", "es": "spa",
    "fr": "fre", "de": "ger", "ru": "rus", "zh": "chi", "it": "ita",
    "pt": "por", "ar": "ara", "tr": "tur",
}

# language display name -> iso3, built from the maps above, used when we've
# normalized a track (e.g. unknown subtitle -> "English") and need to stamp
# a matching iso3 tag rather than trusting whatever the source had.
NAME_TO_ISO3 = {}
for _code2, _name in LANG_MAP.items():
    _iso3 = ISO2_TO_ISO3.get(_code2)
    if _iso3 and _name not in NAME_TO_ISO3:
        NAME_TO_ISO3[_name] = _iso3


def iso3_for_language(language_name):
    return NAME_TO_ISO3.get(language_name, "und")


# ============================================================================
# NORMALIZATION
# ============================================================================

def normalize_audio_lang(raw_code, raw_name=None):
    """
    Audio must NEVER guess. If we cannot identify it, it stays 'Unknown'.
    """
    code = (raw_code or "").strip().lower()
    if code in LANG_MAP:
        return LANG_MAP[code]

    # try matching a full language name MediaInfo sometimes gives instead of a code
    name = (raw_name or "").strip()
    if name:
        for full in LANG_MAP.values():
            if name.lower() == full.lower():
                return full

    if code in UNKNOWN_TOKENS or not code:
        return "Unknown"

    # unrecognized code, still don't guess a language for it
    return "Unknown"


def normalize_subtitle_lang(raw_code, raw_name=None):
    """
    Subtitles: unknown/blank/und collapses to English.
    """
    code = (raw_code or "").strip().lower()
    if code in LANG_MAP:
        return LANG_MAP[code]

    name = (raw_name or "").strip()
    if name:
        for full in LANG_MAP.values():
            if name.lower() == full.lower():
                return full

    if code in UNKNOWN_TOKENS or not code:
        return "English"

    return "English"


# ============================================================================
# MEDIAINFO
# ============================================================================

def inspect_tracks(file_path):
    """
    Returns:
        audio_tracks = [ { "stream_index": int, "language": "English" }, ... ]
        subtitle_tracks = [ { "stream_index": int, "language": "English" }, ... ]
    stream_index is the 0-based index WITHIN that track type only, matching
    ffmpeg's `-map 0:a:<stream_index>` / `-map 0:s:<stream_index>` addressing.
    """
    media = MediaInfo.parse(str(file_path))
    audio_tracks = []
    subtitle_tracks = []
    audio_pos = 0
    sub_pos = 0

    for track in media.tracks:
        if track.track_type == "Audio":
            lang = normalize_audio_lang(track.language, getattr(track, "language_full", None))
            audio_tracks.append({"stream_index": audio_pos, "language": lang})
            audio_pos += 1
        elif track.track_type == "Text":
            lang = normalize_subtitle_lang(track.language, getattr(track, "language_full", None))
            subtitle_tracks.append({"stream_index": sub_pos, "language": lang})
            sub_pos += 1

    if not audio_tracks:
        audio_tracks = [{"stream_index": 0, "language": "Unknown"}]

    return audio_tracks, subtitle_tracks


# ============================================================================
# FFMPEG — remux only, never re-encode
# ============================================================================

def remux_single_audio(source_path, output_path, audio_track, subtitle_tracks):
    """
    Produces exactly one output file containing:
      - the original video stream
      - ONE specific audio stream (by its audio-only index)
      - all subtitle streams from this same source file (if any)
    All streams are stream-copied (-c copy) -> no quality loss, no re-encoding.

    IMPORTANT: we do NOT use a blanket -map_metadata -1. That strips per-stream
    language tags too (not just the global title), which is what caused
    subtitles/audio to show as "Unknown" downstream. Instead we only drop the
    global container metadata, and explicitly stamp each mapped stream with
    the language we've already normalized/decided on, so the output is always
    correctly tagged regardless of what the source container had.
    """
    audio_stream_index = audio_track["stream_index"]
    audio_iso3 = iso3_for_language(audio_track["language"])

    cmd = [
        "ffmpeg", "-y",
        "-i", str(source_path),
        "-map", "0:v:0",
        "-map", f"0:a:{audio_stream_index}",
    ]

    for sub in subtitle_tracks:
        cmd += ["-map", f"0:s:{sub['stream_index']}"]

    cmd += ["-c", "copy", "-map_chapters", "-1"]

    # Tag the single output audio stream (always index 0 in the output)
    cmd += ["-metadata:s:a:0", f"language={audio_iso3}"]

    # Tag each output subtitle stream in the same order they were mapped
    for out_idx, sub in enumerate(subtitle_tracks):
        sub_iso3 = iso3_for_language(sub["language"])
        cmd += [f"-metadata:s:s:{out_idx}", f"language={sub_iso3}"]

    cmd.append(str(output_path))

    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 or not os.path.exists(output_path) or os.path.getsize(output_path) < 1024:
        raise Exception(f"ffmpeg remux failed: {result.stderr[-500:] if result.stderr else 'unknown error'}")

    return True


# ============================================================================
# NAMING / VIDARA / BEAM / DOWNLOAD  (unchanged behavior from your existing script)
# ============================================================================

def clean_string_for_vidara(text):
    if not text:
        return ""
    text = text.replace(".", "")
    text = text.replace("/", "-")
    text = re.sub(r'[:*?"<>|]', "", text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def build_filename(tmdb_name, year, quality, language):
    clean_title = clean_string_for_vidara(tmdb_name)
    if year:
        return f"{clean_title} ({year}) {quality} {language}.mkv"
    return f"{clean_title} {quality} {language}.mkv"


def fetch_vidara_upload_server():
    try:
        res = requests.get("https://api.vidara.so/v1/upload/server", params={"api_key": VIDARA_API_KEY}, timeout=30)
        res.raise_for_status()
        data = res.json()
        return data.get("result", {}).get("upload_server") or data.get("upload_server") or "https://api.vidara.so/v1/upload/server"
    except Exception as e:
        print(f"    [WARN] Vidara server fetch failed: {e}")
        return "https://api.vidara.so/v1/upload/server"


def upload_to_vidara(file_path, custom_name):
    upload_server = fetch_vidara_upload_server()
    print(f"    Uploading to Vidara: {custom_name} ({round(os.path.getsize(file_path) / 1048576, 1)} MB)")

    with open(file_path, "rb") as fh:
        encoder = MultipartEncoder(fields={
            "api_key": VIDARA_API_KEY,
            "file": (custom_name, fh, "video/x-matroska")
        })
        monitor = MultipartEncoderMonitor(encoder)
        response = requests.post(upload_server, data=monitor, headers={"Content-Type": monitor.content_type}, timeout=None)

    if response.status_code == 200:
        data = response.json()
        filecode = (
            data.get("filecode")
            or data.get("url")
            or data.get("result", {}).get("url")
            or data.get("result", {}).get("filecode")
        )
        if not filecode:
            raise Exception(f"Vidara upload returned no filecode/url: {data}")
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


def beam_upsert(jwt, tmdb_id, quality, language, url):
    res = requests.post(f"{BEAM_WORKER_URL}/admin/vidara/upsert", json={
        "content_type": "movie",
        "tmdb_id": int(tmdb_id),
        "url": url,
        "quality": quality,
        "audio_languages": [language]
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


def safe_delete(path):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception as e:
        print(f"    [WARN] Could not delete {path}: {e}")


def format_error(link_number, language, stage, reason):
    lang_line = f"Language:\n{language}\n\n" if language else ""
    return (
        f"FAILED\n\n"
        f"Link #{link_number}\n\n"
        f"{lang_line}"
        f"Stage:\n{stage}\n\n"
        f"Reason:\n{reason}"
    )[:1500]


# ============================================================================
# CORE: process one quality cell for one row
# ============================================================================

def process_quality(jwt, tmdb_id, tmdb_name, year, quality, links_raw, row_idx):
    """
    Returns (status, error_text)
        status = "Done" or "Failed"
        error_text = "" on success, formatted failure detail otherwise
    Never raises — all failures are caught and turned into a status/error pair.
    """
    links = [l.strip() for l in links_raw.splitlines() if l.strip()]
    if not links:
        return "Done", ""  # nothing to do for this quality

    processed_languages = set()

    for link_number, link in enumerate(links, start=1):
        temp_path = os.path.join(TEMP_FOLDER, f"row{row_idx}_{quality}_link{link_number}.mkv")

        print(f"    -> {quality} Link #{link_number}: downloading...")
        try:
            ok = download_file(link, temp_path)
        except Exception as e:
            safe_delete(temp_path)
            return "Failed", format_error(link_number, None, "Download", str(e))

        if not ok:
            safe_delete(temp_path)
            return "Failed", format_error(link_number, None, "Download", "Download failed after retries")

        try:
            audio_tracks, subtitle_tracks = inspect_tracks(temp_path)
        except Exception as e:
            safe_delete(temp_path)
            return "Failed", format_error(link_number, None, "MediaInfo", str(e))

        print(f"       Found audio languages: {[a['language'] for a in audio_tracks]}")
        if subtitle_tracks:
            print(f"       Found subtitle languages: {[s['language'] for s in subtitle_tracks]}")

        for track in audio_tracks:
            lang = track["language"]

            if lang in processed_languages:
                print(f"       Skipping duplicate language: {lang}")
                continue

            output_name = build_filename(tmdb_name, year, quality, lang)
            output_path = os.path.join(OUTPUT_FOLDER, output_name)

            try:
                remux_single_audio(temp_path, output_path, track, subtitle_tracks)
            except Exception as e:
                safe_delete(output_path)
                safe_delete(temp_path)
                return "Failed", format_error(link_number, lang, "FFmpeg Remux", str(e))

            try:
                vidara_url = upload_to_vidara(output_path, output_name)
                beam_upsert(jwt, tmdb_id, quality, lang, vidara_url)
            except Exception as e:
                safe_delete(output_path)
                safe_delete(temp_path)
                return "Failed", format_error(link_number, lang, "Vidara Upload / BEAM Upsert", str(e))

            safe_delete(output_path)
            processed_languages.add(lang)
            print(f"       [OK] {lang} uploaded and registered.")

        safe_delete(temp_path)

    return "Done", ""


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 60)
    print("BEAM MOVIE DOWNLOADER v2 — STARTING")
    print("=" * 60)

    raw_json_str = os.environ.get("GOOGLE_SHEETS_JSON")
    if not raw_json_str:
        raise ValueError("GOOGLE_SHEETS_JSON is missing.")
    creds_dict = json.loads(raw_json_str)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    print("[OK] Connected to Google Sheets API")

    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    try:
        queue_sheet = spreadsheet.worksheet("Queue")
        archive_sheet = spreadsheet.worksheet("Archive")
    except Exception:
        queue_sheet = spreadsheet.get_worksheet(0)
        archive_sheet = spreadsheet.get_worksheet(1)
        print("[WARN] Named tabs not found, falling back to sheet indices 0/1.")

    raw_values = queue_sheet.get_all_values()
    if not raw_values:
        raise Exception("Queue worksheet is empty.")

    headers = [h.strip() for h in raw_values[0]]

    def col(name):
        return headers.index(name) + 1

    required = [
        "Filename", "Status", "TMDB_ID", "TMDB_NAME", "YEAR",
        "Link_1080p", "Link_720p", "Link_480p",
        "DOWNLOAD_STATUS_1080p", "DOWNLOAD_STATUS_720p", "DOWNLOAD_STATUS_480p",
        "Duplicate_Check", "ERROR_1080p", "ERROR_720p", "ERROR_480p"
    ]
    missing = [h for h in required if h not in headers]
    if missing:
        raise Exception(f"Missing required columns: {missing}. Found headers: {headers}")

    cols = {name: col(name) for name in required}

    all_rows = []
    for row_cells in raw_values[1:]:
        padded = row_cells + [""] * (len(headers) - len(row_cells))
        all_rows.append({headers[i]: padded[i] for i in range(len(headers)) if headers[i]})

    print(f"\nLoaded {len(all_rows)} rows.")

    jwt = beam_login()
    print("[OK] Logged into BEAM worker\n")

    QUALITIES = [
        ("1080p", "Link_1080p", "DOWNLOAD_STATUS_1080p", "ERROR_1080p"),
        ("720p", "Link_720p", "DOWNLOAD_STATUS_720p", "ERROR_720p"),
        ("480p", "Link_480p", "DOWNLOAD_STATUS_480p", "ERROR_480p"),
    ]

    for idx in range(len(all_rows) - 1, -1, -1):
        row = all_rows[idx]
        row_idx = idx + 2

        tmdb_id = str(row.get("TMDB_ID", "")).strip()
        tmdb_name = str(row.get("TMDB_NAME", "")).strip()
        year = str(row.get("YEAR", "")).strip()

        if not tmdb_id:
            continue

        if str(row.get("Duplicate_Check", "")).strip().upper() == "DUPLICATE":
            print(f"Skipping Row {row_idx}: DUPLICATE")
            continue

        print(f"\n{'='*60}\nRow {row_idx}: {tmdb_name} ({year})\n{'='*60}")

        row_final_statuses = {}

        for quality, link_col_name, status_col_name, error_col_name in QUALITIES:
            link_cell = str(row.get(link_col_name, "")).strip()
            current_status = str(row.get(status_col_name, "")).strip().lower()

            if not link_cell:
                row_final_statuses[quality] = current_status or ""
                continue

            if current_status == "done":
                row_final_statuses[quality] = "done"
                continue

            print(f"\n -> {quality}: starting (current status: '{current_status or 'empty'}')")

            # Checkpoint write: Running
            queue_sheet.update_cell(row_idx, cols[status_col_name], "Running")

            status, error_text = process_quality(
                jwt, tmdb_id, tmdb_name, year, quality, link_cell, row_idx
            )

            if status == "Done":
                # Checkpoint write: Done + clear error
                queue_sheet.update_cell(row_idx, cols[status_col_name], "Done")
                queue_sheet.update_cell(row_idx, cols[error_col_name], "")
                print(f"    [DONE] {quality} completed successfully.")
            else:
                # Checkpoint write: Failed + error detail
                queue_sheet.update_cell(row_idx, cols[status_col_name], "Failed")
                queue_sheet.update_cell(row_idx, cols[error_col_name], error_text)
                print(f"    [FAILED] {quality}:\n{error_text}")

            row_final_statuses[quality] = status.lower()

        # Archive check: only when every quality that HAS a link is Done
        present_qualities = [q for q, lc, _, _ in QUALITIES if str(row.get(lc, "")).strip()]
        all_done = all(row_final_statuses.get(q) == "done" for q in present_qualities) and present_qualities

        if all_done:
            print(f"\nRow {row_idx} fully completed. Archiving...")
            archive_row = [
                row.get("Filename", ""),
                row.get("Status", ""),
                tmdb_id,
                tmdb_name,
                year,
                row.get("Link_1080p", ""),
                row.get("Link_720p", ""),
                row.get("Link_480p", ""),
                "Done" if row.get("Link_1080p", "").strip() else "",
                "Done" if row.get("Link_720p", "").strip() else "",
                "Done" if row.get("Link_480p", "").strip() else "",
                row.get("Duplicate_Check", ""),
                "", "", ""
            ]
            archive_sheet.append_row(archive_row, value_input_option="USER_ENTERED")
            queue_sheet.delete_rows(row_idx)
            print(f"[OK] Row {row_idx} archived and removed from Queue.")

    try:
        shutil.rmtree(OUTPUT_FOLDER, ignore_errors=True)
        shutil.rmtree(TEMP_FOLDER, ignore_errors=True)
    except Exception:
        pass

    print(f"\n{'='*60}\nPIPELINE COMPLETE\n{'='*60}")


if __name__ == "__main__":
    main()
