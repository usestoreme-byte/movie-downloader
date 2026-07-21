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
import time
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

# Internet Archive S3-style credentials, used to host extracted English
# subtitles so Vidara can fetch them by direct URL.
# SECURITY NOTE: hardcoded here only for quick testing — swap these for a
# GitHub Secret (IA_ACCESS_KEY / IA_SECRET_KEY, same pattern as
# VIDARA_API_KEY above) before running this long-term. Anyone with read
# access to this file/repo gets full write access to your IA account
# with these sitting here in plain text.
IA_ACCESS_KEY = os.environ.get("IA_ACCESS_KEY", "EQ6XJ3AACbxfK4n7").strip()
IA_SECRET_KEY = os.environ.get("IA_SECRET_KEY", "BlzN7vT0uJo7g3n2").strip()

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


def extract_vidara_urls(data):
    """
    Returns (full_url, bare_filecode).

    full_url: whatever URL Vidara actually returned in `url` (or
    result.url), stored AS-IS into BEAM — Vidara's embed domain has changed
    more than once (vidara.so -> vidaraa.cc -> vidara.to), so reconstructing
    or hardcoding a domain is fragile. Store exactly what they give back.

    bare_filecode: just the last path segment, needed ONLY internally for
    the subtitle-attach API, which requires the bare code rather than a URL.
    """
    full_url = data.get("url") or data.get("result", {}).get("url")
    filecode = data.get("filecode") or data.get("result", {}).get("filecode")

    if not full_url and not filecode:
        raise Exception(f"Vidara upload returned no url/filecode: {data}")

    if not full_url:
        full_url = filecode

    if not filecode:
        filecode = full_url.rstrip("/").split("/")[-1]

    return full_url, filecode


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
        return extract_vidara_urls(data)  # (full_url, filecode)
    else:
        raise Exception(f"Vidara upload failed: {response.status_code} {response.text[:200]}")


# ============================================================================
# SUBTITLES — extract English tracks only, host them permanently and freely
# on Internet Archive, then tell Vidara to attach that URL to the uploaded
# video's filecode. Extracting straight from the same source file we split
# the audio from guarantees the subtitle timing matches — no separate
# re-sync possible.
# ============================================================================

def extract_subtitle_to_srt(source_path, subtitle_stream_index, output_srt_path):
    """
    Pulls ONE subtitle stream out of the source file as a standalone .srt.
    Text-based subtitle codecs (srt/ass/webvtt/etc.) convert to srt cleanly
    via -c:s srt. If a track is image-based (e.g. PGS/VobSub) ffmpeg can't
    convert it to srt and this will fail — that's expected and handled by
    the caller as a skip, not a hard error.
    """
    cmd = [
        "ffmpeg", "-y",
        "-i", str(source_path),
        "-map", f"0:s:{subtitle_stream_index}",
        "-c:s", "srt",
        str(output_srt_path)
    ]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 or not os.path.exists(output_srt_path) or os.path.getsize(output_srt_path) < 10:
        raise Exception(f"ffmpeg subtitle extraction failed: {result.stderr[-300:] if result.stderr else 'unknown error'}")
    return True


def slugify_for_ia(text, max_len=80):
    """
    Internet Archive item/bucket identifiers and S3 keys only allow
    alphanumerics, -, _, . — anything else gets collapsed to a dash.
    """
    text = re.sub(r'[^a-zA-Z0-9\-_.]', '-', text or "")
    text = re.sub(r'-+', '-', text).strip('-_.')
    return (text.lower() or "item")[:max_len]


def upload_to_archive_org(file_path, bucket_hint, key_hint, content_type="application/x-subrip", wait_seconds=60):
    """
    Uploads via Internet Archive's S3-compatible endpoint. `bucket_hint`
    should be stable per movie so multiple subtitle files land in the same
    IA "item" instead of creating a new one per file. x-amz-auto-make-bucket
    creates that item automatically if it doesn't exist yet. Storage is
    free and permanent — no expiry to manage.

    IA can take anywhere from a few seconds to a couple minutes to make a
    freshly uploaded file publicly fetchable, so this polls the direct
    download URL briefly before handing it back — Vidara needs to fetch it
    immediately, so handing back a URL that 404s yet would just move the
    same failure mode over to a different host.
    """
    bucket = slugify_for_ia(f"beamplay-subs-{bucket_hint}")
    key = slugify_for_ia(key_hint) + ".srt"
    upload_url = f"https://s3.us.archive.org/{bucket}/{key}"

    headers = {
        "authorization": f"LOW {IA_ACCESS_KEY}:{IA_SECRET_KEY}",
        "x-amz-auto-make-bucket": "1",
        "x-archive-meta-mediatype": "texts",
        "x-archive-meta-collection": "opensource",
        "x-archive-ignore-preexisting-bucket": "1",
        "Content-Type": content_type,
    }

    with open(file_path, "rb") as fh:
        data = fh.read()

    response = requests.put(upload_url, data=data, headers=headers, timeout=60)
    if response.status_code not in (200, 201):
        raise Exception(f"Archive.org upload failed: {response.status_code} {response.text[:200]}")

    direct_url = f"https://archive.org/download/{bucket}/{key}"

    attempts = max(1, wait_seconds // 5)
    for _ in range(attempts):
        try:
            check = requests.head(direct_url, timeout=10, allow_redirects=True)
            if check.status_code == 200:
                return direct_url
        except Exception:
            pass
        time.sleep(5)

    print(f"       [WARN] Archive.org file not confirmed reachable after {wait_seconds}s, proceeding anyway: {direct_url}")
    return direct_url


def attach_subtitle_to_vidara(filecode, sub_url, sub_lang="English"):
    res = requests.get(
        "https://api.vidara.so/v1/upload/sub",
        params={"api_key": VIDARA_API_KEY, "filecode": filecode, "sub_lang": sub_lang, "sub_url": sub_url},
        timeout=30
    )
    res.raise_for_status()
    data = res.json()
    if data.get("status") != 200:
        raise Exception(f"Vidara subtitle attach failed: {data}")
    return True


def prepare_english_subtitle_urls(source_path, subtitle_tracks, bucket_hint, tmp_prefix):
    """
    Extracts every subtitle track normalized to 'English', uploads each to
    Internet Archive (one IA "item" per bucket_hint, reused across every
    subtitle for that movie instead of creating a new item per file), and
    returns (urls, failure_reasons). Best-effort: a track that fails to
    extract/upload is skipped (reason recorded) rather than aborting the
    whole link — the video itself matters more than a caption attach.
    """
    urls = []
    failures = []
    english_tracks = [s for s in subtitle_tracks if s["language"] == "English"]
    if not english_tracks:
        return urls, failures

    for idx, sub in enumerate(english_tracks):
        srt_path = os.path.join(TEMP_FOLDER, f"{tmp_prefix}_sub{idx}.srt")
        try:
            extract_subtitle_to_srt(source_path, sub["stream_index"], srt_path)
            url = upload_to_archive_org(srt_path, bucket_hint, f"{tmp_prefix}_sub{idx}")
            urls.append(url)
            print(f"       [SUB] English subtitle #{idx+1} hosted -> {url}")
        except Exception as e:
            failures.append(f"track #{idx+1}: {e}")
            print(f"       [WARN] Could not prepare English subtitle #{idx+1}: {e}")
        finally:
            safe_delete(srt_path)

    return urls, failures


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
    Returns (status, error_text, subtitle_warnings)
        status = "Done" or "Failed"
        error_text = "" on success, formatted failure detail otherwise
        subtitle_warnings = list of human-readable notes for captions that
            didn't attach (video still uploaded fine) — includes the direct
            Archive.org link so it can be downloaded and attached by hand.
    Never raises — all failures are caught and turned into a status/error pair.
    """
    links = [l.strip() for l in links_raw.splitlines() if l.strip()]
    if not links:
        return "Done", "", []  # nothing to do for this quality

    processed_languages = set()
    subtitle_warnings = []

    for link_number, link in enumerate(links, start=1):
        temp_path = os.path.join(TEMP_FOLDER, f"row{row_idx}_{quality}_link{link_number}.mkv")

        print(f"    -> {quality} Link #{link_number}: downloading...")
        try:
            ok = download_file(link, temp_path)
        except Exception as e:
            safe_delete(temp_path)
            return "Failed", format_error(link_number, None, "Download", str(e)), subtitle_warnings

        if not ok:
            safe_delete(temp_path)
            return "Failed", format_error(link_number, None, "Download", "Download failed after retries"), subtitle_warnings

        try:
            audio_tracks, subtitle_tracks = inspect_tracks(temp_path)
        except Exception as e:
            safe_delete(temp_path)
            return "Failed", format_error(link_number, None, "MediaInfo", str(e)), subtitle_warnings

        print(f"       Found audio languages: {[a['language'] for a in audio_tracks]}")
        if subtitle_tracks:
            print(f"       Found subtitle languages: {[s['language'] for s in subtitle_tracks]}")

        # Extract + host every English subtitle track ONCE per link — same
        # captions get attached to every audio-language video we upload
        # below, so there's no need to redo this per audio track.
        subtitle_urls, prep_failures = prepare_english_subtitle_urls(
            temp_path, subtitle_tracks, f"{tmdb_id}", f"{tmdb_id}_{quality}_link{link_number}"
        )
        for fail_reason in prep_failures:
            subtitle_warnings.append(
                f"{quality} Link #{link_number}: could not extract/host English subtitle — {fail_reason}"
            )

        for track in audio_tracks:
            lang = track["language"]

            if lang in processed_languages:
                print(f"       Skipping duplicate language: {lang}")
                continue

            output_name = build_filename(tmdb_name, year, quality, lang)
            output_path = os.path.join(OUTPUT_FOLDER, output_name)

            try:
                remux_single_audio(temp_path, output_path, track, [])  # subs handled via API below, not embedded
            except Exception as e:
                safe_delete(output_path)
                safe_delete(temp_path)
                return "Failed", format_error(link_number, lang, "FFmpeg Remux", str(e)), subtitle_warnings

            try:
                video_url, filecode = upload_to_vidara(output_path, output_name)
                beam_upsert(jwt, tmdb_id, quality, lang, video_url)
            except Exception as e:
                safe_delete(output_path)
                safe_delete(temp_path)
                return "Failed", format_error(link_number, lang, "Vidara Upload / BEAM Upsert", str(e)), subtitle_warnings

            safe_delete(output_path)
            processed_languages.add(lang)
            print(f"       [OK] {lang} uploaded and registered ({video_url}).")

            # Best-effort: attach every prepared English subtitle to this filecode.
            for sub_url in subtitle_urls:
                try:
                    attach_subtitle_to_vidara(filecode, sub_url, sub_lang="English")
                    print(f"       [SUB] Attached English caption to {filecode}")
                except Exception as e:
                    warning = (
                        f"{quality} Link #{link_number} [{lang}] video {video_url} (filecode {filecode}): "
                        f"video uploaded OK but subtitle attach failed ({e}). "
                        f"Download the caption yourself here: {sub_url}"
                    )
                    subtitle_warnings.append(warning)
                    print(f"       [WARN] {warning}")

        safe_delete(temp_path)

    return "Done", "", subtitle_warnings


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
        row_final_errors = {}

        for quality, link_col_name, status_col_name, error_col_name in QUALITIES:
            link_cell = str(row.get(link_col_name, "")).strip()
            current_status = str(row.get(status_col_name, "")).strip().lower()

            if not link_cell:
                row_final_statuses[quality] = current_status or ""
                row_final_errors[quality] = str(row.get(error_col_name, "")).strip()
                continue

            if current_status == "done":
                row_final_statuses[quality] = "done"
                # Preserve any existing note (e.g. a past subtitle warning)
                # for a quality we're not touching again this run.
                row_final_errors[quality] = str(row.get(error_col_name, "")).strip()
                continue

            print(f"\n -> {quality}: starting (current status: '{current_status or 'empty'}')")

            # Checkpoint write: Running
            queue_sheet.update_cell(row_idx, cols[status_col_name], "Running")

            status, error_text, subtitle_warnings = process_quality(
                jwt, tmdb_id, tmdb_name, year, quality, link_cell, row_idx
            )

            if status == "Done":
                queue_sheet.update_cell(row_idx, cols[status_col_name], "Done")
                if subtitle_warnings:
                    # Still Done — video uploaded fine. Error just notes
                    # which captions need manual attaching, with the direct
                    # download link for each one.
                    note = "DONE — but some subtitles need manual attach:\n\n" + "\n\n".join(subtitle_warnings)
                    note = note[:1500]
                    queue_sheet.update_cell(row_idx, cols[error_col_name], note)
                    row_final_errors[quality] = note
                    print(f"    [DONE with subtitle warnings] {quality}")
                else:
                    queue_sheet.update_cell(row_idx, cols[error_col_name], "")
                    row_final_errors[quality] = ""
                    print(f"    [DONE] {quality} completed successfully.")
            else:
                # Checkpoint write: Failed + error detail
                queue_sheet.update_cell(row_idx, cols[status_col_name], "Failed")
                queue_sheet.update_cell(row_idx, cols[error_col_name], error_text)
                row_final_errors[quality] = error_text
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
                row_final_errors.get("1080p", ""),
                row_final_errors.get("720p", ""),
                row_final_errors.get("480p", ""),
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
