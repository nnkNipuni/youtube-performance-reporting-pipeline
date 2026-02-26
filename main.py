import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.cloud import bigquery
import isodate

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")
CHANNEL_IDS = [c.strip() for c in os.getenv("CHANNEL_IDS", "").split(",") if c.strip()]

if not YOUTUBE_API_KEY:
    raise ValueError("Missing YOUTUBE_API_KEY in .env")
if not (BQ_PROJECT and BQ_DATASET and BQ_TABLE):
    raise ValueError("Missing BigQuery config (BQ_PROJECT/BQ_DATASET/BQ_TABLE) in .env")
if not CHANNEL_IDS:
    raise ValueError("Missing CHANNEL_IDS in .env (comma-separated channel IDs)")

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
bq_client = bigquery.Client(project=BQ_PROJECT)

TABLE_FQN = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_uploads_playlist_id(channel_id: str) -> Optional[str]:
    resp = youtube.channels().list(
        part="contentDetails,snippet",
        id=channel_id
    ).execute()

    items = resp.get("items", [])
    if not items:
        return None

    uploads = items[0]["contentDetails"]["relatedPlaylists"].get("uploads")
    return uploads


def get_channel_name(channel_id: str) -> Optional[str]:
    resp = youtube.channels().list(
        part="snippet",
        id=channel_id
    ).execute()
    items = resp.get("items", [])
    if not items:
        return None
    return items[0]["snippet"].get("title")


def list_recent_upload_video_ids(uploads_playlist_id: str, start_utc: datetime) -> List[str]:
    """
    Pull video IDs from the uploads playlist and filter by publishedAt >= start_utc.
    Note: playlistItems returns publishedAt of the playlist item.
    """
    video_ids = []
    page_token = None

    while True:
        resp = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=page_token
        ).execute()

        for it in resp.get("items", []):
            published_at = it["contentDetails"].get("videoPublishedAt")
            vid = it["contentDetails"].get("videoId")
            if not (published_at and vid):
                continue

            published_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            if published_dt >= start_utc:
                video_ids.append(vid)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

        # Small optimization: if we already passed the 24h window (playlist sorted newest first),
        # we can stop when we see items older than start_utc.
        # But since the API returns only contentDetails here, we can’t guarantee ordering in all cases.
        # Keeping it simple and safe.

    # Remove duplicates, keep stable order
    seen = set()
    out = []
    for v in video_ids:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def fetch_videos_details(video_ids: List[str]) -> Dict[str, dict]:
    """
    Returns mapping video_id -> merged data from videos().list
    """
    details = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        resp = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(batch)
        ).execute()

        for it in resp.get("items", []):
            details[it["id"]] = it
    return details


def seconds_from_duration_iso8601(dur: str) -> Optional[int]:
    try:
        td = isodate.parse_duration(dur)
        return int(td.total_seconds())
    except Exception:
        return None


def insert_rows(rows: List[dict]) -> None:
    if not rows:
        return
    errors = bq_client.insert_rows_json(TABLE_FQN, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")


def main():
    end_utc = utc_now()
    start_utc = end_utc - timedelta(hours=24)

    capture_date = end_utc.date()
    capture_ts = end_utc

    all_rows = []

    for channel_id in CHANNEL_IDS:
        channel_name = get_channel_name(channel_id) or ""
        uploads_playlist_id = get_uploads_playlist_id(channel_id)

        if not uploads_playlist_id:
            print(f"[WARN] No uploads playlist for channel {channel_id}")
            continue

        recent_video_ids = list_recent_upload_video_ids(uploads_playlist_id, start_utc)

        if not recent_video_ids:
            print(f"[INFO] No videos in last 24h for channel {channel_id} ({channel_name})")
            continue

        vid_details = fetch_videos_details(recent_video_ids)

        for vid in recent_video_ids:
            it = vid_details.get(vid)
            if not it:
                continue

            snippet = it.get("snippet", {})
            stats = it.get("statistics", {})
            content = it.get("contentDetails", {})

            published_at_str = snippet.get("publishedAt")
            published_at = None
            if published_at_str:
                published_at = datetime.fromisoformat(published_at_str.replace("Z", "+00:00"))

            dur_iso = content.get("duration")
            dur_seconds = seconds_from_duration_iso8601(dur_iso) if dur_iso else None

            # Playlist fields: YouTube videos().list does not directly give playlist membership.
            # We leave these as None unless you later add an extra mapping step.
            row = {
                "video_id": vid,
                "video_title": snippet.get("title"),
                "channel_id": channel_id,
                "channel_name": channel_name,

                "playlist_id": None,
                "playlist_name": None,

                "published_at": published_at.isoformat() if published_at else None,
                "video_duration_seconds": dur_seconds,

                "like_count": int(stats.get("likeCount", 0)) if stats.get("likeCount") is not None else None,
                "view_count": int(stats.get("viewCount", 0)) if stats.get("viewCount") is not None else None,
                "comment_count": int(stats.get("commentCount", 0)) if stats.get("commentCount") is not None else None,

                "data_capture_date": str(capture_date),
                "data_capture_timestamp_utc": capture_ts.isoformat()
            }
            all_rows.append(row)

    insert_rows(all_rows)
    print(f"[DONE] Inserted {len(all_rows)} rows into {TABLE_FQN} for window {start_utc.isoformat()} to {end_utc.isoformat()}")

if __name__ == "__main__":
    main()