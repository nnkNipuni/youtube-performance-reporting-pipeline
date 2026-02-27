"""
backfill.py — Run this script ONLY when you need to restore historical data.

Use cases:
- First time setup (initial 28-day backfill)
- If BigQuery table is accidentally deleted or corrupted
- If you need to add a new channel and want its historical data

DO NOT run this as part of the daily pipeline.
The daily pipeline (build_pipeline.py) handles ongoing data collection.

To change the backfill window, modify the BACKFILL_DAYS variable below.
"""

from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.cloud import bigquery
import isodate
import os
from datetime import datetime, timezone, timedelta

load_dotenv(override=True)

youtube = build("youtube", "v3", developerKey=os.getenv("YOUTUBE_API_KEY"))
bq = bigquery.Client(project=os.getenv("BQ_PROJECT"))
TABLE = f"{os.getenv('BQ_PROJECT')}.{os.getenv('BQ_DATASET')}.{os.getenv('BQ_TABLE')}"

CHANNEL_IDS = [c.strip() for c in os.getenv("CHANNEL_IDS", "").split(",")]
now_utc = datetime.now(timezone.utc)

# Change this value to backfill more or fewer days
BACKFILL_DAYS = 28

cutoff = now_utc - timedelta(days=BACKFILL_DAYS)
published_after = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"Starting backfill for last {BACKFILL_DAYS} days ({published_after} to now)", flush=True)

def get_channel_info(channel_id):
    resp = youtube.channels().list(part="snippet", id=channel_id).execute()
    item = resp["items"][0]
    return item["snippet"]["title"]

def search_videos(channel_id):
    video_ids = []
    next_page_token = None
    page = 0
    while True:
        page += 1
        print(f"    Searching page {page}... (collected {len(video_ids)} videos so far)", flush=True)
        resp = youtube.search().list(
            part="id",
            channelId=channel_id,
            type="video",
            publishedAfter=published_after,
            maxResults=50,
            pageToken=next_page_token,
            order="date"
        ).execute()
        for item in resp["items"]:
            video_ids.append(item["id"]["videoId"])
        next_page_token = resp.get("nextPageToken")
        if not next_page_token:
            break
    return video_ids

def get_playlist_map(channel_id):
    print(f"  Fetching playlists...", flush=True)
    video_to_playlist = {}
    next_page_token = None
    playlists = []

    while True:
        resp = youtube.playlists().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in resp["items"]:
            playlists.append({
                "playlist_id": item["id"],
                "playlist_name": item["snippet"]["title"]
            })
        next_page_token = resp.get("nextPageToken")
        if not next_page_token:
            break

    print(f"  Found {len(playlists)} playlists. Mapping videos...", flush=True)

    for pl in playlists:
        next_page_token = None
        while True:
            resp = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=pl["playlist_id"],
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            for item in resp["items"]:
                vid = item["contentDetails"]["videoId"]
                published_at = item["contentDetails"].get("videoPublishedAt")
                if published_at:
                    publish_time = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                    if publish_time >= cutoff and vid not in video_to_playlist:
                        video_to_playlist[vid] = {
                            "playlist_id": pl["playlist_id"],
                            "playlist_name": pl["playlist_name"]
                        }
            next_page_token = resp.get("nextPageToken")
            if not next_page_token:
                break

    print(f"  Mapped {len(video_to_playlist)} videos to playlists.", flush=True)
    return video_to_playlist

def get_video_details(video_ids, channel_name, channel_id, playlist_map):
    rows = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        print(f"    Fetching details for videos {i+1} to {i+len(batch)}...", flush=True)
        resp = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(batch)
        ).execute()
        for item in resp["items"]:
            vid_id = item["id"]
            duration_iso = item.get("contentDetails", {}).get("duration")
            duration_seconds = int(isodate.parse_duration(duration_iso).total_seconds()) if duration_iso else 0
            pl_info = playlist_map.get(vid_id, {})
            rows.append({
                "video_id": vid_id,
                "video_title": item["snippet"]["title"],
                "channel_name": channel_name,
                "channel_id": channel_id,
                "playlist_id": pl_info.get("playlist_id", None),
                "playlist_name": pl_info.get("playlist_name", None),
                "published_at": item["snippet"]["publishedAt"],
                "video_duration_seconds": duration_seconds,
                "like_count": int(item.get("statistics", {}).get("likeCount", 0)),
                "view_count": int(item.get("statistics", {}).get("viewCount", 0)),
                "comment_count": int(item.get("statistics", {}).get("commentCount", 0)),
                "data_capture_date": now_utc.date().isoformat(),
                "data_capture_timestamp_utc": now_utc.isoformat(),
            })
    return rows

all_rows = []
for channel_id in CHANNEL_IDS:
    print(f"Processing channel: {channel_id}", flush=True)
    channel_name = get_channel_info(channel_id)
    print(f"  Channel name: {channel_name}", flush=True)
    playlist_map = get_playlist_map(channel_id)
    print(f"  Searching for videos published after {published_after}...", flush=True)
    video_ids = search_videos(channel_id)
    print(f"  Found {len(video_ids)} videos in last {BACKFILL_DAYS} days", flush=True)
    print(f"  Fetching video details...", flush=True)
    rows = get_video_details(video_ids, channel_name, channel_id, playlist_map)
    all_rows.extend(rows)

print(f"Total rows to insert: {len(all_rows)}", flush=True)

if all_rows:
    errors = bq.insert_rows_json(TABLE, all_rows)
    if errors:
        print("Errors:", errors)
    else:
        print(f"Successfully inserted {len(all_rows)} rows!")
else:
    print(f"No videos found in last {BACKFILL_DAYS} days.")
