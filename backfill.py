
# """
# backfill.py — Smart backfill script.

# This script:
# 1. Checks what data already exists in BigQuery
# 2. Identifies the earliest video date in the database
# 3. Fetches any missing videos going back 28 days from today
# 4. Automatically removes duplicates before inserting
# 5. Safe to run multiple times — will never create duplicates

# Run this when:
# - First time setup
# - Data was accidentally deleted
# - You want to ensure no gaps in historical data
# """

# from dotenv import load_dotenv
# from googleapiclient.discovery import build
# from google.cloud import bigquery
# import isodate
# import os
# from datetime import datetime, timezone, timedelta

# load_dotenv(override=True)

# youtube = build("youtube", "v3", developerKey=os.getenv("YOUTUBE_API_KEY"))
# bq = bigquery.Client(project=os.getenv("BQ_PROJECT"))
# TABLE = f"{os.getenv('BQ_PROJECT')}.{os.getenv('BQ_DATASET')}.{os.getenv('BQ_TABLE')}"

# CHANNEL_IDS = [c.strip() for c in os.getenv("CHANNEL_IDS", "").split(",")]
# now_utc = datetime.now(timezone.utc)
# BACKFILL_DAYS = 28
# full_cutoff = now_utc - timedelta(days=BACKFILL_DAYS)

# # --- Step 1: Check what video IDs already exist in the database ---
# print("Checking existing data in BigQuery...", flush=True)
# existing_query = f"SELECT DISTINCT video_id FROM `{TABLE}`"
# existing_rows = bq.query(existing_query).result()
# existing_video_ids = set(row.video_id for row in existing_rows)
# print(f"Found {len(existing_video_ids)} existing videos in database.", flush=True)

# # --- Step 2: Check earliest published_at in database ---
# if existing_video_ids:
#     date_query = f"""
#         SELECT 
#             MIN(published_at) as oldest,
#             MAX(published_at) as newest
#         FROM `{TABLE}`
#     """
#     date_result = list(bq.query(date_query).result())[0]
#     print(f"Oldest video in DB: {date_result.oldest}", flush=True)
#     print(f"Newest video in DB: {date_result.newest}", flush=True)
# else:
#     print("Database is empty. Will fetch full 28 days.", flush=True)

# print(f"Will fetch videos published after: {full_cutoff.strftime('%Y-%m-%dT%H:%M:%SZ')}", flush=True)

# # --- Helper functions ---
# def get_channel_info(channel_id):
#     resp = youtube.channels().list(part="snippet", id=channel_id).execute()
#     item = resp["items"][0]
#     return item["snippet"]["title"]

# def search_videos(channel_id):
#     video_ids = []
#     next_page_token = None
#     page = 0
#     published_after = full_cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
#     while True:
#         page += 1
#         print(f"    Searching page {page}... (collected {len(video_ids)} videos so far)", flush=True)
#         resp = youtube.search().list(
#             part="id",
#             channelId=channel_id,
#             type="video",
#             publishedAfter=published_after,
#             maxResults=50,
#             pageToken=next_page_token,
#             order="date"
#         ).execute()
#         for item in resp["items"]:
#             video_ids.append(item["id"]["videoId"])
#         next_page_token = resp.get("nextPageToken")
#         if not next_page_token:
#             break
#     return video_ids

# def get_playlist_map(channel_id):
#     print(f"  Fetching playlists...", flush=True)
#     video_to_playlist = {}
#     next_page_token = None
#     playlists = []
#     while True:
#         resp = youtube.playlists().list(
#             part="snippet",
#             channelId=channel_id,
#             maxResults=50,
#             pageToken=next_page_token
#         ).execute()
#         for item in resp["items"]:
#             playlists.append({
#                 "playlist_id": item["id"],
#                 "playlist_name": item["snippet"]["title"]
#             })
#         next_page_token = resp.get("nextPageToken")
#         if not next_page_token:
#             break

#     print(f"  Found {len(playlists)} playlists. Mapping videos...", flush=True)
#     for pl in playlists:
#         next_page_token = None
#         while True:
#             resp = youtube.playlistItems().list(
#                 part="contentDetails",
#                 playlistId=pl["playlist_id"],
#                 maxResults=50,
#                 pageToken=next_page_token
#             ).execute()
#             for item in resp["items"]:
#                 vid = item["contentDetails"]["videoId"]
#                 published_at = item["contentDetails"].get("videoPublishedAt")
#                 if published_at:
#                     publish_time = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
#                     if publish_time >= full_cutoff and vid not in video_to_playlist:
#                         video_to_playlist[vid] = {
#                             "playlist_id": pl["playlist_id"],
#                             "playlist_name": pl["playlist_name"]
#                         }
#             next_page_token = resp.get("nextPageToken")
#             if not next_page_token:
#                 break

#     print(f"  Mapped {len(video_to_playlist)} videos to playlists.", flush=True)
#     return video_to_playlist

# def get_video_details(video_ids, channel_name, channel_id, playlist_map):
#     rows = []
#     for i in range(0, len(video_ids), 50):
#         batch = video_ids[i:i+50]
#         print(f"    Fetching details for videos {i+1} to {i+len(batch)}...", flush=True)
#         resp = youtube.videos().list(
#             part="snippet,contentDetails,statistics",
#             id=",".join(batch)
#         ).execute()
#         for item in resp["items"]:
#             vid_id = item["id"]
#             duration_iso = item.get("contentDetails", {}).get("duration")
#             duration_seconds = int(isodate.parse_duration(duration_iso).total_seconds()) if duration_iso else 0
#             pl_info = playlist_map.get(vid_id, {})
#             rows.append({
#                 "video_id": vid_id,
#                 "video_title": item["snippet"]["title"],
#                 "channel_name": channel_name,
#                 "channel_id": channel_id,
#                 "playlist_id": pl_info.get("playlist_id", None),
#                 "playlist_name": pl_info.get("playlist_name", None),
#                 "published_at": item["snippet"]["publishedAt"],
#                 "video_duration_seconds": duration_seconds,
#                 "like_count": int(item.get("statistics", {}).get("likeCount", 0)),
#                 "view_count": int(item.get("statistics", {}).get("viewCount", 0)),
#                 "comment_count": int(item.get("statistics", {}).get("commentCount", 0)),
#                 "data_capture_date": now_utc.date().isoformat(),
#                 "data_capture_timestamp_utc": now_utc.isoformat(),
#             })
#     return rows

# # --- Step 3: Fetch all videos from last 28 days ---
# all_rows = []
# for channel_id in CHANNEL_IDS:
#     print(f"\nProcessing channel: {channel_id}", flush=True)
#     channel_name = get_channel_info(channel_id)
#     print(f"  Channel name: {channel_name}", flush=True)
#     playlist_map = get_playlist_map(channel_id)
#     print(f"  Searching for all videos in last {BACKFILL_DAYS} days...", flush=True)
#     video_ids = search_videos(channel_id)
#     print(f"  Found {len(video_ids)} videos total", flush=True)

#     # Step 4: Filter out videos already in database
#     missing_video_ids = [v for v in video_ids if v not in existing_video_ids]
#     print(f"  Already in database: {len(video_ids) - len(missing_video_ids)}", flush=True)
#     print(f"  Missing from database: {len(missing_video_ids)}", flush=True)

#     if missing_video_ids:
#         print(f"  Fetching details for missing videos...", flush=True)
#         rows = get_video_details(missing_video_ids, channel_name, channel_id, playlist_map)
#         all_rows.extend(rows)
#     else:
#         print(f"  No missing videos for this channel!", flush=True)

# print(f"\nTotal missing rows to insert: {len(all_rows)}", flush=True)

# if all_rows:
#     # Step 5: Remove any duplicates in the existing table before inserting
#     print("Removing any existing duplicates from database...", flush=True)
#     dedup_query = f"""
#         CREATE OR REPLACE TABLE `{TABLE}` AS
#         SELECT * EXCEPT(rn)
#         FROM (
#             SELECT *,
#                 ROW_NUMBER() OVER (
#                     PARTITION BY video_id, data_capture_date
#                     ORDER BY data_capture_timestamp_utc DESC
#                 ) as rn
#             FROM `{TABLE}`
#         )
#         WHERE rn = 1
#     """
#     bq.query(dedup_query).result()
#     print("Deduplication complete.", flush=True)

#     # Step 6: Insert missing rows
#     errors = bq.insert_rows_json(TABLE, all_rows)
#     if errors:
#         print("Errors during insert:", errors)
#     else:
#         print(f"Successfully inserted {len(all_rows)} missing rows!", flush=True)

#     # Step 7: Final count
#     count_result = list(bq.query(f"SELECT COUNT(*) as total FROM `{TABLE}`").result())[0]
#     print(f"\nFinal row count in database: {count_result.total}", flush=True)
# else:
#     print("No missing videos found! Database is already complete.", flush=True)


"""
backfill.py — Full historical backfill script.

This script:
1. Fetches ALL videos from each channel (entire history, no date limit)
2. Checks what video IDs already exist in BigQuery
3. Inserts only the missing videos
4. Automatically removes duplicates before inserting
5. Safe to run multiple times — will never create duplicates

Run this when:
- First time setup
- Database was accidentally deleted or corrupted
- You want to ensure no gaps in historical data

DO NOT run this as part of the daily pipeline.
The daily pipeline (build_pipeline.py) handles ongoing data collection.
"""

from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.cloud import bigquery
import isodate
import os
from datetime import datetime, timezone

load_dotenv(override=True)

youtube = build("youtube", "v3", developerKey=os.getenv("YOUTUBE_API_KEY"))
bq = bigquery.Client(project=os.getenv("BQ_PROJECT"))
TABLE = f"{os.getenv('BQ_PROJECT')}.{os.getenv('BQ_DATASET')}.{os.getenv('BQ_TABLE')}"

CHANNEL_IDS = [c.strip() for c in os.getenv("CHANNEL_IDS", "").split(",")]
now_utc = datetime.now(timezone.utc)

# --- Step 1: Check what video IDs already exist in the database ---
print("Checking existing data in BigQuery...", flush=True)
try:
    existing_rows = bq.query(f"SELECT DISTINCT video_id FROM `{TABLE}`").result()
    existing_video_ids = set(row.video_id for row in existing_rows)
    print(f"Found {len(existing_video_ids)} existing videos in database.", flush=True)
except Exception as e:
    print(f"Could not query existing data (table may be empty): {e}", flush=True)
    existing_video_ids = set()

# --- Helper functions ---
def get_channel_info(channel_id):
    resp = youtube.channels().list(part="snippet", id=channel_id).execute()
    item = resp["items"][0]
    return item["snippet"]["title"]

def search_all_videos(channel_id):
    """Fetch ALL video IDs from a channel with no date filter."""
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
    """Build a map of video_id -> (playlist_id, playlist_name)."""
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
                if vid not in video_to_playlist:
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

# --- Step 2: Fetch ALL videos from each channel ---
all_rows = []
for channel_id in CHANNEL_IDS:
    print(f"\nProcessing channel: {channel_id}", flush=True)
    channel_name = get_channel_info(channel_id)
    print(f"  Channel name: {channel_name}", flush=True)

    # Fetch playlist map for this channel
    playlist_map = get_playlist_map(channel_id)

    # Fetch ALL video IDs (no date filter)
    print(f"  Searching for ALL videos in channel history...", flush=True)
    video_ids = search_all_videos(channel_id)
    print(f"  Found {len(video_ids)} total videos in channel", flush=True)

    # Step 3: Filter out videos already in database
    missing_video_ids = [v for v in video_ids if v not in existing_video_ids]
    print(f"  Already in database: {len(video_ids) - len(missing_video_ids)}", flush=True)
    print(f"  Missing from database: {len(missing_video_ids)}", flush=True)

    if missing_video_ids:
        print(f"  Fetching details for missing videos...", flush=True)
        rows = get_video_details(missing_video_ids, channel_name, channel_id, playlist_map)
        all_rows.extend(rows)
    else:
        print(f"  No missing videos for this channel!", flush=True)

print(f"\nTotal missing rows to insert: {len(all_rows)}", flush=True)

if all_rows:
    # Step 4: Remove any duplicates in the existing table before inserting
    print("Removing any existing duplicates from database...", flush=True)
    dedup_query = f"""
        CREATE OR REPLACE TABLE `{TABLE}` AS
        SELECT * EXCEPT(rn)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY video_id, data_capture_date
                    ORDER BY data_capture_timestamp_utc DESC
                ) as rn
            FROM `{TABLE}`
        )
        WHERE rn = 1
    """
    bq.query(dedup_query).result()
    print("Deduplication complete.", flush=True)

    # Step 5: Insert missing rows
    errors = bq.insert_rows_json(TABLE, all_rows)
    if errors:
        print("Errors during insert:", errors)
    else:
        print(f"Successfully inserted {len(all_rows)} missing rows!", flush=True)

    # Step 6: Final count
    count_result = list(bq.query(f"SELECT COUNT(*) as total FROM `{TABLE}`").result())[0]
    print(f"\nFinal row count in database: {count_result.total}", flush=True)
else:
    print("No missing videos found! Database is already complete.", flush=True)