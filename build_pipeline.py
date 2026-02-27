

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

# Daily pipeline fetches only last 24 hours of new videos
# If you need to backfill historical data, run backfill.py instead
cutoff_24h = now_utc - timedelta(hours=24)
published_after = cutoff_24h.strftime("%Y-%m-%dT%H:%M:%SZ")

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

    cutoff = now_utc - timedelta(hours=24)
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
    print(f"  Found {len(video_ids)} videos in last 24 hours", flush=True)
    print(f"  Fetching video details...", flush=True)
    rows = get_video_details(video_ids, channel_name, channel_id, playlist_map)
    all_rows.extend(rows)


print(f"Total rows to insert: {len(all_rows)}", flush=True)

if all_rows:
    from google.cloud import bigquery

    STAGE_TABLE = f"{os.getenv('BQ_PROJECT')}.{os.getenv('BQ_DATASET')}.video_daily_metrics_stage"

    # 1) Load into staging (overwrite staging each run)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    print("Loading rows into staging table (WRITE_TRUNCATE)...", flush=True)
    load_job = bq.load_table_from_json(all_rows, STAGE_TABLE, job_config=job_config)
    load_job.result()
    print("Staging load complete.", flush=True)

    # 2) MERGE staging into main (upsert on video_id + data_capture_date)
    merge_sql = f"""
    MERGE `{TABLE}` T
    USING `{STAGE_TABLE}` S
    ON T.video_id = S.video_id AND T.data_capture_date = S.data_capture_date
    WHEN MATCHED THEN
      UPDATE SET
        video_title = S.video_title,
        channel_name = S.channel_name,
        channel_id = S.channel_id,
        playlist_id = S.playlist_id,
        playlist_name = S.playlist_name,
        published_at = S.published_at,
        video_duration_seconds = S.video_duration_seconds,
        like_count = S.like_count,
        view_count = S.view_count,
        comment_count = S.comment_count,
        data_capture_timestamp_utc = S.data_capture_timestamp_utc
    WHEN NOT MATCHED THEN
      INSERT (
        video_id, video_title, channel_name, channel_id,
        playlist_id, playlist_name, published_at, video_duration_seconds,
        like_count, view_count, comment_count,
        data_capture_date, data_capture_timestamp_utc
      )
      VALUES (
        S.video_id, S.video_title, S.channel_name, S.channel_id,
        S.playlist_id, S.playlist_name, S.published_at, S.video_duration_seconds,
        S.like_count, S.view_count, S.comment_count,
        S.data_capture_date, S.data_capture_timestamp_utc
      )
    """
    print("Merging staging into main table (idempotent upsert)...", flush=True)
    bq.query(merge_sql).result()
    print("Merge complete. Main table updated.", flush=True)

else:
    print("No new videos found in last 24 hours.", flush=True)