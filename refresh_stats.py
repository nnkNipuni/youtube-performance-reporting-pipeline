from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.cloud import bigquery
import isodate
import os
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv(override=True)

youtube = build("youtube", "v3", developerKey=os.getenv("YOUTUBE_API_KEY"))
bq = bigquery.Client(project=os.getenv("BQ_PROJECT"))

TABLE = f"{os.getenv('BQ_PROJECT')}.{os.getenv('BQ_DATASET')}.{os.getenv('BQ_TABLE')}"
STAGE_TABLE = f"{os.getenv('BQ_PROJECT')}.{os.getenv('BQ_DATASET')}.video_daily_metrics_stage"
now_utc = datetime.now(timezone.utc)

# 1) Collect video_ids and their latest playlist info
query = f"""
WITH RankedVideos AS (
  SELECT
    video_id,
    playlist_id,
    playlist_name,
    ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY data_capture_timestamp_utc DESC) as rn
  FROM `{TABLE}`
  WHERE TIMESTAMP(published_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)
)
SELECT video_id, playlist_id, playlist_name
FROM RankedVideos
WHERE rn = 1
"""

video_info_map = {row.video_id: {"playlist_id": row.playlist_id, "playlist_name": row.playlist_name} for row in bq.query(query).result()}
video_ids = list(video_info_map.keys())

print(f"Found {len(video_ids)} videos to refresh (last 28 days).", flush=True)

if not video_ids:
    print("No videos to refresh. Exiting.", flush=True)
    raise SystemExit(0)

rows = []

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_video_stats_batch(batch):
    return youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=",".join(batch)
    ).execute()

# 2) Fetch updated stats in batches of 50
for i in range(0, len(video_ids), 50):
    batch = video_ids[i:i+50]
    print(f"Fetching stats for videos {i+1} to {min(i+len(batch), len(video_ids))}...", flush=True)

    resp = get_video_stats_batch(batch)

    for item in resp.get("items", []):
        duration_iso = item.get("contentDetails", {}).get("duration")
        duration_seconds = int(isodate.parse_duration(duration_iso).total_seconds()) if duration_iso else 0
        vid = item["id"]

        rows.append({
            "video_id": vid,
            "video_title": item["snippet"]["title"],
            "channel_name": item["snippet"]["channelTitle"],
            "channel_id": item["snippet"]["channelId"],
            "playlist_id": video_info_map[vid]["playlist_id"],
            "playlist_name": video_info_map[vid]["playlist_name"],
            "published_at": item["snippet"]["publishedAt"],
            "video_duration_seconds": duration_seconds,
            "like_count": int(item.get("statistics", {}).get("likeCount", 0)),
            "view_count": int(item.get("statistics", {}).get("viewCount", 0)),
            "comment_count": int(item.get("statistics", {}).get("commentCount", 0)),
            "data_capture_date": now_utc.date().isoformat(),
            "data_capture_timestamp_utc": now_utc.isoformat(),
        })

print(f"Rows to insert: {len(rows)}", flush=True)

# 3) Insert as snapshots (idempotent overwrite for today)
if rows:
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    print("Loading rows into staging table (WRITE_TRUNCATE)...", flush=True)
    load_job = bq.load_table_from_json(rows, STAGE_TABLE, job_config=job_config)
    load_job.result()
    print("Staging load complete.", flush=True)

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
    print("Refresh complete. Main table updated.", flush=True)