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
now_utc = datetime.now(timezone.utc)

# 1) Collect video_ids to refresh (videos published in last 28 days)
query = f"""
SELECT DISTINCT video_id
FROM `{TABLE}`
WHERE TIMESTAMP(published_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)
"""

video_ids = [row.video_id for row in bq.query(query).result()]
print(f"Found {len(video_ids)} videos to refresh (last 28 days).", flush=True)

if not video_ids:
    print("No videos to refresh. Exiting.", flush=True)
    raise SystemExit(0)

rows = []

# 2) Fetch updated stats in batches of 50
for i in range(0, len(video_ids), 50):
    batch = video_ids[i:i+50]
    print(f"Fetching stats for videos {i+1} to {min(i+len(batch), len(video_ids))}...", flush=True)

    resp = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=",".join(batch)
    ).execute()

    for item in resp.get("items", []):
        duration_iso = item.get("contentDetails", {}).get("duration")
        duration_seconds = int(isodate.parse_duration(duration_iso).total_seconds()) if duration_iso else 0

        rows.append({
            "video_id": item["id"],
            "video_title": item["snippet"]["title"],
            "channel_name": item["snippet"]["channelTitle"],
            "channel_id": item["snippet"]["channelId"],
            "playlist_id": None,
            "playlist_name": None,
            "published_at": item["snippet"]["publishedAt"],
            "video_duration_seconds": duration_seconds,
            "like_count": int(item.get("statistics", {}).get("likeCount", 0)),
            "view_count": int(item.get("statistics", {}).get("viewCount", 0)),
            "comment_count": int(item.get("statistics", {}).get("commentCount", 0)),
            "data_capture_date": now_utc.date().isoformat(),
            "data_capture_timestamp_utc": now_utc.isoformat(),
        })

print(f"Rows to insert: {len(rows)}", flush=True)

# 3) Insert as snapshots (append)
errors = bq.insert_rows_json(TABLE, rows)
if errors:
    print("Insert errors:", errors, flush=True)
    raise SystemExit(1)

print("Refresh complete. Snapshot rows inserted.", flush=True)