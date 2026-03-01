# 📊 Daily YouTube Performance Pipeline

## Overview

This project implements an **end-to-end automated data pipeline** to collect, store, refresh, and visualize YouTube channel performance data for a Digital Media Manager.

The system automatically:
- Fetches **newly uploaded videos** (last 24 hours)
- Refreshes **latest engagement metrics** (views, likes, comments) for existing videos
- Stores **time-based snapshots** in BigQuery
- Powers a **live dashboard** in Looker Studio

The pipeline is fully automated using **Google Cloud Run** and **Cloud Scheduler**.

---

## 🏗️ Architecture

**High-level flow:**

<pre>
YouTube Data API
       ↓
Cloud Run (Python / Flask)
       ↓
BigQuery (Analytics Tables)
       ↓
Looker Studio Dashboard
</pre>

**Automation:**
- Cloud Scheduler triggers pipelines daily
- No manual intervention required after deployment

---

## ⚙️ Technologies Used

- Python
- YouTube Data API v3
- Google Cloud Run
- Google Cloud Scheduler
- Google BigQuery
- Looker Studio
- Docker
- GitHub

---

## 📂 Project Structure

<pre>
youtube_pipeline/
│
├── notebooks/
│   └── explore_youtube.ipynb        # API exploration &amp; schema validation
│
├── build_pipeline.py                # Fetches newly published videos
├── backfill.py                      # Backfills historical videos
├── refresh_stats.py                 # Refreshes latest metrics for existing videos
│
├── main.py                          # Flask app (Cloud Run entrypoint)
├── Dockerfile                       # Container definition for Cloud Run
├── requirements.txt                 # Python dependencies
│
├── env.yaml                         # Environment variables for Cloud Run
├── .env                             # Local environment variables (gitignored)
├── .gitignore                       # Git ignore rules
│
└── README.md
</pre>

---

## 🔁 Pipelines Explained

### 1️⃣ Backfill Pipeline (`backfill.py`)
- Fetches **all historical videos** from configured channels
- Inserts only missing videos (deduplicated by `video_id`)
- Used for:
  - Initial setup
  - Data recovery if the table is deleted
- **Not scheduled** (manual execution only)

---

### 2️⃣ Daily Ingestion Pipeline (`build_pipeline.py`)
- Fetches videos uploaded in the **last 24 hours**
- Stores daily snapshot rows in BigQuery
- Triggered automatically via Cloud Scheduler
- Ensures continuous ingestion of new content

---

### 3️⃣ Metrics Refresh Pipeline (`refresh_stats.py`)
- Fetches **latest views, likes, and comments** for existing videos
- Updates engagement metrics for videos published in the last 28 days
- Appends refreshed snapshots to BigQuery
- Ensures the dashboard always shows **latest performance**

---

## ☁️ Cloud Run Endpoints

| Endpoint | Purpose |
|--------|--------|
| `/` | Health check |
| `/run` | Trigger daily ingestion pipeline |
| `/refresh` | Trigger metrics refresh pipeline |

**Manual trigger (optional):**
```bash
curl -X POST https://<cloud-run-url>/run
curl -X POST https://<cloud-run-url>/refresh
```
## Setup Instructions
1️⃣ Clone Repository
```
git clone <your-github-repo-url>
cd youtube_pipeline
```
2️⃣ Create Virtual Environment
```
python -m venv .venv
source .venv/bin/activate   # macOS / Linux
```
3️⃣ Install Dependencies
```
pip install -r requirements.txt
```
4️⃣ Environment Variables

Create a .env file (not committed to GitHub):
```
YOUTUBE_API_KEY=your_youtube_api_key
BQ_PROJECT=your_gcp_project_id
BQ_DATASET=youtube_analytics
BQ_TABLE=video_daily_metrics
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account.json
CHANNEL_IDS=channel_id_1,channel_id_2
```

5️⃣ Run Pipelines Locally (Optional)
```
python backfill.py
python build_pipeline.py
python refresh_stats.py
```

