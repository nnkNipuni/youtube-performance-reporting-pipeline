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
## ☁️ Cloud Deployment & Automation (GCP)

This project is automated using **Cloud Run** + **Cloud Scheduler**.

### 1️⃣ Prerequisites
- A GCP project with billing enabled
- Enabled APIs:
  - Cloud Run API
  - Cloud Build API
  - Artifact Registry API
  - Cloud Scheduler API
  - BigQuery API

### 2️⃣ Authenticate and set project
```bash
gcloud auth login
gcloud config set project <YOUR_PROJECT_ID>
gcloud config set run/region asia-south1
```
### 3️⃣ Deploy to Cloud Run (from source)

Deploy the Flask app (main.py) which exposes /run and /refresh endpoints.
```
gcloud run deploy youtube-daily-pipeline \
  --source . \
  --region asia-south1 \
  --allow-unauthenticated
```
After deployment, note the Service URL

### 4️⃣ Configure environment variables on Cloud Run
Create an env.yaml file (not committed to GitHub):
```
CHANNEL_IDS: "channel_id_1,channel_id_2"
BQ_PROJECT: "<YOUR_PROJECT_ID>"
BQ_DATASET: "youtube_analytics"
BQ_TABLE: "video_daily_metrics"
YOUTUBE_API_KEY: "<YOUR_YOUTUBE_API_KEY>"
```
Apply it:
```
gcloud run services update youtube-daily-pipeline \
  --region asia-south1 \
  --env-vars-file env.yaml
```
### 5️⃣ Create Cloud Scheduler jobs (Automation)

Daily ingestion (new videos - last 24h)
```
gcloud scheduler jobs create http youtube-daily-trigger \
  --location=asia-south1 \
  --schedule="0 1 * * *" \
  --uri="https://<CLOUD_RUN_URL>/run" \
  --http-method=POST \
  --time-zone="UTC"
```
Daily ingestion (new videos - last 24h)
```
gcloud scheduler jobs create http youtube-daily-trigger \
  --location=asia-south1 \
  --schedule="0 1 * * *" \
  --uri="https://<CLOUD_RUN_URL>/run" \
  --http-method=POST \
  --time-zone="UTC"
```
### 6️⃣ Verify automation

Check Scheduler jobs:
```
gcloud scheduler jobs list --location=asia-south1
```
Manually trigger a run (optional):
```
curl -X POST "https://<CLOUD_RUN_URL>/run"
curl -X POST "https://<CLOUD_RUN_URL>/refresh"
```
Note: Schedules are in UTC. Sri Lanka time is UTC+5:30.
