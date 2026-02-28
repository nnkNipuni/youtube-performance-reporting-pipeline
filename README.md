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

YouTube Data API
        ↓
Cloud Run (Python – Flask)
        ↓
BigQuery (Analytics Tables)
        ↓
Looker Studio Dashboard


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
youtube_pipeline/
│
├── notebooks/
│   └── explore_youtube.ipynb        # API exploration & schema validation
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
