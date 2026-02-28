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
Cloud Run (Python)
↓
BigQuery
↓
Looker Studio Dashboard
