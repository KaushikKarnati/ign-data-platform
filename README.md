# IGN Data Platform 🎮

A fully automated, cloud-native data platform that collects YouTube analytics from the IGN channel daily and exposes them through an AI-powered natural language query API.

**Ask a question in plain English. Get real data back in seconds.**

```bash
curl -X POST https://zsom726c8b.execute-api.us-east-1.amazonaws.com/Prod/query \
  -H "Content-Type: application/json" \
  -d '{"question": "What are the top 5 most viewed IGN videos?"}'
```

```json
{
  "question": "What are the top 5 most viewed IGN videos?",
  "sql": "SELECT title, view_count FROM ign_videos ORDER BY view_count DESC LIMIT 5",
  "results": [
    {"title": "GTA 6 Official Trailer", "view_count": 4200000},
    {"title": "Elden Ring — Official Gameplay Reveal", "view_count": 3800000},
    {"title": "The Last of Us Part III Reveal", "view_count": 3100000},
    {"title": "Warhammer 40K: Dawn of War 4 Trailer", "view_count": 2900000},
    {"title": "Helldivers 2 — Launch Trailer", "view_count": 2400000}
  ],
  "count": 5
}
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                 EC2 (t3.small) — Airflow                │
│                                                         │
│  Daily at 14:00 UTC                                     │
│  ┌──────────────────────────────────────────────────┐   │
│  │  get_playlist_id → get_video_ids                 │   │
│  │        → extract_video_stats (20,000 videos)     │   │
│  │        → transform_data                          │   │
│  │        → upload_to_s3                            │   │
│  │        → trigger_glue_crawler                    │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
            S3: nl-sql-data-867207177403
            └── ign_videos/ign_videos.csv
                          │
                          ▼
                 AWS Glue Data Catalog
                 (schema auto-updated)
                          │
                          ▼
                   Amazon Athena
               (serverless SQL layer)
                          │
                          ▼
            AWS Lambda + Amazon Bedrock
              (NL → SQL generation)
                          │
                          ▼
                 API Gateway (REST)
            POST /query → plain English
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 on AWS EC2 |
| Containerisation | Docker + Docker Compose |
| Data Source | YouTube Data API v3 |
| Storage | Amazon S3 |
| Schema Catalog | AWS Glue |
| Query Engine | Amazon Athena |
| AI/NL-to-SQL | Amazon Bedrock (Nova Pro) |
| Compute | AWS Lambda (Python 3.13) |
| API | AWS API Gateway |
| IaC | AWS SAM |
| CI/CD | GitHub Actions |

---

## How It Works

### Pipeline (runs daily at 14:00 UTC)

The Airflow DAG `ign_youtube_to_s3` runs 6 tasks in sequence:

1. **get_playlist_id** — Calls the YouTube Channels API to fetch IGN's uploads playlist ID
2. **get_video_ids** — Paginates through the playlist collecting all 20,000+ video IDs
3. **extract_video_stats** — Batch-fetches views, likes, comments, and duration for every video
4. **transform_data** — Cleans and enriches the raw data:
   - Parses ISO 8601 duration (`PT4M3S`) → integer seconds
   - Adds `duration_category` (Short / Medium / Long / Extra Long)
   - Calculates `engagement_rate` = ((likes + comments) / views) × 100
   - Extracts `published_year` and `published_month` for easy filtering
5. **upload_to_s3** — Serializes 20,000 rows to CSV in memory and uploads to S3
6. **trigger_glue_crawler** — Refreshes the Glue Data Catalog so Athena always sees the latest schema

### NL-to-SQL API

When a user sends a plain English question:

1. API Gateway receives the POST request
2. Lambda calls Amazon Bedrock (Nova Pro) with the question + table schema
3. Bedrock generates a valid SQL query
4. Lambda executes the query in Athena against the S3 data
5. Results are returned as JSON — typically in under 3 seconds

---

## Dataset

**20,000 IGN YouTube videos** — refreshed daily.

| Column | Type | Description |
|---|---|---|
| `video_id` | string | Unique YouTube video ID |
| `title` | string | Full video title |
| `published_date` | string | Date published (YYYY-MM-DD) |
| `published_year` | int | Year extracted for GROUP BY |
| `published_month` | int | Month number (1–12) |
| `duration_seconds` | int | Video length in seconds |
| `duration_category` | string | Short / Medium / Long / Extra Long |
| `view_count` | int | Total views |
| `like_count` | int | Total likes |
| `comment_count` | int | Total comments |
| `engagement_rate` | float | ((likes + comments) / views) × 100 |

---

## Demo Questions

Try these against the live API:

```bash
# Most viewed videos
curl -X POST https://zsom726c8b.execute-api.us-east-1.amazonaws.com/Prod/query \
  -H "Content-Type: application/json" \
  -d '{"question": "What are the top 10 most viewed IGN videos?"}'

# Monthly publishing trends
curl -X POST https://zsom726c8b.execute-api.us-east-1.amazonaws.com/Prod/query \
  -H "Content-Type: application/json" \
  -d '{"question": "How many videos did IGN publish each month in 2025?"}'

# Engagement by content type
curl -X POST https://zsom726c8b.execute-api.us-east-1.amazonaws.com/Prod/query \
  -H "Content-Type: application/json" \
  -d '{"question": "What is the average engagement rate for each duration category?"}'

# High performers
curl -X POST https://zsom726c8b.execute-api.us-east-1.amazonaws.com/Prod/query \
  -H "Content-Type: application/json" \
  -d '{"question": "Which videos have over 1 million views?"}'

# Recent content
curl -X POST https://zsom726c8b.execute-api.us-east-1.amazonaws.com/Prod/query \
  -H "Content-Type: application/json" \
  -d '{"question": "What are the top 5 most liked videos from 2026?"}'
```

---

## Repository Structure

```
ign-data-platform/
├── pipeline/                        # Airflow pipeline (runs on EC2)
│   ├── dags/
│   │   ├── tasks/
│   │   │   ├── video_stats.py       # YouTube API extraction
│   │   │   ├── transform.py         # Data cleaning + enrichment
│   │   │   ├── s3_upload.py         # CSV upload to S3
│   │   │   └── glue_trigger.py      # Glue crawler trigger
│   │   └── dag.py                   # DAG definition
│   ├── Dockerfile                   # Custom Airflow image
│   ├── docker-compose.yaml          # Airflow stack (LocalExecutor)
│   └── requirements.txt
│
├── api/                             # NL-to-SQL engine (AWS Lambda)
│   ├── src/query_handler/
│   │   └── app.py                   # Lambda handler
│   ├── infrastructure/
│   │   └── template.yaml            # SAM template
│   └── tests/
│
└── .github/workflows/
    └── deploy.yml                   # CI/CD — auto deploys on push to main
```

---

## Setup Overview

### Prerequisites
- AWS account with S3, Glue, Athena, Lambda, Bedrock, API Gateway access
- YouTube Data API v3 key (Google Cloud Console)
- Docker + Docker Compose on EC2

### Pipeline (EC2)
```bash
git clone https://github.com/KaushikKarnati/ign-data-platform.git
cd ign-data-platform/pipeline
cp .env.example .env   # fill in AWS + YouTube API credentials
docker-compose build
docker-compose up airflow-init
docker-compose up -d
```

Access Airflow UI via SSH tunnel:
```bash
ssh -i ~/.ssh/ign-airflow-key.pem -L 8080:localhost:80 ubuntu@EC2_IP -N
# Open http://localhost:8080
```

### API (SAM)
```bash
cd api/infrastructure
sam build
sam deploy
```

CI/CD via GitHub Actions — any push to `main` automatically deploys the API.

---

## Author

**Ram Karnati** — Data Analyst & AI Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-blue?style=flat&logo=linkedin)](https://linkedin.com/in/ramkarnati)
[![GitHub](https://img.shields.io/badge/GitHub-black?style=flat&logo=github)](https://github.com/KaushikKarnati)
[![Research](https://img.shields.io/badge/arXiv-AQUA--7B-red?style=flat)](https://arxiv.org/abs/2507.20520)
