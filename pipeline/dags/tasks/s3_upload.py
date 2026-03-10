"""
s3_upload.py
------------
Airflow task: Serialize cleaned video data to CSV and upload to S3.

Responsibilities:
  - Convert the list of clean dicts to CSV format in memory (no temp files)
  - Upload to s3://nl-sql-data-{ACCOUNT_ID}/ign_videos/ign_videos.csv
  - Overwrite the existing file on every run (full refresh pattern)
  - Return the S3 URI so downstream tasks can log or use it
"""

import csv
import io
import logging
import boto3
from airflow.decorators import task
from airflow.models import Variable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Airflow Variables (set in Airflow UI → Admin → Variables)
# ---------------------------------------------------------------------------
S3_BUCKET = Variable.get("S3_BUCKET")                          # nl-sql-data-867207177403
S3_KEY    = Variable.get("S3_KEY", default_var="ign_videos/ign_videos.csv")
AWS_REGION = Variable.get("AWS_REGION", default_var="us-east-1")

# Column order for the CSV — must match Athena table schema
CSV_COLUMNS = [
    "video_id",
    "title",
    "published_date",
    "published_year",
    "published_month",
    "duration_seconds",
    "duration_category",
    "view_count",
    "like_count",
    "comment_count",
    "engagement_rate",
]


# ---------------------------------------------------------------------------
# Airflow task
# ---------------------------------------------------------------------------

@task(task_id="upload_to_s3")
def upload_to_s3(clean_videos: list[dict]) -> str:
    """
    Converts the cleaned video list to a CSV and uploads it to S3.

    Uses io.StringIO (in-memory buffer) instead of writing to disk.
    This is the standard pattern for cloud-native pipelines:
      - No disk I/O
      - No temp file cleanup needed
      - Works inside Docker containers with limited filesystem access

    The file is uploaded with a fixed S3 key (ign_videos/ign_videos.csv)
    so it overwrites the previous day's file — Athena always reads the
    latest snapshot. This is called a full-refresh pattern.

    Returns the S3 URI of the uploaded file.
    """
    if not clean_videos:
        raise ValueError("No data to upload — transform task returned empty list.")

    # --- Build CSV in memory ---
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=CSV_COLUMNS,
        extrasaction="ignore",   # silently drop any extra fields
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(clean_videos)

    csv_bytes = buffer.getvalue().encode("utf-8")
    row_count = len(clean_videos)

    logger.info(f"CSV built in memory — {row_count} rows, {len(csv_bytes):,} bytes")

    # --- Upload to S3 ---
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    s3_client.put_object(
        Bucket      = S3_BUCKET,
        Key         = S3_KEY,
        Body        = csv_bytes,
        ContentType = "text/csv",
    )

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    logger.info(f"Uploaded {row_count} rows to {s3_uri}")
    return s3_uri
