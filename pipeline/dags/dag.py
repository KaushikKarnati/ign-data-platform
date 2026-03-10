"""
dag.py
------
Airflow DAG: ign_youtube_to_s3

Orchestrates the full IGN YouTube data pipeline:

    1. get_playlist_id     — Fetch the uploads playlist ID for the IGN channel
    2. get_video_ids       — Paginate through all video IDs in that playlist
    3. extract_video_stats — Batch-fetch stats for every video from YouTube API
    4. transform_data      — Clean, enrich, and type-cast the raw data
    5. upload_to_s3        — Write cleaned data as CSV to S3
    6. trigger_glue_crawler— Refresh the Glue Data Catalog so Athena sees new data

Schedule: Daily at 14:00 UTC
Downstream: AWS Glue → Athena → Lambda NL-to-SQL API
"""

import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from tasks.video_stats  import get_playlist_id, get_video_ids, extract_video_stats
from tasks.transform    import transform_data
from tasks.s3_upload    import upload_to_s3
from tasks.glue_trigger import trigger_glue_crawler

# ---------------------------------------------------------------------------
# DAG defaults
# ---------------------------------------------------------------------------
default_args = {
    "owner":             "ram",
    "depends_on_past":   False,
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "dagrun_timeout":    timedelta(hours=2),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id      = "ign_youtube_to_s3",
    description = (
        "Daily pipeline: Extract IGN YouTube stats → Transform → "
        "Upload to S3 → Trigger Glue crawler for Athena NL-to-SQL queries"
    ),
    default_args = default_args,
    start_date   = datetime(2025, 1, 1, tzinfo=pendulum.timezone("UTC")),
    schedule     = "0 14 * * *",   # Every day at 14:00 UTC
    catchup      = False,
    tags         = ["youtube", "s3", "glue", "ign", "etl"],
) as dag:

    # -- Task instances --
    playlist_id    = get_playlist_id()
    video_ids      = get_video_ids(playlist_id)
    raw_stats      = extract_video_stats(video_ids)
    clean_data     = transform_data(raw_stats)
    s3_uri         = upload_to_s3(clean_data)
    crawler_result = trigger_glue_crawler(s3_uri)

    # -- Pipeline dependency chain --
    # get_playlist_id
    #       ↓
    # get_video_ids
    #       ↓
    # extract_video_stats
    #       ↓
    # transform_data
    #       ↓
    # upload_to_s3
    #       ↓
    # trigger_glue_crawler
    #
    # Airflow infers the chain from the task call order above.
    # XCom automatically passes return values between tasks.
