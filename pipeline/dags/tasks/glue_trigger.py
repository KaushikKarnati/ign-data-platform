"""
glue_trigger.py
---------------
Airflow task: Trigger the AWS Glue crawler and wait for it to finish.

Responsibilities:
  - Start the Glue crawler after the S3 upload completes
  - Poll every 15 seconds until the crawler reaches READY state
  - Raise an exception if the crawler fails (so Airflow marks the task red)
  - Log the metrics returned by Glue (tables created/updated/deleted)
"""

import time
import logging
import boto3
from airflow.decorators import task
from airflow.models import Variable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Airflow Variables
# ---------------------------------------------------------------------------
CRAWLER_NAME = Variable.get("GLUE_CRAWLER_NAME", default_var="nl-sql-sales-crawler")
AWS_REGION   = Variable.get("AWS_REGION",         default_var="us-east-1")

POLL_INTERVAL_SECS = 15
MAX_WAIT_SECS      = 600   # 10 minutes — crawler should never take this long


# ---------------------------------------------------------------------------
# Airflow task
# ---------------------------------------------------------------------------

@task(task_id="trigger_glue_crawler")
def trigger_glue_crawler(s3_uri: str) -> str:
    """
    Starts the Glue crawler and blocks until it finishes.

    s3_uri is passed in from upload_to_s3 via XCom — it's logged
    for traceability (shows exactly which file triggered this run).

    Glue crawler states:
        READY    — idle, ready to run (this is the terminal success state)
        RUNNING  — currently scanning S3
        STOPPING — finishing up

    Returns "SUCCESS" on completion or raises on failure.
    """
    logger.info(f"Triggering Glue crawler '{CRAWLER_NAME}' after upload: {s3_uri}")

    glue = boto3.client("glue", region_name=AWS_REGION)

    # --- Start the crawler ---
    try:
        glue.start_crawler(Name=CRAWLER_NAME)
        logger.info("Crawler started successfully.")
    except glue.exceptions.CrawlerRunningException:
        logger.warning("Crawler is already running — waiting for it to finish.")

    # --- Poll until done ---
    elapsed = 0
    while elapsed < MAX_WAIT_SECS:
        time.sleep(POLL_INTERVAL_SECS)
        elapsed += POLL_INTERVAL_SECS

        response = glue.get_crawler(Name=CRAWLER_NAME)
        crawler  = response["Crawler"]
        state    = crawler["State"]

        logger.info(f"Crawler state: {state} (waited {elapsed}s)")

        if state == "READY":
            # Log what the crawler did this run
            last_crawl = crawler.get("LastCrawl", {})
            status     = last_crawl.get("Status", "UNKNOWN")
            summary    = last_crawl.get("Summary", "no summary")

            if status == "FAILED":
                error_msg = last_crawl.get("ErrorMessage", "unknown error")
                raise RuntimeError(f"Glue crawler failed: {error_msg}")

            logger.info(f"Crawler finished. Status: {status} | Summary: {summary}")
            return "SUCCESS"

    raise TimeoutError(
        f"Glue crawler '{CRAWLER_NAME}' did not finish within {MAX_WAIT_SECS}s."
    )
