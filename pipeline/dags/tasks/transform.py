"""
transform.py
------------
Airflow task: Clean and enrich raw YouTube API data.

Responsibilities:
  - Parse ISO 8601 duration (PT4M3S) into integer seconds
  - Categorise videos by duration (Short / Medium / Long / Extra Long)
  - Cast view/like/comment counts from strings to integers
  - Handle nulls (some videos have comments or likes disabled)
  - Calculate engagement rate: ((likes + comments) / views) * 100
  - Extract year and month from publishedAt for easy GROUP BY queries
  - Return list of clean dicts ready for CSV conversion
"""

import re
import logging
from datetime import datetime
from airflow.decorators import task

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers (pure functions — no Airflow dependency, easy to unit test)
# ---------------------------------------------------------------------------

def _parse_duration(iso_str: str) -> int:
    """
    Convert an ISO 8601 duration string to total seconds.

    Examples:
        PT1M19S  ->  79
        PT4M3S   ->  243
        PT1H2M3S ->  3723
        PT19S    ->  19
        PT1M     ->  60
    """
    if not iso_str or iso_str == "PT":
        return 0
    hours   = int((re.search(r"(\d+)H", iso_str) or [0, 0])[1])
    minutes = int((re.search(r"(\d+)M", iso_str) or [0, 0])[1])
    seconds = int((re.search(r"(\d+)S", iso_str) or [0, 0])[1])
    return hours * 3600 + minutes * 60 + seconds


def _duration_category(seconds: int) -> str:
    """
    Label a video by its length — mirrors YouTube's own content categories.

        Short      < 60 s   (YouTube Shorts territory)
        Medium     1–5 min  (typical trailer / clip)
        Long       5–20 min (review / feature)
        Extra Long > 20 min (documentary / stream highlight)
    """
    if seconds < 60:
        return "Short"
    elif seconds < 300:
        return "Medium"
    elif seconds < 1200:
        return "Long"
    else:
        return "Extra Long"


def _safe_int(value) -> int:
    """Cast to int, returning 0 for None / empty string."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _engagement_rate(views: int, likes: int, comments: int) -> float:
    """
    Engagement rate = (likes + comments) / views * 100.
    Returns 0.0 if views is 0 to avoid division by zero.
    """
    if views == 0:
        return 0.0
    return round((likes + comments) / views * 100, 2)


# ---------------------------------------------------------------------------
# Airflow task
# ---------------------------------------------------------------------------

@task(task_id="transform_data")
def transform_data(raw_videos: list[dict]) -> list[dict]:
    """
    Cleans and enriches the raw list returned by extract_video_stats.

    Input fields (from YouTube API):
        video_id, title, published_at, duration (ISO 8601),
        view_count, like_count, comment_count  (all strings or None)

    Output fields (clean, typed, enriched):
        video_id, title, published_date, published_year, published_month,
        duration_seconds, duration_category,
        view_count, like_count, comment_count,
        engagement_rate
    """
    cleaned = []
    skipped = 0

    for row in raw_videos:
        try:
            # Parse timestamp — YouTube returns ISO 8601 e.g. 2026-01-27T17:27:05Z
            dt = datetime.strptime(
                row["published_at"].replace("Z", ""),
                "%Y-%m-%dT%H:%M:%S"
            )

            dur_secs = _parse_duration(row.get("duration", "PT0S"))
            views    = _safe_int(row.get("view_count"))
            likes    = _safe_int(row.get("like_count"))
            comments = _safe_int(row.get("comment_count"))

            cleaned.append({
                "video_id":          row["video_id"],
                "title":             row["title"],
                "published_date":    dt.strftime("%Y-%m-%d"),
                "published_year":    dt.year,
                "published_month":   dt.month,
                "duration_seconds":  dur_secs,
                "duration_category": _duration_category(dur_secs),
                "view_count":        views,
                "like_count":        likes,
                "comment_count":     comments,
                "engagement_rate":   _engagement_rate(views, likes, comments),
            })

        except Exception as e:
            logger.warning(f"Skipping video {row.get('video_id')} — {e}")
            skipped += 1

    logger.info(f"Transform complete. {len(cleaned)} rows clean, {skipped} skipped.")
    return cleaned
