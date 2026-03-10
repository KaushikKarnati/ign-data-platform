"""
video_stats.py
--------------
Airflow task: Extract video statistics from the YouTube Data API v3.

Responsibilities:
  - Fetch the IGN channel's uploads playlist ID
  - Paginate through all video IDs in that playlist
  - Batch-fetch stats (views, likes, comments, duration) for every video
  - Return raw list of video dicts for the next task (transform)
"""

import logging
import requests
from airflow.decorators import task
from airflow.models import Variable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Airflow Variables (set these in Airflow UI → Admin → Variables)
# ---------------------------------------------------------------------------
API_KEY        = Variable.get("YT_API_KEY")
CHANNEL_HANDLE = Variable.get("YT_CHANNEL_HANDLE")   # e.g. @IGN
MAX_RESULTS    = 50                                    # YouTube API max per page


# ---------------------------------------------------------------------------
# Task 1 — Get the uploads playlist ID for the channel
# ---------------------------------------------------------------------------
@task(task_id="get_playlist_id")
def get_playlist_id() -> str:
    """
    Calls the YouTube Channels API to get the uploads playlist ID.
    Every YouTube channel has a hidden 'uploads' playlist that contains
    every video it has ever published — this is the most reliable way
    to enumerate all videos for a channel.
    """
    url = (
        "https://www.googleapis.com/youtube/v3/channels"
        f"?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
    )
    logger.info(f"Fetching playlist ID for channel: {CHANNEL_HANDLE}")

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data        = response.json()
    playlist_id = data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    logger.info(f"Playlist ID: {playlist_id}")
    return playlist_id


# ---------------------------------------------------------------------------
# Task 2 — Get all video IDs from the playlist
# ---------------------------------------------------------------------------
@task(task_id="get_video_ids")
def get_video_ids(playlist_id: str) -> list[str]:
    """
    Paginates through the PlaylistItems API to collect every video ID
    in the uploads playlist. IGN has 20,000+ videos so this may take
    several hundred API calls (50 results per page).
    """
    video_ids  = []
    page_token = None
    base_url   = (
        "https://www.googleapis.com/youtube/v3/playlistItems"
        f"?part=contentDetails&maxResults={MAX_RESULTS}"
        f"&playlistId={playlist_id}&key={API_KEY}"
    )

    logger.info("Paginating through playlist to collect video IDs...")

    while True:
        url      = base_url + (f"&pageToken={page_token}" if page_token else "")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()
        for item in data.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])

        page_token = data.get("nextPageToken")
        logger.info(f"Collected {len(video_ids)} video IDs so far...")

        if not page_token:
            break

    logger.info(f"Total video IDs collected: {len(video_ids)}")
    return video_ids


# ---------------------------------------------------------------------------
# Task 3 — Fetch detailed stats for every video ID
# ---------------------------------------------------------------------------
@task(task_id="extract_video_stats")
def extract_video_stats(video_ids: list[str]) -> list[dict]:
    """
    Calls the YouTube Videos API in batches of 50 to get full stats
    for every video: title, publish date, duration, views, likes, comments.

    Returns a list of raw dicts — one per video — ready for transformation.
    """
    def batches(lst: list, size: int):
        """Yield successive chunks of `size` from lst."""
        for i in range(0, len(lst), size):
            yield lst[i : i + size]

    extracted = []

    for batch in batches(video_ids, MAX_RESULTS):
        ids_str = ",".join(batch)
        url     = (
            "https://www.googleapis.com/youtube/v3/videos"
            f"?part=snippet,contentDetails,statistics&id={ids_str}&key={API_KEY}"
        )
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        for item in response.json().get("items", []):
            stats = item.get("statistics", {})
            extracted.append({
                "video_id":     item["id"],
                "title":        item["snippet"].get("title"),
                "published_at": item["snippet"].get("publishedAt"),
                "duration":     item["contentDetails"].get("duration"),
                "view_count":   stats.get("viewCount"),
                "like_count":   stats.get("likeCount"),
                "comment_count":stats.get("commentCount"),
            })

        logger.info(f"Extracted stats for {len(extracted)} videos so far...")

    logger.info(f"Extraction complete. Total videos: {len(extracted)}")
    return extracted
