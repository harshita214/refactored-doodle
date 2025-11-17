#!/usr/bin/env python3
"""
monitor_etag.py

Scalable, event-oriented status feed monitor using conditional GETs (ETag / Last-Modified).
Prints affected service + latest status message when a new incident appears.
Optional Slack alerts via SLACK_WEBHOOK env var.

Usage:
    python monitor_etag.py
"""

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Optional, Dict, List

import aiohttp
import feedparser
from dotenv import load_dotenv
import httpx  # used for Slack (async-friendly)

load_dotenv()

# -----------------------
# Configuration
# -----------------------
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

# Concurrency: how many feeds to fetch concurrently
CONCURRENT_FETCHES = int(os.getenv("CONCURRENT_FETCHES", "10"))

# Default check interval in seconds (per-feed can override)
DEFAULT_INTERVAL = int(os.getenv("DEFAULT_INTERVAL", "300"))  # 5 minutes

# Backoff settings
INITIAL_BACKOFF = 5
MAX_BACKOFF = 300

# Some feeds to monitor. Add as many as you like.
# Each feed can set: name, url, interval_seconds
FEEDS = [
    {"name": "openai-atom", "url": "https://status.openai.com/feed.atom", "interval": 300},
    {"name": "openai-rss", "url": "https://status.openai.com/feed.rss", "interval": 300},
    # Add more providers here, e.g.:
    # {"name": "aws-health", "url": "https://status.aws.amazon.com/rss/all.rss", "interval": 600},
]

# -----------------------
# Logging
# -----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("monitor_etag")


# -----------------------
# Per-feed state container
# -----------------------
@dataclass
class FeedState:
    name: str
    url: str
    interval: int
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    last_entry_id: Optional[str] = None
    backoff: int = field(default=INITIAL_BACKOFF)


# -----------------------
# Utility: guess affected service(s) from title
# -----------------------
SERVICE_KEYWORDS = [
    "ChatGPT", "Batch", "Responses", "Embeddings", "gpt-4", "gpt-4o", "gpt-5",
    "Completions", "API", "Platform", "Fine Tuning", "File Uploads"
]


def infer_services_from_title(title: str) -> List[str]:
    title_lower = title.lower()
    found = [kw for kw in SERVICE_KEYWORDS if kw.lower() in title_lower]
    return found or ["OpenAI (general)"]


# -----------------------
# Send Slack alert (optional)
# -----------------------
async def send_slack_alert(text: str):
    if not SLACK_WEBHOOK_URL:
        return
    # Use httpx for simple async POST
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(SLACK_WEBHOOK_URL, json={"text": text})
    except Exception as e:
        logger.warning("Slack notify failed: %s", e)


# -----------------------
# Fetch & parse with conditional GET
# -----------------------
async def fetch_feed(session: aiohttp.ClientSession, state: FeedState) -> Optional[str]:
    """
    Performs a conditional GET. Returns None if not modified or on parse error.
    Returns latest entry id if feed has new content (even if not newest entry).
    """
    headers = {"User-Agent": "OpenAI-Status-Monitor/1.0"}
    if state.etag:
        headers["If-None-Match"] = state.etag
    if state.last_modified:
        headers["If-Modified-Since"] = state.last_modified

    try:
        async with session.get(state.url, headers=headers, timeout=20, allow_redirects=True) as resp:
            # 304 -> not modified
            if resp.status == 304:
                logger.debug("[%s] 304 Not Modified", state.name)
                # reset backoff on success
                state.backoff = INITIAL_BACKOFF
                return None

            if resp.status >= 400:
                logger.warning("[%s] HTTP %d while fetching feed", state.name, resp.status)
                raise aiohttp.ClientResponseError(
                    request_info=resp.request_info, history=resp.history, status=resp.status
                )

            # update etag/last-modified if present
            etag = resp.headers.get("ETag")
            lm = resp.headers.get("Last-Modified")
            if etag:
                state.etag = etag
            if lm:
                state.last_modified = lm

            text = await resp.text()
            parsed = feedparser.parse(text)

            if not parsed.entries:
                logger.debug("[%s] feed parsed but has 0 entries", state.name)
                state.backoff = INITIAL_BACKOFF
                return None

            # pick the newest entry (feed typically sorted newest-first)
            newest = parsed.entries[0]
            entry_id = newest.get("id") or newest.get("link") or newest.get("title")
            return entry_id

    except Exception as exc:
        logger.error("[%s] Fetch error: %s", state.name, exc)
        # increase backoff
        state.backoff = min(state.backoff * 2, MAX_BACKOFF)
        return None


# -----------------------
# Worker task for each feed
# -----------------------
async def monitor_feed(state: FeedState, semaphore: asyncio.Semaphore):
    logger.info("Started monitor for %s (%s) every %ds", state.name, state.url, state.interval)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:

        while True:
            async with semaphore:
                entry_id = await fetch_feed(session, state)

            # If fetch_feed returned an entry_id, compare with last_entry_id
            if entry_id:
                if state.last_entry_id is None:
                    logger.info("[%s] baseline set to %s", state.name, entry_id)
                    state.last_entry_id = entry_id
                elif entry_id != state.last_entry_id:
                    # New event detected
                    logger.info("🔥 NEW incident for %s", state.name)
                    # We need to fetch the feed content to print title and summary
                    try:
                        async with session.get(state.url, timeout=20, headers={"User-Agent": "OpenAI-Status-Monitor/1.0"}) as resp:
                            text = await resp.text()
                            parsed = feedparser.parse(text)
                            newest = parsed.entries[0] if parsed.entries else None
                            if newest:
                                title = newest.get("title", "No title")
                                summary = newest.get("summary", "").strip()
                                services = infer_services_from_title(title)
                                print("\n=== NEW OPENAI STATUS UPDATE ===")
                                print(f"Feed: {state.name}")
                                print(f"Title: {title}")
                                print(f"Affected: {', '.join(services)}")
                                print(f"Message: {summary[:400]}")  # truncate long
                                print(f"Link: {newest.get('link')}")
                                print("==============================\n")

                                # Slack alert (optional)
                                if SLACK_WEBHOOK_URL:
                                    text_msg = f"*{state.name}* — *{title}*\nAffected: {', '.join(services)}\n{summary[:600]}\n{newest.get('link')}"
                                    await send_slack_alert(text_msg)

                    except Exception as e:
                        logger.warning("[%s] could not fetch full entry: %s", state.name, e)

                    state.last_entry_id = entry_id
                    # reset backoff after success
                    state.backoff = INITIAL_BACKOFF

                else:
                    logger.debug("[%s] no change (same entry_id)", state.name)
                    # reset backoff
                    state.backoff = INITIAL_BACKOFF
            else:
                # entry_id None can be due to 304 or fetch error; respect backoff
                logger.debug("[%s] no new entry_id (maybe 304 or error)", state.name)

            # Wait for interval (or backoff if error)
            wait_seconds = state.backoff if state.backoff > INITIAL_BACKOFF else state.interval
            logger.debug("[%s] sleeping %ds (backoff=%d)", state.name, wait_seconds, state.backoff)
            await asyncio.sleep(wait_seconds)


# -----------------------
# Main runner
# -----------------------
async def main():
    # build feed states
    states: List[FeedState] = []
    for f in FEEDS:
        interval = f.get("interval") or DEFAULT_INTERVAL
        states.append(FeedState(name=f["name"], url=f["url"], interval=interval))

    # semaphore to limit concurrent fetches
    semaphore = asyncio.Semaphore(CONCURRENT_FETCHES)

    # launch monitors
    tasks = [asyncio.create_task(monitor_feed(s, semaphore)) for s in states]

    logger.info("Launched %d feed monitors", len(tasks))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")




# import time
# import logging
# import feedparser
# import requests
# import urllib3

# # Disable warnings for verify=False
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# STATUS_FEED_URL = "https://status.openai.com/feed.atom"
# CHECK_INTERVAL = 20
# LAST_EVENT_ID = None

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s",
# )

# def fetch_feed():
#     try:
#         res = requests.get(
#             STATUS_FEED_URL,
#             timeout=10,
#             verify=False,  # SSL FIX
#             headers={"User-Agent": "Mozilla/5.0"}
#         )
#         res.raise_for_status()
#         feed = feedparser.parse(res.text)
#         return feed.entries
#     except Exception as e:
#         logging.error(f"Error fetching feed: {e}")
#         return []

# def parse_event(entry):
#     title = entry.get("title", "")
#     summary = entry.get("summary", "")
#     eid = entry.get("id", entry.get("link", ""))
#     published = entry.get("published", "")

#     keywords = ["ChatGPT", "Batch API", "Responses", "Embeddings", "gpt", "API"]
#     affected = [k for k in keywords if k.lower() in title.lower()]
#     if not affected:
#         affected = ["General OpenAI Services"]

#     return {
#         "id": eid,
#         "title": title,
#         "summary": summary,
#         "affected": affected,
#         "time": published
#     }

# def monitor_status():
#     global LAST_EVENT_ID

#     logging.info("👀 Starting OpenAI Status Monitor...")

#     while True:
#         entries = fetch_feed()

#         if not entries:
#             logging.warning("⚠️ Unable to read feed.")
#             time.sleep(CHECK_INTERVAL)
#             continue

#         latest = parse_event(entries[0])
#         current_id = latest["id"]

#         if LAST_EVENT_ID is None:
#             LAST_EVENT_ID = current_id
#             logging.info("Initialized baseline event. Monitoring…")
#         elif current_id != LAST_EVENT_ID:
#             LAST_EVENT_ID = current_id

#             logging.info("🔥 NEW INCIDENT DETECTED!")
#             logging.info(f"📌 {latest['title']}")
#             logging.info(f"🔧 {', '.join(latest['affected'])}")
#             logging.info(f"📝 {latest['summary'][:200]}...")
#             logging.info(f"⏱ {latest['time']}")
#         else:
#             logging.info("✓ No new updates")

#         time.sleep(CHECK_INTERVAL)


# if __name__ == "__main__":
#     monitor_status()
