# !pip install feedparser

import feedparser
import time

FEED_URL = "https://status.openai.com/history.atom"
seen_ids = set()

while True:
    feed = feedparser.parse(FEED_URL)

    for entry in feed.entries:
        entry_id = entry.get("id") or entry.get("link")
        if entry_id not in seen_ids:
            seen_ids.add(entry_id)
            title = entry.get("title", "No title")
            summary = entry.get("summary", "No details")
            print("=== 🚨 New OpenAI Status Update Detected ===")
            print(f"📌 Title: {title}")
            print(f"📝 Details: {summary}\n")

    time.sleep(300)
