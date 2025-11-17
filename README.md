
# OpenAI Status Feed Monitor

A lightweight system for monitoring **OpenAI Status Page** updates via Atom/RSS feeds.

This repo includes:

* **main.py** → A scalable, async, event-driven, ETag-based monitor
* **trial.py** → A simple polling-based version 

Both detect new incidents and print them to the console.
`main.py` additionally supports Slack alerts.

---


## 🚀 Features (main.py)

* Efficient conditional GET (ETag + Last-Modified)
* Detects new incidents without re-reading the feed
* Concurrency for multiple feeds
* Intelligent affected-service extraction
* Optional Slack notifications
* Backoff retry logic
* Easy to add new status feeds (AWS, Cloudflare, etc.)

---

## 📦 Installation

```bash
git clone <repo-url>
cd status_tracker
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## 🔧 Environment Variables

Create a `.env` file:

```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxxx/yyyy/zzzz
CONCURRENT_FETCHES=10
DEFAULT_INTERVAL=300
```

Slack is optional (leave empty to disable).

---

## ▶️ Run (Async Monitor)

```bash
python3 main.py
```

Sample log:

```
Launched 2 feed monitors
Started monitor for openai-atom...
baseline set to <incident-id>
```

When a new incident is released (Example):

<img width="392" height="148" alt="Screenshot 2025-11-17 at 11 46 09 PM" src="https://github.com/user-attachments/assets/90534ca2-acc4-40d4-8a41-c1bdf83525db" />

---

## ▶️ Run (Polling Version)

`trial.py` is the simplest version — useful for demos or testing.

### Run:

```bash
python3 trial.py
```

What it does:

* Polls the status feed every 5 minutes
* Prints new incidents that haven't been seen before
* No Slack, no concurrency, no ETag logic

Example output:

```
=== 🚨 New OpenAI Status Update Detected ===
📌 Title: API Latency Issues
📝 Details: We are currently investigating increased latency...
```

---

## 🛠️ Add More Status Feeds (main.py)

Edit the `FEEDS` section:

```python
FEEDS = [
    {"name": "openai-atom", "url": "https://status.openai.com/feed.atom", "interval": 300},
    {"name": "openai-rss", "url": "https://status.openai.com/feed.rss", "interval": 300},
    # Example:
    # {"name": "aws-health", "url": "https://status.aws.amazon.com/rss/all.rss", "interval": 600},
]
```

---

## 📝 Requirements

```python 
pip install aiohttp feedparser python-dotenv httpx
```

* aiohttp
* feedparser
* httpx
* python-dotenv

