# BBG Deal Scout

**Automated multifamily property listing scanner for Blue Bear Group Corp.**

Scans Greater Edmonton and Greater Montreal daily for multifamily properties (5–50 units), scores them against BBG's Tier 1 Scorecard, and delivers results via email digest, Slack, and a web dashboard.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    BBG DEAL SCOUT                            │
│                                                              │
│  ┌─────────────┐ ┌─────────────┐ ┌────────────┐ ┌────────┐ │
│  │ Bing Search │ │ RSS Feeds   │ │ URL Watcher│ │ Email  │ │
│  │ API         │ │ (Brokerage) │ │ (Change    │ │ Alert  │ │
│  │ (free tier) │ │             │ │  Detect)   │ │ Parser │ │
│  └──────┬──────┘ └──────┬──────┘ └─────┬──────┘ └───┬────┘ │
│         │               │              │             │       │
│         └───────────────┼──────────────┼─────────────┘       │
│                         ▼                                    │
│              ┌─────────────────────┐                         │
│              │   DEDUPLICATION     │                         │
│              │   (SHA-256 fingerp.)│                         │
│              └─────────┬───────────┘                         │
│                        ▼                                     │
│              ┌─────────────────────┐                         │
│              │   TIER 1 SCORECARD  │                         │
│              │   (auto-score if    │                         │
│              │    data available)   │                         │
│              └─────────┬───────────┘                         │
│                        ▼                                     │
│              ┌─────────────────────┐                         │
│              │   SQLite Database   │                         │
│              └─────────┬───────────┘                         │
│                        │                                     │
│         ┌──────────────┼──────────────┐                      │
│         ▼              ▼              ▼                       │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐               │
│  │ Email      │ │ Slack      │ │ Web        │               │
│  │ Digest     │ │ Webhook    │ │ Dashboard  │               │
│  │ (morning)  │ │ (instant)  │ │ (FastAPI)  │               │
│  └────────────┘ └────────────┘ └────────────┘               │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## Data Sources

| Source | How It Works | Cost |
|--------|-------------|------|
| **Bing Search API** | Runs targeted queries ("multifamily for sale Edmonton") weekly, filtered for listing domains | Free tier: 1,000 calls/month |
| **RSS Feeds** | Monitors brokerage RSS/Atom feeds for new entries | Free |
| **URL Watcher** | Checks brokerage listing pages for content changes, extracts new items | Free |
| **Email Alerts** | Parses Realtor.ca, Centris.ca, LoopNet email alerts via IMAP | Free |

**Total estimated cost: $0–5/month** (Bing free tier covers ~30 queries/day)

---

## Quick Start

### Prerequisites
- Python 3.10+
- A Bing Search API key (free tier — see setup)

### 1. Clone and install

```bash
cd bbg-deal-scout
pip install -r requirements.txt
```

### 2. Run setup wizard

```bash
python -m src.cli setup
```

This creates `config.yaml` from the template and walks you through credentials.

### 3. Configure your Bing API key

Go to [Azure Portal](https://portal.azure.com):
1. Click **Create a resource**
2. Search for **Bing Search v7**
3. Create the resource (free tier = 1,000 calls/month)
4. Copy the API key

Either paste it into `config.yaml`:
```yaml
bing_search:
  api_key: "YOUR_ACTUAL_KEY_HERE"
```

Or set it as an environment variable:
```bash
export BBG_BING_API_KEY="your_key_here"
```

### 4. Run your first scan

```bash
python -m src.cli scan
```

### 5. Start the dashboard

```bash
python -m src.cli dashboard
```

Open `http://localhost:8050` — default login is `admin` / `changeme123`.

**Change the default passwords in config.yaml immediately.**

---

## Daily Automation (Cron)

The simplest deployment for your machine:

```bash
# Open crontab editor
crontab -e

# Add this line — runs at 7:00 AM Mountain Time daily
0 7 * * * cd /full/path/to/bbg-deal-scout && /usr/bin/python3 -m src.cli scan >> logs/cron.log 2>&1
```

To keep the dashboard running permanently:

```bash
# Option A: tmux session (simplest)
tmux new -s dealscout
python -m src.cli dashboard
# Press Ctrl+B then D to detach

# Option B: systemd service (more robust — see below)
```

### Systemd Service (Linux)

Create `/etc/systemd/system/bbg-deal-scout-dashboard.service`:

```ini
[Unit]
Description=BBG Deal Scout Dashboard
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/full/path/to/bbg-deal-scout
ExecStart=/usr/bin/python3 -m src.cli dashboard
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable bbg-deal-scout-dashboard
sudo systemctl start bbg-deal-scout-dashboard
```

---

## Commands Reference

| Command | Description |
|---------|-------------|
| `python -m src.cli scan` | Run a single scan now |
| `python -m src.cli dashboard` | Start web dashboard on port 8050 |
| `python -m src.cli schedule` | Run scans on a daily schedule (foreground) |
| `python -m src.cli setup` | Interactive setup wizard |
| `python -m src.cli stats` | Print current database statistics |
| `python -m src.cli export` | Export all listings to CSV |

---

## Setting Up Email Alerts (Recommended)

This is the highest-signal source — Realtor.ca and Centris alerts catch listings the moment they're published.

1. **Create a dedicated Gmail**: `bbg.dealalerts@gmail.com`
2. **Enable 2FA** on the account
3. **Create an App Password**: Google Account → Security → App Passwords
4. **Set up alerts**:
   - **Realtor.ca**: Search for multifamily in Edmonton → Save Search → Enable email alerts
   - **Centris.ca**: Search for multi-logements in Montreal → Save → Enable alerts
   - **LoopNet.com**: Search multifamily Alberta/Quebec → Save → Enable alerts
5. **Update config.yaml**:
```yaml
email_parsing:
  enabled: true
  email_address: "bbg.dealalerts@gmail.com"
  email_password: "your_app_password_here"
```

---

## Adding Custom Watch URLs

You can monitor any brokerage listing page. Add entries to `custom_watch_urls` in config.yaml:

```yaml
custom_watch_urls:
  - url: "https://www.avenuerealestate.ca/listings?type=multifamily&region=edmonton"
    label: "Avenue RE Edmonton MF"
  - url: "https://www.sutton.com/search?type=revenue&city=montreal"
    label: "Sutton Montreal Revenue"
```

The URL watcher computes a SHA-256 hash of each page's content. When the hash changes, it extracts any listing-like items and adds them to the database.

---

## Tier 1 Scorecard

Each listing is automatically scored on 7 criteria (when data is available):

| # | Check | Threshold |
|---|-------|-----------|
| 1 | Cap rate | ≥ 5.0% |
| 2 | DSCR (estimated) | ≥ 1.20x |
| 3 | Price per unit | ≤ market average |
| 4 | Occupancy | ≥ 85% |
| 5 | Environmental red flags | None detected |
| 6 | Target geography | In BBG regions |
| 7 | Value-add potential | Identified |

Scores are displayed as colored pills in the dashboard:
- **4+/7** = Green (strong lead)
- **2–3/7** = Orange (worth reviewing)
- **0–1/7** = Red (weak or insufficient data)

Most listings from web searches will have limited data (score 1–3). The real value is in surfacing them fast so the team can manually evaluate.

---

## Folder Structure

```
bbg-deal-scout/
├── config.yaml.example      # Template — copy to config.yaml
├── config.yaml               # Your local config (git-ignored)
├── requirements.txt
├── README.md
├── data/
│   └── deal_scout.db         # SQLite database
├── logs/
│   └── deal_scout_YYYYMMDD.log
└── src/
    ├── __init__.py
    ├── cli.py                 # CLI entry point
    ├── config.py              # Config loader
    ├── database.py            # Models & DB operations
    ├── scanner.py             # Main orchestrator
    ├── scoring.py             # Tier 1 Scorecard engine
    ├── collectors/
    │   ├── __init__.py
    │   ├── base.py            # Base collector class
    │   ├── web_search.py      # Bing Search API
    │   ├── rss_monitor.py     # RSS/Atom feed parser
    │   ├── url_watcher.py     # Page change detection
    │   └── email_parser.py    # IMAP email alert parser
    ├── notifications/
    │   ├── __init__.py
    │   ├── email_digest.py    # Morning email digest
    │   └── slack_notify.py    # Slack webhook
    └── dashboard/
        ├── __init__.py
        ├── app.py             # FastAPI dashboard
        └── templates/
            ├── login.html
            └── index.html
```

---

## Environment Variables

All sensitive values can be set via environment variables instead of config.yaml:

| Variable | Overrides |
|----------|-----------|
| `BBG_BING_API_KEY` | `bing_search.api_key` |
| `BBG_EMAIL_PASSWORD` | `email_parsing.email_password` |
| `BBG_SMTP_PASSWORD` | `notifications.email.sender_password` |
| `BBG_SLACK_WEBHOOK` | `notifications.slack.webhook_url` |

---

## Known Limitations

1. **No MLS scraping** — Realtor.ca and Centris.ca block automated scraping. We use their email alerts + Bing search results instead. This is by design (legal compliance).

2. **Data extraction is heuristic** — Unit counts, prices, and cap rates are extracted from text via regex patterns. Expect ~70% accuracy on these fields. Always verify before acting on a listing.

3. **URL watcher is coarse** — It detects *any* page change, not just new listings. Some changes may be cosmetic (updated timestamps, ad rotations). The deduplication layer handles this well over time.

4. **Scoring requires data** — Most web search results won't have enough data for a full 7/7 score. Listings scoring "insufficient data" aren't bad — they just need manual review.

5. **Single machine** — This runs on your local machine via cron. It's not a cloud-hosted SaaS. If your machine is off at scan time, that day's scan is skipped.

---

## Troubleshooting

**"Bing API key not configured"**: Run `python -m src.cli setup` and follow the Azure portal steps.

**No results from Bing**: Check your query limit (free tier = 1,000/month). Also verify queries in `config.yaml` are returning real results manually.

**Email parsing not working**: Verify IMAP credentials, ensure the Gmail app password (not your real password) is correct, and check that alerts are actually arriving in the inbox.

**Dashboard won't start**: Check port 8050 isn't already in use. Try `lsof -i :8050` to diagnose.

**Cron job not running**: Check `crontab -l` to verify the entry exists. Check `logs/cron.log` for errors. Ensure the Python path and project path are absolute.
