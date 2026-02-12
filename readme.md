# FiniexDataCollector

**Real-time tick data collection for cryptocurrency and forex markets**

> ⚠️ **No financial advice.** This software is for educational and research purposes only.

> **Version:** 1.0  
> **Status:** Production Ready  
> **Target:** Developers who need reliable tick data for backtesting systems

---

## What is FiniexDataCollector?

FiniexDataCollector is a real-time tick data collection system that captures market data from cryptocurrency exchanges and forex brokers. It produces standardized JSON tick files compatible with FiniexTestingIDE for backtesting.

**1.0 delivers:**
- ✅ Kraken WebSocket v2 ticker collection (8 crypto pairs)
- ✅ JSON output format matching MT5 TickCollector
- ✅ Automatic file rotation at 50,000 ticks
- ✅ Lock file protection for active files
- ✅ Live monitoring with disk space, folder scanning, reconnect tracking
- ✅ Telegram bot with commands (/report, /help) for on-demand reports
- ✅ Weekly summary reports (configurable day/time)

---

## Features

### Data Collection
- **Kraken WebSocket v2** - Real-time ticker stream for crypto pairs
- **Multi-Symbol Support** - 8 symbols simultaneously (BTC, ETH, SOL, ADA, XRP, DASH, LTC, ETH/EUR)
- **Automatic Reconnection** - Exponential backoff (1s → 60s max) with tracking
- **Heartbeat Monitoring** - Detects stale connections and forces reconnect
- **Reconnect Tracking** - Records all reconnect events with duration

### Output Format
- **MT5-Compatible JSON** - Identical structure to TickCollector.mq5 output
- **Configurable Rotation** - Files close at N ticks (default: 50,000)
- **Lock File Protection** - `.lock` files prevent processing of active files
- **Quality Metrics** - Spread calculation, tick frequency, error tracking

### Monitoring & Health
- **Live Display** - Real-time status with Rich TUI interface
- **Disk Space Monitoring** - Continuous tracking with critical alerts (<20% free)
- **Folder Scanning** - Automatic file counting per broker/symbol
- **Reconnect Tracking** - Duration tracking with configurable alerts
- **Connection Health** - WebSocket status and error tracking

### Telegram Integration
- **Bot Commands**:
  - `/report` - Generate weekly report on demand
  - `/help` - Show available commands
- **Automatic Alerts**:
  - File rotation notices (optional)
  - Reconnect warnings (with cooldown)
  - Critical disk space alerts
  - Collector start/stop notifications
- **Weekly Reports** - Scheduled summary with folder sizes, statistics

### Configuration System
- **Pydantic Validation** - Type-safe configuration with clear error messages
- **User Override Pattern** - `user_configs/app_config.json` overrides base config
- **Environment Separation** - Gitignored user config for secrets (bot tokens)
- **Flexible Scheduling** - Configurable report day/hour/minute in UTC

---

## Quick Start

```
1. Configure Telegram    →  user_configs/app_config.json
2. Start collector       →  docker-compose up -d
3. Fetch broker config   →  docker-compose run broker-config
4. Monitor via Telegram  →  /report command for status
```

### Detailed Setup

```bash
# 1. Clone/Extract project
cd FiniexDataCollector

# 2. Create user config
cp user_configs/app_config.example.json user_configs/app_config.json

# 3. Configure (edit user_configs/app_config.json)
#    - Set telegram.bot_token and telegram.chat_id
#    - Adjust symbols if needed (or use defaults)
#    - Set max_ticks_per_file (default: 50000)

# 4. Start collection
docker-compose up -d collector

# 5. Monitor logs
docker logs -f finiex-data-collector

# 6. Fetch broker config (once)
docker-compose run --rm broker-config

# 7. Check status via Telegram
#    Send /report to your bot
```

### Without Docker

```bash
# Install dependencies
pip install -r requirements.txt

# Start collector
python python/main.py collect

# Fetch broker config
python python/main.py broker-config

# Check status
python python/main.py status
```

---

## Collected Symbols (Default)

| Symbol | Description | Tick Size | Digits |
|--------|-------------|-----------|--------|
| BTCUSD | Bitcoin vs US Dollar | 0.1 | 1 |
| ETHUSD | Ethereum vs US Dollar | 0.01 | 2 |
| SOLUSD | Solana vs US Dollar | 0.001 | 3 |
| ADAUSD | Cardano vs US Dollar | 0.00001 | 5 |
| XRPUSD | Ripple vs US Dollar | 0.00001 | 5 |
| DASHUSD | Dash vs US Dollar | 0.001 | 3 |
| LTCUSD | Litecoin vs US Dollar | 0.001 | 3 |
| ETHEUR | Ethereum vs Euro | 0.01 | 2 |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA FLOW                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Kraken WebSocket v2                                            │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  MESSAGE PARSER                                         │    │
│  │  Kraken JSON → TickData                                 │    │
│  │  Symbol normalization (BTC/USD → BTCUSD)                │    │
│  └─────────────────────────────────────────────────────────┘    │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  MONITORING & STATS                                     │    │
│  │  • Disk space tracking                                  │    │
│  │  • Folder file counts                                   │    │
│  │  • Reconnect events                                     │    │
│  │  • Live display updates                                 │    │
│  └─────────────────────────────────────────────────────────┘    │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  JSON TICK WRITER                                       │    │
│  │  50k rotation, .lock protection, atomic writes          │    │
│  └─────────────────────────────────────────────────────────┘    │
│         │                                                       │
│         ▼                                                       │
│  data/raw/kraken/{SYMBOL}_{TIMESTAMP}_ticks.json                │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  TELEGRAM BOT                                           │    │
│  │  • Commands: /report, /help                             │    │
│  │  • Alerts: rotation, reconnect, disk space              │    │
│  │  • Weekly reports (scheduled)                           │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Live Display

FiniexDataCollector includes a rich terminal UI showing real-time status:

```
╭────────────────────────────── 📡 FiniexDataCollector Live ──────────────────────────────╮
│ 📋 Streams: trade │ ⏱️ Uptime: 02:15:33 │ 📁 Files: 24 │ 🔌 WS: ● connected             │
│                                                                                         │
│ 💾 Disk: 503.3 GB free (53%) ✅ │ Last Check: Sun 08.02 17:30                           │
│                                                                                         │
│   Symbol       Current File      Files    Last Price    Volume    Status               │
│  ─────────────────────────────────────────────────────────────────────────────          │
│   BTCUSD      5,234 / 50,000    2        71,158.30     0.0004     ✅ Active            │
│   ETHUSD      12,456 / 50,000   1         2,112.35     0.0010     ✅ Active            │
│   ...                                                                                   │
│                                                                                         │
│ 📁 Storage: Kraken: 24 files │ MT5: - │ Logs: 3 files │ Reconnects: 1                  │
│                                                                                         │
│ 📄 Last file: BTCUSD_20260208_160013_ticks.json (50,000 ticks)                         │
╰─────────────────────────────────────────────────────────────────────────────────────────╯
```

---

## Output Format

### JSON Tick File Structure

```json
{
  "metadata": {
    "symbol": "BTCUSD",
    "broker": "Kraken",
    "server": "kraken_spot",
    "data_format_version": "1.0.5",
    "data_collector": "kraken",
    "symbol_info": {
      "point_value": 0.1,
      "digits": 1,
      "tick_size": 0.1
    },
    "collection_settings": {
      "max_ticks_per_file": 50000
    }
  },
  "ticks": [
    {
      "timestamp": "2025.01.13 14:30:45",
      "time_msc": 1736775045123,
      "bid": 45000.0,
      "ask": 45010.0,
      "last": 45005.0,
      "spread_points": 100,
      "spread_pct": 0.022,
      "session": "24h",
      "tick_flags": "BID ASK"
    }
  ],
  "summary": {
    "total_ticks": 50000,
    "total_errors": 0,
    "data_stream_status": "HEALTHY"
  }
}
```

### File Naming Convention

```
{SYMBOL}_{YYYYMMDD}_{HHMMSS}_ticks.json
{SYMBOL}_{YYYYMMDD}_{HHMMSS}_ticks.json.lock  (active file)

Example:
BTCUSD_20250113_143052_ticks.json
BTCUSD_20250113_143052_ticks.json.lock
```

---

## Configuration

### Configuration Files Structure

```
FiniexDataCollector/
├── configs/
│   └── app_config.json          # Base configuration (version controlled)
└── user_configs/
    ├── app_config.json          # User overrides (gitignored, for secrets)
    └── app_config.example.json  # Template for user config
```

**Configuration Loading:**
1. Load `configs/app_config.json` (base)
2. Merge with `user_configs/app_config.json` (overrides)
3. Validate with Pydantic schemas

### User Config Override (user_configs/app_config.json)

```json
{
  "telegram": {
    "enabled": true,
    "bot_token": "YOUR_BOT_TOKEN_HERE",
    "chat_id": "YOUR_CHAT_ID_HERE",
    "send_on_rotation": true
  },
  "kraken": {
    "symbols": ["BTC/USD", "ETH/USD"],
    "max_ticks_per_file": 100
  }
}
```

### Configuration Options

#### Kraken Section
- `enabled` - Enable Kraken collection
- `symbols` - Array of Kraken symbol pairs (e.g., "BTC/USD")
- `max_ticks_per_file` - Ticks before file rotation (min: 100)
- `streams` - WebSocket streams to subscribe (default: ["trade"])

#### Monitoring Section
- `disk_space_check_interval_seconds` - How often to check disk space (10-600s)
- `folder_scan_interval_seconds` - How often to scan folders (10-600s)
- `reconnect_alert_cooldown_minutes` - Min time between reconnect alerts (1-1440min)

#### Telegram Section
- `enabled` - Enable Telegram integration
- `bot_token` - Telegram bot token from @BotFather
- `chat_id` - Your Telegram chat ID from @userinfobot
- `send_on_rotation` - Alert when files rotate
- `send_on_error` - Alert on errors
- `send_weekly_report` - Send scheduled reports

#### Scheduler Section
- `report_day` - Day for weekly report (monday-sunday)
- `report_hour_utc` - Hour for report (0-23 UTC)
- `report_minute_utc` - Minute for report (0-59)

### Telegram Bot Setup

1. Message [@BotFather](https://t.me/botfather) on Telegram
2. Create new bot: `/newbot`
3. Copy the bot token to `user_configs/app_config.json`
4. Get your chat ID (message [@userinfobot](https://t.me/userinfobot))
5. Set `telegram.enabled: true`
6. Test with `/help` command

**Available Commands:**
- `/report` - Generate weekly summary on demand
- `/help` - Show available commands

---

## Telegram Alerts

### File Rotation Notice
```
📁 File Rotation: BTCUSD
File: BTCUSD_20260208_160013_ticks.json
Ticks: 50,000
```

### Reconnect Warning
```
🔌 Connection Restored
WebSocket reconnected after 3m downtime
```

### Weekly Report
```
📊 Weekly Collection Report
Sunday, 08.02.2026 08:00 UTC

⏱️ Uptime
• Runtime: 167.5 hours
• Files Created: 248
• Errors: 0 | Warnings: 2

📁 Data Storage
• Kraken: 2.45 GB (248 files)
• MT5: 0.00 GB (0 files)
• Logs: 0.15 GB (7 files)
• Total Data: 2.60 GB

💾 Disk Space
• Total: 952.6 GB
• Used: 449.3 GB (47%)
• Free: 503.3 GB (53%) ✅

🔌 Connection Health
• Reconnects This Week: 3
• Current Status: connected

📈 Per Symbol
• BTCUSD: 42 files created
• ETHUSD: 38 files created
• ...
```

## Vision & Roadmap

For Vision & Roadmap see issue:

- [#8 — FiniexDataCollector Vision & Roadmap](https://github.com/dc-deal/FiniexDataCollector/issues/8)

## Integration with FiniexTestingIDE

FiniexDataCollector outputs JSON tick files that can be processed for use with FiniexTestingIDE. See:

- [#138 — FiniexTestingIDE Vision & Roadmap](https://github.com/dc-deal/FiniexTestingIDE/issues/138)


## Debug Mode

For troubleshooting, enable DEBUG logging:

```json
{
  "logging": {
    "console_level": "DEBUG",
    "file_level": "DEBUG"
  }
}
```

Debug logs include structured markers for filtering:
- `[TICK]` - Tick processing
- `[ROTATION]` - File rotation events
- `[STATUS]` - WebSocket status changes
- `[RECONNECT]` - Reconnect tracking
- `[FOLDER_SCAN]` - Folder monitoring
- `[DISK_MONITOR]` - Disk space checks
- `[TELEGRAM]` - Telegram operations

**Filter logs:**
```bash
# Live filtering
tail -f logs/collector_*.log | grep "\[ROTATION\]\|\[RECONNECT\]"

# Search for specific events
grep "\[RECONNECT\]" logs/collector_20260208.log
```

---

## License

MIT License - see [LICENSE](LICENSE)