# FiniexDataCollector

**Real-time tick data collection for cryptocurrency and forex markets**

> ⚠️ **No financial advice.** This software is for educational and research purposes only.

> **Version:** 1.2.0  
> **Status:** Production Ready  
> **Target:** Developers who need reliable tick data for backtesting systems

---

## What is FiniexDataCollector?

FiniexDataCollector is a real-time tick data collection system that captures market data from cryptocurrency exchanges and forex brokers. It produces standardized JSON tick files compatible with FiniexTestingIDE for backtesting.

**1.0 delivers:**
- ✅ Kraken WebSocket v2 trade + ticker collection, see the symbol table below
- ✅ JSON output format matching MT5 TickCollector
- ✅ Automatic file rotation at 50,000 ticks or the UTC day boundary
- ✅ Write-ahead log, so a crash costs the last tick rather than the whole buffer
- ✅ Live monitoring with disk space, folder scanning, reconnect tracking
- ✅ Telegram bot with commands (/report, /help) for on-demand reports
- ✅ Weekly summary reports (configurable day/time)

---

## Features

### Data Collection
- **Kraken WebSocket v2** - trade stream for the ticks, ticker stream for the quote each
  trade executed against
- **Multi-Symbol Support** - every symbol in `kraken.symbols` collected in parallel
- **Automatic Reconnection** - Exponential backoff (1s → 60s max) with tracking
- **Heartbeat Monitoring** - Detects stale connections and forces reconnect
- **Reconnect Tracking** - Records all reconnect events with duration

### Output Format
- **MT5-Compatible JSON** - Identical structure to TickCollector.mq5 output
- **Configurable Rotation** - Files close at N ticks (default: 50,000)
- **Atomic Writes** - a `*_ticks.json` never exists in a partial state: it is written to a
  temp file and renamed into place. This is the guarantee a consumer relies on. Do not
  "simplify" it into a direct write to the final path.
- **Write-Ahead Log** - every tick is appended to a `.jsonl.part` sidecar before it counts as
  collected, and that log is removed only after the archive file exists. A crash costs the
  last tick rather than the whole buffer; the next start rebuilds the file from the log.
- **Quality Metrics** - Spread calculation, tick frequency, error tracking

### Status API
- **HTTP routes** - liveness, build identity, live metrics, effective configuration,
  archive inventory, finished files and log excerpts. Liveness and build identity are
  open; every other route needs a token carrying that surface as a named grant
- **Off by default** - `api.enabled` in the configuration; it binds loopback and the port
  never reaches the internet directly
- See [docs/architecture/status_api.md](docs/architecture/status_api.md)

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
3. Monitor via Telegram  →  /report command for status
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

# 6. Check status via Telegram
#    Send /report to your bot
```

### Without Docker

```bash
# Install dependencies
pip install -r requirements.txt

# Start collector
python python/main.py collect

# Check status
python python/main.py status
```

---

## Collected Symbols (Default)

| Symbol | Description | Tick Size | Digits |
|--------|-------------|-----------|--------|
| BTCUSD | Bitcoin vs US Dollar | 0.1 | 1 |
| ETHUSD | Ethereum vs US Dollar | 0.01 | 2 |
| SOLUSD | Solana vs US Dollar | 0.01 | 2 |
| ADAUSD | Cardano vs US Dollar | 0.000001 | 6 |
| XRPUSD | Ripple vs US Dollar | 0.00001 | 5 |
| DASHUSD | Dash vs US Dollar | 0.001 | 3 |
| LTCUSD | Litecoin vs US Dollar | 0.01 | 2 |
| ETHEUR | Ethereum vs Euro | 0.01 | 2 |
| DOTUSD | Polkadot vs US Dollar | 0.0001 | 4 |

These are fetched from Kraken's AssetPairs API at every start and are reproduced here for
orientation only - `BrokerConfig` is the authority. SOLUSD, ADAUSD and LTCUSD were wrong in
this table until 2026-09-15, and those two columns decide `spread_points` and the rounding of
every price written.

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
│  │  50k rotation, write-ahead log, atomic writes            │    │
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
    "server": "kraken_websocket",
    "broker_type": "kraken_spot",
    "local_device_time": "2026.03.29 11:34:47",
    "broker_server_time": "2026.03.29 09:34:47",
    "data_format_version": "1.7.0",
    "data_collector": "kraken",
    "collected_msc_timebase": "utc",
    "origin": {
      "instance_id": "a3f8c21d9b04",
      "collected_on": "kraken-prod-01",
      "producer": "finiex-data-collector",
      "producer_version": "1.2.0"
    },
    "anchor_resyncs": 0,
    "anchor_max_correction_ms": 0,
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
      "collected_msc": 1736775045130,
      "bid": 45000.0,
      "ask": 45010.0,
      "last": 45005.0,
      "spread_points": 100,
      "spread_pct": 0.022,
      "quote_age_ms": 84,
      "trade_id": 107991033,
      "session": "24h",
      "tick_flags": "BUY"
    }
  ],
  "summary": {
    "total_ticks": 50000,
    "total_errors": 0,
    "data_stream_status": "HEALTHY",
    "anchor": {
      "resyncs": 0,
      "max_correction_ms": 0
    }
  }
}
```

### Key Tick Fields

- `last`: The execution price - what was actually traded.
- `bid` / `ask` / `spread_points` / `spread_pct`: The quote the trade executed against,
  taken from the ticker channel. Kraken's trade channel reports executions only, so
  without the ticker subscription these would all collapse onto the trade price with a
  spread of zero. A backtest that pays no spread produces a curve that is too
  favourable, and a parameter sweep then optimises against a cost that does not exist.
- `quote_age_ms`: How old the quote was when the trade arrived. `null` means no quote
  had been observed yet - the first trades after a start or reconnect - and the trade
  price fills both sides, as it did before 1.6.0. Never `0` in that case: zero would
  claim a quote seen in the same millisecond. This field is what separates a measured
  spread from a stale one.
- `trade_id`: Kraken's own identifier for the execution, carried through from 1.7.0.
  It makes a tick addressable in the exchange's terms, so a duplicate delivered across
  a reconnect is recognisable as the same execution rather than inferred from matching
  prices and timestamps.
- `tick_flags`: The taker side, `BUY` or `SELL`. A buy lifted the ask, a sell hit the
  bid, which is what makes a later spread reconstruction of older files tractable -
  only the width stays unknown, not the direction.

### Key Metadata Fields

- `data_format_version`: Schema version of the collector output. A constant of the
  code, not a configurable input - it identifies the code that wrote the file.
  Scoped per collector, so it does not move in lockstep with the MT5 collector.
- `collected_msc_timebase`: Time base of `collected_msc`, `"utc"` from 1.5.0 onwards.
  Every tick is stamped from the OS clock in Unix epoch milliseconds UTC, so the
  value lands a few milliseconds after the event time in `time_msc`. Files without
  the field predate the declaration; the import pipeline reads its absence as
  device-local time and refuses to guess.
- `anchor_resyncs` / `anchor_max_correction_ms`: How often a `collected_msc` stamp had
  to be held back because the OS clock stepped backwards, and the largest such step.
  Cumulative over the collection session. They appear again in `summary.anchor` with
  the state at file close, so a file whose closing count exceeds its opening count is
  one that absorbed a correction. Both zero is the normal case.
- `origin`: Which instance produced the file - `instance_id`, `collected_on`,
  `producer` and `producer_version`, from 1.7.0. An identity, not a declaration: the
  collector says who it is and nothing about what that means, and FiniexTestingIDE
  resolves identity to trust in a registry it owns. An `environment: production` field
  would have been the obvious choice and the wrong one - a configuration copied from
  the server to a laptop still says whatever the server said. The id is minted once
  into `instance.json` at the data root; back that file up and never copy it into a
  second data directory. See
  [docs/architecture/output_contract.md](docs/architecture/output_contract.md).
- `local_device_time` / `broker_server_time`: Wall clock of the collecting machine and
  of the exchange at file creation. Informational - nothing downstream derives a UTC
  offset from them.
- `broker_type`: Broker identifier, `"kraken_spot"`. Selects the offset registry entry
  on import, which is 0 h for Kraken.

### File Naming Convention

```
{SYMBOL}_{YYYYMMDD}_{HHMMSS}_ticks.json
{SYMBOL}_{YYYYMMDD}_{HHMMSS}_ticks.jsonl.part  (write-ahead log of the open file)

Example:
BTCUSD_20250113_143052_ticks.json
BTCUSD_20250113_143052_ticks.jsonl.part
```

---

## Status API

The collector answers what it is doing over HTTP, so "is it running, which version, what
has it written" stops being a question that needs a session on the machine. Six routes;
two of them need no credential. Off unless `api.enabled` is set, and it binds loopback.

- **[Status API](docs/architecture/status_api.md)** - the routes and what each answers
- **[Connect contract](docs/architecture/connect_contract.md)** - address, tokens, grants

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

The default logging config writes DEBUG to file only, keeping the console clean for the Live Display:

```json
{
  "logging": {
    "console_level": "INFO",
    "file_level": "DEBUG"
  }
}
```

For troubleshooting, temporarily set `console_level` to `"DEBUG"` (note: this will cause flickering with the Live Display).

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