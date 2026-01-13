# FiniexDataCollector

**Real-time tick data collection for cryptocurrency and forex markets**

> ⚠️ **No financial advice.** This software is for educational and research purposes only.

> **Version:** 1.0 Alpha  
> **Status:** Core Collection Implemented  
> **Target:** Developers who need reliable tick data for backtesting systems

---

## What is FiniexDataCollector?

FiniexDataCollector is a real-time tick data collection system that captures market data from cryptocurrency exchanges and forex brokers. It produces standardized JSON tick files compatible with FiniexTestingIDE for backtesting.

**1.0 Alpha delivers:**
- ✅ Kraken WebSocket v2 ticker collection (8 crypto pairs)
- ✅ JSON output format matching MT5 TickCollector
- ✅ Automatic file rotation at 50,000 ticks
- ✅ Lock file protection for active files
- ✅ Weekly Parquet conversion (Saturday 06:00 UTC)
- ✅ Telegram alerts for errors and weekly reports

---

## Features

### Data Collection
- **Kraken WebSocket v2** - Real-time ticker stream for crypto pairs
- **Multi-Symbol Support** - 8 symbols simultaneously (BTC, ETH, SOL, ADA, MATIC, AVAX, LINK, DOT)
- **Automatic Reconnection** - Exponential backoff (1s → 60s max)
- **Heartbeat Monitoring** - Detects stale connections

### Output Format
- **MT5-Compatible JSON** - Identical structure to TickCollector.mq5 output
- **50k Tick Rotation** - Files close at 50,000 ticks
- **Lock File Protection** - `.lock` files prevent processing of active files
- **Quality Metrics** - Spread calculation, tick frequency, error tracking

### Weekly Processing
- **Parquet Conversion** - JSON → Parquet every Saturday 06:00 UTC
- **Broker Config Fetch** - Symbol specifications from Kraken API
- **Telegram Reports** - Weekly summary with statistics

### Alerting
- **Telegram Bot Integration** - Error alerts, rotation notices, weekly reports
- **Configurable Triggers** - Enable/disable per alert type

---

## Quick Start

```
1. Configure Telegram    →  configs/app_config.json
2. Start collector       →  docker-compose up -d
3. Fetch broker config   →  docker-compose run broker-config
4. Weekly conversion     →  Automatic (Saturday 06:00 UTC)
```

### Detailed Setup

```bash
# 1. Clone/Extract project
cd FiniexDataCollector

# 2. Configure (edit configs/app_config.json)
#    - Set telegram.bot_token and telegram.chat_id
#    - Adjust symbols if needed

# 3. Start collection
docker-compose up -d collector

# 4. Monitor logs
docker logs -f finiex-data-collector

# 5. Fetch broker config (once)
docker-compose run --rm broker-config

# 6. Manual conversion (optional)
docker-compose run --rm converter
```

### Without Docker

```bash
# Install dependencies
pip install -r requirements.txt

# Start collector
python python/main.py collect

# Fetch broker config
python python/main.py broker-config

# Run conversion
python python/main.py convert

# Check status
python python/main.py status
```

---

## Collected Symbols

| Symbol | Description | Tick Size | Digits |
|--------|-------------|-----------|--------|
| BTCUSD | Bitcoin vs US Dollar | 0.1 | 1 |
| ETHUSD | Ethereum vs US Dollar | 0.01 | 2 |
| SOLUSD | Solana vs US Dollar | 0.001 | 3 |
| ADAUSD | Cardano vs US Dollar | 0.00001 | 5 |
| MATICUSD | Polygon vs US Dollar | 0.00001 | 5 |
| AVAXUSD | Avalanche vs US Dollar | 0.001 | 3 |
| LINKUSD | Chainlink vs US Dollar | 0.001 | 3 |
| DOTUSD | Polkadot vs US Dollar | 0.001 | 3 |

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
│  │  JSON TICK WRITER                                       │    │
│  │  50k rotation, .lock protection, atomic writes          │    │
│  └─────────────────────────────────────────────────────────┘    │
│         │                                                       │
│         ▼                                                       │
│  data/raw/kraken/{SYMBOL}/{SYMBOL}_{TIMESTAMP}_ticks.json       │
│         │                                                       │
│         │ (Saturday 06:00 UTC)                                  │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  PARQUET CONVERTER                                      │    │
│  │  JSON → Parquet, metadata preservation                  │    │
│  └─────────────────────────────────────────────────────────┘    │
│         │                                                       │
│         ▼                                                       │
│  data/processed/kraken/ticks/{SYMBOL}/*.parquet                 │
│         │                                                       │
│         │ (Transfer to FiniexTestingIDE)                        │
│         ▼                                                       │
│  FiniexTestingIDE/data/processed/kraken/ticks/                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
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

### Main Config (configs/app_config.json)

```json
{
  "kraken": {
    "enabled": true,
    "symbols": ["BTC/USD", "ETH/USD", ...],
    "max_ticks_per_file": 50000
  },
  "telegram": {
    "enabled": true,
    "bot_token": "YOUR_BOT_TOKEN",
    "chat_id": "YOUR_CHAT_ID",
    "send_on_error": true,
    "send_weekly_report": true
  },
  "scheduler": {
    "conversion_day": "saturday",
    "conversion_hour_utc": 6
  }
}
```

### Telegram Bot Setup

1. Message [@BotFather](https://t.me/botfather) on Telegram
2. Create new bot: `/newbot`
3. Copy the bot token to `app_config.json`
4. Get your chat ID (message [@userinfobot](https://t.me/userinfobot))
5. Set `telegram.enabled: true`

---

## Testing

### Unit Tests (21 tests)

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_message_parser.py -v
pytest tests/test_json_writer.py -v
```

### Test Coverage

| Module | Tests | Coverage |
|--------|-------|----------|
| Message Parser | 12 | Symbol normalization, tick parsing, error detection |
| JSON Writer | 9 | File rotation, lock files, content format |

---

## Directory Structure

```
FiniexDataCollector/
├── configs/
│   ├── app_config.json           # Main configuration
│   └── brokers/
│       └── kraken_demo.json      # Generated broker config
├── data/
│   ├── raw/kraken/{SYMBOL}/      # JSON tick files
│   └── processed/kraken/ticks/   # Parquet files
├── logs/                         # Application logs
├── python/
│   ├── collectors/kraken/        # WebSocket client
│   ├── writers/                  # JSON tick writer
│   ├── converters/               # Parquet converter
│   ├── alerts/                   # Telegram integration
│   ├── scheduler/                # Weekly jobs
│   └── main.py                   # Entry point
└── tests/                        # Unit tests
```

---

## Current Limitations (Alpha)

- **Kraken Only** - No MT5 live collection (uses existing MQL5 scripts)
- **Spot Markets Only** - No futures or margin data
- **No Automatic Transfer** - Manual copy to FiniexTestingIDE required
- **Single Instance** - No horizontal scaling

> **Note on 24/7 Operation:** Crypto markets never close. The collector runs continuously with automatic reconnection. Weekly conversion only processes completed files (no .lock).

---

## Vision & Roadmap

### Post-Alpha (Next)
- MT5 JSON consolidation (from existing TickCollector.mq5)
- Automatic rsync transfer to FiniexTestingIDE
- More crypto exchanges (Binance, Coinbase)

### Phase 2: Multi-Source
| Source | Type | Status |
|--------|------|--------|
| Kraken WebSocket | Crypto Spot | ✅ Alpha |
| MT5 TickCollector | Forex/CFD | Planned |
| Binance WebSocket | Crypto Spot | Planned |

### Phase 3: Distribution
- Compressed Parquet delivery
- Weekly data packages
- Cloud storage integration

### Phase 4: Service Layer
- REST API for data queries
- Real-time streaming to clients
- Multi-tenant support

---

## Integration with FiniexTestingIDE

FiniexDataCollector outputs are designed for direct use with FiniexTestingIDE:

```bash
# After weekly conversion, copy to FiniexTestingIDE
cp -r data/processed/kraken/* /path/to/FiniexTestingIDE/data/processed/kraken/

# Rebuild indexes in FiniexTestingIDE
python python/cli/data_index_cli.py rebuild
python python/cli/bar_index_cli.py render --clean
```

The Parquet format is identical - no conversion needed.

---

## License

MIT License - see [LICENSE](LICENSE)

**Trademarks:** Finiex™ is property of Frank Krätzig
