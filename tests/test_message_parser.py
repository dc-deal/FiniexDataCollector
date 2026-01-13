"""
FiniexDataCollector - Message Parser Tests
Tests for Kraken WebSocket message parsing.

Location: tests/test_message_parser.py
"""

import pytest
import json
import time

from python.collectors.kraken.message_parser import KrakenMessageParser
from python.collectors.kraken.symbols import normalize_symbol, to_kraken_format


class TestKrakenMessageParser:
    """Tests for KrakenMessageParser."""
    
    @pytest.fixture
    def parser(self):
        """Create parser instance."""
        return KrakenMessageParser()
    
    def test_parse_ticker_snapshot(self, parser):
        """Test parsing ticker snapshot message."""
        message = json.dumps({
            "channel": "ticker",
            "type": "snapshot",
            "data": [{
                "symbol": "BTC/USD",
                "bid": 45123.50,
                "bid_qty": 1.5,
                "ask": 45125.75,
                "ask_qty": 2.3,
                "last": 45124.60,
                "volume": 12345.67,
                "vwap": 45100.00,
                "low": 44500.00,
                "high": 45800.00,
                "change": 125.50,
                "change_pct": 0.28
            }]
        })
        
        ticks = parser.parse_message(message)
        
        assert ticks is not None
        assert len(ticks) == 1
        
        tick = ticks[0]
        assert tick.bid == 45123.5
        assert tick.ask == 45125.8  # Rounded to 1 digit for BTC
        assert tick.last == 45124.6
        assert tick.session == "24h"
        assert tick.tick_flags == "BID ASK"
    
    def test_parse_ticker_update(self, parser):
        """Test parsing ticker update message."""
        message = json.dumps({
            "channel": "ticker",
            "type": "update",
            "data": [{
                "symbol": "ETH/USD",
                "bid": 2500.25,
                "bid_qty": 10.0,
                "ask": 2500.75,
                "ask_qty": 15.0,
                "last": 2500.50,
                "volume": 50000.0,
                "vwap": 2495.00,
                "low": 2450.00,
                "high": 2550.00,
                "change": 25.00,
                "change_pct": 1.0
            }]
        })
        
        ticks = parser.parse_message(message)
        
        assert ticks is not None
        assert len(ticks) == 1
        assert ticks[0].bid == 2500.25
    
    def test_skip_non_ticker_messages(self, parser):
        """Test that non-ticker messages return None."""
        messages = [
            json.dumps({"channel": "heartbeat"}),
            json.dumps({"method": "subscribe", "success": True}),
            json.dumps({"channel": "book", "type": "snapshot", "data": []}),
        ]
        
        for msg in messages:
            result = parser.parse_message(msg)
            assert result is None
    
    def test_spread_calculation(self, parser):
        """Test spread points and percentage calculation."""
        message = json.dumps({
            "channel": "ticker",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "bid": 45000.0,
                "ask": 45010.0,
                "last": 45005.0,
                "volume": 100.0,
                "vwap": 45000.0,
                "low": 44000.0,
                "high": 46000.0,
                "change": 0,
                "change_pct": 0
            }]
        })
        
        ticks = parser.parse_message(message)
        tick = ticks[0]
        
        # Spread = 10 USD, tick_size for BTC = 0.1
        # spread_points = 10 / 0.1 = 100
        assert tick.spread_points == 100
        
        # spread_pct = (10 / 45000) * 100 ≈ 0.022%
        assert tick.spread_pct > 0
    
    def test_is_subscription_confirmation(self, parser):
        """Test subscription confirmation detection."""
        confirm_msg = json.dumps({
            "method": "subscribe",
            "success": True,
            "result": {"channel": "ticker"}
        })
        
        non_confirm = json.dumps({
            "channel": "ticker",
            "type": "update"
        })
        
        assert parser.is_subscription_confirmation(confirm_msg) is True
        assert parser.is_subscription_confirmation(non_confirm) is False
    
    def test_is_error_message(self, parser):
        """Test error message detection."""
        error_msg = json.dumps({
            "success": False,
            "error": "Invalid symbol"
        })
        
        success_msg = json.dumps({
            "success": True,
            "result": {}
        })
        
        assert parser.is_error_message(error_msg) == "Invalid symbol"
        assert parser.is_error_message(success_msg) is None
    
    def test_tick_counter_increments(self, parser):
        """Test that chart_tick_volume increments per symbol."""
        message = json.dumps({
            "channel": "ticker",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "bid": 45000.0,
                "ask": 45010.0,
                "last": 45005.0,
                "volume": 100.0,
                "vwap": 45000.0,
                "low": 44000.0,
                "high": 46000.0,
                "change": 0,
                "change_pct": 0
            }]
        })
        
        # Parse multiple times
        tick1 = parser.parse_message(message)[0]
        tick2 = parser.parse_message(message)[0]
        tick3 = parser.parse_message(message)[0]
        
        assert tick1.chart_tick_volume == 1
        assert tick2.chart_tick_volume == 2
        assert tick3.chart_tick_volume == 3
        
        # Reset and verify
        parser.reset_tick_counter()
        tick4 = parser.parse_message(message)[0]
        assert tick4.chart_tick_volume == 1


class TestSymbolNormalization:
    """Tests for symbol normalization utilities."""
    
    def test_normalize_basic(self):
        """Test basic symbol normalization."""
        assert normalize_symbol("BTC/USD") == "BTCUSD"
        assert normalize_symbol("ETH/USD") == "ETHUSD"
        assert normalize_symbol("SOL/USD") == "SOLUSD"
    
    def test_normalize_kraken_xbt(self):
        """Test XBT to BTC mapping."""
        assert normalize_symbol("XBT/USD") == "BTCUSD"
        assert normalize_symbol("XXBT/USD") == "BTCUSD"
    
    def test_to_kraken_format(self):
        """Test conversion to Kraken format."""
        assert to_kraken_format("BTCUSD") == "BTC/USD"
        assert to_kraken_format("ETHUSD") == "ETH/USD"
        assert to_kraken_format("BTC/USD") == "BTC/USD"  # Already formatted
    
    def test_already_normalized(self):
        """Test handling of already normalized symbols."""
        assert normalize_symbol("BTCUSD") == "BTCUSD"
        assert normalize_symbol("ETHEUR") == "ETHEUR"


class TestMultiSymbolParsing:
    """Tests for multi-symbol message handling."""
    
    @pytest.fixture
    def parser(self):
        """Create parser instance."""
        return KrakenMessageParser()
    
    def test_parse_multiple_symbols_in_data(self, parser):
        """Test parsing message with multiple symbols."""
        message = json.dumps({
            "channel": "ticker",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "bid": 45000.0,
                    "ask": 45010.0,
                    "last": 45005.0,
                    "volume": 100.0,
                    "vwap": 45000.0,
                    "low": 44000.0,
                    "high": 46000.0,
                    "change": 0,
                    "change_pct": 0
                },
                {
                    "symbol": "ETH/USD",
                    "bid": 2500.0,
                    "ask": 2501.0,
                    "last": 2500.5,
                    "volume": 1000.0,
                    "vwap": 2495.0,
                    "low": 2450.0,
                    "high": 2550.0,
                    "change": 0,
                    "change_pct": 0
                }
            ]
        })
        
        ticks = parser.parse_message(message)
        
        assert ticks is not None
        assert len(ticks) == 2
        assert ticks[0].bid == 45000.0
        assert ticks[1].bid == 2500.0
