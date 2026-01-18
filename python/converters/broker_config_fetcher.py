"""
FiniexDataCollector - Broker Config Fetcher
Fetches symbol configuration from Kraken REST API.

Creates kraken_public.json with symbol specifications.

Location: python/converters/broker_config_fetcher.py
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

import aiohttp

from python.exceptions.collector_exceptions import BrokerConfigError, ConfigurationError
from python.utils.logging_setup import get_logger


class KrakenBrokerConfigFetcher:
    """
    Fetches broker configuration from Kraken API.

    Uses AssetPairs endpoint to get symbol specifications.
    Output compatible with FiniexTestingIDE BrokerConfig loader.

    STRICT MODE: Raises error if any requested symbol is not found.
    """

    API_BASE = "https://api.kraken.com/0/public"
    CONFIG_FILENAME = "kraken_public.json"

    # Kraken symbol name mappings
    KRAKEN_TO_STANDARD = {
        "XBT": "BTC",
        "XXBT": "BTC",
        "XETH": "ETH",
    }

    def __init__(self, output_dir: Path, symbols: List[str]):
        """
        Initialize broker config fetcher.

        Args:
            output_dir: Directory to write broker config
            symbols: List of symbols to include (REQUIRED)

        Raises:
            ConfigurationError: If symbols list is empty or has duplicates
        """
        if not symbols:
            raise ConfigurationError(
                "Symbols list cannot be empty",
                missing_key="kraken.symbols"
            )

        # Check for duplicates
        normalized_symbols = [self._normalize_symbol(s) for s in symbols]
        duplicates = [
            s for s in normalized_symbols if normalized_symbols.count(s) > 1]
        if duplicates:
            unique_dups = list(set(duplicates))
            raise ConfigurationError(
                f"Duplicate symbols in config: {', '.join(unique_dups)}",
                config_file="app_config.json"
            )

        self._output_dir = Path(output_dir)
        self._symbols = symbols
        self._logger = get_logger("FiniexDataCollector.broker_config")

        # Ensure output directory exists
        self._output_dir.mkdir(parents=True, exist_ok=True)

    async def fetch_and_save(self) -> Tuple[Path, Dict[str, Any]]:
        """
        Fetch symbol info and save to JSON.

        Returns:
            Tuple of (path to config file, config dict)

        Raises:
            BrokerConfigError: If any symbol is not found on Kraken
        """
        self._logger.info("Fetching Kraken broker configuration...")

        # Fetch asset pairs
        pairs_data = await self._fetch_asset_pairs()

        # Build config structure (with strict validation)
        config = self._build_config(pairs_data)

        # Save to file
        output_path = self._output_dir / self.CONFIG_FILENAME

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)

        self._logger.info(f"Broker config saved: {output_path}")
        return output_path, config

    async def _fetch_asset_pairs(self) -> Dict[str, Any]:
        """
        Fetch asset pairs from Kraken API.

        Returns:
            Dict with pair info
        """
        url = f"{self.API_BASE}/AssetPairs"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=30) as response:
                    if response.status != 200:
                        raise BrokerConfigError(
                            f"API returned {response.status}",
                            broker="kraken",
                            endpoint="AssetPairs"
                        )

                    data = await response.json()

                    if data.get("error"):
                        raise BrokerConfigError(
                            f"API error: {data['error']}",
                            broker="kraken",
                            endpoint="AssetPairs"
                        )

                    return data.get("result", {})

        except aiohttp.ClientError as e:
            raise BrokerConfigError(
                f"Network error: {e}",
                broker="kraken",
                endpoint="AssetPairs"
            )

    def _build_config(self, pairs_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build broker config structure.

        STRICT: Raises error if any symbol is not found.

        Args:
            pairs_data: Raw API response

        Returns:
            Complete config dict

        Raises:
            BrokerConfigError: If any requested symbol is not found
        """
        now = datetime.now(timezone.utc)

        config = {
            "_comment": "Static broker configuration for FiniexTestingIDE - Kraken Spot",
            "_version": "1.0",
            "broker_type": "kraken_spot",
            "export_info": {
                "timestamp": now.isoformat(),
                "source": "Kraken Public API",
                "exporter_version": "1.01",
                "symbols_total": len(self._symbols)
            },
            "broker_info": {
                "company": "Kraken",
                "server": "kraken_spot",
                "name": "kraken_public",
                "trade_mode": "demo",
                "api_base_url": "https://api.kraken.com",
                "websocket_url": "wss://ws.kraken.com/v2"
            },
            "fee_structure": {
                "model": "maker_taker",
                "maker_fee": 0.16,
                "taker_fee": 0.26,
                "fee_currency": "quote"
            },
            "trading_permissions": {
                "trade_allowed": True,
                "limit_orders": 1000,
                "order_types": {
                    "market": True,
                    "limit": True,
                    "stop": True,
                    "stop_limit": True
                }
            },
            "symbols": {}
        }

        # Track missing symbols for error reporting
        missing_symbols = []

        # Process each target symbol
        for symbol in self._symbols:
            symbol_config = self._find_and_build_symbol(symbol, pairs_data)
            if symbol_config:
                normalized = self._normalize_symbol(symbol)
                config["symbols"][normalized] = symbol_config
            else:
                missing_symbols.append(symbol)

        # STRICT: Fail if any symbol not found
        if missing_symbols:
            raise BrokerConfigError(
                f"Symbols not found on Kraken: {', '.join(missing_symbols)}. "
                f"Check symbol names in app_config.json (format: BASE/QUOTE, e.g., BTC/USD)",
                broker="kraken",
                endpoint="AssetPairs"
            )

        return config

    def _find_and_build_symbol(
        self,
        target_symbol: str,
        pairs_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Find symbol in API data and build config entry.

        Args:
            target_symbol: Symbol to find (e.g., "BTC/USD")
            pairs_data: API response

        Returns:
            Symbol config dict or None if not found
        """
        # Normalize target for matching
        if "/" not in target_symbol:
            self._logger.error(
                f"Invalid symbol format: {target_symbol} (expected BASE/QUOTE)")
            return None

        target_base, target_quote = target_symbol.split("/")
        target_base = self.KRAKEN_TO_STANDARD.get(target_base, target_base)

        # Search in API data
        for pair_name, pair_info in pairs_data.items():
            wsname = pair_info.get("wsname", "")

            if "/" in wsname:
                api_base, api_quote = wsname.split("/")
                api_base = self.KRAKEN_TO_STANDARD.get(api_base, api_base)

                if api_base == target_base and api_quote == target_quote:
                    return self._build_symbol_config(
                        target_symbol,
                        pair_info
                    )

        return None

    def _build_symbol_config(
        self,
        symbol: str,
        pair_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Build symbol configuration entry.

        Args:
            symbol: Target symbol (e.g., "BTC/USD")
            pair_info: API pair info

        Returns:
            Symbol config dict
        """
        base, quote = symbol.split("/")
        base = self.KRAKEN_TO_STANDARD.get(base, base)

        # Extract values from API
        pair_decimals = pair_info.get("pair_decimals", 1)
        lot_decimals = pair_info.get("lot_decimals", 8)
        ordermin = float(pair_info.get("ordermin", 0.0001))

        # Calculate tick size from decimals
        tick_size = 10 ** (-pair_decimals)

        normalized = self._normalize_symbol(symbol)

        return {
            "path": f"Crypto/Major/{normalized}",
            "description": f"{base} vs {quote}",
            "base_currency": base,
            "quote_currency": quote,
            "trade_mode": "full",
            "trade_allowed": True,
            "volume_min": ordermin,
            "volume_max": 10000.0,
            "volume_step": 10 ** (-lot_decimals),
            "volume_limit": 0.0,
            "contract_size": 1.0,
            "tick_size": tick_size,
            "point": tick_size,
            "digits": pair_decimals,
            "spread_float": False,
            "stops_level": 0,
            "freeze_level": 0
        }

    def _normalize_symbol(self, symbol: str) -> str:
        """
        Normalize symbol to standard format.

        Args:
            symbol: Symbol with slash (e.g., "BTC/USD")

        Returns:
            Normalized symbol (e.g., "BTCUSD")
        """
        if "/" in symbol:
            parts = symbol.split("/")
            base = self.KRAKEN_TO_STANDARD.get(parts[0], parts[0])
            quote = parts[1]
            return f"{base}{quote}"
        return symbol


async def fetch_kraken_broker_config(
    output_dir: Path,
    symbols: List[str]
) -> Tuple[Path, Dict[str, Any]]:
    """
    Convenience function to fetch and save broker config.

    Args:
        output_dir: Directory to save config
        symbols: List of symbols (REQUIRED)

    Returns:
        Tuple of (path to config file, config dict)

    Raises:
        ConfigurationError: If symbols have duplicates
        BrokerConfigError: If any symbol not found on Kraken
    """
    fetcher = KrakenBrokerConfigFetcher(output_dir, symbols)
    return await fetcher.fetch_and_save()
