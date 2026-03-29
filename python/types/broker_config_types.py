"""
FiniexDataCollector - Broker Configuration Types
Loads symbol configuration from Kraken public API or JSON file.

Location: python/types/broker_config_types.py
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import aiohttp

from python.exceptions.collector_exceptions import ConfigurationError


@dataclass(frozen=True)
class SymbolConfig:
    """
    Symbol configuration from broker API.

    Attributes:
        symbol: Normalized symbol (e.g., "BTCUSD")
        digits: Decimal places for price
        tick_size: Minimum price increment
        point: Point value (usually same as tick_size)
        volume_min: Minimum order volume
        volume_max: Maximum order volume
        base_currency: Base currency (e.g., "BTC")
        quote_currency: Quote currency (e.g., "USD")
    """
    symbol: str
    digits: int
    tick_size: float
    point: float
    volume_min: float
    volume_max: float
    base_currency: str
    quote_currency: str


class BrokerConfig:
    """
    Broker configuration singleton.

    Broker configuration singleton.

    Loads symbol configs from Kraken API and provides
    lookup methods for tick collection components.

    Usage:
        # Load once at startup
        await BrokerConfig.load_from_api(symbols)

        # Get config anywhere
        config = BrokerConfig.get_symbol("BTCUSD")
        digits = config.digits
    """

    _instance: Optional["BrokerConfig"] = None
    _symbols: Dict[str, SymbolConfig] = {}
    _loaded: bool = False
    _config_path: Optional[Path] = None
    _broker_type: Optional[str] = None
    _server_name: Optional[str] = None

    @classmethod
    def load_from_file(cls, config_path: Path) -> None:
        """
        Load broker configuration from JSON file.

        Args:
            config_path: Path to broker_config.json

        Raises:
            ConfigurationError: If file missing or invalid
        """
        if not config_path.exists():
            raise ConfigurationError(
                f"Broker config not found: {config_path}",
                config_file=str(config_path)
            )

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ConfigurationError(
                f"Invalid JSON in broker config: {e}",
                config_file=str(config_path)
            )

        # Load broker_type (top-level or in broker_info)
        cls._broker_type = data.get("broker_type")
        if not cls._broker_type:
            broker_info = data.get("broker_info", {})
            cls._broker_type = broker_info.get("broker_type")

        # Load server_name from broker_info
        broker_info = data.get("broker_info", {})
        cls._server_name = broker_info.get("server")

        symbols_data = data.get("symbols", {})
        if not symbols_data:
            raise ConfigurationError(
                "No symbols found in broker config",
                config_file=str(config_path)
            )

        cls._symbols = {}
        for symbol, info in symbols_data.items():
            cls._symbols[symbol] = SymbolConfig(
                symbol=symbol,
                digits=info.get("digits", 2),
                tick_size=info.get("tick_size", 0.01),
                point=info.get("point", 0.01),
                volume_min=info.get("volume_min", 0.001),
                volume_max=info.get("volume_max", 10000.0),
                base_currency=info.get("base_currency", ""),
                quote_currency=info.get("quote_currency", "")
            )

        cls._loaded = True
        cls._config_path = config_path

    @classmethod
    async def load_from_api(cls, symbols: list) -> None:
        """
        Load symbol configuration from Kraken public AssetPairs API.

        Args:
            symbols: List of symbols from config (e.g., ["BTC/USD", "ETH/USD"])

        Raises:
            ConfigurationError: If API fails or symbols not found
        """
        # Build pair keys: "BTC/USD" -> "BTCUSD"
        pair_keys = [s.replace("/", "") for s in symbols]
        url = "https://api.kraken.com/0/public/AssetPairs"
        params = {"pair": ",".join(pair_keys)}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, ssl=False, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
        except (aiohttp.ClientError, TimeoutError) as e:
            raise ConfigurationError(
                f"Failed to fetch symbol config from Kraken API: {e}"
            )

        errors = data.get("error", [])
        if errors:
            raise ConfigurationError(
                f"Kraken API error: {', '.join(errors)}"
            )

        result = data.get("result", {})
        if not result:
            raise ConfigurationError(
                "Kraken API returned no symbol data"
            )

        cls._symbols = {}
        for _pair_key, info in result.items():
            wsname = info.get("wsname", "")
            if not wsname:
                continue

            normalized = normalize_symbol(wsname)
            tick_size = float(info.get("tick_size", "0.01"))
            digits = int(info.get("pair_decimals", 2))

            base, _, quote = wsname.partition("/")

            cls._symbols[normalized] = SymbolConfig(
                symbol=normalized,
                digits=digits,
                tick_size=tick_size,
                point=tick_size,
                volume_min=float(info.get("ordermin", "0.001")),
                volume_max=10000.0,
                base_currency=base,
                quote_currency=quote
            )

        cls._broker_type = "kraken_spot"
        cls._loaded = True
        cls._config_path = None

    @classmethod
    def is_loaded(cls) -> bool:
        """Check if config is loaded."""
        return cls._loaded

    @classmethod
    def get_symbol(cls, symbol: str) -> SymbolConfig:
        """
        Get configuration for a symbol.

        Args:
            symbol: Normalized symbol (e.g., "BTCUSD")

        Returns:
            SymbolConfig instance

        Raises:
            ConfigurationError: If symbol not found or config not loaded
        """
        if not cls._loaded:
            raise ConfigurationError(
                "Broker config not loaded. Call BrokerConfig.load_from_api() first."
            )

        if symbol not in cls._symbols:
            available = ", ".join(sorted(cls._symbols.keys()))
            raise ConfigurationError(
                f"Symbol '{symbol}' not found in broker config. "
                f"Available: {available}",
                missing_key=symbol
            )

        return cls._symbols[symbol]

    @classmethod
    def get_digits(cls, symbol: str) -> int:
        """Get digits for symbol."""
        return cls.get_symbol(symbol).digits

    @classmethod
    def get_tick_size(cls, symbol: str) -> float:
        """Get tick size for symbol."""
        return cls.get_symbol(symbol).tick_size

    @classmethod
    def get_all_symbols(cls) -> list:
        """Get list of all configured symbols."""
        return list(cls._symbols.keys())

    @classmethod
    def has_symbol(cls, symbol: str) -> bool:
        """Check if symbol exists in config."""
        return symbol in cls._symbols

    @classmethod
    def get_broker_type(cls) -> Optional[str]:
        """
        Get broker type identifier.

        Returns:
            Broker type string or None if not loaded
        """
        if not cls._loaded:
            raise ConfigurationError(
                "Broker config not loaded. Call BrokerConfig.load_from_api() first."
            )
        return cls._broker_type

    @classmethod
    def get_server_name(cls) -> Optional[str]:
        """
        Get server name.

        Returns:
            Server name string or None if not loaded
        """
        if not cls._loaded:
            raise ConfigurationError(
                "Broker config not loaded. Call BrokerConfig.load_from_api() first."
            )
        return cls._server_name

    @classmethod
    def reset(cls) -> None:
        """Reset loaded config (for testing)."""
        cls._symbols = {}
        cls._loaded = False
        cls._config_path = None
        cls._broker_type = None
        cls._server_name = None


# =============================================================================
# Symbol Name Utilities (moved from symbols.py)
# =============================================================================

# Kraken uses XBT instead of BTC
KRAKEN_SYMBOL_MAP = {
    "XBT": "BTC",
    "XXBT": "BTC",
    "XETH": "ETH",
    "XLTC": "LTC",
    "XXRP": "XRP",
    "XDASH": "DASH",
}

REVERSE_SYMBOL_MAP = {v: k for k, v in KRAKEN_SYMBOL_MAP.items()}


def normalize_symbol(kraken_symbol: str) -> str:
    """
    Normalize Kraken symbol to standard format.

    Args:
        kraken_symbol: Kraken format (e.g., "BTC/USD", "XBT/USD")

    Returns:
        Normalized symbol (e.g., "BTCUSD")

    Examples:
        "BTC/USD" -> "BTCUSD"
        "XBT/USD" -> "BTCUSD"
        "ETH/EUR" -> "ETHEUR"
    """
    # Remove slash
    symbol = kraken_symbol.replace("/", "")

    # Map Kraken-specific symbols
    for kraken, standard in KRAKEN_SYMBOL_MAP.items():
        if symbol.startswith(kraken):
            symbol = symbol.replace(kraken, standard, 1)
            break

    return symbol.upper()


def to_kraken_format(symbol: str) -> str:
    """
    Convert normalized symbol to Kraken WebSocket format.

    Args:
        symbol: Normalized symbol (e.g., "BTCUSD") or already formatted

    Returns:
        Kraken format (e.g., "BTC/USD")

    Examples:
        "BTCUSD" -> "BTC/USD"
        "BTC/USD" -> "BTC/USD"
    """
    # Already has slash
    if "/" in symbol:
        return symbol

    # Common quote currencies
    for quote in ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"]:
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            return f"{base}/{quote}"

    # Fallback: assume last 3 chars are quote
    if len(symbol) >= 6:
        return f"{symbol[:-3]}/{symbol[-3:]}"

    return symbol
