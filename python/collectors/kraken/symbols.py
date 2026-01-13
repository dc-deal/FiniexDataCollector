"""
FiniexDataCollector - Kraken Symbol Utilities
Symbol normalization and mapping for Kraken exchange.

Location: python/collectors/kraken/symbols.py
"""

from typing import Dict, Optional


# Kraken uses XBT for Bitcoin internally
KRAKEN_SYMBOL_MAP: Dict[str, str] = {
    "XBT": "BTC",
    "XXBT": "BTC",
    "XETH": "ETH",
    "XXRP": "XRP",
}

# Reverse mapping for subscription
STANDARD_TO_KRAKEN: Dict[str, str] = {
    "BTC": "XBT",
}


def normalize_symbol(kraken_symbol: str) -> str:
    """
    Convert Kraken symbol to standard format.
    
    Kraken format: "BTC/USD" or "XBT/USD"
    Standard format: "BTCUSD"
    
    Args:
        kraken_symbol: Symbol in Kraken format
        
    Returns:
        Normalized symbol string (e.g., "BTCUSD")
    """
    # Handle already normalized symbols
    if "/" not in kraken_symbol:
        return kraken_symbol.upper()
    
    # Split and normalize
    parts = kraken_symbol.upper().split("/")
    if len(parts) != 2:
        return kraken_symbol.replace("/", "").upper()
    
    base, quote = parts
    
    # Map Kraken-specific names
    base = KRAKEN_SYMBOL_MAP.get(base, base)
    quote = KRAKEN_SYMBOL_MAP.get(quote, quote)
    
    return f"{base}{quote}"


def to_kraken_format(standard_symbol: str) -> str:
    """
    Convert standard symbol to Kraken subscription format.
    
    Standard format: "BTCUSD"
    Kraken format: "BTC/USD"
    
    Args:
        standard_symbol: Symbol in standard format
        
    Returns:
        Symbol in Kraken format (e.g., "BTC/USD")
    """
    # Already in Kraken format
    if "/" in standard_symbol:
        return standard_symbol.upper()
    
    # Common quote currencies
    quote_currencies = ["USD", "EUR", "GBP", "JPY", "USDT", "USDC"]
    
    symbol = standard_symbol.upper()
    
    for quote in quote_currencies:
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            # Map standard to Kraken if needed
            base = STANDARD_TO_KRAKEN.get(base, base)
            return f"{base}/{quote}"
    
    # Fallback: assume last 3 chars are quote
    if len(symbol) >= 6:
        base = symbol[:-3]
        quote = symbol[-3:]
        base = STANDARD_TO_KRAKEN.get(base, base)
        return f"{base}/{quote}"
    
    return symbol


def get_symbol_info(symbol: str) -> Dict[str, str]:
    """
    Get base and quote currency from symbol.
    
    Args:
        symbol: Symbol in any format
        
    Returns:
        Dict with 'base' and 'quote' keys
    """
    kraken_format = to_kraken_format(symbol)
    
    if "/" in kraken_format:
        parts = kraken_format.split("/")
        return {
            "base": parts[0],
            "quote": parts[1],
            "kraken_format": kraken_format,
            "standard_format": normalize_symbol(kraken_format)
        }
    
    return {
        "base": kraken_format,
        "quote": "USD",
        "kraken_format": f"{kraken_format}/USD",
        "standard_format": f"{kraken_format}USD"
    }


def get_tick_size(symbol: str) -> float:
    """
    Get tick size for symbol.
    
    Args:
        symbol: Trading symbol
        
    Returns:
        Tick size (minimum price increment)
    """
    normalized = normalize_symbol(symbol)
    
    # BTC pairs typically have 0.1 tick size
    if normalized.startswith("BTC"):
        return 0.1
    
    # ETH pairs
    if normalized.startswith("ETH"):
        return 0.01
    
    # Most altcoins
    return 0.00001


def get_digits(symbol: str) -> int:
    """
    Get price decimal places for symbol.
    
    Args:
        symbol: Trading symbol
        
    Returns:
        Number of decimal places
    """
    normalized = normalize_symbol(symbol)
    
    if normalized.startswith("BTC"):
        return 1
    
    if normalized.startswith("ETH"):
        return 2
    
    if normalized.startswith(("SOL", "AVAX", "LINK", "DOT")):
        return 3
    
    if normalized.startswith(("ADA", "MATIC")):
        return 5
    
    return 5
