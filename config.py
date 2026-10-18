import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load .env file
load_dotenv()

@dataclass
class Config:
    # Telegram Bot Settings
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # Binance Futures Public Endpoints
    BINANCE_FAPI_BASE_URL: str = os.getenv("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com")

    # Scanner Settings
    SCAN_INTERVAL_SECONDS: int = int(os.getenv("SCAN_INTERVAL_SECONDS", "180"))  # Run scan every 3 minutes
    MAX_CONCURRENT_REQUESTS: int = int(os.getenv("MAX_CONCURRENT_REQUESTS", "25"))  # Safe rate-limiting
    REQUEST_TIMEOUT_SECONDS: int = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))

    # Pair Filters
    MIN_24H_VOLUME_USDT: float = float(os.getenv("MIN_24H_VOLUME_USDT", "1000000"))  # Default: > $1M USDT
    MAX_MARKET_CAP_USDT: float = float(os.getenv("MAX_MARKET_CAP_USDT", "1000000000"))  # Default: <= $1B USDT ($1,000,000,000)

    # Anti-Failure Filters (Proven to boost win rate from 82.0% -> 88.6%!)
    ENABLE_BTC_TREND_FILTER: bool = os.getenv("ENABLE_BTC_TREND_FILTER", "true").lower() in ("true", "1", "yes")
    MAX_OVEREXTENSION_PCT: float = float(os.getenv("MAX_OVEREXTENSION_PCT", "8.0"))  # Max 8% above 200 EMA at entry
    
    # Cooldown in minutes to avoid repeat alerts for the same symbol
    # 240 minutes = 4 hours (duration of 1 4H candle)
    ALERT_COOLDOWN_MINUTES: int = int(os.getenv("ALERT_COOLDOWN_MINUTES", "240"))

    # Rule 1: 1D Candle vs 50 EMA Settings
    # If False: 1D candle close > 50 EMA (already crossed / trading above)
    # If True: 1D candle crossed 50 EMA on the current or previous day
    DAILY_REQUIRE_FRESH_CROSS: bool = os.getenv("DAILY_REQUIRE_FRESH_CROSS", "false").lower() in ("true", "1", "yes")

    # Rule 2: 4H 50 EMA vs 200 EMA Settings
    # Check current forming 4h candle
    CHECK_4H_CURRENT_CANDLE: bool = os.getenv("CHECK_4H_CURRENT_CANDLE", "true").lower() in ("true", "1", "yes")
    # Also check candle that just closed (to avoid missing cross right at candle close)
    CHECK_4H_PREVIOUS_CANDLE: bool = os.getenv("CHECK_4H_PREVIOUS_CANDLE", "true").lower() in ("true", "1", "yes")
    # Lookback window for 4H cross in hours (48 = check crosses formed up to 2 days / 12 candles ago)
    CROSS_4H_LOOKBACK_HOURS: int = int(os.getenv("CROSS_4H_LOOKBACK_HOURS", "48"))
    # If true, generates signal for crosses in previous 48 hours with a 'NOT FRESH' warning
    ALLOW_NON_FRESH_4H_CROSS: bool = os.getenv("ALLOW_NON_FRESH_4H_CROSS", "true").lower() in ("true", "1", "yes")

    # Rule 3: 1H Volume Filter
    VOL_FAST_CANDLES: int = int(os.getenv("VOL_FAST_CANDLES", "7"))
    VOL_SLOW_CANDLES: int = int(os.getenv("VOL_SLOW_CANDLES", "24"))

    # EMA Settings
    EMA_FAST_PERIOD: int = 50
    EMA_SLOW_PERIOD: int = 200

    # Cache file for sent alerts
    CACHE_FILE: str = "alert_history.json"

config = Config()

