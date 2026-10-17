import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import aiohttp
from config import config

logger = logging.getLogger("BinanceClient")

MCAP_CACHE_FILE = "market_caps_cache.json"
MCAP_CACHE_DURATION_SECONDS = 4 * 3600  # 4 hours in seconds

class BinanceFuturesClient:
    """
    High-performance async client for Binance Futures Public Endpoints (USDⓈ-M).
    No API Key required.
    """
    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self.base_url = config.BINANCE_FAPI_BASE_URL.rstrip('/')
        self._session = session
        self._own_session = False
        self.semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json"
        }
        self._mcap_cache: Dict[str, float] = {}
        self._mcap_cache_time: float = 0.0

    async def __aenter__(self):
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT_SECONDS)
            connector = aiohttp.TCPConnector(limit=100, ssl=False)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector, headers=self.headers)
            self._own_session = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._own_session and self._session:
            await self._session.close()

    async def get_active_usdt_pairs(self) -> List[str]:
        """
        Fetch all active USDT-M Perpetual futures trading pairs.
        """
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        try:
            async with self.semaphore:
                async with self._session.get(url) as response:
                    if response.status != 200:
                        logger.error(f"Failed to fetch exchange info, status {response.status}")
                        return []
                    data = await response.json()
                    symbols = data.get("symbols", [])
                    usdt_pairs = [
                        s["symbol"] for s in symbols
                        if s.get("status") == "TRADING"
                        and s.get("contractType") == "PERPETUAL"
                        and s.get("quoteAsset") == "USDT"
                    ]
                    logger.info(f"Loaded {len(usdt_pairs)} active USDT-M Perpetual contracts.")
                    return sorted(usdt_pairs)
        except Exception as e:
            logger.error(f"Error fetching active pairs: {e}")
            return []

    async def get_market_caps(self) -> Dict[str, float]:
        """
        Fetch market caps for top 1000 coins with 4-hour persistent caching.
        Only requests new data once every 4 hours (14,400s) to avoid API spam.
        """
        now = time.time()

        # 1. Check in-memory cache
        if self._mcap_cache and (now - self._mcap_cache_time) < MCAP_CACHE_DURATION_SECONDS:
            return self._mcap_cache

        # 2. Check disk cache file
        if os.path.exists(MCAP_CACHE_FILE):
            try:
                with open(MCAP_CACHE_FILE, "r", encoding="utf-8") as f:
                    cache_obj = json.load(f)
                    cached_ts = cache_obj.get("timestamp", 0)
                    cached_data = cache_obj.get("data", {})
                    age_seconds = now - cached_ts
                    if age_seconds < MCAP_CACHE_DURATION_SECONDS and cached_data:
                        self._mcap_cache = cached_data
                        self._mcap_cache_time = cached_ts
                        age_hours = age_seconds / 3600.0
                        remaining_min = (MCAP_CACHE_DURATION_SECONDS - age_seconds) / 60.0
                        logger.info(f"Using cached Market Cap data ({len(cached_data)} coins, {age_hours:.1f}h old). Next refresh in {remaining_min:.0f} min.")
                        return self._mcap_cache
            except Exception as e:
                logger.debug(f"Error reading {MCAP_CACHE_FILE}: {e}")

        # 3. Cache expired or missing -> Request fresh data
        logger.info("Market Cap 4-hour cache expired or missing. Requesting updated market cap data...")
        url = "https://www.binance.com/bapi/composite/v1/public/promo/cmc/cryptocurrency/listings/latest?limit=1000"
        try:
            async with self.semaphore:
                async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        d = await response.json()
                        data = d.get("data", {}).get("body", {}).get("data", [])
                        mcap_map = {}
                        for item in data:
                            sym = item.get("symbol")
                            usd = item.get("quote", {}).get("USD", {})
                            mcap = usd.get("market_cap")
                            if sym and mcap:
                                mcap_map[sym.upper()] = float(mcap)
                        
                        if mcap_map:
                            self._mcap_cache = mcap_map
                            self._mcap_cache_time = now
                            # Persist to disk
                            now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                            with open(MCAP_CACHE_FILE, "w", encoding="utf-8") as f:
                                json.dump({
                                    "timestamp": now,
                                    "updated_at": now_utc,
                                    "count": len(mcap_map),
                                    "data": mcap_map
                                }, f, indent=2)
                            logger.info(f"Successfully updated Market Cap data ({len(mcap_map)} coins). Cached for the next 4 hours.")
                            return mcap_map
        except Exception as e:
            logger.warning(f"Could not fetch fresh market caps: {e}. Falling back to existing cache.")

        if self._mcap_cache:
            return self._mcap_cache
        return {}

    async def get_filtered_pairs(self) -> List[Dict[str, Any]]:
        """
        Fetch active pairs applying:
        1. 24h Volume >= config.MIN_24H_VOLUME_USDT (Default: > $1M USDT)
        2. Market Cap <= config.MAX_MARKET_CAP_USDT (Default: <= $500M USDT)
        """
        # 1. Exchange info
        url_ex = f"{self.base_url}/fapi/v1/exchangeInfo"
        async with self._session.get(url_ex) as r:
            ex_data = await r.json()
            symbols = [
                s for s in ex_data.get("symbols", [])
                if s.get("status") == "TRADING"
                and s.get("contractType") == "PERPETUAL"
                and s.get("quoteAsset") == "USDT"
            ]

        # 2. 24h Ticker Volumes
        url_tickers = f"{self.base_url}/fapi/v1/ticker/24hr"
        async with self._session.get(url_tickers) as r:
            tickers_data = await r.json()
            tickers_map = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in tickers_data}

        # 3. Market Caps
        mcap_map = await self.get_market_caps()

        def clean_base_symbol(base: str) -> str:
            b = base.upper()
            for prefix in ["1000000", "1000", "100"]:
                if b.startswith(prefix) and len(b) > len(prefix):
                    cand = b[len(prefix):]
                    if cand in mcap_map:
                        return cand
            return b

        filtered = []
        for s in symbols:
            sym = s["symbol"]
            base = s["baseAsset"]
            vol = tickers_map.get(sym, 0.0)

            # Volume filter
            if vol < config.MIN_24H_VOLUME_USDT:
                continue

            # Market cap filter
            clean_sym = clean_base_symbol(base)
            mcap = mcap_map.get(clean_sym)
            if mcap is not None and mcap > config.MAX_MARKET_CAP_USDT:
                continue

            filtered.append({
                "symbol": sym,
                "base_asset": base,
                "vol_24h": vol,
                "mcap": mcap
            })

        logger.info(f"Filtered {len(filtered)} / {len(symbols)} pairs (Vol > ${config.MIN_24H_VOLUME_USDT/1e6:.1f}M, MCap <= ${config.MAX_MARKET_CAP_USDT/1e6:.0f}M).")
        return filtered

    async def get_all_tickers(self) -> Dict[str, float]:
        """Fetch current price ticker for all Binance Futures pairs."""
        url = f"{self.base_url}/fapi/v1/ticker/price"
        try:
            async with self.semaphore:
                async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {t["symbol"]: float(t["price"]) for t in data}
        except Exception as e:
            logger.error(f"Error fetching ticker prices: {e}")
        return {}

    async def get_klines(self, symbol: str, interval: str, limit: int = 500) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch candlestick (kline) data for a given symbol and timeframe interval.
        Binance kline index mapping:
        0: Open time
        1: Open
        2: High
        3: Low
        4: Close
        5: Volume
        6: Close time
        7: Quote asset volume
        8: Number of trades
        9: Taker buy base asset volume
        10: Taker buy quote asset volume
        11: Ignore
        """
        url = f"{self.base_url}/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        try:
            async with self.semaphore:
                async with self._session.get(url, params=params) as response:
                    if response.status == 429:
                        logger.warning(f"Rate limited (429) while requesting {symbol}. Sleeping 5s...")
                        await asyncio.sleep(5)
                        return None
                    if response.status != 200:
                        return None
                    raw_klines = await response.json()
                    if not isinstance(raw_klines, list) or len(raw_klines) == 0:
                        return None
                    
                    parsed = []
                    for k in raw_klines:
                        parsed.append({
                            "open_time": int(k[0]),
                            "open": float(k[1]),
                            "high": float(k[2]),
                            "low": float(k[3]),
                            "close": float(k[4]),
                            "volume": float(k[5]),
                            "close_time": int(k[6]),
                            "quote_volume": float(k[7]),
                            "trades": int(k[8])
                        })
                    return parsed
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.debug(f"Error fetching klines for {symbol} ({interval}): {e}")
            return None

