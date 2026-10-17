import asyncio
import aiohttp
import json
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timezone

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

BASE_URL = "https://fapi.binance.com"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

def compute_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

async def compare_sl_strategies():
    print("=================================================================")
    print(" 🔬 EXPERIMENT: Fixed -7% SL vs 4H Candle Close Below 200 EMA")
    print("=================================================================\n")

    connector = aiohttp.TCPConnector(limit=50, ssl=False)
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        # Load filtered pairs
        url_cmc = "https://www.binance.com/bapi/composite/v1/public/promo/cmc/cryptocurrency/listings/latest?limit=1000"
        async with session.get(url_cmc, timeout=aiohttp.ClientTimeout(total=10)) as r:
            cmc_data = (await r.json()).get("data", {}).get("body", {}).get("data", [])
        mcap_map = {item.get("symbol").upper(): float(item.get("quote", {}).get("USD", {}).get("market_cap"))
                    for item in cmc_data if item.get("symbol") and item.get("quote", {}).get("USD", {}).get("market_cap")}

        url_tickers = f"{BASE_URL}/fapi/v1/ticker/24hr"
        async with session.get(url_tickers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            tickers = {t["symbol"]: float(t["quoteVolume"]) for t in await r.json()}

        url_ex = f"{BASE_URL}/fapi/v1/exchangeInfo"
        async with session.get(url_ex, timeout=aiohttp.ClientTimeout(total=10)) as r:
            ex_data = await r.json()
            symbols = ex_data["symbols"]

        def clean_sym(base):
            b = base.upper()
            for p in ["1000000", "1000", "100"]:
                if b.startswith(p) and b[len(p):] in mcap_map: return b[len(p):]
            return b

        filtered_pairs = []
        for s in symbols:
            if s["status"] != "TRADING" or s["contractType"] != "PERPETUAL" or s["quoteAsset"] != "USDT": continue
            sym = s["symbol"]
            vol = tickers.get(sym, 0)
            mcap = mcap_map.get(clean_sym(s["baseAsset"]))
            if vol < 1_000_000 or (mcap is not None and mcap > 500_000_000): continue
            filtered_pairs.append(sym)

        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        cutoff_30d_ts = now_ts - (30 * 24 * 3600 * 1000)
        semaphore = asyncio.Semaphore(25)

        async def fetch_pair_data(sym):
            url = f"{BASE_URL}/fapi/v1/klines"
            params = {"symbol": sym, "interval": "4h", "limit": 450}
            try:
                async with semaphore:
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200: return []
                        kl_4h = await resp.json()
            except:
                return []
            if not kl_4h or len(kl_4h) < 220: return []

            # 1D check
            url_1d = f"{BASE_URL}/fapi/v1/klines"
            params_1d = {"symbol": sym, "interval": "1d", "limit": 120}
            try:
                async with semaphore:
                    async with session.get(url_1d, params=params_1d) as resp:
                        if resp.status != 200: return []
                        kl_1d = await resp.json()
            except:
                return []

            closes_1d = pd.Series([float(k[4]) for k in kl_1d])
            ema50_1d = compute_ema(closes_1d, 50)
            df_1d = pd.DataFrame({"open_time": [k[0] for k in kl_1d], "close": closes_1d, "ema50": ema50_1d})

            closes_4h = pd.Series([float(k[4]) for k in kl_4h])
            ema50_4h = compute_ema(closes_4h, 50)
            ema200_4h = compute_ema(closes_4h, 200)

            signals = []
            for i in range(1, len(kl_4h) - 4):
                k = kl_4h[i]
                open_ts = k[0]
                if open_ts < cutoff_30d_ts: continue

                if ema50_4h.iloc[i-1] <= ema200_4h.iloc[i-1] and ema50_4h.iloc[i] > ema200_4h.iloc[i]:
                    day_match = df_1d[df_1d["open_time"] <= open_ts]
                    if day_match.empty or day_match.iloc[-1]["close"] <= day_match.iloc[-1]["ema50"]:
                        continue

                    entry_p = float(k[4])
                    future_bars = []
                    # Keep track of candle OHLC and the 200 EMA at that bar
                    for f_idx in range(i+1, min(i+43, len(kl_4h))):
                        fb = kl_4h[f_idx]
                        future_bars.append({
                            "open": float(fb[1]),
                            "high": float(fb[2]),
                            "low": float(fb[3]),
                            "close": float(fb[4]),
                            "ema200": float(ema200_4h.iloc[f_idx])
                        })
                    if len(future_bars) < 4: continue
                    signals.append({"symbol": sym, "entry_price": entry_p, "bars": future_bars})
            return signals

        tasks = [fetch_pair_data(s) for s in filtered_pairs]
        results = await asyncio.gather(*tasks)
        all_signals = [s for sublist in results for s in sublist]
        print(f"Total Filtered Setups for Comparative Testing: {len(all_signals)}\n")

    # Now let's run the exact side-by-side comparison under the SAME profit targets (TP1 +8%, TP2 +20%, Runner):
    # Model 1: Stop Loss = Fixed -7.0% intra-candle wick
    # Model 2: Stop Loss = 4H Candle CLOSE below 200 EMA (ignore wicks!)
    # Model 3: Stop Loss = 4H Candle CLOSE below 200 EMA + Emergency Hard Stop at -12%

    def run_model_1(sig):
        # Fixed -7% SL
        entry = sig["entry_price"]
        sl = -7.0
        cur_sl = sl
        tp1_hit = False
        tp2_hit = False

        for idx, b in enumerate(sig["bars"]):
            gain = ((b["high"] - entry) / entry) * 100
            loss = ((b["low"] - entry) / entry) * 100

            if not tp1_hit:
                if loss <= cur_sl:
                    return cur_sl, "LOSS (Fixed -7% Wick Stop)"
                if gain >= 8.0:
                    tp1_hit = True
                    cur_sl = 0.0 # BE
            if tp1_hit and not tp2_hit:
                if loss <= cur_sl:
                    return 4.0, "WIN (TP1 + BE)"
                if gain >= 20.0:
                    tp2_hit = True
                    cur_sl = 8.0
            if tp2_hit:
                if loss <= cur_sl or idx >= len(sig["bars"]) - 1:
                    runner_ret = max(cur_sl, ((b["close"] - entry) / entry) * 100)
                    return 4.0 + 5.0 + (0.25 * runner_ret), "BIG WIN"

        final_ret = ((sig["bars"][-1]["close"] - entry) / entry) * 100
        return (4.0 + 0.5 * final_ret, "WIN") if tp1_hit else (final_ret, "WIN" if final_ret > 0 else "LOSS")

    def run_model_2(sig):
        # 4H Candle CLOSE Below 200 EMA
        entry = sig["entry_price"]
        tp1_hit = False
        tp2_hit = False
        cur_sl_pct = None

        for idx, b in enumerate(sig["bars"]):
            gain = ((b["high"] - entry) / entry) * 100
            close_p = b["close"]
            ema200 = b["ema200"]

            # SL Condition: 4H Candle CLOSE is strictly below 200 EMA!
            is_close_below_200 = close_p < ema200

            if not tp1_hit:
                if is_close_below_200:
                    exit_loss = ((close_p - entry) / entry) * 100
                    return exit_loss, "LOSS (4H Close < 200 EMA)"
                if gain >= 8.0:
                    tp1_hit = True
                    cur_sl_pct = 0.0 # BE
            elif tp1_hit and not tp2_hit:
                # Stop loss is now Break-Even
                loss = ((b["low"] - entry) / entry) * 100
                if loss <= 0.0 or is_close_below_200:
                    return 4.0, "WIN (TP1 + BE)"
                if gain >= 20.0:
                    tp2_hit = True
                    cur_sl_pct = 8.0
            elif tp2_hit:
                loss = ((b["low"] - entry) / entry) * 100
                if loss <= 8.0 or is_close_below_200 or idx >= len(sig["bars"]) - 1:
                    runner_ret = max(8.0, ((close_p - entry) / entry) * 100)
                    return 4.0 + 5.0 + (0.25 * runner_ret), "BIG WIN"

        final_ret = ((sig["bars"][-1]["close"] - entry) / entry) * 100
        return (4.0 + 0.5 * final_ret, "WIN") if tp1_hit else (final_ret, "WIN" if final_ret > 0 else "LOSS")

    def run_model_3(sig):
        # 4H Candle CLOSE Below 200 EMA WITH -12% Emergency Circuit Breaker
        entry = sig["entry_price"]
        tp1_hit = False
        tp2_hit = False

        for idx, b in enumerate(sig["bars"]):
            gain = ((b["high"] - entry) / entry) * 100
            loss = ((b["low"] - entry) / entry) * 100
            close_p = b["close"]
            ema200 = b["ema200"]

            # Emergency hard stop at -12% intra-candle
            if not tp1_hit and loss <= -12.0:
                return -12.0, "LOSS (Emergency Hard Stop)"

            # SL Condition: 4H Candle CLOSE below 200 EMA
            is_close_below_200 = close_p < ema200

            if not tp1_hit:
                if is_close_below_200:
                    exit_loss = ((close_p - entry) / entry) * 100
                    return exit_loss, "LOSS (4H Close < 200 EMA)"
                if gain >= 8.0:
                    tp1_hit = True
            elif tp1_hit and not tp2_hit:
                if loss <= 0.0 or is_close_below_200:
                    return 4.0, "WIN (TP1 + BE)"
                if gain >= 20.0:
                    tp2_hit = True
            elif tp2_hit:
                if loss <= 8.0 or is_close_below_200 or idx >= len(sig["bars"]) - 1:
                    runner_ret = max(8.0, ((close_p - entry) / entry) * 100)
                    return 4.0 + 5.0 + (0.25 * runner_ret), "BIG WIN"

        final_ret = ((sig["bars"][-1]["close"] - entry) / entry) * 100
        return (4.0 + 0.5 * final_ret, "WIN") if tp1_hit else (final_ret, "WIN" if final_ret > 0 else "LOSS")

    models = [
        ("Model 1: Fixed -7.0% Hard Stop (Wick Exit)", run_model_1),
        ("Model 2: 4H Candle Close Below 200 EMA (Ignore Wicks)", run_model_2),
        ("Model 3: 4H Candle Close < 200 EMA + Emergency -12% Cap", run_model_3)
    ]

    print("=================================================================")
    print(" 📊 HEAD-TO-HEAD RESULTS: DOES 4H 200 EMA CLOSE IMPROVE RETURN?")
    print("=================================================================")
    print(f"{'SL Model Name':<45} | {'Win Rate':<10} | {'Avg PnL/Trade':<15} | {'Total Return':<14} | {'Avg Loss on Losers':<18} | {'Saved Trades':<12}")
    print("-" * 125)

    m1_res = [run_model_1(s) for s in all_signals]
    
    for name, fn in models:
        res = [fn(s) for s in all_signals]
        pnls = [r[0] for r in res]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        wr = (len(wins) / len(pnls)) * 100
        avg_ret = sum(pnls) / len(pnls)
        tot_ret = sum(pnls)
        avg_loss = sum(losses) / len(losses) if losses else 0.0

        # Compare how many trades that were stopped out in Model 1 survived and went on to win in this model!
        saved = 0
        for i in range(len(all_signals)):
            if m1_res[i][0] <= 0 and pnls[i] > 0:
                saved += 1

        print(f"{name:<45} | {wr:>8.1f}% | {avg_ret:>+13.2f}% | {tot_ret:>+12.1f}% | {avg_loss:>16.2f}% | {saved:>10} trades")

    print("=================================================================\n")

if __name__ == "__main__":
    asyncio.run(compare_sl_strategies())

