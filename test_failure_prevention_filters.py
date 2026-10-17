import asyncio
import aiohttp
import json
import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime, timezone

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

BASE_URL = "https://fapi.binance.com"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

def compute_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

async def test_failure_prevention():
    print("=================================================================")
    print(" 🔬 EXPERIMENT: TESTING ANTI-FAILURE FILTERS TO PREVENT SLs")
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

        # Fetch BTC 4H candles to check BTC trend correlation
        url_btc = f"{BASE_URL}/fapi/v1/klines?symbol=BTCUSDT&interval=4h&limit=450"
        async with session.get(url_btc, timeout=aiohttp.ClientTimeout(total=10)) as r:
            kl_btc = await r.json()
        closes_btc = pd.Series([float(k[4]) for k in kl_btc])
        ema50_btc = compute_ema(closes_btc, 50)
        df_btc = pd.DataFrame({
            "open_time": [k[0] for k in kl_btc],
            "close": closes_btc,
            "ema50": ema50_btc
        })

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

            # 1H klines for volume check
            url_1h = f"{BASE_URL}/fapi/v1/klines"
            params_1h = {"symbol": sym, "interval": "1h", "limit": 800}
            try:
                async with semaphore:
                    async with session.get(url_1h, params=params_1h) as resp:
                        if resp.status != 200: kl_1h = []
                        else: kl_1h = await resp.json()
            except:
                kl_1h = []

            signals = []
            for i in range(2, len(kl_4h) - 4):
                k = kl_4h[i]
                open_ts = k[0]
                if open_ts < cutoff_30d_ts: continue

                # 4H 50/200 cross
                if ema50_4h.iloc[i-1] <= ema200_4h.iloc[i-1] and ema50_4h.iloc[i] > ema200_4h.iloc[i]:
                    day_match = df_1d[df_1d["open_time"] <= open_ts]
                    if day_match.empty or day_match.iloc[-1]["close"] <= day_match.iloc[-1]["ema50"]:
                        continue

                    entry_p = float(k[4])
                    curr_ema50 = ema50_4h.iloc[i]
                    prev_ema50 = ema50_4h.iloc[i-1]
                    curr_ema200 = ema200_4h.iloc[i]

                    # 1. Slope of 50 EMA (Is 50 EMA pointing up?)
                    is_50_slope_up = curr_ema50 > prev_ema50

                    # 2. Overextension: distance of entry above 200 EMA
                    dist_above_200_pct = ((entry_p - curr_ema200) / curr_ema200) * 100
                    is_not_overextended = dist_above_200_pct <= 8.0 # entry within 8% of 200 EMA

                    # 3. 1H Volume Filter
                    vol_pass = False
                    if kl_1h:
                        bars_up_to = [b for b in kl_1h if b[0] <= open_ts]
                        if len(bars_up_to) >= 24:
                            v_7 = sum([float(b[5]) for b in bars_up_to[-7:]]) / 7
                            v_24 = sum([float(b[5]) for b in bars_up_to[-24:]]) / 24
                            vol_pass = v_7 > v_24

                    # 4. BTC Macro Trend Filter
                    btc_match = df_btc[df_btc["open_time"] <= open_ts]
                    btc_bullish = False
                    if not btc_match.empty:
                        btc_bullish = btc_match.iloc[-1]["close"] > btc_match.iloc[-1]["ema50"]

                    future_bars = []
                    for f_idx in range(i+1, min(i+43, len(kl_4h))):
                        fb = kl_4h[f_idx]
                        future_bars.append({
                            "open": float(fb[1]),
                            "high": float(fb[2]),
                            "low": float(fb[3]),
                            "close": float(fb[4]),
                            "ema50": float(ema50_4h.iloc[f_idx]),
                            "ema200": float(ema200_4h.iloc[f_idx])
                        })
                    if len(future_bars) < 4: continue

                    signals.append({
                        "symbol": sym,
                        "open_time": open_ts,
                        "entry_price": entry_p,
                        "is_50_slope_up": is_50_slope_up,
                        "is_not_overextended": is_not_overextended,
                        "vol_pass": vol_pass,
                        "btc_bullish": btc_bullish,
                        "bars": future_bars
                    })
            return signals

        tasks = [fetch_pair_data(s) for s in filtered_pairs]
        results = await asyncio.gather(*tasks)
        all_signals = [s for sublist in results for s in sublist]
        print(f"Total Filtered Historical Signals Evaluated: {len(all_signals)}\n")

    # Trade Execution Simulator (Using Champion 3-Tier Model: SL 4H close < 200 EMA, TP1 +10%, TP2 +25%, 30% runner)
    def evaluate_pnl(sig):
        entry = sig["entry_price"]
        tp1_hit = False
        tp2_hit = False

        for idx, b in enumerate(sig["bars"]):
            high = b["high"]
            low = b["low"]
            close = b["close"]
            ema50 = b["ema50"]
            ema200 = b["ema200"]

            gain = ((high - entry) / entry) * 100
            loss = ((low - entry) / entry) * 100
            is_close_below_200 = close < ema200

            if not tp1_hit:
                if is_close_below_200:
                    exit_loss = ((close - entry) / entry) * 100
                    return exit_loss, False
                if gain >= 10.0:
                    tp1_hit = True
            elif tp1_hit and not tp2_hit:
                if loss <= 0.0 or is_close_below_200:
                    return 4.0, True # 40% at +10% + 60% at 0%
                if gain >= 25.0:
                    tp2_hit = True
            elif tp2_hit:
                if close < ema50 or loss <= 10.0 or idx >= len(sig["bars"]) - 1:
                    runner_ret = max(10.0, ((close - entry) / entry) * 100)
                    return 4.0 + 7.5 + (0.30 * runner_ret), True

        final_ret = ((sig["bars"][-1]["close"] - entry) / entry) * 100
        if tp1_hit:
            return 4.0 + (0.6 * max(0.0, final_ret)), True
        return final_ret, (final_ret > 0)

    # Let's test combinations of filters:
    test_cases = [
        ("Baseline (No Extra Anti-Failure Filters)", lambda s: True),
        ("Filter 1: Volume Filter Made HARD (1H 7h > 24h Vol)", lambda s: s["vol_pass"]),
        ("Filter 2: BTC Trend Filter (BTC 4H > 50 EMA)", lambda s: s["btc_bullish"]),
        ("Filter 3: Non-Overextended Only (Entry <= +8% from 200 EMA)", lambda s: s["is_not_overextended"]),
        ("Filter 4: 50 EMA Upward Slope Required", lambda s: s["is_50_slope_up"]),
        ("Combo A: Baseline + BTC Trend Filter", lambda s: s["btc_bullish"]),
        ("Combo B: Baseline + BTC Trend + Non-Overextended", lambda s: s["btc_bullish"] and s["is_not_overextended"]),
        ("Combo C: Volume HARD + BTC Trend + Non-Overextended (Ultra Strict)", lambda s: s["vol_pass"] and s["btc_bullish"] and s["is_not_overextended"]),
    ]

    print("=========================================================================================================================")
    print(" 📊 HEAD-TO-HEAD: DO ANTI-FAILURE FILTERS IMPROVE WIN RATE & TOTAL PROFIT?")
    print("=========================================================================================================================")
    print(f"{'Filter Strategy Tested':<48} | {'Trades':<7} | {'Win Rate':<9} | {'Avg PnL':<10} | {'Total Net Return':<17} | {'Profit Factor':<13}")
    print("-" * 125)

    comparison_data = []
    for name, cond in test_cases:
        subset = [s for s in all_signals if cond(s)]
        if not subset: continue
        sims = [evaluate_pnl(s) for s in subset]
        pnls = [r[0] for r in sims]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        wr = (len(wins) / len(pnls)) * 100
        avg_pnl = np.mean(pnls)
        tot_ret = np.sum(pnls)
        gross_win = np.sum(wins)
        gross_loss = abs(np.sum(losses)) if losses else 0.001
        pf = gross_win / gross_loss

        comparison_data.append({
            "name": name,
            "trades": len(subset),
            "win_rate": wr,
            "avg_pnl": avg_pnl,
            "tot_ret": tot_ret,
            "pf": pf
        })
        print(f"{name:<48} | {len(subset):>7} | {wr:>7.1f}% | {avg_pnl:>+8.2f}% | {tot_ret:>+15.1f}% | {pf:>11.2f}")

    print("=========================================================================================================================\n")

    # Save to json
    with open("anti_failure_filters_results.json", "w", encoding="utf-8") as f:
        json.dump(comparison_data, f, indent=2)

if __name__ == "__main__":
    asyncio.run(test_failure_prevention())

