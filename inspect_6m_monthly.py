import asyncio
import aiohttp
import json
import os
import sys
import numpy as np
import pandas as pd
import time
from datetime import datetime, timezone

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from simulate_6m_spot_universe import compute_ema, BASE_URL, HEADERS

async def analyze_monthly():
    # Load the same qualifying pairs
    connector = aiohttp.TCPConnector(limit=25, ssl=False)
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        url_cmc = "https://www.binance.com/bapi/composite/v1/public/promo/cmc/cryptocurrency/listings/latest?limit=1000"
        async with session.get(url_cmc, timeout=aiohttp.ClientTimeout(total=10)) as r:
            cmc_data = (await r.json()).get("data", {}).get("body", {}).get("data", [])
        mcap_map = {item.get("symbol").upper(): float(item.get("quote", {}).get("USD", {}).get("market_cap"))
                    for item in cmc_data if item.get("symbol") and item.get("quote", {}).get("USD", {}).get("market_cap")}

        url_24h = f"{BASE_URL}/api/v3/ticker/24hr"
        async with session.get(url_24h, timeout=aiohttp.ClientTimeout(total=10)) as r:
            tickers_24h = await r.json()

        def clean_sym(base):
            b = base.upper()
            for p in ["1000000", "1000", "100"]:
                if b.startswith(p) and b[len(p):] in mcap_map: return b[len(p):]
            return b

        qualifying_pairs = []
        for t in tickers_24h:
            sym = t["symbol"]
            if not sym.endswith("USDT"): continue
            base = sym[:-4]
            if base in ["USDC", "FDUSD", "TUSD", "BUSD", "EUR", "DAI", "AEUR"]: continue
            if "UP" in base or "DOWN" in base or "BEAR" in base or "BULL" in base: continue
            vol = float(t.get("quoteVolume", 0))
            mcap = mcap_map.get(clean_sym(base))
            if vol >= 1_000_000 and (mcap is None or mcap <= 1_000_000_000):
                qualifying_pairs.append(sym)

        now_ts = int(time.time() * 1000)
        start_ts = now_ts - (240 * 24 * 3600 * 1000)
        cutoff_6m_ts = now_ts - (180 * 24 * 3600 * 1000)

        # BTC 4H benchmark
        url_btc1 = f"{BASE_URL}/api/v3/klines?symbol=BTCUSDT&interval=4h&startTime={start_ts}&limit=800"
        async with session.get(url_btc1) as r:
            btc_p1 = await r.json()
        btc_last_ts = btc_p1[-1][0]
        url_btc2 = f"{BASE_URL}/api/v3/klines?symbol=BTCUSDT&interval=4h&startTime={btc_last_ts + 1}&limit=800"
        async with session.get(url_btc2) as r:
            btc_p2 = await r.json()
        kl_btc = btc_p1 + btc_p2

        closes_btc = pd.Series([float(k[4]) for k in kl_btc])
        ema50_btc = compute_ema(closes_btc, 50)
        df_btc = pd.DataFrame({
            "open_time": [k[0] for k in kl_btc],
            "btc_close": closes_btc,
            "btc_ema50": ema50_btc
        })

        semaphore = asyncio.Semaphore(15)

        async def fetch_pair_signals(sym):
            try:
                async with semaphore:
                    await asyncio.sleep(0.03)
                    u1 = f"{BASE_URL}/api/v3/klines?symbol={sym}&interval=4h&startTime={start_ts}&limit=800"
                    async with session.get(u1, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        if resp.status != 200: return []
                        kl_4h_p1 = await resp.json()
                    if not kl_4h_p1 or len(kl_4h_p1) < 100: return []
                    l_ts = kl_4h_p1[-1][0]

                    u2 = f"{BASE_URL}/api/v3/klines?symbol={sym}&interval=4h&startTime={l_ts + 1}&limit=800"
                    async with session.get(u2, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        if resp.status != 200: return []
                        kl_4h_p2 = await resp.json()

                    kl_4h = kl_4h_p1 + kl_4h_p2
                    if len(kl_4h) < 250: return []

                    u_1d = f"{BASE_URL}/api/v3/klines?symbol={sym}&interval=1d&limit=300"
                    async with session.get(u_1d, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        if resp.status != 200: return []
                        kl_1d = await resp.json()
                    if len(kl_1d) < 60: return []

            except Exception:
                return []

            closes_1d = pd.Series([float(k[4]) for k in kl_1d])
            ema50_1d = compute_ema(closes_1d, 50)
            df_1d = pd.DataFrame({"open_time": [k[0] for k in kl_1d], "close": closes_1d, "ema50": ema50_1d})

            closes_4h = pd.Series([float(k[4]) for k in kl_4h])
            ema50_4h = compute_ema(closes_4h, 50)
            ema200_4h = compute_ema(closes_4h, 200)

            signals = []
            for i in range(200, len(kl_4h) - 4):
                k = kl_4h[i]
                open_ts = k[0]
                if open_ts < cutoff_6m_ts: continue

                if ema50_4h.iloc[i-1] <= ema200_4h.iloc[i-1] and ema50_4h.iloc[i] > ema200_4h.iloc[i]:
                    day_match = df_1d[df_1d["open_time"] <= open_ts]
                    if day_match.empty or day_match.iloc[-1]["close"] <= day_match.iloc[-1]["ema50"]:
                        continue

                    entry_p = float(k[4])
                    btc_row = df_btc[df_btc["open_time"] <= open_ts]
                    btc_bullish = False
                    if not btc_row.empty:
                        btc_bullish = float(btc_row.iloc[-1]["btc_close"]) > float(btc_row.iloc[-1]["btc_ema50"])

                    future_bars = []
                    for f_idx in range(i+1, min(i+61, len(kl_4h))):
                        fb = kl_4h[f_idx]
                        future_bars.append({
                            "open_time": fb[0],
                            "open": float(fb[1]),
                            "high": float(fb[2]),
                            "low": float(fb[3]),
                            "close": float(fb[4]),
                            "ema50": float(ema50_4h.iloc[f_idx]),
                            "ema200": float(ema200_4h.iloc[f_idx])
                        })
                    if len(future_bars) < 4: continue

                    # Individual trade outcome evaluation
                    tp1_hit = False
                    tp2_hit = False
                    win = False
                    pnl_pct = 0.0
                    for fb in future_bars:
                        gh = (fb["high"] - entry_p) / entry_p * 100
                        ll = (fb["low"] - entry_p) / entry_p * 100
                        cl = (fb["close"] - entry_p) / entry_p * 100
                        if not tp1_hit:
                            if fb["close"] < fb["ema200"] or ll <= -12.0:
                                pnl_pct = cl if ll > -12.0 else -12.0
                                break
                            elif gh >= 10.0:
                                tp1_hit = True
                                win = True
                        elif tp1_hit and not tp2_hit:
                            if ll <= 0.0 or fb["close"] < fb["ema200"]:
                                pnl_pct = 4.0 # 40% * 10%
                                break
                            elif gh >= 25.0:
                                tp2_hit = True
                        elif tp2_hit:
                            if fb["close"] < fb["ema50"] or ll <= 10.0 or fb == future_bars[-1]:
                                pnl_pct = 4.0 + 7.5 + (0.3 * max(10.0, cl))
                                break

                    signals.append({
                        "symbol": sym,
                        "open_time": open_ts,
                        "date_str": datetime.fromtimestamp(open_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d"),
                        "month_str": datetime.fromtimestamp(open_ts/1000, tz=timezone.utc).strftime("%Y-%m"),
                        "entry_price": entry_p,
                        "btc_bullish": btc_bullish,
                        "win": win,
                        "pnl_pct": pnl_pct
                    })
            return signals

        tasks = [fetch_pair_signals(s) for s in qualifying_pairs]
        results = await asyncio.gather(*tasks)
        all_signals = [s for sublist in results for s in sublist]

    df_sigs = pd.DataFrame(all_signals)
    print("\n=========================================================================================")
    print(" 📅 6-MONTH MONTH-BY-MONTH BREAKDOWN (Win Rate & Market Regimes)")
    print("=========================================================================================")
    for m, grp in df_sigs.groupby("month_str"):
        total = len(grp)
        wins = grp["win"].sum()
        wr = (wins / total) * 100
        avg_pnl = grp["pnl_pct"].mean()
        btc_bull = grp["btc_bullish"].sum()
        print(f"Month: {m} | Setups: {total:>3} | Wins: {wins:>2} | Win Rate: {wr:>5.1f}% | Avg PnL/Trade: {avg_pnl:>+5.1f}% | BTC Bullish Setups: {btc_bull}/{total}")

    print("=========================================================================================\n")

if __name__ == "__main__":
    asyncio.run(analyze_monthly())

