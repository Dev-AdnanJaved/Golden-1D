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

async def run_filtered_analysis():
    print("=================================================================")
    print(" 🎯 FILTERED SCAN & OPTIMAL ENTRY/EXIT STRATEGY BACKTEST")
    print(" Filters: 24h Vol > $1M USDT | Market Cap <= $500M USDT")
    print("=================================================================\n")

    connector = aiohttp.TCPConnector(limit=50, ssl=False)
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        # 1. Fetch Market Caps
        url_cmc = "https://www.binance.com/bapi/composite/v1/public/promo/cmc/cryptocurrency/listings/latest?limit=1000"
        async with session.get(url_cmc, timeout=aiohttp.ClientTimeout(total=10)) as r:
            cmc_data = (await r.json()).get("data", {}).get("body", {}).get("data", [])
        
        mcap_map = {}
        for item in cmc_data:
            sym = item.get("symbol")
            usd = item.get("quote", {}).get("USD", {})
            mcap = usd.get("market_cap")
            if sym and mcap:
                mcap_map[sym.upper()] = float(mcap)

        # 2. Fetch 24h Tickers
        url_tickers = f"{BASE_URL}/fapi/v1/ticker/24hr"
        async with session.get(url_tickers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            tickers = {t["symbol"]: float(t["quoteVolume"]) for t in await r.json()}

        # 3. Fetch Exchange Info
        url_ex = f"{BASE_URL}/fapi/v1/exchangeInfo"
        async with session.get(url_ex, timeout=aiohttp.ClientTimeout(total=10)) as r:
            ex_data = await r.json()
            symbols = ex_data["symbols"]

        def get_base_mcap(base):
            b = base.upper()
            for prefix in ["1000000", "1000", "100"]:
                if b.startswith(prefix):
                    clean = b[len(prefix):]
                    if clean in mcap_map: return mcap_map[clean]
            return mcap_map.get(b)

        filtered_pairs = []
        for s in symbols:
            if s["status"] != "TRADING" or s["contractType"] != "PERPETUAL" or s["quoteAsset"] != "USDT":
                continue
            sym = s["symbol"]
            base = s["baseAsset"]
            vol = tickers.get(sym, 0)
            mcap = get_base_mcap(base)

            # Rule 1: 24h Volume > 1M USDT
            if vol < 1_000_000:
                continue
            # Rule 2: Market Cap <= 500M USDT
            if mcap is not None and mcap > 500_000_000:
                continue
            filtered_pairs.append((sym, vol, mcap))

        print(f"Total USDT Futures pairs evaluated: {len(symbols)}")
        print(f"Pairs qualifying under filters (Vol > 1M & MCap <= 500M): {len(filtered_pairs)}\n")

        # Now scan all qualifying pairs over last 30 days
        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        cutoff_30d_ts = now_ts - (30 * 24 * 3600 * 1000)
        semaphore = asyncio.Semaphore(25)

        async def backtest_pair(sym_info):
            sym = sym_info[0]
            url = f"{BASE_URL}/fapi/v1/klines"
            params = {"symbol": sym, "interval": "4h", "limit": 450}
            try:
                async with semaphore:
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200: return []
                        kl_4h = await resp.json()
            except:
                return []

            if not kl_4h or len(kl_4h) < 220:
                return []

            closes_4h = pd.Series([float(k[4]) for k in kl_4h])
            ema50_4h = compute_ema(closes_4h, 50)
            ema200_4h = compute_ema(closes_4h, 200)

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
            df_1d = pd.DataFrame({
                "open_time": [k[0] for k in kl_1d],
                "close": closes_1d,
                "ema50": ema50_1d
            })

            signals = []
            for i in range(1, len(kl_4h) - 4):
                k = kl_4h[i]
                open_ts = k[0]
                if open_ts < cutoff_30d_ts:
                    continue

                # 4H Golden Cross
                if ema50_4h.iloc[i-1] <= ema200_4h.iloc[i-1] and ema50_4h.iloc[i] > ema200_4h.iloc[i]:
                    # 1D Filter check
                    day_match = df_1d[df_1d["open_time"] <= open_ts]
                    if day_match.empty:
                        continue
                    if day_match.iloc[-1]["close"] <= day_match.iloc[-1]["ema50"]:
                        continue

                    entry_price = float(k[4])
                    future_bars = kl_4h[i+1 : i+43] # next 7 days
                    if not future_bars:
                        continue

                    signals.append({
                        "symbol": sym,
                        "open_time": open_ts,
                        "entry_price": entry_price,
                        "future_bars": future_bars,
                        "mcap": sym_info[2],
                        "vol24": sym_info[1]
                    })
            return signals

        tasks = [backtest_pair(p) for p in filtered_pairs]
        results = await asyncio.gather(*tasks)
        all_signals = [s for sublist in results for s in sublist]
        print(f"Total Setups Formed on Filtered Pairs (Last 30 Days): {len(all_signals)}\n")

    # Now let's simulate and compare 4 distinct Trading Strategies:
    # -------------------------------------------------------------
    # Strategy 1: Fixed 1:1.5 R:R (SL -7%, TP +10.5%)
    # Strategy 2: Fixed 1:2 R:R (SL -7%, TP +14%)
    # Strategy 3: Multi-Stage Institutional Scale-Out (The "Free Ride" Strategy):
    #             - Initial SL = -7.0%
    #             - TP1 (+8.0%): Close 50% position, move SL to BREAK-EVEN (0%).
    #             - TP2 (+20.0%): Close 25% position, trail SL to +8.0%.
    #             - Runner (remaining 25%): Exit when 4H candle closes below 4H 50 EMA (or at bar 42).
    # Strategy 4: Trailing Stop Strategy (SL -7%, once +10% reached trail 7% from peak).

    def sim_strat_1(sig):
        entry = sig["entry_price"]
        sl = -7.0
        tp = 10.5
        for b in sig["future_bars"]:
            gain = ((float(b[2]) - entry) / entry) * 100
            loss = ((float(b[3]) - entry) / entry) * 100
            if gain >= tp and loss > sl: return tp, "WIN"
            if loss <= sl and gain < tp: return sl, "LOSS"
            if gain >= tp and loss <= sl:
                return (tp, "WIN") if float(b[4]) >= float(b[1]) else (sl, "LOSS")
        # Exit at end
        final_ret = ((float(sig["future_bars"][-1][4]) - entry) / entry) * 100
        return final_ret, ("WIN" if final_ret > 0 else "LOSS")

    def sim_strat_2(sig):
        entry = sig["entry_price"]
        sl = -7.0
        tp = 14.0
        for b in sig["future_bars"]:
            gain = ((float(b[2]) - entry) / entry) * 100
            loss = ((float(b[3]) - entry) / entry) * 100
            if gain >= tp and loss > sl: return tp, "WIN"
            if loss <= sl and gain < tp: return sl, "LOSS"
            if gain >= tp and loss <= sl:
                return (tp, "WIN") if float(b[4]) >= float(b[1]) else (sl, "LOSS")
        final_ret = ((float(sig["future_bars"][-1][4]) - entry) / entry) * 100
        return final_ret, ("WIN" if final_ret > 0 else "LOSS")

    def sim_strat_3(sig):
        # Multi-stage Scale-Out Strategy
        entry = sig["entry_price"]
        cur_sl = -7.0
        tp1_hit = False
        tp2_hit = False
        total_pnl = 0.0

        for idx, b in enumerate(sig["future_bars"]):
            high = float(b[2])
            low = float(b[3])
            close = float(b[4])
            gain = ((high - entry) / entry) * 100
            loss = ((low - entry) / entry) * 100

            # Check Stop Loss first if not hit TP1
            if not tp1_hit:
                if loss <= cur_sl:
                    return cur_sl, "LOSS (SL Hit)"
                if gain >= 8.0:
                    tp1_hit = True
                    cur_sl = 0.0 # Move SL to Break-Even! Risk is now ZERO!

            # If TP1 was hit, check if stopped out at Break-Even
            if tp1_hit and not tp2_hit:
                if loss <= cur_sl:
                    # 50% at +8%, 50% at 0%
                    return 4.0, "WIN (TP1 Hit + BE)"
                if gain >= 20.0:
                    tp2_hit = True
                    cur_sl = 8.0 # Lock in +8% for remainder!

            # If TP2 hit, let remaining 25% run until peak or bar end
            if tp2_hit:
                if loss <= cur_sl or idx >= len(sig["future_bars"]) - 1:
                    runner_ret = max(cur_sl, ((close - entry) / entry) * 100)
                    # 50% at 8% (4.0) + 25% at 20% (5.0) + 25% at runner_ret
                    realized = 4.0 + 5.0 + (0.25 * runner_ret)
                    return realized, "BIG WIN (Full Scale-Out)"

        # If trade finishes 7 days without hitting TP1 or SL:
        final_ret = ((float(sig["future_bars"][-1][4]) - entry) / entry) * 100
        if tp1_hit:
            return 4.0 + (0.5 * final_ret), "WIN (TP1 Hit)"
        return final_ret, ("WIN" if final_ret > 0 else "LOSS")

    def sim_strat_4(sig):
        # Dynamic Trailing Stop (Trailing 7% from peak after +10%)
        entry = sig["entry_price"]
        peak = 0.0
        sl = -7.0
        for b in sig["future_bars"]:
            gain = ((float(b[2]) - entry) / entry) * 100
            loss = ((float(b[3]) - entry) / entry) * 100
            if gain > peak:
                peak = gain
                if peak >= 10.0:
                    sl = peak - 7.0 # Trail 7% below peak!
            if loss <= sl:
                return sl, ("WIN" if sl > 0 else "LOSS")
        final_ret = ((float(sig["future_bars"][-1][4]) - entry) / entry) * 100
        return final_ret, ("WIN" if final_ret > 0 else "LOSS")

    strats = [
        ("Strategy 1: Fixed 1:1.5 R:R (SL -7%, TP +10.5%)", sim_strat_1),
        ("Strategy 2: Fixed 1:2 R:R (SL -7%, TP +14.0%)", sim_strat_2),
        ("Strategy 3: Institutional 3-Tier Scale-Out (TP1 +8%, BE, TP2 +20%, Runner)", sim_strat_3),
        ("Strategy 4: Trailing Stop (Trail 7% after +10% gain)", sim_strat_4),
    ]

    print("=================================================================")
    print(" 📊 STRATEGY COMPARISON ON FILTERED MID/LOW-CAP PAIRS")
    print("=================================================================")
    print(f"{'Strategy Name':<45} | {'Win Rate':<10} | {'Avg Return/Trade':<18} | {'Total Net Return':<16} | {'Profit Factor':<12}")
    print("-" * 115)

    comparison_results = []
    for name, fn in strats:
        pnls = [fn(s)[0] for s in all_signals]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        wr = (len(wins) / len(pnls)) * 100 if pnls else 0
        avg_ret = sum(pnls) / len(pnls) if pnls else 0
        tot_net = sum(pnls)
        gross_win = sum(wins)
        gross_loss = abs(sum(losses)) if losses else 0.001
        pf = gross_win / gross_loss

        comparison_results.append({
            "name": name,
            "win_rate": wr,
            "avg_ret": avg_ret,
            "tot_net": tot_net,
            "pf": pf,
            "pnls": pnls
        })
        print(f"{name:<45} | {wr:>8.1f}% | {avg_ret:>+16.2f}% | {tot_net:>+14.1f}% | {pf:>10.2f}")

    print("=================================================================\n")

    # Save detailed comparison
    with open("strategy_comparison.json", "w", encoding="utf-8") as f:
        json.dump([{
            "name": r["name"],
            "win_rate": round(r["win_rate"], 2),
            "avg_return_per_trade": round(r["avg_ret"], 2),
            "total_net_return": round(r["tot_net"], 2),
            "profit_factor": round(r["pf"], 2)
        } for r in comparison_results], f, indent=2)

if __name__ == "__main__":
    asyncio.run(run_filtered_analysis())

