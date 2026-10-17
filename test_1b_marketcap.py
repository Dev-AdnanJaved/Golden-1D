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

async def compare_500m_vs_1b():
    print("=================================================================")
    print(" 🔬 COMPARISON: Market Cap <= 500M vs Market Cap <= 1B ($1,000,000,000)")
    print("=================================================================\n")

    connector = aiohttp.TCPConnector(limit=50, ssl=False)
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        # Load CMC market caps
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

        pairs_500m = []
        pairs_1b = []
        newly_added_500m_to_1b = []

        for s in symbols:
            if s["status"] != "TRADING" or s["contractType"] != "PERPETUAL" or s["quoteAsset"] != "USDT": continue
            sym = s["symbol"]
            vol = tickers.get(sym, 0)
            mcap = mcap_map.get(clean_sym(s["baseAsset"]))

            if vol < 1_000_000: continue

            # Check 500M
            if mcap is None or mcap <= 500_000_000:
                pairs_500m.append((sym, vol, mcap))

            # Check 1B
            if mcap is None or mcap <= 1_000_000_000:
                pairs_1b.append((sym, vol, mcap))
                if mcap is not None and mcap > 500_000_000:
                    newly_added_500m_to_1b.append((sym, vol, mcap))

        print(f"Qualifying Pairs under <= 500M: {len(pairs_500m)}")
        print(f"Qualifying Pairs under <= 1B:   {len(pairs_1b)} (+{len(pairs_1b) - len(pairs_500m)} extra pairs)")
        print(f"Sample pairs added in the 500M - 1B zone:")
        for sym, vol, mc in newly_added_500m_to_1b[:10]:
            print(f"  • #{sym}: MCap = ${mc/1e6:.1f}M, 24h Vol = ${vol/1e6:.1f}M")
        print()

        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        cutoff_30d_ts = now_ts - (30 * 24 * 3600 * 1000)
        semaphore = asyncio.Semaphore(25)

        # Backtest across all pairs up to 1B
        async def fetch_pair_data(sym_info):
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
                        "mcap": sym_info[2],
                        "vol24": sym_info[1],
                        "bars": future_bars
                    })
            return signals

        tasks = [fetch_pair_data(s) for s in pairs_1b]
        results = await asyncio.gather(*tasks)
        all_signals_1b = [s for sublist in results for s in sublist]

    # Evaluate Trade Returns using Champion Strategy (TP1 +10%, TP2 +25%, 30% runner, SL 4H close < 200 EMA)
    def evaluate_champion(sig):
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

    # Filter signals for 500M vs 1B
    sigs_500m = [s for s in all_signals_1b if s["mcap"] is None or s["mcap"] <= 500_000_000]
    sigs_added = [s for s in all_signals_1b if s["mcap"] is not None and s["mcap"] > 500_000_000 and s["mcap"] <= 1_000_000_000]

    def calc_stats(sigs):
        if not sigs: return 0, 0, 0, 0, 0
        sims = [evaluate_champion(s) for s in sigs]
        pnls = [r[0] for r in sims]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        wr = (len(wins) / len(pnls)) * 100
        avg_ret = np.mean(pnls)
        tot_ret = np.sum(pnls)
        gross_win = np.sum(wins)
        gross_loss = abs(np.sum(losses)) if losses else 0.001
        pf = gross_win / gross_loss
        return len(sigs), wr, avg_ret, tot_ret, pf

    cnt_500, wr_500, avg_500, tot_500, pf_500 = calc_stats(sigs_500m)
    cnt_1b, wr_1b, avg_1b, tot_1b, pf_1b = calc_stats(all_signals_1b)
    cnt_add, wr_add, avg_add, tot_add, pf_add = calc_stats(sigs_added)

    print("=====================================================================================================")
    print(" 📊 HEAD-TO-HEAD RESULTS: MARKET CAP <= 500M vs MARKET CAP <= 1B")
    print("=====================================================================================================")
    print(f"{'Market Cap Universe':<30} | {'Total Setups':<12} | {'Win Rate':<10} | {'Avg PnL/Trade':<15} | {'Total Return (30D)':<18} | {'Profit Factor':<12}")
    print("-" * 115)
    print(f"{'Option 1: Market Cap <= 500M':<30} | {cnt_500:>12} | {wr_500:>9.1f}% | {avg_500:>+14.2f}% | {tot_500:>+17.1f}% | {pf_500:>12.2f}")
    print(f"{'Option 2: Market Cap <= 1.0B':<30} | {cnt_1b:>12} | {wr_1b:>9.1f}% | {avg_1b:>+14.2f}% | {tot_1b:>+17.1f}% | {pf_1b:>12.2f}")
    print(f"{'Difference / Impact':<30} | {f'+{cnt_1b - cnt_500} setups':>12} | {f'{wr_1b - wr_500:+.1f}%':>10} | {f'{avg_1b - avg_500:+.2f}%':>15} | {f'{tot_1b - tot_500:+.1f}%':>17} | {f'{pf_1b - pf_500:+.2f}':>12}")
    print("-----------------------------------------------------------------------------------------------------")
    print(f"Performance of ONLY the new coins added ($500M to $1B): {cnt_add} setups | Win Rate: {wr_add:.1f}% | Avg PnL: {avg_add:+.2f}% | Total Return: {tot_add:+.1f}% | PF: {pf_add:.2f}")
    print("=====================================================================================================\n")

if __name__ == "__main__":
    asyncio.run(compare_500m_vs_1b())

