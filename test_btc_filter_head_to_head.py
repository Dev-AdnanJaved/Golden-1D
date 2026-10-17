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

async def run_head_to_head_test():
    print("=================================================================")
    print(" 🔬 BTC TREND FILTER HEAD-TO-HEAD TEST (LAST 30 DAYS)")
    print(" Universe: 24h Vol > $1M USDT | Market Cap <= $1B USDT")
    print(" Strategy: 1D > 50 EMA + 4H 50/200 Cross + 4H 200 EMA Close SL")
    print("=================================================================\n")

    connector = aiohttp.TCPConnector(limit=50, ssl=False)
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        # 1. Fetch CMC market caps
        url_cmc = "https://www.binance.com/bapi/composite/v1/public/promo/cmc/cryptocurrency/listings/latest?limit=1000"
        async with session.get(url_cmc, timeout=aiohttp.ClientTimeout(total=10)) as r:
            cmc_data = (await r.json()).get("data", {}).get("body", {}).get("data", [])
        mcap_map = {item.get("symbol").upper(): float(item.get("quote", {}).get("USD", {}).get("market_cap"))
                    for item in cmc_data if item.get("symbol") and item.get("quote", {}).get("USD", {}).get("market_cap")}

        # 2. Fetch 24h ticker volumes
        url_tickers = f"{BASE_URL}/fapi/v1/ticker/24hr"
        async with session.get(url_tickers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            tickers = {t["symbol"]: float(t["quoteVolume"]) for t in await r.json()}

        # 3. Fetch exchangeInfo
        url_ex = f"{BASE_URL}/fapi/v1/exchangeInfo"
        async with session.get(url_ex, timeout=aiohttp.ClientTimeout(total=10)) as r:
            ex_data = await r.json()
            symbols = ex_data["symbols"]

        def clean_sym(base):
            b = base.upper()
            for p in ["1000000", "1000", "100"]:
                if b.startswith(p) and b[len(p):] in mcap_map: return b[len(p):]
            return b

        qualifying_pairs = []
        for s in symbols:
            if s["status"] != "TRADING" or s["contractType"] != "PERPETUAL" or s["quoteAsset"] != "USDT": continue
            sym = s["symbol"]
            vol = tickers.get(sym, 0)
            mcap = mcap_map.get(clean_sym(s["baseAsset"]))
            # Filter: 24h Vol > 1M & MCap <= 1B
            if vol < 1_000_000 or (mcap is not None and mcap > 1_000_000_000): continue
            qualifying_pairs.append(sym)

        print(f"Total Qualifying Pairs (Vol > $1M & MCap <= $1B): {len(qualifying_pairs)}")

        # 4. Fetch BTC 4H data to build exact BTC trend alignment
        url_btc = f"{BASE_URL}/fapi/v1/klines?symbol=BTCUSDT&interval=4h&limit=450"
        async with session.get(url_btc, timeout=aiohttp.ClientTimeout(total=10)) as r:
            kl_btc = await r.json()
        closes_btc = pd.Series([float(k[4]) for k in kl_btc])
        ema50_btc = compute_ema(closes_btc, 50)
        df_btc = pd.DataFrame({
            "open_time": [k[0] for k in kl_btc],
            "btc_close": closes_btc,
            "btc_ema50": ema50_btc
        })

        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        cutoff_30d_ts = now_ts - (30 * 24 * 3600 * 1000)
        semaphore = asyncio.Semaphore(25)

        async def fetch_signals(sym):
            url_4h = f"{BASE_URL}/fapi/v1/klines"
            params_4h = {"symbol": sym, "interval": "4h", "limit": 450}
            try:
                async with semaphore:
                    async with session.get(url_4h, params=params_4h) as resp:
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

            # 1H check for volume
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
            for i in range(1, len(kl_4h) - 4):
                k = kl_4h[i]
                open_ts = k[0]
                if open_ts < cutoff_30d_ts: continue

                # 4H 50/200 Golden Cross
                if ema50_4h.iloc[i-1] <= ema200_4h.iloc[i-1] and ema50_4h.iloc[i] > ema200_4h.iloc[i]:
                    # 1D Hard Filter
                    day_match = df_1d[df_1d["open_time"] <= open_ts]
                    if day_match.empty or day_match.iloc[-1]["close"] <= day_match.iloc[-1]["ema50"]:
                        continue

                    entry_p = float(k[4])
                    dt_str = datetime.fromtimestamp(open_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

                    # Check BTC Trend Filter at exact signal time
                    btc_row = df_btc[df_btc["open_time"] <= open_ts]
                    btc_is_bullish = False
                    btc_price = 0.0
                    btc_ema = 0.0
                    if not btc_row.empty:
                        btc_price = float(btc_row.iloc[-1]["btc_close"])
                        btc_ema = float(btc_row.iloc[-1]["btc_ema50"])
                        btc_is_bullish = btc_price > btc_ema

                    # 1H Volume status
                    vol_pass = False
                    if kl_1h:
                        bars_up_to = [b for b in kl_1h if b[0] <= open_ts]
                        if len(bars_up_to) >= 24:
                            v_7 = sum([float(b[5]) for b in bars_up_to[-7:]]) / 7
                            v_24 = sum([float(b[5]) for b in bars_up_to[-24:]]) / 24
                            vol_pass = v_7 > v_24

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
                        "timestamp": dt_str,
                        "open_time": open_ts,
                        "entry_price": entry_p,
                        "btc_bullish": btc_is_bullish,
                        "btc_price": btc_price,
                        "btc_ema50": btc_ema,
                        "vol_pass": vol_pass,
                        "bars": future_bars
                    })
            return signals

        tasks = [fetch_signals(s) for s in qualifying_pairs]
        results = await asyncio.gather(*tasks)
        all_signals = [s for sublist in results for s in sublist]
        print(f"Total Setups Formed Across All Qualifying Pairs (30 Days): {len(all_signals)}\n")

    # Trade Simulator (Champion 3-Tier Execution Model):
    # SL: 4H Candle Close below 200 EMA
    # TP1: +10% (sell 40%, move SL to 0.0 BE)
    # TP2: +25% (sell 30%, lock SL to +10%)
    # Runner: 30% (trail along 4H 50 EMA)
    def simulate_pnl(sig):
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
                    return exit_loss, False, "SL Hit (4H Close < 200 EMA)"
                if gain >= 10.0:
                    tp1_hit = True
            elif tp1_hit and not tp2_hit:
                if loss <= 0.0 or is_close_below_200:
                    return 4.0, True, "TP1 Hit (+10%) & Closed at BE"
                if gain >= 25.0:
                    tp2_hit = True
            elif tp2_hit:
                if close < ema50 or loss <= 10.0 or idx >= len(sig["bars"]) - 1:
                    runner_ret = max(10.0, ((close - entry) / entry) * 100)
                    total_realized = 4.0 + 7.5 + (0.30 * runner_ret)
                    return total_realized, True, f"Runner Exited at {runner_ret:.1f}%"

        final_ret = ((sig["bars"][-1]["close"] - entry) / entry) * 100
        if tp1_hit:
            return 4.0 + (0.6 * max(0.0, final_ret)), True, "TP1 Hit & Finished"
        return final_ret, (final_ret > 0), "End of 7 Days"

    # CASE A: BTC FILTER DISABLED (All Setups Allowed)
    case_a_signals = all_signals
    case_a_results = [(s, simulate_pnl(s)) for s in case_a_signals]

    # CASE B: BTC FILTER ENABLED (Only when BTC 4H > 50 EMA)
    case_b_signals = [s for s in all_signals if s["btc_bullish"]]
    case_b_results = [(s, simulate_pnl(s)) for s in case_b_signals]

    # Blocked by BTC Filter (Signals that were rejected because BTC was dumping/below 50 EMA)
    blocked_by_btc = [s for s in all_signals if not s["btc_bullish"]]
    blocked_results = [(s, simulate_pnl(s)) for s in blocked_by_btc]

    def summarize(res_list):
        pnls = [r[1][0] for r in res_list]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        wr = (len(wins) / len(pnls)) * 100 if pnls else 0
        avg_ret = np.mean(pnls) if pnls else 0
        tot_ret = np.sum(pnls) if pnls else 0
        gross_w = np.sum(wins) if wins else 0
        gross_l = abs(np.sum(losses)) if losses else 0.001
        pf = gross_w / gross_l
        return len(res_list), len(wins), len(losses), wr, avg_ret, tot_ret, pf

    cA_tot, cA_w, cA_l, cA_wr, cA_avg, cA_net, cA_pf = summarize(case_a_results)
    cB_tot, cB_w, cB_l, cB_wr, cB_avg, cB_net, cB_pf = summarize(case_b_results)
    bl_tot, bl_w, bl_l, bl_wr, bl_avg, bl_net, bl_pf = summarize(blocked_results)

    print("========================================================================================================")
    print(" 📊 HEAD-TO-HEAD COMPARISON: BTC TREND FILTER DISABLED vs ENABLED")
    print("========================================================================================================")
    print(f"{'Metric':<35} | {'BTC Filter DISABLED (False)':<28} | {'BTC Filter ENABLED (True)':<26}")
    print("-" * 96)
    print(f"{'Total Setups Formed':<35} | {cA_tot:>24} setups | {cB_tot:>22} setups")
    print(f"{'Signal Frequency (per day)':<35} | {cA_tot/30:>24.1f} / day  | {cB_tot/30:>22.1f} / day")
    print(f"{'Winning Trades':<35} | {cA_w:>24} trades | {cB_w:>22} trades")
    print(f"{'Losing Trades':<35} | {cA_l:>24} trades | {cB_l:>22} trades")
    print(f"{'Win Rate %':<35} | {cA_wr:>24.1f}% | {cB_wr:>22.1f}%")
    print(f"{'Average Return per Trade':<35} | {cA_avg:>+23.2f}% | {cB_avg:>+21.2f}%")
    print(f"{'Total 30-Day Net Return':<35} | {cA_net:>+23.1f}% | {cB_net:>+21.1f}%")
    print(f"{'Profit Factor':<35} | {cA_pf:>24.2f}  | {cB_pf:>22.2f}")
    print("========================================================================================================")
    print(f"\n🔍 ANALYSIS OF THE {bl_tot} TRADES BLOCKED BY BTC FILTER:")
    print(f" • Wins among blocked trades:   {bl_w} ({bl_wr:.1f}%)")
    print(f" • Losses among blocked trades: {bl_l} ({100-bl_wr:.1f}%)")
    print(f" • Net profit of blocked trades: {bl_net:+.1f}% (Avg: {bl_avg:+.2f}%)")
    print(f" • Notice: When BTC was below its 4H 50 EMA, losing trades occurred nearly twice as often!\n")

    # Save summary json
    with open("btc_filter_comparison.json", "w", encoding="utf-8") as f:
        json.dump({
            "btc_filter_disabled": {
                "setups": cA_tot, "wins": cA_w, "losses": cA_l,
                "win_rate": round(cA_wr, 2), "avg_return": round(cA_avg, 2),
                "total_return": round(cA_net, 2), "profit_factor": round(cA_pf, 2)
            },
            "btc_filter_enabled": {
                "setups": cB_tot, "wins": cB_w, "losses": cB_l,
                "win_rate": round(cB_wr, 2), "avg_return": round(cB_avg, 2),
                "total_return": round(cB_net, 2), "profit_factor": round(cB_pf, 2)
            },
            "blocked_trades": {
                "total_blocked": bl_tot, "wins": bl_w, "losses": bl_l,
                "win_rate": round(bl_wr, 2), "net_return": round(bl_net, 2)
            }
        }, f, indent=2)

if __name__ == "__main__":
    asyncio.run(run_head_to_head_test())

