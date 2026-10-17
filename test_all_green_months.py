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

from test_all_month_survival_rules import compute_ema, compute_rsi, compute_adx, BASE_URL, HEADERS

async def run_all_weather_test():
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

        # BTC 4H & 1D
        url_btc1 = f"{BASE_URL}/api/v3/klines?symbol=BTCUSDT&interval=4h&startTime={start_ts}&limit=800"
        async with session.get(url_btc1) as r:
            btc_p1 = await r.json()
        btc_last_ts = btc_p1[-1][0]
        url_btc2 = f"{BASE_URL}/api/v3/klines?symbol=BTCUSDT&interval=4h&startTime={btc_last_ts + 1}&limit=800"
        async with session.get(url_btc2) as r:
            btc_p2 = await r.json()
        kl_btc_4h = btc_p1 + btc_p2
        closes_btc_4h = pd.Series([float(k[4]) for k in kl_btc_4h])
        ema50_btc_4h = compute_ema(closes_btc_4h, 50)
        df_btc_4h = pd.DataFrame({"open_time": [k[0] for k in kl_btc_4h], "close": closes_btc_4h, "ema50": ema50_btc_4h})

        url_btc_1d = f"{BASE_URL}/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=300"
        async with session.get(url_btc_1d) as r:
            kl_btc_1d = await r.json()
        closes_btc_1d = pd.Series([float(k[4]) for k in kl_btc_1d])
        ema50_btc_1d = compute_ema(closes_btc_1d, 50)
        df_btc_1d = pd.DataFrame({"open_time": [k[0] for k in kl_btc_1d], "close": closes_btc_1d, "ema50": ema50_btc_1d})

        semaphore = asyncio.Semaphore(15)

        async def fetch_signals(sym):
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
            highs_4h = pd.Series([float(k[2]) for k in kl_4h])
            lows_4h = pd.Series([float(k[3]) for k in kl_4h])
            vols_4h = pd.Series([float(k[5]) for k in kl_4h])

            ema50_4h = compute_ema(closes_4h, 50)
            ema200_4h = compute_ema(closes_4h, 200)
            rsi_4h = compute_rsi(closes_4h, 14)
            adx_4h = compute_adx(highs_4h, lows_4h, closes_4h, 14)
            vol_sma20 = vols_4h.rolling(20).mean()

            signals = []
            for i in range(200, len(kl_4h) - 4):
                k = kl_4h[i]
                open_ts = k[0]
                if open_ts < cutoff_6m_ts: continue

                # 4H Golden Cross
                if ema50_4h.iloc[i-1] <= ema200_4h.iloc[i-1] and ema50_4h.iloc[i] > ema200_4h.iloc[i]:
                    day_match = df_1d[df_1d["open_time"] <= open_ts]
                    if day_match.empty or day_match.iloc[-1]["close"] <= day_match.iloc[-1]["ema50"]:
                        continue

                    entry_p = float(k[4])

                    # Dual BTC Filter
                    btc_4h_match = df_btc_4h[df_btc_4h["open_time"] <= open_ts]
                    btc_4h_bullish = False
                    if not btc_4h_match.empty:
                        btc_4h_bullish = float(btc_4h_match.iloc[-1]["close"]) > float(btc_4h_match.iloc[-1]["ema50"])

                    btc_1d_match = df_btc_1d[df_btc_1d["open_time"] <= open_ts]
                    btc_1d_bullish = False
                    if not btc_1d_match.empty:
                        btc_1d_bullish = float(btc_1d_match.iloc[-1]["close"]) > float(btc_1d_match.iloc[-1]["ema50"])

                    rsi_val = float(rsi_4h.iloc[i]) if not np.isnan(rsi_4h.iloc[i]) else 50.0
                    adx_val = float(adx_4h.iloc[i]) if not np.isnan(adx_4h.iloc[i]) else 20.0
                    vol_val = float(vols_4h.iloc[i])
                    vol_avg = float(vol_sma20.iloc[i]) if not np.isnan(vol_sma20.iloc[i]) else vol_val
                    volume_surge = vol_val > vol_avg

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

                    signals.append({
                        "symbol": sym,
                        "open_time": open_ts,
                        "entry_price": entry_p,
                        "btc_4h_bullish": btc_4h_bullish,
                        "btc_1d_bullish": btc_1d_bullish,
                        "rsi": rsi_val,
                        "adx": adx_val,
                        "volume_surge": volume_surge,
                        "bars": future_bars
                    })
            return signals

        tasks = [fetch_signals(s) for s in qualifying_pairs]
        results = await asyncio.gather(*tasks)
        all_signals = [s for sublist in results for s in sublist]

    # Portfolio simulation
    def run_sim(signals_list, tp1_gain=7.0, tp2_gain=20.0, max_concurrent=5):
        sorted_sigs = sorted(signals_list, key=lambda x: x["open_time"])
        wallet = 100.0
        min_eq = 100.0
        pos_notional = 50.0
        active_pos = []
        trade_hist = []
        monthly_res = {}

        all_ts = set()
        for s in sorted_sigs:
            all_ts.add(s["open_time"])
            for b in s["bars"]: all_ts.add(b["open_time"])
        timeline = sorted(list(all_ts))

        sigs_map = {}
        for s in sorted_sigs: sigs_map.setdefault(s["open_time"], []).append(s)

        for cur_ts in timeline:
            cur_month = datetime.fromtimestamp(cur_ts/1000, tz=timezone.utc).strftime("%Y-%m")
            monthly_res.setdefault(cur_month, 0.0)

            still_open = []
            worst_dip = 0.0

            for p in active_pos:
                bm = [b for b in p["bars"] if b["open_time"] == cur_ts]
                if not bm:
                    still_open.append(p)
                    continue
                b = bm[0]
                entry = p["entry_price"]
                gh = ((b["high"] - entry) / entry) * 100
                ll = ((b["low"] - entry) / entry) * 100
                cl = ((b["close"] - entry) / entry) * 100
                rem = pos_notional * p["size_remaining"]
                worst_dip += rem * (ll / 100.0)

                closed = False
                if not p["tp1_hit"]:
                    if b["close"] < b["ema200"] or ll <= -10.0:
                        exit_loss = cl if ll > -10.0 else -10.0
                        loss_d = pos_notional * (exit_loss / 100.0)
                        wallet += loss_d
                        monthly_res[cur_month] += loss_d
                        trade_hist.append({"win": False, "pnl": loss_d})
                        closed = True
                    elif gh >= tp1_gain:
                        tp1_d = (pos_notional * 0.40) * (tp1_gain / 100.0)
                        wallet += tp1_d
                        monthly_res[cur_month] += tp1_d
                        p["tp1_hit"] = True
                        p["size_remaining"] = 0.60
                        p["sl_be"] = True

                elif p["tp1_hit"] and not p["tp2_hit"]:
                    if ll <= 0.0 or b["close"] < b["ema200"]:
                        trade_hist.append({"win": True, "pnl": (pos_notional * 0.40) * (tp1_gain / 100.0)})
                        closed = True
                    elif gh >= tp2_gain:
                        tp2_d = (pos_notional * 0.30) * (tp2_gain / 100.0)
                        wallet += tp2_d
                        monthly_res[cur_month] += tp2_d
                        p["tp2_hit"] = True
                        p["size_remaining"] = 0.30

                elif p["tp2_hit"]:
                    if b["close"] < b["ema50"] or ll <= tp1_gain or b == p["bars"][-1]:
                        runner_gain = max(tp1_gain, cl)
                        run_d = (pos_notional * 0.30) * (runner_gain / 100.0)
                        wallet += run_d
                        monthly_res[cur_month] += run_d
                        trade_hist.append({"win": True, "pnl": 99.0})
                        closed = True

                if not closed: still_open.append(p)

            active_pos = still_open
            eq = wallet + worst_dip
            if eq < min_eq: min_eq = eq

            if cur_ts in sigs_map:
                for s in sigs_map[cur_ts]:
                    if len(active_pos) < max_concurrent:
                        active_pos.append({
                            "symbol": s["symbol"], "entry_price": s["entry_price"],
                            "bars": s["bars"], "tp1_hit": False, "tp2_hit": False,
                            "size_remaining": 1.0, "sl_be": False
                        })

        wins = [t for t in trade_hist if t["win"]]
        wr = (len(wins) / len(trade_hist) * 100) if trade_hist else 0.0
        return {
            "min_eq": round(min_eq, 2),
            "final_bal": round(wallet, 2),
            "roi": round(((wallet - 100)/100)*100, 1),
            "trades": len(trade_hist) + len(active_pos),
            "win_rate": round(wr, 1),
            "monthly": {k: round(v, 2) for k, v in monthly_res.items() if round(v, 2) != 0.0}
        }

    # All-Weather Filter: Dual BTC Bullish + Volume Surge + RSI >= 50
    filtered_all_weather = [
        s for s in all_signals 
        if s["btc_4h_bullish"] and s["btc_1d_bullish"] and s["volume_surge"] and s["rsi"] >= 50
    ]

    print("\n=========================================================================================")
    print(" 🌟 THE ALL-WEATHER BULLETPROOF CONFIGURATION")
    print(" Rules: Dual BTC Filter (1D & 4H) + Volume Surge + RSI >= 50 + Fast BE (TP1 +7%)")
    print("=========================================================================================")
    res = run_sim(filtered_all_weather, tp1_gain=7.0, tp2_gain=20.0, max_concurrent=5)
    print(f"Lowest Equity Ever: ${res['min_eq']} (Max Drawdown: -{round(100 - res['min_eq'], 1)}%)")
    print(f"Final Balance: ${res['final_bal']} (ROI: {res['roi']:>+5.1f}%)")
    print(f"Win Rate: {res['win_rate']}% across {res['trades']} trades")
    print("\nMonth-by-Month Profit Breakdown:")
    for m, pnl in res["monthly"].items():
        status_icon = "🟢 PROFIT" if pnl > 0 else ("⚪ BREAKEVEN" if pnl >= -1.0 else "🔴 LOSS")
        print(f"  Month {m}: ${pnl:>+6.2f} {status_icon}")
    print("=========================================================================================\n")

if __name__ == "__main__":
    asyncio.run(run_all_weather_test())

