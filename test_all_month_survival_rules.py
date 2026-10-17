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

BASE_URL = "https://api.binance.com"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

def compute_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def compute_adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()

    up_move = high - high.shift(1)
    down_move = low.shift(1) - low

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    plus_di = 100 * (pd.Series(plus_dm).rolling(period).mean() / (atr + 1e-9))
    minus_di = 100 * (pd.Series(minus_dm).rolling(period).mean() / (atr + 1e-9))
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9))
    return dx.rolling(period).mean()

async def run_optimizer():
    connector = aiohttp.TCPConnector(limit=25, ssl=False)
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        # CMC Market Caps
        url_cmc = "https://www.binance.com/bapi/composite/v1/public/promo/cmc/cryptocurrency/listings/latest?limit=1000"
        async with session.get(url_cmc, timeout=aiohttp.ClientTimeout(total=10)) as r:
            cmc_data = (await r.json()).get("data", {}).get("body", {}).get("data", [])
        mcap_map = {item.get("symbol").upper(): float(item.get("quote", {}).get("USD", {}).get("market_cap"))
                    for item in cmc_data if item.get("symbol") and item.get("quote", {}).get("USD", {}).get("market_cap")}

        # 24h Tickers
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
                    # 1D > 50 EMA
                    day_match = df_1d[df_1d["open_time"] <= open_ts]
                    if day_match.empty or day_match.iloc[-1]["close"] <= day_match.iloc[-1]["ema50"]:
                        continue

                    entry_p = float(k[4])

                    # BTC Filters
                    btc_4h_match = df_btc_4h[df_btc_4h["open_time"] <= open_ts]
                    btc_4h_bullish = False
                    if not btc_4h_match.empty:
                        btc_4h_bullish = float(btc_4h_match.iloc[-1]["close"]) > float(btc_4h_match.iloc[-1]["ema50"])

                    btc_1d_match = df_btc_1d[df_btc_1d["open_time"] <= open_ts]
                    btc_1d_bullish = False
                    if not btc_1d_match.empty:
                        btc_1d_bullish = float(btc_1d_match.iloc[-1]["close"]) > float(btc_1d_match.iloc[-1]["ema50"])

                    # Extra Filters
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
                        "month": datetime.fromtimestamp(open_ts/1000, tz=timezone.utc).strftime("%Y-%m"),
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

    print(f"Total raw setups: {len(all_signals)}")

    # Function to simulate a strategy configuration
    def test_configuration(name, signals_list, tp1_gain=10.0, tp2_gain=25.0, max_concurrent=5):
        sorted_sigs = sorted(signals_list, key=lambda x: x["open_time"])
        wallet_balance = 100.0
        min_equity = 100.0
        position_notional = 50.0
        liquidated = False
        trade_history = []
        active_positions = []

        all_ts_set = set()
        for s in sorted_sigs:
            all_ts_set.add(s["open_time"])
            for b in s["bars"]: all_ts_set.add(b["open_time"])
        timeline = sorted(list(all_ts_set))

        sigs_by_time = {}
        for s in sorted_sigs:
            sigs_by_time.setdefault(s["open_time"], []).append(s)

        monthly_pnl = {}

        for current_ts in timeline:
            if liquidated: break
            still_open = []
            worst_dip_pnl = 0.0
            unrealized_pnl = 0.0

            m_str = datetime.fromtimestamp(current_ts/1000, tz=timezone.utc).strftime("%Y-%m")
            monthly_pnl.setdefault(m_str, 0.0)

            for pos in active_positions:
                bar_match = [b for b in pos["bars"] if b["open_time"] == current_ts]
                if not bar_match:
                    still_open.append(pos)
                    continue

                b = bar_match[0]
                entry = pos["entry_price"]
                gh = ((b["high"] - entry) / entry) * 100
                ll = ((b["low"] - entry) / entry) * 100
                cl = ((b["close"] - entry) / entry) * 100

                notional_rem = position_notional * pos["size_remaining"]
                worst_dip_pnl += notional_rem * (ll / 100.0)
                unrealized_pnl += notional_rem * (cl / 100.0)

                is_closed = False

                if not pos["tp1_hit"]:
                    if b["close"] < b["ema200"] or ll <= -12.0:
                        exit_loss_pct = cl if ll > -12.0 else -12.0
                        dollar_loss = position_notional * (exit_loss_pct / 100.0)
                        wallet_balance += dollar_loss
                        monthly_pnl[m_str] += dollar_loss
                        is_closed = True
                        trade_history.append({"win": False, "pnl": dollar_loss, "month": m_str})
                    elif gh >= tp1_gain:
                        realized_tp1 = (position_notional * 0.40) * (tp1_gain / 100.0)
                        wallet_balance += realized_tp1
                        monthly_pnl[m_str] += realized_tp1
                        pos["tp1_hit"] = True
                        pos["size_remaining"] = 0.60
                        pos["sl_be"] = True

                elif pos["tp1_hit"] and not pos["tp2_hit"]:
                    if ll <= 0.0 or b["close"] < b["ema200"]:
                        is_closed = True
                        trade_history.append({"win": True, "pnl": (position_notional * 0.40)*(tp1_gain/100), "month": m_str})
                    elif gh >= tp2_gain:
                        realized_tp2 = (position_notional * 0.30) * (tp2_gain / 100.0)
                        wallet_balance += realized_tp2
                        monthly_pnl[m_str] += realized_tp2
                        pos["tp2_hit"] = True
                        pos["size_remaining"] = 0.30

                elif pos["tp2_hit"]:
                    if b["close"] < b["ema50"] or ll <= (tp1_gain) or b == pos["bars"][-1]:
                        runner_gain = max(tp1_gain, cl)
                        realized_runner = (position_notional * 0.30) * (runner_gain / 100.0)
                        wallet_balance += realized_runner
                        monthly_pnl[m_str] += realized_runner
                        is_closed = True
                        trade_history.append({"win": True, "pnl": 99.0, "month": m_str})

                if not is_closed:
                    still_open.append(pos)

            active_positions = still_open

            current_eq = wallet_balance + worst_dip_pnl
            if current_eq < min_equity: min_equity = current_eq

            if current_eq <= (len(active_positions) * position_notional * 0.005) or current_eq <= 0:
                liquidated = True
                break

            if current_ts in sigs_by_time:
                for sig in sigs_by_time[current_ts]:
                    if len(active_positions) < max_concurrent:
                        active_positions.append({
                            "symbol": sig["symbol"],
                            "entry_price": sig["entry_price"],
                            "bars": sig["bars"],
                            "tp1_hit": False,
                            "tp2_hit": False,
                            "size_remaining": 1.0,
                            "sl_be": False
                        })

        wins = [t for t in trade_history if t["win"]]
        wr = (len(wins) / len(trade_history) * 100) if trade_history else 0.0
        return {
            "name": name,
            "liquidated": liquidated,
            "lowest_equity": round(min_equity, 2),
            "final_balance": round(wallet_balance if not liquidated else 0.0, 2),
            "total_trades": len(trade_history) + len(active_positions),
            "win_rate": round(wr, 1),
            "roi_pct": round(((wallet_balance - 100.0) / 100.0) * 100 if not liquidated else -100.0, 1),
            "monthly_pnl": {k: round(v, 2) for k, v in monthly_pnl.items() if round(v, 2) != 0.0}
        }

    # Test Configurations
    configs = [
        ("Base: 4H BTC Filter + TP1 10%", [s for s in all_signals if s["btc_4h_bullish"]], 10.0, 25.0, 5),
        ("Option 1: Dual BTC Filter (1D + 4H Bullish)", [s for s in all_signals if s["btc_4h_bullish"] and s["btc_1d_bullish"]], 10.0, 25.0, 5),
        ("Option 2: Dual BTC Filter + Volume Surge", [s for s in all_signals if s["btc_4h_bullish"] and s["btc_1d_bullish"] and s["volume_surge"]], 10.0, 25.0, 5),
        ("Option 3: Dual BTC + Quick Break-Even (TP1 +6%, TP2 +18%)", [s for s in all_signals if s["btc_4h_bullish"] and s["btc_1d_bullish"]], 6.0, 18.0, 5),
        ("Option 4: Dual BTC + RSI Momentum (>52) + ADX (>20)", [s for s in all_signals if s["btc_4h_bullish"] and s["btc_1d_bullish"] and s["rsi"] >= 52 and s["adx"] >= 20], 8.0, 20.0, 5),
    ]

    print("\n=============================================================================================================================")
    print(" 🧪 TESTING STRATEGIES TO SURVIVE ALL MONTHS & ACHIEVE POSITIVE EVERY-MONTH PROFITS")
    print("=============================================================================================================================")
    for title, sigs, tp1, tp2, mc in configs:
        res = test_configuration(title, sigs, tp1_gain=tp1, tp2_gain=tp2, max_concurrent=mc)
        liq_s = "LIQUIDATED" if res["liquidated"] else "SURVIVED ✅"
        print(f"\n🔹 {title}")
        print(f"   Status: {liq_s} | Lowest Equity: ${res['lowest_equity']} | Final Balance: ${res['final_balance']} ({res['roi_pct']:>+5.1f}%) | Win Rate: {res['win_rate']}% | Trades: {res['total_trades']}")
        print(f"   Monthly Net Profit Breakdown: {res['monthly_pnl']}")
    print("=============================================================================================================================\n")

if __name__ == "__main__":
    asyncio.run(run_optimizer())

