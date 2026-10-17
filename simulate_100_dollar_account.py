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

async def run_simulation():
    print("=================================================================")
    print(" 💵 $100 CAPITAL SIMULATION (10x LEVERAGE, $5 MARGIN / $50 POSITION)")
    print(" Testing: Max 5 Trades vs Max 10 Trades | BTC Filter True vs False")
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

        qualifying_pairs = []
        for s in symbols:
            if s["status"] != "TRADING" or s["contractType"] != "PERPETUAL" or s["quoteAsset"] != "USDT": continue
            sym = s["symbol"]
            vol = tickers.get(sym, 0)
            mcap = mcap_map.get(clean_sym(s["baseAsset"]))
            if vol < 1_000_000 or (mcap is not None and mcap > 1_000_000_000): continue
            qualifying_pairs.append(sym)

        # BTC 4H data
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
                    btc_row = df_btc[df_btc["open_time"] <= open_ts]
                    btc_bullish = False
                    if not btc_row.empty:
                        btc_bullish = float(btc_row.iloc[-1]["btc_close"]) > float(btc_row.iloc[-1]["btc_ema50"])

                    future_bars = []
                    for f_idx in range(i+1, min(i+43, len(kl_4h))):
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
                        "btc_bullish": btc_bullish,
                        "bars": future_bars
                    })
            return signals

        tasks = [fetch_signals(s) for s in qualifying_pairs]
        results = await asyncio.gather(*tasks)
        all_signals = [s for sublist in results for s in sublist]
        print(f"Loaded {len(all_signals)} historical signals for portfolio simulation.\n")

    # Chronological timeline simulation of cross-margin wallet
    def run_portfolio_sim(signals_list, max_concurrent_trades=5, initial_balance=100.0, margin_per_trade=5.0, leverage=10.0):
        # Sort all signals strictly by trigger time
        sorted_sigs = sorted(signals_list, key=lambda x: x["open_time"])
        position_notional = margin_per_trade * leverage # $5 * 10 = $50 position

        wallet_balance = initial_balance
        min_equity_ever = initial_balance
        liquidated = False
        liquidation_time = None

        active_positions = [] # List of open position dicts
        trade_history = []

        # Gather all distinct 4H timestamps across the timeline
        all_ts_set = set()
        for s in sorted_sigs:
            all_ts_set.add(s["open_time"])
            for b in s["bars"]:
                all_ts_set.add(b["open_time"])
        timeline = sorted(list(all_ts_set))

        # Map open_time -> signals triggered at this time
        sigs_by_time = {}
        for s in sorted_sigs:
            sigs_by_time.setdefault(s["open_time"], []).append(s)

        for current_ts in timeline:
            if liquidated:
                break

            # 1. Update existing open positions on this bar
            still_open_positions = []
            total_unrealized_pnl = 0.0
            worst_case_candle_pnl = 0.0

            for pos in active_positions:
                # Find the candle for this position at current_ts
                bar_match = [b for b in pos["bars"] if b["open_time"] == current_ts]
                if not bar_match:
                    # Trade has ended or no bar
                    still_open_positions.append(pos)
                    continue

                b = bar_match[0]
                entry = pos["entry_price"]
                high = b["high"]
                low = b["low"]
                close = b["close"]
                ema50 = b["ema50"]
                ema200 = b["ema200"]

                gain_high = ((high - entry) / entry) * 100
                loss_low = ((low - entry) / entry) * 100
                close_gain = ((close - entry) / entry) * 100

                # Track unrealized PnL at close and lowest point
                notional_remaining = position_notional * pos["size_remaining"]
                unrealized_close = notional_remaining * (close_gain / 100.0)
                unrealized_dip = notional_remaining * (loss_low / 100.0)

                total_unrealized_pnl += unrealized_close
                worst_case_candle_pnl += unrealized_dip

                # In-Trade Management: Stop Loss & Scaling
                is_closed = False

                # Check Stop Loss: 4H Candle Close below 200 EMA
                if not pos["tp1_hit"]:
                    if close < ema200 or loss_low <= -12.0:
                        exit_loss_pct = close_gain if loss_low > -12.0 else -12.0
                        dollar_loss = position_notional * (exit_loss_pct / 100.0)
                        wallet_balance += dollar_loss
                        is_closed = True
                        trade_history.append({"symbol": pos["symbol"], "pnl": dollar_loss, "reason": "SL Hit"})
                    elif gain_high >= 10.0:
                        # TP1 Hit: Sell 40%, move SL to BE!
                        realized_tp1 = (position_notional * 0.40) * 0.10 # +$2.00
                        wallet_balance += realized_tp1
                        pos["tp1_hit"] = True
                        pos["size_remaining"] = 0.60
                        pos["sl_be"] = True

                elif pos["tp1_hit"] and not pos["tp2_hit"]:
                    if loss_low <= 0.0 or close < ema200:
                        # Stopped out at Breakeven
                        is_closed = True
                        trade_history.append({"symbol": pos["symbol"], "pnl": 2.0, "reason": "TP1 + BE"})
                    elif gain_high >= 25.0:
                        # TP2 Hit: Sell 30%, lock +10% on runner!
                        realized_tp2 = (position_notional * 0.30) * 0.25 # +$3.75
                        wallet_balance += realized_tp2
                        pos["tp2_hit"] = True
                        pos["size_remaining"] = 0.30

                elif pos["tp2_hit"]:
                    if close < ema50 or loss_low <= 10.0 or b == pos["bars"][-1]:
                        runner_gain = max(10.0, close_gain)
                        realized_runner = (position_notional * 0.30) * (runner_gain / 100.0)
                        wallet_balance += realized_runner
                        is_closed = True
                        trade_history.append({"symbol": pos["symbol"], "pnl": 2.0 + 3.75 + realized_runner, "reason": "Runner Exit"})

                if not is_closed:
                    still_open_positions.append(pos)

            active_positions = still_open_positions

            # 2. Check Liquidation Condition during this candle
            # Cross margin equity = wallet_balance + total_unrealized_pnl
            current_equity = wallet_balance + total_unrealized_pnl
            intra_candle_min_equity = wallet_balance + worst_case_candle_pnl

            if intra_candle_min_equity < min_equity_ever:
                min_equity_ever = intra_candle_min_equity

            # Maintenance margin on Binance for $50 position is 0.5% ($0.25/trade)
            maint_margin_req = len(active_positions) * position_notional * 0.005
            if intra_candle_min_equity <= maint_margin_req or intra_candle_min_equity <= 0:
                liquidated = True
                liquidation_time = datetime.fromtimestamp(current_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                break

            # 3. Open new signals triggered at this timestamp (if slots available)
            if current_ts in sigs_by_time:
                for sig in sigs_by_time[current_ts]:
                    if len(active_positions) < max_concurrent_trades:
                        active_positions.append({
                            "symbol": sig["symbol"],
                            "entry_price": sig["entry_price"],
                            "bars": sig["bars"],
                            "tp1_hit": False,
                            "tp2_hit": False,
                            "size_remaining": 1.0,
                            "sl_be": False
                        })

        final_equity = wallet_balance + sum(position_notional * p["size_remaining"] * (((p["bars"][-1]["close"] - p["entry_price"])/p["entry_price"])) for p in active_positions)

        return {
            "initial_balance": initial_balance,
            "final_balance": round(wallet_balance if not liquidated else 0.0, 2),
            "final_equity": round(final_equity if not liquidated else 0.0, 2),
            "min_equity_ever": round(min_equity_ever, 2),
            "liquidated": liquidated,
            "liquidation_time": liquidation_time,
            "total_trades_taken": len(trade_history) + len(active_positions),
            "total_profit_dollar": round((wallet_balance - initial_balance) if not liquidated else -initial_balance, 2),
            "roi_pct": round(((wallet_balance - initial_balance)/initial_balance)*100 if not liquidated else -100.0, 1)
        }

    # Run the 4 scenarios
    scenarios = [
        ("Scenario 1: Max 5 Trades | BTC Filter DISABLED", all_signals, 5),
        ("Scenario 2: Max 5 Trades | BTC Filter ENABLED", [s for s in all_signals if s["btc_bullish"]], 5),
        ("Scenario 3: Max 10 Trades | BTC Filter DISABLED", all_signals, 10),
        ("Scenario 4: Max 10 Trades | BTC Filter ENABLED", [s for s in all_signals if s["btc_bullish"]], 10),
    ]

    print("================================================================================================================")
    print(" 📊 HEAD-TO-HEAD LIQUIDATION & FINAL BALANCE TEST ($100 ACCOUNT, 10x LEVERAGE)")
    print("================================================================================================================")
    print(f"{'Scenario Name':<45} | {'Liquidated?':<12} | {'Lowest Equity':<14} | {'Trades Taken':<13} | {'Final Balance':<15} | {'ROI %':<10}")
    print("-" * 128)

    sim_results = []
    for name, sigs, max_t in scenarios:
        res = run_portfolio_sim(sigs, max_concurrent_trades=max_t, initial_balance=100.0, margin_per_trade=5.0, leverage=10.0)
        sim_results.append((name, res))
        liq_str = "YES (REKT)" if res["liquidated"] else "NO (SURVIVED)"
        print(f"{name:<45} | {liq_str:<12} | ${res['min_equity_ever']:>12.2f} | {res['total_trades_taken']:>13} | ${res['final_balance']:>13.2f} | {res['roi_pct']:>+8.1f}%")

    print("================================================================================================================\n")

    # Safety stress test: testing max concurrent trades from 1 to 15
    print("🔍 STRESS-TESTING MAX OPEN TRADES LIMIT (Finding the exact safe limit):")
    print(f"{'Max Open Trades Allowed':<25} | {'Liquidated?':<12} | {'Lowest Equity Ever':<20} | {'Final Balance ($100 Start)':<25} | {'Net Profit'}")
    print("-" * 105)
    for cap in [3, 4, 5, 6, 7, 8, 9, 10, 12, 15]:
        r = run_portfolio_sim([s for s in all_signals if s["btc_bullish"]], max_concurrent_trades=cap)
        l_str = "LIQUIDATED" if r["liquidated"] else "SURVIVED ✅"
        print(f"Max {cap:<2} concurrent trades   | {l_str:<12} | ${r['min_equity_ever']:>18.2f} | ${r['final_balance']:>23.2f} | {r['roi_pct']:>+8.1f}%")
    print("================================================================================================================\n")

if __name__ == "__main__":
    asyncio.run(run_simulation())

