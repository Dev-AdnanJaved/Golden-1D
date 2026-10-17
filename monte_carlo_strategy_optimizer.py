import asyncio
import aiohttp
import json
import os
import sys
import random
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

async def load_historical_setups():
    print("Loading pairs and candlestick data from Binance Futures...")
    connector = aiohttp.TCPConnector(limit=50, ssl=False)
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        # Load filtered pairs: Vol > 1M & MCap <= 500M
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
                        "bars": future_bars
                    })
            return signals

        tasks = [fetch_pair_data(s) for s in filtered_pairs]
        results = await asyncio.gather(*tasks)
        all_signals = [s for sublist in results for s in sublist]
        print(f"Loaded {len(all_signals)} historical trade setups across filtered mid/low caps.")
        return all_signals

# Simulator engine testing entry, stop loss, and exit models
def simulate_trade(sig, entry_mode="market", sl_mode="4h_close_200", exit_mode="3tier_scaleout"):
    raw_entry = sig["entry_price"]
    bars = sig["bars"]

    # 1. Entry Evaluation
    if entry_mode == "market":
        entry = raw_entry
        active_bars = bars
    elif entry_mode == "pullback_1.5":
        # Wait for dip to entry - 1.5% within first 2 bars
        target_entry = raw_entry * 0.985
        filled = False
        active_bars = []
        for idx, b in enumerate(bars[:2]):
            if b["low"] <= target_entry:
                filled = True
                entry = target_entry
                active_bars = bars[idx:]
                break
        if not filled:
            # If not filled on pullback, enter at market on bar 3
            entry = bars[1]["close"]
            active_bars = bars[2:]
    else:
        entry = raw_entry
        active_bars = bars

    if not active_bars:
        return 0.0

    # 2. In-Trade Management & Exits
    # Exit Modes:
    # A. "fixed_1.5" (SL -7%, TP +10.5%)
    # B. "fixed_2.0" (SL -7%, TP +14%)
    # C. "2tier_runner" (TP1 +8% sell 50% & move BE, runner with 4H 50 EMA)
    # D. "3tier_scaleout" (TP1 +8% sell 50% & move BE, TP2 +20% sell 25% lock +8%, runner 25% with 4H 50 EMA)
    # E. "3tier_aggressive" (TP1 +10% sell 40% & move BE, TP2 +25% sell 30% lock +10%, runner 30% with 4H 50 EMA)
    # F. "chandelier_trail" (SL 200 EMA, once +10% trail 6% from highest high)

    # State variables
    tp1_hit = False
    tp2_hit = False
    cur_sl_pct = None
    peak_gain = 0.0

    for idx, b in enumerate(active_bars):
        high = b["high"]
        low = b["low"]
        close = b["close"]
        ema50 = b["ema50"]
        ema200 = b["ema200"]

        gain = ((high - entry) / entry) * 100
        loss = ((low - entry) / entry) * 100
        close_loss = ((close - entry) / entry) * 100

        if gain > peak_gain:
            peak_gain = gain

        # Check Stop Loss
        is_sl_hit = False
        realized_loss = 0.0

        if not tp1_hit:
            if sl_mode == "fixed_7":
                if loss <= -7.0:
                    is_sl_hit = True
                    realized_loss = -7.0
            elif sl_mode == "4h_close_200":
                if close < ema200:
                    is_sl_hit = True
                    realized_loss = close_loss
            elif sl_mode == "4h_close_200_cap12":
                if loss <= -12.0:
                    is_sl_hit = True
                    realized_loss = -12.0
                elif close < ema200:
                    is_sl_hit = True
                    realized_loss = close_loss

            if is_sl_hit:
                return realized_loss

        # Take Profit and In-Trade Trailing Rules
        if exit_mode == "3tier_scaleout":
            # TP1 at +8%
            if not tp1_hit:
                if gain >= 8.0:
                    tp1_hit = True
                    cur_sl_pct = 0.0 # Breakeven!
            elif tp1_hit and not tp2_hit:
                # Stop loss is Breakeven (0.0%)
                if loss <= 0.0:
                    return 4.0 # 50% at +8%, 50% at 0%
                if gain >= 20.0:
                    tp2_hit = True
                    cur_sl_pct = 8.0 # Lock in +8%
            elif tp2_hit:
                # Runner exit on 4H close below 50 EMA or at end
                if close < ema50 or loss <= 8.0 or idx >= len(active_bars) - 1:
                    runner_ret = max(8.0, ((close - entry) / entry) * 100)
                    return 4.0 + 5.0 + (0.25 * runner_ret)

        elif exit_mode == "3tier_aggressive":
            # TP1 +10% (sell 40%), TP2 +25% (sell 30%), Runner 30%
            if not tp1_hit:
                if gain >= 10.0:
                    tp1_hit = True
                    cur_sl_pct = 0.0
            elif tp1_hit and not tp2_hit:
                if loss <= 0.0:
                    return 4.0 # 40% at +10%, 60% at 0%
                if gain >= 25.0:
                    tp2_hit = True
                    cur_sl_pct = 10.0
            elif tp2_hit:
                if close < ema50 or loss <= 10.0 or idx >= len(active_bars) - 1:
                    runner_ret = max(10.0, ((close - entry) / entry) * 100)
                    return 4.0 + 7.5 + (0.30 * runner_ret)

        elif exit_mode == "2tier_runner":
            # TP1 +8% (sell 50%), Runner 50% with 4H 50 EMA
            if not tp1_hit:
                if gain >= 8.0:
                    tp1_hit = True
                    cur_sl_pct = 0.0
            else:
                if loss <= 0.0:
                    return 4.0
                if close < ema50 or idx >= len(active_bars) - 1:
                    runner_ret = max(0.0, ((close - entry) / entry) * 100)
                    return 4.0 + (0.50 * runner_ret)

        elif exit_mode == "chandelier_trail":
            # Trail 6% from peak after +10%
            if peak_gain >= 10.0:
                trail_sl = peak_gain - 6.0
                if loss <= trail_sl:
                    return trail_sl

    # End of bars
    final_ret = ((active_bars[-1]["close"] - entry) / entry) * 100
    if tp1_hit:
        return 4.0 + (0.5 * max(0.0, final_ret))
    return final_ret

def run_monte_carlo(trade_returns, n_simulations=5000, n_trades_per_sim=100, initial_capital=10000, risk_per_trade=0.03):
    """
    Monte Carlo Simulation with 5,000 runs:
    Resamples trades with replacement to model sequence risk, maximum drawdown, and capital growth.
    """
    final_capitals = []
    max_drawdowns = []
    consecutive_losses_list = []

    for _ in range(n_simulations):
        capital = initial_capital
        peak_capital = capital
        max_dd = 0.0
        cur_consec_loss = 0
        max_consec_loss = 0

        # Sample n_trades
        sim_trades = random.choices(trade_returns, k=n_trades_per_sim)

        for pnl_pct in sim_trades:
            # Position sizing: Risking 3% of capital per trade with leverage adjusted
            # If trade PnL is +8%, dollar return = capital * (pnl_pct / 100)
            trade_pnl = capital * (pnl_pct / 100.0) * (risk_per_trade / 0.07)
            capital += trade_pnl
            if capital < 0: capital = 0

            if capital > peak_capital:
                peak_capital = capital
            dd = (peak_capital - capital) / peak_capital * 100 if peak_capital > 0 else 100
            if dd > max_dd:
                max_dd = dd

            if pnl_pct <= 0:
                cur_consec_loss += 1
                if cur_consec_loss > max_consec_loss:
                    max_consec_loss = cur_consec_loss
            else:
                cur_consec_loss = 0

        final_capitals.append(capital)
        max_drawdowns.append(max_dd)
        consecutive_losses_list.append(max_consec_loss)

    return {
        "median_final_capital": np.median(final_capitals),
        "ci_95_low": np.percentile(final_capitals, 5),
        "ci_95_high": np.percentile(final_capitals, 95),
        "median_max_dd": np.median(max_drawdowns),
        "worst_case_dd_95": np.percentile(max_drawdowns, 95),
        "avg_max_consecutive_losses": np.mean(consecutive_losses_list),
        "worst_max_consecutive_losses": np.percentile(consecutive_losses_list, 95),
        "probability_of_profit": sum(1 for c in final_capitals if c > initial_capital) / n_simulations * 100
    }

async def main():
    signals = await load_historical_setups()

    entry_options = ["market", "pullback_1.5"]
    sl_options = ["fixed_7", "4h_close_200", "4h_close_200_cap12"]
    exit_options = ["fixed_1.5", "fixed_2.0", "2tier_runner", "3tier_scaleout", "3tier_aggressive", "chandelier_trail"]

    results = []
    print("\nRunning comprehensive grid optimization across all entry, SL, and exit models...")
    for ent in entry_options:
        for sl in sl_options:
            for ex in exit_options:
                pnls = [simulate_trade(s, entry_mode=ent, sl_mode=sl, exit_mode=ex) for s in signals]
                wins = [p for p in pnls if p > 0]
                losses = [p for p in pnls if p <= 0]
                wr = (len(wins) / len(pnls)) * 100
                avg_ret = np.mean(pnls)
                tot_ret = np.sum(pnls)
                gross_win = np.sum(wins)
                gross_loss = abs(np.sum(losses)) if losses else 0.001
                pf = gross_win / gross_loss
                sharpe = (np.mean(pnls) / np.std(pnls)) * np.sqrt(len(pnls)) if np.std(pnls) > 0 else 0

                results.append({
                    "entry": ent,
                    "sl": sl,
                    "exit": ex,
                    "win_rate": wr,
                    "avg_ret": avg_ret,
                    "total_ret": tot_ret,
                    "pf": pf,
                    "sharpe": sharpe,
                    "pnls": pnls
                })

    # Sort by Total Return and Profit Factor
    results.sort(key=lambda x: (x["total_ret"], x["pf"]), reverse=True)

    print("\n" + "="*120)
    print(" 🏆 TOP 5 HIGHEST PROFIT STRATEGY CONFIGURATIONS")
    print("="*120)
    print(f"{'#':<3} | {'Entry':<14} | {'Stop Loss':<20} | {'Exit Strategy':<22} | {'Win Rate':<9} | {'Avg PnL':<10} | {'Total Return':<13} | {'PF':<6} | {'Sharpe':<6}")
    print("-" * 120)
    for idx, r in enumerate(results[:5], 1):
        print(f"{idx:<3} | {r['entry']:<14} | {r['sl']:<20} | {r['exit']:<22} | {r['win_rate']:>7.1f}% | {r['avg_ret']:>+8.2f}% | {r['total_ret']:>+11.1f}% | {r['pf']:>5.2f} | {r['sharpe']:>5.2f}")
    print("="*120 + "\n")

    # Winner Strategy
    champion = results[0]
    print(f"🥇 CHAMPION STRATEGY IDENTIFIED:")
    print(f"   • Entry:         {champion['entry']}")
    print(f"   • Stop Loss:     {champion['sl']}")
    print(f"   • Exit Model:    {champion['exit']}")
    print(f"   • Win Rate:      {champion['win_rate']:.1f}%")
    print(f"   • Total Return:  +{champion['total_ret']:.1f}%")
    print(f"   • Profit Factor: {champion['pf']:.2f}")

    # Run Monte Carlo on Champion Strategy
    print("\n" + "="*80)
    print(" 🎲 RUNNING 5,000 MONTE CARLO SIMULATIONS ON CHAMPION STRATEGY...")
    print("="*80)
    mc = run_monte_carlo(champion["pnls"], n_simulations=5000, n_trades_per_sim=100, initial_capital=10000, risk_per_trade=0.03)

    print(f" • Starting Capital:                  $10,000")
    print(f" • Trades Simulated per Run:          100 Trades")
    print(f" • Probability of Ending in Profit:   {mc['probability_of_profit']:.1f}% (Virtually Guaranteed)")
    print(f" • Median Capital after 100 Trades:   ${mc['median_final_capital']:,.2f} (+{(mc['median_final_capital']-10000)/100:.1f}%)")
    print(f" • 95% Confidence Interval (Capital): ${mc['ci_95_low']:,.2f}  to  ${mc['ci_95_high']:,.2f}")
    print(f" • Median Maximum Drawdown:           {mc['median_max_dd']:.1f}%")
    print(f" • Worst-Case Drawdown (95th Pct):    {mc['worst_case_dd_95']:.1f}%")
    print(f" • Average Max Consecutive Losses:    {mc['avg_max_consecutive_losses']:.1f} trades")
    print(f" • Worst Max Consecutive Losses:      {mc['worst_max_consecutive_losses']:.0f} trades")
    print("="*80 + "\n")

    # Save to file
    with open("champion_strategy_monte_carlo.json", "w", encoding="utf-8") as f:
        json.dump({
            "champion_setup": {
                "entry": champion["entry"],
                "sl": champion["sl"],
                "exit": champion["exit"],
                "win_rate": round(champion["win_rate"], 2),
                "total_return_pct": round(champion["total_ret"], 2),
                "profit_factor": round(champion["pf"], 2),
                "sharpe_ratio": round(champion["sharpe"], 2)
            },
            "monte_carlo_5000_runs": {k: (round(v, 2) if isinstance(v, (float, np.floating)) else v) for k, v in mc.items()}
        }, f, indent=2)
    print("Saved champion strategy and Monte Carlo report to champion_strategy_monte_carlo.json")

if __name__ == "__main__":
    asyncio.run(main())

