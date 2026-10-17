import asyncio
import aiohttp
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np

# Reconfigure stdout/stderr to UTF-8
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

BASE_URL = "https://fapi.binance.com"
MAX_CONCURRENCY = 25
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENCY)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

def compute_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

async def get_active_pairs(session: aiohttp.ClientSession) -> List[str]:
    url = f"{BASE_URL}/fapi/v1/exchangeInfo"
    async with session.get(url) as r:
        data = await r.json()
        return sorted([
            s["symbol"] for s in data["symbols"]
            if s["status"] == "TRADING"
            and s["contractType"] == "PERPETUAL"
            and s["quoteAsset"] == "USDT"
        ])

async def fetch_klines(session: aiohttp.ClientSession, symbol: str, interval: str, limit: int = 500) -> Optional[List[Dict[str, Any]]]:
    url = f"{BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        async with SEMAPHORE:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                if resp.status != 200:
                    return None
                raw = await resp.json()
                if not isinstance(raw, list) or len(raw) < 100:
                    return None
                return [{
                    "open_time": int(k[0]),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    "close_time": int(k[6])
                } for k in raw]
    except Exception:
        return None

async def backtest_symbol(session: aiohttp.ClientSession, symbol: str, cutoff_30d_ts: int) -> List[Dict[str, Any]]:
    # Fetch 4H klines (limit=500 -> ~83 days of 4h data)
    kl_4h = await fetch_klines(session, symbol, "4h", limit=500)
    if not kl_4h or len(kl_4h) < 250:
        return []

    # Calculate 4H EMAs
    closes_4h = pd.Series([k["close"] for k in kl_4h])
    ema50_4h = compute_ema(closes_4h, 50)
    ema200_4h = compute_ema(closes_4h, 200)

    # Find all 4H crosses in the last 30 days
    signals = []
    for i in range(1, len(kl_4h)):
        k = kl_4h[i]
        if k["open_time"] < cutoff_30d_ts:
            continue
        
        prev_50 = ema50_4h.iloc[i-1]
        prev_200 = ema200_4h.iloc[i-1]
        curr_50 = ema50_4h.iloc[i]
        curr_200 = ema200_4h.iloc[i]

        # Rule 2: 4H 50 EMA crossed above 200 EMA
        if prev_50 <= prev_200 and curr_50 > curr_200:
            signals.append({
                "symbol": symbol,
                "bar_idx": i,
                "open_time": k["open_time"],
                "trigger_price": k["close"],
                "ema50_4h": curr_50,
                "ema200_4h": curr_200
            })

    if not signals:
        return []

    # If signals found, fetch 1D and 1H to evaluate Rule 1 and Rule 3
    kl_1d = await fetch_klines(session, symbol, "1d", limit=120)
    if not kl_1d or len(kl_1d) < 60:
        return []

    closes_1d = pd.Series([k["close"] for k in kl_1d])
    ema50_1d = compute_ema(closes_1d, 50)
    df_1d = pd.DataFrame({
        "open_time": [k["open_time"] for k in kl_1d],
        "close_time": [k["close_time"] for k in kl_1d],
        "close": closes_1d,
        "ema50": ema50_1d
    })

    # Fetch 1H klines (limit=1000)
    kl_1h = await fetch_klines(session, symbol, "1h", limit=1000)

    validated_signals = []
    for sig in signals:
        sig_ts = sig["open_time"]

        # Rule 1 Check: Find 1D candle active at sig_ts
        # Daily candle starts at 00:00 UTC
        day_match = df_1d[df_1d["open_time"] <= sig_ts]
        if day_match.empty:
            continue
        day_row = day_match.iloc[-1]
        
        # Hard Rule 1: 1D Close > 50 EMA
        if day_row["close"] <= day_row["ema50"]:
            continue  # Hard filter failed

        # Rule 3 Check (Soft Filter): 1H Volume
        vol_passed = False
        vol_ratio = 0.0
        avg_vol_7 = 0.0
        avg_vol_24 = 0.0
        if kl_1h:
            # Find 1H bars up to sig_ts
            bars_up_to = [b for b in kl_1h if b["open_time"] <= sig_ts]
            if len(bars_up_to) >= 24:
                vols_7 = [b["volume"] for b in bars_up_to[-7:]]
                vols_24 = [b["volume"] for b in bars_up_to[-24:]]
                avg_vol_7 = sum(vols_7) / 7
                avg_vol_24 = sum(vols_24) / 24
                vol_passed = avg_vol_7 > avg_vol_24
                vol_ratio = ((avg_vol_7 - avg_vol_24) / avg_vol_24) * 100 if avg_vol_24 > 0 else 0.0

        # Now measure forward outcome over the next 42 bars (~7 days)
        entry_idx = sig["bar_idx"]
        entry_price = sig["trigger_price"]
        forward_bars = kl_4h[entry_idx + 1 : entry_idx + 43]

        if not forward_bars:
            continue

        highest_price = entry_price
        lowest_price = entry_price
        time_to_peak_hours = 0
        peak_bar_offset = 0

        for offset, fb in enumerate(forward_bars, 1):
            if fb["high"] > highest_price:
                highest_price = fb["high"]
                time_to_peak_hours = offset * 4
                peak_bar_offset = offset
            if fb["low"] < lowest_price:
                lowest_price = fb["low"]

        max_pump_pct = ((highest_price - entry_price) / entry_price) * 100
        max_drawdown_pct = ((lowest_price - entry_price) / entry_price) * 100

        # Outcome categorization:
        # Huge Win: >= 20%
        # Solid Win: 10% to 20%
        # Modest Win: 5% to 10%
        # Scratch: 2% to 5%
        # Failed: Max gain < 2% or Max Drawdown exceeded -7% before any gain
        is_success = max_pump_pct >= 5.0
        failure_reason = ""
        if not is_success:
            if not vol_passed:
                failure_reason = "No volume confirmation (Soft filter failed)"
            elif max_drawdown_pct < -5.0:
                failure_reason = "Immediate sharp pullback / fakeout"
            else:
                failure_reason = "Stagnant / sideways chop"

        dt_str = datetime.fromtimestamp(sig_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        validated_signals.append({
            "symbol": symbol,
            "timestamp": dt_str,
            "entry_price": entry_price,
            "highest_price": highest_price,
            "lowest_price": lowest_price,
            "max_pump_pct": round(max_pump_pct, 2),
            "max_drawdown_pct": round(max_drawdown_pct, 2),
            "time_to_peak_hours": time_to_peak_hours,
            "vol_passed": vol_passed,
            "vol_ratio": round(vol_ratio, 1),
            "is_success": is_success,
            "failure_reason": failure_reason
        })

    return validated_signals

async def main():
    start_time = time.time()
    now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    cutoff_30d_ts = now_ts - (30 * 24 * 3600 * 1000)

    print("=================================================================")
    print(" 🚀 BINANCE FUTURES 30-DAY BACKTEST ACROSS ALL PAIRS")
    print("=================================================================")
    print(f"Period: Last 30 Days (from {datetime.fromtimestamp(cutoff_30d_ts/1000, tz=timezone.utc).strftime('%Y-%m-%d')} to {datetime.now(timezone.utc).strftime('%Y-%m-%d')})")
    print("Criteria: Rule 1 (1D > 50 EMA) + Rule 2 (4H 50/200 Cross) + Rule 3 (1H Volume)")
    print("=================================================================\n")

    connector = aiohttp.TCPConnector(limit=100, ssl=False)
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        pairs = await get_active_pairs(session)
        print(f"Loaded {len(pairs)} active USDT-M Perpetual pairs. Running backtest...")

        all_results = []
        batch_size = 35
        for i in range(0, len(pairs), batch_size):
            batch = pairs[i:i+batch_size]
            tasks = [backtest_symbol(session, p, cutoff_30d_ts) for p in batch]
            res_list = await asyncio.gather(*tasks)
            for r in res_list:
                all_results.extend(r)
            print(f"Progress: {min(i+batch_size, len(pairs))}/{len(pairs)} pairs checked | Signals found so far: {len(all_results)}")
            await asyncio.sleep(0.15)

    elapsed = time.time() - start_time
    print(f"\n✅ Backtest completed in {elapsed:.1f}s!\n")

    # Save raw results
    with open("backtest_results.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    # Detailed Analysis
    df = pd.DataFrame(all_results)
    if df.empty:
        print("No signals found in the last 30 days.")
        return

    df.to_csv("backtest_results.csv", index=False)

    total_signals = len(df)
    huge_wins = df[df["max_pump_pct"] >= 20.0]
    solid_wins = df[(df["max_pump_pct"] >= 10.0) & (df["max_pump_pct"] < 20.0)]
    modest_wins = df[(df["max_pump_pct"] >= 5.0) & (df["max_pump_pct"] < 10.0)]
    scratches = df[(df["max_pump_pct"] >= 2.0) & (df["max_pump_pct"] < 5.0)]
    losses = df[df["max_pump_pct"] < 5.0]

    vol_pass_df = df[df["vol_passed"] == True]
    vol_warn_df = df[df["vol_passed"] == False]

    avg_pump = df["max_pump_pct"].mean()
    median_pump = df["max_pump_pct"].median()
    avg_dd = df["max_drawdown_pct"].mean()
    avg_hours_to_peak = df["time_to_peak_hours"].mean()

    print("=================================================================")
    print(" 📊 30-DAY BACKTEST PERFORMANCE SUMMARY")
    print("=================================================================")
    print(f"Total Setups Triggered:       {total_signals}")
    print(f"Average Signal Frequency:     {total_signals / 30.0:.1f} signals / day")
    print(f"Average Max Pump:             +{avg_pump:.2f}%")
    print(f"Median Max Pump:              +{median_pump:.2f}%")
    print(f"Average Max Drawdown:         {avg_dd:.2f}%")
    print(f"Average Time to Peak:         {avg_hours_to_peak:.1f} hours ({avg_hours_to_peak/24.0:.1f} days)")
    print("-----------------------------------------------------------------")
    print(" 🎯 PROFIT DISTRIBUTION:")
    print(f" • Huge Pumps (>= +20%):      {len(huge_wins)} ({len(huge_wins)/total_signals*100:.1f}%)")
    print(f" • Solid Pumps (+10% to +20%):{len(solid_wins)} ({len(solid_wins)/total_signals*100:.1f}%)")
    print(f" • Modest Pumps (+5% to +10%):{len(modest_wins)} ({len(modest_wins)/total_signals*100:.1f}%)")
    print(f" • Small Move (+2% to +5%):   {len(scratches)} ({len(scratches)/total_signals*100:.1f}%)")
    print(f" • Failed (< +5% gain):       {len(losses)} ({len(losses)/total_signals*100:.1f}%)")
    print(f" 🌟 OVERALL WIN RATE (>= +5%): {(total_signals - len(losses)) / total_signals * 100:.1f}%")
    print(f" 🌟 HIGH WIN RATE (>= +10%):  {(len(huge_wins) + len(solid_wins)) / total_signals * 100:.1f}%")
    print("-----------------------------------------------------------------")
    print(" 🔍 RULE 3 VOLUME FILTER IMPACT ANALYSIS:")
    print(f" When Volume Filter PASSED (n={len(vol_pass_df)}):")
    print(f"   • Win Rate (>= +5%):       {len(vol_pass_df[vol_pass_df['is_success']])/len(vol_pass_df)*100:.1f}%")
    print(f"   • Average Pump:            +{vol_pass_df['max_pump_pct'].mean():.2f}%")
    print(f"   • Average Drawdown:        {vol_pass_df['max_drawdown_pct'].mean():.2f}%")
    print(f" When Volume Filter had WARNING (n={len(vol_warn_df)}):")
    print(f"   • Win Rate (>= +5%):       {len(vol_warn_df[vol_warn_df['is_success']])/len(vol_warn_df)*100:.1f}%")
    print(f"   • Average Pump:            +{vol_warn_df['max_pump_pct'].mean():.2f}%")
    print(f"   • Average Drawdown:        {vol_warn_df['max_drawdown_pct'].mean():.2f}%")
    print("-----------------------------------------------------------------")
    print(" 📉 FAILURE CAUSE ANALYSIS (Why setups failed):")
    failure_counts = losses["failure_reason"].value_counts()
    for reason, count in failure_counts.items():
        print(f"   • {reason}: {count} ({count/len(losses)*100:.1f}% of failures)")
    print("-----------------------------------------------------------------")
    print(" 🏆 TOP 10 BIGGEST PUMPS IN LAST 30 DAYS:")
    top10 = df.sort_values(by="max_pump_pct", ascending=False).head(10)
    for idx, row in top10.iterrows():
        vol_flag = "Vol Confirmed" if row["vol_passed"] else "Vol Warning"
        print(f"   #{row['symbol']} ({row['timestamp']}): Entry ${row['entry_price']} -> High ${row['highest_price']} | Pump: +{row['max_pump_pct']}% in {row['time_to_peak_hours']}h ({vol_flag})")
    print("=================================================================\n")

if __name__ == "__main__":
    asyncio.run(main())

