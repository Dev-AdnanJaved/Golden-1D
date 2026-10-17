import asyncio
import aiohttp
import json
import os
import sys
import pandas as pd
from datetime import datetime, timezone

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

BASE_URL = "https://fapi.binance.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

async def analyze_sequence():
    with open("backtest_results.json", encoding="utf-8") as f:
        signals = json.load(f)

    # Let's inspect each signal bar-by-bar
    # We will test realistic Take Profit & Stop Loss combinations:
    # Model A: SL = -7%, TP1 = +7% (1:1), TP2 = +14% (2:1), trailing SL to break-even once +7% hit
    # Model B: SL = -7%, Pure TP = +10%
    # Model C: SL = -7%, Pure TP = +5%
    # Model D: SL = -5%, Pure TP = +5%

    connector = aiohttp.TCPConnector(limit=50, ssl=False)
    semaphore = asyncio.Semaphore(25)

    detailed_trades = []

    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        async def process_sig(sig):
            sym = sig["symbol"]
            ts = int(datetime.strptime(sig["timestamp"], "%Y-%m-%d %H:%M UTC").replace(tzinfo=timezone.utc).timestamp() * 1000)
            entry_price = sig["entry_price"]

            url = f"{BASE_URL}/fapi/v1/klines"
            params = {"symbol": sym, "interval": "4h", "startTime": ts, "limit": 45}
            try:
                async with semaphore:
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200: return None
                        kl = await resp.json()
                        if len(kl) < 2: return None
            except:
                return None

            # Candle 0 is the trigger candle. Future candles are 1..42
            future_bars = kl[1:]

            # Track bar by bar
            hit_tp_5 = False
            hit_tp_7 = False
            hit_tp_10 = False
            hit_tp_15 = False

            hit_sl_5 = False
            hit_sl_7 = False
            hit_sl_10 = False

            # Sequence determination for SL -7% vs TP +7%
            # What was hit FIRST?
            first_event_7 = "HOLDING/OPEN"
            bar_of_event_7 = 0

            max_pump_before_sl7 = 0.0
            max_dd_before_tp7 = 0.0

            cur_highest = entry_price
            cur_lowest = entry_price

            for bar_idx, b in enumerate(future_bars, 1):
                high = float(b[2])
                low = float(b[3])
                close = float(b[4])

                gain = ((high - entry_price) / entry_price) * 100
                loss = ((low - entry_price) / entry_price) * 100

                if gain > cur_highest:
                    cur_highest = gain
                if loss < cur_lowest:
                    cur_lowest = loss

                # Check what was hit first: +7% TP or -7% SL
                if first_event_7 == "HOLDING/OPEN":
                    if gain >= 7.0 and loss > -7.0:
                        first_event_7 = "TP (+7%) HIT FIRST"
                        bar_of_event_7 = bar_idx
                    elif loss <= -7.0 and gain < 7.0:
                        first_event_7 = "SL (-7%) HIT FIRST"
                        bar_of_event_7 = bar_idx
                    elif gain >= 7.0 and loss <= -7.0:
                        # Intra-bar collision! Check open/close to estimate order
                        open_p = float(b[1])
                        if close >= open_p:
                            # Green bar: likely dipped first then pumped, or opened and pumped
                            first_event_7 = "TP (+7%) HIT FIRST"
                        else:
                            first_event_7 = "SL (-7%) HIT FIRST"
                        bar_of_event_7 = bar_idx

            return {
                "symbol": sym,
                "timestamp": sig["timestamp"],
                "entry_price": entry_price,
                "max_pump_overall": sig["max_pump_pct"],
                "max_drawdown_overall": sig["max_drawdown_pct"],
                "first_event_7": first_event_7,
                "bar_of_event_7": bar_of_event_7,
                "hours_to_event": bar_of_event_7 * 4
            }

        tasks = [process_sig(s) for s in signals]
        results = await asyncio.gather(*tasks)
        detailed_trades = [r for r in results if r]

    # Analysis
    df = pd.DataFrame(detailed_trades)
    total = len(df)
    tp_first = df[df["first_event_7"] == "TP (+7%) HIT FIRST"]
    sl_first = df[df["first_event_7"] == "SL (-7%) HIT FIRST"]
    neither = df[df["first_event_7"] == "HOLDING/OPEN"]

    print("=================================================================")
    print(" 🎯 SEQUENCE ANALYSIS: TP (+7%) vs SL (-7%)")
    print("=================================================================")
    print(f"Total Validated Trades:          {total}")
    print(f"Trades hitting +7% TP FIRST:     {len(tp_first)} ({len(tp_first)/total*100:.1f}%)")
    print(f"Trades hitting -7% SL FIRST:     {len(sl_first)} ({len(sl_first)/total*100:.1f}%)")
    print(f"Trades hitting neither in 7 days:{len(neither)} ({len(neither)/total*100:.1f}%)")
    print("-----------------------------------------------------------------")
    print(f"Average hours to reach +7% TP:   {tp_first['hours_to_event'].mean():.1f} hours ({tp_first['hours_to_event'].mean()/24.0:.1f} days)")
    print(f"Average hours to hit -7% SL:     {sl_first['hours_to_event'].mean():.1f} hours ({sl_first['hours_to_event'].mean()/24.0:.1f} days)")
    print("=================================================================\n")

    # Sample cases where max_drawdown was <= -7% but TP (+7% or +20%) was hit first!
    pump_then_dump = df[(df["first_event_7"] == "TP (+7%) HIT FIRST") & (df["max_drawdown_overall"] <= -7.0)]
    print(f"Trades that pumped +7% or higher FIRST, then dumped -7% days later:")
    print(f"Count: {len(pump_then_dump)} trades ({len(pump_then_dump)/total*100:.1f}% of all trades!)")
    print("\nSample examples of PUMP FIRST, THEN DUMP LATER:")
    for _, r in pump_then_dump.head(8).iterrows():
        print(f"  #{r['symbol']} ({r['timestamp']}): Pumped +{r['max_pump_overall']}% FIRST! (Later drew down {r['max_drawdown_overall']}%) -> TP Hit in {r['hours_to_event']}h")

if __name__ == "__main__":
    asyncio.run(analyze_sequence())
