import asyncio
import os
import sys
import time
from datetime import datetime, timezone
from config import config
from strategy import SignalResult, Rule1DailyResult, Rule24HResult, Rule3VolumeResult
from telegram_notifier import TelegramNotifier
from signal_tracker import TrackedSignal

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

async def run_all_alerts_test():
    print("=================================================================")
    print(" 🧪 TELEGRAM NOTIFICATION SYSTEM FULL AUDIT & TEST SUITE")
    print("=================================================================\n")

    notifier = TelegramNotifier()
    if not notifier.is_configured():
        print("❌ ERROR: Telegram is NOT configured in your .env file!")
        print("Please ensure TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are set in .env")
        return

    print(f"Target Chat ID: {config.TELEGRAM_CHAT_ID}")
    print("Sending all 6 types of Telegram alerts sequentially to verify formatting...\n")

    # -------------------------------------------------------------
    # 1. Startup Notification
    # -------------------------------------------------------------
    print("[1/6] Testing: Bot Startup Notification...")
    ok1 = await notifier.send_startup_alert()
    if ok1:
        print("   ✅ SUCCESS: Startup Alert delivered to Telegram!\n")
    else:
        print("   ❌ FAILED: Check logs above for Telegram API error.\n")
    await asyncio.sleep(1.5)

    # -------------------------------------------------------------
    # 2. Main Signal Alert (Volume PASSED)
    # -------------------------------------------------------------
    print("[2/6] Testing: Main Buy/Long Signal (Volume PASSED)...")
    sig_passed = SignalResult(
        symbol="NEARUSDT",
        is_valid_signal=True,
        current_price=5.2450,
        rule1_daily=Rule1DailyResult(
            passed=True,
            current_close=5.2450,
            ema50=4.9210,
            fresh_cross=False,
            details="1D Candle above 50 EMA"
        ),
        rule2_4h=Rule24HResult(
            passed=True,
            trigger_type="CURRENT_CANDLE_CROSS",
            ema50=5.1200,
            ema200=5.0850,
            diff_pct=0.688,
            details="4H 50 EMA crossed above 200 EMA"
        ),
        rule3_volume=Rule3VolumeResult(
            passed=True,
            avg_vol_7=1452300.0,
            avg_vol_24=985400.0,
            ratio_pct=47.4,
            details="Volume spike confirmed"
        ),
        has_volume_warning=False,
        candle_time=int(time.time() * 1000),
        vol_24h=18500000.0,
        mcap=520000000.0
    )
    ok2 = await notifier.send_signal(sig_passed)
    if ok2:
        print("   ✅ SUCCESS: Buy Signal (Volume Passed) delivered to Telegram!\n")
    else:
        print("   ❌ FAILED: Check logs above.\n")
    await asyncio.sleep(1.5)

    # -------------------------------------------------------------
    # 3. Main Signal Alert (Volume WARNING)
    # -------------------------------------------------------------
    print("[3/6] Testing: Main Buy/Long Signal (Volume WARNING)...")
    sig_warn = SignalResult(
        symbol="APTUSDT",
        is_valid_signal=True,
        current_price=9.8500,
        rule1_daily=Rule1DailyResult(
            passed=True,
            current_close=9.8500,
            ema50=9.1200,
            fresh_cross=True,
            details="1D Fresh cross"
        ),
        rule2_4h=Rule24HResult(
            passed=True,
            trigger_type="PREVIOUS_CANDLE_CROSS",
            ema50=9.6500,
            ema200=9.6100,
            diff_pct=0.416,
            details="4H 50 EMA crossed 200 EMA"
        ),
        rule3_volume=Rule3VolumeResult(
            passed=False,
            avg_vol_7=620000.0,
            avg_vol_24=850000.0,
            ratio_pct=-27.1,
            details="Low volume warning"
        ),
        has_volume_warning=True,
        candle_time=int(time.time() * 1000),
        vol_24h=24000000.0,
        mcap=850000000.0
    )
    ok3 = await notifier.send_signal(sig_warn)
    if ok3:
        print("   ✅ SUCCESS: Buy Signal (Volume Warning) delivered to Telegram!\n")
    else:
        print("   ❌ FAILED: Check logs above.\n")
    await asyncio.sleep(1.5)

    # -------------------------------------------------------------
    # 4. Milestone Take Profit Alert (+10% & +50%)
    # -------------------------------------------------------------
    print("[4/6] Testing: Milestone Take Profit Alerts (+10% & +50%)...")
    ok4a = await notifier.send_tp_hit_alert(
        symbol="NEARUSDT",
        entry_price=5.2450,
        current_price=5.7700,
        target_pct=10,
        max_gain_pct=10.01,
        elapsed_hours=3.5
    )
    await asyncio.sleep(1.0)
    ok4b = await notifier.send_tp_hit_alert(
        symbol="NEARUSDT",
        entry_price=5.2450,
        current_price=7.8675,
        target_pct=50,
        max_gain_pct=50.25,
        elapsed_hours=28.4
    )
    if ok4a and ok4b:
        print("   ✅ SUCCESS: Milestone TP alerts delivered to Telegram!\n")
    else:
        print("   ❌ FAILED: Check logs above.\n")
    await asyncio.sleep(1.5)

    # -------------------------------------------------------------
    # 5. Stop Loss Alert (with 7-Day Continued Monitoring notice)
    # -------------------------------------------------------------
    print("[5/6] Testing: Stop Loss Trigger Alert...")
    ok5 = await notifier.send_sl_hit_alert(
        symbol="SUIUSDT",
        entry_price=2.1500,
        exit_price=1.9950,
        loss_pct=-7.21,
        reason="4H Candle closed below 200 EMA ($2.0250)",
        elapsed_hours=14.2
    )
    if ok5:
        print("   ✅ SUCCESS: Stop Loss Alert delivered to Telegram!\n")
    else:
        print("   ❌ FAILED: Check logs above.\n")
    await asyncio.sleep(1.5)

    # -------------------------------------------------------------
    # 6. 7-Day Monitoring Completed Summary Card
    # -------------------------------------------------------------
    print("[6/6] Testing: 7-Day Monitoring Completed Summary Card...")
    mock_tracked = TrackedSignal(
        id="NEARUSDT_mock",
        symbol="NEARUSDT",
        entry_time=time.time() - (7 * 86400),
        entry_date="2026-10-11 11:45:00 UTC",
        entry_price=5.2450,
        monitored_until=time.time(),
        monitored_until_date="2026-10-18 11:45:00 UTC",
        current_price=7.9500,
        highest_price=8.1500,
        lowest_price=5.1200,
        max_gain_pct=55.39,
        max_loss_pct=-2.38,
        tps_hit=[10, 20, 30, 50],
        sl_hit=False,
        status="COMPLETED_7_DAYS"
    )
    ok6 = await notifier.send_7d_completion_alert(mock_tracked)
    if ok6:
        print("   ✅ SUCCESS: 7-Day Completion Card delivered to Telegram!\n")
    else:
        print("   ❌ FAILED: Check logs above.\n")

    print("=================================================================")
    all_passed = all([ok1, ok2, ok3, ok4a, ok4b, ok5, ok6])
    if all_passed:
        print(" 🎉 ALL 6 TELEGRAM ALERT TYPES PASSED 100%! ZERO HTML ERRORS!")
        print(" Check your Telegram app — you should see all 6 test cards!")
    else:
        print(" ⚠️ One or more alerts failed. Review the specific step above.")
    print("=================================================================\n")

if __name__ == "__main__":
    asyncio.run(run_all_alerts_test())
