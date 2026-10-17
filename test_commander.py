import asyncio
import sys
from signal_tracker import SignalTracker
from telegram_notifier import TelegramNotifier
from telegram_commander import TelegramCommander
from binance_client import BinanceFuturesClient

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

async def test_commander():
    notifier = TelegramNotifier()
    tracker = SignalTracker("test_tracked_signals.json")
    
    # Add a mock signal
    tracker.add_signal("SOLUSDT", 150.0)

    commander = TelegramCommander(notifier, tracker)

    print("=================================================================")
    print(" 🧪 TESTING TELEGRAM INTERACTIVE COMMANDS")
    print("=================================================================\n")

    async with BinanceFuturesClient() as client:
        # 1. /help
        print("1. Testing /help...")
        res_help = await commander.handle_command("/help", client)
        assert "/status" in res_help and "/btc" in res_help
        print("   ✅ /help output generated.\n")

        # 2. /status
        print("2. Testing /status...")
        res_status = await commander.handle_command("/status", client)
        assert "BOT OPERATIONAL STATUS" in res_status
        print("   ✅ /status output generated.\n")

        # 3. /tracked
        print("3. Testing /tracked...")
        res_tracked = await commander.handle_command("/tracked", client)
        assert "#SOLUSDT" in res_tracked
        print("   ✅ /tracked output generated with active signals.\n")

        # 4. /stats
        print("4. Testing /stats...")
        res_stats = await commander.handle_command("/stats", client)
        assert "SIGNAL TRACKER PERFORMANCE STATS" in res_stats
        print("   ✅ /stats output generated.\n")

        # 5. /btc
        print("5. Testing /btc real-time analysis...")
        res_btc = await commander.handle_command("/btc", client)
        assert "BITCOIN REAL-TIME TREND HEALTH" in res_btc
        print("   ✅ /btc analysis generated successfully!")
        print("-" * 50)
        print(res_btc[:300] + "...")
        print("-" * 50)

    # Cleanup test db
    import os
    if os.path.exists("test_tracked_signals.json"):
        os.remove("test_tracked_signals.json")

    print("\n🎉 ALL TELEGRAM COMMANDS VERIFIED AND WORKING PERFECTLY!\n")

if __name__ == "__main__":
    asyncio.run(test_commander())
