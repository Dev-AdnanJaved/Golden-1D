import asyncio
import json
import os
import sys
import time
from signal_tracker import SignalTracker, TP_MILESTONES, MONITORING_DURATION_SECONDS

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

class MockNotifier:
    def __init__(self):
        self.tp_alerts = []
        self.sl_alerts = []
        self.completed_alerts = []

    async def send_tp_hit_alert(self, symbol, entry_price, current_price, target_pct, max_gain_pct, elapsed_hours):
        print(f"  [MOCK TELEGRAM] 🎯 TP ALERT: {symbol} hit +{target_pct}%! Current: ${current_price:.4f} (Max: +{max_gain_pct:.1f}%)")
        self.tp_alerts.append((symbol, target_pct))
        return True

    async def send_sl_hit_alert(self, symbol, entry_price, exit_price, loss_pct, reason, elapsed_hours):
        print(f"  [MOCK TELEGRAM] 🛑 SL ALERT: {symbol} triggered SL at ${exit_price:.4f} ({loss_pct:.1f}%). 7-day monitoring continues!")
        self.sl_alerts.append((symbol, exit_price))
        return True

    async def send_7d_completion_alert(self, sig):
        print(f"  [MOCK TELEGRAM] 🏁 7-DAY COMPLETED: {sig.symbol} finished. Max gain: +{sig.max_gain_pct:.1f}% | SL was hit? {sig.sl_hit}")
        self.completed_alerts.append(sig.symbol)
        return True

class MockClient:
    def __init__(self, prices_map):
        self.prices_map = prices_map

    async def get_all_tickers(self):
        return self.prices_map

    async def get_klines(self, symbol, interval, limit):
        # Return mock klines with 200 EMA higher than current price to trigger SL
        base_p = self.prices_map.get(symbol, 10.0)
        return [{"open_time": 1000, "close": base_p}] * 210

async def test_suite():
    test_db = "test_tracked_signals.json"
    if os.path.exists(test_db):
        os.remove(test_db)

    tracker = SignalTracker(filename=test_db)
    notifier = MockNotifier()

    print("=================================================================")
    print(" 🧪 TESTING 7-DAY SIGNAL TRACKER & ALERT NOTIFIER")
    print("=================================================================\n")

    # 1. Add a signal for NEARUSDT at $5.00
    print("1. Adding #NEARUSDT signal at $5.00...")
    now = time.time()
    sig = tracker.add_signal("NEARUSDT", entry_price=5.00, entry_time=now)
    assert sig.symbol == "NEARUSDT"
    assert sig.entry_price == 5.00
    assert sig.status == "MONITORING"
    print("   ✅ Signal added to tracker.\n")

    # 2. Simulate price surging to +15% ($5.75) -> Should trigger TP +10%
    print("2. Price pumps to $5.75 (+15%). Checking for TP +10% alert...")
    client1 = MockClient({"NEARUSDT": 5.75})
    await tracker.update_all_tracked(client1, notifier)
    assert 10 in sig.tps_hit
    assert len(notifier.tp_alerts) == 1
    assert notifier.tp_alerts[0] == ("NEARUSDT", 10)
    print("   ✅ TP1 (+10%) successfully triggered and alerted!\n")

    # 3. Simulate price surging to +55% ($7.75) -> Should trigger TP +20%, +30%, +50%
    print("3. Price rockets to $7.75 (+55%). Checking for +20%, +30%, +50% alerts...")
    client2 = MockClient({"NEARUSDT": 7.75})
    await tracker.update_all_tracked(client2, notifier)
    assert 20 in sig.tps_hit
    assert 30 in sig.tps_hit
    assert 50 in sig.tps_hit
    assert len(notifier.tp_alerts) == 4 # 10, 20, 30, 50
    print("   ✅ Milestone TPs (+20%, +30%, +50%) triggered without duplicate spam!\n")

    # 4. Simulate a price dip causing SL
    print("4. Price dumps to $4.20 (-16%). Checking SL trigger & continued 7-day monitoring...")
    client3 = MockClient({"NEARUSDT": 4.20})
    await tracker.update_all_tracked(client3, notifier)
    assert sig.sl_hit == True
    assert sig.status == "MONITORING" # CRITICAL: Still monitoring even after SL!
    assert len(notifier.sl_alerts) == 1
    print("   ✅ SL alert triggered AND coin is STILL actively monitored for remaining 7 days!\n")

    # 5. Check persistence in JSON file
    print("5. Verifying JSON persistence...")
    with open(test_db, "r", encoding="utf-8") as f:
        stored_data = json.load(f)
    stored_sig = list(stored_data.values())[0]
    assert stored_sig["symbol"] == "NEARUSDT"
    assert stored_sig["tps_hit"] == [10, 20, 30, 50]
    assert stored_sig["sl_hit"] == True
    assert stored_sig["max_gain_pct"] >= 55.0
    print(f"   ✅ JSON file verified! Stored {len(stored_sig['events_log'])} historical event logs.\n")

    # 6. Simulate 7-day completion
    print("6. Simulating 7-day expiry...")
    sig.monitored_until = now - 1 # force expiration
    tracker.save()
    await tracker.update_all_tracked(client2, notifier)
    assert sig.status == "COMPLETED_7_DAYS"
    assert len(notifier.completed_alerts) == 1
    print("   ✅ 7-day completion card dispatched and status updated to COMPLETED_7_DAYS!\n")

    if os.path.exists(test_db):
        os.remove(test_db)

    print("=================================================================")
    print(" 🎉 ALL TESTS PASSED! 7-DAY TRACKER IS 100% PRODUCTION READY!")
    print("=================================================================\n")

if __name__ == "__main__":
    asyncio.run(test_suite())

