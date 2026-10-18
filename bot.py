import asyncio
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

from colorama import Fore, Style, init
from config import config
from binance_client import BinanceFuturesClient
from strategy import (
    evaluate_rule_2_4h_cross,
    evaluate_rule_1_daily,
    evaluate_rule_3_volume,
    SignalResult,
    Rule1DailyResult,
    Rule24HResult,
    Rule3VolumeResult
)
from telegram_notifier import TelegramNotifier
from signal_tracker import SignalTracker
from telegram_commander import TelegramCommander

# Reconfigure stdout/stderr to UTF-8 for Windows console emoji support
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

init(autoreset=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("BotRunner")


class AlertHistoryManager:
    """Manages cooldowns to avoid sending duplicate alerts for the same symbol."""
    def __init__(self, filename: str = config.CACHE_FILE):
        self.filename = filename
        self.history: Dict[str, float] = self._load()

    def _load(self) -> Dict[str, float]:
        if os.path.exists(self.filename):
            try:
                with open(self.filename, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save(self):
        try:
            with open(self.filename, "w", encoding="utf-8") as f:
                json.dump(self.history, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save alert history: {e}")

    def is_on_cooldown(self, symbol: str) -> bool:
        last_alert_time = self.history.get(symbol)
        if not last_alert_time:
            return False
        elapsed_minutes = (time.time() - last_alert_time) / 60.0
        return elapsed_minutes < config.ALERT_COOLDOWN_MINUTES

    def record_alert(self, symbol: str):
        self.history[symbol] = time.time()
        self._cleanup()
        self._save()

    def _cleanup(self):
        now = time.time()
        max_age = config.ALERT_COOLDOWN_MINUTES * 60 * 2
        self.history = {
            s: t for s, t in self.history.items()
            if (now - t) < max_age
        }


@dataclass
class SymbolScanResult:
    symbol: str
    is_valid_signal: bool
    signal: Optional[SignalResult]
    r1: Optional[Rule1DailyResult]
    r2: Optional[Rule24HResult]
    r3: Optional[Rule3VolumeResult]
    current_price: float
    vol_24h: float
    mcap: Optional[float]
    rejection_reason: str


class BinanceScannerBot:
    def __init__(self):
        self.alert_history = AlertHistoryManager()
        self.notifier = TelegramNotifier()
        self.tracker = SignalTracker()

    async def scan_single_symbol(
        self,
        client: BinanceFuturesClient,
        symbol: str,
        vol_24h: float = 0.0,
        mcap: Optional[float] = None
    ) -> SymbolScanResult:
        """
        Evaluate a single symbol across all timeframes:
        1. 4H and 1D Kline fetched concurrently
        2. Evaluate Rule 1 (1D > 50 EMA) & Rule 2 (4H 50/200 cross)
        3. If both pass, evaluate Rule 3 (1H Volume filter)
        """
        try:
            klines_4h, klines_1d = await asyncio.gather(
                client.get_klines(symbol, interval="4h", limit=400),
                client.get_klines(symbol, interval="1d", limit=120),
                return_exceptions=True
            )
        except Exception as e:
            return SymbolScanResult(
                symbol=symbol,
                is_valid_signal=False,
                signal=None,
                r1=None,
                r2=None,
                r3=None,
                current_price=0.0,
                vol_24h=vol_24h,
                mcap=mcap,
                rejection_reason=f"Network error: {e}"
            )

        if not klines_4h or isinstance(klines_4h, Exception) or not klines_1d or isinstance(klines_1d, Exception):
            return SymbolScanResult(
                symbol=symbol,
                is_valid_signal=False,
                signal=None,
                r1=None,
                r2=None,
                r3=None,
                current_price=0.0,
                vol_24h=vol_24h,
                mcap=mcap,
                rejection_reason="Insufficient candle history on Binance"
            )

        r1 = evaluate_rule_1_daily(klines_1d)
        r2 = evaluate_rule_2_4h_cross(klines_4h)
        current_price = klines_4h[-1]["close"]
        candle_time = klines_4h[-1]["open_time"]

        # If Rule 1 or Rule 2 fails:
        if not r1.passed or not r2.passed:
            if not r1.passed and not r2.passed:
                if r2.diff_pct > 0:
                    reason = f"1D < 50 EMA & 4H old cross (>48h ago, Diff +{r2.diff_pct:.2f}%)"
                elif abs(r2.diff_pct) <= 0.8:
                    reason = f"1D < 50 EMA & 4H near cross (Diff {r2.diff_pct:+.2f}%)"
                else:
                    reason = f"1D < 50 EMA & 4H 50 EMA < 200 EMA (Diff {r2.diff_pct:+.2f}%)"
            elif not r1.passed and r2.passed:
                reason = f"1D candle below 50 EMA (${r1.current_close:.4f} < ${r1.ema50:.4f}) despite 4H cross"
            elif r1.passed and not r2.passed:
                if r2.diff_pct > 0:
                    reason = f"4H cross occurred > {config.CROSS_4H_LOOKBACK_HOURS}h ago (Diff +{r2.diff_pct:.2f}%)"
                elif abs(r2.diff_pct) <= 0.8:
                    reason = f"4H approaching cross (Diff {r2.diff_pct:+.2f}%, watching for trigger!)"
                else:
                    reason = f"4H 50 EMA < 200 EMA (Diff {r2.diff_pct:+.2f}%, no cross)"
            else:
                reason = "Conditions not satisfied"

            return SymbolScanResult(
                symbol=symbol,
                is_valid_signal=False,
                signal=None,
                r1=r1,
                r2=r2,
                r3=None,
                current_price=current_price,
                vol_24h=vol_24h,
                mcap=mcap,
                rejection_reason=reason
            )

        # Both Rule 1 and Rule 2 passed! Evaluate Rule 3 (Soft Volume Filter)
        klines_1h = await client.get_klines(symbol, interval="1h", limit=35)
        if not klines_1h:
            r3 = Rule3VolumeResult(
                passed=True, avg_vol_7=0.0, avg_vol_24=0.0, ratio_pct=0.0, details="1H data unavailable"
            )
        else:
            r3 = evaluate_rule_3_volume(klines_1h)

        is_fresh = getattr(r2, "is_fresh", True)
        cross_warning = None if is_fresh else f"⚠️ 4H Cross formed {r2.hours_ago:.0f}h ago ({r2.candles_ago} candles ago)"

        signal = SignalResult(
            symbol=symbol,
            is_valid_signal=True,
            current_price=current_price,
            rule1_daily=r1,
            rule2_4h=r2,
            rule3_volume=r3,
            has_volume_warning=not r3.passed,
            candle_time=candle_time,
            vol_24h=vol_24h,
            mcap=mcap,
            is_fresh_cross=is_fresh,
            cross_warning=cross_warning
        )

        return SymbolScanResult(
            symbol=symbol,
            is_valid_signal=True,
            signal=signal,
            r1=r1,
            r2=r2,
            r3=r3,
            current_price=current_price,
            vol_24h=vol_24h,
            mcap=mcap,
            rejection_reason="ALL CONDITIONS MET (SIGNAL DETECTED)"
        )

    async def run_scan_cycle(self, client: BinanceFuturesClient, target_symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute filtered scan across active Binance Futures USDT pairs.
        Filters: 24h Vol >= $1M USDT and Market Cap <= $1000M USDT
        """
        start_time = time.time()
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"\n{Fore.CYAN}{'='*65}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}🔍 Starting Scan Cycle at {now_utc}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*65}{Style.RESET_ALL}")

        if target_symbol:
            target_list = [{"symbol": target_symbol.upper(), "vol_24h": 0.0, "mcap": None}]
        else:
            print(f"Applying Pair Filters: 24h Vol > ${config.MIN_24H_VOLUME_USDT/1e6:.1f}M USDT | Market Cap <= ${config.MAX_MARKET_CAP_USDT/1e6:.0f}M USDT...")
            target_list = await client.get_filtered_pairs()

        if not target_list:
            logger.error("No qualifying USDT pairs retrieved. Will retry on next scan.")
            return {}

        total_pairs = len(target_list)
        print(f"Scanning {Fore.YELLOW}{total_pairs}{Style.RESET_ALL} qualifying pairs for 4H 50/200 EMA cross (Fresh + 48h lookback)...")
        print(f"{Fore.LIGHTBLACK_EX}Evaluating Rule 1 (1D > 50 EMA) & Rule 2 (4H 50/200 Golden Cross) pair by pair:{Style.RESET_ALL}\n")

        # Process in concurrent batches
        batch_size = 25
        detected_signals: List[SignalResult] = []
        all_evaluations: List[SymbolScanResult] = []
        pair_counter = 0
        r1_passed_count = 0
        r2_above_count = 0

        for i in range(0, total_pairs, batch_size):
            batch = target_list[i:i + batch_size]
            tasks = [self.scan_single_symbol(client, p["symbol"], p.get("vol_24h", 0.0), p.get("mcap")) for p in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if not isinstance(res, SymbolScanResult):
                    continue
                pair_counter += 1
                all_evaluations.append(res)

                if res.r1 and res.r1.passed:
                    r1_passed_count += 1
                if res.r2 and res.r2.diff_pct > 0:
                    r2_above_count += 1

                # 1D column formatting
                if res.r1 and res.r1.passed:
                    d_col = f"{Fore.GREEN}1D: PASSED{Style.RESET_ALL} (Close > 50 EMA)"
                elif res.r1:
                    d_col = f"{Fore.RED}1D: FAIL{Style.RESET_ALL} (Close < 50 EMA)"
                else:
                    d_col = f"{Fore.LIGHTBLACK_EX}1D: N/A{Style.RESET_ALL}"

                # 4H column formatting
                if res.r2 and res.r2.passed:
                    if getattr(res.r2, "is_fresh", True):
                        h4_col = f"{Fore.GREEN}4H: FRESH CROSS!{Style.RESET_ALL} ({res.r2.diff_pct:+.2f}%)"
                    else:
                        h4_col = f"{Fore.YELLOW}4H: RECENT CROSS{Style.RESET_ALL} ({res.r2.hours_ago:.0f}h ago, {res.r2.diff_pct:+.2f}%)"
                elif res.r2 and res.r2.diff_pct > 0:
                    h4_col = f"{Fore.LIGHTBLACK_EX}4H: NO CROSS{Style.RESET_ALL} (Old cross >48h, +{res.r2.diff_pct:.2f}%)"
                elif res.r2 and abs(res.r2.diff_pct) <= 0.8:
                    h4_col = f"{Fore.CYAN}4H: NEAR CROSS{Style.RESET_ALL} ({res.r2.diff_pct:+.2f}%)"
                elif res.r2:
                    h4_col = f"{Fore.LIGHTBLACK_EX}4H: NO CROSS{Style.RESET_ALL} (50 < 200 EMA, {res.r2.diff_pct:+.2f}%)"
                else:
                    h4_col = f"{Fore.LIGHTBLACK_EX}4H: N/A{Style.RESET_ALL}"

                # Status / Verdict column formatting
                if res.is_valid_signal:
                    if getattr(res.r2, "is_fresh", True):
                        verdict = f"{Fore.GREEN}🎯 [SIGNAL DETECTED - FRESH] -> ALERT DISPATCHED!{Style.RESET_ALL}"
                    else:
                        verdict = f"{Fore.YELLOW}🎯 [SIGNAL DETECTED - ⚠️ NON-FRESH ({res.r2.hours_ago:.0f}h ago)] -> ALERT DISPATCHED!{Style.RESET_ALL}"
                    detected_signals.append(res.signal)
                    logger.info(f"[{pair_counter:3d}/{total_pairs}] #{res.symbol}: SIGNAL DETECTED! Fresh: {getattr(res.r2, 'is_fresh', True)}, Price: ${res.current_price}")
                else:
                    verdict = f"{Fore.LIGHTBLACK_EX}❌ {res.rejection_reason}{Style.RESET_ALL}"

                # Single-line log per pair
                print(f"[{pair_counter:3d}/{total_pairs}] {Fore.MAGENTA}{res.symbol:12}{Style.RESET_ALL} | {d_col} | {h4_col} | {verdict}")

            # Brief pause to keep Binance rate limit very low
            await asyncio.sleep(0.05)

        elapsed = time.time() - start_time
        r1_pct = (r1_passed_count / total_pairs * 100) if total_pairs else 0
        fresh_signals = [s for s in detected_signals if getattr(s.rule2_4h, "is_fresh", True)]
        recent_signals = [s for s in detected_signals if not getattr(s.rule2_4h, "is_fresh", True)]

        print(f"\n{Fore.CYAN}{'-'*65}{Style.RESET_ALL}")
        print(f"✅ Scan completed in {Fore.GREEN}{elapsed:.2f}s{Style.RESET_ALL}. Pairs checked: {total_pairs}")
        print(f"📊 Market Conditions Breakdown:")
        print(f"  • Rule 1 (1D Candle > 50 EMA): {Fore.YELLOW}{r1_passed_count} / {total_pairs}{Style.RESET_ALL} pairs passed ({r1_pct:.1f}%)")
        print(f"  • Rule 2 (4H 50 EMA > 200 EMA): {Fore.YELLOW}{r2_above_count} / {total_pairs}{Style.RESET_ALL} pairs currently above")
        print(f"  • Rule 2 (Fresh Golden Cross): {Fore.GREEN}{len(fresh_signals)}{Style.RESET_ALL} pairs triggered in current/prev candle")
        print(f"  • Rule 2 (Recent Cross in 48h): {Fore.YELLOW}{len(recent_signals)}{Style.RESET_ALL} pairs triggered with non-fresh warning")
        print(f"Signals Found: {Fore.GREEN}{len(detected_signals)}{Style.RESET_ALL} ({len(fresh_signals)} fresh, {len(recent_signals)} recent 48h)")

        # Watchlist: Top 5 closest pairs to Golden Cross (diff_pct nearest 0%)
        closest_candidates = [
            r for r in all_evaluations
            if r.r2 is not None and not r.is_valid_signal
        ]
        closest_candidates.sort(key=lambda x: abs(x.r2.diff_pct))

        top_closest_summary = []
        if closest_candidates:
            print(f"\n🔭 {Fore.CYAN}Top 5 Pairs Closest to 4H 50/200 EMA Cross (Watchlist):{Style.RESET_ALL}")
            for c in closest_candidates[:5]:
                d_pass = bool(c.r1 and c.r1.passed)
                d_mark = f"{Fore.GREEN}1D > 50EMA: YES{Style.RESET_ALL}" if d_pass else f"{Fore.RED}1D > 50EMA: NO{Style.RESET_ALL}"
                print(f"  • {Fore.MAGENTA}#{c.symbol:12}{Style.RESET_ALL} | 4H Spread: {c.r2.diff_pct:+.2f}% | Price: ${c.current_price} | {d_mark}")
                top_closest_summary.append({
                    "symbol": c.symbol,
                    "diff": c.r2.diff_pct,
                    "price": c.current_price,
                    "1d_pass": d_pass
                })

        if detected_signals:
            for sig in detected_signals:
                vol_status = (
                    f"{Fore.GREEN}Volume PASSED (+{sig.rule3_volume.ratio_pct:.1f}%){Style.RESET_ALL}"
                    if not sig.has_volume_warning
                    else f"{Fore.YELLOW}⚠️ Volume WARNING ({sig.rule3_volume.ratio_pct:.1f}%){Style.RESET_ALL}"
                )
                print(f"  • {Fore.MAGENTA}#{sig.symbol}{Style.RESET_ALL} at ${sig.current_price} | {sig.rule2_4h.trigger_type} | {vol_status}")

                # Check cooldown before sending Telegram alert
                if self.alert_history.is_on_cooldown(sig.symbol):
                    logger.info(f"Symbol {sig.symbol} is on cooldown. Alert skipped.")
                    continue

                # Dispatch Telegram alert
                sent = await self.notifier.send_signal(sig)
                if sent:
                    self.alert_history.record_alert(sig.symbol)
                    # Add to 7-day persistent tracker
                    self.tracker.add_signal(sig.symbol, sig.current_price)
        else:
            print(f"{Fore.LIGHTBLACK_EX}No pairs currently meeting both 1D and 4H hard conditions.{Style.RESET_ALL}")

        # Update 7-Day Monitoring Tracker across all actively monitored coins
        try:
            tracked_alerts = await self.tracker.update_all_tracked(client, self.notifier)
            stats = self.tracker.get_summary_stats()
            if stats["actively_monitoring"] > 0:
                print(
                    f"{Fore.CYAN}📡 7-Day Signal Tracker:{Style.RESET_ALL} Active: {Fore.YELLOW}{stats['actively_monitoring']}{Style.RESET_ALL} coins | "
                    f"TP Hits: {Fore.GREEN}{stats['trades_hitting_tp']}{Style.RESET_ALL} | "
                    f"SL Hits: {Fore.RED}{stats['trades_hitting_sl']}{Style.RESET_ALL}"
                )
                if tracked_alerts:
                    print(f"{Fore.GREEN}🎯 Dispatched {len(tracked_alerts)} TP/SL milestone alerts!{Style.RESET_ALL}")
        except Exception as te:
            logger.error(f"Error updating 7-day tracker: {te}")

        print(f"{Fore.CYAN}{'='*65}{Style.RESET_ALL}\n")

        return {
            "total_pairs": total_pairs,
            "r1_passed": r1_passed_count,
            "r1_pct": r1_pct,
            "r2_above": r2_above_count,
            "signals_found": len(detected_signals),
            "signals_fresh": len(fresh_signals),
            "signals_recent": len(recent_signals),
            "elapsed": elapsed,
            "closest": top_closest_summary
        }

    async def start(self, scan_once: bool = False, target_symbol: Optional[str] = None):
        """
        Main execution loop.
        """
        print(f"{Fore.GREEN}===================================================={Style.RESET_ALL}")
        print(f"{Fore.GREEN}   🚀 BINANCE FUTURES 1D/4H/1H SIGNAL SCANNER BOT   {Style.RESET_ALL}")
        print(f"{Fore.GREEN}===================================================={Style.RESET_ALL}")
        print(f"• Rule 1 (Hard): 1D Candle > 50 EMA (Fresh cross required: {config.DAILY_REQUIRE_FRESH_CROSS})")
        lookback_str = f"Current/Prev candle + Last {config.CROSS_4H_LOOKBACK_HOURS}h (Non-fresh warning enabled)" if config.ALLOW_NON_FRESH_4H_CROSS else "Strictly Fresh Current/Prev Candle"
        print(f"• Rule 2 (Hard): 4H 50 EMA crossed 200 EMA ({lookback_str})")
        print(f"• Rule 3 (Soft): 1H Volume 7-bar avg > 24-bar avg (warns if not met)")
        print(f"• Telegram Alerts: {'Configured' if self.notifier.is_configured() else 'NOT Configured (set .env)'}")
        print(f"• Scan Interval: Every {config.SCAN_INTERVAL_SECONDS} seconds")
        print(f"• Alert Cooldown: {config.ALERT_COOLDOWN_MINUTES} minutes per pair")
        print(f"{Fore.GREEN}===================================================={Style.RESET_ALL}\n")

        async with BinanceFuturesClient() as client:
            if scan_once or target_symbol:
                await self.run_scan_cycle(client, target_symbol=target_symbol)
                return

            # Send Startup Notification when 24/7 bot starts
            if self.notifier.is_configured():
                logger.info("Sending bot startup notification to Telegram...")
                await self.notifier.send_startup_alert()

            # Initialize and run interactive Telegram command listener
            self.commander = TelegramCommander(
                self.notifier,
                self.tracker,
                scan_callback=lambda: self.run_scan_cycle(client)
            )
            listener_task = asyncio.create_task(self.commander.listen_for_commands(client))

            try:
                while True:
                    try:
                        await self.run_scan_cycle(client)
                    except Exception as e:
                        logger.error(f"Error during scan cycle: {e}", exc_info=True)

                    logger.info(f"Sleeping for {config.SCAN_INTERVAL_SECONDS}s until next scan...")
                    await asyncio.sleep(config.SCAN_INTERVAL_SECONDS)
            finally:
                listener_task.cancel()


def main():
    parser = argparse.ArgumentParser(description="Binance Futures Telegram Signal Scanner Bot")
    parser.add_argument("--scan-once", action="store_true", help="Run a single scan cycle across all pairs and exit")
    parser.add_argument("--symbol", type=str, default=None, help="Scan a specific symbol (e.g. BTCUSDT or NIGHTUSDT)")
    parser.add_argument("--test-telegram", action="store_true", help="Send a test message to Telegram to verify setup")
    parser.add_argument("--simulate-signal", type=str, default=None, nargs="?", const="NIGHTUSDT",
                        help="Simulate an alert on a symbol (default: NIGHTUSDT) to preview Telegram formatting")
    args = parser.parse_args()

    if args.test_telegram:
        notifier = TelegramNotifier()
        if not notifier.is_configured():
            print(f"{Fore.RED}Error: Telegram is not configured in .env file!{Style.RESET_ALL}")
            print("Please set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")
            sys.exit(1)
        print(f"Sending test message to Telegram chat {notifier.chat_id}...")
        success = asyncio.run(notifier.send_test_message())
        if success:
            print(f"{Fore.GREEN}Success! Test message delivered to your Telegram chat.{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Failed to send message. Please verify your bot token and chat ID.{Style.RESET_ALL}")
        return

    if args.simulate_signal:
        sym = args.simulate_signal.upper()
        print(f"Generating simulated signal for {sym} to preview formatting and Telegram delivery...")
        async def simulate():
            async with BinanceFuturesClient() as client:
                k4h = await client.get_klines(sym, "4h", limit=400)
                k1d = await client.get_klines(sym, "1d", limit=120)
                k1h = await client.get_klines(sym, "1h", limit=35)
                if not k4h or not k1d or not k1h:
                    print(f"{Fore.RED}Error: Could not retrieve market data for {sym}{Style.RESET_ALL}")
                    return

                r1 = evaluate_rule_1_daily(k1d)
                r2 = evaluate_rule_2_4h_cross(k4h)
                r3 = evaluate_rule_3_volume(k1h)

                # For simulation, ensure Rule 1 & Rule 2 are visually active
                from strategy import Rule24HResult, Rule1DailyResult
                sim_r1 = Rule1DailyResult(
                    passed=True,
                    current_close=k1d[-1]["close"],
                    ema50=r1.ema50,
                    fresh_cross=True,
                    details=r1.details
                )
                sim_r2 = Rule24HResult(
                    passed=True,
                    trigger_type="Current 4H Candle (In Progress)",
                    ema50=r2.ema50 if r2.ema50 > 0 else k4h[-1]["close"] * 1.01,
                    ema200=r2.ema200 if r2.ema200 > 0 else k4h[-1]["close"] * 0.99,
                    diff_pct=0.45,
                    details="Simulated 4H 50 EMA golden cross above 200 EMA"
                )

                signal = SignalResult(
                    symbol=sym,
                    is_valid_signal=True,
                    current_price=k4h[-1]["close"],
                    rule1_daily=sim_r1,
                    rule2_4h=sim_r2,
                    rule3_volume=r3,
                    has_volume_warning=not r3.passed,
                    candle_time=k4h[-1]["open_time"]
                )

                notifier = TelegramNotifier()
                formatted = notifier.format_signal_message(signal)
                print(f"\n{Fore.CYAN}--- MESSAGE PREVIEW ---{Style.RESET_ALL}\n")
                # Remove HTML tags for terminal print
                import re
                clean_text = re.sub('<[^<]+?>', '', formatted)
                print(clean_text)
                print(f"\n{Fore.CYAN}-----------------------{Style.RESET_ALL}\n")

                if notifier.is_configured():
                    print("Sending simulated alert to Telegram...")
                    sent = await notifier.send_signal(signal)
                    if sent:
                        print(f"{Fore.GREEN}✅ Alert delivered to Telegram successfully!{Style.RESET_ALL}")
                else:
                    print(f"{Fore.YELLOW}Telegram not configured in .env. Above preview shows how it will appear.{Style.RESET_ALL}")

        asyncio.run(simulate())
        return

    bot = BinanceScannerBot()
    try:
        asyncio.run(bot.start(scan_once=args.scan_once, target_symbol=args.symbol))
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Bot stopped by user. Goodbye!{Style.RESET_ALL}")


if __name__ == "__main__":
    main()
