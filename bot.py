import asyncio
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from colorama import Fore, Style, init
from config import config
from binance_client import BinanceFuturesClient
from strategy import (
    evaluate_rule_2_4h_cross,
    evaluate_rule_1_daily,
    evaluate_rule_3_volume,
    SignalResult
)
from telegram_notifier import TelegramNotifier
from signal_tracker import SignalTracker

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
    ) -> Optional[SignalResult]:
        """
        Evaluate a single symbol step-by-step:
        1. 4H EMA cross (Rule 2) - checked first for efficiency
        2. 1D EMA 50 (Rule 1)
        3. 1H Volume (Rule 3)
        """
        # Step 1: 4H Kline
        klines_4h = await client.get_klines(symbol, interval="4h", limit=400)
        if not klines_4h:
            return None

        r2 = evaluate_rule_2_4h_cross(klines_4h)
        if not r2.passed:
            return None

        logger.info(f"{Fore.GREEN}[4H CROSS MATCH]{Style.RESET_ALL} {symbol}: 50 EMA crossed 200 EMA ({r2.trigger_type})")

        # Step 2: 1D Kline
        klines_1d = await client.get_klines(symbol, interval="1d", limit=120)
        if not klines_1d:
            return None

        r1 = evaluate_rule_1_daily(klines_1d)
        if not r1.passed:
            logger.info(f"{Fore.YELLOW}[1D FAILED]{Style.RESET_ALL} {symbol}: 1D candle not above 50 EMA")
            return None

        logger.info(f"{Fore.GREEN}[1D PASSED]{Style.RESET_ALL} {symbol}: 1D candle above 50 EMA")

        # Step 3: 1H Kline (Soft Volume Filter)
        klines_1h = await client.get_klines(symbol, interval="1h", limit=35)
        if not klines_1h:
            return None

        r3 = evaluate_rule_3_volume(klines_1h)

        current_price = klines_4h[-1]["close"]
        candle_time = klines_4h[-1]["open_time"]

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
            mcap=mcap
        )
        return signal

    async def run_scan_cycle(self, client: BinanceFuturesClient, target_symbol: Optional[str] = None):
        """
        Execute filtered scan across active Binance Futures USDT pairs.
        Filters: 24h Vol >= $1M USDT and Market Cap <= $500M USDT
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
            return

        total_pairs = len(target_list)
        print(f"Scanning {Fore.YELLOW}{total_pairs}{Style.RESET_ALL} qualifying pairs for 4H 50/200 EMA cross...")

        # Process in batches with high concurrency
        batch_size = 35
        detected_signals: List[SignalResult] = []

        for i in range(0, total_pairs, batch_size):
            batch = target_list[i:i + batch_size]
            tasks = [self.scan_single_symbol(client, p["symbol"], p.get("vol_24h", 0.0), p.get("mcap")) for p in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, SignalResult) and res.is_valid_signal:
                    detected_signals.append(res)

            # Brief pause to keep Binance rate limit very low
            await asyncio.sleep(0.1)

        elapsed = time.time() - start_time
        print(f"\n{Fore.CYAN}{'-'*65}{Style.RESET_ALL}")
        print(f"✅ Scan completed in {Fore.GREEN}{elapsed:.2f}s{Style.RESET_ALL}. Pairs checked: {total_pairs}")
        print(f"Signals Found: {Fore.GREEN}{len(detected_signals)}{Style.RESET_ALL}")

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

    async def start(self, scan_once: bool = False, target_symbol: Optional[str] = None):
        """
        Main execution loop.
        """
        print(f"{Fore.GREEN}===================================================={Style.RESET_ALL}")
        print(f"{Fore.GREEN}   🚀 BINANCE FUTURES 1D/4H/1H SIGNAL SCANNER BOT   {Style.RESET_ALL}")
        print(f"{Fore.GREEN}===================================================={Style.RESET_ALL}")
        print(f"• Rule 1 (Hard): 1D Candle > 50 EMA (Fresh cross required: {config.DAILY_REQUIRE_FRESH_CROSS})")
        print(f"• Rule 2 (Hard): 4H 50 EMA crossed 200 EMA in current candle")
        print(f"• Rule 3 (Soft): 1H Volume 7-bar avg > 24-bar avg (warns if not met)")
        print(f"• Telegram Alerts: {'Configured' if self.notifier.is_configured() else 'NOT Configured (set .env)'}")
        print(f"• Scan Interval: Every {config.SCAN_INTERVAL_SECONDS} seconds")
        print(f"• Alert Cooldown: {config.ALERT_COOLDOWN_MINUTES} minutes per pair")
        print(f"{Fore.GREEN}===================================================={Style.RESET_ALL}\n")

        async with BinanceFuturesClient() as client:
            if scan_once or target_symbol:
                await self.run_scan_cycle(client, target_symbol=target_symbol)
                return

            while True:
                try:
                    await self.run_scan_cycle(client)
                except Exception as e:
                    logger.error(f"Error during scan cycle: {e}", exc_info=True)

                logger.info(f"Sleeping for {config.SCAN_INTERVAL_SECONDS}s until next scan...")
                await asyncio.sleep(config.SCAN_INTERVAL_SECONDS)


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
