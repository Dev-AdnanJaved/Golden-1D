import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional, Callable
import aiohttp
import pandas as pd

from config import config
from signal_tracker import SignalTracker

logger = logging.getLogger("TelegramCommander")

class TelegramCommander:
    """
    Listens for interactive Telegram commands (/status, /tracked, /stats, /btc, /scan, /help)
    and dispatches on-demand responses back to the user.
    """
    def __init__(self, notifier, tracker: SignalTracker, scan_callback: Optional[Callable] = None):
        self.notifier = notifier
        self.tracker = tracker
        self.scan_callback = scan_callback
        self.token = config.TELEGRAM_BOT_TOKEN
        self.chat_id = str(config.TELEGRAM_CHAT_ID)
        self.last_update_id = 0
        self.start_time = time.time()
        self.is_scanning = False

    def get_uptime_str(self) -> str:
        elapsed = int(time.time() - self.start_time)
        days = elapsed // 86400
        hours = (elapsed % 86400) // 3600
        mins = (elapsed % 3600) // 60
        secs = elapsed % 60
        parts = []
        if days > 0: parts.append(f"{days}d")
        if hours > 0: parts.append(f"{hours}h")
        if mins > 0: parts.append(f"{mins}m")
        parts.append(f"{secs}s")
        return " ".join(parts)

    async def handle_command(self, cmd: str, client) -> Optional[str]:
        cmd = cmd.strip().lower().split("@")[0] # strip @botname if in group

        # 1. /start or /help
        if cmd in ("/start", "/help"):
            return (
                f"🤖 <b>BINANCE FUTURES SCANNER BOT - COMMAND MENU</b> 🤖\n\n"
                f"Use these commands anytime to monitor and control your scanner:\n\n"
                f"🚀 <code>/running</code> - View all running signals with live PnL, peak, drawdown & hours\n"
                f"🪙 <code>/coin SYMBOL</code> - Full 7-day stats or live analysis of any coin (e.g. <code>/coin NEAR</code>)\n"
                f"📊 <code>/status</code> - Bot health, uptime, settings & scan info\n"
                f"📡 <code>/tracked</code> - Same as /running (active monitored signals)\n"
                f"📈 <code>/stats</code> - Win rate, TP milestone hits & SL summary\n"
                f"₿ <code>/btc</code> - Real-time Bitcoin 1D & 4H trend health\n"
                f"⚡ <code>/scan</code> - Trigger an immediate manual scan right now\n"
                f"ℹ️ <code>/help</code> - Show this menu"
            )

        # 2. /status
        elif cmd == "/status":
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            uptime = self.get_uptime_str()
            stats = self.tracker.get_summary_stats()
            btc_shield = "🟢 <b>ACTIVE</b>" if config.ENABLE_BTC_TREND_FILTER else "⚪ <b>DISABLED</b>"

            return (
                f"⚙️ <b>BOT OPERATIONAL STATUS</b> ⚙️\n\n"
                f"🟢 <b>Status:</b> Running 24/7 (Online)\n"
                f"⏳ <b>Uptime:</b> <code>{uptime}</code>\n"
                f"⏰ <b>Server Time:</b> <code>{now_str}</code>\n"
                f"📡 <b>Scan Cycle:</b> Every <code>{config.SCAN_INTERVAL_SECONDS}s</code>\n"
                f"🛡️ <b>BTC Trend Filter:</b> {btc_shield}\n"
                f"💰 <b>Pair Filters:</b> Vol &gt; ${config.MIN_24H_VOLUME_USDT/1e6:.1f}M | MCap &le; ${config.MAX_MARKET_CAP_USDT/1e6:.0f}M\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"📡 <b>7-Day Signal Tracker:</b>\n"
                f"  • Actively Monitoring: <b>{stats['actively_monitoring']} coins</b>\n"
                f"  • Total Historical Tracked: <b>{stats['total_tracked']}</b>\n"
                f"  • Completed 7-Day Window: <b>{stats['completed_7_days']}</b>"
            )

        # 3. /running, /tracked, /signals
        elif cmd in ("/running", "/tracked", "/signals"):
            active = [s for s in self.tracker.signals.values() if s.status == "MONITORING"]
            stats = self.tracker.get_summary_stats()
            now = time.time()

            def fmt_p(val):
                return f"{val:,.2f}" if val >= 100 else (f"{val:.4f}" if val >= 1 else f"{val:.6f}")

            if not active:
                return (
                    f"🚀 <b>RUNNING SIGNALS PORTFOLIO AUDIT</b> 🚀\n\n"
                    f"ℹ️ <b>No signals currently running.</b>\n\n"
                    f"• Total Signals Generated (Historical): <b>{stats['total_tracked']}</b>\n"
                    f"• Completed 7-Day Cycles: <b>{stats['completed_7_days']}</b>\n"
                    f"• Trades Reaching TP Targets: <b>{stats['trades_hitting_tp']}</b>\n"
                    f"• Trades Triggering SL: <b>{stats['trades_hitting_sl']}</b>\n\n"
                    f"💡 <i>Why no signals in last 12h?</i>\n"
                    f"4H Golden Crosses are high-timeframe swing setups (~4-5 formed per day across 346 pairs). When the market pulls back or consolidates, quiet periods of 12-24h with 0 crosses are normal.\n\n"
                    f"👉 Type <code>/btc</code> to check Bitcoin's trend health or <code>/scan</code> to force a manual scan now!"
                )

            lines = [
                f"🚀 <b>RUNNING SIGNALS PORTFOLIO AUDIT</b> 🚀\n"
                f"Actively Running: <b>{len(active)}</b> | Total Generated: <b>{stats['total_tracked']}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
            ]

            for s in sorted(active, key=lambda x: x.entry_time, reverse=True):
                running_secs = max(0.0, now - s.entry_time)
                running_h = running_secs / 3600.0
                running_d = running_h / 24.0

                rem_secs = max(0.0, s.monitored_until - now)
                rem_h = rem_secs / 3600.0
                rem_d = rem_h / 24.0

                cur_p = s.current_price
                entry_p = s.entry_price
                cur_gain = ((cur_p - entry_p) / entry_p) * 100.0 if entry_p > 0 else 0.0
                gain_color = "🟢" if cur_gain >= 0 else "🔴"

                tps = ", ".join([f"+{t}%" for t in s.tps_hit]) if s.tps_hit else "None yet"
                sl_badge = f"🔴 <b>SL Hit</b> ({s.sl_reason})" if s.sl_hit else "🟢 <b>SL Safe</b>"

                lines.append(
                    f"💎 <b>#{s.symbol}</b>\n"
                    f"  • <b>Running Time:</b> <code>{running_h:.1f} hours ({running_d:.1f} days)</code>\n"
                    f"  • <b>Entry:</b> <code>${fmt_p(entry_p)}</code> (at {s.entry_date})\n"
                    f"  • <b>Current Price:</b> <code>${fmt_p(cur_p)}</code> ({gain_color} <b>{cur_gain:+.2f}%</b>)\n"
                    f"  • <b>Max Peak Gain:</b> <code>+{s.max_gain_pct:.2f}%</code> 🚀\n"
                    f"  • <b>Max Drawdown:</b> <code>{s.max_loss_pct:.2f}%</code> 📉\n"
                    f"  • <b>Milestone TPs Hit:</b> <code>{tps}</code>\n"
                    f"  • <b>Stop Loss Status:</b> {sl_badge}\n"
                    f"  • <b>7-Day Remaining:</b> <code>{rem_d:.1f} days ({rem_h:.0f}h)</code>\n"
                    f"  📈 <a href=\"https://www.binance.com/en/futures/{s.symbol}\">View Chart</a>\n"
                )

            return "\n".join(lines)

        # 4. /stats
        elif cmd == "/stats":
            stats = self.tracker.get_summary_stats()
            total = stats["total_tracked"]
            if total == 0:
                return "📊 <b>TRACKER STATISTICS</b> 📊\n\n<i>No trades recorded yet. Stats will compile as signals are generated.</i>"

            wr = (stats["trades_hitting_tp"] / total) * 100.0 if total > 0 else 0.0
            return (
                f"📊 <b>SIGNAL TRACKER PERFORMANCE STATS</b> 📊\n\n"
                f"🎯 <b>Win Rate (Hitting ≥ +10% TP):</b> <b>{wr:.1f}%</b>\n"
                f"📈 <b>Trades Achieving Targets:</b> <code>{stats['trades_hitting_tp']} / {total}</code>\n"
                f"🛑 <b>Trades Hitting Stop Loss:</b> <code>{stats['trades_hitting_sl']} / {total}</code>\n"
                f"📡 <b>Actively Monitored Now:</b> <code>{stats['actively_monitoring']}</code>\n"
                f"🏁 <b>Completed 7-Day Cycle:</b> <code>{stats['completed_7_days']}</code>\n\n"
                f"<i>Full historical event audits are saved in tracked_signals.json</i>"
            )

        # 5. /btc
        elif cmd == "/btc":
            try:
                # 4H BTC
                kl_4h = await client.get_klines("BTCUSDT", interval="4h", limit=120)
                # 1D BTC
                kl_1d = await client.get_klines("BTCUSDT", interval="1d", limit=120)

                if not kl_4h or not kl_1d:
                    return "⚠️ Could not retrieve live Bitcoin data from Binance."

                c_4h = pd.Series([float(k["close"]) for k in kl_4h])
                ema50_4h = c_4h.ewm(span=50, adjust=False).mean().iloc[-1]
                cur_btc = c_4h.iloc[-1]

                c_1d = pd.Series([float(k["close"]) for k in kl_1d])
                ema50_1d = c_1d.ewm(span=50, adjust=False).mean().iloc[-1]

                b_4h = cur_btc > ema50_4h
                b_1d = cur_btc > ema50_1d

                badge_4h = "🟢 <b>BULLISH (Above 50 EMA)</b>" if b_4h else "🔴 <b>BEARISH (Below 50 EMA)</b>"
                badge_1d = "🟢 <b>BULLISH (Macro Uptrend)</b>" if b_1d else "🔴 <b>BEARISH (Macro Downtrend)</b>"

                trend_status = "🟢 <b>TRADING FULLY ACTIVE</b> (High Conviction)" if (b_4h and b_1d) else (
                    "🟡 <b>CAUTION / PULLBACK</b>" if b_4h else "🛡️ <b>BEAR REGIME - CASH SHIELD ACTIVE</b>"
                )

                return (
                    f"₿ <b>BITCOIN REAL-TIME TREND HEALTH</b> ₿\n\n"
                    f"💰 <b>Current Price:</b> <code>${cur_btc:,.2f}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 <b>1D Daily 50 EMA:</b> <code>${ema50_1d:,.2f}</code>\n"
                    f"   • Status: {badge_1d}\n\n"
                    f"📊 <b>4H 50 EMA:</b> <code>${ema50_4h:,.2f}</code>\n"
                    f"   • Status: {badge_4h}\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🛡️ <b>Bot Action:</b> {trend_status}"
                )
            except Exception as e:
                return f"⚠️ Error evaluating Bitcoin trend: {e}"

        # 6. /scan (Manual trigger)
        elif cmd == "/scan":
            if self.is_scanning:
                return "⏳ A scan cycle is already in progress. Please wait a moment."
            
            if self.scan_callback:
                asyncio.create_task(self._trigger_manual_scan())
                return "⚡ <b>Manual scan initiated!</b> Scanning all qualifying Binance Futures pairs now... You will receive an update once complete."
            else:
                return "⚠️ Manual scan trigger not attached."

        # 7. /coin <SYMBOL> or /check <SYMBOL> or /<SYMBOL>
        else:
            target_sym = None
            if cmd.startswith("/coin ") or cmd.startswith("/check ") or cmd.startswith("/info "):
                parts = cmd.split()
                if len(parts) > 1:
                    target_sym = parts[1].strip()
            elif cmd.startswith("/") and len(cmd) > 1:
                cand = cmd[1:].strip()
                # Accept tickers like /near, /sol, /btc, /1000pepe, /doge
                if cand.replace("1000", "").isalnum():
                    target_sym = cand

            if target_sym:
                target_sym = target_sym.upper()
                if not target_sym.endswith("USDT"):
                    target_sym += "USDT"

                def fmt_p(val):
                    return f"{val:,.2f}" if val >= 100 else (f"{val:.4f}" if val >= 1 else f"{val:.6f}")

                # 1. Search in 7-day signal tracker
                matching = [s for s in self.tracker.signals.values() if s.symbol == target_sym]
                if matching:
                    sig = sorted(matching, key=lambda x: x.entry_time)[-1]
                    now = time.time()
                    cur_price = sig.current_price
                    entry = sig.entry_price
                    cur_gain = ((cur_price - entry) / entry) * 100.0 if entry > 0 else 0.0

                    rem_h = max(0.0, (sig.monitored_until - now) / 3600.0)
                    rem_d = rem_h / 24.0
                    time_info = f"<code>{rem_d:.1f} days ({rem_h:.0f}h)</code> remaining" if sig.status == "MONITORING" else f"Completed on {sig.completed_date}"

                    tps = ", ".join([f"+{t}%" for t in sig.tps_hit]) if sig.tps_hit else "None reached yet"
                    sl_str = f"🔴 <b>HIT</b> ({sig.sl_reason})" if sig.sl_hit else "🟢 <b>SAFE (Not Triggered)</b>"
                    status_icon = "🟢 <b>ACTIVELY MONITORING</b>" if sig.status == "MONITORING" else "🏁 <b>7-DAY TRACKING COMPLETED</b>"

                    events_str = ""
                    if sig.events_log:
                        recent_events = sig.events_log[-4:]
                        ev_lines = [f"• <i>{ev.get('time','')}</i>: {ev.get('message','')}" for ev in recent_events]
                        events_str = "\n\n📜 <b>RECENT EVENTS:</b>\n" + "\n".join(ev_lines)

                    return (
                        f"💎 <b>7-DAY SIGNAL REPORT: #{sig.symbol}</b> 💎\n\n"
                        f"📊 <b>Status:</b> {status_icon}\n"
                        f"⏰ <b>Entry Time:</b> <code>{sig.entry_date}</code>\n"
                        f"💰 <b>Entry Price:</b> <code>${fmt_p(entry)}</code>\n"
                        f"📈 <b>Current Price:</b> <code>${fmt_p(cur_price)}</code> (<b>{cur_gain:+.2f}%</b>)\n"
                        f"🚀 <b>Max Gain Achieved:</b> <code>+{sig.max_gain_pct:.2f}%</code>\n"
                        f"📉 <b>Max Drawdown:</b> <code>{sig.max_loss_pct:.2f}%</code>\n"
                        f"🎯 <b>Milestones Reached:</b> <code>{tps}</code>\n"
                        f"🛑 <b>Stop Loss Status:</b> {sl_str}\n"
                        f"⏱️ <b>7-Day Window:</b> {time_info}"
                        f"{events_str}\n\n"
                        f"📈 <a href=\"https://www.binance.com/en/futures/{sig.symbol}\">Open on Binance Futures</a>"
                    )
                else:
                    # 2. Live technical analysis on unmonitored coin
                    try:
                        kl_4h = await client.get_klines(target_sym, interval="4h", limit=250)
                        kl_1d = await client.get_klines(target_sym, interval="1d", limit=120)
                        if not kl_4h or not kl_1d:
                            return f"⚠️ Could not find active futures pair <code>#{target_sym}</code> on Binance."

                        cur_price = float(kl_4h[-1]["close"])
                        c_4h = pd.Series([float(k["close"]) for k in kl_4h])
                        ema50_4h = c_4h.ewm(span=50, adjust=False).mean().iloc[-1]
                        ema200_4h = c_4h.ewm(span=200, adjust=False).mean().iloc[-1]

                        c_1d = pd.Series([float(k["close"]) for k in kl_1d])
                        ema50_1d = c_1d.ewm(span=50, adjust=False).mean().iloc[-1]
                        last_1d_close = c_1d.iloc[-1]

                        r1_pass = last_1d_close > ema50_1d
                        r2_pass = ema50_4h > ema200_4h
                        spread = ((ema50_4h - ema200_4h) / ema200_4h) * 100.0

                        r1_badge = "✅ <b>PASSED (Above 50 EMA)</b>" if r1_pass else "❌ <b>FAILED (Below 50 EMA)</b>"
                        r2_badge = f"✅ <b>BULLISH (+{spread:.2f}%)</b>" if r2_pass else f"❌ <b>BEARISH ({spread:.2f}%)</b>"
                        verdict = "🔥 <b>GOLDEN ALIGNMENT ACTIVE</b>" if (r1_pass and r2_pass) else "⏳ <b>WAITING FOR ALIGNMENT</b>"

                        return (
                            f"🔍 <b>LIVE ANALYSIS: #{target_sym}</b> 🔍\n\n"
                            f"💰 <b>Current Price:</b> <code>${fmt_p(cur_price)}</code>\n"
                            f"━━━━━━━━━━━━━━━━━━━━━\n"
                            f"1️⃣ <b>1D Macro vs 50 EMA:</b> {r1_badge}\n"
                            f"   • 1D Close: <code>${fmt_p(last_1d_close)}</code> | 50 EMA: <code>${fmt_p(ema50_1d)}</code>\n\n"
                            f"2️⃣ <b>4H 50/200 EMA Spread:</b> {r2_badge}\n"
                            f"   • 4H 50 EMA: <code>${fmt_p(ema50_4h)}</code>\n"
                            f"   • 4H 200 EMA: <code>${fmt_p(ema200_4h)}</code>\n\n"
                            f"━━━━━━━━━━━━━━━━━━━━━\n"
                            f"📋 <b>Verdict:</b> {verdict}\n"
                            f"<i>Note: No active signal in 7-day tracker for this coin.</i>"
                        )
                    except Exception as e:
                        return f"⚠️ Error analyzing #{target_sym}: {e}"

        return None

    async def _trigger_manual_scan(self):
        self.is_scanning = True
        try:
            if self.scan_callback:
                summary = await self.scan_callback()
                if summary and self.notifier.is_configured():
                    elapsed = summary.get("elapsed", 0)
                    total = summary.get("total_pairs", 0)
                    signals = summary.get("signals_found", 0)
                    fresh_cnt = summary.get("signals_fresh", 0)
                    recent_cnt = summary.get("signals_recent", 0)
                    r1_pass = summary.get("r1_passed", 0)
                    r1_pct = summary.get("r1_pct", 0)
                    r2_above = summary.get("r2_above", 0)

                    count_breakdown = f" ({fresh_cnt} fresh, {recent_cnt} recent 48h)" if recent_cnt > 0 else ""
                    msg = (
                        f"⚡ <b>MANUAL SCAN COMPLETED</b> (in {elapsed:.1f}s)\n\n"
                        f"Pairs Checked: <b>{total}</b>\n"
                        f"Signals Found: <b>{signals}</b>{count_breakdown}\n\n"
                        f"📊 <b>Market Conditions Breakdown:</b>\n"
                        f"• 1D Candle &gt; 50 EMA: <b>{r1_pass} / {total}</b> ({r1_pct:.1f}%)\n"
                        f"• 4H EMA 50 &gt; 200 EMA: <b>{r2_above}</b> (Total above)\n"
                        f"• 4H Fresh Golden Cross: <b>{fresh_cnt}</b>\n"
                        f"• 4H Recent Cross (&le;48h): <b>{recent_cnt}</b>\n"
                    )

                    top_closest = summary.get("closest", [])
                    if top_closest:
                        msg += "\n🔭 <b>Watchlist - Closest to 4H Cross:</b>\n"
                        for c in top_closest[:5]:
                            d_icon = "🟢 1D &gt; 50EMA" if c.get("1d_pass") else "🔴 1D &le; 50EMA"
                            msg += f"• <b>#{c['symbol']}</b>: Spread <code>{c['diff']:+.2f}%</code> | {d_icon}\n"

                    await self.notifier.send_message(msg)
        except Exception as e:
            logger.error(f"Manual scan error: {e}")
        finally:
            self.is_scanning = False

    async def listen_for_commands(self, client):
        """
        Background polling task using Telegram Bot API getUpdates.
        """
        if not self.token or not self.chat_id:
            logger.warning("Telegram Commander disabled (token or chat_id missing).")
            return

        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        logger.info("Telegram interactive command listener started (/status, /tracked, /stats, /btc, /scan, /help).")

        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    params = {
                        "offset": self.last_update_id + 1,
                        "timeout": 20
                    }
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=25)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            results = data.get("result", [])
                            for update in results:
                                self.last_update_id = update["update_id"]
                                msg = update.get("message", {})
                                text = msg.get("text", "")
                                sender_chat_id = str(msg.get("chat", {}).get("id", ""))

                                # Only respond to authorized chat_id for security
                                if sender_chat_id == self.chat_id and text.startswith("/"):
                                    logger.info(f"Received Telegram command: {text}")
                                    reply = await self.handle_command(text, client)
                                    if reply:
                                        await self.notifier.send_message(reply)
                        elif resp.status == 409:
                            # Another polling instance is running
                            await asyncio.sleep(5)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.debug(f"Error in command listener: {e}")
                    await asyncio.sleep(3)

                await asyncio.sleep(1)

