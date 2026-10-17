import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional
import aiohttp
from config import config
from strategy import SignalResult

logger = logging.getLogger("TelegramNotifier")

class TelegramNotifier:
    """
    Asynchronous Telegram notification client using Telegram Bot API.
    """
    def __init__(self, token: Optional[str] = None, chat_id: Optional[str] = None):
        self.token = token or config.TELEGRAM_BOT_TOKEN
        self.chat_id = chat_id or config.TELEGRAM_CHAT_ID
        self.api_url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    def is_configured(self) -> bool:
        if not self.token or not self.chat_id:
            return False
        if "YOUR_TELEGRAM_BOT_TOKEN" in self.token or "YOUR_TELEGRAM_CHAT_ID" in self.chat_id:
            return False
        return True

    def format_signal_message(self, signal: SignalResult) -> str:
        """
        Format a professional HTML Telegram alert card.
        """
        sym = signal.symbol
        price = signal.current_price
        r1 = signal.rule1_daily
        r2 = signal.rule2_4h
        r3 = signal.rule3_volume

        time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        # Format price with appropriate decimals
        if price >= 100:
            price_str = f"{price:,.2f}"
        elif price >= 1:
            price_str = f"{price:.4f}"
        else:
            price_str = f"{price:.6f}"

        # Volume status badge
        if not signal.has_volume_warning:
            vol_badge = "✅ <b>PASSED</b>"
            vol_note = f"   • Spike: <b>+{r3.ratio_pct:.1f}%</b> above 24h baseline"
        else:
            vol_badge = "⚠️ <b>WARNING (SOFT FILTER FAILED)</b>"
            vol_note = (
                f"   • Difference: <b>{r3.ratio_pct:.1f}%</b> vs 24h average\n"
                f"   ⚠️ <i>Caution: 1H Volume condition not met! Trade with extra risk management.</i>"
            )

        # Fresh cross badge for 1D
        fresh_badge = " (Fresh Cross 🔥)" if r1.fresh_cross else " (Holding Above 50 EMA)"

        # Calculate Champion 3-Tier Execution Plan
        sl_price = price * 0.88  # -12% Emergency Circuit Breaker
        tp1_price = price * 1.10  # +10% Target
        tp2_price = price * 1.25  # +25% Target

        def fmt_p(val):
            return f"{val:,.2f}" if val >= 100 else (f"{val:.4f}" if val >= 1 else f"{val:.6f}")

        # Market Cap and Volume string
        mcap_str = f"${signal.mcap/1e6:.1f}M" if signal.mcap else "Micro / Under $50M"
        vol24_str = f"${signal.vol_24h/1e6:.1f}M USDT" if signal.vol_24h else "N/A"

        msg = (
            f"🚀 <b>BINANCE FUTURES SIGNAL: BUY / LONG</b> 🚀\n\n"
            f"💎 <b>Pair:</b> <code>#{sym}</code>\n"
            f"💰 <b>Entry Price:</b> <code>${price_str}</code>\n"
            f"📊 <b>24h Vol:</b> <code>{vol24_str}</code> | <b>MCap:</b> <code>{mcap_str}</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>OPTIMAL EXECUTION PLAN (Champion Setup):</b>\n"
            f"🛑 <b>Stop Loss:</b> 4H Close below 200 EMA (<code>${fmt_p(r2.ema200)}</code>) | Max: <code>${fmt_p(sl_price)}</code>\n"
            f"🎯 <b>TP1 (+10%):</b> <code>${fmt_p(tp1_price)}</code> <i>(Sell 40%, Move SL to Entry $0.0)</i>\n"
            f"🎯 <b>TP2 (+25%):</b> <code>${fmt_p(tp2_price)}</code> <i>(Sell 30%, Trail SL to +10%)</i>\n"
            f"🏃 <b>Runner (30%):</b> <i>Trail with 4H 50 EMA for monster pump</i>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 <b>RULES VERIFICATION:</b>\n\n"
            f"1️⃣ <b>1D Candle vs 50 EMA:</b> ✅ <b>PASSED</b>\n"
            f"   • 1D Close: <code>{r1.current_close}</code>\n"
            f"   • 1D 50 EMA: <code>{r1.ema50:.4f}</code>{fresh_badge}\n\n"
            f"2️⃣ <b>4H 50/200 EMA Cross:</b> ✅ <b>PASSED</b>\n"
            f"   • Trigger: <b>{r2.trigger_type}</b>\n"
            f"   • 4H 50 EMA: <code>{r2.ema50:.4f}</code>\n"
            f"   • 4H 200 EMA: <code>{r2.ema200:.4f}</code>\n"
            f"   • Bullish Spread: <code>{r2.diff_pct:+.3f}%</code>\n\n"
            f"3️⃣ <b>1H Volume Filter (Soft):</b> {vol_badge}\n"
            f"   • 7-Bar 1H Avg: <code>{r3.avg_vol_7:,.1f}</code>\n"
            f"   • 24-Bar 1H Avg: <code>{r3.avg_vol_24:,.1f}</code>\n"
            f"{vol_note}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏰ <i>Time: {time_str}</i>\n"
            f"📈 <a href=\"https://www.binance.com/en/futures/{sym}\">Open {sym} on Binance Futures</a>"
        )
        return msg

    async def send_message(self, text: str, retries: int = 3) -> bool:
        """
        Send message to Telegram chat via Bot API.
        """
        if not self.is_configured():
            logger.warning("Telegram Bot Token or Chat ID not configured. Message skipped.")
            return False

        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        }

        async with aiohttp.ClientSession() as session:
            for attempt in range(1, retries + 1):
                try:
                    async with session.post(self.api_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        resp_data = await resp.json()
                        if resp.status == 200 and resp_data.get("ok"):
                            logger.info(f"Telegram notification sent successfully to chat {self.chat_id}.")
                            return True
                        else:
                            desc = resp_data.get("description", "Unknown error")
                            logger.error(f"Telegram API error (attempt {attempt}/{retries}): {desc}")
                except Exception as e:
                    logger.error(f"Error sending Telegram message (attempt {attempt}/{retries}): {e}")
                
                if attempt < retries:
                    await asyncio.sleep(2)

        return False

    async def send_signal(self, signal: SignalResult) -> bool:
        """Format and dispatch signal alert."""
        text = self.format_signal_message(signal)
        return await self.send_message(text)

    async def send_test_message(self) -> bool:
        """Send a test message to verify Telegram bot setup."""
        time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        test_msg = (
            f"🤖 <b>Binance Futures Scanner Bot</b>\n\n"
            f"✅ <b>Telegram Connection Successful!</b>\n\n"
            f"• <b>Status:</b> Online & Monitoring\n"
            f"• <b>Time:</b> {time_str}\n"
            f"• <b>Hard Rules:</b> 1D > 50 EMA & 4H 50/200 EMA Cross\n"
            f"• <b>Soft Rule:</b> 1H 7-bar vs 24-bar Volume\n\n"
            f"<i>You will receive real-time alerts whenever a pair matches your rules!</i>"
        )
        return await self.send_message(test_msg)

    async def send_tp_hit_alert(
        self,
        symbol: str,
        entry_price: float,
        current_price: float,
        target_pct: int,
        max_gain_pct: float,
        elapsed_hours: float
    ) -> bool:
        """Format and dispatch milestone Take Profit alert."""
        time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        def fmt_p(val):
            return f"{val:,.2f}" if val >= 100 else (f"{val:.4f}" if val >= 1 else f"{val:.6f}")

        if target_pct >= 100:
            badge = f"🚀🚀🚀 <b>TARGET ACHIEVED: +{target_pct}% EXPLOSION!</b> 🚀🚀🚀"
        elif target_pct >= 50:
            badge = f"🔥 <b>TARGET ACHIEVED: +{target_pct}% PUMP!</b> 🔥"
        else:
            badge = f"🎯 <b>TARGET ACHIEVED: +{target_pct}% PROFIT!</b> 🎯"

        text = (
            f"{badge}\n\n"
            f"💎 <b>Pair:</b> <code>#{symbol}</code>\n"
            f"💰 <b>Entry Price:</b> <code>${fmt_p(entry_price)}</code>\n"
            f"📈 <b>Current Price:</b> <code>${fmt_p(current_price)}</code>\n"
            f"⚡ <b>Max Gain So Far:</b> <code>+{max_gain_pct:.2f}%</code>\n"
            f"⏱️ <b>Time Since Entry:</b> <code>{elapsed_hours:.1f} hours</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 <i>Status: 7-day monitoring active</i>\n"
            f"⏰ <i>{time_str}</i>\n"
            f"📈 <a href=\"https://www.binance.com/en/futures/{symbol}\">View on Binance Futures</a>"
        )
        return await self.send_message(text)

    async def send_sl_hit_alert(
        self,
        symbol: str,
        entry_price: float,
        exit_price: float,
        loss_pct: float,
        reason: str,
        elapsed_hours: float
    ) -> bool:
        """Format and dispatch Stop Loss alert."""
        time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        def fmt_p(val):
            return f"{val:,.2f}" if val >= 100 else (f"{val:.4f}" if val >= 1 else f"{val:.6f}")

        text = (
            f"🛑 <b>STOP LOSS TRIGGERED: #{symbol}</b> 🛑\n\n"
            f"💎 <b>Pair:</b> <code>#{symbol}</code>\n"
            f"💰 <b>Entry Price:</b> <code>${fmt_p(entry_price)}</code>\n"
            f"📉 <b>Exit Price:</b> <code>${fmt_p(exit_price)}</code>\n"
            f"⚠️ <b>Loss:</b> <code>{loss_pct:+.2f}%</code>\n"
            f"📋 <b>Reason:</b> <code>{reason}</code>\n"
            f"⏱️ <b>Time Since Entry:</b> <code>{elapsed_hours:.1f} hours</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🛡️ <b>NOTE: 7-Day Monitoring Continues!</b>\n"
            f"<i>We continue tracking this coin for the remaining duration to monitor post-SL recovery and stop-hunt rallies.</i>\n"
            f"⏰ <i>{time_str}</i>\n"
            f"📈 <a href=\"https://www.binance.com/en/futures/{symbol}\">View on Binance Futures</a>"
        )
        return await self.send_message(text)

    async def send_7d_completion_alert(self, sig) -> bool:
        """Format and dispatch 7-day monitoring final summary card."""
        time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        def fmt_p(val):
            return f"{val:,.2f}" if val >= 100 else (f"{val:.4f}" if val >= 1 else f"{val:.6f}")

        tps_str = ", ".join([f"+{t}%" for t in sig.tps_hit]) if sig.tps_hit else "None reached"
        sl_str = f"YES ({sig.sl_reason})" if sig.sl_hit else "NO ✅ (Zero SL hit)"

        text = (
            f"🏁 <b>7-DAY MONITORING COMPLETED: #{sig.symbol}</b> 🏁\n\n"
            f"💎 <b>Pair:</b> <code>#{sig.symbol}</code>\n"
            f"💰 <b>Entry Price:</b> <code>${fmt_p(sig.entry_price)}</code>\n"
            f"📊 <b>Final 7D Price:</b> <code>${fmt_p(sig.current_price)}</code>\n"
            f"🚀 <b>Max Gain Achieved:</b> <code>+{sig.max_gain_pct:.2f}%</code>\n"
            f"📉 <b>Max Drawdown:</b> <code>{sig.max_loss_pct:.2f}%</code>\n"
            f"🎯 <b>Milestones Reached:</b> <code>{tps_str}</code>\n"
            f"🛑 <b>Stop Loss Hit?</b> <code>{sl_str}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"💾 <i>Full historical audit saved to tracked_signals.json</i>\n"
            f"⏰ <i>{time_str}</i>"
        )
        return await self.send_message(text)
