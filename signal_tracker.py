import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
import pandas as pd

from config import config

logger = logging.getLogger("SignalTracker")

# Defined milestones requested by user:
TP_MILESTONES = [10, 20, 30, 50, 75, 100, 125, 150, 175, 200]
MONITORING_DURATION_SECONDS = 7 * 24 * 3600  # 7 days in seconds

@dataclass
class TrackedSignal:
    id: str
    symbol: str
    entry_time: float                      # Unix epoch timestamp in seconds
    entry_date: str                        # Human readable UTC
    entry_price: float
    monitored_until: float                 # entry_time + 7 days
    monitored_until_date: str
    current_price: float
    highest_price: float
    lowest_price: float
    max_gain_pct: float
    max_loss_pct: float
    tps_hit: List[int] = field(default_factory=list)      # e.g. [10, 20, 30]
    sl_hit: bool = False
    sl_time: Optional[str] = None
    sl_price: Optional[float] = None
    sl_reason: Optional[str] = None
    status: str = "MONITORING"             # "MONITORING" or "COMPLETED_7_DAYS"
    completed_date: Optional[str] = None
    events_log: List[Dict] = field(default_factory=list)
    last_updated: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'TrackedSignal':
        return cls(**data)


class SignalTracker:
    """
    Monitors all alerted signals for 7 full days.
    Tracks TP milestones: +10%, +20%, +30%, +50%, +75%, +100%, +125%, +150%, +175%, +200%.
    Tracks Stop-Loss hits (4H close below 200 EMA), and continues monitoring for 7 days even if SL hits.
    Saves JSON database immediately after every alert.
    """
    def __init__(self, filename: str = "tracked_signals.json"):
        self.filename = filename
        self.signals: Dict[str, TrackedSignal] = {}
        self.load()

    def load(self):
        if not os.path.exists(self.filename):
            self.signals = {}
            return
        try:
            with open(self.filename, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.signals = {k: TrackedSignal.from_dict(v) for k, v in data.items()}
            logger.info(f"Loaded {len(self.signals)} tracked signals from {self.filename}")
        except Exception as e:
            logger.error(f"Error loading {self.filename}: {e}")
            self.signals = {}

    def save(self):
        try:
            data = {k: v.to_dict() for k, v in self.signals.items()}
            temp_file = f"{self.filename}.tmp"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            os.replace(temp_file, self.filename)
            logger.info(f"Saved tracked signals database to {self.filename}")
        except Exception as e:
            logger.error(f"Failed to save {self.filename}: {e}")

    def add_signal(self, symbol: str, entry_price: float, entry_time: Optional[float] = None) -> TrackedSignal:
        now = entry_time or time.time()
        sig_id = f"{symbol}_{int(now)}"
        now_dt = datetime.fromtimestamp(now, tz=timezone.utc)
        mon_until = now + MONITORING_DURATION_SECONDS
        until_dt = datetime.fromtimestamp(mon_until, tz=timezone.utc)

        tracked = TrackedSignal(
            id=sig_id,
            symbol=symbol,
            entry_time=now,
            entry_date=now_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
            entry_price=entry_price,
            monitored_until=mon_until,
            monitored_until_date=until_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
            current_price=entry_price,
            highest_price=entry_price,
            lowest_price=entry_price,
            max_gain_pct=0.0,
            max_loss_pct=0.0,
            tps_hit=[],
            sl_hit=False,
            status="MONITORING",
            events_log=[{
                "time": now_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "event": "SIGNAL_CREATED",
                "price": entry_price,
                "message": f"Signal initialized for 7-day monitoring until {until_dt.strftime('%Y-%m-%d %H:%M UTC')}"
            }],
            last_updated=now_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        )
        self.signals[sig_id] = tracked
        self.save()
        logger.info(f"Added #{symbol} to 7-day tracker at entry ${entry_price}. ID: {sig_id}")
        return tracked

    async def update_all_tracked(self, client, notifier) -> List[Dict]:
        """
        Check all active signals still within their 7-day monitoring window.
        Inspect current prices, evaluate TP milestones and SL triggers,
        dispatch Telegram alerts, and persist updates to JSON immediately.
        """
        active_sigs = [s for s in self.signals.values() if s.status == "MONITORING"]
        if not active_sigs:
            return []

        now = time.time()
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        alerts_dispatched = []

        # Get unique symbols needed
        symbols_needed = list({s.symbol for s in active_sigs})
        tickers_map = await client.get_all_tickers() # Fetch latest real-time prices

        for sig in active_sigs:
            sym = sig.symbol
            cur_price = tickers_map.get(sym)
            if not cur_price or cur_price <= 0:
                continue

            entry = sig.entry_price
            gain_pct = ((cur_price - entry) / entry) * 100.0

            # Update highs, lows, and current price
            sig.current_price = cur_price
            if cur_price > sig.highest_price:
                sig.highest_price = cur_price
            if cur_price < sig.lowest_price:
                sig.lowest_price = cur_price

            sig.max_gain_pct = max(sig.max_gain_pct, ((sig.highest_price - entry) / entry) * 100.0)
            sig.max_loss_pct = min(sig.max_loss_pct, ((sig.lowest_price - entry) / entry) * 100.0)
            sig.last_updated = now_str

            data_changed = False

            # 1. Check Take Profit Milestones (+10%, +20%, +30%, +50%, +75%, +100%, +125%, +150%, +175%, +200%)
            for milestone in TP_MILESTONES:
                target_price = entry * (1.0 + (milestone / 100.0))
                if sig.highest_price >= target_price and milestone not in sig.tps_hit:
                    sig.tps_hit.append(milestone)
                    sig.tps_hit.sort()
                    elapsed_hours = (now - sig.entry_time) / 3600.0

                    event = {
                        "time": now_str,
                        "event": "TP_HIT",
                        "milestone_pct": milestone,
                        "target_price": target_price,
                        "current_price": cur_price,
                        "elapsed_hours": round(elapsed_hours, 1),
                        "message": f"Target +{milestone}% achieved at ${cur_price} ({elapsed_hours:.1f}h after entry)"
                    }
                    sig.events_log.append(event)
                    data_changed = True

                    # Send Telegram Alert
                    sent = await notifier.send_tp_hit_alert(
                        symbol=sym,
                        entry_price=entry,
                        current_price=cur_price,
                        target_pct=milestone,
                        max_gain_pct=sig.max_gain_pct,
                        elapsed_hours=elapsed_hours
                    )
                    alerts_dispatched.append(event)
                    logger.info(f"🎯 TP ALERT DISPATCHED: #{sym} reached +{milestone}%!")

            # 2. Check Stop Loss Condition (4H Candle Close below 200 EMA)
            # Only trigger once; even if hit, monitoring continues for 7 days!
            if not sig.sl_hit:
                # Fetch recent 4H klines to evaluate 200 EMA
                klines_4h = await client.get_klines(sym, interval="4h", limit=220)
                if klines_4h and len(klines_4h) >= 200:
                    closes = pd.Series([float(k["close"]) for k in klines_4h])
                    ema200 = closes.ewm(span=200, adjust=False).mean().iloc[-1]
                    last_closed_bar = klines_4h[-2] # Just-closed 4H candle
                    last_close = float(last_closed_bar["close"])

                    # If closed below 200 EMA or catastrophic -12% dip
                    if last_close < ema200 or gain_pct <= -12.0:
                        sig.sl_hit = True
                        sig.sl_time = now_str
                        sig.sl_price = cur_price
                        loss_pct = ((cur_price - entry) / entry) * 100.0
                        reason = f"4H Candle closed below 200 EMA (${ema200:.4f})" if last_close < ema200 else "Safety Circuit Breaker (-12%)"
                        sig.sl_reason = reason
                        elapsed_hours = (now - sig.entry_time) / 3600.0

                        event = {
                            "time": now_str,
                            "event": "SL_HIT",
                            "exit_price": cur_price,
                            "loss_pct": round(loss_pct, 2),
                            "reason": reason,
                            "elapsed_hours": round(elapsed_hours, 1),
                            "message": f"Stop loss triggered at ${cur_price} ({loss_pct:+.2f}%). Continuing 7-day monitoring."
                        }
                        sig.events_log.append(event)
                        data_changed = True

                        # Send Telegram Alert
                        await notifier.send_sl_hit_alert(
                            symbol=sym,
                            entry_price=entry,
                            exit_price=cur_price,
                            loss_pct=loss_pct,
                            reason=reason,
                            elapsed_hours=elapsed_hours
                        )
                        alerts_dispatched.append(event)
                        logger.warning(f"🛑 SL ALERT DISPATCHED: #{sym} triggered SL. Monitoring continues for 7 days.")

            # 3. Check 7-Day Completion Window
            if now >= sig.monitored_until:
                sig.status = "COMPLETED_7_DAYS"
                sig.completed_date = now_str
                event = {
                    "time": now_str,
                    "event": "7D_MONITORING_COMPLETED",
                    "final_price": cur_price,
                    "final_gain_pct": round(gain_pct, 2),
                    "max_gain_pct": round(sig.max_gain_pct, 2),
                    "max_loss_pct": round(sig.max_loss_pct, 2),
                    "tps_achieved": sig.tps_hit,
                    "sl_was_hit": sig.sl_hit,
                    "message": "7-day tracking period officially finished."
                }
                sig.events_log.append(event)
                data_changed = True

                # Send 7-Day Summary Card
                await notifier.send_7d_completion_alert(sig)
                logger.info(f"🏁 7-DAY MONITORING COMPLETED: #{sym}. Max gain: +{sig.max_gain_pct:.1f}%")

            # Save immediately to JSON if any alert or status changed
            if data_changed:
                self.save()

        return alerts_dispatched

    def get_summary_stats(self) -> dict:
        total = len(self.signals)
        active = sum(1 for s in self.signals.values() if s.status == "MONITORING")
        completed = sum(1 for s in self.signals.values() if s.status == "COMPLETED_7_DAYS")
        sl_hits = sum(1 for s in self.signals.values() if s.sl_hit)
        tp_hits = sum(1 for s in self.signals.values() if len(s.tps_hit) > 0)
        return {
            "total_tracked": total,
            "actively_monitoring": active,
            "completed_7_days": completed,
            "trades_hitting_tp": tp_hits,
            "trades_hitting_sl": sl_hits
        }

