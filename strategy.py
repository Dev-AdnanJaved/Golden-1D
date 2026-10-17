from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from config import config

@dataclass
class Rule1DailyResult:
    passed: bool
    current_close: float
    ema50: float
    fresh_cross: bool
    details: str

@dataclass
class Rule24HResult:
    passed: bool
    trigger_type: str  # "Current Candle" or "Previous Candle (Closed)" or "None"
    ema50: float
    ema200: float
    diff_pct: float
    details: str

@dataclass
class Rule3VolumeResult:
    passed: bool
    avg_vol_7: float
    avg_vol_24: float
    ratio_pct: float
    details: str

@dataclass
class SignalResult:
    symbol: str
    is_valid_signal: bool
    current_price: float
    rule1_daily: Rule1DailyResult
    rule2_4h: Rule24HResult
    rule3_volume: Rule3VolumeResult
    has_volume_warning: bool
    candle_time: int  # Timestamp for deduplication
    vol_24h: float = 0.0
    mcap: Optional[float] = None


def compute_ema(series: pd.Series, period: int) -> pd.Series:
    """Compute Exponential Moving Average."""
    return series.ewm(span=period, adjust=False).mean()


def evaluate_rule_1_daily(klines_1d: List[Dict[str, Any]]) -> Rule1DailyResult:
    """
    Rule 1 (Hard Filter): 1D candle is crossed above 50 EMA.
    Checks:
    - Current 1D candle close > 50 EMA.
    - Also evaluates whether it crossed above in the current or previous daily bar.
    """
    if len(klines_1d) < config.EMA_FAST_PERIOD + 10:
        return Rule1DailyResult(
            passed=False, current_close=0.0, ema50=0.0, fresh_cross=False,
            details=f"Insufficient 1D data ({len(klines_1d)} bars, required at least {config.EMA_FAST_PERIOD + 10})"
        )

    closes = pd.Series([k["close"] for k in klines_1d])
    lows = pd.Series([k["low"] for k in klines_1d])
    ema50 = compute_ema(closes, config.EMA_FAST_PERIOD)

    curr_close = closes.iloc[-1]
    prev_close = closes.iloc[-2]
    curr_ema = ema50.iloc[-1]
    prev_ema = ema50.iloc[-2]
    curr_low = lows.iloc[-1]

    # Fresh cross definitions:
    # 1. Previous candle close was below EMA and current is above, OR
    # 2. Current candle dipped below EMA (low <= EMA) and closed/trades above EMA
    fresh_cross = (prev_close <= prev_ema and curr_close > curr_ema) or (curr_low <= curr_ema and curr_close > curr_ema)

    is_above = curr_close > curr_ema

    if config.DAILY_REQUIRE_FRESH_CROSS:
        passed = fresh_cross and is_above
        status_text = "Fresh 1D Cross Above 50 EMA" if passed else "No fresh 1D cross"
    else:
        # Default: 1D candle has crossed above & is holding above 50 EMA
        passed = is_above
        status_text = "1D Candle Above 50 EMA" if passed else "1D Candle Below 50 EMA"

    cross_label = "Fresh Cross Today" if fresh_cross else "Holding Above EMA"
    details = f"{status_text} (Close: {curr_close:.4f}, EMA50: {curr_ema:.4f}, {cross_label})"

    return Rule1DailyResult(
        passed=passed,
        current_close=curr_close,
        ema50=curr_ema,
        fresh_cross=fresh_cross,
        details=details
    )


def evaluate_rule_2_4h_cross(klines_4h: List[Dict[str, Any]]) -> Rule24HResult:
    """
    Rule 2 (Hard Filter): 4 hour me 50ema crossed 200ema in current candle.
    Checks:
    - Current forming 4H candle: EMA 50 crossed above EMA 200 (ema50[-2] <= ema200[-2] and ema50[-1] > ema200[-1])
    - Optionally previous closed candle: (ema50[-3] <= ema200[-3] and ema50[-2] > ema200[-2])
    """
    if len(klines_4h) < config.EMA_SLOW_PERIOD + 15:
        return Rule24HResult(
            passed=False, trigger_type="None", ema50=0.0, ema200=0.0, diff_pct=0.0,
            details=f"Insufficient 4H data ({len(klines_4h)} bars, required at least {config.EMA_SLOW_PERIOD + 15})"
        )

    closes = pd.Series([k["close"] for k in klines_4h])
    ema50 = compute_ema(closes, config.EMA_FAST_PERIOD)
    ema200 = compute_ema(closes, config.EMA_SLOW_PERIOD)

    curr_ema50 = ema50.iloc[-1]
    curr_ema200 = ema200.iloc[-1]
    prev_ema50 = ema50.iloc[-2]
    prev_ema200 = ema200.iloc[-2]
    prev2_ema50 = ema50.iloc[-3]
    prev2_ema200 = ema200.iloc[-3]

    diff_pct = ((curr_ema50 - curr_ema200) / curr_ema200) * 100

    cross_in_current = (prev_ema50 <= prev_ema200) and (curr_ema50 > curr_ema200)
    cross_in_prev = (prev2_ema50 <= prev2_ema200) and (prev_ema50 > prev_ema200)

    passed = False
    trigger_type = "None"

    if config.CHECK_4H_CURRENT_CANDLE and cross_in_current:
        passed = True
        trigger_type = "Current 4H Candle (In Progress)"
    elif config.CHECK_4H_PREVIOUS_CANDLE and cross_in_prev:
        passed = True
        trigger_type = "Previous 4H Candle (Confirmed at Close)"

    details = (
        f"4H 50 EMA: {curr_ema50:.4f}, 200 EMA: {curr_ema200:.4f} "
        f"(Trigger: {trigger_type}, Diff: {diff_pct:+.3f}%)"
    )

    return Rule24HResult(
        passed=passed,
        trigger_type=trigger_type,
        ema50=curr_ema50,
        ema200=curr_ema200,
        diff_pct=diff_pct,
        details=details
    )


def evaluate_rule_3_volume(klines_1h: List[Dict[str, Any]]) -> Rule3VolumeResult:
    """
    Rule 3 (Soft Filter):
    Average volume of last 7 candles of 1h is greater than last 24 hours candle average.
    Signal should be generated without it too but with warning sign.
    """
    req_bars = max(config.VOL_FAST_CANDLES, config.VOL_SLOW_CANDLES) + 1
    if len(klines_1h) < req_bars:
        return Rule3VolumeResult(
            passed=False, avg_vol_7=0.0, avg_vol_24=0.0, ratio_pct=0.0,
            details=f"Insufficient 1H data ({len(klines_1h)} bars, required {req_bars})"
        )

    volumes = [k["volume"] for k in klines_1h]

    # Calculate last 7 candles and last 24 candles average volume
    fast_vols = volumes[-config.VOL_FAST_CANDLES:]
    slow_vols = volumes[-config.VOL_SLOW_CANDLES:]

    avg_vol_7 = sum(fast_vols) / len(fast_vols)
    avg_vol_24 = sum(slow_vols) / len(slow_vols)

    passed = avg_vol_7 > avg_vol_24
    ratio_pct = ((avg_vol_7 - avg_vol_24) / avg_vol_24) * 100 if avg_vol_24 > 0 else 0.0

    if passed:
        details = f"PASSED: 7h Avg ({avg_vol_7:,.1f}) > 24h Avg ({avg_vol_24:,.1f}) by +{ratio_pct:.1f}%"
    else:
        details = f"WARNING: 7h Avg ({avg_vol_7:,.1f}) <= 24h Avg ({avg_vol_24:,.1f}) ({ratio_pct:.1f}%)"

    return Rule3VolumeResult(
        passed=passed,
        avg_vol_7=avg_vol_7,
        avg_vol_24=avg_vol_24,
        ratio_pct=ratio_pct,
        details=details
    )


def evaluate_pair(
    symbol: str,
    klines_4h: List[Dict[str, Any]],
    klines_1d: List[Dict[str, Any]],
    klines_1h: List[Dict[str, Any]]
) -> Optional[SignalResult]:
    """
    Evaluate all 3 rules for a given pair.
    Rules 1 & 2 are HARD FILTERS: if either fails, NO signal is generated.
    Rule 3 is a SOFT FILTER: if failed, signal is still generated with warning sign.
    """
    # Evaluate Rule 2 (4H Cross)
    r2 = evaluate_rule_2_4h_cross(klines_4h)
    if not r2.passed:
        return None  # Hard filter failed

    # Evaluate Rule 1 (1D Cross above 50 EMA)
    r1 = evaluate_rule_1_daily(klines_1d)
    if not r1.passed:
        return None  # Hard filter failed

    # Evaluate Rule 3 (1H Volume filter - soft filter)
    r3 = evaluate_rule_3_volume(klines_1h)

    current_price = klines_4h[-1]["close"]
    candle_time = klines_4h[-1]["open_time"]

    return SignalResult(
        symbol=symbol,
        is_valid_signal=True,
        current_price=current_price,
        rule1_daily=r1,
        rule2_4h=r2,
        rule3_volume=r3,
        has_volume_warning=not r3.passed,
        candle_time=candle_time
    )

