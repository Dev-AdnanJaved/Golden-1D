# 🚀 Binance Futures Public Scanner & Telegram Signal Bot

A high-performance Python bot that scans **all active Binance Futures (USDT-M Perpetual) pairs** using **Binance Public API endpoints** (no API keys required) and sends real-time trading signals directly to your **Telegram**.

---

## 📌 Trading Rules & Strategy Logic

| Rule | Timeframe | Type | Description |
| :--- | :---: | :---: | :--- |
| **Rule 1** | **1D (Daily)** | **HARD** | Candle is crossed above / holding above **50 EMA**. If failed, **NO signal** is generated. |
| **Rule 2** | **4H (4-Hour)** | **HARD** | **50 EMA crosses above 200 EMA** (Golden Cross) in the current forming 4H candle (or last closed candle). If failed, **NO signal** is generated. |
| **Rule 3** | **1H (1-Hour)** | **SOFT** | Average volume of the **last 7 candles of 1H** is greater than the **last 24-hour candles average volume**. If this volume filter fails, the signal is **STILL generated**, but with a prominent **⚠️ WARNING SIGN**. |

---

## 🛠️ Project Structure

```text
├── bot.py                  # Main scanner runner and periodic scheduler
├── binance_client.py       # High-performance async Binance Futures public client
├── strategy.py             # Indicator calculation (EMA50, EMA200, Volume) and rule evaluator
├── telegram_notifier.py    # Formatted HTML Telegram alert card dispatcher
├── config.py               # Bot configuration & environment loader
├── get_chat_id.py          # Helper script to easily find your Telegram Chat ID
├── .env.example            # Template for environment settings
├── .env                    # Your private Telegram credentials and settings
└── requirements.txt        # Python dependencies
```

---

## ⚡ Quick Start Guide

### 1. Install Dependencies
Make sure you have Python 3.10+ installed:
```bash
pip install -r requirements.txt
```

---

### 2. Set Up Telegram Bot (2 Minutes)

1. Open Telegram and search for **[@BotFather](https://t.me/BotFather)**.
2. Send `/newbot`, choose a name and username.
3. BotFather will provide your **HTTP API Token** (e.g., `782348274:ABCdefghijk...`).
4. Copy your token into `.env`:
   ```ini
   TELEGRAM_BOT_TOKEN=your_bot_token_here
   ```
5. Search for your new bot on Telegram and tap **START** (or send `/start` or `hi`).
6. Run the Chat ID discovery script:
   ```bash
   python get_chat_id.py
   ```
   It will display your `TELEGRAM_CHAT_ID`.
7. Paste it into `.env`:
   ```ini
   TELEGRAM_CHAT_ID=your_chat_id_here
   ```

---

### 3. Verify Telegram Setup

Test your Telegram credentials:
```bash
python bot.py --test-telegram
```
You will receive a confirmation message in your Telegram chat!

---

### 4. Preview / Simulate an Alert

To see exactly how a signal message looks in Telegram and in the terminal:
```bash
python bot.py --simulate-signal NIGHTUSDT
```

---

### 5. Run the Scanner

#### Continuous Mode (Scans every 3 minutes automatically):
```bash
python bot.py
```

#### Run a Single Scan (Scan all 525+ pairs once and exit):
```bash
python bot.py --scan-once
```

#### Inspect a Specific Pair:
```bash
python bot.py --symbol NIGHTUSDT
```

---

## ⚙️ Configuration Options (`.env`)

| Variable | Default | Description |
| :--- | :---: | :--- |
| `TELEGRAM_BOT_TOKEN` | - | Your Telegram bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | - | Your Telegram user or channel/group ID |
| `SCAN_INTERVAL_SECONDS` | `180` | Delay between scans (in seconds). Default: 3 minutes |
| `MAX_CONCURRENT_REQUESTS`| `25` | Concurrency limit to stay safely below Binance rate limits |
| `ALERT_COOLDOWN_MINUTES` | `240` | Cooldown per pair (240 min = 4 hours) to prevent repeated alerts |
| `DAILY_REQUIRE_FRESH_CROSS` | `false` | `false` = 1D Close > 50 EMA; `true` = requires cross to have occurred today |
| `CHECK_4H_CURRENT_CANDLE` | `true` | Detects EMA 50/200 cross happening in the current active 4H candle |
| `CHECK_4H_PREVIOUS_CANDLE`| `true` | Detects EMA 50/200 cross that completed on the last closed 4H candle |
| `VOL_FAST_CANDLES` | `7` | Number of recent 1H candles for volume average |
| `VOL_SLOW_CANDLES` | `24` | Number of baseline 1H candles for volume average |

---

## 📱 Sample Telegram Signal Card

```text
🚀 BINANCE FUTURES SIGNAL: BUY / LONG 🚀

💎 Pair: #NIGHTUSDT
💰 Current Price: $0.0410

━━━━━━━━━━━━━━━━━━━━━
📊 RULES VERIFICATION:

1️⃣ 1D Candle vs 50 EMA: ✅ PASSED
   • 1D Close: 0.04103
   • 1D 50 EMA: 0.0343 (Holding Above 50 EMA)

2️⃣ 4H 50/200 EMA Cross: ✅ PASSED
   • Trigger: Current 4H Candle (In Progress)
   • 4H 50 EMA: 0.0439
   • 4H 200 EMA: 0.0376
   • Bullish Spread: +0.450%

3️⃣ 1H Volume Filter (Soft): ⚠️ WARNING (SOFT FILTER FAILED)
   • 7-Bar 1H Avg: 7,544,124.7
   • 24-Bar 1H Avg: 8,728,953.5
   • Difference: -13.6% vs 24h average
   ⚠️ Caution: 1H Volume condition not met! Trade with extra risk management.
━━━━━━━━━━━━━━━━━━━━━
⏰ Time: 2026-10-17 18:25:26 UTC
📈 Open NIGHTUSDT on Binance Futures
```

---

## 🛡️ Rate Limit & Optimization

- Binance Futures allows up to 2400 weight/minute.
- The bot employs a **multi-stage filtering pipeline**:
  1. It first scans **4H candles** across all pairs.
  2. Because a 4H 50/200 EMA golden cross happens rarely (~0-3 pairs at any moment), **only candidate pairs** fetch 1D and 1H data.
  3. This cuts down API consumption by over **90%**, running a full market scan across **525 pairs in under 15 seconds** without hitting rate limits!

