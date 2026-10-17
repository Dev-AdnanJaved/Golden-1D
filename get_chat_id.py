"""
Helper script to find your Telegram Chat ID easily.
Steps:
1. Open Telegram and search for your bot username.
2. Send /start or any text message to your bot.
3. Run this script: python get_chat_id.py
"""
import sys
import os
import requests
from dotenv import load_dotenv

load_dotenv()

def get_chat_id():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if len(sys.argv) > 1:
        token = sys.argv[1]

    if not token or token == "YOUR_TELEGRAM_BOT_TOKEN":
        token = input("Enter your Telegram Bot Token: ").strip()

    if not token:
        print("Error: No bot token provided.")
        return

    print(f"\nChecking updates for bot token: {token[:10]}...{token[-5:]}")
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if not data.get("ok"):
            print("Telegram API Error:", data.get("description"))
            return

        results = data.get("result", [])
        if not results:
            print("\n❌ No messages received yet!")
            print("👉 Open Telegram, open a chat with your bot, send a message (like /start or 'hi'), then re-run this script!")
            return

        print("\n✅ Found incoming messages!\n")
        seen_chats = set()
        for update in reversed(results):
            msg = update.get("message") or update.get("channel_post")
            if msg and "chat" in msg:
                chat = msg["chat"]
                cid = chat["id"]
                if cid not in seen_chats:
                    seen_chats.add(cid)
                    c_type = chat.get("type", "unknown")
                    title = chat.get("title") or chat.get("first_name") or chat.get("username", "Unknown")
                    print(f" Chat ID: {cid}")
                    print(f" Name/Title: {title}")
                    print(f" Type: {c_type}")
                    print(f" 👉 Put this in your .env file: TELEGRAM_CHAT_ID={cid}\n")
    except Exception as e:
        print(f"Connection error: {e}")

if __name__ == "__main__":
    get_chat_id()

