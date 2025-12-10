import os
import sys
import asyncio
import logging
import pandas as pd
from telegram import Bot
from telegram.constants import ParseMode
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure Logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class SignalBroadcaster:
    """
    Reads generated signal CSVs and broadcasts alerts via Telegram.
    """

    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        # Validate credentials
        if not self.bot_token or not self.chat_id:
            logging.critical("Missing Telegram credentials (TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID) in .env")
            sys.exit(1)

        self.bot = Bot(token=self.bot_token)

        # Path configuration (Relative to project root)
        # Assumes structure: /root/alerting/this_script.py
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.input_dir = os.path.join(self.base_dir, "technical_analysis")

        # Mapping: CSV Filename -> Display Name for Telegram
        self.file_map = {
            "signals_ema.csv": "⚡ EMA Crossover (Fast)",
            "signals_ema_trend.csv": "🌊 Trend Follower (EMA 26/99)",
            "signals_macd.csv": "💎 MACD Momentum",
            "signals_rsi.csv": "📊 Multi-RSI Strategy"
        }

    def format_message(self, strategy_name, df):
        """Formats the DataFrame into a readable Telegram message with Emojis."""
        if df.empty:
            return None

        # Header
        message = f"**{strategy_name}**\n"
        message += "----------------\n"

        # Rows
        for _, row in df.iterrows():
            symbol = row['Symbol']
            signal = row['Signal'] # 'BUY' or 'SELL' or 'Compra'/'Venda'
            
            # Emoji Logic
            icon = "🟢" if str(signal).upper() in ['BUY', 'COMPRA'] else "🔴"
            
            message += f"{icon} **{symbol}**: {signal}\n"

        return message

    async def broadcast(self):
        logging.info("Checking for signals to broadcast...")
        messages_sent = 0

        for filename, display_name in self.file_map.items():
            file_path = os.path.join(self.input_dir, filename)

            if not os.path.exists(file_path):
                logging.warning(f"Signal file not found: {filename}")
                continue

            try:
                # Read CSV
                df = pd.read_csv(file_path)
                
                # Validation: Check if it has data
                if df.empty:
                    continue
                
                # Format Payload
                msg_text = self.format_message(display_name, df)
                
                # Send to Telegram
                if msg_text:
                    await self.bot.send_message(
                        chat_id=self.chat_id, 
                        text=msg_text, 
                        parse_mode=ParseMode.MARKDOWN
                    )
                    logging.info(f"Sent alert for {display_name}")
                    messages_sent += 1

            except Exception as e:
                logging.error(f"Failed to process {filename}: {e}")

        if messages_sent == 0:
            logging.info("No active signals to broadcast this cycle.")
        else:
            logging.info(f"Broadcast complete. Sent {messages_sent} alert groups.")

if __name__ == "__main__":
    broadcaster = SignalBroadcaster()
    try:
        asyncio.run(broadcaster.broadcast())
    except KeyboardInterrupt:
        sys.exit(0)
