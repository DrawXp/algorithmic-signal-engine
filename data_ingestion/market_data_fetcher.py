import os
import csv
import sys
import logging
from datetime import datetime
from binance.client import Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class MarketDataFetcher:
    """
    Fetches historical candlestick data (OHLCV) from Binance API.
    Designed to retrieve High, Low, Close, and Quote Volume for technical analysis.
    """

    def __init__(self):
        # API Credentials
        self.api_key = os.getenv("BINANCE_API_KEY")
        self.api_secret = os.getenv("BINANCE_SECRET_KEY")
        
        if not self.api_key or not self.api_secret:
            logging.critical("Missing Binance credentials in .env")
            sys.exit(1)

        self.client = Client(self.api_key, self.api_secret)

        # Output configuration (Relative paths for portability)
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(self.base_dir, "raw_data")
        
        # Ensure output directory exists
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.path_high_low = os.path.join(self.data_dir, "15m_high_low.csv")
        self.path_close = os.path.join(self.data_dir, "15m_close.csv")
        self.path_volume = os.path.join(self.data_dir, "15m_volume.csv")

        # Target Assets
        self.symbols = [
            'AAVEUSDT', 'ADAUSDT', 'APTUSDT', 'AUDIOUSDT', 'AVAXUSDT', 'BCHUSDT', 'BNBUSDT', 
            'BTCUSDT', 'CKBUSDT', 'DYDXUSDT', 'ETHUSDT', 'FILUSDT', 'FLOWUSDT', 'FTMUSDT', 
            'GALAUSDT', 'GLMUSDT', 'ICPUSDT', 'IMXUSDT', 'LINKUSDT', 'LTCUSDT', 'MASKUSDT', 
            'NEARUSDT', 'OPUSDT', 'RONINUSDT', 'RUNEUSDT', 'SEIUSDT', 'SNXUSDT', 'SOLUSDT', 
            'TONUSDT', 'TRXUSDT', 'UNIUSDT', 'WLDUSDT'
        ]

    def save_to_csv(self, file_path, header, data):
        """Helper to write list of lists to CSV."""
        try:
            with open(file_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(data)
            logging.info(f"Data saved to: {file_path}")
        except IOError as e:
            logging.error(f"Failed to save CSV {file_path}: {e}")

    def fetch_data(self):
        logging.info(f"Starting data fetch for {len(self.symbols)} pairs (Timeframe: 15m)...")
        
        data_high_low = []
        data_close = []
        data_volume = []
        
        for symbol in self.symbols:
            try:
                # Fetch last 200 candles (15 minute interval)
                candles = self.client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_15MINUTE, limit=200)
                
                for candle in candles:
                    # Binance API response structure:
                    # [Open time, Open, High, Low, Close, Volume, Close time, Quote asset volume, ...]
                    
                    timestamp = candle[0]
                    readable_time = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    
                    high_price = candle[2]
                    low_price = candle[3]
                    close_price = candle[4]
                    quote_volume = candle[7] # Using Quote Volume (USDT volume) is often more accurate for analysis
                    
                    # Append structured data
                    data_high_low.append([symbol, readable_time, high_price, low_price])
                    data_close.append([symbol, readable_time, close_price])
                    data_volume.append([symbol, readable_time, quote_volume])
                
                logging.info(f"Fetched {symbol}")
                
            except Exception as e:
                logging.error(f"Error fetching {symbol}: {e}")

        # Batch Save
        self.save_to_csv(self.path_high_low, ['Symbol', 'Datetime', 'High', 'Low'], data_high_low)
        self.save_to_csv(self.path_close, ['Symbol', 'Datetime', 'Close'], data_close)
        self.save_to_csv(self.path_volume, ['Symbol', 'Datetime', 'Volume'], data_volume)
        
        logging.info("Data ingestion complete.")

if __name__ == '__main__':
    fetcher = MarketDataFetcher()
    try:
        fetcher.fetch_data()
    except KeyboardInterrupt:
        sys.exit(0)
