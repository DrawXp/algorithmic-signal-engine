import os
import sys
import logging
import pandas as pd
import talib as ta

# Configure Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class EMACrossoverAnalyzer:
    """
    Computes Exponential Moving Averages (EMA) and detects crossovers.
    Strategy: 
    - BUY when EMA 12 crosses above EMA 26 (Golden Cross logic).
    - SELL when EMA 12 crosses below EMA 26 (Death Cross logic).
    """

    def __init__(self):
        # Paths relative to project root
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.input_file = os.path.join(self.base_dir, "data_ingestion", "raw_data", "15m_close.csv")
        self.output_file = os.path.join(self.base_dir, "technical_analysis", "signals_ema.csv")
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

    def analyze(self):
        if not os.path.exists(self.input_file):
            logging.error(f"Input file not found: {self.input_file}")
            return

        try:
            # Read CSV with correct header mapping from data fetcher
            # Expected columns from previous script: Symbol, Datetime, Close
            df = pd.read_csv(self.input_file)
        except Exception as e:
            logging.error(f"Failed to read CSV: {e}")
            return

        if len(df) < 26:
            logging.warning("Insufficient data points for EMA calculation (Need > 26).")
            return

        # Prepare output container
        signals_list = []
        
        # Detect latest timestamp in the dataset to filter only fresh signals
        latest_ts = df['Datetime'].max()
        logging.info(f"Analyzing data for timestamp: {latest_ts}")

        # Group by Symbol to handle multiple assets in single CSV
        for symbol, group in df.groupby('Symbol'):
            try:
                # Sort and reset index for calculation
                asset_df = group.sort_values('Datetime').reset_index(drop=True)
                
                # Calculate Technical Indicators (TA-Lib)
                asset_df['EMA12'] = ta.EMA(asset_df['Close'], timeperiod=12)
                asset_df['EMA26'] = ta.EMA(asset_df['Close'], timeperiod=26)
                
                # Signal Logic
                asset_df['Signal'] = None
                
                # Vectorized crossover detection would be faster, but keeping iterative logic for clarity/fidelity
                # Check the last closed candle (index -1) against previous (index -2)
                if len(asset_df) < 2: continue
                
                curr = asset_df.iloc[-1]
                prev = asset_df.iloc[-2]
                
                # Golden Cross (Bullish)
                if prev['EMA12'] < prev['EMA26'] and curr['EMA12'] > curr['EMA26']:
                    signal_type = 'BUY'
                # Death Cross (Bearish)
                elif prev['EMA12'] > prev['EMA26'] and curr['EMA12'] < curr['EMA26']:
                    signal_type = 'SELL'
                else:
                    signal_type = None

                # Only save if there is a signal AND it corresponds to the latest batch
                if signal_type and curr['Datetime'] == latest_ts:
                    signals_list.append({
                        'Symbol': symbol,
                        'Datetime': curr['Datetime'],
                        'Signal': signal_type
                    })
            except Exception as e:
                logging.error(f"Error processing {symbol}: {e}")

        # Export Results
        output_df = pd.DataFrame(signals_list, columns=['Symbol', 'Datetime', 'Signal'])
        output_df.to_csv(self.output_file, index=False)
        
        if not output_df.empty:
            logging.info(f"EMA Analysis Complete. {len(output_df)} signals found.")
        else:
            logging.info("EMA Analysis Complete. No signals found.")

if __name__ == "__main__":
    analyzer = EMACrossoverAnalyzer()
    analyzer.analyze()
