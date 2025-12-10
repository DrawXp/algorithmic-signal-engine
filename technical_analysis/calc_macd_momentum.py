import os
import sys
import logging
import pandas as pd
import talib as ta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class MACDMomentumAnalyzer:
    """
    Computes Moving Average Convergence Divergence (MACD).
    Strategy:
    - MACD Line = EMA(12) - EMA(26)
    - Signal Line = EMA(9) of MACD Line
    - BUY when MACD Line crosses ABOVE Signal Line.
    - SELL when MACD Line crosses BELOW Signal Line.
    """

    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.input_file = os.path.join(self.base_dir, "data_ingestion", "raw_data", "15m_close.csv")
        self.output_file = os.path.join(self.base_dir, "technical_analysis", "signals_macd.csv")
        
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

    def analyze(self):
        if not os.path.exists(self.input_file):
            logging.error(f"Input file missing: {self.input_file}")
            return

        df = pd.read_csv(self.input_file)
        
        # Ensure correct types
        df['Datetime'] = pd.to_datetime(df['Datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        df = df.dropna(subset=['Datetime', 'Close'])
        df = df.sort_values('Datetime').reset_index(drop=True)
        df['Close'] = pd.to_numeric(df['Close'])

        # Detect latest timestamp
        latest_ts = df['Datetime'].max()
        
        results = []

        for symbol, group in df.groupby('Symbol'):
            asset_df = group.sort_values('Datetime').reset_index(drop=True)
            
            if len(asset_df) < 35:
                continue

            # Calculate MACD using TA-Lib (returns 3 series: macd, signal, hist)
            macd, signal, hist = ta.MACD(
                asset_df['Close'], 
                fastperiod=12, 
                slowperiod=26, 
                signalperiod=9
            )
            
            asset_df['MACD'] = macd
            asset_df['SignalLine'] = signal
            
            # Logic: Cross detection on the LAST candle
            # (index -1 vs index -2)
            curr = asset_df.iloc[-1]
            prev = asset_df.iloc[-2]
            
            sig_type = None
            
            # Crossover Logic
            # Prev: MACD < Signal | Curr: MACD > Signal => BUY
            if prev['MACD'] < prev['SignalLine'] and curr['MACD'] > curr['SignalLine']:
                sig_type = 'BUY'
            
            # Prev: MACD > Signal | Curr: MACD < Signal => SELL
            elif prev['MACD'] > prev['SignalLine'] and curr['MACD'] < curr['SignalLine']:
                sig_type = 'SELL'
            
            if sig_type and curr['Datetime'] == latest_ts:
                results.append({
                    'Symbol': symbol,
                    'Datetime': curr['Datetime'],
                    'Signal': sig_type
                })

        # Save
        out_df = pd.DataFrame(results, columns=['Symbol', 'Datetime', 'Signal'])
        out_df.to_csv(self.output_file, index=False)
        logging.info(f"MACD Analysis Complete. Signals: {len(out_df)}")

if __name__ == "__main__":
    analyzer = MACDMomentumAnalyzer()
    analyzer.analyze()
