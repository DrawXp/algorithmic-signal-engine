import os
import sys
import logging
import pandas as pd
import talib as ta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class EMATrendAnalyzer:
    """
    Long-term Trend Analysis using EMA Crossovers.
    Strategy:
    - BUY: EMA(26) crosses ABOVE EMA(99).
    - SELL: EMA(26) crosses BELOW EMA(99).
    This setup filters noise and captures significant market shifts.
    """

    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.input_file = os.path.join(self.base_dir, "data_ingestion", "raw_data", "15m_close.csv")
        self.output_file = os.path.join(self.base_dir, "technical_analysis", "signals_ema_trend.csv")
        
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

    def analyze(self):
        if not os.path.exists(self.input_file):
            logging.error(f"Input file missing: {self.input_file}")
            return

        df = pd.read_csv(self.input_file)
        # Type conversion
        df['Datetime'] = pd.to_datetime(df['Datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
        df = df.dropna(subset=['Datetime', 'Close'])
        
        latest_ts = df['Datetime'].max()
        results = []

        for symbol, group in df.groupby('Symbol'):
            asset_df = group.sort_values('Datetime').reset_index(drop=True)
            
            if len(asset_df) < 100: # Need at least 99 periods
                continue

            asset_df['EMA26'] = ta.EMA(asset_df['Close'], timeperiod=26)
            asset_df['EMA99'] = ta.EMA(asset_df['Close'], timeperiod=99)
            
            if len(asset_df) < 2: continue
            curr = asset_df.iloc[-1]
            prev = asset_df.iloc[-2]
            
            sig_type = None
            
            # Golden Cross (26 > 99)
            if prev['EMA26'] < prev['EMA99'] and curr['EMA26'] > curr['EMA99']:
                sig_type = 'BUY'
            # Death Cross (26 < 99)
            elif prev['EMA26'] > prev['EMA99'] and curr['EMA26'] < curr['EMA99']:
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
        logging.info(f"Trend Analysis (EMA 26/99) Complete. Signals: {len(out_df)}")

if __name__ == "__main__":
    analyzer = EMATrendAnalyzer()
    analyzer.analyze()
