import os
import sys
import logging
import pandas as pd
import talib as ta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class MultiRSIAnalyzer:
    """
    Computes Relative Strength Index (RSI) across multiple periods.
    Strategy: 
    - STRONG BUY: RSI(6), RSI(12), and RSI(24) are ALL below 30 (Oversold confluence).
    - STRONG SELL: RSI(6), RSI(12), and RSI(24) are ALL above 70 (Overbought confluence).
    """

    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.input_file = os.path.join(self.base_dir, "data_ingestion", "raw_data", "15m_close.csv")
        self.output_file = os.path.join(self.base_dir, "technical_analysis", "signals_rsi.csv")
        
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

    def analyze(self):
        if not os.path.exists(self.input_file):
            logging.error(f"Input file missing: {self.input_file}")
            return

        df = pd.read_csv(self.input_file)
        latest_ts = df['Datetime'].max()
        
        results = []

        for symbol, group in df.groupby('Symbol'):
            try:
                asset_df = group.sort_values('Datetime').reset_index(drop=True)
                
                # Compute multiple RSI periods for trend confirmation
                asset_df['RSI_6'] = ta.RSI(asset_df['Close'], timeperiod=6)
                asset_df['RSI_12'] = ta.RSI(asset_df['Close'], timeperiod=12)
                asset_df['RSI_24'] = ta.RSI(asset_df['Close'], timeperiod=24)
                
                # Check latest candle
                curr = asset_df.iloc[-1]
                
                signal = None
                # Confluence Logic
                if (curr['RSI_6'] < 30) and (curr['RSI_12'] < 30) and (curr['RSI_24'] < 30):
                    signal = 'BUY'
                elif (curr['RSI_6'] > 70) and (curr['RSI_12'] > 70) and (curr['RSI_24'] > 70):
                    signal = 'SELL'
                
                if signal and curr['Datetime'] == latest_ts:
                    results.append({
                        'Symbol': symbol,
                        'Datetime': curr['Datetime'],
                        'Signal': signal
                    })

            except Exception as e:
                logging.error(f"RSI Error on {symbol}: {e}")

        # Save
        out_df = pd.DataFrame(results, columns=['Symbol', 'Datetime', 'Signal'])
        out_df.to_csv(self.output_file, index=False)
        logging.info(f"RSI Analysis Complete. Signals: {len(out_df)}")

if __name__ == "__main__":
    analyzer = MultiRSIAnalyzer()
    analyzer.analyze()
