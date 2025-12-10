import os
import time
import subprocess
import logging
from datetime import datetime

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s'
)

class PipelineScheduler:
    """
    Orchestration Engine for the Crypto Analysis Pipeline.
    
    Responsibilities:
    1. Time-based Triggering: Executes jobs at specific 15-minute intervals (00, 15, 30, 45).
    2. Dependency Management: Ensures data ingestion completes before analysis starts.
    3. Parallel Processing: Runs technical indicator calculators (RSI, MACD, EMA) concurrently.
    4. Signal Dispatch: Triggers the alerting module once analysis is finalized.
    """

    def __init__(self):
        # Define paths relative to the project root for portability
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 1. Data Ingestion Module
        self.ingestion_script = os.path.join(self.base_dir, "data_ingestion", "market_data_fetcher.py")
        
        # 2. Technical Analysis Modules (Parallel Execution)
        self.analysis_dir = os.path.join(self.base_dir, "technical_analysis")
        self.indicators = [
            os.path.join(self.analysis_dir, "calc_ema_crossover.py"), 
            os.path.join(self.analysis_dir, "calc_rsi_multi.py"),      
            os.path.join(self.analysis_dir, "calc_ema_trend.py"),       
            os.path.join(self.analysis_dir, "calc_macd_momentum.py")    
        ]
        
        # 3. Alerting Module (Final Step)
        self.alert_script = os.path.join(self.base_dir, "alerting", "signal_broadcaster.py")

    def run_ingestion(self):
        """Step 1: Fetch fresh data from Binance."""
        if os.path.exists(self.ingestion_script):
            logging.info(f"Starting Data Ingestion: {self.ingestion_script}")
            # Use sys.executable to ensure the same python environment
            subprocess.run(["python", self.ingestion_script], check=True)
        else:
            logging.error(f"Ingestion script not found: {self.ingestion_script}")

    def run_analysis_parallel(self):
        """Step 2: Calculate indicators concurrently."""
        logging.info("Starting Parallel Analysis...")
        processes = []
        
        for script in self.indicators:
            if os.path.exists(script):
                p = subprocess.Popen(["python", script])
                processes.append(p)
            else:
                logging.warning(f"Analysis script missing: {script}")
        
        # Wait for all indicators to finish
        for p in processes:
            p.wait()
        logging.info("All technical indicators calculated.")

    def run_alerting(self):
        """Step 3: Check conditions and send Telegram signals."""
        if os.path.exists(self.alert_script):
            logging.info(f"Running Signal Broadcaster: {self.alert_script}")
            subprocess.run(["python", self.alert_script], check=True)
        else:
            logging.error(f"Alert script not found: {self.alert_script}")

    def execute_pipeline(self):
        """Full pipeline execution flow."""
        start_time = time.time()
        
        try:
            self.run_ingestion()
            self.run_analysis_parallel()
            self.run_alerting()
            
            elapsed = time.time() - start_time
            logging.info(f"Pipeline finished successfully in {elapsed:.2f} seconds.")
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Pipeline failed during execution: {e}")
        except Exception as e:
            logging.error(f"Critical Scheduler Error: {e}")

    def start_scheduler(self):
        """Main loop that triggers the pipeline on specific minute marks."""
        logging.info("Scheduler started. Waiting for :00, :15, :30, :45 marks...")
        
        # State tracking to avoid double execution within the same minute
        executed_minutes = {0: False, 15: False, 30: False, 45: False}

        while True:
            now = datetime.now()
            current_minute = now.minute

            # Reset state when moving to a new 15-min window
            # Logic: If we are at :01, reset the :00 flag, etc.
            # Simplified approach: Just check strictly.
            
            if current_minute in executed_minutes:
                if not executed_minutes[current_minute]:
                    logging.info(f"Triggering scheduled job at {now.strftime('%H:%M:%S')}")
                    
                    self.execute_pipeline()
                    
                    # Mark this slot as done
                    executed_minutes[current_minute] = True
                    
                    # Reset others (simple logic: if I run 15, reset 0, 30, 45)
                    for m in executed_minutes:
                        if m != current_minute:
                            executed_minutes[m] = False
            
            time.sleep(10) # Check clock every 10 seconds

if __name__ == '__main__':
    scheduler = PipelineScheduler()
    try:
        scheduler.start_scheduler()
    except KeyboardInterrupt:
        logging.info("Scheduler stopped by user.")
