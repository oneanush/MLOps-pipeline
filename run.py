import argparse
import logging
import yaml
import json
import time
import pandas as pd
import numpy as np
import sys

def setup_logger(log_file):
    # Sets up logging to write to the specified file with timestamps
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def main():
    start_time = time.time()
    
    # 1. Parse Arguments
    parser = argparse.ArgumentParser(description="MLOps Pipeline")
    parser.add_argument('--input', required=True, help="Input CSV file")
    parser.add_argument('--config', required=True, help="Config YAML file")
    parser.add_argument('--output', required=True, help="Output JSON metrics file")
    parser.add_argument('--log-file', required=True, help="Log file path")
    args = parser.parse_args()

    setup_logger(args.log_file)
    logging.info("Job started")

    try:
        # 2. Configuration Loading
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
        
        seed = config['seed']
        window = config['window']
        version = config['version']
        
        # Set deterministic seed
        np.random.seed(seed)
        logging.info(f"Config loaded: seed={seed}, window={window}, version={version}")

        # 3. Data Ingestion & Validation
        df = pd.read_csv(args.input)
        if 'close' not in df.columns:
            raise ValueError("Missing required 'close' column in dataset.")
        
        rows_processed = len(df)
        logging.info(f"Data loaded: {rows_processed} rows")

        # 4. Rolling Mean Computation
        # Handles initial rows automatically with pandas 
        rolling_mean = df['close'].rolling(window=window).mean()
        logging.info(f"Rolling mean calculated with window={window}")

        # 5. Signal Generation
        # 1 if close > rolling_mean, else 0.
        df['signal'] = np.where(df['close'] > rolling_mean, 1, 0)
        logging.info("Signals generated")

        # 6. Metrics Calculation
        signal_rate = float(df['signal'].mean())
        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success"
        }
        
        logging.info(f"Metrics: signal_rate={metrics['value']:.4f}, rows_processed={rows_processed}")

        with open(args.output, 'w') as f:
            json.dump(metrics, f, indent=4)
        
        logging.info(f"Job completed successfully in {latency_ms}ms")
        

        print(json.dumps(metrics, indent=4))

    except Exception as e:
        # Robust Error Handling
        error_msg = str(e)
        logging.error(f"Job failed: {error_msg}")
        
        error_output = {
            "version": config.get('version', 'unknown') if 'config' in locals() else "unknown",
            "status": "error",
            "error_message": error_msg
        }
        
        with open(args.output, 'w') as f:
            json.dump(error_output, f, indent=4)
            
        # Print error JSON to stdout
        print(json.dumps(error_output, indent=4))
        
        sys.exit(1)

        

if __name__ == "__main__":
    main()