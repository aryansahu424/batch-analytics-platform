# load/load_to_neon.py
import os
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

# ----------------------------
# Load environment variables from GitHub Secrets
# ----------------------------

DB_URL = os.environ.get("ENVIRONMENTS")
if not DB_URL:
    raise EnvironmentError("Missing DB_URL environment variable from GitHub Secrets")

# ----------------------------
# Configuration
# ----------------------------
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ----------------------------
# Logging configuration
# ----------------------------
logging.basicConfig(
    filename=LOG_DIR / "load_to_neon.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ----------------------------
# Helper functions
# ----------------------------
def get_latest_parquet(process_date: datetime):
    year = process_date.strftime("%Y")
    month = process_date.strftime("%m")
    day = process_date.strftime("%d")
    file_path = PROCESSED_DIR / year / month / day / "cleaned_transactions.parquet"
    if not file_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {file_path}")
    return file_path

# ----------------------------
# Main load function
# ----------------------------
def load_to_neon(process_date: datetime = None):
    if process_date is None:
        process_date = datetime.now() - timedelta(days=1)

    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            logging.info(f"Starting load to Neon for {process_date.date()}")

            # Read cleaned Parquet
            parquet_file = get_latest_parquet(process_date)
            df = pd.read_parquet(parquet_file, engine="pyarrow")
            record_count = len(df)

            if record_count == 0:
                logging.warning("No records to load.")
                return

            # Connect to Neon
            engine = create_engine(DB_URL)

            # Load data into fact_transactions
            df.to_sql(
                "fact_transactions",
                engine,
                if_exists="append",
                index=False
            )

            logging.info(
                f"Load successful | Date: {process_date.date()} | Records: {record_count}"
            )
            print(f"✅ Successfully loaded {record_count} records into Neon.")
            return

        except Exception as e:
            attempt += 1
            logging.error(
                f"Attempt {attempt} failed | Error: {str(e)}"
            )
            print(f"❌ Attempt {attempt} failed: {str(e)}")

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logging.critical(
                    f"Load failed after {MAX_RETRIES} attempts for {process_date.date()}"
                )
                raise

# ----------------------------
# CLI support
# ----------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD", type=str)
    args = parser.parse_args()

    if args.date:
        run_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        run_date = None

    load_to_neon(run_date)
