# load/load_to_neon.py
import os
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

# ----------------------------
# Load environment variables from GitHub Secrets
# ----------------------------

DB_URL = os.environ.get("Neon_key")
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
        process_date = datetime.utcnow() - timedelta(days=1)

    attempt = 0
    while attempt < MAX_RETRIES:
        engine = None
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
            engine = create_engine(DB_URL, pool_pre_ping=True)
            
            with engine.begin() as conn:
            
                # ------------------------
                # 1️⃣ Load dim_date
                # ------------------------
                df_date = df[['date_key']].drop_duplicates().copy()
                df_date['full_date'] = pd.to_datetime(
                    df_date['date_key'].astype(str), format='%Y%m%d'
                )
                df_date['day'] = df_date['full_date'].dt.day
                df_date['month'] = df_date['full_date'].dt.month
                df_date['quarter'] = df_date['full_date'].dt.quarter
                df_date['year'] = df_date['full_date'].dt.year
                df_date['weekday_flag'] = (
                    df_date['full_date'].dt.weekday < 5
                ).astype(int)
            
                df_date.to_sql("tmp_dim_date", conn, if_exists="replace", index=False)
            
                conn.execute(text("""
                    INSERT INTO dim_date (
                        date_key, full_date, day, month,
                        quarter, year, weekday_flag
                    )
                    SELECT
                        date_key, full_date, day, month,
                        quarter, year, weekday_flag
                    FROM tmp_dim_date
                    ON CONFLICT (date_key)
                    DO UPDATE SET
                        full_date = EXCLUDED.full_date,
                        day = EXCLUDED.day,
                        month = EXCLUDED.month,
                        quarter = EXCLUDED.quarter,
                        year = EXCLUDED.year,
                        weekday_flag = EXCLUDED.weekday_flag
                """))
            
                conn.execute(text("DROP TABLE tmp_dim_date"))
            
                # ------------------------
                # 2️⃣ Load dim_channel
                # ------------------------
                df_channels = df[['channel_key', 'channel_name', 'fee_percent']].drop_duplicates()
            
                df_channels.to_sql("tmp_dim_channel", conn, if_exists="replace", index=False)
            
                conn.execute(text("""
                    INSERT INTO dim_channel (
                        channel_key, channel_name, fee_percent
                    )
                    SELECT
                        channel_key, channel_name, fee_percent
                    FROM tmp_dim_channel
                    ON CONFLICT (channel_key)
                    DO UPDATE SET
                        channel_name = EXCLUDED.channel_name,
                        fee_percent = EXCLUDED.fee_percent
                    WHERE
                        dim_channel.channel_name IS DISTINCT FROM EXCLUDED.channel_name
                        OR dim_channel.fee_percent IS DISTINCT FROM EXCLUDED.fee_percent
                """))
            
                conn.execute(text("DROP TABLE tmp_dim_channel"))
            
                # ------------------------
                # 3️⃣ Load fact_transactions
                # ------------------------
                fact_cols = [
                    "transaction_id",
                    "date_key",
                    "customer_key",
                    "channel_key",
                    "amount",
                    "status",
                    "processing_time",
                    "processing_delay_bucket",
                    "revenue"
                ]
            
                df_fact = df[fact_cols]
            
                df_fact.to_sql("tmp_fact_transactions", conn, if_exists="replace", index=False)
            
                conn.execute(text("""
                    INSERT INTO fact_transactions (
                        transaction_id,
                        date_key,
                        customer_key,
                        channel_key,
                        amount,
                        status,
                        processing_time,
                        processing_delay_bucket,
                        revenue
                    )
                    SELECT
                        transaction_id,
                        date_key,
                        customer_key,
                        channel_key,
                        amount,
                        status,
                        processing_time,
                        processing_delay_bucket,
                        revenue
                    FROM tmp_fact_transactions
                    ON CONFLICT (transaction_id)
                    DO UPDATE SET
                        date_key = EXCLUDED.date_key,
                        customer_key = EXCLUDED.customer_key,
                        channel_key = EXCLUDED.channel_key,
                        amount = EXCLUDED.amount,
                        status = EXCLUDED.status,
                        processing_time = EXCLUDED.processing_time,
                        processing_delay_bucket = EXCLUDED.processing_delay_bucket,
                        revenue = EXCLUDED.revenue
                    WHERE
                        fact_transactions.amount IS DISTINCT FROM EXCLUDED.amount
                        OR fact_transactions.status IS DISTINCT FROM EXCLUDED.status
                        OR fact_transactions.revenue IS DISTINCT FROM EXCLUDED.revenue
                        OR fact_transactions.processing_time IS DISTINCT FROM EXCLUDED.processing_time
                        OR fact_transactions.processing_delay_bucket IS DISTINCT FROM EXCLUDED.processing_delay_bucket
                """))
            
                conn.execute(text("DROP TABLE tmp_fact_transactions"))
    
            
            logging.info(
                f"Load successful | Date: {process_date.date()} | "
                f"Transactions: {record_count} | Channels loaded: {len(df_channels)}"
            )
            
            print(f"✅ Successfully loaded {record_count} transactions into Neon.")
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
                
        finally:
            if engine is not None:
                engine.dispose()

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
