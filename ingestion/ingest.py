import os
import random
import logging
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import time
import argparse

random.seed(42) 

# ----------------------------
# Configuration
# ----------------------------
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2
RECORDS_PER_DAY = 500  # Adjust as needed

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ----------------------------
# Logging Configuration
# ----------------------------
logging.basicConfig(
    filename=LOG_DIR / "ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ----------------------------
# Synthetic Dimension Values
# ----------------------------
CUSTOMER_SEGMENTS = ["Retail", "Corporate", "SME"]
CHANNELS = [
    {"channel_key": 1, "channel_name": "Credit Card", "fee_percent": 2.5},
    {"channel_key": 2, "channel_name": "Debit Card", "fee_percent": 1.0},
    {"channel_key": 3, "channel_name": "UPI", "fee_percent": 0.5},
    {"channel_key": 4, "channel_name": "Net Banking", "fee_percent": 1.5},
]


def generate_synthetic_transactions(process_date: datetime, num_records: int):
    """Generate synthetic transactional data for a given date."""

    records = []

    for i in range(num_records):
        transaction_id = f"T{process_date.strftime('%Y%m%d')}{i:05d}"
        customer_key = random.randint(1, 1000)
        channel = random.choice(CHANNELS)
        amount = round(random.uniform(10, 1000), 2)
        status = random.choices(["success", "failed"], weights=[0.9, 0.1])[0]
        processing_time = round(random.uniform(0.5, 5.0), 2)

        record = {
            "transaction_id": transaction_id,
            "date_key": int(process_date.strftime("%Y%m%d")),
            "customer_key": customer_key,
            "channel_key": channel["channel_key"],
            "amount": amount,
            "status": status,
            "processing_time": processing_time,
        }

        records.append(record)

    return pd.DataFrame(records)


def save_transactions(df: pd.DataFrame, process_date: datetime):
    """Save transactions to partitioned folder structure."""

    year = process_date.strftime("%Y")
    month = process_date.strftime("%m")
    day = process_date.strftime("%d")

    output_dir = RAW_DATA_DIR / year / month / day
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "transactions.csv"

    df.to_csv(output_file, index=False)
    print("Saving to:", output_dir)
    return output_file


def run_ingestion():
    """Main ingestion logic with retry."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD", type=str)
    args = parser.parse_args()

    if args.date:
        process_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        process_date = datetime.now() - timedelta(days=1)

    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            logging.info(f"Starting ingestion for {process_date.date()}")

            df = generate_synthetic_transactions(
                process_date, RECORDS_PER_DAY
            )

            output_file = save_transactions(df, process_date)

            logging.info(
                f"Ingestion successful | Date: {process_date.date()} | "
                f"Records: {len(df)} | File: {output_file}"
            )

            print(f"✅ Successfully ingested {len(df)} records.")
            return

        except Exception as e:
            attempt += 1
            logging.error(
                f"Attempt {attempt} failed for {process_date.date()} | Error: {str(e)}"
            )

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logging.critical(
                    f"Ingestion failed after {MAX_RETRIES} attempts "
                    f"for {process_date.date()}"
                )
                print("❌ Ingestion failed after maximum retries.")
                raise


if __name__ == "__main__":
    run_ingestion()
