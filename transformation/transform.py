import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import argparse

# ----------------------------
# Configuration
# ----------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

MAX_RETRIES = 3

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    filename=LOG_DIR / "processing.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ----------------------------
# Channel Reference (Dimension Simulation)
# ----------------------------
CHANNEL_DIM = pd.DataFrame([
    {"channel_key": 1, "channel_name": "Credit Card", "fee_percent": 2.5},
    {"channel_key": 2, "channel_name": "Debit Card", "fee_percent": 1.0},
    {"channel_key": 3, "channel_name": "UPI", "fee_percent": 0.5},
    {"channel_key": 4, "channel_name": "Net Banking", "fee_percent": 1.5},
])


def get_latest_raw_file(process_date):
    year = process_date.strftime("%Y")
    month = process_date.strftime("%m")
    day = process_date.strftime("%d")
    return RAW_DIR / year / month / day / "transactions.csv"


def derive_processing_bucket(seconds):
    if seconds < 1:
        return "fast"
    elif seconds < 3:
        return "medium"
    else:
        return "slow"


def run_transformation():

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD", type=str)
    args = parser.parse_args()

    if args.date:
        process_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        process_date = datetime.now() - timedelta(days=1)
    raw_file = get_latest_raw_file(process_date)

    try:
        logging.info(f"Starting transformation for {process_date.date()}")

        df = pd.read_csv(raw_file)

        initial_count = len(df)

        # -----------------------------------
        # 1️⃣ Remove duplicates
        # -----------------------------------
        df = df.drop_duplicates()

        # -----------------------------------
        # 2️⃣ Validation Rules
        # -----------------------------------
        df = df[df["amount"] > 0]
        df = df[df["status"].isin(["success", "failed"])]
        df = df[df["transaction_id"].notna()]

        # -----------------------------------
        # 3️⃣ Create Surrogate Key
        # -----------------------------------
        df = df.reset_index(drop=True)
        df["transaction_sk"] = np.arange(1, len(df) + 1)

        # -----------------------------------
        # 4️⃣ Join Channel Dimension
        # -----------------------------------
        df = df.merge(CHANNEL_DIM, on="channel_key", how="left")

        # -----------------------------------
        # 5️⃣ Derive processing_delay_bucket
        # -----------------------------------
        df["processing_delay_bucket"] = df["processing_time"].apply(
            derive_processing_bucket
        )

        # -----------------------------------
        # 6️⃣ Revenue Calculation
        # -----------------------------------
        df["revenue"] = df["amount"] * (df["fee_percent"] / 100)

        # -----------------------------------
        # Output Path
        # -----------------------------------
        year = process_date.strftime("%Y")
        month = process_date.strftime("%m")
        day = process_date.strftime("%d")

        output_dir = PROCESSED_DIR / year / month / day
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / "cleaned_transactions.parquet"

        df.to_parquet(output_file, index=False, engine="pyarrow")

        logging.info(
            f"Transformation successful | "
            f"Initial Records: {initial_count} | "
            f"Final Records: {len(df)} | "
            f"Output: {output_file}"
        )

        print(f"✅ Processed {len(df)} records successfully.")

    except Exception as e:
        logging.error(f"Transformation failed | Error: {str(e)}")
        print("❌ Transformation failed.")
        raise


if __name__ == "__main__":
    run_transformation()
