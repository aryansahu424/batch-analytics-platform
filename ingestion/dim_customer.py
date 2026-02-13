import pandas as pd
import random
from faker import Faker
from pathlib import Path
from datetime import datetime, timedelta

fake = Faker()

# ----------------------------
# CONFIG
# ----------------------------
BASE_PATH = Path("data/processed")
INPUT_FILENAME = "cleaned_transactions.parquet"
OUTPUT_FILENAME = "dim_customer.parquet"
CUSTOMER_KEY_COLUMN = "customer_key"

SEGMENTS = ["Retail", "Corporate", "SMB", "Enterprise"]
# ----------------------------


def get_yesterday_partition():
    yesterday = datetime.today() - timedelta(days=1)
    return (
        str(yesterday.year),
        f"{yesterday.month:02d}",
        f"{yesterday.day:02d}"
    )


def generate_customer_record(customer_key):
    seed = hash(customer_key) % (2**32)
    random.seed(seed)
    fake.seed_instance(seed)

    return {
        "customer_key": customer_key,
        "customer_id": f"CUST-{random.randint(100000, 999999)}",
        "signup_date": fake.date_between(start_date="-5y", end_date="today"),
        "segment": random.choice(SEGMENTS)
    }


def main():
    year, month, day = get_yesterday_partition()

    folder_path = BASE_PATH / year / month / day
    input_path = folder_path / INPUT_FILENAME
    output_path = folder_path / OUTPUT_FILENAME

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    print(f"Reading from: {input_path}")

    df = pd.read_parquet(input_path)

    if CUSTOMER_KEY_COLUMN not in df.columns:
        raise ValueError(f"{CUSTOMER_KEY_COLUMN} not found in file")

    # Get unique customers
    customer_keys = df[CUSTOMER_KEY_COLUMN].drop_duplicates()

    # Generate dimension table
    dim_customer = pd.DataFrame(
        [generate_customer_record(key) for key in customer_keys]
    )

    # Save in SAME folder
    dim_customer.to_parquet(output_path, index=False)

    print(f"dim_customer saved at: {output_path}")


if __name__ == "__main__":
    main()
