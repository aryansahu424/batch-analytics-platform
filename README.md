# Batch Analytics Platform

A production-grade ETL pipeline for processing transactional data with automated ingestion, transformation, and warehouse loading capabilities. Features a real-time analytics dashboard for monitoring revenue metrics and transaction performance.

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌─────────────┐
│  Ingestion  │───▶│Transformation│───▶│  Warehouse   │───▶│  Dashboard  │
│   (CSV)     │    │  (Parquet)   │    │   (Neon DB)  │    │ (Streamlit) │
└─────────────┘    └──────────────┘    └──────────────┘    └─────────────┘
```

## Features

- **Automated Daily Pipeline**: GitHub Actions workflow runs at 2 AM UTC
- **Synthetic Data Generation**: 500 transactions/day across 4 payment channels
- **Data Quality**: Deduplication, validation, and error handling with retry logic
- **Star Schema**: Optimized dimensional model (fact + dimension tables)
- **Real-time Dashboard**: Interactive KPIs with channel-level filtering
- **Partitioned Storage**: Date-based folder structure (YYYY/MM/DD)

## Project Structure

```
batch-analytics-platform/
├── ingestion/
│   ├── ingest.py              # Generate synthetic transactions
│   └── dim_customer.py        # Customer dimension loader
├── transformation/
│   └── transform.py           # Data cleaning & enrichment
├── warehouse/
│   ├── load.py                # Load to Neon PostgreSQL
│   └── load_dim_customer.py  # Customer dimension loader
├── dashboard/
│   └── app.py                 # Streamlit analytics dashboard
├── data/
│   ├── raw/                   # CSV files (partitioned)
│   └── processed/             # Parquet files (partitioned)
├── .github/workflows/
│   └── pipeline.yml           # CI/CD automation
└── requirements.txt
```

## Quick Start

### Prerequisites

- Python 3.8+
- PostgreSQL database (Neon recommended)
- Git

### Installation

```bash
# Clone repository
git clone <repository-url>
cd batch-analytics-platform

# Install dependencies
pip install -r requirements.txt
```

### Database Schema

```sql
-- Dimension Tables
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    quarter INT,
    year INT,
    weekday_flag INT
);

CREATE TABLE dim_channel (
    channel_key INT PRIMARY KEY,
    channel_name VARCHAR(50),
    fee_percent DECIMAL(5,2)
);

CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY,
    customer_name VARCHAR(100),
    segment VARCHAR(50)
);

CREATE TABLE dim_city (
    city_key INT PRIMARY KEY,
    city_name VARCHAR(100),
    state VARCHAR(100),
    region VARCHAR(50)
);

-- Fact Table
CREATE TABLE fact_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    channel_key INT REFERENCES dim_channel(channel_key),
    city_key INT REFERENCES dim_city(city_key),
    amount DECIMAL(10,2),
    status VARCHAR(20),
    processing_time DECIMAL(5,2),
    processing_delay_bucket VARCHAR(20),
    revenue DECIMAL(10,2)
);
```

## Usage

### Manual Execution

```bash
# Run full pipeline for yesterday
python ingestion/ingest.py
python transformation/transform.py
python warehouse/load.py

# Run for specific date
python ingestion/ingest.py --date 2024-01-15
python transformation/transform.py --date 2024-01-15
python warehouse/load.py --date 2024-01-15
```

### Launch Dashboard

```bash
streamlit run dashboard/app.py
```

Access at `https://batch-analytics-platform.streamlit.app`

## Pipeline Details

### 1. Ingestion (`ingestion/ingest.py`)

- Generates 500 synthetic transactions per day
- Supports 4 payment channels: Credit Card, Debit Card, UPI, Net Banking
- 90% success rate, 10% failure rate
- Outputs to `data/raw/YYYY/MM/DD/transactions.csv`
- Retry logic: 3 attempts with 2-second delays

### 2. Transformation (`transformation/transform.py`)

- Removes duplicates
- Validates: amount > 0, valid status, non-null transaction_id
- Enriches with channel metadata (fee_percent)
- Derives processing delay buckets (fast/medium/slow)
- Calculates revenue: `amount × (fee_percent / 100)`
- Outputs to `data/processed/YYYY/MM/DD/cleaned_transactions.parquet`

### 3. Warehouse Load (`warehouse/load.py`)

- Upserts to dim_date, dim_channel, fact_transactions
- Handles conflicts with ON CONFLICT DO UPDATE
- Uses temporary staging tables for atomic operations
- Connection pooling with retry logic

### 4. Dashboard (`dashboard/app.py`)

**KPIs:**
- Total Revenue (successful transactions only)
- Failure Rate
- Average Processing Time

**Dynamic Filters:**
- Date range selector
- City/State/Region (cascading filters)
- Channel
- Customer Segment

**Trend Charts:**
- Revenue trend with 7-day moving average
- Failure rate trend with 7-day moving average
- Avg processing time trend with 7-day moving average
- Dynamic breakdown: Shows top 4 items when filter set to "All"

**Comparison Charts:**
- Default: Failure Rate & Avg Processing Time by Channel
- Single filter: Top 6 items by selected dimension
- Adaptive titles based on data availability

**Features:**
- Multi-dimensional filtering with cascading options
- Glass morphism UI design
- Interactive hover tooltips with series names
- Horizontal legends positioned inside charts
- Auto-refresh every 10 minutes
- Responsive layout

## CI/CD

GitHub Actions workflow (`.github/workflows/pipeline.yml`):

```yaml
Trigger: Daily at 2 AM IST + Manual dispatch
Steps:
  1. Checkout code
  2. Install dependencies
  3. Run ingestion (transactions + customers)
  4. Transform data
  5. Load to warehouse (facts + dimensions)
```

## Data Flow

1. **Raw Data**: CSV files with transaction records
2. **Processed Data**: Parquet files with enriched metrics
3. **Warehouse**: Star schema in PostgreSQL
4. **Dashboard**: Real-time analytics via SQL queries

## Error Handling

- Retry logic on all pipeline stages (3 attempts)
- Comprehensive logging to `logs/` directory
- Graceful degradation on failures
- Transaction rollback on database errors

## Performance

- Parquet compression reduces storage by ~70%
- Indexed primary/foreign keys for fast joins
- Connection pooling minimizes overhead
- Cached dashboard queries (10-min TTL)

## Monitoring

Check logs:
```bash
tail -f logs/ingestion.log
tail -f logs/processing.log
tail -f logs/load_to_neon.log
```

## Dependencies

Core libraries:
- `pandas`: Data manipulation
- `pyarrow`: Parquet I/O
- `sqlalchemy`: Database ORM
- `psycopg2-binary`: PostgreSQL driver
- `streamlit`: Dashboard framework
- `plotly`: Interactive charts
- `faker`: Synthetic data generation
