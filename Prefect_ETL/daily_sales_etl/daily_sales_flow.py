from prefect import flow, task
import pandas as pd
from pathlib import Path

# Base directory — always points to this file's folder, regardless of CWD
BASE_DIR = Path(__file__).parent

# Tasks: individual steps — decorated with @task
@task(name="Extract Orders")
def extract_orders(file_path: str) -> pd.DataFrame:
    """Read raw order data from a CSV file."""
    df = pd.read_csv(file_path)
    print(f"Extracted: {len(df):,} rows")
    return df

@task(name="Transform Orders")
def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich order data."""
    df = df.dropna(subset=["order_id"])
    df["revenue"] = df["qty"] * df["unit_price"]
    df["region"]  = df["region"].str.strip().str.upper()
    print(f"Transformed: {len(df):,} rows")
    return df

@task(name="Load Orders")
def load_orders(df: pd.DataFrame, output_path: str) -> None:
    """Save cleaned data to Parquet."""
    df.to_parquet(output_path, index=False)
    print(f"✅ Loaded {len(df):,} rows to {output_path}")

# Flow: the orchestrated pipeline
@flow(name="Daily Sales ETL", log_prints=True)
def daily_sales_etl(input_path: str = str(BASE_DIR / "data" / "orders.csv"),
                    output_path: str = str(BASE_DIR / "output" / "orders_clean.parquet")):
    """Complete ETL pipeline for daily sales data."""
    df_raw   = extract_orders(input_path)
    df_clean = transform_orders(df_raw)
    load_orders(df_clean, output_path)
    print("✅ Pipeline complete!")

# Run it — just call the function!
if __name__ == "__main__":
    daily_sales_etl()
