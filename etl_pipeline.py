# ETL Pipeline
# Tools: Python, SQL, Snowflake, dbt

import pandas as pd
import snowflake.connector
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Snowflake connection config
SNOWFLAKE_CONFIG = {
    'user': 'your_username',
    'password': 'your_password',
    'account': 'your_account',
    'warehouse': 'your_warehouse',
    'database': 'your_database',
    'schema': 'your_schema'
}

# EXTRACT — Load raw data from CSV sources
def extract_data(file_paths):
    logger.info("Starting data extraction...")
    dataframes = []
    for path in file_paths:
        df = pd.read_csv(path)
        dataframes.append(df)
        logger.info(f"Extracted {len(df)} rows from {path}")
    combined = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Total rows extracted: {len(combined)}")
    return combined

# TRANSFORM — Clean and transform data
def transform_data(df):
    logger.info("Starting data transformation...")

    # Remove duplicates
    df = df.drop_duplicates()

    # Drop nulls
    df = df.dropna()

    # Standardise column names
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

    # Add metadata columns
    df['created_at'] = datetime.now()
    df['pipeline_version'] = '1.0'

    logger.info(f"Rows after transformation: {len(df)}")
    return df

# LOAD — Load data into Snowflake
def load_data(df, table_name):
    logger.info(f"Loading data into Snowflake table: {table_name}")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    # Create table if not exists
    columns = ', '.join([f"{col} VARCHAR" for col in df.columns])
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} ({columns})
    """)

    # Insert rows
    for _, row in df.iterrows():
        values = ', '.join([f"'{val}'" for val in row.values])
        cursor.execute(f"""
            INSERT INTO {table_name} VALUES ({values})
        """)

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Successfully loaded {len(df)} rows into {table_name}")

# DATA QUALITY CHECK
def run_quality_checks(df):
    logger.info("Running data quality checks...")
    assert df.isnull().sum().sum() == 0, "Null values found"
    assert df.duplicated().sum() == 0, "Duplicate rows found"
    logger.info("All quality checks passed")

# MAIN PIPELINE
if __name__ == "__main__":
    # Define source files
    source_files = [
        'data/source1.csv',
        'data/source2.csv',
        'data/source3.csv'
    ]

    # Run ETL
    raw_data = extract_data(source_files)
    transformed_data = transform_data(raw_data)
    run_quality_checks(transformed_data)
    load_data(transformed_data, 'sales_data')

    logger.info("ETL pipeline completed successfully")
