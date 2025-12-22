"""
H&M Fashion Data Loader
=======================
Loads H&M dataset (articles, customers, transactions) into PostgreSQL.
Uses chunked loading for the 15M transactions file.

Usage:
    python data_generation/load_hm_data.py
"""

import pandas as pd
from sqlalchemy import create_engine, text
import time
import os

# =============================================
# CONFIGURATION
# =============================================

# Database connection
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'ecommerce')
DB_USER = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# File paths
DATA_DIR = "data/raw"
ARTICLES_FILE = f"{DATA_DIR}/articles.csv"
CUSTOMERS_FILE = f"{DATA_DIR}/customers_sampled.csv"
TRANSACTIONS_FILE = f"{DATA_DIR}/transactions_sampled.csv"

# Chunk size for transactions (100K rows at a time)
CHUNK_SIZE = 100_000


# =============================================
# DATABASE CONNECTION
# =============================================

def get_engine():
    """Create database connection engine."""
    print(f"Connecting to database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    engine = create_engine(DATABASE_URL)
    return engine


def test_connection(engine):
    """Test if database connection works."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Database connection successful!\n")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


# =============================================
# DATA LOADERS
# =============================================

def load_articles(engine):
    """Load articles (products) data - small file, full load."""
    print("=" * 50)
    print("Loading ARTICLES...")
    print("=" * 50)
    
    start_time = time.time()
    
    # Read CSV
    df = pd.read_csv(ARTICLES_FILE)
    print(f"Read {len(df):,} articles from CSV")
    
    # Load to PostgreSQL
    df.to_sql(
        name='articles',
        schema='raw',
        con=engine,
        if_exists='replace',
        index=False
    )
    
    elapsed = time.time() - start_time
    print(f"✅ Articles loaded in {elapsed:.1f} seconds\n")
    
    return len(df)


def load_customers(engine):
    """Load customers data - medium file, full load."""
    print("=" * 50)
    print("Loading CUSTOMERS...")
    print("=" * 50)
    
    start_time = time.time()
    
    # Read CSV
    df = pd.read_csv(CUSTOMERS_FILE)
    print(f"Read {len(df):,} customers from CSV")
    
    # Load to PostgreSQL
    df.to_sql(
        name='customers',
        schema='raw',
        con=engine,
        if_exists='replace',
        index=False
    )
    
    elapsed = time.time() - start_time
    print(f"✅ Customers loaded in {elapsed:.1f} seconds\n")
    
    return len(df)


def load_transactions(engine):
    """Load transactions data - large file, chunked load."""
    print("=" * 50)
    print("Loading TRANSACTIONS (chunked)...")
    print("=" * 50)
    
    start_time = time.time()
    total_rows = 0
    chunk_num = 0
    
    # Read and load in chunks
    for chunk in pd.read_csv(TRANSACTIONS_FILE, chunksize=CHUNK_SIZE):
        chunk_num += 1
        
        # First chunk replaces, rest append
        if_exists = 'replace' if chunk_num == 1 else 'append'
        
        chunk.to_sql(
            name='transactions',
            schema='raw',
            con=engine,
            if_exists=if_exists,
            index=False
        )
        
        total_rows += len(chunk)
        elapsed = time.time() - start_time
        
        # Progress update
        print(f"  Chunk {chunk_num}: Loaded {total_rows:,} rows ({elapsed:.1f}s elapsed)")
    
    elapsed = time.time() - start_time
    print(f"\n✅ Transactions loaded: {total_rows:,} rows in {elapsed:.1f} seconds\n")
    
    return total_rows


# =============================================
# MAIN FUNCTION
# =============================================

def main():
    """Main function to load all H&M data."""
    print("\n" + "=" * 50)
    print("H&M DATA LOADER")
    print("=" * 50 + "\n")
    
    overall_start = time.time()
    
    # Connect to database
    engine = get_engine()
    
    if not test_connection(engine):
        return
    
    # Load tables in order
    articles_count = load_articles(engine)
    customers_count = load_customers(engine)
    transactions_count = load_transactions(engine)
    
    # Summary
    overall_elapsed = time.time() - overall_start
    
    print("=" * 50)
    print("LOAD COMPLETE!")
    print("=" * 50)
    print(f"  Articles:     {articles_count:,} rows")
    print(f"  Customers:    {customers_count:,} rows")
    print(f"  Transactions: {transactions_count:,} rows")
    print(f"  Total time:   {overall_elapsed:.1f} seconds ({overall_elapsed/60:.1f} minutes)")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()