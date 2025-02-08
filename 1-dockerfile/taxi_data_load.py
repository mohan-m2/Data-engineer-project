import argparse
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import psycopg2
from time import time

def main(params):
    # Extract command-line parameters
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table = params.table
    schema = params.schema
    file_path = params.file
    chunk_size = params.chunksize

    # PostgreSQL connection string
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(db_url, pool_size=10, max_overflow=20)

    # ✅ Ensure schema exists
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    # ✅ Read the Parquet file schema (empty DataFrame)
    df = pd.read_parquet(file_path)
    df.head(0).to_sql(name=table, con=engine, schema=schema, if_exists='replace', index=False)

    print("✅ Table created successfully. Starting data insertion...")

    # ✅ Insert data in chunks
    start_time = time()
    
    conn = engine.raw_connection()  # Get raw PostgreSQL connection
    cursor = conn.cursor()

    # Process data in chunks manually
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i : i + chunk_size]  # Get a chunk

        # ✅ Save chunk as CSV for fast COPY ingestion
        chunk.to_csv("data_chunk.csv", index=False)

        # ✅ Use COPY for fast insertion
        with open("data_chunk.csv", "r") as f:
            cursor.copy_expert(f"COPY {schema}.{table} FROM STDIN WITH CSV HEADER", f)

        conn.commit()  # Commit after each chunk
        print(f"✅ Inserted {i + len(chunk)} records...")

    cursor.close()
    conn.close()  # ✅ Close connection

    print(f"✅ Data inserted successfully in {time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PostgreSQL Data Ingestion Script")

    parser.add_argument('--user', help='User name for PostgreSQL', required=True)
    parser.add_argument('--password', help='Password for PostgreSQL', required=True)
    parser.add_argument('--host', help='Database host', default='localhost')
    parser.add_argument('--port', help='Database port', type=int, default=5432)
    parser.add_argument('--database', help='Database name', required=True)
    parser.add_argument('--table', help='Table name to insert data', required=True)
    parser.add_argument('--schema', help='Schema name (default: public)', default='public')
    parser.add_argument('--file', help='Path to the Parquet file', required=True)
    parser.add_argument('--chunksize', help='Chunk size for batch processing', type=int, default=100000)

    args = parser.parse_args()
    main(args)
