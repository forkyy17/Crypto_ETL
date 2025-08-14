from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, round, lit
from pyspark.sql.window import Window
import clickhouse_connect
from dotenv import load_dotenv
import os
import logging
from datetime import datetime, timezone

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configurations
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5433'),
    'dbname': os.getenv('POSTGRES_DB', 'crypto'),
    'user': os.getenv('POSTGRES_USER', 'crypto'),
    'password': os.getenv('POSTGRES_PASSWORD', 'crypto')
}

CLICKHOUSE_CONFIG = {
    'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
    'port': int(os.getenv('CLICKHOUSE_PORT', '8123')),
    'user': os.getenv('CLICKHOUSE_USER', 'default'),
    'password': os.getenv('CLICKHOUSE_PASSWORD', ''),
    'database': os.getenv('CLICKHOUSE_DATABASE', 'analytics')
}

def create_spark_session():
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName("CryptoHourlyChange") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    logger.info("Spark session created successfully")
    return spark

def read_from_postgres(spark):
    """Read data from PostgreSQL using psycopg and convert to Spark DataFrame"""
    try:
        import psycopg
        
        # Connect to PostgreSQL
        with psycopg.connect(**POSTGRES_CONFIG) as conn:
            with conn.cursor() as cur:
                # Query to get data from last 2 hours for hourly change calculation
                cur.execute("""
                    SELECT ts, name, symbol, price, market_cap
                    FROM cmc_quotes 
                    WHERE ts >= NOW() - INTERVAL '2 hours'
                    ORDER BY symbol, ts
                """)
                
                # Fetch all data
                rows = cur.fetchall()
                columns = ['ts', 'name', 'symbol', 'price', 'market_cap']
                
                logger.info(f"Successfully read {len(rows)} records from PostgreSQL")
                
                # Convert to Spark DataFrame
                df = spark.createDataFrame(rows, columns)
                return df
                
    except Exception as e:
        logger.error(f"Error reading from PostgreSQL: {e}")
        raise

def calculate_hourly_change(df):
    """Calculate hourly price change percentage using window functions"""
    try:
        # Create window partitioned by symbol, ordered by timestamp
        window_spec = Window.partitionBy("symbol").orderBy("ts")
        
        # Add lagged price (previous hour)
        df_with_lag = df.withColumn(
            "price_prev", 
            lag("price").over(window_spec)
        )
        
        # Calculate percentage change
        df_with_pct = df_with_lag.withColumn(
            "pct_1h",
            when(
                col("price_prev").isNotNull(),
                round(((col("price") - col("price_prev")) / col("price_prev")) * 100, 2)
            ).otherwise(lit(0))  # Fill first records with 0%
        )
        
        # Keep all records (including first ones with 0%)
        result_df = df_with_pct
        
        logger.info(f"Calculated hourly changes for {result_df.count()} records")
        return result_df
        
    except Exception as e:
        logger.error(f"Error calculating hourly changes: {e}")
        raise

def write_to_clickhouse(df):
    """Write results to ClickHouse without pandas to avoid datetime casting issues"""
    try:
        # Select only required columns
        selected_df = df.select("ts", "name", "symbol", "price", "market_cap", "pct_1h")

       
        rows = selected_df.collect()

        # Prepare data for insertion
        data = []
        for row in rows:
            data.append([
                row["ts"],
                row["name"],
                row["symbol"],
                float(row["price"]) if row["price"] is not None else None,
                float(row["market_cap"]) if row["market_cap"] is not None else None,
                float(row["pct_1h"]) if row["pct_1h"] is not None else None,
            ])

        if not data:
            logger.info("No data to write to ClickHouse")
            return

        # Connect to ClickHouse and insert
        client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        client.insert(
            'hourly_prices',
            data,
            column_names=['ts', 'name', 'symbol', 'price', 'market_cap', 'pct_1h']
        )
        client.close()
        logger.info(f"Successfully wrote {len(data)} records to ClickHouse")

    except Exception as e:
        logger.error(f"Error writing to ClickHouse: {e}")
        raise

def main():
    """Main function"""
    try:
        logger.info("Starting hourly change calculation...")
        
        # Create Spark session
        spark = create_spark_session()
        
        # Read data from PostgreSQL
        df = read_from_postgres(spark)
        
        # Calculate hourly changes
        result_df = calculate_hourly_change(df)

        # Only keep rows where we have a previous price (no pct=0 rows)
        filtered_df = result_df.filter(col("price_prev").isNotNull())

        # Write to ClickHouse
        write_to_clickhouse(filtered_df)
        
        logger.info("Hourly change calculation completed successfully!")
        
       
        spark.stop()
        
    except Exception as e:
        logger.error(f"Hourly change calculation failed: {e}")
        raise

if __name__ == "__main__":
    main() 