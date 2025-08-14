import os
import requests
import psycopg
from datetime import datetime, timezone
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CMC_API_KEY = os.getenv('CMC_API_KEY')
CMC_API_URL = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5433'),
    'dbname': os.getenv('POSTGRES_DB', 'crypto'),
    'user': os.getenv('POSTGRES_USER', 'crypto'),
    'password': os.getenv('POSTGRES_PASSWORD', 'crypto')
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_cmc_data():
    """Fetch top 100 cryptocurrencies from CoinMarketCap API"""
    headers = {
        'X-CMC_PRO_API_KEY': CMC_API_KEY,
        'Accept': 'application/json'
    }
    
    params = {
        'start': 1,
        'limit': 100,
        'convert': 'USD'
    }
    
    logger.info("Fetching data from CoinMarketCap API...")
    response = requests.get(CMC_API_URL, headers=headers, params=params)
    response.raise_for_status()
    
    data = response.json()
    logger.info(f"Successfully fetched {len(data['data'])} cryptocurrencies")
    return data['data']

def process_crypto_data(crypto_list):
    """Process and transform cryptocurrency data"""
    processed_data = []
    current_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    
    for crypto in crypto_list:
        quote = crypto['quote']['USD']
        processed_data.append({
            'ts': current_time,
            'name': crypto['name'],
            'symbol': crypto['symbol'],
            'price': quote['price'],
            'market_cap': quote['market_cap']
        })
    
    logger.info(f"Processed {len(processed_data)} records")
    return processed_data

def insert_to_postgres(data):
    """Insert data into PostgreSQL"""
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Insert data
                cur.executemany(
                    """
                    INSERT INTO cmc_quotes (ts, name, symbol, price, market_cap)
                    VALUES (%(ts)s, %(name)s, %(symbol)s, %(price)s, %(market_cap)s)
                    """,
                    data
                )
                conn.commit()
                logger.info(f"Successfully inserted {len(data)} records into PostgreSQL")
                
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        raise

def main():
    """Main function"""
    try:
        # Check if API key is set
        if not CMC_API_KEY:
            logger.error("CMC_API_KEY not found in environment variables")
            return
        
        # Fetch data from API
        crypto_data = fetch_cmc_data()
        
        # Process data
        processed_data = process_crypto_data(crypto_data)
        
        # Insert into database
        insert_to_postgres(processed_data)
        
        logger.info("Data ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}")
        raise

if __name__ == "__main__":
    main() 