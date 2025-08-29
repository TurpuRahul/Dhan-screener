import os
import asyncio
import logging
import requests
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

DHAN_API_URL = os.getenv("DHAN_API_URL", "https://api.dhan.co")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")

# Database session (assuming db.py and models.py are correctly set up)
# from db import SessionLocal, engine
# from models import StockData, Base

# Global mapping from security_id to symbol
security_id_to_symbol = {}

# Rate limiting parameters
REQUESTS_PER_MINUTE = 30  # Reduced to be conservative (adjust based on API docs)
DELAY_BETWEEN_REQUESTS = 60 / REQUESTS_PER_MINUTE  # Seconds between requests
REQUEST_COUNT = 0
LAST_RESET_TIME = time.time()

def get_securities_to_track():
    """
    Downloads and filters securities to track, ensuring valid futures contracts.
    Fetches all stock futures and selects the nearest expiry contract for each.
    """
    global security_id_to_symbol
    url = "https://images.dhan.co/api-data/api-scrip-master.csv"
    try:
        df = pd.read_csv(url, low_memory=False)
        fno_df = df[df['SEM_INSTRUMENT_NAME'] == 'FUTSTK']

        # Get current date for filtering valid expiry dates
        current_date = datetime.now().date()
        fno_df['SEM_EXPIRY_DATE'] = pd.to_datetime(fno_df['SEM_EXPIRY_DATE'], format='%Y-%m-%d').dt.date
        fno_df = fno_df[fno_df['SEM_EXPIRY_DATE'] >= current_date]  # Filter out expired contracts

        # Group by ticker symbol and find the nearest expiry for each
        fno_df = fno_df.loc[fno_df.groupby('SEM_TICKER_SYMBOL')['SEM_EXPIRY_DATE'].idxmin()]

        filtered_stocks = []
        for _, row in fno_df.iterrows():
            security_id = str(row['SEM_SMST_SECURITY_ID'])
            symbol = row['SEM_TRADING_SYMBOL']
            instrument_type = row['SEM_INSTRUMENT_NAME']
            expiry_date = row['SEM_EXPIRY_DATE']

            # Skip if expiry is too far in the future (e.g., > 3 months)
            if (expiry_date - current_date).days <= 90:
                filtered_stocks.append({
                    "symbol": symbol,
                    "security_id": security_id,
                    "instrument_type": instrument_type,
                    "expiry_date": expiry_date
                })
                security_id_to_symbol[security_id] = symbol
            else:
                logger.warning(f"Skipping {symbol} with far-future expiry {expiry_date}")

        return filtered_stocks
    except Exception as e:
        logger.error(f"Error fetching or parsing securities CSV: {e}")
        return []

def is_market_open():
    """
    Check if the current time is within NSE market hours (9:15 AM to 3:30 PM IST).
    """
    now = datetime.now()
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close and now.weekday() < 5  # Monday to Friday

def fetch_intraday_data(security_id, instrument_type, from_date, to_date, retries=3, backoff_factor=2):
    """
    Fetches intraday data from Dhan API v2 with retry logic.
    """
    global REQUEST_COUNT, LAST_RESET_TIME

    # Simple rate limiting check
    current_time = time.time()
    if current_time - LAST_RESET_TIME >= 60:
        REQUEST_COUNT = 0
        LAST_RESET_TIME = current_time

    if REQUEST_COUNT >= REQUESTS_PER_MINUTE:
        sleep_time = 60 - (current_time - LAST_RESET_TIME)
        if sleep_time > 0:
            logger.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds.")
            time.sleep(sleep_time)
        REQUEST_COUNT = 0
        LAST_RESET_TIME = time.time()

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'access-token': ACCESS_TOKEN
    }

    payload = {
        "securityId": security_id,
        "exchangeSegment": "NSE_FNO",
        "instrument": instrument_type,
        "interval": "ONE_MINUTE",
        "fromDate": from_date,
        "toDate": to_date
    }

    url = f"{DHAN_API_URL}/v2/charts/intraday"

    for attempt in range(retries):
        try:
            logger.info(f"API Request: {url}, Security ID: {security_id}, Payload: {json.dumps(payload)}, Attempt: {attempt + 1}")
            response = requests.post(url, headers=headers, json=payload)
            REQUEST_COUNT += 1

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                logger.warning(f"Rate limit exceeded for {security_id}. Retrying after backoff...")
                sleep_time = backoff_factor ** attempt
                time.sleep(sleep_time)
            elif response.status_code == 400:
                logger.error(f"Bad Request for {security_id}: {response.text}")
                return None
            else:
                logger.error(f"API Error: {response.status_code} - {response.text}")
                return None

        except requests.RequestException as e:
            logger.error(f"Error fetching intraday data for {security_id}: {e}")
            if attempt < retries - 1:
                sleep_time = backoff_factor ** attempt
                logger.info(f"Retrying after {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logger.error(f"Max retries reached for {security_id}")
                return None

    return None

def save_intraday_data_to_csv(symbol, data_df):
    """
    Saves intraday data to a CSV, appending only new data.
    """
    if data_df.empty:
        logger.warning(f"No data to save for {symbol}")
        return

    out_path = f"data/{symbol}_intraday.csv"
    os.makedirs("data", exist_ok=True)

    if os.path.exists(out_path):
        try:
            existing_df = pd.read_csv(out_path)
            last_timestamp = existing_df['timestamp'].max()
            new_data_df = data_df[data_df['timestamp'] > last_timestamp]

            if not new_data_df.empty:
                new_data_df.to_csv(out_path, mode="a", header=False, index=False)
                logger.info(f"Appended {len(new_data_df)} new rows for {symbol}.")
            else:
                logger.info(f"No new intraday data to save for {symbol}.")
        except Exception as e:
            logger.error(f"Error processing existing CSV for {symbol}: {e}. Overwriting file.")
            data_df.to_csv(out_path, index=False)
    else:
        data_df.to_csv(out_path, index=False)
        logger.info(f"Saved {len(data_df)} intraday rows for {symbol}.")

async def start_collector():
    """
    Fetches intraday data for all securities every 5 minutes during market hours.
    """
    if not ACCESS_TOKEN or not CLIENT_ID:
        logger.error("DHAN_ACCESS_TOKEN and DHAN_CLIENT_ID must be set in environment variables")
        return

    securities = get_securities_to_track()
    if not securities:
        logger.error("No securities to track. Aborting data collector.")
        return

    logger.info(f"Starting intraday data collector for {len(securities)} securities.")

    while True:
        if is_market_open():
            now = datetime.now()
            market_open_time = now.replace(hour=9, minute=15, second=0, microsecond=0)

            from_date_str = market_open_time.strftime('%Y-%m-%d %H:%M:%S')
            to_date_str = now.strftime('%Y-%m-%d %H:%M:%S')

            logger.info("Market is open. Fetching data for the day...")
            for security in securities:
                try:
                    intraday_data = fetch_intraday_data(
                        security['security_id'],
                        security['instrument_type'],
                        from_date_str,
                        to_date_str
                    )

                    if not intraday_data or not intraday_data.get('open'):
                        logger.warning(f"No or empty intraday data found for {security['symbol']}")
                        continue

                    df = pd.DataFrame(intraday_data)
                    save_intraday_data_to_csv(security['symbol'], df)

                    await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

                except Exception as e:
                    logger.error(f"Error processing {security['symbol']}: {e}")
                    continue

            logger.info("Completed a cycle. Sleeping for 5 minutes.")
            await asyncio.sleep(300)

        else:
            logger.info("Market is closed. Sleeping for 5 minutes.")
            await asyncio.sleep(300)

if __name__ == "__main__":
    # Start the collector
    asyncio.run(start_collector())
