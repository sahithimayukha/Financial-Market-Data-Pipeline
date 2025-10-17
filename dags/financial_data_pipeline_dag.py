from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import yfinance as yf
import time
import json
from typing import List, Dict, Any, Optional

# --- HELPER FUNCTIONS FOR RELIABILITY (DAY 3 SKILL: EXPONENTIAL BACKOFF) ---

def fetch_ticker_data_with_retry(ticker: str, max_retries: int = 5) -> Optional[Dict[str, Any]]:
    """
    Implements a robust API call with exponential backoff retries.
    """
    print(f"Attempting to fetch data for {ticker}...")
    
    for attempt in range(max_retries):
        try:
            # E: Extraction - Fetch the most recent price data
            ticker_info = yf.Ticker(ticker)
            current_price_data = ticker_info.history(period="1d")
            
            if current_price_data.empty:
                raise ValueError(f"No market data returned for {ticker}.")

            # Return the latest available record
            return current_price_data.iloc[-1].to_dict()

        except Exception as e:
            if attempt < max_retries - 1:
                # Reliability: Implement Exponential Backoff
                delay = 2 ** attempt
                print(f"API Error for {ticker}: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"❌ FATAL: Failed to fetch {ticker} after {max_retries} attempts.")
                return None
    return None


# --- ETL TASKS ---

def extract_and_transform_data(**kwargs):
    """
    Day 3/4: Reads configuration, extracts live data reliably, and transforms it (Pandas).
    """
    # 1. BEST PRACTICE: Read configuration dynamically from config.json
    try:
        # Use the absolute path where the DAG and config file are mounted inside the container
        with open('/opt/airflow/dags/config.json', 'r') as f: 
            config = json.load(f)
        STOCK_TICKERS = config['stock_tickers']
        ALERT_THRESHOLD = config['alert_threshold']
        
    except FileNotFoundError:
        print("ERROR: config.json not found! Ensure it is in the DAGs directory.")
        raise
        
    print(f"--- Starting Extraction and Transformation for {STOCK_TICKERS} ---")
    
    data_list = []
    current_timestamp = datetime.utcnow().isoformat()
    current_date_key = int(datetime.utcnow().strftime('%Y%m%d'))

    for ticker in STOCK_TICKERS:
        raw_data = fetch_ticker_data_with_retry(ticker)
        
        if raw_data is None:
            continue
        
        # T: Transformation (Pandas operations)
        open_price = raw_data['Open']
        close_price = raw_data['Close']
        price_change_percent = ((close_price - open_price) / open_price) * 100
        
        data_list.append({
            'ticker': ticker,
            'open_price': open_price,
            'close_price': close_price,
            'high_price': raw_data['High'],
            'low_price': raw_data['Low'],
            'volume': raw_data['Volume'],
            'timestamp_utc': current_timestamp,
            'date_key': current_date_key,
            'price_change_percent': price_change_percent
        })

    df = pd.DataFrame(data_list)
    print(f"Generated DataFrame (Count: {len(df)})")
    
    # Push the DataFrame and the Alert Threshold for the next tasks
    kwargs['ti'].xcom_push(key='ALERT_THRESHOLD', value=ALERT_THRESHOLD)
    return df.to_json()


def load_data_to_postgres(**kwargs):
    """
    Day 5: Loads the transformed data using advanced UPSERT logic (ON CONFLICT DO UPDATE).
    """
    print("--- Starting Advanced Loading (UPSERT) ---")
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='extract_and_transform', key='return_value')
    df = pd.read_json(json_data)
    
    if df.empty:
        print("No data to load. Skipping.")
        return

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # --- DIMENSION TABLE INSERTS (Simple ON CONFLICT DO NOTHING) ---
    
    # 1. Insert into dim_security 
    unique_securities = df[['ticker']].drop_duplicates()
    for index, row in unique_securities.iterrows():
        pg_hook.run(f"""
        INSERT INTO dim_security (ticker, company_name, exchange, industry)
        VALUES ('{row['ticker']}', '{row['ticker']} Corp.', 'NASDAQ', 'Technology')
        ON CONFLICT (ticker) DO NOTHING;
        """)

    # 2. Insert into dim_date
    date_key = df['date_key'].iloc[0]
    full_date = pd.to_datetime(df['timestamp_utc'].iloc[0]).date()
    pg_hook.run(f"""
        INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, month_name, year, quarter) 
        VALUES ({date_key}, '{full_date}', 1, 'Monday', 'October', 2025, 4) 
        ON CONFLICT (date_key) DO NOTHING;
    """)

    # --- FACT TABLE UPSERT (Day 5 Skill: ON CONFLICT DO UPDATE) ---
    
    # Fetch PKs
    unique_tickers = df['ticker'].unique()
    ticker_sql_list = "('" + "', '".join(unique_tickers) + "')" 
    pk_map = pg_hook.get_pandas_df(
        f"SELECT ticker, security_key FROM dim_security WHERE ticker IN {ticker_sql_list}"
    ).set_index('ticker')['security_key'].to_dict()

    # Iterate and perform UPSERT
    for index, row in df.iterrows():
        security_key = pk_map.get(row['ticker'])
        if security_key:
            # Advanced Loading: UPSERT Logic (ON CONFLICT DO UPDATE)
            upsert_sql = f"""
            INSERT INTO fact_market_data (
                security_key, date_key, timestamp_utc, open_price, close_price, 
                high_price, low_price, volume, price_change_percent
            )
            VALUES (
                {security_key}, 
                {row['date_key']}, 
                '{row['timestamp_utc']}', 
                {row['open_price']}, 
                {row['close_price']}, 
                {row['high_price']}, 
                {row['low_price']}, 
                {row['volume']}, 
                {row['price_change_percent']}
            )
            ON CONFLICT (security_key, timestamp_utc) 
            DO UPDATE SET 
                close_price = EXCLUDED.close_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                volume = EXCLUDED.volume,
                price_change_percent = EXCLUDED.price_change_percent;
            """
            pg_hook.run(upsert_sql)
            
    print(f"Successfully loaded/updated {len(df)} market data records using UPSERT.")
    
    # Push the closing price of MSFT for the next alert task
    msft_price = df[df['ticker'] == 'MSFT']['close_price'].iloc[0]
    kwargs['ti'].xcom_push(key='MSFT_CLOSE_PRICE', value=msft_price)


def check_for_alerts(**kwargs):
    """
    Final Task: Checks price against dynamic threshold and generates a mock alert.
    (Features 'Alerting System')
    """
    print("--- Starting Alert Check ---")
    ti = kwargs['ti']
    
    # Pull data from the previous task and the config from the first task
    msft_price = ti.xcom_pull(task_ids='load_to_database', key='MSFT_CLOSE_PRICE')
    alert_threshold = ti.xcom_pull(task_ids='extract_and_transform', key='ALERT_THRESHOLD')
    
    if msft_price is None or alert_threshold is None:
        print("Alert data not found in XCom. Skipping alert check.")
        return
    
    print(f"MSFT Current Close Price: ${msft_price:.2f}")
    
    if msft_price < alert_threshold: 
        alert_message = f"🚨 LOW PRICE ALERT: MSFT closed at ${msft_price:.2f}, below the critical threshold of ${alert_threshold:.2f}!"
        print("\n" + "*"*60)
        print(alert_message)
        print("ACTION: Triggering external alert service (simulated).")
        print("*"*60 + "\n")
    else:
        print(f"✅ MSFT price (${msft_price:.2f}) is above the alert threshold. No action needed.")


# --- AIRFLOW DAG DEFINITION ---

with DAG(
    dag_id="live_market_data_and_alerting_pipeline_final",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule='0 9 * * 1-5', # Automated scheduling
    catchup=False,
    tags=["financial", "alerting", "live-data", "reliability", "best-practice"],
) as dag:
    
    task_extract_and_transform = PythonOperator(
        task_id="extract_and_transform",
        python_callable=extract_and_transform_data,
    )

    task_load_data = PythonOperator(
        task_id="load_to_database",
        python_callable=load_data_to_postgres,
    )
    
    task_alerting = PythonOperator(
        task_id="check_for_alerts",
        python_callable=check_for_alerts,
    )

    # Define the workflow sequence: ETL -> Alerting
    task_extract_and_transform >> task_load_data >> task_alerting