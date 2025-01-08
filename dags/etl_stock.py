import datetime
import json
from datetime import timedelta

import pandas as pd
import psycopg2
import requests
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

API_KEY = "KH5R8XPV2QNOGX53"
TARGET_SYMBOLS = ["IBM", "AAPL", "AMZN", "GOOG", "MSFT"]


def transform_all_stocks(**kwargs):
    ti = kwargs["ti"]
    stock_symbols = kwargs["stock_symbols"]
    fetched_data = ti.xcom_pull(
        task_ids=[f"fetch_{symbol}" for symbol in stock_symbols]
    )
    transformed_data = []
    for fetched in fetched_data:
        time_series = fetched["Time Series (5min)"]
        df = pd.DataFrame(time_series).T
        df.reset_index(inplace=True)
        df.rename(
            columns={
                "index": "timestamp",
                "1. open": "open",
                "2. high": "high",
                "3. low": "low",
                "4. close": "close",
                "5. volume": "volume",
            },
            inplace=True,
        )

        df["symbol"] = fetched["Meta Data"]["2. Symbol"]
        transformed_data.append(df)

    return transformed_data


def load_data(**kwargs):
    conn = psycopg2.connect(
        dbname="finance",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432",
    )
    cur = conn.cursor()

    ti = kwargs["ti"]
    transformed_data = ti.xcom_pull(task_ids="transform_data")

    for stock_data in transformed_data:
        # Iterate over rows in the DataFrame
        for index, row in stock_data.iterrows():
            timestamp = row["timestamp"]
            symbol = row["symbol"]
            open_price = row["open"]
            high_price = row["high"]
            low_price = row["low"]
            close_price = row["close"]
            volume = row["volume"]

            # Insert or update stock price record
            cur.execute(
                """
                INSERT INTO stock_price (symbol, timestamp, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, timestamp) DO NOTHING;
                """,
                (
                    symbol,
                    timestamp,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                ),
            )

    conn.commit()
    cur.close()
    conn.close()


def fetch_data(symbol):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "5min",
        "apikey": API_KEY,
    }

    r = requests.get(url, params=params)
    data = r.json()
    return data


default_args = {
    "owner": "chanchakorn",
    "start_date": days_ago(0),
    "email": ["gamechanchakorn@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "etl_stocks",
    default_args=default_args,
    description="Get the stock data from Alpha Vantage and load to the dbms",
    schedule_interval=timedelta(days=1),
)

fetch_tasks = []
for symbol in TARGET_SYMBOLS:
    task = PythonOperator(
        task_id=f"fetch_{symbol}",
        python_callable=fetch_data,
        op_args=[symbol],
        dag=dag,
    )
    fetch_tasks.append(task)

transform = PythonOperator(
    task_id=f"transform_data",
    python_callable=transform_all_stocks,
    op_kwargs={
        "stock_symbols": TARGET_SYMBOLS
    },  # Pass the symbols to the transform function
    dag=dag,
)

load = PythonOperator(task_id=f"load", python_callable=load_data, dag=dag)

for task in fetch_tasks:
    task >> transform  # All fetch tasks must complete before transform

transform >> load  # Transform must complete before loading
