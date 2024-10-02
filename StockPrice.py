from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

def return_snowflake_conn():

  # Initialize the SnowflakeHook
  hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
  # Execute the query and fetch results
  return hook.get_conn()

@task
def extract(url):
  r = requests.get(url)
  data = r.json()
  return (data)

@task
def transform(data):
  results = []   # empyt list for now to hold the 90 days of stock info (open, high, low, close, volume)

  for d in data["Time Series (Daily)"]:   # here d is a date: "YYYY-MM-DD"
    stock_info = data["Time Series (Daily)"][d]
    stock_info["date"] = d
    results.append(stock_info)

  return results[:90]

@task
def load(lines, target_table, symbol):
  conn = return_snowflake_conn()
  cur = conn.cursor()

  try:
    cur.execute("BEGIN;")
    cur.execute(f"""
    CREATE OR REPLACE TABLE {target_table} (
      date DATE PRIMARY KEY,
      open DECIMAL (10, 4) NOT NULL,
      high DECIMAL (10, 4) NOT NULL,
      low DECIMAL (10, 4) NOT NULL,
      close DECIMAL (10, 4) NOT NULL,
      volume INT NOT NULL,
      symbol VARCHAR(10) NOT NULL
    )""")

    # to complete this, first create a cursor object via
    for r in lines:
      open = r["1. open"]
      high = r["2. high"]
      low = r["3. low"]
      close = r["4. close"]
      volume = r["5. volume"]
      date = r['date']
      insert_sql = f"INSERT INTO {target_table} (date, open, high, low, close, volume, symbol) VALUES ('{date}', {open}, {high}, {low}, {close}, {volume}, '{symbol}')"
      cur.execute(insert_sql)
    cur.execute("COMMIT;")

  except Exception as e:
    cur.execute("ROLLBACK;")
    print(e)
    raise e
  
  finally:
    cur.close()
    conn.close()

with DAG(
  dag_id = 'StockPrice',
  start_date = datetime(2024,10,1),
  catchup=False,
  tags=['ETL'],
  schedule = '0 10 * * *'
) as dag:
  target_table = "dev.raw_data.stock_price"
  vantage_api_key = Variable.get("vantage_api_key")
  symbol = "SPOT"
  url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'

  #Tasks
  data = extract(url)
  lines = transform(data)
  load(lines, target_table, symbol)