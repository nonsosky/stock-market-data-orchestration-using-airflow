import pandas as pd
import requests
import dotenv
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
import os
import logging
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

#logging setup
log_file = "myscript_logs.log"
def log_message(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, "a") as f:
        f.write(f"[{timestamp}] {message}\n")

try:

    # Load environment variables
    dotenv.load_dotenv()

    API_KEY = os.getenv("API_KEY")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PORT = os.getenv("DB_PORT")
    DB_PASS = os.getenv("DB_PASS")
    DB_HOST = os.getenv("DB_HOST")

    # Ingestion Layer
    BASE_URL = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey={API_KEY}'

    stock_market = []
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        data = response.json()
        stock_market.append(data)
    else:
        print('Unable to fetch data')


    # Transformation layer
    stock_market = data["Time Series (Daily)"]

    df = pd.DataFrame.from_dict(stock_market, orient='index')

    # Rename columns for clarity
    df.rename(columns={
        "1. open": "Open",
        "2. high": "High",
        "3. low": "Low",
        "4. close": "Close",
        "5. volume": "Volume"
    }, inplace=True)

    # Reset index to have the date as a column
    df.reset_index(inplace=True)
    df.rename(columns={"index": "Date"}, inplace=True)

    #Convert datatype
    df['Date'] = pd.to_datetime(df["Date"])
    df['Open'] = pd.to_numeric(df["Open"])
    df['High'] = pd.to_numeric(df["High"])
    df['Low'] = pd.to_numeric(df["Low"])
    df['Close'] = pd.to_numeric(df["Close"])
    df['Volume'] = pd.to_numeric(df["Volume"])


    #Loading Layer
    Base = declarative_base()

    class Stock_Price(Base):
        __tablename__ = 'new_stock'

        id = Column(Integer, primary_key=True)
        Date = Column(DateTime)
        Open = Column(Float)
        High = Column(Float)
        Low = Column(Float)
        Close = Column(Float)
        Volume = Column(Integer)

    dotenv.load_dotenv()
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}')

    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    data_to_insert = df.to_dict(orient='records')
    session.bulk_insert_mappings(Stock_Price, data_to_insert)
    session.commit()
    session.close()
    log_message("Script execution completed successfully")
except Exception as e:
    log_message(f"An error occurred: {str(e)}")