import pandas as pd
import requests
import dotenv
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
import os
import logging
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# #logging setup
# log_file = "myscript_logs.log"
# def log_message(message):
#     timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     with open(log_file, "a") as f:
#         f.write(f"[{timestamp}] {message}\n")



# Load environment variables
dotenv.load_dotenv()

API_KEY = os.getenv("API_KEY")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PORT = os.getenv("DB_PORT")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")

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


# Ingestion Layer
def extract_data(**kwargs):

    BASE_URL = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey={API_KEY}'

    stock_market = []
    try:

        response = requests.get(BASE_URL)
        if response.status_code == 200:
            data = response.json()
            stock_market.append(data)
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        print("Error fetching data: {e}")
    finally:
        if len(stock_market) == 0:
            logging.error("No data fetched")
            print("No data fetched")
        else:
            logging.info("Data fetched successfully")
            print("Data fetched successfully")
    ti = kwargs['ti']
    ti.xcom_push(key='stock_market', value=stock_market)
    return stock_market, data


# Transformation layer
def transform_data(**kwargs):
    ti = kwargs['ti']
    stock_market = ti.xcom_pull(key='stock_market_data', task_ids='extract_stock_market')
    data = ti.xcom_pull(key='stock_market_data', task_ids='extract_stock_market')
    try:

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
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        print(f"Error transforming data: {e}")
    finally:
        if len(df) == 0:
            logging.error("No data transformed")
            print("No data transformed")
        else:
            logging.info("Data transformed successfully")
            print("Data transformed successfully")

    ti.xcom_push(key='df', value=df)
    return df


def load_data(**kwargs):
    #Loading Layer
    ti = kwargs['ti']
    df = ti.xcom_pull(key='df', task_ids='transform_stock_data')
    dotenv.load_dotenv()
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}')
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    try:
        data_to_insert = df.to_dict(orient='records')
        session.bulk_insert_mappings(Stock_Price, data_to_insert)
        session.commit()
        logging.info("Data loaded successfully")
        print("Data loaded successfully")
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Error loading data: {e}")
        print(f"Error loading data: {e}")
    finally:
        session.close()