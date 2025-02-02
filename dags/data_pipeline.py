from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from utils import extract_data, transform_data, load_data

defaults_args = {
    'owner': 'airflow',
    'email': 'nonsoskyokpara@gmail.com',
    'email_on_failure': True,
    'depends_on_past': True,
    'retries': 3,
    'retry delay': timedelta(minutes=5),
    'scgedule_interval': '@daily'
}

with DAG(
'stock_market_data_pipeline',
defaults_args=defaults_args,
description='A simple stock market data pipeline',
tags=['stock_market', 'etl', 'pipeline', 'data engineering'],
) as dag:
    extract_task = PythonOperator(
        task_id = 'extract_stock_market',
        python_callable = extract_data
    )

    transfrom_task = PythonOperator(
        task_id = 'transform_stock_data',
        python_callable = transform_data
    )

    load_task = PythonOperator(
        task_id = 'load_stock_data',
        python_callable = load_data
    )

    extract_task >> transfrom_task >> load_task