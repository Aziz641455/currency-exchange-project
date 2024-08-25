from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2
import logging

# Connection info for PostgreSQL
DB_HOST = "postgres"
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASS = "airflow"

def calculate_exchange_rate_change():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST
        )
        cur = conn.cursor()

        # Example query: Fetch current and day-before rates
        query = """
        SELECT pair, rate, timestamp
        FROM exchange_rates
        WHERE timestamp = (
            SELECT MAX(timestamp)
            FROM exchange_rates
            WHERE timestamp < NOW() - INTERVAL '1 hour'
        );
        """
        cur.execute(query)
        results = cur.fetchall()

        for row in results:
            # Logic to calculate the percentage change
            # Example: pct_change = ((current_rate - previous_rate) / previous_rate) * 100
            pass
        
        conn.close()
    except Exception as e:
        logging.error(f"Error calculating exchange rate: {e}")

# DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='currency_exchange_dag',
    default_args=default_args,
    schedule_interval='@hourly',  # Run every hour
    start_date=days_ago(1),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='calculate_exchange_rate_change',
        python_callable=calculate_exchange_rate_change
    )
