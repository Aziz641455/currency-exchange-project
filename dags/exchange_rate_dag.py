from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import psycopg2
import pytz

# Connection info for PostgreSQL
DB_HOST = "postgres"
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASS = "airflow"

# Timezone settings for New York
NEW_YORK_TZ = pytz.timezone("America/New_York")

def calculate_exchange_rate_change_part_a():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST
        )
        cur = conn.cursor()

        # Get yesterday's date and the time 5 PM New York time
        now = datetime.now(NEW_YORK_TZ)
        yesterday = now - timedelta(days=1)
        yesterday_5pm = yesterday.replace(hour=17, minute=0, second=0, microsecond=0)
        timestamp_5pm = int(yesterday_5pm.timestamp() * 1000)  # Convert to milliseconds

        # Fetch the rates at 5 PM New York time yesterday
        query_previous_5pm = f"""
        SELECT ccy_couple, rate, event_time
        FROM rates
        WHERE event_time = {timestamp_5pm};
        """
        
        query_latest = """
        SELECT ccy_couple, rate, event_time
        FROM rates
        WHERE event_time = (SELECT MAX(event_time) FROM rates);
        """

        cur.execute(query_previous_5pm)
        previous_rates = cur.fetchall()

        cur.execute(query_latest)
        latest_rates = cur.fetchall()

        # Calculate the percentage change
        for latest, previous in zip(latest_rates, previous_rates):
            ccy_couple = latest[0]
            latest_rate = latest[1]
            prev_rate = previous[1]
            pct_change = ((latest_rate - prev_rate) / prev_rate) * 100
            timestamp = datetime.fromtimestamp(latest[2] / 1000)  # Convert ms to seconds

            # Insert into exchange_rates table
            cur.execute("""
                INSERT INTO exchange_rates (pair, rate, pct_change, timestamp)
                VALUES (%s, %s, %s, %s)
                """, (ccy_couple, latest_rate, pct_change, timestamp))
        
        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error calculating exchange rate: {e}")

# DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='part_a_currency_exchange_dag',
    default_args=default_args,
    schedule_interval='@hourly',  # Run every hour
    start_date=days_ago(1),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='calculate_exchange_rate_change_part_a',
        python_callable=calculate_exchange_rate_change_part_a
    )
