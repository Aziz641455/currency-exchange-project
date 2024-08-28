Let's revise the solution, focusing on the purpose of calculating exchange rates and their percentage changes. Here's the solution with code, detailed steps, and architecture.

### Architecture Overview

1. **Database: PostgreSQL**:
   - Table `rates`: Holds the raw exchange rate data ingested from a CSV file.
   - Table `exchange_rates`: Holds the calculated percentage changes for each currency pair.
   - Table `exchange_rates_part_b`: Holds the calculated percentage changes for each currency pair for part B of the assignment.

2. **Airflow**: 
   - DAG `currency_exchange_dag`: Scheduled to run every hour, fetches data from the `rates` table, calculates the percentage change, and updates the `exchange_rates` table.
   - DAG `currency_exchange_dag_part_b`: Scheduled to run every one minute, fetches data from the `rates` table, calculates the percentage change, and updates the `exchange_rates_part_b` table.

### Steps and Code

#### 0. Requirements:
      - You need `Git` installed.
      - You need `Docker` installed to run the env.

#### 1. Set up the environment for the project (Using Docker compose)
You should already have PostgreSQL running inside Docker. Here's the setup.

1. **Start Docker Compose:**
   ```bash
   docker-compose up -d
   ```

#### 2. Create Database Tables

Connect to your database and create the required tables.
   ```bash
   docker exec -it currency-exchange-project-postgres-1 psql -U airflow -d airflow
   ```

Run these commands:
```sql
CREATE TABLE IF NOT EXISTS rates (
    event_id BIGINT PRIMARY KEY,
    event_time BIGINT,
    ccy_couple VARCHAR(10),
    rate NUMERIC
);

CREATE TABLE IF NOT EXISTS exchange_rates (
    id SERIAL PRIMARY KEY,
    pair VARCHAR(10),
    rate NUMERIC,
    pct_change NUMERIC,
    timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS exchange_rates_part_b (
    id SERIAL PRIMARY KEY,
    pair VARCHAR(10),
    rate DECIMAL,
    pct_change DECIMAL,
    timestamp TIMESTAMP
);


```

then you need to exite the container, in windows is ctrl+D

#### 3. Ingest Data Manually into the `rates` Table

Assuming the data ingestion is manual:

```bash
docker cp /path/to/your/rates_sample.csv currency-exchange-project-postgres-1:/tmp/rates_sample.csv

docker exec -it currency-exchange-project-postgres-1 psql -U airflow -d airflow -c "\COPY rates(event_id, event_time, ccy_couple, rate) FROM '/tmp/rates_sample.csv' DELIMITER ',' CSV HEADER;"
```

#### 4. Define the DAG to Calculate Exchange Rates

Here's the DAG to fetch the latest and previous rates and calculate the percentage change.
To add a dag under airflow here you need to add locally under the folder dags then run:
   ```bash
   docker-compose up -d
   ```


#### 5. Run the Airflow DAG

2. **Access Airflow UI:**
   - Open [localhost:8080](http://localhost:8080) in your browser.
   - Trigger the `currency_exchange_dag` to run manually or wait for the scheduled interval.

3. **Check Results:**
   - After running the DAG, query the `exchange_rates` table to verify the calculated percentage changes.
   
   ```bash
   docker exec -it currency-exchange-project-postgres-1 psql -U airflow -d airflow -c "SELECT * FROM exchange_rates;"
   ```

This completes the solution for calculating exchange rates using Airflow and PostgreSQL. Let me know if you need any more adjustments!
