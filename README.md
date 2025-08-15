# Junior Data Engineer Solidgate

## Task

1. Create a script that will generate data **every 10 minutes** according to the structure below and write it into the `orders` table in the `postgres-1` database, which needs to be set up locally:

   - **order_id** (uuid)
   - **customer_email** (string)
   - **order_date** (datetime)
   - **amount** (float with 2 decimal places)
   - **currency** (string)

2. Every 10 minutes, 5000 new rows must appear in the table.

3. Deliver the data from this table to another local database (`postgres-2`) on an hourly basis.

4. The data in the table in `postgres-2` must be converted to a single currency – Euro. Let's call the table with converted amounts `orders_eur`.

![diagram](./assets/diagram.png)

## Additional Conditions

1. Scripts must run on Apache Airflow, which is set up locally in a Docker container.
2. For currency conversion, use information from the service: [OpenExchangeRate][1].

   The service has a free plan, which is sufficient to complete the task.

3. The `orders` and `orders_eur` tables can be extended with additional columns (if such are needed to complete the task).
4. The list of currencies for the generator can also be taken from the [OpenExchangeRate][1] service.
5. The table filled by the generator must contain data from the range “for the last 7 days” (i.e. `order_date between current_date - 7d and current_date`).

## Results

The solution must be provided as:

- `docker-compose.yaml` file
- `.py` files (with DAGs) needed to complete the tasks
- Uploaded to GitHub

## How to run and check correctness

1. Ensure you have Docker and Docker Compose installed.
2. Clone the repository and navigate to the project directory.
3. Ensure you have `uv` installed. If not, you can install it using pip:

   ```bash
   pip install uv
   ```

   or see the [Astral uv docs](https://docs.astral.sh/uv/) for more installation options.

4. Sync the dependencies:

   ```bash
   uv sync
   ```

5. Before running the project, create a `.env` file in the root directory:

   ```bash
   PG1_DSN="postgresql://airflow:airflow@localhost:5433/db1"
   PG2_DSN="postgresql://airflow:airflow@localhost:5434/db2"
   OER_APP_ID="your_openexchangerates_app_id_here"
   AIRFLOW_FERNET_KEY="your_generated_fernet_key_here"
   AIRFLOW_CONN_POSTGRES_1="postgresql+psycopg2://airflow:airflow@postgres-1:5432/db1"
   AIRFLOW_CONN_POSTGRES_2="postgresql+psycopg2://airflow:airflow@postgres-2:5432/db2"
   ```

   You can use `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` to generate a Fernet key for `AIRFLOW_FERNET_KEY`.

   Register at [OpenExchangeRates][2] to get your free API key.

6. Run the Docker Compose command to start the services:

   ```bash
   docker-compose up -d
   ```

7. Access the Airflow web interface at `http://localhost:8080` to monitor the DAGs.

8. Check the logs of the Airflow tasks to ensure they are running correctly.

9. Verify the data in the `orders` and `orders_eur` tables to ensure they are populated as expected by accessing to Adminer or pgAdmin (at `http://localhost:8081` or `http://localhost:8082`) and connecting to the databases.

   **Connection details:**

   - **postgres-1**: server: `postgres-1`, username: `airflow`, password: `airflow`, database: `db1`
   - **postgres-2**: server: `postgres-2`, username: `airflow`, password: `airflow`, database: `db2`

## Example of Result

Data of 5000 rows were generated in the `orders` table. Here is one sample row:

![Sample Order](/assets/sample_order.png)

Now we need to import this data into the `orders_eur` table in the `postgres-2` database, converting all amounts to Euro.

Here is the same sample row, but with the amount converted to Euro:

![Sample Order EUR](/assets/sample_order_eur.png)

1 EUR = 48.52 UAH

X EUR = 387.65 UAH

X = 387.65 / 48.52 = 7.99 EUR

![alt text](./assets/dag.png)
_How DAG looks like_

Everything looks to work fine!

[1]: https://docs.openexchangerates.org/reference/api-introduction
[2]: https://openexchangerates.org/account/app-ids
