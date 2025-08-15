import logging, os
from dotenv import load_dotenv

from src.tasks.generator import generate_to_postgres


def test_postgres1_generation():

    load_dotenv()
    dsn = os.getenv("PG1_DSN")

    if not dsn:
        raise ValueError("PG1_DSN environment variable is not set. Please set it in .env file.")

    num_orders = 5000

    inserted = generate_to_postgres(dsn, n=num_orders)
    logging.debug(f"Inserted {inserted} rows into Postgres-1")
    assert inserted == num_orders
