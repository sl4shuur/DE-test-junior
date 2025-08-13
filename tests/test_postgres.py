import logging, os
from dotenv import load_dotenv
from datetime import datetime, timezone

from src.tasks.generator import generate_orders
from src.repositories.postgres_repo import ensure_orders_table, insert_orders


def generate_to_postgres(dsn: str, n: int = 5000, seed: int | None = 69) -> int:
    """
    Generate orders and insert them into Postgres.

    Args:
        dsn (str): Data Source Name for Postgres connection.
        n (int, optional): Number of orders to generate. Defaults to 5000.
        seed (int | None, optional): Random seed for order generation. Defaults to 69.

    Returns:
        int: Number of inserted rows.
    """
    ensure_orders_table(dsn)

    now = datetime.now(timezone.utc)
    rows = generate_orders(n, now, seed=seed)
    return insert_orders(dsn, rows)


def test_postgres1_generation():
    
    load_dotenv()
    dsn = os.getenv("PG_DSN")

    if not dsn:
        raise ValueError("PG_DSN environment variable is not set. Please set it in .env file.")
    
    num_orders = 5000

    inserted = generate_to_postgres(dsn, n=num_orders)
    logging.debug(f"Inserted {inserted} rows into Postgres-1")
    assert inserted == num_orders
