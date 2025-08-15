import psycopg2
import psycopg2.extras
import logging
from typing import List, Dict, Any

ORDERS_DDL = """
CREATE TABLE IF NOT EXISTS public.orders (
    order_id UUID PRIMARY KEY,
    customer_email VARCHAR(255) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    currency VARCHAR(10) NOT NULL
);
"""


def ensure_orders_table(dsn: str) -> None:
    """
    Create orders table if it does not exist.
    """
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(ORDERS_DDL)


def insert_orders(dsn: str, rows: list[dict]) -> int:
    """
    Bulk insert orders and return the actual number of inserted rows.
    Uses ON CONFLICT DO NOTHING and counts via RETURNING.
    """
    if not rows:
        return 0

    records = [
        (
            r["order_id"],
            r["customer_email"],
            r["order_date"],
            r["amount"],
            r["currency"],
        )
        for r in rows
    ]
    sql = """
        INSERT INTO public.orders (order_id, customer_email, order_date, amount, currency)
        VALUES %s
        ON CONFLICT (order_id) DO NOTHING
        RETURNING 1
    """

    TOTAL_INSERTED = 0
    CHUNK_SIZE = 1000

    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i : i + CHUNK_SIZE]
            # Force single statement per chunk to fetch the correct count
            psycopg2.extras.execute_values(
                cur, sql, chunk, template="(%s,%s,%s,%s,%s)", page_size=len(chunk)
            )
            TOTAL_INSERTED += len(cur.fetchall())
        return TOTAL_INSERTED


ORDERS_EUR_DDL = """
CREATE TABLE IF NOT EXISTS public.orders_eur (
    order_id UUID PRIMARY KEY,
    customer_email VARCHAR(255) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    amount_eur NUMERIC(12,2) NOT NULL,
    src_currency VARCHAR(10) NOT NULL,
    fx_rate_used NUMERIC(18,8) NOT NULL,
    fx_asof TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def bulk_insert_orders_eur(dsn: str, records: list[tuple]) -> int:
    """
    Bulk insert converted EUR orders

    Args:
        dsn: Database connection string
        records: List of tuples (order_id, email, date, amount_eur, src_currency, fx_rate, fx_asof)
    """
    if not records:
        return 0

    sql = """
        INSERT INTO public.orders_eur 
        (order_id, customer_email, order_date, amount_eur, src_currency, fx_rate_used, fx_asof)
        VALUES %s
        ON CONFLICT (order_id) DO NOTHING
        RETURNING 1
    """

    total_inserted = 0
    chunk_size = 1000

    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            psycopg2.extras.execute_values(
                cur, sql, chunk, template="(%s,%s,%s,%s,%s,%s,%s)", page_size=len(chunk)
            )
            total_inserted += len(cur.fetchall())
        return total_inserted


def ensure_orders_eur_table(dsn: str) -> None:
    """Create orders_eur table if it does not exist"""
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(ORDERS_EUR_DDL)


def insert_orders_eur(dsn: str, eur_orders: List[Dict[str, Any]]) -> int:
    """Insert EUR orders into orders_eur table"""

    if not eur_orders:
        return 0

    query = """
    INSERT INTO public.orders_eur 
    (order_id, customer_email, order_date, amount_eur, src_currency, fx_rate_used, fx_asof)
    VALUES (%(order_id)s, %(customer_email)s, %(order_date)s, %(amount_eur)s, %(src_currency)s, %(fx_rate_used)s, %(fx_asof)s)
    ON CONFLICT (order_id) DO UPDATE SET
        amount_eur = EXCLUDED.amount_eur,
        fx_rate_used = EXCLUDED.fx_rate_used,
        fx_asof = EXCLUDED.fx_asof
    """

    try:
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            cur.executemany(query, eur_orders)
            inserted = cur.rowcount
            conn.commit()

        return inserted

    except Exception as e:
        logging.error(f"Failed to insert EUR orders: {e}")
        raise
