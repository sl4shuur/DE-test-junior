import psycopg2
import psycopg2.extras

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
