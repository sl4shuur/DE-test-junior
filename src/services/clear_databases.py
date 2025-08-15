import os, logging, psycopg2
from dotenv import load_dotenv


def clear_databases():
    """Clear both databases for testing"""
    load_dotenv()

    pg1_dsn = os.getenv("PG1_DSN")
    pg2_dsn = os.getenv("PG2_DSN")

    # Clear postgres-1
    try:
        with psycopg2.connect(pg1_dsn) as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE public.orders;")
            conn.commit()
        logging.success("✅ Cleared orders table in postgres-1")
    except Exception as e:
        logging.error(f"❌ Error clearing postgres-1: {e}")

    # Clear postgres-2
    try:
        with psycopg2.connect(pg2_dsn) as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE public.orders_eur;")
            conn.commit()
        logging.success("✅ Cleared orders_eur table in postgres-2")
    except Exception as e:
        logging.error(f"❌ Error clearing postgres-2: {e}")

    logging.info("\n🎯 Databases cleared!")


if __name__ == "__main__":
    clear_databases()
