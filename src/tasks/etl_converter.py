import psycopg2
from datetime import datetime, timedelta, timezone

from src.services.exchange_rates import get_exchange_rates, convert_to_eur


def fetch_orders_from_source(
    dsn: str, start_time: datetime, end_time: datetime
) -> list[dict]:
    """Fetch orders from source database within time range"""
    
    sql = """
    SELECT order_id, customer_email, order_date, amount, currency
    FROM public.orders
    WHERE order_date >= %s AND order_date < %s
    ORDER BY order_date
    """

    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, (start_time, end_time))
        if cur.description is None:
            return []
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def convert_orders_batch(
    orders: list[dict], rates: dict[str, float], fx_timestamp: int
) -> list[tuple]:
    """
    Convert batch of orders to EUR

    Args:
        orders: List of orders to convert
        rates: Exchange rates for conversion
        fx_timestamp: Timestamp for the exchange rates

    Returns:
        List of tuples ready for database insertion
    """
    converted_records = []
    fx_asof = datetime.fromtimestamp(fx_timestamp)

    for order in orders:
        og_currency = order["currency"]
        amount = float(order["amount"])

        # Convert to EUR
        eur_amount = convert_to_eur(amount, og_currency, rates)
        if eur_amount is None:
            # Skip unsupported currencies
            continue

        # Calculate the rate used for audit purposes
        if og_currency == "EUR":
            fx_rate_used = 1.0
        else:
            fx_rate_used = eur_amount / amount

        converted_records.append(
            (
                order["order_id"],
                order["customer_email"],
                order["order_date"],
                eur_amount,
                og_currency,
                fx_rate_used,
                fx_asof,
            )
        )

    return converted_records


def etl_hourly_conversion(source_dsn: str, target_dsn: str, hours_back: int = 1) -> int:
    """
    Complete ETL process: extract, convert, load

    Args:
        source_dsn: Source database (postgres-1)
        target_dsn: Target database (postgres-2)
        hours_back: How many hours back to process

    Returns:
        Number of records converted and inserted
    """
    from src.repositories.postgres_repo import (
        ensure_orders_eur_table,
        bulk_insert_orders_eur,
    )

    # Calculate time window
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours_back)

    # Ensure target table exists
    ensure_orders_eur_table(target_dsn)

    # Extract orders from source
    orders = fetch_orders_from_source(source_dsn, start_time, end_time)
    if not orders:
        return 0

    # Get current exchange rates
    rates, fx_timestamp = get_exchange_rates()

    # Convert to EUR
    converted_records = convert_orders_batch(orders, rates, fx_timestamp)
    if not converted_records:
        return 0

    # Load to target database
    return bulk_insert_orders_eur(target_dsn, converted_records)
