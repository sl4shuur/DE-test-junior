import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import psycopg2
from src.repositories.postgres_repo import ensure_orders_eur_table, insert_orders_eur
from src.services.exchange_rates import get_exchange_rates, convert_to_eur


def extract_orders_from_source(
    source_dsn: str, hours_back: int = 1
) -> List[Dict[str, Any]]:
    """Extract orders from source database for the last N hours"""

    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)

    query = """
    SELECT order_id, customer_email, order_date, amount, currency
    FROM public.orders 
    WHERE order_date >= %s
    ORDER BY order_date DESC
    """

    try:
        with psycopg2.connect(source_dsn) as conn, conn.cursor() as cur:
            cur.execute(query, (cutoff_time,))

            if cur.description is not None:
                columns = [desc[0] for desc in cur.description]
                orders = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                orders = []

        logging.info(f"Extracted {len(orders)} orders from {cutoff_time}")
        return orders

    except Exception as e:
        logging.error(f"Failed to extract orders: {e}")
        raise


def transform_orders_to_eur(
    orders: List[Dict[str, Any]], rates: Dict[str, float]
) -> List[Dict[str, Any]]:
    """Transform orders to EUR using provided exchange rates"""

    eur_orders = []
    conversion_stats = {}

    for order in orders:
        # Convert amount to EUR
        amount_eur = convert_to_eur(
            amount=order["amount"], from_currency=order["currency"], rates=rates
        )

        if amount_eur is not None:
            # Calculate exchange rate used
            fx_rate = (
                rates.get(order["currency"], 1.0) if order["currency"] != "EUR" else 1.0
            )

            eur_order = {
                "order_id": order["order_id"],
                "customer_email": order["customer_email"],
                "order_date": order["order_date"],
                "amount_eur": amount_eur,
                "src_currency": order["currency"],
                "fx_rate_used": fx_rate,
                "fx_asof": datetime.now(timezone.utc),
            }

            eur_orders.append(eur_order)

            # Track conversion stats
            currency = order["currency"]
            conversion_stats[currency] = conversion_stats.get(currency, 0) + 1
        else:
            logging.warning(
                f"Failed to convert order {order['order_id']} from {order['currency']}"
            )

    logging.info(f"Conversion stats: {conversion_stats}")
    return eur_orders


def load_orders_to_target(target_dsn: str, eur_orders: List[Dict[str, Any]]) -> int:
    """Load EUR orders to target database"""

    if not eur_orders:
        return 0

    # Ensure target table exists
    ensure_orders_eur_table(target_dsn)

    # Insert orders
    inserted_count = insert_orders_eur(target_dsn, eur_orders)

    logging.info(f"Loaded {inserted_count} EUR orders to target database")
    return inserted_count


def etl_hourly_conversion(source_dsn: str, target_dsn: str, hours_back: int = 1) -> int:
    """Complete ETL process - kept for backward compatibility"""

    # Extract
    orders = extract_orders_from_source(source_dsn, hours_back)

    if not orders:
        logging.info("No orders to process")
        return 0

    # Transform
    rates, timestamp = get_exchange_rates()
    eur_orders = transform_orders_to_eur(orders, rates)

    # Load
    loaded_count = load_orders_to_target(target_dsn, eur_orders)

    return loaded_count
