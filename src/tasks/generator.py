from faker import Faker
from datetime import datetime, timedelta
import uuid, random

def generate_orders(n: int, now: datetime) -> list[dict]:
    """
    Generate a list of fake order data.

    Args:
        n (int): Number of orders to generate.
        now (datetime): Current date and time.

    Returns:
        list[dict]: A list of dictionaries containing fake order data.
    """
    fake = Faker()
    start = now - timedelta(days=7)
    rows = []
    for _ in range(n):
        rows.append({
            "order_id": str(uuid.uuid4()),
            "customer_email": fake.email(),
            "order_date": fake.date_time_between(start_date=start, end_date=now),
            "amount": round(random.uniform(1, 999), 2),
            # TODO: replace with OpenExchangeRates API call
            "currency": random.choice(["USD", "EUR", "GBP", "PLN"])
        })
    return rows
