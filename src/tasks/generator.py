from faker import Faker
from datetime import datetime, timedelta
import uuid, random


def generate_orders(n: int, now: datetime, seed: int | None = 69) -> list[dict]:
    """
    Generate a list of fake order data.

    Args:
        n (int): Number of orders to generate.
        now (datetime): Current date and time.
        seed (int | None): Optional seed for random number generation.

    Returns:
        list[dict]: A list of dictionaries containing fake order data.
    """
    rnd = random.Random(seed)
    fake = Faker()
    if seed is not None:
        Faker.seed(seed)

    start = now - timedelta(days=7)
    rows: list[dict] = []
    # TODO: replace with OpenExchangeRates API call
    currencies = ["USD", "EUR", "GBP", "PLN"]

    for _ in range(n):
        # Uniformly distribute timestamps within [start, now]
        t = start + (now - start) * rnd.random()
        rows.append(
            {
                "order_id": str(uuid.uuid4()),
                "customer_email": fake.email(),
                "order_date": t,
                "amount": round(rnd.uniform(1, 999), 2),
                "currency": rnd.choice(currencies),
            }
        )
    return rows
