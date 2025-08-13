import uuid, random, logging
from faker import Faker
from datetime import datetime, timezone, timedelta
from src.repositories.postgres_repo import ensure_orders_table, insert_orders
from src.services.exchange_rates import get_supported_currencies


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
    # currencies = list(get_supported_currencies().keys())
    currencies = ["USD", "EUR", "GBP", "BTC"]
    logging.debug(f"Supported currencies: {currencies}")

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
