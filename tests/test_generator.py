from src.tasks.generator import generate_orders
from datetime import datetime

def test_generate_orders_size():
    rows = generate_orders(5000, datetime(2025, 8, 13))
    assert len(rows) == 5000
    assert {"order_id","customer_email","order_date","amount","currency"} <= rows[0].keys()  # subset check
