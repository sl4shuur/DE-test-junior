from src.services.exchange_rates import convert_to_eur
from src.tasks.etl_converter import convert_orders_batch


def test_eur_conversion():
    """Test EUR conversion logic"""
    # Mock rates: 1 USD = 1.0, 1 EUR = 0.85, 1 GBP = 0.8
    rates = {"USD": 1.0, "EUR": 0.85, "GBP": 0.8}

    # Test EUR to EUR (no conversion)
    result = convert_to_eur(100.0, "EUR", rates)
    assert result == 100.0

    # Test USD to EUR: 100 USD -> 100 * 0.85 = 85 EUR
    result = convert_to_eur(100.0, "USD", rates)
    assert result == 85.0

    # Test GBP to EUR: 100 GBP -> (100/0.8) * 0.85 = 106.25 EUR
    result = convert_to_eur(100.0, "GBP", rates)
    assert result == 106.25


def test_orders_batch_conversion():
    """Test batch conversion of orders"""
    orders = [
        {
            "order_id": "1",
            "customer_email": "test@test.com",
            "order_date": "2025-01-01",
            "amount": 100.0,
            "currency": "USD",
        },
        {
            "order_id": "2",
            "customer_email": "test2@test.com",
            "order_date": "2025-01-01",
            "amount": 50.0,
            "currency": "EUR",
        },
    ]

    rates = {"USD": 1.0, "EUR": 0.85}

    result = convert_orders_batch(orders, rates, 1640995200)  # Mock timestamp

    assert len(result) == 2
    assert result[0][3] == 85.0  # 100 USD -> 85 EUR
    assert result[1][3] == 50.0  # 50 EUR -> 50 EUR
