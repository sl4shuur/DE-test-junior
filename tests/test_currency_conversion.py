from src.services.exchange_rates import convert_to_eur


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
