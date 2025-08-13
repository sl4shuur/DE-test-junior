import requests, os, logging, dotenv

dotenv.load_dotenv()

APP_ID_ENV = os.getenv("OER_APP_ID")
if not APP_ID_ENV:
    logging.warning("OER_APP_ID environment variable is not set.")


def get_exchange_rates(
    app_id: str | None = APP_ID_ENV, base: str = "EUR"
) -> tuple[dict[str, float], int]:
    """
    Fetch latest exchange rates from OpenExchangeRates API

    Args:
        app_id: OpenExchangeRates API key
        base: Base currency for rates (default: EUR)

    Returns:
        Tuple of (rates_dict, timestamp)

    Example
    -------
        ({
            "USD": 1.0,
            "EUR": 0.85,
            "BTC": 8.216601e-06
        }, 1245678990)
    """
    if not app_id:
        raise ValueError("OER_APP_ID environment variable is required")

    url = "https://openexchangerates.org/api/latest.json"
    # BUG: base parameter is not free :(
    params = {"app_id": app_id}

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    logging.debug(f"Fetched exchange rates: {data['rates']} at {data['timestamp']}")
    return data["rates"], data["timestamp"]


def get_supported_currencies(app_id: str | None = APP_ID_ENV) -> dict[str, str]:
    """Get list of supported currencies from OpenExchangeRates

    Example
    -------
        {
            "USD": "United States Dollar",
            "EUR": "Euro",
            ...
        }
    """
    if not app_id:
        raise ValueError("OER_APP_ID environment variable is required")

    url = "https://openexchangerates.org/api/currencies.json"
    params = {"app_id": app_id}

    response = requests.get(url, params=params)
    response.raise_for_status()

    return response.json()



def convert_to_eur(amount: float, from_currency: str, rates: dict[str, float]) -> float | None:
    """
    Convert amount from any currency to EUR using USD as base
    
    Args:
        amount: Amount to convert
        from_currency: Source currency code
        rates: Rates dict from OpenExchangeRates (base USD)
    
    Returns:
        Converted amount in EUR or None if conversion not possible
    """
    if from_currency == "EUR":
        return amount
    
    # Free plan uses USD as base: 1 USD = rates[CURRENCY]
    # Convert: CURRENCY -> USD -> EUR
    if from_currency not in rates or "EUR" not in rates:
        logging.warning(f"Conversion from {from_currency} to EUR not supported")
        return None
    
    # 1 CURRENCY = 1/rates[CURRENCY] USD
    usd_amount = amount / rates[from_currency]
    # USD * rates["EUR"] = EUR
    eur_amount = usd_amount * rates["EUR"]
    
    return round(eur_amount, 2)
