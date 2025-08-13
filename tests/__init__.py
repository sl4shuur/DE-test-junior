from tests.test_generator import test_generate_orders_size
from tests.test_postgres import test_postgres1_generation
from tests.test_currency_conversion import test_eur_conversion, test_orders_batch_conversion
from tests.test_conversion import test_currency_conversion
from tests.test_exchange_api import test_api_connection

__all__ = [
    "test_generate_orders_size",
    "test_postgres1_generation",
    "test_eur_conversion",
    "test_orders_batch_conversion",
    "test_currency_conversion",
    "test_api_connection",
]
