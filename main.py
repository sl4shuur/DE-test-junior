import logging, os, dotenv

dotenv.load_dotenv()
source_dsn = os.getenv("PG1_DSN")
target_dsn = os.getenv("PG2_DSN")

if not source_dsn or not target_dsn:
    logging.error("Source or target DSN is not set.")
    raise ValueError("Please set PG1_DSN and PG2_DSN in .env file.")


from tests import *
from src.services.exchange_rates import get_exchange_rates
from src.utils.logging_config import setup_logging

setup_logging(logging.DEBUG, full_color=True, include_function=True)


def main():
    logging.info("Hello from de-test-junior!")


if __name__ == "__main__":
    try:
        test_api_connection()
        logging.success("All tests passed!")
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        raise e
