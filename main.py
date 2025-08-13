import logging

from tests import *
from src.utils.logging_config import setup_logging

setup_logging(logging.DEBUG, full_color=True, include_function=True)

def main():
    logging.info("Hello from de-test-junior!")


if __name__ == "__main__":
    try:
        test_generate_orders_size()
        test_postgres1_generation()
        logging.success("All tests passed!")
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        raise e

