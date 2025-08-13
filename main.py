import logging

from tests.test_generator import test_generate_orders_size
from src.utils.logging_config import setup_logging

setup_logging(logging.INFO, full_color=True, include_function=True)

def main():
    logging.info("Hello from de-test-junior!")


if __name__ == "__main__":
    test_generate_orders_size()
    logging.success("All tests passed!")
