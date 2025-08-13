from pathlib import Path
from logging_config import setup_logging

import os, dotenv, logging

setup_logging(level=logging.DEBUG, full_color=True, include_function=True)

# Base of the project
BASE_DIR = Path(__file__).parent.parent.parent

# .env file loading
dotenv_path = BASE_DIR / ".env"
if not dotenv_path.exists():
    raise FileNotFoundError(f"Environment file not found: {dotenv_path}")

dotenv.load_dotenv(dotenv_path)


if __name__ == "__main__":
    logging.debug(f"Base directory: {BASE_DIR}")
