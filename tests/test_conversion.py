import logging
from datetime import datetime

from src.tasks.etl_converter import etl_hourly_conversion

def test_currency_conversion(source_dsn: str, target_dsn: str):
    """Test script for currency conversion ETL"""
    
    logging.debug(f"Starting ETL conversion test at {datetime.now()}")
    
    # Process last 24 hours for testing
    converted = etl_hourly_conversion(source_dsn, target_dsn, hours_back=24)

    logging.info(f"Converted and inserted {converted} orders to EUR")
