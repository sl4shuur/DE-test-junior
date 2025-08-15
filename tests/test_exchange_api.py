import logging
from dotenv import load_dotenv
from src.services.exchange_rates import get_exchange_rates, convert_to_eur

def test_api_connection():
    """Test OpenExchangeRates API connection"""
    load_dotenv()
    
    try:
        rates, timestamp = get_exchange_rates()
        logging.info(f"✅ API works! Got {len(rates)} currencies")
        logging.info(f"📅 Rates timestamp: {timestamp}")
        logging.info(f"💰 Sample rates: USD={rates.get('USD', 'N/A')}, GBP={rates.get('GBP', 'N/A')}")
        
        # Test conversion
        test_amount = 100.0
        usd_to_eur = convert_to_eur(test_amount, "USD", rates)
        gbp_to_eur = convert_to_eur(test_amount, "GBP", rates)

        logging.info(f"🔄 Conversion test:")
        logging.info(f"   {test_amount} USD = {usd_to_eur} EUR")
        logging.info(f"   {test_amount} GBP = {gbp_to_eur} EUR")

        return True
        
    except Exception as e:
        logging.error(f"❌ API test failed: {e}")
        return False
