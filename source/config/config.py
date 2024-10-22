import os
from dotenv import load_dotenv

load_dotenv()

# Google Sheets Configuration
GOOGLE_SHEET_ID = os.get('GOOGLE_SHEET_ID')
SERVICE_ACCOUNT_FILE = os.get('SERVICE_ACCOUNT_FILE')
GOOGLE_SHEET_RANGE = 'Influencer_Campaign_Data!A:C'

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}
SNOWFLAKE_TABLE_NAME = 'INFLUENCER_CAMPAIGN_METRICS'

# Logging Configuration
LOG_FILE = 'logs/influencer_metrics.log'