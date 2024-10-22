import logging
from extractors.google_sheet_extractor import extract_google_sheet_data
from extractors.youtube_scraper import scrape_youtube_data
from loaders.snowflake_loader import load_to_snowflake
from utils import setup_logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def run():
    setup_logging()
    logger.info("Starting influencer campaign metrics update")
    
    try:
        # Extract data from Google Sheet
        df = extract_google_sheet_data()
        logger.info(f"Retrieved {len(df)} rows from Google Sheet")

        # Enrich with YouTube data
        df = scrape_youtube_data(df)
        logger.info("Enriched data with YouTube metrics")

        # Update Snowflake table
        load_to_snowflake(df)
        logger.info("Updated Snowflake table successfully")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

    logger.info("Influencer campaign metrics update completed")

if __name__ == "__main__":
    run()