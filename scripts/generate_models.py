import logging
import os
import sys

from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.dbt import ModelGenerator
from common.google.drive import GoogleDriveService, GoogleSheetsClient
from common.templates.dbt import CONFIG, DDP_QUERY

# Load environment variables from a .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    # Path to your credentials JSON file
    CREDENTIALS_JSON_PATH = os.getenv("CREDENTIALS_PATH")

    # Google Sheets details
    SPREADSHEET_ID = os.getenv("CONFIG_SPREADSHEET_ID")
    RANGE_NAME = os.getenv("TABLE_INFO_RANGE")

    # Output directory for generated models
    OUTPUT_DIR = "./dbt_medallion/models/silver/"
    PREFIX = "silver__"

    # Initialize services
    drive_service = GoogleDriveService(CREDENTIALS_JSON_PATH)
    sheets_client = GoogleSheetsClient(drive_service)
    model_generator = ModelGenerator(
        sheets_client, DDP_QUERY, OUTPUT_DIR, PREFIX, CONFIG
    )

    # Generate models
    model_generator.generate_models(SPREADSHEET_ID, RANGE_NAME)


if __name__ == "__main__":
    main()
