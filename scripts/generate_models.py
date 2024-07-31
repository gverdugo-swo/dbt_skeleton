import logging
import os
import sys

from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.dbt import ModelGenerator
from common.dbt.templates import CONFIG, DDP_QUERY
from common.google.drive import GoogleDriveService, GoogleSheetsClient

# Load environment variables from a .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    # Path to your credentials JSON file
    CREDENTIALS_JSON_PATH = os.getenv(
        "CREDENTIALS_PATH", "./dbt_medallion/dbt_runner.json"
    )

    # Google Sheets details
    SPREADSHEET_ID = os.getenv(
        "CONFIG_SPREADSHEET_ID", "1JvKh3nY7bFPKLc5Mt2_NwTj1M3fnM9Qac2VgEnMgeGY"
    )
    RANGE_NAME = os.getenv("TABLE_INFO_RANGE", "TABLE_INFO")
    DBT_PROJECT_FOLDER = os.getenv("DBT_PROJECT_FOLDER", "./dbt_medallion")

    # Output directory for generated models
    OUTPUT_DIR = f"{DBT_PROJECT_FOLDER}/models/silver/"
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
