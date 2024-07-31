import logging
import os
import sys

from dotenv import load_dotenv

# Add the parent directory to the system path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.dbt import SourcesGenerator
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
    TABLES_RANGE = os.getenv("TABLE_INFO_RANGE", "TABLE_INFO")
    FIELDS_RANGE = os.getenv("FIELD_INFO_RANGE", "FIELD_INFO")
    DBT_PROJECT_FOLDER = os.getenv("DBT_PROJECT_FOLDER", "./dbt_medallion")

    # Output path for sources.yml
    OUTPUT_PATH = f"{DBT_PROJECT_FOLDER}/models/sources.yml"

    # Initialize services
    drive_service = GoogleDriveService(CREDENTIALS_JSON_PATH)
    sheets_client = GoogleSheetsClient(drive_service)
    sources_generator = SourcesGenerator(sheets_client)

    # Fetch tables and fields data
    sources_data = sources_generator.fetch_tables_and_fields(
        SPREADSHEET_ID, TABLES_RANGE, SPREADSHEET_ID, FIELDS_RANGE
    )

    # Update sources.yml
    sources_generator.update_sources_yml(sources_data, OUTPUT_PATH)


if __name__ == "__main__":
    main()
