import logging

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


class GoogleDriveService:
    """
    This class handles authentication and communication with Google Sheets.

    Attributes:
        credentials (Credentials): The Google service account credentials.
        service (Resource): The Google Sheets service object.
    """

    def __init__(self, credentials_json_path: str):
        """
        Initializes the Google Drive service with the provided credentials file.

        Args:
            credentials_json_path (str): The path to the JSON credentials file for the service account.
        """
        self.credentials = Credentials.from_service_account_file(credentials_json_path)
        self.service = build("sheets", "v4", credentials=self.credentials)
        logging.info(
            "GoogleDriveService initialized with credentials from %s",
            credentials_json_path,
        )

    def get_sheet_values(self, spreadsheet_id: str, range_name: str) -> dict:
        """
        Retrieves the values from a specific Google Sheets sheet.

        Args:
            spreadsheet_id (str): The ID of the Google Sheets spreadsheet.
            range_name (str): The cell range to read in A1 notation.

        Returns:
            dict: The data retrieved from the spreadsheet.
        """
        logging.info(
            "Fetching data from spreadsheet ID: %s, range: %s",
            spreadsheet_id,
            range_name,
        )
        sheet = self.service.spreadsheets()
        result = (
            sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        )
        logging.info("Data fetched successfully.")
        return result
