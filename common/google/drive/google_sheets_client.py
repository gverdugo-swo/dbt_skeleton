import logging

from .google_drive_service import GoogleDriveService
from .google_sheets_parser import GoogleSheetsParser


class GoogleSheetsClient:
    """
    This class acts as an interface for interacting with the Google Sheets service and processing the obtained data.

    Attributes:
        drive_service (GoogleDriveService): An instance of the Google Drive service.
    """

    def __init__(self, drive_service: GoogleDriveService):
        """
        Initializes the Google Sheets client with the provided Google Drive service.

        Args:
            drive_service (GoogleDriveService): An instance of GoogleDriveService to handle authentication and requests.
        """
        self.drive_service = drive_service
        logging.info("GoogleSheetsClient initialized.")

    def get_sheet_as_json(self, spreadsheet_id: str, range_name: str) -> str:
        """
        Retrieves data from a Google Sheets sheet and converts it to JSON format.

        Args:
            spreadsheet_id (str): The ID of the Google Sheets spreadsheet.
            range_name (str): The cell range to read in A1 notation.

        Returns:
            str: A JSON-formatted string of the spreadsheet data.
        """
        logging.info(
            "Getting sheet as JSON for spreadsheet ID: %s, range: %s",
            spreadsheet_id,
            range_name,
        )
        data = self.drive_service.get_sheet_values(spreadsheet_id, range_name)
        json_data = GoogleSheetsParser.to_json(data)
        logging.info("Sheet data successfully converted to JSON.")
        return json_data
