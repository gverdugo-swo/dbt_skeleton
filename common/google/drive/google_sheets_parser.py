import json
import logging


class GoogleSheetsParser:
    """
    This class handles transforming the data obtained from Google Sheets into various formats.
    """

    @staticmethod
    def to_json(data: dict) -> str:
        """
        Converts a dictionary of data into a formatted JSON string.

        Args:
            data (dict): The data to convert.

        Returns:
            str: A JSON-formatted string of the data.
        """
        logging.info("Converting data to JSON format.")
        json_data = json.dumps(data, indent=4)
        logging.info("Data conversion to JSON successful.")
        return json_data
