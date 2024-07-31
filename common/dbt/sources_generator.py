import json
import logging
import os

import yaml
from dotenv import load_dotenv

from common.google.drive import GoogleSheetsClient

load_dotenv()


class SourcesGenerator:
    """
    Generates and updates the sources.yml file for dbt, particularly for the bronze dataset in BigQuery.

    Attributes:
        client (GoogleSheetsClient): An instance of GoogleSheetsClient to fetch data from Google Sheets.
    """

    def __init__(self, client: GoogleSheetsClient):
        """
        Initializes the SourcesGenerator with the provided Google Sheets client.

        Args:
            client (GoogleSheetsClient): The Google Sheets client to fetch data.
        """
        self.client = client
        logging.info("SourcesGenerator initialized.")

    def fetch_tables_and_fields(
        self,
        tables_sheet_id: str,
        tables_range: str,
        fields_sheet_id: str,
        fields_range: str,
    ):
        """
        Fetches tables and fields information from Google Sheets.

        Args:
            tables_sheet_id (str): The ID of the Google Sheets spreadsheet containing tables.
            tables_range (str): The range of cells in the sheet containing tables data.
            fields_sheet_id (str): The ID of the Google Sheets spreadsheet containing fields.
            fields_range (str): The range of cells in the sheet containing fields data.

        Returns:
            dict: A dictionary containing the structured data for the sources.yml.
        """
        tables_data = self.client.get_sheet_as_json(tables_sheet_id, tables_range)
        fields_data = self.client.get_sheet_as_json(fields_sheet_id, fields_range)

        tables = self._parse_rows(tables_data)
        fields = self._parse_rows(fields_data)

        return self._structure_source_data(tables, fields)

    def _parse_rows(self, json_data: str) -> list:
        """
        Parses JSON data into a list of dictionaries representing rows.

        Args:
            json_data (str): The JSON data to parse.

        Returns:
            list: A list of dictionaries, each representing a row.
        """
        logging.info("Parsing rows from JSON data.")
        data = json.loads(json_data)
        rows = []
        headers = data.get("values")[0]
        for row in data.get("values")[1:]:
            row_data = dict(zip(headers, row))
            rows.append(row_data)
        logging.info("Parsed %d rows.", len(rows))
        return rows

    def _structure_source_data(self, tables: list, fields: list) -> dict:
        """
        Structures the data into the format required for sources.yml.

        Args:
            tables (list): List of table data from the Google Sheet.
            fields (list): List of field data from the Google Sheet.

        Returns:
            dict: The structured data for sources.yml.
        """
        sources_data = {}
        for table in tables:
            table_name = table["table"]
            table_entry = {
                "name": table_name,
                "description": f"Table {table_name} from the bronze dataset.",
                "columns": [],
            }

            for field in fields:
                if field["table"] == table_name:
                    field_entry = {
                        "name": field["field"],
                        "description": field.get("description", ""),
                        "type": field.get("type", ""),
                    }
                    table_entry["columns"].append(field_entry)

            sources_data[table_name] = table_entry

        return sources_data

    def update_sources_yml(self, sources_data: dict, output_path: str):
        """
        Updates the sources.yml file with the provided data.

        Args:
            sources_data (dict): The data to update in sources.yml.
            output_path (str): The path to the sources.yml file.
        """
        if os.path.exists(output_path):
            with open(output_path, "r") as f:
                current_data = yaml.safe_load(f)
        else:
            current_data = {"version": 2, "sources": []}

        updated_sources = [
            source for source in current_data["sources"] if source["name"] != "bronze"
        ]

        bronze_source = {
            "name": "bronze",
            "description": "Dataset bronze in BigQuery.",
            "database": os.getenv("GCP_PROJECT_ID", "ip-trabajo-gverdugo"),
            "schema": "bronze",
            "tables": list(sources_data.values()),
        }
        updated_sources.append(bronze_source)

        # Ensure version is at the top
        final_data = {
            "version": current_data.get("version", 2),
            "sources": updated_sources,
        }

        with open(output_path, "w") as f:
            yaml.dump(final_data, f, default_flow_style=False, sort_keys=False)
        logging.info("sources.yml updated at %s", output_path)
