import json
import logging
import os

from common.google.drive import GoogleSheetsClient


class ModelGenerator:
    """
    This class generates dbt models from data retrieved from Google Sheets.

    Attributes:
        client (GoogleSheetsClient): An instance of GoogleSheetsClient to fetch data.
        template (str): The template for the model files.
        output_dir (str): The directory where the model files will be saved.
        prefix (str): The prefix to prepend to each model file name.
    """

    def __init__(
        self,
        client: GoogleSheetsClient,
        template: str,
        output_dir: str,
        prefix: str = "",
        config: str = "",
    ):
        """
        Initializes the DbtModelGenerator with the provided client, template, output directory, and prefix.

        Args:
            client (GoogleSheetsClient): The Google Sheets client to fetch data.
            template (str): The template for the model files.
            output_dir (str): The directory where the model files will be saved.
            prefix (str): The prefix to prepend to each model file name.
            config (str): Custom dbt configuration to prepend to each model file.
        """
        self.client = client
        self.template = template
        self.output_dir = output_dir
        self.prefix = prefix
        self.config = config
        logging.info("DbtModelGenerator initialized with prefix '%s'.", prefix)

    def generate_models(self, spreadsheet_id: str, range_name: str):
        """
        Generates dbt models for each row in the Google Sheet.

        Args:
            spreadsheet_id (str): The ID of the Google Sheets spreadsheet.
            range_name (str): The cell range to read in A1 notation.
        """
        data = self.client.get_sheet_as_json(spreadsheet_id, range_name)
        rows = self._parse_rows(data)

        for row in rows:
            source_table = row.get("table")
            model_name = row.get("name", source_table) or source_table
            materialization = row.get(
                "materialization", "view"
            )  # Default materialization
            materialization_keys = row.get("materialization_keys", "")
            tags = row.get("tags", "")

            # Additional tags with materialization type
            final_tags = f"{materialization}-materialization,{tags}".strip(",")

            context = {
                "source_table": source_table,
                "model_name": model_name,
                "materialization": materialization,
                "materialization_keys": materialization_keys,
                "tags": final_tags,
            }
            self._create_model_file(model_name, context)

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

    def _create_model_file(self, model_name: str, context: dict):
        """
        Creates a dbt model file based on the template and context.

        Args:
            model_name (str): The name of the model to create.
            context (dict): The context dictionary with data for the template.
        """
        # Get additional tags from context
        additional_tags = context.get("tags", "")

        # Combine existing config tags with additional tags
        combined_tags = self._merge_tags(self.config, additional_tags)

        # Update the config with combined tags
        config_with_combined_tags = self.config.replace(
            "tags=['auto-generated']", f"tags=[{combined_tags}]"
        )

        # Define the path for the new model file
        file_path = os.path.join(self.output_dir, f"{self.prefix}{model_name}.sql")

        # Write the updated config and model content to the file
        with open(file_path, "w") as model_file:
            model_content = (
                f"{config_with_combined_tags}\n{self.template.format(**context)}"
            )
            model_file.write(model_content)

        logging.info("Model file created at %s", file_path)

    def _merge_tags(self, config: str, additional_tags: str) -> str:
        """
        Merges additional tags from the context with existing tags from the config.

        Args:
            config (str): The configuration string containing existing tags.
            additional_tags (str): A comma-separated string of additional tags.

        Returns:
            str: A comma-separated string of combined tags.
        """
        # Extract existing tags from config
        start = config.find("tags=[") + len("tags=[")
        end = config.find("]", start)
        existing_tags = set(config[start:end].replace("'", "").split(","))

        # Add additional tags, splitting by comma and stripping any spaces
        if additional_tags:
            new_tags = set(tag.strip() for tag in additional_tags.split(","))
        else:
            new_tags = set()

        # Combine the two sets of tags
        combined_tags = existing_tags.union(new_tags)

        # Return combined tags as a formatted string
        return ",".join(f"'{tag}'" for tag in combined_tags if tag)
