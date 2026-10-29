import json
import logging

logger = logging.getLogger(__name__)

def load_data(data_date):
    file_path = f"./data/YT_data_{data_date}.json" # Файл с выгрузкой за дату запуска

    try:
        logger.info(f"Processing file: {file_path}") # Log the file being processed
        with open(file_path, 'r', encoding='utf-8') as raw_data: # Open the JSON file for reading
            data = json.load(raw_data) # Load the contents of the JSON file into a Python dictionary

        return data # Return the loaded data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}") # Log an error if the file is not found
        raise
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from file: {file_path}") # Log an error if there is an issue decoding the JSON
        raise