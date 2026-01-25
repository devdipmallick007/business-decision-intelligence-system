# core/featcher.py
import sys
from pathlib import Path

# Add parent directory to path so 'core' module can be found
sys.path.append(str(Path(__file__).parent.parent.resolve()))
from core.db import get_engine
from core.log import logger
import pandas as pd


class DataFetcher:
    """
    Dynamically fetch all tables and their data from a MySQL database.
    Logs table count, column count, and supports custom table/column validation.
    """

    def __init__(self, schema_name: str, expected_schema: dict = None):
        """
        :param schema_name: Name of MySQL database/schema
        :param expected_schema: Optional dict to validate tables and columns
                                Format: {"table_name": ["col1", "col2", ...]}
        """
        self.schema_name = schema_name
        self.expected_schema = expected_schema or {}
        self.engine = get_engine()
        self.all_data = {}

    def get_all_table_names(self):
        """
        Fetch all table names from the schema.
        """
        query = f"""
            SELECT TABLE_NAME
            FROM information_schema.tables
            WHERE table_schema = '{self.schema_name}';
        """
        try:
            tables = pd.read_sql(query, self.engine)
            table_list = tables['TABLE_NAME'].tolist()  # MySQL returns uppercase
            logger.info(f"Found {len(table_list)} tables in schema '{self.schema_name}'")
            return table_list
        except Exception as e:
            logger.exception("Failed to fetch table names")
            raise e

    def fetch_table(self, table_name: str):
        """
        Fetch a single table into a DataFrame, log column names.
        Also validates expected columns if provided.
        """
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", self.engine)
            logger.info(
                f"Fetched table '{table_name}' with {len(df)} rows and {len(df.columns)} columns"
            )
            logger.info(f"Columns in '{table_name}': {list(df.columns)}")

            # Optional column validation
            if table_name in self.expected_schema:
                expected_cols = set(self.expected_schema[table_name])
                actual_cols = set(df.columns)
                missing = expected_cols - actual_cols
                extra = actual_cols - expected_cols

                if missing:
                    logger.warning(f"Table '{table_name}' is missing columns: {missing}")
                if extra:
                    logger.warning(f"Table '{table_name}' has extra columns: {extra}")

            return df
        except Exception as e:
            logger.exception(f"Failed to fetch table '{table_name}'")
            raise e

    def fetch_all_tables(self):
        """
        Fetch all tables dynamically and store in self.all_data dictionary
        """
        table_list = self.get_all_table_names()
        for table in table_list:
            df = self.fetch_table(table)
            self.all_data[table] = df
        logger.info(f"Successfully fetched all tables: {list(self.all_data.keys())}")
        return self.all_data

